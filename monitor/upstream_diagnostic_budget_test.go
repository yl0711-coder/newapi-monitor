package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Real time is intentional: the old 6s client / 25s handler budgets passed
// mocked-clock unit tests but failed against Spring's mandatory 8s spacing.
// Only the final network transport is simulated; no external calls are made.
func TestDiagnosticSpringFullPlanHonorsRealSpacingAndTotalBudget(t *testing.T) {
	t.Parallel()
	m := newChannelUpstreamTestMonitor(t)
	guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{Jitter: func() time.Duration { return 0 }})
	guard.hostState("aicodewith.ai:443").nextStart = time.Now().Add(aiCodeWithUsageRequestInterval)
	client, closeIdle := newGuardedDiagnosticHTTPClient(guard)
	defer closeIdle()
	starts := []time.Time{}
	client.Transport.(*upstreamGuardTransport).base.(*diagnosticNetworkTransport).base = upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		starts = append(starts, time.Now())
		if r.Method != http.MethodGet {
			t.Error("diagnostic attempted a write")
		}
		body := `<html>AICodeWith</html>`
		switch r.URL.Path {
		case "/api/v1/balance":
			body = `{"balance":10,"currency":"USD"}`
		case "/api/v1/api-keys/usage":
			q := r.URL.Query()
			body = fmt.Sprintf(`{"data":{"api_key_id":7,"group_by":"day","period":{"start":%q,"end":%q},"summary":{"cost":0,"total_tokens":0,"requests":0},"daily":[]}}`, q.Get("start"), q.Get("end"))
		default:
			if r.Header.Get("Authorization") != "" {
				t.Error("public probe carried credentials")
			}
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	row := ChannelUpstreamAccount{Domain: "spring.test", BaseURL: "https://aicodewith.ai", Provider: upstreamProviderAICodeWith, CredentialVersion: upstreamCredentialVersion}
	var err error
	row.Credential, err = m.sealUpstreamCredential(row.Domain, row.Provider, aiCodeWithCredential{APIKey: "sk-acw-local-test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), diagnosticTimeout)
	defer cancel()
	report := upstreamDiagnosticReport{}
	diagnosePublicUpstream(ctx, client, row.BaseURL, &report)
	if err := m.probeSavedUpstream(ctx, client, row, &report); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil || len(starts) != 5 || report.Checks[len(report.Checks)-1].Code != "usage_readable" {
		t.Fatalf("calls=%d context=%v report=%+v", len(starts), ctx.Err(), report)
	}
	for i := 1; i < len(starts); i++ {
		// Dispatch bookkeeping occurs after the guard timestamp; allow 10ms of
		// scheduler/measurement variation without accepting an early 6s request.
		if starts[i].Sub(starts[i-1]) < aiCodeWithUsageRequestInterval-10*time.Millisecond {
			t.Fatal("mandatory Spring pacing was weakened")
		}
	}
}

type diagnosticBlockingBody struct{ ctx context.Context }

func (b diagnosticBlockingBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b diagnosticBlockingBody) Close() error { return nil }

func TestDiagnosticNetworkBudgetCoversHeadersAndBodyAndReleasesHost(t *testing.T) {
	for _, slowBody := range []bool{false, true} {
		t.Run(fmt.Sprint(slowBody), func(t *testing.T) {
			guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }})
			client, closeIdle := newGuardedDiagnosticHTTPClient(guard)
			defer closeIdle()
			network := client.Transport.(*upstreamGuardTransport).base.(*diagnosticNetworkTransport)
			network.timeout = 20 * time.Millisecond
			network.base = upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if !slowBody {
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: diagnosticBlockingBody{r.Context()}}, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := diagnosticGET(ctx, client, "https://budget.example", nil)
			if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				t.Fatalf("network deadline did not fire independently: err=%v outer=%v", err, ctx.Err())
			}
			if len(guard.globalSem) != 0 || len(guard.hostState("budget.example:443").sem) != 0 {
				t.Fatal("timed-out request retained a host/global slot")
			}
		})
	}
}

func TestDiagnosticQueueCancellationNeverReachesNetwork(t *testing.T) {
	guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{Jitter: func() time.Duration { return 0 }})
	guard.hostState("aicodewith.ai:443").nextStart = time.Now().Add(aiCodeWithUsageRequestInterval)
	client, closeIdle := newGuardedDiagnosticHTTPClient(guard)
	defer closeIdle()
	client.Transport.(*upstreamGuardTransport).base = upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("canceled queue reached network")
		return nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := diagnosticGET(ctx, client, "https://aicodewith.ai", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(guard.globalSem) != 0 || len(guard.hostState("aicodewith.ai:443").sem) != 0 {
		t.Fatal("canceled queue retained a slot")
	}
}
