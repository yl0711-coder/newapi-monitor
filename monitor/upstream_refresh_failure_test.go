package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestUpstreamRefreshFailureClassificationAcrossProviders(t *testing.T) {
	for _, provider := range []string{upstreamProviderNewAPI, upstreamProviderSub2API, upstreamProviderTokenForce} {
		for _, status := range []int{0, 400, 401, 403, 404, 408, 425, 429, 500, 503} {
			t.Run(provider+"/"+http.StatusText(status), func(t *testing.T) {
				client := &http.Client{Transport: upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if status == 0 {
						return nil, context.DeadlineExceeded
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"36000"}}, Body: io.NopCloser(strings.NewReader(`{"message":"fixture failure"}`))}, nil
				})}
				row := ChannelUpstreamAccount{Domain: "fixture.example", BaseURL: "https://fixture.example"}
				var err error
				switch provider {
				case upstreamProviderNewAPI:
					_, err = refreshNewAPICredential(context.Background(), client, row, newAPICredential{SessionID: "test-session"})
				case upstreamProviderSub2API:
					_, err = refreshSub2API(context.Background(), client, row, sub2APICredential{RefreshToken: "test-refresh"})
				case upstreamProviderTokenForce:
					_, err = refreshTokenForce(context.Background(), client, row, tokenForceCredential{RefreshToken: "test-refresh"})
				}
				if err == nil {
					t.Fatal("failed refresh reported success")
				}
				var auth *upstreamAuthError
				wantAuth := status == 401 || status == 403
				if errors.As(err, &auth) != wantAuth {
					t.Fatalf("auth classification=%v want %v", err, wantAuth)
				}
				now := time.Now().Unix()
				balance, usage := row, row
				applyUpstreamSyncResult(&balance, upstreamBalanceResult{}, err, now, Settings{})
				applyUpstreamUsageResult(&usage, upstreamUsageResult{}, err, now, Settings{})
				wantIsolated := status == 401 || status == 400 || status == 404
				if (balance.NextSyncAt == upstreamAccountIsolatedUntil) != wantIsolated || (usage.UsageNextSyncAt == upstreamAccountIsolatedUntil) != wantIsolated {
					t.Fatal("temporary refresh error permanently isolated a task")
				}
				if status == 429 && (balance.NextSyncAt < now+36000 || usage.UsageNextSyncAt < now+36000) {
					t.Fatal("Retry-After was lost or shortened")
				}
			})
		}
	}
}

func TestDiagnosticNewAPINeverRefreshesSession(t *testing.T) {
	for _, tc := range []struct {
		name         string
		expires      int64
		unauthorized bool
		wantCalls    int
	}{
		{"expired", 1, false, 0},
		{"static", 0, false, 3},
		{"valid", time.Now().Add(time.Hour).Unix(), false, 3},
		{"revoked", 0, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Monitor{cfg: Settings{SessionSecret: "test-monitor-secret"}}
			row := ChannelUpstreamAccount{Domain: "fixture.example", BaseURL: "https://fixture.example", Provider: upstreamProviderNewAPI, UserID: 7, CredentialVersion: upstreamCredentialVersion, BalanceUnit: 500000}
			var err error
			row.Credential, err = m.sealUpstreamCredential(row.Domain, row.Provider, newAPICredential{AccessToken: "test-access", SessionID: "test-session", ExpiresAt: tc.expires})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			client := &http.Client{Transport: upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodGet {
					t.Fatal("diagnostic issued a write/refresh")
				}
				status, body := http.StatusOK, ""
				switch r.URL.Path {
				case "/api/user/self":
					body = `{"success":true,"data":{"quota":1000000}}`
					if tc.unauthorized {
						status, body = 401, `{"message":"revoked"}`
					}
				case "/api/status":
					body = `{"success":true,"data":{"quota_per_unit":500000}}`
				case "/api/log/self":
					body = `{"success":true,"data":{"total":0,"items":[]}}`
				default:
					t.Fatalf("unexpected path: %s", r.URL.Path)
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			report := upstreamDiagnosticReport{}
			if err := m.probeSavedUpstream(context.Background(), client, row, &report); err != nil {
				t.Fatal(err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("calls=%d want %d", calls, tc.wantCalls)
			}
			if tc.wantCalls == 3 && report.Checks[len(report.Checks)-1].Code != "usage_readable" {
				t.Fatalf("%+v", report.Checks)
			}
		})
	}
}
