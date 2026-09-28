package monitor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newFinanceUpgradeSnapshotFixture(t *testing.T) (*Monitor, financeReportRequest, time.Time, []byte) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "upgrade-snapshot.example")
	t.Cleanup(m.Close)
	m.cfg.FinanceFastSnapshotEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(10800, 0), configurationHash: "accounting-config"}
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":3600,"to":10800,"generated_at":%d,"statement":{"known_user_consumption":{"micro_usd":"1234567"}}}`, now.Unix()))
	if err := m.persistFinanceReportSnapshotShadow(financePreviousProjectionKey(request), "previous-source", payload, now); err != nil {
		t.Fatal(err)
	}
	return m, request, now, payload
}

func TestFinanceUpgradeSnapshotDisplayOnlyNeverPromotesMoney(t *testing.T) {
	m, request, now, payload := newFinanceUpgradeSnapshotFixture(t)
	for _, hours := range []int{0, 1, 24, 48} {
		candidate := request
		candidate.to = request.to.Add(time.Duration(hours) * time.Hour)
		got, ok, err := m.loadFinanceUpgradeSnapshot(candidate, now)
		if err != nil || !ok || !bytes.Equal(got, payload) {
			t.Fatalf("hours=%d: ok=%t err=%v", hours, ok, err)
		}
		if _, _, _, ok, err := m.loadFinanceReportSnapshot(candidate, now); err != nil || ok {
			t.Fatalf("old projection became a current snapshot: ok=%t err=%v", ok, err)
		}
		if _, _, ok := m.financeFastSnapshotPayload(candidate, now); ok {
			t.Fatal("old snapshot promoted into current in-memory key")
		}
	}
}

func TestFinanceUpgradeSnapshotRetainsDeployedProjectionFallback(t *testing.T) {
	m, request, now, payload := newFinanceUpgradeSnapshotFixture(t)
	// A distinct range has only the already-supported deployed projection.
	request.from = time.Unix(0, 0)
	payload = bytes.Replace(payload, []byte(`"from":3600`), []byte(`"from":0`), 1)
	key := financeUpgradeProjectionKey(request, "accounting-delivery-compat-v1")
	if err := m.persistFinanceReportSnapshotShadow(key, "legacy", payload, now); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.loadFinanceUpgradeSnapshot(request, now)
	if err != nil || !ok || !bytes.Equal(got, payload) {
		t.Fatalf("deployed fallback: ok=%t err=%v", ok, err)
	}
	if _, _, _, ok, err := m.loadFinanceReportSnapshot(request, now); err != nil || ok {
		t.Fatal("legacy promoted to current", err)
	}
}

func TestFinanceUpgradeSnapshotIsolationAndExpiry(t *testing.T) {
	for _, scenario := range []string{"config", "start", "earlier-end", "too-distant", "expired", "missing-config", "local-snapshot", "clamped", "disabled", "read-disabled", "future-generated", "bounds", "checksum", "unsupported-projection"} {
		t.Run(scenario, func(t *testing.T) {
			m, request, now, _ := newFinanceUpgradeSnapshotFixture(t)
			key := financePreviousProjectionKey(request)
			switch scenario {
			case "config":
				request.configurationHash = "changed"
			case "start":
				request.from = time.Unix(0, 0)
			case "earlier-end":
				request.to = request.to.Add(-time.Hour)
			case "too-distant":
				request.to = request.to.Add(49 * time.Hour)
			case "expired":
				now = now.Add(financeReportPersistentStale)
			case "missing-config":
				request.configurationHash = ""
			case "local-snapshot":
				request.snapshotAsOf = 10800
			case "clamped":
				request.snapshotClamped = true
			case "disabled":
				m.cfg.FinanceFastSnapshotEnabled = false
			case "read-disabled":
				m.cfg.FinanceReportSnapshotReadEnabled = false
			case "future-generated", "bounds":
				generated, to := now.Unix(), request.to.Unix()
				if scenario == "future-generated" {
					generated += 120
				} else {
					to += 1
				}
				bad := []byte(fmt.Sprintf(`{"from":3600,"to":%d,"generated_at":%d}`, to, generated))
				if err := m.persistFinanceReportSnapshotShadow(key, "old", bad, now); err != nil {
					t.Fatal(err)
				}
			case "checksum":
				path, _ := financeReportSnapshotPath(m.cfg.StorePath, key)
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				raw = bytes.Replace(raw, []byte("1234567"), []byte("9999999"), 1)
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "unsupported-projection":
				path, _ := financeReportSnapshotPath(m.cfg.StorePath, key)
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				raw = bytes.Replace(raw, []byte("accounting-amount-evidence-v2"), []byte("unsupported-accounting-v0"), 1)
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok, _ := m.loadFinanceUpgradeSnapshot(request, now); ok {
				t.Fatal("ineligible legacy money was displayed")
			}
		})
	}
}

func TestFinanceUpgradeSnapshotHTTPKeepsFailedUpdateVisibleAndPrefersCurrent(t *testing.T) {
	m, request, now, payload := newFinanceUpgradeSnapshotFixture(t)
	// A finished failure in backoff prevents actual computations in this test.
	m.financeAsyncQueue.jobs = map[string]*financeReportJob{request.logicalKey(): {
		key: request.logicalKey(), state: "failed", finished: now, unresolvedFailure: true,
	}}
	serve := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/finance/report", nil)
		m.serveFinanceQueuedReport(c, request)
		return w
	}
	w := serve()
	if w.Code != http.StatusOK || w.Header().Get("X-Monitor-Finance-Cache") != "persistent-upgrade-prior-stale-queued" ||
		w.Header().Get("X-Monitor-Finance-Update") != "failed" || !bytes.Equal(w.Body.Bytes(), payload) {
		t.Fatalf("failed refresh lost dated fallback/status: code=%d headers=%v", w.Code, w.Header())
	}
	current := bytes.Replace(payload, []byte("1234567"), []byte("2345678"), 1)
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "new-source", current, now); err != nil {
		t.Fatal(err)
	}
	w = serve()
	if !bytes.Equal(w.Body.Bytes(), current) || w.Header().Get("X-Monitor-Finance-Cache") != "fast-snapshot-stale" {
		t.Fatal("upgrade fallback shadowed current projection")
	}
	if !m.financeAsyncQueue.shutdown(context.Background()) {
		t.Fatal("queue did not shut down")
	}
}
