package monitor

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestFinanceUpgradeHourDiagnosticsPreservesV7MoneyWithoutPromotion(t *testing.T) {
	m, request, now, _ := newFinanceUpgradeSnapshotFixture(t)
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":3600,"to":10800,"generated_at":%d,"statement":{"contribution_profit":{"micro_usd":"42"},"contribution_margin_percent":"1.2"},"cost_details":[{"provider":"tokenforce"}],"pairing_audit":{"paired_rows":3}}`, now.Unix()))
	key := financeUpgradeProjectionKey(request, "accounting-recharge-zero-proof-v7")
	if err := m.persistFinanceReportSnapshotShadow(key, "v7-source", payload, now); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.loadFinanceUpgradeSnapshot(request, now)
	if err != nil || !ok || !bytes.Equal(got, payload) {
		t.Fatalf("display-only diagnostic upgrade changed amounts: ok=%t err=%v", ok, err)
	}
	if _, _, _, ok, err := m.loadFinanceReportSnapshot(request, now); err != nil || ok {
		t.Fatal("v7 without hour diagnosis promoted to current v8 report", err)
	}
}

func TestFinanceUpgradeSnapshotV3PreservesKnownMoneyButNotOldExactContribution(t *testing.T) {
	m, request, now, _ := newFinanceUpgradeSnapshotFixture(t)
	statement := `{"known_user_consumption":{"micro_usd":"24000000"},"known_contribution_profit":{"micro_usd":"8000000"},"contribution_profit":{"micro_usd":"8000000"},"contribution_margin_percent":"66.67","paired_contribution_margin_percent":"66.67"}`
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":3600,"to":10800,"generated_at":%d,"unknown_extension":9007199254740993,"statement":%s,"periods":[{"period":"2026-05","statement":%s}],"days":[{"date":"2026-05-01","statement":%s}]}`, now.Unix(), statement, statement, statement))
	key := financeUpgradeProjectionKey(request, "accounting-partial-correction-v3")
	if err := m.persistFinanceReportSnapshotShadow(key, "old-source", payload, now); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.loadFinanceUpgradeSnapshot(request, now)
	if err != nil || !ok {
		t.Fatalf("v3 known amounts lost during upgrade: %v", err)
	}
	var report financeOperatingReport
	if err := json.Unmarshal(got, &report); err != nil {
		t.Fatal(err)
	}
	for _, s := range []financeStatementView{report.Statement, report.Periods[0].Statement, report.Days[0].Statement} {
		if s.ContributionProfit != nil || s.ContributionMargin != nil || s.KnownContributionProfit.MicroUSD != "8000000" || s.KnownUserConsumption.MicroUSD != "24000000" || s.PairedContributionMargin == nil {
			t.Fatalf("old exact claim reused or known amounts removed: %+v", s)
		}
	}
	if !bytes.Contains(got, []byte(`"unknown_extension":9007199254740993`)) {
		t.Fatal("upgrade lost an extension or its integer precision")
	}
	if _, _, _, ok, err := m.loadFinanceReportSnapshot(request, now); err != nil || ok {
		t.Fatal("sanitized prior snapshot became current proof", err)
	}
	stored, _, _, ok, err := m.loadFinanceReportSnapshotKey(request, key, now)
	if err != nil || !ok || !bytes.Equal(stored, payload) {
		t.Fatal("upgrade mutated the stored historical snapshot", err)
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
