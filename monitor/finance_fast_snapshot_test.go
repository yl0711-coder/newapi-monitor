package monitor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestFinanceFastSnapshotRequiresExistingPersistenceGates(t *testing.T) {
	base := Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", FinanceFastSnapshotEnabled: true}
	if err := validateFinanceSettings(base); err == nil {
		t.Fatal("fast snapshot accepted without persistence gates")
	}
	base.FinanceReportSnapshotShadowEnabled = true
	base.FinanceReportSnapshotReadEnabled = true
	if err := validateFinanceSettings(base); err != nil {
		t.Fatal(err)
	}
	base.FinanceEnabled = false
	if err := validateFinanceSettings(base); err == nil {
		t.Fatal("fast snapshot accepted without finance page")
	}
}

func TestFinanceFastSnapshotIsBoundedByRangeConfigurationAndAge(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "fast-cache.example")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0), configurationHash: "config-v1"}
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`, request.from.Unix(), request.to.Unix(), now.Unix()))
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "source-v1", payload, now); err != nil {
		t.Fatal(err)
	}
	got, status, ok := m.financeFastSnapshotPayload(request, now)
	if !ok || status != "fast-snapshot-stale" || string(got) != string(payload) {
		t.Fatalf("fast snapshot mismatch: ok=%t status=%q payload=%s", ok, status, got)
	}
	changedConfig := request
	changedConfig.configurationHash = "config-v2"
	if _, _, ok := m.financeFastSnapshotPayload(changedConfig, now); ok {
		t.Fatal("changed accounting configuration reused old amount")
	}
	changedRange := request
	changedRange.to = changedRange.to.Add(time.Hour)
	if _, _, ok := m.financeFastSnapshotPayload(changedRange, now); ok {
		t.Fatal("changed range reused old amount")
	}
	if _, _, ok := m.financeFastSnapshotPayload(request, now.Add(financeFastSnapshotMaxAge+time.Second)); ok {
		t.Fatal("expired fast snapshot suppressed source verification")
	}
}

func TestFinanceFastSnapshotRejectsMismatchedPayloadBounds(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "bad-fast-cache.example")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0), configurationHash: "config-v1"}
	for _, payload := range [][]byte{
		[]byte(fmt.Sprintf(`{"from":%d,"to":%d,"generated_at":%d}`, request.from.Unix(), request.to.Unix()+3600, now.Unix())),
		[]byte(fmt.Sprintf(`{"from":%d,"to":%d,"generated_at":%d}`, request.from.Unix(), request.to.Unix(), now.Add(2*time.Minute).Unix())),
	} {
		if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "source-v1", payload, now); err != nil {
			t.Fatal(err)
		}
		if _, _, ok := m.financeFastSnapshotPayload(request, now); ok {
			t.Fatalf("invalid snapshot bounds were displayed: %s", payload)
		}
	}
}

func TestFinanceFastSnapshotSkipsBuildButManualRefreshDoesNot(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "fast-handler.example")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	configHash, err := m.financeReportConfigurationHash(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, loc)
	to := from.AddDate(0, 0, 1)
	request := financeReportRequest{from: from, to: to, configurationHash: configHash}
	now := time.Now().Truncate(time.Second)
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`, from.Unix(), to.Unix(), now.Unix()))
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "source-v1", payload, now); err != nil {
		t.Fatal(err)
	}
	serve := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, path, nil)
		m.serveFinanceOperatingReport(c)
		return w
	}
	w := serve("/finance/report?from=2026-05-01&to=2026-05-01")
	if w.Code != http.StatusOK || w.Header().Get("X-Monitor-Finance-Cache") != "fast-snapshot-stale" || w.Body.String() != string(payload) {
		t.Fatalf("fast handler result: code=%d cache=%q body=%s", w.Code, w.Header().Get("X-Monitor-Finance-Cache"), w.Body.String())
	}
	// Disable optional snapshot writes for the forced-build half of the test so
	// no detached file writer outlives the temporary test directory.
	m.cfg.FinanceReportSnapshotShadowEnabled = false
	w = serve("/finance/report?from=2026-05-01&to=2026-05-01&fresh=1")
	if w.Code != http.StatusOK || w.Header().Get("X-Monitor-Finance-Cache") != "refresh" || w.Body.String() == string(payload) {
		t.Fatalf("forced refresh reused stored payload: code=%d cache=%q", w.Code, w.Header().Get("X-Monitor-Finance-Cache"))
	}
}

func TestFinanceFastSnapshotRefreshIsGloballyPaced(t *testing.T) {
	m := &Monitor{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // do not launch a rebuild in this scheduler-only test
	m.backgroundCtx = ctx
	now := time.Now()
	m.scheduleFinanceFastSnapshotRefresh(financeReportRequest{}, now)
	first := m.financeFastRefreshAfter.Load()
	if first != now.Add(financeFastSnapshotRefreshAfter).UnixNano() {
		t.Fatal("initial refresh deadline was not recorded")
	}
	m.scheduleFinanceFastSnapshotRefresh(financeReportRequest{}, now.Add(time.Second))
	if m.financeFastRefreshAfter.Load() != first {
		t.Fatal("concurrent page reads bypassed global refresh pacing")
	}
}

func TestFinanceFastSnapshotUnchangedVersionRenewsWithoutRebuilding(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "unchanged-fast.example")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	from := time.Unix(3600, 0)
	request := financeReportRequest{from: from, to: from.Add(time.Hour), configurationHash: "config-v1"}
	fingerprint, err := m.financeReportSourceFingerprint(context.Background(), from.Unix(), request.to.Unix())
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`, from.Unix(), request.to.Unix(), old.Unix()))
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), fingerprint, payload, old); err != nil {
		t.Fatal(err)
	}
	m.refreshFinanceFastSnapshot(context.Background(), request)
	request.sourceFingerprint = fingerprint
	got, verifiedAt, state, ok, err := m.loadFinanceReportSnapshot(request, time.Now())
	if err != nil || !ok || state != "fresh" || string(got) != string(payload) {
		t.Fatalf("unchanged version triggered rebuild: ok=%t state=%q err=%v", ok, state, err)
	}
	if !verifiedAt.After(old) {
		t.Fatalf("unchanged source did not renew snapshot: before=%s after=%s", old, verifiedAt)
	}
}
