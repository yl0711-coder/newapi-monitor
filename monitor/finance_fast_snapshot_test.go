package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestFinanceFastSnapshotPublishesHistoricalCorrectionAndKeepsPriorOnFailure(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "historical-fast.example")
	defer m.Close()
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	hour := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 59, Name: "historical", BaseDomain: "historical-fast.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 59, ModelName: "test", Grp: "g", Success: 1, Quota: 500_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 1, Quota: 500_000, TrafficClassVersion: stabilityTrafficClassificationVersion, UpdatedAt: hour + 1}).Error; err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	request := financeReportRequest{from: time.Unix(hour, 0).In(loc), to: time.Unix(hour+3600, 0).In(loc)}
	var err error
	request.configurationHash, err = m.financeReportConfigurationHash(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	readAmount := func() string {
		t.Helper()
		payload, _, ok := m.financeFastSnapshotPayload(request, time.Now())
		if !ok {
			t.Fatal("verified payload missing")
		}
		var report financeOperatingReport
		if err := json.Unmarshal(payload, &report); err != nil {
			t.Fatal(err)
		}
		return report.Statement.KnownUserConsumption.MicroUSD
	}
	if err := m.refreshFinanceFastSnapshot(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := readAmount(); got != "1000000" {
		t.Fatalf("initial amount=%s", got)
	}
	// Correct a closed historical hour and publish its new source version.
	if err := m.storeDB.Model(&StabilityHourSample{}).Where("hour_ts = ?", hour).Update("quota", 1_000_000).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&StabilityHourIngestState{}).Where("hour_ts = ?", hour).Updates(map[string]any{"quota": 1_000_000, "updated_at": hour + 2}).Error; err != nil {
		t.Fatal(err)
	}
	if got := readAmount(); got != "1000000" {
		t.Fatal("display changed before background publication")
	}
	if err := m.refreshFinanceFastSnapshot(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := readAmount(); got != "2000000" {
		t.Fatalf("historical correction not published: %s", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.refreshFinanceFastSnapshot(ctx, request); err == nil {
		t.Fatal("canceled verification reported success")
	}
	if got := readAmount(); got != "2000000" {
		t.Fatal("failed verification replaced valid result")
	}
}

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

func TestFinanceFastSnapshotManualAndColdReadsUseQueue(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "fast-handler.example")
	defer m.Close()
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	started := make(chan struct{})
	m.financeAsyncQueue.submit(context.Background(), "blocker", false, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started
	configHash, err := m.financeReportConfigurationHash(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, loc)
	to := from.AddDate(0, 0, 1)
	request := financeReportRequest{from: from, to: to, configurationHash: configHash}
	now := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
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
	w = serve("/finance/report?from=2026-05-01&to=2026-05-01&fresh=1")
	if w.Code != http.StatusOK || w.Body.String() != string(payload) || m.financeAsyncQueue.stats().Pending != 1 {
		t.Fatalf("manual refresh failed to preserve old result/deduplicate: code=%d stats=%+v", w.Code, m.financeAsyncQueue.stats())
	}
	w = serve("/finance/report?from=2026-05-02&to=2026-05-02")
	if w.Code != http.StatusAccepted || m.financeAsyncQueue.stats().Pending != 2 {
		t.Fatalf("cold range should queue without blocking: code=%d stats=%+v", w.Code, m.financeAsyncQueue.stats())
	}
}

func TestFinanceFastSnapshotUnchangedVersionRenewsWithoutRebuilding(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "unchanged-fast.example")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	from := time.Unix(3600, 0)
	request := financeReportRequest{from: from, to: from.Add(time.Hour), configurationHash: "config-v1"}
	request.configurationHash, _ = m.financeReportConfigurationHash(context.Background())
	fingerprint, err := m.financeReportSourceFingerprint(context.Background(), from.Unix(), request.to.Unix())
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`, from.Unix(), request.to.Unix(), old.Unix()))
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), fingerprint, payload, old); err != nil {
		t.Fatal(err)
	}
	if err := m.refreshFinanceFastSnapshot(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.sourceFingerprint = fingerprint
	got, verifiedAt, state, ok, err := m.loadFinanceReportSnapshot(request, time.Now())
	if err != nil || !ok || state != "fresh" || string(got) != string(payload) {
		t.Fatalf("unchanged version triggered rebuild: ok=%t state=%q err=%v", ok, state, err)
	}
	if !verifiedAt.Equal(old) {
		t.Fatalf("unchanged source rewrote snapshot file: before=%s after=%s", old, verifiedAt)
	}
	if cached, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now()); !ok || string(cached) != string(payload) {
		t.Fatal("verified snapshot was not warmed into memory")
	}
}
