//go:build unix

package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type giftReportResponse struct {
	Cache  string                 `json:"cache"`
	Report financeOperatingReport `json:"report"`
}

func giftReportLocalFixture(t *testing.T) (*Monitor, []financeGiftLocalEvidence, []financeGiftScopeTarget, int64) {
	t.Helper()
	backup, paths, hour := giftMixedLocalFixture(t)
	dir := filepath.Join(t.TempDir(), "job")
	plan, _, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, paths)
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeDB)
	m := newFinanceReportTestMonitor(t, "report-gift.example")
	m.usageFactsDB = db
	m.cfg.LocalSnapshotOnly = true
	if err = m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "test", Included: false}).Error; err != nil {
		t.Fatal(err)
	}
	if err = m.storeDB.Create(&ChannelSnap{ID: 1, BaseDomain: "report-gift.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	type key struct {
		hour  int64
		group string
	}
	samples := map[key]StabilityHourSample{}
	var evidence []financeGiftLocalEvidence
	for _, path := range paths {
		var e financeGiftLocalEvidence
		if _, err = giftLocalReadJSON(path, &e); err != nil {
			t.Fatal(err)
		}
		evidence = append(evidence, e)
		for _, row := range e.Rows {
			k := key{e.Hour, *row.Group}
			s := samples[k]
			s.HourTs = e.Hour
			s.ChannelID = 1
			s.ModelName = "fixture"
			s.Grp = *row.Group
			s.TrafficClassVersion = stabilityTrafficClassificationVersion
			if row.Type == 2 {
				s.Success++
				s.Quota += *row.Quota
			} else {
				s.RefundRecords++
				s.RefundQuota += *row.Quota
			}
			samples[k] = s
		}
	}
	for _, s := range samples {
		if err = m.storeDB.Create(&s).Error; err != nil {
			t.Fatal(err)
		}
	}
	for h := hour; h < hour+7200; h += 3600 {
		if err = m.storeDB.Create(&StabilityHourIngestState{HourTs: h, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	return m, evidence, plan.Targets, hour
}

func giftReadReportHTTP(t *testing.T, m *Monitor) giftReportResponse {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/finance/report?from=2026-05-01&to=2026-05-01", nil)
	m.serveFinanceOperatingReport(c)
	if w.Code != http.StatusOK {
		t.Fatalf("report HTTP %d: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("finance response cache privacy missing")
	}
	response := giftReportResponse{Cache: w.Header().Get("X-Monitor-Finance-Cache")}
	if err := json.Unmarshal(w.Body.Bytes(), &response.Report); err != nil {
		t.Fatal(err)
	}
	return response
}

func giftWaitReportRefresh(t *testing.T, m *Monitor, unknown int64) giftReportResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		response := giftReadReportHTTP(t, m)
		if response.Cache == "hit" && response.Report.GiftCoverage.ScopeUnknownEvents == unknown {
			return response
		}
		if time.Now().After(deadline) {
			t.Fatalf("background refresh did not settle: %+v", response.Report.GiftCoverage)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func giftAssertReportAmounts(t *testing.T, r financeOperatingReport, unknown int64) {
	t.Helper()
	if !r.UserCoverage.Complete || r.GiftCoverage.ScopeUnknownEvents != unknown || r.GiftCoverage.Complete != (unknown == 0) {
		t.Fatalf("wrong coverage: user=%+v gift=%+v", r.UserCoverage, r.GiftCoverage)
	}
	statements := []financeStatementView{r.Statement}
	if len(r.Periods) != 1 || len(r.Days) != 1 || len(r.CostDetails) != 1 {
		t.Fatalf("missing details: periods=%d days=%d costs=%d", len(r.Periods), len(r.Days), len(r.CostDetails))
	}
	statements = append(statements, r.Periods[0].Statement, r.Days[0].Statement)
	for _, s := range statements {
		if s.UserConsumption == nil || s.UserConsumption.MicroUSD != "165000000" || s.GrossUserConsumption.MicroUSD != "180000000" || s.UserRefunds.MicroUSD != "15000000" {
			t.Fatalf("usage changed: %+v", s)
		}
		if unknown > 0 {
			if s.RegistrationGiftConsumption != nil || s.OperatingRevenue != nil {
				t.Fatal("incomplete gift scope published revenue")
			}
		} else if s.RegistrationGiftConsumption == nil || s.RegistrationGiftConsumption.MicroUSD != "95000000" || s.OperatingRevenue == nil || s.OperatingRevenue.MicroUSD != "70000000" {
			t.Fatalf("wrong gift/revenue: %+v", s)
		}
		if s.OperatingProfit != nil || s.RawCorrectedUpstreamCost != nil {
			t.Fatal("missing upstream/AWS evidence treated as zero cost")
		}
	}
	if r.CostDetails[0].UserConsumption.MicroUSD != "165000000" {
		t.Fatal("supplier user consumption disagrees with total")
	}
}

func giftPeriodCacheSnapshot(m *Monitor) map[string]string {
	cache := m.getFinancePeriodCache()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	result := map[string]string{}
	for k, v := range cache.items {
		result[k] = string(v.Value.(*boundedByteEntry).value)
	}
	return result
}

func TestFinanceGiftScopeReportHTTPRefreshAfterRepair(t *testing.T) {
	m, evidence, targets, hour := giftReportLocalFixture(t)
	ctx := context.Background()
	source, err := giftLocalSource(ctx, evidence)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	moneyBefore := giftMixedMonetarySnapshot(t, m.usageFactsStore())
	before := giftReadReportHTTP(t, m)
	if before.Cache != "miss" {
		t.Fatal("expected initial miss", before.Cache)
	}
	giftAssertReportAmounts(t, before.Report, 8)
	hit := giftReadReportHTTP(t, m)
	if hit.Cache != "hit" || !reflect.DeepEqual(before.Report, hit.Report) {
		t.Fatal("repeat did not reuse report")
	}
	baseBefore := giftPeriodCacheSnapshot(m)
	if len(baseBefore) == 0 {
		t.Fatal("monthly base cache not populated")
	}
	responses := map[string]giftReportResponse{"before": before}
	previous := before.Report
	for _, stage := range []struct {
		name      string
		targets   []financeGiftScopeTarget
		remaining int64
	}{
		{"partial", targets[:1], 5}, {"complete", targets[1:], 0},
	} {
		for _, target := range stage.targets {
			if _, err = repairFinanceGiftBoundaryScope(ctx, m.usageFactsStore(), source, target.SourceEpoch, target.HourTs, target.UserID, time.Now().Unix()); err != nil {
				t.Fatal(err)
			}
		}
		stale := giftReadReportHTTP(t, m)
		if stale.Cache != "stale-refreshing" {
			t.Fatal("old report not marked stale", stale.Cache)
		}
		if !reflect.DeepEqual(stale.Report, previous) {
			t.Fatal("refresh replaced the previous report before new evidence was ready")
		}
		responses[stage.name+"_stale"] = stale
		current := giftWaitReportRefresh(t, m, stage.remaining)
		giftAssertReportAmounts(t, current.Report, stage.remaining)
		responses[stage.name] = current
		previous = current.Report
		if !reflect.DeepEqual(baseBefore, giftPeriodCacheSnapshot(m)) {
			t.Fatal("scope-only repair rebuilt or mutated base month cache")
		}
	}
	if giftMixedMonetarySnapshot(t, m.usageFactsStore()) != moneyBefore {
		t.Fatal("reporting or repair changed monetary facts")
	}
	// Simulate restart with the verified full-report disk snapshot, no L1 cache.
	configuration, err := m.financeReportConfigurationHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := m.financeReportSourceFingerprint(ctx, hour, hour+7200)
	if err != nil {
		t.Fatal(err)
	}
	request := financeReportRequest{from: time.Unix(hour, 0), to: time.Unix(hour+7200, 0), snapshotAsOf: hour + 7200, snapshotClamped: true, configurationHash: configuration, sourceFingerprint: fingerprint}
	payload, err := json.Marshal(responses["complete"].Report)
	if err != nil {
		t.Fatal(err)
	}
	writer := &Monitor{cfg: m.cfg}
	writer.cfg.FinanceReportSnapshotShadowEnabled = true
	if err = writer.persistFinanceReportSnapshotShadow(request.logicalKey(), fingerprint, payload, time.Now()); err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, usageFactsDB: m.usageFactsDB}
	restarted.cfg.FinanceReportSnapshotShadowEnabled = false
	restarted.cfg.FinanceReportSnapshotReadEnabled = true
	disk := giftReadReportHTTP(t, restarted)
	if disk.Cache != "persistent-hit" {
		t.Fatal("restart did not reuse verified snapshot", disk.Cache)
	}
	diskPayload, _ := json.Marshal(disk.Report)
	if !bytes.Equal(payload, diskPayload) {
		t.Fatal("snapshot altered financial values")
	}
	responses["restarted"] = disk
	if path := os.Getenv("MONITOR_GIFT_REPORT_ACCEPTANCE_OUTPUT"); path != "" {
		data, err := json.MarshalIndent(responses, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = giftLocalWriteNew(path, data); err != nil {
			t.Fatal(err)
		}
	}
}
