package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestModelRecentProofKeepsGapsAndSharedFinalization(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	ctx := withReportProjectionCache(context.Background())
	old := MetricFinalizeState{ID: 1, CoverageFromTs: 600, NextTs: 6000, SemanticsVersion: stabilityTrafficClassificationVersion, TTFTCoverageFromTs: 600, TTFTCoverageThroughTs: 6000, TTFTSemanticsVersion: ttftCoverageSemanticsVersion}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	write := func(from, to int64) {
		t.Helper()
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error { return publishModelMinuteCoverage(tx, from, to, 12000) }); err != nil {
			t.Fatal(err)
		}
	}
	write(6060, 12000)
	requests, frt, err := m.modelMinuteProof(ctx, 12000)
	if err != nil || requests.covers(600, 12000) || frt.covers(600, 12000) {
		t.Fatal("bridged missing minute", requests, frt, err)
	}
	end, err := m.modelStatisticsAvailableEnd(ctx, 12000, false)
	if err != nil || end != 12000 {
		t.Fatal("tail blocked by historical gap", end, err)
	}
	write(6000, 6060)
	requests, frt, err = m.modelMinuteProof(ctx, 12000)
	if err != nil || !requests.covers(600, 12000) || !frt.covers(600, 12000) {
		t.Fatal("adjacent completed retry did not repair proof", requests, frt, err)
	}
	if complete, _ := m.capacityWindowCoverage(ctx, 600, 12000, false); complete {
		t.Fatal("changed shared Capacity finalization policy")
	}
	var after MetricFinalizeState
	m.storeDB.First(&after, 1)
	if after.NextTs != old.NextTs {
		t.Fatal("advanced historical watermark")
	}
	m.metricFinalizeLastFailure.Store(12)
	m.metricFinalizeLastSuccess.Store(11)
	if m.modelReportCollectionFailed(false) {
		t.Fatal("historical replay failure blocks proven live tail")
	}
	m.sourceLastFailureAt.Store(12)
	if !m.modelReportCollectionFailed(false) {
		t.Fatal("real source failure hidden")
	}
}

func TestModelNormalSamplerPublishesLatestWithoutGovernanceAndSurvivesEnrichmentFailure(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = true
	m.cfg.CustomerHealthSourceEnabled = false
	m.cfg.BackgroundSourceMinStartIntervalMS = -1
	m.prodDB = frtRecomputeProdDB(t)
	now := time.Now().Unix() / 60 * 60
	from := now - 120
	frtRecomputeInsertLog(t, m.prodDB, 1, from+1, 1, 2, 1, 1, "g", "m", `{"frt":1000}`, 1)
	read := func() {
		t.Helper()
		if _, err := m.sampleRange(context.Background(), from, now); err != nil {
			t.Fatal(err)
		}
	}
	read()
	// Restart uses only the durable proof, not process-local atomics.
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	end, err := restarted.modelStatisticsAvailableEnd(context.Background(), now, false)
	if err != nil || end != now {
		t.Fatal("normal sampler proof missing", end, err)
	}
	if ok, _, _, _ := restarted.modelStatisticsFactsCoverage(context.Background(), from, now); !ok {
		t.Fatal("latest request population not covered")
	}
	frt, err := restarted.modelFRTCoverage(context.Background(), from, now)
	if err != nil || !frt.Complete {
		t.Fatal("latest FRT proof missing", frt, err)
	}
	m.cfg.GroupGovernanceEnabled = true
	if err = m.storeDB.Callback().Create().Before("gorm:create").Register("gg-window-fail", func(tx *gorm.DB) {
		if tx.Statement.Table == "group_governance_error_minutes" {
			tx.AddError(errors.New("enrichment failed"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = m.prodDB.Exec("INSERT INTO logs(id,created_at,user_id,channel_id,model_name,`group`,type,content,other,request_id) VALUES(2,?,1,1,'m','g',5,'openai_error','{}','req-2')", from+2); err != nil {
		t.Fatal(err)
	}
	read()
	if ok, _, _, _ := restarted.modelStatisticsFactsCoverage(context.Background(), from, now); !ok {
		t.Fatal("optional governance failure blocked shared requests")
	}
	m.storeDB.Callback().Create().Remove("gg-window-fail")
	read()
	if modelStatisticsWindowTarget(m, time.Unix(now+25, 0)) != now || m.modelReportTiming(false).FinalizeDelaySeconds != 0 {
		t.Fatal("ordinary mode still waits an hour")
	}
}
