package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFinancePeriodCacheReusesImmutableCopyAndInvalidatesOnPublishedChange(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "period-cache.example")
	scope := stabilityScope{FromTs: 1_777_516_800, ToTs: 1_777_520_400}
	internalAccounts := financeConfiguredInternalEvidence{Complete: true}

	first, hit, err := m.buildFinancePeriodComponent(
		context.Background(), scope, scope.ToTs, "config-v1", nil, channelFinanceSnapshot{},
		financeInternalTestCostEvidence{}, internalAccounts, map[string]bool{},
	)
	if err != nil || hit {
		t.Fatalf("first monthly component build: hit=%v err=%v", hit, err)
	}
	if len(first.Days) == 0 {
		t.Fatal("monthly component omitted daily views")
	}
	first.Days[0].Date = "mutated-by-caller"

	second, hit, err := m.buildFinancePeriodComponent(
		context.Background(), scope, scope.ToTs, "config-v1", nil, channelFinanceSnapshot{},
		financeInternalTestCostEvidence{}, internalAccounts, map[string]bool{},
	)
	if err != nil || !hit {
		t.Fatalf("second monthly component read: hit=%v err=%v", hit, err)
	}
	if second.Days[0].Date == "mutated-by-caller" {
		t.Fatal("finance period cache leaked caller mutation")
	}

	state := StabilityHourIngestState{
		HourTs: scope.FromTs, Status: "complete", Rows: 1, Requests: 1, Tokens: 1, Quota: 1,
		TrafficClassVersion: stabilityTrafficClassificationVersion,
		UpdatedAt:           scope.FromTs + 10,
		CompletedAt:         scope.FromTs + 10,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	_, hit, err = m.buildFinancePeriodComponent(
		context.Background(), scope, scope.ToTs, "config-v1", nil, channelFinanceSnapshot{},
		financeInternalTestCostEvidence{}, internalAccounts, map[string]bool{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("changed in-range publication reused stale monthly component")
	}
}

func TestFinancePeriodSnapshotExpiresEvenWithMatchingFingerprint(t *testing.T) {
	m := &Monitor{cfg: Settings{StorePath: filepath.Join(t.TempDir(), "monitor.db"), FinanceReportSnapshotReadEnabled: true, FinanceReportSnapshotShadowEnabled: true}}
	payload, _ := json.Marshal(financePeriodComponent{})
	if err := m.persistFinancePeriodSnapshot("month", "source", payload); err != nil {
		t.Fatal(err)
	}
	path, err := financePeriodSnapshotPath(m.cfg.StorePath, "month")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var envelope financeReportSnapshotEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.StoredAt = time.Now().Add(-financePeriodCacheTTL - time.Hour).Unix()
	raw, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.loadFinancePeriodSnapshot("month", "source"); err != nil || ok {
		t.Fatalf("expired monthly snapshot reused: %v %v", ok, err)
	}
}

func TestFinancePeriodSnapshotPersistsExactVersionAndReplacesOldFingerprint(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	scope := stabilityScope{FromTs: 1_777_516_800, ToTs: 1_780_195_200}
	logicalKey := financePeriodLogicalKey(scope, "config-v1")
	payloadV1, _ := json.Marshal(financePeriodComponent{Days: []financeDailyView{{Date: "2026-05-01"}}})
	if err := m.persistFinancePeriodSnapshot(logicalKey, "source-v1", payloadV1); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := m.loadFinancePeriodSnapshot(logicalKey, "source-v1"); err != nil || !ok || string(got) != string(payloadV1) {
		t.Fatalf("persistent monthly cache roundtrip: ok=%v payload=%s err=%v", ok, got, err)
	}
	if _, ok, err := m.loadFinancePeriodSnapshot(logicalKey, "source-v2"); err != nil || ok {
		t.Fatalf("mismatched monthly source version was accepted: ok=%v err=%v", ok, err)
	}

	payloadV2, _ := json.Marshal(financePeriodComponent{Days: []financeDailyView{{Date: "2026-05-02"}}})
	if err := m.persistFinancePeriodSnapshot(logicalKey, "source-v2", payloadV2); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.loadFinancePeriodSnapshot(logicalKey, "source-v2")
	if err != nil || !ok || string(got) != string(payloadV2) {
		t.Fatalf("new monthly source version did not replace old payload: ok=%v payload=%s err=%v", ok, got, err)
	}
	cacheDir := filepath.Join(dir, financePeriodSnapshotDirName)
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("one logical month created %d persistent files", len(entries))
	}
}
