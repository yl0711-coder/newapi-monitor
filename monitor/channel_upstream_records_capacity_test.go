package monitor

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAICodeWithRecordDiskGatePreservesCheckpointAndBillsThenResumes(t *testing.T) {
	m := newStabilityTestMonitor(t)
	fixture := &recordTestServer{}
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	defer server.Close()
	m.upstreamClient = server.Client()
	day := recordTestDay()
	row := ChannelUpstreamAccount{Domain: "spring.test", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1}
	round := AICodeWithUsageRound{Domain: row.Domain, RoundID: "r", WindowFrom: day, WindowTo: day + 86400, RecordMode: true}
	cp := AICodeWithRecordCheckpoint{Domain: row.Domain, RoundID: round.RoundID, SlotID: "slot", Unit: 1}
	if err := m.storeDB.Create(&cp).Error; err != nil {
		t.Fatal(err)
	}
	bill := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: day, BucketSeconds: 86400, CostUSD: 10, UnitPerUSD: 1}
	if err := m.storeDB.Create(&bill).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.UsageFactsHistoryMinFreeBytes = math.MaxInt64 / 4 // Force a safe denial, without filling a disk.
	_, ready, err := m.fetchAICodeWithRecordWindow(context.Background(), row, "one", round, "slot", day+86400, intBudget(4))
	if ready || !errors.Is(err, errAICodeWithRecordCapacity) || len(fixture.calls) != 0 {
		t.Fatalf("gate failed before network: ready=%v err=%v calls=%v", ready, err, fixture.calls)
	}
	before := cp
	if err := m.commitAICodeWithRecordPage(context.Background(), &cp, round, aiCodeWithRecordPage{KeyID: 17}, day+86400); !errors.Is(err, errAICodeWithRecordCapacity) {
		t.Fatalf("write gate failed: %v", err)
	}
	var persisted AICodeWithRecordCheckpoint
	if err := m.storeDB.First(&persisted, "domain = ? AND round_id = ? AND slot_id = ?", row.Domain, round.RoundID, "slot").Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cp, before) || !reflect.DeepEqual(persisted, before) {
		t.Fatal("disk denial mutated the checkpoint")
	}
	var storedBill ChannelUpstreamUsageHour
	if err := m.storeDB.First(&storedBill, "domain = ? AND hour_ts = ?", row.Domain, day).Error; err != nil || storedBill.CostUSD != 10 {
		t.Fatalf("published bill changed: bill=%+v err=%v", storedBill, err)
	}
	m.cfg.UsageFactsHistoryMinFreeBytes = 0
	_, ready, err = m.fetchAICodeWithRecordWindow(context.Background(), row, "one", round, "slot", day+86400, intBudget(4))
	if err != nil || !ready {
		t.Fatalf("could not resume after capacity recovered: ready=%v err=%v", ready, err)
	}
}

func TestAICodeWithRecordDiskGateUsesMainVolumeAndFailsClosed(t *testing.T) {
	m := newStabilityTestMonitor(t)
	// An inaccessible facts path must not block a healthy MAIN volume.
	m.cfg.UsageFactsStorePath = filepath.Join(t.TempDir(), "missing", "facts.db")
	if err := m.ensureAICodeWithRecordCapacity(); err != nil {
		t.Fatal(err)
	}
	m.cfg.StorePath = filepath.Join(t.TempDir(), "missing", "monitor.db")
	if err := m.ensureAICodeWithRecordCapacity(); !errors.Is(err, errAICodeWithRecordCapacity) {
		t.Fatalf("unreadable main volume was accepted: %v", err)
	}
	m.cfg.StorePath = ""
	if err := m.ensureAICodeWithRecordCapacity(); !errors.Is(err, errAICodeWithRecordCapacity) {
		t.Fatalf("empty main path was accepted: %v", err)
	}
}
