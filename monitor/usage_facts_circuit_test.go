package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestUsageFactBulkCircuitTimeoutBackoffAndHalfOpenRecovery(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	state := UsageFactSyncState{}
	timeout := &mysql.MySQLError{Number: 3024, Message: "maximum execution time exceeded"}
	applyUsageFactBulkFailure(&state, timeout, now)
	if state.HistoryBulkCircuitState != usageFactBulkCircuitOpen || state.HistoryBulkOpenedUntil != now.Add(15*time.Minute).Unix() {
		t.Fatalf("first timeout state=%+v", state)
	}
	applyUsageFactBulkFailure(&state, context.DeadlineExceeded, now.Add(time.Minute))
	applyUsageFactBulkFailure(&state, timeout, now.Add(2*time.Minute))
	if state.HistoryBulkOpenedUntil != now.Add(62*time.Minute).Unix() || state.HistoryBulkFailureStreak != 3 {
		t.Fatalf("third timeout must open one hour: %+v", state)
	}

	state.HistoryBulkCircuitState = usageFactBulkCircuitHalfOpen
	state.HistoryBulkOpenedUntil = 0
	fast := usageFactHistoryRange{SourceQueries: 2, QueryDuration: time.Second}
	for i := 0; i < 2; i++ {
		applyUsageFactBulkSuccess(&state, fast, now.Add(time.Duration(63+i)*time.Minute))
		if state.HistoryBulkCircuitState != usageFactBulkCircuitHalfOpen {
			t.Fatalf("half-open closed before three probes: %+v", state)
		}
	}
	applyUsageFactBulkSuccess(&state, fast, now.Add(65*time.Minute))
	if state.HistoryBulkCircuitState != usageFactBulkCircuitClosed || state.HistoryBulkHalfOpenSuccesses != 0 {
		t.Fatalf("three healthy probes did not close circuit: %+v", state)
	}
}

func TestUsageFactBulkCircuitTripsOnThreeSlowSuccessfulQueries(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	state := UsageFactSyncState{}
	slow := usageFactHistoryRange{SourceQueries: 2, QueryDuration: 5 * time.Second}
	for i := 0; i < 3; i++ {
		applyUsageFactBulkSuccess(&state, slow, now.Add(time.Duration(i)*time.Minute))
	}
	if state.HistoryBulkCircuitState != usageFactBulkCircuitOpen || state.HistoryBulkSlowStreak != 3 ||
		state.HistoryBulkOpenedUntil != now.Add(62*time.Minute).Unix() {
		t.Fatalf("slow circuit state=%+v", state)
	}
}

func TestUsageFactBulkCircuitPersistsAcrossSchedulerTurns(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	now := time.Unix(3_000_000, 0)
	timeout := &mysql.MySQLError{Number: 3024, Message: "maximum execution time exceeded"}
	if err := m.recordUsageFactHistoryBulkFailure(context.Background(), timeout, now); err != nil {
		t.Fatal(err)
	}
	allowed, halfOpen, err := m.usageFactHistoryBulkCircuitAllowed(context.Background(), now.Add(14*time.Minute))
	if err != nil || allowed || halfOpen {
		t.Fatalf("open circuit allowed cold work: allowed=%v half=%v err=%v", allowed, halfOpen, err)
	}
	allowed, halfOpen, err = m.usageFactHistoryBulkCircuitAllowed(context.Background(), now.Add(16*time.Minute))
	if err != nil || !allowed || !halfOpen {
		t.Fatalf("expired circuit did not enter half-open: allowed=%v half=%v err=%v", allowed, halfOpen, err)
	}
	var persisted UsageFactSyncState
	if err := m.usageFactsStore().First(&persisted, 1).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.HistoryBulkCircuitState != usageFactBulkCircuitHalfOpen || persisted.HistoryBulkFailureStreak != 1 {
		t.Fatalf("circuit state not durable: %+v", persisted)
	}
}
