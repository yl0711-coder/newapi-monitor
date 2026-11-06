package monthlysource

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestTokenUsageDiagnosticsSeparateTimingFromMoneyMismatch(t *testing.T) {
	db, d, req := tokenUsageSource(t)
	d.usage.data[0] = usageTestRow(2, "wallet", "1500001", 1500001)
	o, err := CaptureTokenUsageWindow(context.Background(), db, req)
	if err == nil || TokenUsageFailureStage(err) != "validation" || TokenUsageFailureReason(err) != "group_totals_mismatch" || !reflect.DeepEqual(o, TokenUsageWindowObservation{}) || d.usage.s.committed {
		t.Fatal("financial mismatch hidden or returned as partial source data", TokenUsageFailureReason(err), err)
	}
	if TokenUsageFailureReason(errors.New("group_totals_mismatch private fake error")) != "" {
		t.Fatal("untrusted error text became a trusted diagnostic")
	}
}

func TestTokenUsageDiagnosticsTimingStillRejectsBeforeAndAfterReadOnlyCommit(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		db, d, req := tokenUsageSource(t)
		start := time.Now()
		calls := 0
		clock := func() time.Time {
			calls++
			if calls == 1 {
				return start
			}
			if afterCommit && calls == 2 {
				return start.Add(time.Millisecond)
			}
			return start.Add(usageWindowTimeout + time.Millisecond)
		}
		o, err := captureTokenUsageWindow(context.Background(), db, req, clock)
		if err == nil || TokenUsageFailureStage(err) != "validation" || TokenUsageFailureReason(err) != "observation_timing" || !reflect.DeepEqual(o, TokenUsageWindowObservation{}) || d.usage.s.committed != afterCommit || db.Stats().InUse != 0 || len(d.usage.s.queries) != 4 {
			t.Fatal("timing rejection widened budget, retried or retained partial data", afterCommit, TokenUsageFailureReason(err), err)
		}
	}
}

func TestTokenUsageDiagnosticCodesAreClosedAndDoNotAlterExistingAcceptance(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{errTokenObservationScope, "scope_or_totals"},
		{errTokenObservationTiming, "observation_timing"},
		{errTokenObservationRow, "token_row"},
		{errTokenObservationEndpoint, "overlapping_log_groups"},
		{errTokenObservationOverflow, "aggregate_overflow"},
		{errTokenObservationMismatch, "group_totals_mismatch"},
		{errTokenObservationByteLimit, "observation_byte_limit"},
	} {
		if TokenUsageFailureReason(errors.Join(errors.New("private unrelated message"), tc.err)) != tc.code {
			t.Fatal("lost fixed diagnostic code", tc.code)
		}
	}
	db, _, req := tokenUsageSource(t)
	o, err := CaptureTokenUsageWindow(context.Background(), db, req)
	if err != nil || ValidateTokenUsageWindowObservation(o) != nil || TokenUsageFailureReason(nil) != "" {
		t.Fatal("diagnostics changed valid capture acceptance", err)
	}
}
