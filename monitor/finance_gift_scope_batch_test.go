package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func giftScopeBatchFixture(t *testing.T) (*financeGiftScopeBatchRunner, []financeGiftScopeTarget, *[]time.Duration, *int) {
	t.Helper()
	m, source, hour := giftScopeRepairFixture(t)
	if _, err := source.Exec("INSERT INTO logs(id,user_id,created_at,type,quota,token_id,token_name,request_id,content,other,`group`) VALUES(5,8,?,2,100,1,'','','','','business')", hour+50); err != nil {
		t.Fatal(err)
	}
	calls := 0
	runner := newFinanceGiftScopeBatchRunner(m.usageFactsStore(), giftScopeHookSource{source, func() { calls++ }})
	now := time.Unix(hour+10000, 0)
	runner.now = func() time.Time { return now }
	delays := []time.Duration{}
	runner.wait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if d > 0 {
			delays = append(delays, d)
			now = now.Add(d)
		}
		return nil
	}
	return runner, []financeGiftScopeTarget{{"v1", hour, 7}, {"v1", hour, 8}}, &delays, &calls
}

func TestFinanceGiftScopeBatchLimitsCooldownAndResume(t *testing.T) {
	r, plan, delays, calls := giftScopeBatchFixture(t)
	var journal []financeGiftScopeBatchEntry
	audit := func(e financeGiftScopeBatchEntry) error { journal = append(journal, e); return nil }
	result, err := r.run(context.Background(), plan, audit)
	if err != nil || result.Status != "complete" || result.Remaining != 0 || *calls != 2 {
		t.Fatalf("result=%+v calls=%d err=%v", result, *calls, err)
	}
	if len(journal) != 2 || journal[0].RowsUpdated != 3 || journal[1].RowsUpdated != 1 {
		t.Fatalf("journal=%+v", journal)
	}
	if !reflect.DeepEqual(*delays, []time.Duration{financeGiftScopeBatchInterval}) {
		t.Fatal("missing cooldown", *delays)
	}
	retry, err := r.run(context.Background(), plan, audit)
	if err != nil || retry.Status != "complete" || *calls != 2 {
		t.Fatalf("retry re-read source: %+v %v", retry, err)
	}
	for _, entry := range retry.Entries {
		if entry.Status != "unchanged" || entry.RowsUpdated != 0 {
			t.Fatal("retry changed facts", entry)
		}
	}
	if len(*delays) != 1 {
		t.Fatal("verified replay should not repeat source cooldown", *delays)
	}
}

func TestFinanceGiftScopeBatchResumeWaitsOnlyForPendingSource(t *testing.T) {
	r, plan, delays, calls := giftScopeBatchFixture(t)
	// Align the pending fixture's publication with the injected clock. GORM's
	// automatic UpdatedAt otherwise uses wall time and wins the monotonic guard,
	// masking whether completion was stamped before or after the simulated wait.
	if err := r.db.Model(&FinanceGiftBoundaryState{}).
		Where("source_epoch=? AND hour_ts=? AND user_id=?", plan[1].SourceEpoch, plan[1].HourTs, plan[1].UserID).
		UpdateColumn("updated_at", r.now().Unix()-1).Error; err != nil {
		t.Fatal(err)
	}
	audit := func(financeGiftScopeBatchEntry) error { return nil }
	if _, err := r.run(context.Background(), plan[:1], audit); err != nil {
		t.Fatal(err)
	}
	nextAttempt := r.nextAttempt
	if _, err := r.run(context.Background(), plan[:1], audit); err != nil {
		t.Fatal(err)
	}
	if *calls != 1 || len(*delays) != 0 || !r.nextAttempt.Equal(nextAttempt) {
		t.Fatal("verified replay changed source cooldown", *delays, *calls)
	}
	result, err := r.run(context.Background(), plan, audit)
	if err != nil || result.Status != "complete" || *calls != 2 || !reflect.DeepEqual(*delays, []time.Duration{financeGiftScopeBatchInterval}) {
		t.Fatalf("pending source must still wait exactly one cooldown: %+v %v delays=%v calls=%d", result, err, *delays, *calls)
	}
	proof, err := loadFinanceGiftScopeSnapshot(context.Background(), r.db, plan[1].SourceEpoch, plan[1].HourTs, plan[1].UserID)
	if err != nil || proof.State.CompletedAt != r.now().Unix() {
		t.Fatalf("completion timestamp must be after cooldown: got=%d want=%d err=%v", proof.State.CompletedAt, r.now().Unix(), err)
	}
}

func TestFinanceGiftScopeBatchFailedSourceStillConsumesCooldown(t *testing.T) {
	r, plan, delays, calls := giftScopeBatchFixture(t)
	original := r.source.(giftScopeHookSource)
	// A failed, incomplete source response must not permit a hot retry.
	if _, err := original.db.Exec("DELETE FROM logs WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	audit := func(financeGiftScopeBatchEntry) error { return nil }
	for attempt := 0; attempt < 2; attempt++ {
		result, err := r.run(context.Background(), plan[:1], audit)
		if err == nil || result.Status != "failed" || result.Remaining != 1 {
			t.Fatalf("expected unchanged failed target: %+v %v", result, err)
		}
	}
	if *calls != 2 || !reflect.DeepEqual(*delays, []time.Duration{financeGiftScopeBatchInterval}) {
		t.Fatal("failed source retry bypassed cooldown", *calls, *delays)
	}
}

func TestFinanceGiftScopeBatchVerifiedProofCorruptionIsNotSkipped(t *testing.T) {
	r, plan, _, calls := giftScopeBatchFixture(t)
	audit := func(financeGiftScopeBatchEntry) error { return nil }
	if _, err := r.run(context.Background(), plan[:1], audit); err != nil {
		t.Fatal(err)
	}
	if err := r.db.Model(&FinanceGiftBoundaryEvent{}).Where("user_id=?", plan[0].UserID).Update("quota", 999).Error; err != nil {
		t.Fatal(err)
	}
	r.wait = func(context.Context, time.Duration) error {
		t.Fatal("corrupt local evidence reached source gate")
		return nil
	}
	result, err := r.run(context.Background(), plan[:1], audit)
	if err == nil || result.Status != "failed" || *calls != 1 {
		t.Fatalf("corrupted completed proof skipped: %+v %v", result, err)
	}
}

func TestFinanceGiftScopeBatchPauseAndContinue(t *testing.T) {
	r, plan, _, calls := giftScopeBatchFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	result, err := r.run(ctx, plan, func(financeGiftScopeBatchEntry) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || result.Status != "paused" || result.Remaining != 1 || len(result.Entries) != 1 || *calls != 1 {
		t.Fatalf("pause=%+v %v", result, err)
	}
	retry, err := r.run(context.Background(), plan, func(financeGiftScopeBatchEntry) error { return nil })
	if err != nil || retry.Status != "complete" || *calls != 2 || retry.Entries[0].Status != "unchanged" || retry.Entries[1].Status != "repaired" {
		t.Fatalf("resume=%+v calls=%d %v", retry, *calls, err)
	}
}

func TestFinanceGiftScopeBatchStopsOnErrorOrAuditFailure(t *testing.T) {
	for _, auditFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "source", true: "audit"}[auditFailure], func(t *testing.T) {
			r, plan, _, calls := giftScopeBatchFixture(t)
			if !auditFailure {
				r.source = nil
			}
			var journal []financeGiftScopeBatchEntry
			result, err := r.run(context.Background(), plan, func(e financeGiftScopeBatchEntry) error {
				journal = append(journal, e)
				if auditFailure {
					return errors.New("disk full")
				}
				return nil
			})
			if err == nil || len(journal) != 1 || len(result.Entries) != 1 {
				t.Fatalf("did not stop: %+v %v", result, err)
			}
			other, err := loadFinanceGiftScopeSnapshot(context.Background(), r.db, plan[1].SourceEpoch, plan[1].HourTs, plan[1].UserID)
			if err != nil {
				t.Fatal(err)
			}
			if other.Events[0].GroupKnown {
				t.Fatal("continued after failure")
			}
			if auditFailure {
				if result.Status != "audit_failed" || result.Remaining != 1 || journal[0].Status != "repaired" || *calls != 1 {
					t.Fatal("lost committed outcome", result)
				}
				retry, err := r.run(context.Background(), plan, func(financeGiftScopeBatchEntry) error { return nil })
				if err != nil || retry.Status != "complete" || retry.Entries[0].RowsUpdated != 0 || *calls != 2 {
					t.Fatal("audit failure caused duplicate replay", retry, err)
				}
			} else if result.Status != "failed" || result.Remaining != 2 || journal[0].ErrorCode != "repair_failed" {
				t.Fatal("incorrect failure accounting", result)
			}
		})
	}
}

func TestFinanceGiftScopeBatchRejectsBadPlansBeforeAnyQuery(t *testing.T) {
	r, plan, _, calls := giftScopeBatchFixture(t)
	large := make([]financeGiftScopeTarget, financeGiftScopeBatchLimit+1)
	for i := range large {
		large[i] = financeGiftScopeTarget{"v1", plan[0].HourTs, int64(i + 1)}
	}
	for _, targets := range [][]financeGiftScopeTarget{nil, large, {plan[0], plan[0]}, {{"v2", plan[0].HourTs, 7}, plan[1]}, {{"v1", r.now().Unix() / 3600 * 3600, 7}}, {{"v1", plan[0].HourTs, 0}}} {
		result, err := r.run(context.Background(), targets, func(financeGiftScopeBatchEntry) error { t.Fatal("invalid plan audited as executed"); return nil })
		if err == nil || result.Status != "rejected" || *calls != 0 {
			t.Fatal("invalid plan executed", result, err)
		}
	}
	if _, err := r.run(context.Background(), plan, nil); err == nil {
		t.Fatal("missing audit accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := r.run(ctx, plan, func(financeGiftScopeBatchEntry) error { t.Fatal("paused before start"); return nil })
	if !errors.Is(err, context.Canceled) || result.Status != "paused" || *calls != 0 {
		t.Fatal("pre-cancel ignored", result, err)
	}
}

func TestFinanceGiftScopeBatchRejectsConcurrentRun(t *testing.T) {
	r, plan, _, calls := giftScopeBatchFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	r.wait = func(ctx context.Context, d time.Duration) error { close(entered); <-release; return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.run(ctx, plan[:1], func(financeGiftScopeBatchEntry) error { return nil })
		done <- err
	}()
	<-entered
	if _, err := r.run(context.Background(), plan[:1], func(financeGiftScopeBatchEntry) error { return nil }); err == nil {
		t.Fatal("parallel batch accepted")
	}
	cancel()
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if *calls != 0 {
		t.Fatal("canceled worker queried source")
	}
}

func TestFinanceGiftScopeBatchWaitCanBeInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitFinanceGiftScopeBatch(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := waitFinanceGiftScopeBatch(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}

func TestFinanceGiftScopeBatchCancellationDuringSourcePreservesFacts(t *testing.T) {
	m, source, hour := giftScopeRepairFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before, err := loadFinanceGiftScopeSnapshot(context.Background(), m.usageFactsStore(), "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	r := newFinanceGiftScopeBatchRunner(m.usageFactsStore(), giftScopeHookSource{source, cancel})
	var entries []financeGiftScopeBatchEntry
	result, err := r.run(ctx, []financeGiftScopeTarget{{"v1", hour, 7}}, func(e financeGiftScopeBatchEntry) error { entries = append(entries, e); return nil })
	if err == nil || result.Status != "paused" || result.Remaining != 1 || len(entries) != 1 || entries[0].RowsUpdated != 0 {
		t.Fatalf("canceled source outcome=%+v %v", result, err)
	}
	after, err := loadFinanceGiftScopeSnapshot(context.Background(), m.usageFactsStore(), "v1", hour, 7)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("source cancellation changed facts", err)
	}
}
