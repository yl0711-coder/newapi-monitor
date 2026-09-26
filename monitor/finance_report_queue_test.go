package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func waitFinanceQueue(t *testing.T, q *financeReportQueue) {
	t.Helper()
	q.mu.Lock()
	done := q.done
	q.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("finance queue did not drain")
	}
}

func TestFinanceQueueBoundedFIFOAndDeduplication(t *testing.T) {
	var q financeReportQueue
	defer q.shutdown(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	var active, peak atomic.Int64
	order := make(chan int, financeReportQueueCapacity)
	q.submit(context.Background(), "running", false, func(ctx context.Context) error {
		active.Add(1)
		peak.Store(1)
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		active.Add(-1)
		return nil
	})
	<-started
	for i := 0; i < financeReportQueueCapacity; i++ {
		i := i
		key := fmt.Sprint(i)
		if state := q.submit(context.Background(), key, false, func(context.Context) error {
			n := active.Add(1)
			if n > peak.Load() {
				peak.Store(n)
			}
			order <- i
			active.Add(-1)
			return nil
		}); state != "queued" {
			t.Fatalf("queue rejected slot %d: %s", i, state)
		}
		if state := q.submit(context.Background(), key, true, nil); state != "queued" {
			t.Fatal("duplicate was not merged")
		}
	}
	if q.submit(context.Background(), "overflow", false, nil) != "busy" {
		t.Fatal("queue was not bounded")
	}
	if q.stats().Running != 1 || q.stats().Pending != financeReportQueueCapacity {
		t.Fatal(q.stats())
	}
	close(release)
	waitFinanceQueue(t, &q)
	for i := 0; i < financeReportQueueCapacity; i++ {
		if got := <-order; got != i {
			t.Fatalf("unfair ordering: %d != %d", got, i)
		}
	}
	if peak.Load() != 1 {
		t.Fatalf("concurrent builds: %d", peak.Load())
	}
}

func TestFinanceQueueFailureBackoffAndBoundedHistory(t *testing.T) {
	var q financeReportQueue
	defer q.shutdown(context.Background())
	var calls atomic.Int64
	q.submit(context.Background(), "failed", false, func(context.Context) error { calls.Add(1); return errors.New("offline") })
	waitFinanceQueue(t, &q)
	for i := 0; i < 20; i++ {
		if q.submit(context.Background(), "failed", true, nil) != "failed" {
			t.Fatal("manual refresh bypassed failure backoff")
		}
	}
	if calls.Load() != 1 || q.stats().RecentFailures != 1 {
		t.Fatal("failed task was retried without backoff")
	}
	for i := 0; i < financeReportQueueHistory*2; i++ {
		q.submit(context.Background(), fmt.Sprint(i), false, func(context.Context) error { return nil })
		waitFinanceQueue(t, &q)
	}
	if len(q.jobs) > financeReportQueueHistory {
		t.Fatal("unbounded job history")
	}
}

func TestFinanceQueueShutdownCancelsActiveAndDiscardsPending(t *testing.T) {
	var q financeReportQueue
	started := make(chan struct{})
	q.submit(context.Background(), "running", false, func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() })
	<-started
	var pendingRan atomic.Bool
	q.submit(context.Background(), "waiting", false, func(context.Context) error { pendingRan.Store(true); return nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !q.shutdown(ctx) || pendingRan.Load() {
		t.Fatal("shutdown failed to cancel/discard tasks")
	}
	if q.submit(context.Background(), "new", false, nil) != "stopped" {
		t.Fatal("closed queue accepted work")
	}
}

func TestFinanceQueueMissingPayloadCanRebuildWithoutSuccessCooldown(t *testing.T) {
	var q financeReportQueue
	defer q.shutdown(context.Background())
	run := func(context.Context) error { return nil }
	q.submit(context.Background(), "evicted", false, run)
	waitFinanceQueue(t, &q)
	if q.submit(context.Background(), "evicted", false, run) != "succeeded" {
		t.Fatal("successful job did not retain verification cooldown")
	}
	q.forgetMissingResult("evicted")
	if q.submit(context.Background(), "evicted", false, run) != "queued" {
		t.Fatal("evicted payload was blocked by a metadata-only success record")
	}
	waitFinanceQueue(t, &q)
	q.submit(context.Background(), "failed", false, func(context.Context) error { return errors.New("offline") })
	waitFinanceQueue(t, &q)
	q.forgetMissingResult("failed")
	if q.submit(context.Background(), "failed", true, run) != "failed" {
		t.Fatal("missing payload bypassed failure backoff")
	}
}
