package monitor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackgroundSourceLowReadHasFreshQueryBudget(t *testing.T) {
	m := &Monitor{}
	// Waiting exceeds the entire query budget. A shared deadline would fail.
	m.deferBackgroundSourceStart(300 * time.Millisecond)
	timing, err := m.withBackgroundSourceLowRead(context.Background(), 3*time.Second, 200*time.Millisecond, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) < 100*time.Millisecond {
			t.Errorf("source wait consumed query budget: deadline=%v", deadline)
		}
		return ctx.Err()
	})
	if err != nil || timing.Wait < 250*time.Millisecond {
		t.Fatalf("cooldown/query isolation: timing=%+v err=%v", timing, err)
	}
	if high, low := m.backgroundSourceWaiterCounts(); high != 0 || low != 0 {
		t.Fatalf("leaked waiters: %d/%d", high, low)
	}
}

func TestBackgroundSourceLowReadCancellationAndRelease(t *testing.T) {
	t.Run("waiting", func(t *testing.T) {
		m := &Monitor{}
		release, err := m.acquireBackgroundSource(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		_, err = m.withBackgroundSourceLowRead(context.Background(), 10*time.Millisecond, time.Second, func(context.Context) error {
			t.Error("query ran without source slot")
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("wait error=%v", err)
		}
		_, low := m.backgroundSourceWaiterCounts()
		if low != 0 {
			t.Fatalf("leaked waiter=%d", low)
		}
	})
	for _, parentCancel := range []bool{false, true} {
		name := "query_timeout"
		if parentCancel {
			name = "parent_cancel"
		}
		t.Run(name, func(t *testing.T) {
			m := &Monitor{}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := m.withBackgroundSourceLowRead(parent, time.Second, 10*time.Millisecond, func(ctx context.Context) error {
				if parentCancel {
					cancel()
				}
				<-ctx.Done()
				return nil // Late success must not hide cancellation.
			})
			want := context.DeadlineExceeded
			if parentCancel {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("error=%v want=%v", err, want)
			}
			probe, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			release, err := m.acquireBackgroundSource(probe)
			if err != nil {
				t.Fatalf("source slot leaked: %v", err)
			}
			release()
		})
	}
}

func TestBackgroundSourceLowReadPreservesHighPriority(t *testing.T) {
	m := &Monitor{}
	// A cooling low lane must not block a high-priority source request.
	m.deferBackgroundSourceStart(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.withBackgroundSourceLowRead(ctx, time.Second, time.Second, func(context.Context) error {
			return errors.New("must not bypass cooldown")
		})
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, low := m.backgroundSourceWaiterCounts()
		if low == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("low waiter did not register")
		}
		time.Sleep(time.Millisecond)
	}
	probe, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	release, err := m.acquireBackgroundSource(probe)
	if err != nil {
		t.Fatal(err)
	}
	release()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("low error=%v", err)
	}
}

func TestStabilityColdWindowWaitsThroughNormalCooldown(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	t.Cleanup(m.Close)
	// The former 12s gate timeout exhausted before this normal low-lane
	// cooldown. Exercise the real migration query and local publication.
	m.deferBackgroundSourceStart(13 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).Unix()
	if _, err := m.sampleStabilityProblemWindow(ctx, base, base+60, true); err != nil {
		t.Fatalf("normal cooldown was treated as query failure: %v", err)
	}
}
