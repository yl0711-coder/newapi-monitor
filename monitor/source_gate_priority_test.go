package monitor

import (
	"context"
	"testing"
	"time"
)

func backgroundSourceCriticalWaitersForTest(m *Monitor) int {
	m.backgroundSourceScheduleMu.Lock()
	defer m.backgroundSourceScheduleMu.Unlock()
	return m.backgroundSourceCriticalWaiters
}

// The minute sampler (critical) must get the slot before any number of
// already-waiting high-lane claims (Usage facts live Tail burst).
func TestBackgroundSourceCriticalBeatsWaitingHigh(t *testing.T) {
	m := &Monitor{}
	release, err := m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make(chan string, 4)
	for i := 0; i < 3; i++ {
		go func() {
			leave, err := m.acquireBackgroundSource(ctx)
			if err != nil {
				return
			}
			order <- "high"
			leave()
		}()
	}
	lifecycleEventually(t, time.Second, func() bool { h, _ := m.backgroundSourceWaiterCounts(); return h == 3 }, "high waiters did not queue")
	go func() {
		leave, err := m.acquireBackgroundSourceCritical(ctx)
		if err != nil {
			return
		}
		order <- "critical"
		leave()
	}()
	lifecycleEventually(t, time.Second, func() bool { return backgroundSourceCriticalWaitersForTest(m) == 1 }, "critical waiter did not queue")
	release()
	select {
	case got := <-order:
		if got != "critical" {
			t.Fatalf("first claim after release = %s, want critical", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no claim after release")
	}
	for i := 0; i < 3; i++ {
		select {
		case <-order:
		case <-time.After(2 * time.Second):
			t.Fatal("high waiters stuck after critical")
		}
	}
}

// A low head that has waited past the starvation threshold claims the slot
// even while high waiters keep arriving; before the threshold it yields.
func TestBackgroundSourceLowHeadClaimsAfterStarvation(t *testing.T) {
	old := backgroundSourceLowStarvation
	backgroundSourceLowStarvation = 300 * time.Millisecond
	t.Cleanup(func() { backgroundSourceLowStarvation = old })

	m := &Monitor{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A sustained high burst: each high holder keeps the slot briefly and a new
	// high waiter is always queued.
	stop := make(chan struct{})
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			leave, err := m.acquireBackgroundSource(ctx)
			if err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
			leave()
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			leave, err := m.acquireBackgroundSource(ctx)
			if err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
			leave()
		}
	}()
	lifecycleEventually(t, time.Second, func() bool { h, _ := m.backgroundSourceWaiterCounts(); return h >= 1 }, "high burst did not start")

	started := time.Now()
	lowCtx, lowCancel := context.WithTimeout(ctx, 3*time.Second)
	defer lowCancel()
	leave, err := m.acquireBackgroundSourceLow(lowCtx)
	if err != nil {
		t.Fatalf("low head starved under a high burst: %v", err)
	}
	waited := time.Since(started)
	t.Logf("low claimed after %s", waited)
	leave()
	close(stop)
	cancel()
	<-burstDone
	if waited < 200*time.Millisecond {
		t.Fatalf("low claimed after %s, before the starvation threshold while high waiters existed", waited)
	}
}

// Critical waiters still go before a starved low head.
func TestBackgroundSourceStarvedLowStillYieldsToCritical(t *testing.T) {
	old := backgroundSourceLowStarvation
	backgroundSourceLowStarvation = 50 * time.Millisecond
	t.Cleanup(func() { backgroundSourceLowStarvation = old })

	m := &Monitor{}
	release, err := m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make(chan string, 2)
	go func() {
		leave, err := m.acquireBackgroundSourceLow(ctx)
		if err != nil {
			return
		}
		order <- "low"
		leave()
	}()
	lifecycleEventually(t, time.Second, func() bool { _, l := m.backgroundSourceWaiterCounts(); return l == 1 }, "low waiter did not queue")
	time.Sleep(100 * time.Millisecond) // low head is now starved
	go func() {
		leave, err := m.acquireBackgroundSourceCritical(ctx)
		if err != nil {
			return
		}
		order <- "critical"
		time.Sleep(20 * time.Millisecond)
		leave()
	}()
	lifecycleEventually(t, time.Second, func() bool { return backgroundSourceCriticalWaitersForTest(m) == 1 }, "critical waiter did not queue")
	release()
	for _, want := range []string{"critical", "low"} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiting for %s", want)
		}
	}
}
