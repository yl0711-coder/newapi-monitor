package monitor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUpstreamAccountGateDoesNotBlockUnrelatedDomain(t *testing.T) {
	m := &Monitor{}
	releaseBusy, err := m.tryAcquireUpstreamAccountBackground("dense.example")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBusy()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	releaseAdmin, err := m.acquireUpstreamAccountAdmin(ctx, "config.example")
	if err != nil {
		t.Fatalf("unrelated account blocked admin save: %v", err)
	}
	releaseAdmin()
}

func TestUpstreamAccountGateSerializesSameDomainAndBackgroundYields(t *testing.T) {
	m := &Monitor{}
	releaseWorker, err := m.tryAcquireUpstreamAccountBackground("same.example")
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan func(), 1)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		release, acquireErr := m.acquireUpstreamAccountAdmin(ctx, "same.example")
		if acquireErr != nil {
			errCh <- acquireErr
			return
		}
		acquired <- release
	}()

	deadline := time.Now().Add(250 * time.Millisecond)
	for m.upstreamAccountGate("same.example").adminWaiters.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, busyErr := m.tryAcquireUpstreamAccountBackground("same.example"); !errors.Is(busyErr, errUpstreamAccountBusy) {
		t.Fatalf("background did not yield to waiting admin: %v", busyErr)
	}

	releaseWorker()
	select {
	case releaseAdmin := <-acquired:
		if _, busyErr := m.tryAcquireUpstreamAccountBackground("same.example"); !errors.Is(busyErr, errUpstreamAccountBusy) {
			t.Fatalf("same-domain background overlapped admin: %v", busyErr)
		}
		releaseAdmin()
	case acquireErr := <-errCh:
		t.Fatal(acquireErr)
	case <-time.After(time.Second):
		t.Fatal("admin did not acquire released account gate")
	}
}

func TestRunUpstreamPeriodicLaneStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runs := make(chan struct{}, 2)
	done := make(chan struct{})
	go func() {
		runUpstreamPeriodicLane(ctx, 0, 5*time.Millisecond, func(context.Context) { runs <- struct{}{} })
		close(done)
	}()
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("periodic lane did not run")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("periodic lane ignored cancellation")
	}
}
