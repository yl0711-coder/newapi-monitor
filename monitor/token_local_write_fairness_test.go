package monitor

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func awaitTokenWriterQueue(t *testing.T, w *tokenLocalWriter, want int) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		w.mu.Lock()
		n := w.waiters.Len()
		w.mu.Unlock()
		if n == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("writer did not reach expected queue length", want)
}

func TestTokenLocalWriterHandsOffBeforeMaintenanceReacquires(t *testing.T) {
	var w tokenLocalWriter
	w.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired := make(chan error, 1)
	release := make(chan struct{})
	go func() {
		err := w.lock(ctx)
		acquired <- err
		if err == nil {
			<-release
			w.Unlock()
		}
	}()
	defer close(release)
	awaitTokenWriterQueue(t, &w, 1)
	w.Unlock()
	// Simulate the association loop immediately starting its next row. The
	// queued collector owns the slot even before its goroutine is scheduled.
	if w.TryLock() {
		w.Unlock()
		t.Fatal("maintenance overtook an already queued collector")
	}
	if err := <-acquired; err != nil {
		t.Fatal("queued collection was starved", err)
	}
}

func TestTokenLocalWriterCanceledWaiterPreservesFollowingOrder(t *testing.T) {
	var w tokenLocalWriter
	w.Lock()
	canceled, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- w.lock(canceled) }()
	awaitTokenWriterQueue(t, &w, 1)
	order := make(chan int, 2)
	for _, id := range []int{2, 3} {
		go func(id int) {
			ctx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			if err := w.lock(ctx); err != nil {
				order <- -id
				return
			}
			order <- id
			w.Unlock()
		}(id)
		awaitTokenWriterQueue(t, &w, id)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("queued cancellation lost", err)
	}
	w.Unlock()
	if a, b := <-order, <-order; a != 2 || b != 3 {
		t.Fatal("canceled waiter changed following order", a, b)
	}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := w.lock(ctx); err != nil {
		t.Fatal("cancellation leaked writer ownership", err)
	}
	w.Unlock()
}

func TestTokenLocalWriterCancellationHandoffRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		var w tokenLocalWriter
		w.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			if err := w.lock(ctx); err == nil {
				w.Unlock()
			}
			close(done)
		}()
		awaitTokenWriterQueue(t, &w, 1)
		go cancel()
		w.Unlock()
		<-done
		if !w.TryLock() {
			t.Fatal("canceled handoff leaked writer ownership", i)
		}
		w.Unlock()
	}
}
