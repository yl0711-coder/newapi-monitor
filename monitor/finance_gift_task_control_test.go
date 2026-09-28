package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func giftControlWait(t *testing.T, q *financeGiftTaskControl) financeGiftTaskView {
	t.Helper()
	q.mu.Lock()
	done := q.done
	q.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not finish")
	}
	v, err := q.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestFinanceGiftTaskControlIdempotencyStopResume(t *testing.T) {
	var q financeGiftTaskControl
	v, err := q.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	expected := financeGiftTaskView{Instance: v.Instance, Revision: v.Revision, TaskID: strings.Repeat("a", 64), RequestID: strings.Repeat("b", 32)}
	run := func(ctx context.Context) (string, error) { <-ctx.Done(); return "", ctx.Err() }
	first, err := q.start(expected, run)
	if err != nil || first.Status != "running" {
		t.Fatal(first, err)
	}
	repeat, err := q.start(expected, func(context.Context) (string, error) { t.Error("duplicate run"); return "complete", nil })
	if err != nil || repeat != first {
		t.Fatal(repeat, err)
	}
	other := expected
	other.RequestID = strings.Repeat("c", 32)
	other.Revision = first.Revision
	if _, err := q.start(other, run); err == nil {
		t.Fatal("parallel task admitted")
	}
	if _, err := q.stop(other); err == nil {
		t.Fatal("wrong attempt stopped")
	}
	if stopped, err := q.stop(first); err != nil || stopped.Status != "stopping" {
		t.Fatal(stopped, err)
	}
	final := giftControlWait(t, &q)
	if final.Status != "stopped" {
		t.Fatal(final)
	}
	repeat, err = q.start(expected, run)
	if err != nil || repeat != final {
		t.Fatal("retry restarted stopped task", repeat, err)
	}
	if _, err := q.start(other, run); err == nil {
		t.Fatal("stale revision admitted")
	}
	other.Revision = final.Revision
	if _, err := q.start(other, func(context.Context) (string, error) { return "complete", nil }); err != nil {
		t.Fatal(err)
	}
	final = giftControlWait(t, &q)
	if final.Status != "complete" {
		t.Fatal(final)
	}
	if _, err := q.start(expected, run); err == nil {
		t.Fatal("older retry restarted task")
	}
}

func TestFinanceGiftTaskControlShutdownAndRestart(t *testing.T) {
	var q financeGiftTaskControl
	v, _ := q.snapshot()
	v.TaskID = "task"
	v.RequestID = "request"
	release := make(chan struct{})
	if _, err := q.start(v, func(context.Context) (string, error) { <-release; return "complete", nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if q.shutdown(ctx) {
		t.Fatal("shutdown claimed stopped while worker remained")
	}
	if _, err := q.start(v, func(context.Context) (string, error) { return "complete", nil }); err == nil {
		t.Fatal("shutdown admitted retry")
	}
	close(release)
	giftControlWait(t, &q)
	if !q.shutdown(context.Background()) {
		t.Fatal("completed task not joined")
	}
	var restarted financeGiftTaskControl
	fresh, _ := restarted.snapshot()
	if fresh.Instance == v.Instance || fresh.Status != "idle" {
		t.Fatal("restart reused process state")
	}
	if _, err := restarted.start(v, func(context.Context) (string, error) { return "complete", nil }); err == nil {
		t.Fatal("old browser retry admitted after restart")
	}
}

func TestFinanceGiftTaskControlSafeFailure(t *testing.T) {
	for _, cause := range []error{errors.New("secret SQL/path"), context.DeadlineExceeded} {
		var q financeGiftTaskControl
		v, _ := q.snapshot()
		v.TaskID = "task"
		v.RequestID = "request"
		_, err := q.start(v, func(context.Context) (string, error) { return "", cause })
		if err != nil {
			t.Fatal(err)
		}
		got := giftControlWait(t, &q)
		if got.Status != "failed" && got.Status != "timed_out" {
			t.Fatal(got)
		}
		if strings.Contains(got.ErrorCode, "secret") {
			t.Fatal("raw worker error leaked")
		}
	}
}

func TestFinanceGiftTaskControlPanicReleasesSlot(t *testing.T) {
	var q financeGiftTaskControl
	v, _ := q.snapshot()
	v.TaskID = "task"
	v.RequestID = "request"
	_, err := q.start(v, func(context.Context) (string, error) { panic("private diagnostic") })
	if err != nil {
		t.Fatal(err)
	}
	got := giftControlWait(t, &q)
	if got.Status != "failed" || got.ErrorCode != "internal_error" {
		t.Fatal(got)
	}
	got.RequestID = "next"
	if _, err := q.start(got, func(context.Context) (string, error) { return "complete", nil }); err != nil {
		t.Fatal(err)
	}
	if got := giftControlWait(t, &q); got.Status != "complete" {
		t.Fatal(got)
	}
}
