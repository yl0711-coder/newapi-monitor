package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// One explicit local task, no queue or automatic resume. Durable progress lives
// in the facts ledger; this bounded state only describes this process's attempt.
type financeGiftTaskControl struct {
	mu     sync.Mutex
	view   financeGiftTaskView
	cancel context.CancelFunc
	done   chan struct{}
	closed bool
}

type financeGiftTaskView struct {
	Instance   string `json:"instance"`
	Revision   uint64 `json:"revision"`
	TaskID     string `json:"task_id"`
	RequestID  string `json:"request_id"`
	Status     string `json:"status"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at"`
	ErrorCode  string `json:"error_code"`
}

var errGiftTaskConflict = errors.New("local task state changed; refresh before acting")
var errGiftTaskPanic = errors.New("local task panic")

const financeGiftTaskTimeout = 3 * time.Minute

func runFinanceGiftTaskSafely(ctx context.Context, run func(context.Context) (string, error)) (status string, err error) {
	defer func() {
		if recover() != nil {
			status, err = "failed", errGiftTaskPanic
		}
	}()
	status, err = run(ctx)
	if err == nil && status != "complete" && status != "partial" && status != "pending" {
		return "failed", errors.New("invalid local task result")
	}
	return status, err
}

func (q *financeGiftTaskControl) snapshot() (financeGiftTaskView, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.view.Instance == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return financeGiftTaskView{}, err
		}
		q.view = financeGiftTaskView{Instance: hex.EncodeToString(b[:]), Status: "idle"}
	}
	return q.view, nil
}

// A process nonce and monotonic revision reject stale requests, including old
// browser retries after a restart. Only the current request can be replayed.
func (q *financeGiftTaskControl) start(expected financeGiftTaskView, run func(context.Context) (string, error)) (financeGiftTaskView, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.view.Instance == "" || expected.Instance != q.view.Instance {
		return q.view, errGiftTaskConflict
	}
	if expected.RequestID == q.view.RequestID && expected.TaskID == q.view.TaskID {
		return q.view, nil
	}
	if q.cancel != nil || expected.Revision != q.view.Revision {
		return q.view, errGiftTaskConflict
	}
	ctx, cancel := context.WithTimeout(context.Background(), financeGiftTaskTimeout)
	q.cancel, q.done = cancel, make(chan struct{})
	q.view = financeGiftTaskView{Instance: q.view.Instance, Revision: q.view.Revision + 1, TaskID: expected.TaskID,
		RequestID: expected.RequestID, Status: "running", StartedAt: time.Now().Unix()}
	done := q.done
	go func() {
		status, err := runFinanceGiftTaskSafely(ctx, run)
		contextErr := ctx.Err()
		cancel()
		q.mu.Lock()
		defer q.mu.Unlock()
		q.view.Status, q.view.ErrorCode = status, ""
		if err != nil {
			q.view.Status, q.view.ErrorCode = "failed", "revalidation_or_execution_failed"
			if errors.Is(err, errGiftTaskPanic) {
				q.view.ErrorCode = "internal_error"
			}
			if errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled) {
				q.view.Status, q.view.ErrorCode = "stopped", "canceled"
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded) {
				q.view.Status, q.view.ErrorCode = "timed_out", "budget_exhausted"
			}
		}
		q.view.FinishedAt = time.Now().Unix()
		q.view.Revision++
		q.cancel = nil
		close(done)
	}()
	return q.view, nil
}

func (q *financeGiftTaskControl) stop(expected financeGiftTaskView) (financeGiftTaskView, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if expected.Instance != q.view.Instance || expected.TaskID != q.view.TaskID || expected.RequestID != q.view.RequestID {
		return q.view, errGiftTaskConflict
	}
	if q.cancel != nil && q.view.Status != "stopping" {
		q.view.Status = "stopping"
		q.view.Revision++
		q.cancel()
	}
	return q.view, nil
}

func (q *financeGiftTaskControl) shutdown(ctx context.Context) bool {
	q.mu.Lock()
	q.closed = true
	if q.cancel != nil {
		q.cancel()
	}
	done := q.done
	q.mu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
