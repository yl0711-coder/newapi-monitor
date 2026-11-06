package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// LocalWorker is opt-in local orchestration. Constructing it starts nothing;
// the local composition root owns Serve's context and must join it BEFORE
// closing Generation/databases. NewLocalWorker is draft-only; only the separate
// opt-in automatic constructor installs publication. No source repair occurs.
type LocalWorker struct {
	g                *Generation
	wake             chan struct{}
	serving          atomic.Bool
	pollMin, pollMax time.Duration
	mu               sync.Mutex
	status           LocalWorkerStatus
	deferred         map[string]workerDeferral // bounded by the 256-task registry
	scope            *generationScope          // immutable after Start; nil only for explicit local tests
	finalize         func(context.Context, monthlytasks.Task, publicationLease) error
	beforeFinalize   func(context.Context, monthlytasks.Task) (func(context.Context, monthlytasks.Task, publicationLease) error, error)
	publications     *publicationStore
	seriesScopes     map[int64]AutomaticDisplaySeriesRange
	dynamicSeries    bool   // explicit registered policy only; immutable before Start
	seriesIdentity   string // validated source identity, never read from a customer request
	seriesResolve    func(context.Context, AutomaticDisplaySeriesRange, monthlytasks.Request) (AutomaticDisplaySeriesRange, error)
}

type LocalWorkerStatus struct {
	Running     bool
	Polls       uint64
	Dispatched  uint64
	DraftsReady uint64
	Published   uint64
	LastCode    string
}

type workerDeferral struct {
	count               int
	until               time.Time
	preparation         string
	preparationFailures int
}

func NewLocalWorker(g *Generation) (*LocalWorker, error) {
	if g == nil || g.receiver == nil || g.tasks == nil || g.root == nil || g.funding == nil || g.verifyCurrent == nil {
		return nil, ErrGenerationInput
	}
	return &LocalWorker{g: g, wake: make(chan struct{}, 1), pollMin: 5 * time.Second,
		pollMax: time.Minute, deferred: make(map[string]workerDeferral)}, nil
}

// This never asserts availability: the customer HTTP boundary checks the
// durable publication itself. Terminal/paused state survives process restart;
// a saved journal without a currently visible bill requires internal review.
func (w *LocalWorker) finalizationState(ctx context.Context, id string) (string, error) {
	if w.getDeferral(id).preparationFailures >= 3 {
		return "needs_review", nil
	}
	r, err := w.publications.read(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return "finalizing", nil
	}
	if err != nil {
		return "", err
	}
	if r.state == publicationPaused || r.state == publicationSaved {
		return "needs_review", nil
	}
	return "finalizing", nil
}

// Wake is coalesced and cannot bypass the minimum poll gap or a database-error
// backoff. It carries no client task ID or scope. Periodic scans recover lost
// wakeups and tasks already present when the process starts.
func (w *LocalWorker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *LocalWorker) Status() LocalWorkerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.status
	out.Running = w.serving.Load()
	return out
}

// Serve is blocking and has one owner. Task leases additionally fence multiple
// workers/processes sharing the receiver. Cancelling waits for Run cleanup;
// its transient failure may be retried after the persisted 30-second cooldown.
func (w *LocalWorker) Serve(ctx context.Context) error {
	if !w.serving.CompareAndSwap(false, true) {
		return ErrGenerationBusy
	}
	defer w.serving.Store(false)
	delay, last, databaseFailure := time.Duration(0), time.Time{}, false
	for {
		if err := w.wait(ctx, last, delay, databaseFailure); err != nil {
			return err
		}
		last = time.Now()
		worked, err := w.step(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		databaseFailure = err != nil
		if err != nil || !worked {
			delay = min(w.pollMax, max(w.pollMin, delay*2))
		} else {
			delay = w.pollMin
		}
	}
}

func (w *LocalWorker) wait(ctx context.Context, last time.Time, delay time.Duration, databaseFailure bool) error {
	deadline := time.Now().Add(delay)
	wake := w.wake
	if databaseFailure {
		wake = nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			return ctx.Err()
		case <-wake:
			timer.Stop()
			earliest := last.Add(w.pollMin)
			if earliest.Before(deadline) {
				deadline = earliest
			}
			// One coalesced wake per wait is enough; repeated clicks cannot spin
			// this loop or make another receiver scan before the same deadline.
			wake = nil
		}
	}
}

func (w *LocalWorker) step(ctx context.Context) (bool, error) {
	var jobs []monthlytasks.WorkCandidate
	var err error
	if w.finalize != nil {
		jobs, err = w.g.tasks.WorkCandidatesWithReady(ctx)
	} else {
		jobs, err = w.g.tasks.WorkCandidates(ctx)
	}
	w.mu.Lock()
	w.status.Polls++
	w.status.LastCode = "idle"
	if err != nil {
		w.status.LastCode = "receiver_unavailable"
	}
	w.mu.Unlock()
	if err != nil {
		return false, err
	}
	present := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		present[j.Task.ID] = true
	}
	for id := range w.deferred {
		if !present[id] {
			w.forgetDeferral(id)
		}
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// Outside rollout is not revocation or evidence failure. Leave queued
		// tasks, attempts, leases and retained inputs entirely unchanged.
		if !w.scope.allows(j.Task.Request.Owner.GroupID, j.Task.Request.Month) {
			continue
		}
		if w.publications != nil && j.Task.State == monthlytasks.DraftReady {
			r, err := w.publications.read(ctx, j.Task.ID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
			if err == nil && !publicationEligible(r, w.publications.now().UnixMilli()) {
				continue
			}
		}
		if time.Now().Before(w.getDeferral(j.Task.ID).until) {
			continue
		}
		err := w.g.currentTask(ctx, j.Task)
		if err == nil && j.Retry {
			err = w.g.tasks.RetryTransient(ctx, j.Task.Request.Owner, j.Task.ID)
		}
		if err == nil && j.Task.State != monthlytasks.DraftReady {
			w.mu.Lock()
			w.status.Dispatched++
			w.mu.Unlock()
			_, err = w.g.Run(ctx, j.Task.Request.Owner, j.Task.ID)
		}
		published := false
		if err == nil && w.finalize != nil {
			var ready monthlytasks.Task
			ready, err = w.g.tasks.Get(ctx, j.Task.Request.Owner, j.Task.ID)
			finalize := w.finalize
			if err == nil && w.beforeFinalize != nil {
				finalize, err = w.beforeFinalize(ctx, ready)
				if err == nil && finalize == nil {
					err = ErrGenerationIntegrity
				}
			}
			if err == nil {
				var lease publicationLease
				var claimed bool
				lease, claimed, err = w.publications.claim(ctx, ready)
				if err == nil && claimed {
					err = finalize(ctx, ready, lease)
					// Even during shutdown retain the result/budget before closing
					// databases. Never reuse a canceled work context for cleanup.
					cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					finishErr := w.publications.finish(cleanup, lease, err)
					cancel()
					if finishErr != nil {
						return true, finishErr
					}
					published = err == nil
				}
			}
		}
		w.mu.Lock()
		if err == nil {
			if j.Task.State != monthlytasks.DraftReady {
				w.status.DraftsReady++
			}
			if published {
				w.status.Published++
				w.status.LastCode = "published"
			} else {
				w.status.LastCode = "draft_ready"
			}
		} else {
			w.status.LastCode = PublicUsageGenerationProblem(err).Code
		}
		w.mu.Unlock()
		if err == nil || errors.Is(err, ErrGenerationChanged) || errors.Is(err, monthlytasks.ErrExhausted) {
			w.forgetDeferral(j.Task.ID)
		} else {
			d := w.getDeferral(j.Task.ID)
			d.count = min(d.count+1, 5)
			d.until = time.Now().Add(min(w.pollMax, w.pollMin*time.Duration(1<<(d.count-1))))
			w.putDeferral(j.Task.ID, d)
		}
		// One dispatch/check per cycle, letting another eligible task run while
		// this task backs off. Ready/invalidated/evidence failures are excluded.
		return true, nil
	}
	return false, nil
}

func (w *LocalWorker) getDeferral(id string) workerDeferral {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deferred[id]
}

func (w *LocalWorker) putDeferral(id string, state workerDeferral) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deferred[id] = state
}

func (w *LocalWorker) forgetDeferral(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.deferred, id)
}
