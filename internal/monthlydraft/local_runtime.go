package monthlydraft

import (
	"context"
	"sync"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// LocalRuntime owns admission and worker lifetime, not database handles. Only
// the explicit automatic constructor installs a trusted publication adapter.
// It is opt-in local wiring; construction starts nothing. Only this wrapper
// should be exposed to HTTP while it owns the worker. Do not bypass it with
// direct Generation calls during shutdown.
type LocalRuntime struct {
	mu          sync.Mutex
	phase       string
	active      int
	workerDone  bool
	ctx         context.Context
	cancel      context.CancelFunc
	drained     chan struct{}
	worker      *LocalWorker
	service     monitor.MonthlyGenerationService
	scope       *generationScope
	preparation *onDemandPreparation
}

func NewLocalRuntime(g *Generation, authorization *gorm.DB) (*LocalRuntime, error) {
	w, err := NewLocalWorker(g)
	if err != nil {
		return nil, err
	}
	return newRuntimeWithWorker(g, authorization, w)
}

// NewAutomaticLocalRuntime connects the on-demand service to automatic
// publication with the same explicit start/stop and drain ownership. It does
// not collect sources, schedule months, open databases or start a goroutine.
// Production OpenDeployment is unchanged until trusted dependencies are wired.
func NewAutomaticLocalRuntime(g *Generation, authorization *gorm.DB, cfg GenerationRollout, metadata AutomaticContext, publish AutomaticPublisher) (*LocalRuntime, error) {
	scope, err := rolloutScope(cfg, time.Now())
	if err != nil || !cfg.Enabled {
		return nil, err
	}
	w, err := NewAutomaticLocalWorker(g, metadata, publish)
	if err != nil {
		return nil, err
	}
	r, err := newRuntimeWithWorker(g, authorization, w)
	if err != nil {
		return nil, err
	}
	r.scope, w.scope = scope, scope
	return r, nil
}

func newRuntimeWithWorker(g *Generation, authorization *gorm.DB, w *LocalWorker) (*LocalRuntime, error) {
	s, err := NewUsageHTTPServiceWithWorker(g, authorization, w)
	if err != nil {
		return nil, err
	}
	return &LocalRuntime{phase: "disabled", workerDone: true, drained: make(chan struct{}), worker: w, service: s}, nil
}

// Start is single-use. A stopped runtime cannot accidentally reopen admission;
// resuming requires constructing a new runtime against the retained receiver.
func (r *LocalRuntime) Start(parent context.Context) error {
	return r.start(parent, nil)
}

func (r *LocalRuntime) start(parent context.Context, background func(context.Context)) error {
	if parent == nil {
		return ErrGenerationInput
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != "disabled" {
		return ErrGenerationBusy
	}
	if err := parent.Err(); err != nil {
		return err
	}
	if r.scope != nil && r.scope.registered {
		calendar, ok := r.service.(*registrationCalendarService)
		if !ok || calendar.scope != r.scope || r.scope.authorize == nil || r.preparation == nil {
			return ErrGenerationInput // no broad-policy runtime without the real eligibility gate
		}
		if r.worker.dynamicSeries && (!generationHex(r.worker.seriesIdentity, 32) || r.worker.seriesResolve == nil || r.preparation.dependencies == nil) {
			return ErrGenerationInput // dynamic bounds require the real source and registered prefix adapter
		}
	}
	r.ctx, r.cancel = context.WithCancel(parent)
	r.phase, r.workerDone = "running", false
	if r.preparation != nil {
		r.active++
		go func() {
			defer func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.active--
				r.finishLocked()
			}()
			r.preparation.serve(r.ctx)
		}()
	}
	// Register before either goroutine can exit. Stop must also join private
	// preparation I/O, not just SQL work inside PrepareMonthlyInput.
	if background != nil {
		r.active++
		go func() {
			defer func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.active--
				r.finishLocked()
			}()
			background(r.ctx)
		}()
	}
	go func() {
		_ = r.worker.Serve(r.ctx)
		r.mu.Lock()
		defer r.mu.Unlock()
		// Unexpected worker exit must also close admission and cancel readers.
		r.cancel()
		r.workerDone = true
		if r.phase == "running" {
			r.phase = "stopping"
		}
		r.finishLocked()
	}()
	return nil
}

// Stop first closes admission, then cancels and joins worker AND active metadata
// requests. Only a nil result permits the caller to close Generation/databases.
// A timeout does not force-close them or claim a completed drain; call Stop
// again with a fresh context. Existing published bill routes are independent.
func (r *LocalRuntime) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrGenerationInput
	}
	r.mu.Lock()
	if r.phase != "stopped" {
		r.phase = "stopping"
		if r.cancel != nil {
			r.cancel()
		}
		r.finishLocked()
	}
	done := r.drained
	r.mu.Unlock()
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *LocalRuntime) Phase() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase == "running" && r.ctx.Err() != nil {
		return "stopping"
	}
	return r.phase
}

func (r *LocalRuntime) finishLocked() {
	if r.phase == "stopping" && r.workerDone && r.active == 0 {
		r.phase = "stopped"
		close(r.drained)
	}
}

func (r *LocalRuntime) call(parent context.Context, gid, version int64, month string, submit bool) (monitor.MonthlyGenerationView, error) {
	if parent == nil || parent.Err() != nil {
		return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	r.mu.Lock()
	if r.phase != "running" || r.ctx.Err() != nil {
		r.mu.Unlock()
		return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	if !r.scope.allows(gid, month) {
		r.mu.Unlock()
		// An inactive organization/month is not an unavailable worker. Do not
		// inspect another scope, enqueue work or disclose its selected inputs.
		if gid <= 0 {
			return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure("invalid_request")
		}
		if submit {
			return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure("not_prepared")
		}
		return monitor.MonthlyGenerationView{Month: month, State: "not_prepared"}, nil
	}
	r.active++
	lifetime := r.ctx
	r.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	defer func() {
		stop()
		cancel()
		r.mu.Lock()
		r.active--
		r.finishLocked()
		r.mu.Unlock()
	}()
	if lifetime.Err() != nil {
		cancel()
	}
	var view monitor.MonthlyGenerationView
	var err error
	if ctx.Err() != nil {
		return view, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	if submit {
		view, err = r.service.SubmitMonthly(ctx, gid, version, month)
	} else {
		view, err = r.service.ReadMonthly(ctx, gid, version, month)
	}
	if ctx.Err() != nil || lifetime.Err() != nil {
		// Admission may already be durable. Do not auto-resubmit or delete it;
		// after resumption, the existing idempotent status lookup recovers it.
		return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	return view, err
}

func (r *LocalRuntime) SubmitMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	return r.call(ctx, gid, version, month, true)
}

func (r *LocalRuntime) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	return r.call(ctx, gid, version, month, false)
}
