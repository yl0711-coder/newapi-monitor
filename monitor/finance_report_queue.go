package monitor

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	financeReportQueueCapacity  = 16
	financeReportQueueHistory   = 32
	financeReportVerifyInterval = time.Minute
	financeReportManualInterval = 5 * time.Second
)

type financeReportJob struct {
	key      string
	state    string
	finished time.Time
	parent   context.Context
	run      func(context.Context) error
}

// Queue metadata is bounded independently of the byte-bounded report cache.
// All enabled report entry points share one worker; browser cancellation never
// owns its context. Completed records provide retry backoff, not cached money.
type financeReportQueue struct {
	mu      sync.Mutex
	jobs    map[string]*financeReportJob
	pending []*financeReportJob
	done    chan struct{}
	cancel  context.CancelFunc
	closed  bool
}

func (q *financeReportQueue) submit(parent context.Context, key string, manual bool, run func(context.Context) error) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || parent.Err() != nil {
		return "stopped"
	}
	if q.jobs == nil {
		q.jobs = make(map[string]*financeReportJob)
	}
	if job := q.jobs[key]; job != nil {
		if job.state == "queued" || job.state == "running" {
			return job.state
		}
		interval := financeReportVerifyInterval
		if manual && job.state == "succeeded" {
			interval = financeReportManualInterval
		}
		if time.Since(job.finished) < interval {
			return job.state
		}
	}
	if len(q.pending) >= financeReportQueueCapacity {
		return "busy"
	}
	// Evict only completed metadata. Pending/running jobs keep their identity
	// until completion, so eviction cannot create duplicate work.
	if len(q.jobs) >= financeReportQueueHistory {
		var oldest *financeReportJob
		for _, job := range q.jobs {
			if !job.finished.IsZero() && (oldest == nil || job.finished.Before(oldest.finished)) {
				oldest = job
			}
		}
		if oldest != nil {
			delete(q.jobs, oldest.key)
		}
	}
	job := &financeReportJob{key: key, state: "queued", parent: parent, run: run}
	q.jobs[key] = job
	q.pending = append(q.pending, job)
	if q.done == nil {
		q.done = make(chan struct{})
		go q.drain()
	}
	return "queued"
}

func (q *financeReportQueue) drain() {
	for {
		q.mu.Lock()
		if q.closed || len(q.pending) == 0 {
			q.pending = nil
			close(q.done)
			q.done = nil
			q.mu.Unlock()
			return
		}
		job := q.pending[0]
		q.pending = q.pending[1:]
		job.state = "running"
		ctx, cancel := context.WithTimeout(job.parent, financeReportBuildTimeout)
		q.cancel = cancel
		q.mu.Unlock()

		err := job.run(ctx)
		if err == nil {
			err = ctx.Err()
		}
		cancel()
		q.mu.Lock()
		job.state = "succeeded"
		if err != nil {
			job.state = "failed"
			if errors.Is(err, context.Canceled) {
				job.state = "stopped"
			}
		}
		job.finished = time.Now()
		job.run = nil
		job.parent = nil
		q.cancel = nil
		q.mu.Unlock()
	}
}

// A result may have been evicted from the bounded payload/file caches while
// its small success record remains. That record must not delay a cold rebuild.
func (q *financeReportQueue) forgetMissingResult(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if job := q.jobs[key]; job != nil && job.state == "succeeded" {
		delete(q.jobs, key)
	}
}

type financeReportQueueStats struct {
	Enabled        bool `json:"enabled"`
	Running        int  `json:"running"`
	Pending        int  `json:"pending"`
	RecentFailures int  `json:"recent_failures"`
	Capacity       int  `json:"capacity"`
}

func (q *financeReportQueue) stats() financeReportQueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := financeReportQueueStats{Pending: len(q.pending), Capacity: financeReportQueueCapacity}
	for _, job := range q.jobs {
		if job.state == "running" {
			stats.Running++
		}
		if job.state == "failed" && time.Since(job.finished) < 5*time.Minute {
			stats.RecentFailures++
		}
	}
	return stats
}

func (q *financeReportQueue) shutdown(ctx context.Context) bool {
	q.mu.Lock()
	q.closed = true
	if q.cancel != nil {
		q.cancel()
	}
	for _, job := range q.pending {
		job.state = "stopped"
		job.finished = time.Now()
		job.run = nil
		job.parent = nil
	}
	q.pending = nil
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
