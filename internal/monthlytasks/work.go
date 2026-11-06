package monthlytasks

import (
	"context"
	"database/sql"
	"sort"
)

// WorkCandidate is internal metadata, not a lease, authorization or customer
// output. Claim/RetryTransient must recheck state after this read-only snapshot.
type WorkCandidate struct {
	Task  Task
	Retry bool
}

// WorkCandidates reads at most the existing 256-task registry. It never mutates
// state, loads financial input or exposes lease secrets. A live lease defers all
// calculation; an expired third attempt remains eligible for fenced exhaustion.
func (s *Store) WorkCandidates(parent context.Context) ([]WorkCandidate, error) {
	return s.workCandidates(parent, false)
}

// Ready candidates are opt-in for a trusted publication worker. The default
// draft worker's behavior stays unchanged. Readers still receive no lease token.
func (s *Store) WorkCandidatesWithReady(parent context.Context) ([]WorkCandidate, error) {
	return s.workCandidates(parent, true)
}

func (s *Store) workCandidates(parent context.Context, includeReady bool) ([]WorkCandidate, error) {
	ctx, cancel := context.WithTimeout(parent, operationLimit)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	observed, err := InspectReceiverTasks(ctx, tx)
	if err != nil {
		return nil, err
	}
	now := s.now().UnixMilli()
	out := make([]WorkCandidate, 0, len(observed))
	live := false
	for _, o := range observed {
		task := o.Task
		if now < task.UpdatedAtMS {
			return nil, ErrClock
		}
		switch task.State {
		case DraftReady:
			if includeReady {
				out = append(out, WorkCandidate{Task: task})
			}
		case Queued:
			out = append(out, WorkCandidate{Task: task})
		case Running:
			r, err := load(ctx, tx, "id", task.ID)
			if err != nil {
				return nil, err
			}
			if r.until > now {
				live = true
			} else {
				out = append(out, WorkCandidate{Task: task})
			}
		case Failed:
			if task.Attempts < maxAttempts && now >= task.RetryAtMS &&
				(task.FailureCode == "source_unavailable" || task.FailureCode == "internal_failure") {
				out = append(out, WorkCandidate{Task: task, Retry: true})
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := tx.Rollback(); err != nil {
		return nil, err
	}
	if live {
		return []WorkCandidate{}, nil
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Task.UpdatedAtMS != out[j].Task.UpdatedAtMS {
			return out[i].Task.UpdatedAtMS < out[j].Task.UpdatedAtMS
		}
		return out[i].Task.ID < out[j].Task.ID
	})
	return out, nil
}
