package ecslogagent

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

const archiveClosureBudget = 8 * time.Second

// Called only after SIGTERM and a successful collector drain/exit. A crash,
// forced kill, expired lease, failed upload, or unfinished request never seals
// the chain. Missing seals stay visible; they cannot be invented on recovery.
func (a *Agent) closeArchive(ctx context.Context) error {
	return a.closeArchiveWithBoundaries(ctx, nil)
}

func (a *Agent) closeArchiveWithBoundaries(ctx context.Context, boundaries map[string]*ecsarchive.FinalBoundary) error {
	if !a.cfg.ArchiveClosure {
		return nil
	}
	if a.archive == nil || a.inflight.Load() != 0 {
		return errors.New("collector closure requires archived, drained requests")
	}
	a.archiveMu.Lock()
	defer a.archiveMu.Unlock()
	for _, lane := range a.cfg.lanes() {
		if a.lease(lane) <= a.now().Add(archiveLeaseMargin).Unix() {
			return errors.New("collector closure lease unavailable")
		}
		state, err := a.readArchiveJournal()
		if err != nil {
			return err
		}
		previous := state.Heads[lane]
		boundary := boundaries[lane]
		if state.Closed[lane] != "" {
			previous = state.Previous[lane+"/"+state.Closed[lane]]
			boundary = state.Boundaries[lane]
		}
		e, err := ecsarchive.SealBoundaryClosure(a.cfg.Audience, a.node, lane, previous, boundary, a.key)
		if err != nil {
			return err
		}
		if state.Closed[lane] == "" {
			if len(state.Previous) >= archiveJournalEntries {
				return errors.New("archive journal full; closure unconfirmed")
			}
			if state.Closed == nil {
				state.Closed = map[string]string{}
			}
			state.Closed[lane], state.Heads[lane] = e.Hash, e.Hash
			if boundary != nil {
				if state.Boundaries == nil {
					state.Boundaries = map[string]*ecsarchive.FinalBoundary{}
				}
				state.Boundaries[lane] = boundary
			}
			state.Previous[lane+"/"+e.Hash] = previous
			if err := a.saveArchiveJournal(state); err != nil {
				return err
			}
		} else if state.Closed[lane] != e.Hash || state.Heads[lane] != e.Hash {
			return errors.New("archive closure journal inconsistent")
		}
		if err := a.putArchiveEnvelope(ctx, lane, e); err != nil {
			return err
		}
	}
	return nil
}
