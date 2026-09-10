package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const ecsShutdownBudget = 15 * time.Second
const ecsDrainRetry = 250 * time.Millisecond

// This drains the files observable at shutdown, NOT a proof that a producer
// has stopped. Authoritative producer state and final manifests are separate
// release gates. Existing Lightsail shutdown does not enter this path.
func shutdownECSCollector(c config, workers *sync.WaitGroup) error {
	ctx, cancel := context.WithTimeout(context.Background(), ecsShutdownBudget)
	defer cancel()
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return errors.New("ECS collector workers did not stop; final drain unverified")
	}
	return drainECSCollector(ctx, c)
}

func drainECSCollector(ctx context.Context, c config) error {
	if c.ecsSocket == "" || c.sourceV2Prepare || c.sourceV2Access || c.sourceV2Error {
		return errors.New("ECS drain requires private legacy-lane transport")
	}
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("ECS final drain incomplete: %w", errors.Join(err, lastErr))
		}
		done, err := drainECSOnce(ctx, c)
		if done && err == nil {
			return nil
		}
		lastErr = err
		timer := time.NewTimer(ecsDrainRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func drainECSOnce(ctx context.Context, c config) (bool, error) {
	if err := runOnce(ctx, c); err != nil {
		return false, err
	}
	if c.errorEnabled {
		if err := runErrorOnce(ctx, c); err != nil {
			return false, err
		}
	}
	if c.evidenceMode != "off" {
		if !c.evidenceFrozenHeartbeat {
			return false, errors.New("ECS drain requires frozen evidence heartbeat")
		}
		// Finish a previously frozen heartbeat before later archive dependencies.
		state, err := loadEvidenceHeartbeatState(c.cursorPath+".evidence-heartbeat.json", c.node)
		if err != nil {
			return false, err
		}
		if state.Pending != nil {
			if err := deliverFrozenEvidenceHeartbeat(ctx, c, time.Now()); err != nil {
				return false, err
			}
		}
		if err := drainEvidenceOnce(ctx, c); err != nil {
			return false, err
		}
		paths, err := listEvidenceOutbox(c.evidenceOutboxPath)
		if err != nil {
			return false, err
		}
		if len(paths) != 0 {
			return false, nil
		}
		gap, err := loadGapState(c.evidenceOutboxPath)
		if err != nil {
			return false, err
		}
		if gap.GapCount != 0 {
			return false, errors.New("evidence contains recorded gaps; shutdown not complete")
		}
	}
	sources := [][2]string{{c.logPath, c.cursorPath}}
	if c.errorEnabled {
		sources = append(sources, [2]string{c.errorLogPath, c.errorCursorPath})
	}
	for _, paths := range sources {
		if err := ecsObservedEOF(paths[0], paths[1]); err != nil {
			return false, err
		}
	}
	return true, nil
}

func ecsObservedEOF(logPath, cursorPath string) error {
	cur, err := loadCursor(cursorPath)
	if err != nil {
		return err
	}
	info, err := os.Stat(logPath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || cur.Inode != fileInode(info) || cur.Offset != info.Size() {
		return errors.New("observed log EOF not reached")
	}
	if cur.Discontinuities != 0 || cur.DiscardedLines != 0 || cur.EvidenceDroppedEvents != 0 || cur.EvidencePersistFailures != 0 {
		return errors.New("source has recorded gaps; EOF alone does not prove complete delivery")
	}
	return nil
}
