package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"time"
)

var recoveringTokenMonthSlots = make(chan struct{}, 1)

type recoveringTokenAction struct {
	ordinal int
	child   bool
	request UsageWindowRequest
}

func recoveringTokenProgress(s recoveringTokenInventory, now time.Time) (TokenMonthCaptureProgress, *recoveringTokenAction) {
	p := s.progress
	half := p.Total / 2
	firstMissing := false
	secondObserved := false
	var first *recoveringTokenAction
	noteWait := func(at int64) {
		if at > now.UnixMilli() && (p.NextEligibleMS == 0 || at < p.NextEligibleMS) {
			p.NextEligibleMS = at
		}
	}
	for i := 0; i < half; i++ {
		if d, ok := s.days[i]; ok {
			for slot := range d.state.records {
				if slot.phase == 1 {
					secondObserved = true
				}
			}
			olderDone := true
			for _, leaf := range d.state.leaves {
				if _, exists := d.state.records[adaptiveTokenSlot{adaptiveTokenNode{leaf.From, leaf.To}, 0}]; !exists {
					olderDone = false
				}
			}
			if olderDone {
				p.Completed++
			} else {
				firstMissing = true
				if first == nil && d.phase == 0 {
					first = &recoveringTokenAction{ordinal: d.ordinal, child: true, request: d.state.root.Request}
				}
			}
			if d.progress.Complete {
				p.Completed++
			}
			noteWait(d.progress.NextEligibleMS)
			continue
		}
		if _, ok := s.old[i]; ok {
			p.Completed++
		} else {
			firstMissing = true
			if first == nil {
				r, _ := tokenCaptureRequest(s.plan, i)
				first = &recoveringTokenAction{ordinal: i, request: r}
			}
		}
		if _, ok := s.old[i+half]; ok {
			p.Completed++
			secondObserved = true
		}
	}
	p.TraversalsFinished = p.Completed == p.Total
	if p.TraversalsFinished {
		p.NextEligibleMS = 0
		return p, nil
	}
	if firstMissing {
		if first != nil {
			p.NextEligibleMS = 0
		}
		return p, first
	}
	// Preserve the initial whole-first-sweep gap. Once an actual second
	// original exists, a late split only restarts its own independent gap;
	// it must not stall unrelated ready windows. Plans alone cannot unlock
	// this path, and missing first reads above still take priority.
	if at := s.oldEnd + 300000; !secondObserved && now.UnixMilli() < at {
		p.NextEligibleMS = at
		return p, nil
	}
	for i := 0; i < half; i++ {
		if d, ok := s.days[i]; ok {
			if !d.progress.Complete && d.phase >= 0 {
				p.NextEligibleMS = 0
				return p, &recoveringTokenAction{ordinal: d.ordinal, child: true, request: d.state.root.Request}
			}
		} else if _, ok := s.old[i+half]; !ok {
			if at := s.old[i].completed + 300000; now.UnixMilli() < at {
				noteWait(at)
				continue
			}
			r, _ := tokenCaptureRequest(s.plan, i+half)
			p.NextEligibleMS = 0
			return p, &recoveringTokenAction{ordinal: i + half, request: r}
		}
	}
	return p, nil
}

func InspectRecoveringTokenMonthCapture(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope) (TokenMonthCaptureProgress, error) {
	if parent == nil || db == nil {
		return TokenMonthCaptureProgress{}, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := ValidateRecoveringTokenMonthStore(ctx, db); err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := loadFundingMonthCapturePlan(ctx, tx, key, scope, time.Now())
	if err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	s, err := loadRecoveringTokenInventory(ctx, tx, plan, time.Now())
	if err != nil {
		return TokenMonthCaptureProgress{}, err
	}
	p, _ := recoveringTokenProgress(s, time.Now())
	return p, tx.Commit()
}

// Explicit opt-in replacement scheduler, one paced read-only capture per call.
// A classified failed daily read is handed off WITHOUT rereading that parent.
func CaptureNextRecoveringTokenMonthSlot(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope) (TokenMonthCaptureProgress, error) {
	return captureNextRecoveringTokenMonthSlot(parent, source, receiver, key, scope, adaptiveTokenCaptureOps{
		now: time.Now, capture: CaptureTokenUsageWindow, pace: func(ctx context.Context) error { return waitFundingInterval(ctx, time.Second) }, jointMonth: true,
	})
}

func captureNextRecoveringTokenMonthSlot(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope, ops adaptiveTokenCaptureOps) (TokenMonthCaptureProgress, error) {
	var empty TokenMonthCaptureProgress
	if parent == nil || source == nil || receiver == nil || source == receiver || reflect.TypeOf(source.Driver()) == reflect.TypeOf(receiver.Driver()) || ops.now == nil || ops.capture == nil || ops.pace == nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	select {
	case recoveringTokenMonthSlots <- struct{}{}:
		defer func() { <-recoveringTokenMonthSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	if err := ValidateRecoveringTokenMonthStore(ctx, receiver); err != nil {
		return empty, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, ops.now())
	if err != nil {
		return empty, err
	}
	s, err := loadRecoveringTokenInventory(ctx, receiver, plan, ops.now())
	if err != nil {
		return empty, err
	}
	p, action := recoveringTokenProgress(s, ops.now())
	if action == nil {
		return p, nil
	}
	used, err := adaptiveTokenRetainedBytes(ctx, receiver, true)
	if err != nil {
		return empty, err
	}
	if used >= tokenMonthCaptureBytes {
		return empty, ErrTokenMonthCaptureBudget
	}
	if action.child {
		ops.jointMonth = true
		a, err := advanceAdaptiveTokenWindow(ctx, source, receiver, key, action.request, ops)
		if err != nil {
			return empty, err
		}
		s, err = loadRecoveringTokenInventory(ctx, receiver, plan, ops.now())
		if err != nil {
			return empty, err
		}
		p, _ = recoveringTokenProgress(s, ops.now())
		p.Captured = a.Captured
		return p, nil
	}
	if action.ordinal >= p.Total/2 {
		// Check the existing same-day proof BEFORE querying a second view.
		if _, err := loadTokenCaptureSlot(ctx, receiver, plan, action.ordinal-p.Total/2); err != nil {
			return empty, err
		}
	}
	if err := ops.pace(ctx); err != nil {
		return empty, err
	}
	connectCtx, stop := context.WithTimeout(ctx, 4*time.Second)
	err = source.PingContext(connectCtx)
	stop()
	if err != nil {
		return empty, err
	}
	o, err := ops.capture(ctx, source, action.request)
	if err != nil {
		if parent.Err() != nil {
			return empty, parent.Err()
		}
		if ctx.Err() == nil && adaptiveTokenCanSplit(err) {
			if err := bindFailedTokenMonthWindow(ctx, receiver, plan, action.ordinal, ops.now()); err != nil {
				return empty, err
			}
			s, err = loadRecoveringTokenInventory(ctx, receiver, plan, ops.now())
			if err != nil {
				return empty, err
			}
			p, _ = recoveringTokenProgress(s, ops.now())
			return p, nil // local handoff counts a work step, not an observation
		}
		return empty, err
	}
	if err := saveRecoveringDailySlot(ctx, receiver, plan, action.ordinal, o, ops.now()); err != nil {
		return empty, err
	}
	s, err = loadRecoveringTokenInventory(ctx, receiver, plan, ops.now())
	if err != nil {
		return empty, err
	}
	p, _ = recoveringTokenProgress(s, ops.now())
	p.Captured = true
	return p, nil
}

func bindFailedTokenMonthWindow(ctx context.Context, db *sql.DB, plan FundingMonthCapturePlan, ordinal int, now time.Time) error {
	r, err := tokenCaptureRequest(plan, ordinal)
	if err != nil {
		return err
	}
	if _, err := SplitUsageWindow(r); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	s, err := loadRecoveringTokenInventory(ctx, tx, plan, now)
	if err != nil {
		return err
	}
	_, action := recoveringTokenProgress(s, now)
	if action == nil || action.child || action.ordinal != ordinal {
		return ErrTokenMonthCaptureIntegrity
	}
	if err := ensureAdaptiveTokenRoot(ctx, tx, plan.Key, r, now); err != nil {
		return err
	}
	_, _, root, err := adaptiveTokenIdentity(plan.Key, r)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO monthly_adaptive_token_splits(root_id,window_from,window_to,phase) VALUES(?,?,?,?)`, root, r.From, r.To, ordinal/(s.progress.Total/2)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO monthly_token_recovery_bindings(job_key,ordinal,root_id,created_at_ms) VALUES(?,?,?,?)`, plan.Key, ordinal, root, now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

func saveRecoveringDailySlot(ctx context.Context, db *sql.DB, plan FundingMonthCapturePlan, ordinal int, o TokenUsageWindowObservation, now time.Time) error {
	r, err := tokenCaptureRequest(plan, ordinal)
	if err != nil || o.Request != r || ValidateTokenUsageWindowObservation(o) != nil || o.CompletedAtMS > now.UnixMilli() {
		return ErrTokenMonthCaptureIntegrity
	}
	u, err := UsageObservationFromToken(o)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	projection, err := usageWindowBytes(u)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	s, err := loadRecoveringTokenInventory(ctx, tx, plan, now)
	if err != nil {
		return err
	}
	_, action := recoveringTokenProgress(s, now)
	if action == nil || action.child || action.ordinal != ordinal {
		return ErrTokenMonthCaptureIntegrity
	}
	used, err := adaptiveTokenRetainedBytes(ctx, tx, true)
	if err != nil {
		return err
	}
	if used > tokenMonthCaptureBytes-int64(len(raw)+len(projection)) {
		return ErrTokenMonthCaptureBudget
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, plan.Key, ordinal, r.UserID, digest(raw), digest(projection), o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_usage_observations(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, digest(projection), r.SourceIdentity, r.UserID, r.From, r.To, o.StartedAtMS, o.CompletedAtMS, projection); err != nil {
		return err
	}
	stored, err := loadTokenCaptureSlot(ctx, tx, plan, ordinal)
	if err != nil {
		return err
	}
	got, _ := json.Marshal(stored)
	if !bytes.Equal(raw, got) {
		return ErrTokenMonthCaptureIntegrity
	}
	changed := false
	if ordinal >= s.progress.Total/2 {
		older, err := loadTokenCaptureSlot(ctx, tx, plan, ordinal-s.progress.Total/2)
		if err != nil {
			return err
		}
		changed = !TokenUsageWindowsMatch(older, o)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if changed {
		return ErrTokenMonthCaptureIntegrity
	} // retain both originals
	return nil
}
