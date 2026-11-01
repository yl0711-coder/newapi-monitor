package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Explicit offline setup only. Startup/HTTP never create or migrate these
// tables. Bindings record real classified failed daily requests, not money.
func InitRecoveringTokenMonthStore(ctx context.Context, db *sql.DB) error {
	if err := InitTokenMonthCaptureStore(ctx, db); err != nil {
		return err
	}
	if err := InitAdaptiveTokenCaptureStore(ctx, db); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS monthly_token_recovery_bindings(job_key TEXT NOT NULL CHECK(length(job_key)=64),ordinal INTEGER NOT NULL CHECK(ordinal>=0 AND ordinal<12400),root_id TEXT NOT NULL UNIQUE CHECK(length(root_id)=64),created_at_ms INTEGER NOT NULL CHECK(created_at_ms>0),PRIMARY KEY(job_key,ordinal))`,
		`CREATE TRIGGER IF NOT EXISTS token_recovery_binding_no_update BEFORE UPDATE ON monthly_token_recovery_bindings BEGIN SELECT RAISE(ABORT,'token recovery bindings are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS token_recovery_binding_no_delete BEFORE DELETE ON monthly_token_recovery_bindings BEGIN SELECT RAISE(ABORT,'token recovery bindings are immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Local schema presence check, not initialization or a source connection.
func ValidateRecoveringTokenMonthStore(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return ErrTokenMonthCaptureIntegrity
	}
	var tables, triggers int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('portal_monthly_token_capture_slots','portal_monthly_usage_observations','portal_monthly_funding_capture_plans','monthly_adaptive_token_roots','monthly_adaptive_token_splits','monthly_adaptive_token_observations','monthly_adaptive_token_faults','monthly_token_recovery_bindings')`).Scan(&tables); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('token_recovery_binding_no_update','token_recovery_binding_no_delete','adaptive_token_roots_no_UPDATE','adaptive_token_roots_no_DELETE','adaptive_token_splits_no_UPDATE','adaptive_token_splits_no_DELETE','adaptive_token_observations_no_UPDATE','adaptive_token_observations_no_DELETE','adaptive_token_faults_no_UPDATE','adaptive_token_faults_no_DELETE')`).Scan(&triggers); err != nil {
		return err
	}
	if tables != 8 || triggers != 10 {
		return ErrTokenMonthCaptureIntegrity
	}
	return ctx.Err()
}

func adaptiveTokenRetainedBytes(ctx context.Context, db walletRowReader, joint bool) (int64, error) {
	var used int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(length(payload)+length(projection)),0) FROM monthly_adaptive_token_observations`).Scan(&used); err != nil {
		return 0, err
	}
	enabled, err := adaptiveTokenResumeSchema(ctx, db)
	if err != nil {
		return 0, err
	}
	if enabled {
		var audit int64
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(length(payload)),0) FROM monthly_adaptive_token_resumptions`).Scan(&audit); err != nil {
			return 0, err
		}
		if audit < 0 || used > adaptiveTokenBytes-audit {
			return 0, ErrAdaptiveTokenBudget
		}
		used += audit
	}
	if joint {
		var old int64
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(length(t.payload)+COALESCE(length(u.payload),0)),0) FROM portal_monthly_token_capture_slots t LEFT JOIN portal_monthly_usage_observations u ON t.usage_id=u.id`).Scan(&old); err != nil {
			return 0, err
		}
		if old < 0 || used < 0 || old > tokenMonthCaptureBytes-used {
			return 0, ErrTokenMonthCaptureBudget
		}
		used += old
	}
	return used, nil
}

type recoveringTokenSlot struct {
	user, started, completed int64
}
type recoveringTokenDay struct {
	ordinal  int // original failed ordinal, not a new source observation
	state    adaptiveTokenState
	progress AdaptiveTokenWindowProgress
	phase    int
}
type recoveringTokenInventory struct {
	plan     FundingMonthCapturePlan
	old      map[int]recoveringTokenSlot
	days     map[int]recoveringTokenDay // first-sweep ordinal, one per failed day/user
	progress TokenMonthCaptureProgress
	oldEnd   int64
}

func loadRecoveringTokenInventory(ctx context.Context, db adaptiveTokenReader, plan FundingMonthCapturePlan, now time.Time) (recoveringTokenInventory, error) {
	return loadRecoveringTokenInventoryMode(ctx, db, plan, now, false)
}

// Only the stopped-owner recovery planner may inspect stopped roots. Normal
// source scheduling keeps its existing hard-stop behavior.
func loadRecoveringTokenInventoryMode(ctx context.Context, db adaptiveTokenReader, plan FundingMonthCapturePlan, now time.Time, inspectStopped bool) (recoveringTokenInventory, error) {
	out := recoveringTokenInventory{plan: plan, old: map[int]recoveringTokenSlot{}, days: map[int]recoveringTokenDay{},
		progress: TokenMonthCaptureProgress{Mode: "recovered_original_token_windows_not_bill_approval", Total: len(plan.Scans) * len(plan.Scope.Funding.UserIDs)}}
	half := out.progress.Total / 2
	rows, err := db.QueryContext(ctx, `SELECT ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms FROM portal_monthly_token_capture_slots WHERE job_key=? ORDER BY ordinal LIMIT 12401`, plan.Key)
	if err != nil {
		return out, err
	}
	maxOrdinal := -1
	for rows.Next() {
		var i int
		var o recoveringTokenSlot
		var sha, usage string
		if err := rows.Scan(&i, &o.user, &sha, &usage, &o.started, &o.completed); err != nil {
			_ = rows.Close()
			return out, err
		}
		r, err := tokenCaptureRequest(plan, i)
		if err != nil || o.user != r.UserID || !validDigest(sha) || !validDigest(usage) || o.started <= 0 || o.completed < o.started || o.completed > now.UnixMilli() {
			_ = rows.Close()
			return out, ErrTokenMonthCaptureIntegrity
		}
		out.old[i], maxOrdinal = o, i
		if i < half {
			out.oldEnd = max(out.oldEnd, o.completed)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return out, err
	}
	rows, err = db.QueryContext(ctx, `SELECT ordinal,root_id,created_at_ms FROM monthly_token_recovery_bindings WHERE job_key=? ORDER BY ordinal LIMIT 129`, plan.Key)
	if err != nil {
		return out, err
	}
	// Close the rows BEFORE loading roots on the same single connection.
	type binding struct {
		ordinal int
		id      string
		at      int64
	}
	bindings := []binding{}
	for rows.Next() {
		var b binding
		if err := rows.Scan(&b.ordinal, &b.id, &b.at); err != nil {
			_ = rows.Close()
			return out, err
		}
		bindings = append(bindings, b)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return out, err
	}
	if len(bindings) > adaptiveTokenJobs {
		return out, ErrTokenMonthCaptureBudget
	}
	rootIDs := map[string]bool{}
	for _, b := range bindings {
		r, err := tokenCaptureRequest(plan, b.ordinal)
		if err != nil || b.at <= 0 || b.at > now.UnixMilli() {
			return out, ErrTokenMonthCaptureIntegrity
		}
		_, _, id, err := adaptiveTokenIdentity(plan.Key, r)
		first := b.ordinal % half
		_, duplicate := out.days[first]
		_, failedSlotExists := out.old[b.ordinal]
		_, olderExists := out.old[first]
		_, newerExists := out.old[first+half]
		if err != nil || id != b.id || duplicate || failedSlotExists || newerExists || (b.ordinal < half && olderExists) || (b.ordinal >= half && !olderExists) {
			return out, ErrTokenMonthCaptureIntegrity
		}
		s, err := loadAdaptiveTokenStateMode(ctx, db, plan.Key, r, now, inspectStopped)
		if err != nil {
			return out, err
		}
		var initialPhase int
		if err := db.QueryRowContext(ctx, `SELECT phase FROM monthly_adaptive_token_splits WHERE root_id=? AND window_from=? AND window_to=?`, id, r.From, r.To).Scan(&initialPhase); err != nil || initialPhase != b.ordinal/half || s.created != b.at {
			return out, ErrTokenMonthCaptureIntegrity
		}
		p, _, phase := adaptiveTokenProgress(s, now)
		if phase == -2 {
			return out, ErrTokenMonthCaptureIntegrity
		}
		if !p.Complete && now.UnixMilli()-s.created > int64((24*time.Hour)/time.Millisecond) {
			return out, ErrAdaptiveTokenBudget
		}
		if olderExists {
			parent, err := loadTokenCaptureSlot(ctx, db, plan, first)
			if err != nil {
				return out, err
			}
			if p.Complete {
				pairs, err := loadAdaptiveTokenPairs(ctx, db, s)
				if err != nil || !tokenPartitionMatchesParent(parent, pairs) {
					return out, errors.Join(ErrTokenMonthCaptureIntegrity, err)
				}
			}
		} else if p.Complete {
			if _, err := loadAdaptiveTokenPairs(ctx, db, s); err != nil {
				return out, err
			}
		}
		for _, leaf := range s.leaves {
			if record, ok := s.records[adaptiveTokenSlot{adaptiveTokenNode{leaf.From, leaf.To}, 0}]; ok {
				out.oldEnd = max(out.oldEnd, record.completed)
			}
		}
		out.days[first] = recoveringTokenDay{ordinal: b.ordinal, state: s, progress: p, phase: phase}
		rootIDs[id] = true
	}
	// An unbound root is not permission to bypass a missing daily original.
	rows, err = db.QueryContext(ctx, `SELECT id,payload FROM monthly_adaptive_token_roots LIMIT 129`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id string
		var raw []byte
		var root adaptiveTokenRoot
		if err := rows.Scan(&id, &raw); err != nil {
			return out, err
		}
		count++
		if count > adaptiveTokenJobs || len(raw) > 2048 || json.Unmarshal(raw, &root) != nil {
			return out, ErrAdaptiveTokenIntegrity
		}
		if root.JobKey == plan.Key && !rootIDs[id] {
			return out, ErrTokenMonthCaptureIntegrity
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return out, err
	}
	if err := rows.Close(); err != nil {
		return out, err
	}
	for i := 0; i <= maxOrdinal; i++ {
		if _, exists := out.old[i]; !exists {
			if _, bound := out.days[i%half]; !bound {
				return out, ErrTokenMonthCaptureIntegrity
			}
		}
	}
	if len(out.days) == 0 {
		if _, err := tokenCaptureInventory(ctx, db, plan.Key, out.progress.Total, now); err != nil {
			return out, err
		}
	}
	// Like the legacy inspector, catch a retained changed second view before
	// another source operation. Full all-day audits remain the seal's job.
	if maxOrdinal >= half {
		a, err := loadTokenCaptureSlot(ctx, db, plan, maxOrdinal-half)
		if err != nil {
			return out, err
		}
		b, err := loadTokenCaptureSlot(ctx, db, plan, maxOrdinal)
		if err != nil || !TokenUsageWindowsMatch(a, b) {
			return out, errors.Join(ErrTokenMonthCaptureIntegrity, err)
		}
	}
	return out, nil
}

func rejectBoundTokenRecovery(ctx context.Context, db walletRowReader, key string) error {
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_token_recovery_bindings'`).Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		var bound int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM monthly_token_recovery_bindings WHERE job_key=?`, key).Scan(&bound); err != nil {
			return err
		}
		if bound != 0 {
			return ErrTokenMonthCaptureIntegrity
		}
	}
	return nil
}
