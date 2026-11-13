package monitor

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errTokenMeteringLease = errors.New("token facts lease lost")
var errTokenMeteringCapacity = errors.New("token facts capacity limit")
var errTokenMeteringConnectionWarm = errors.New("token source connection warmed; resume query next turn")

func ensureTokenMeteringLaneStates(tx *gorm.DB, epoch string, now int64) error {
	lanes := []TokenMeteringLaneState{
		{SourceEpoch: epoch, Lane: tokenMeteringLaneRealtimeTail, UpdatedAt: now},
		{SourceEpoch: epoch, Lane: tokenMeteringLaneForwardCatchup, UpdatedAt: now},
		{SourceEpoch: epoch, Lane: tokenMeteringLaneReplayRepair, UpdatedAt: now},
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&lanes).Error
}

func tokenMeteringLaneStateUpdate(tx *gorm.DB, w TokenMeteringWindow, scanThrough, now int64, values map[string]any) error {
	lane := tokenMeteringWindowLane(w, scanThrough)
	if lane == "" {
		return nil
	}
	values = mapsClone(values)
	values["updated_at"] = now
	return tx.Model(&TokenMeteringLaneState{}).
		Where("source_epoch=? AND lane=?", w.SourceEpoch, lane).Updates(values).Error
}

// mapsClone keeps the lane projection helper from mutating a caller's update
// map when a write transaction adds its durable timestamp.
func mapsClone(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Keep the first durable transaction small enough to fit while the retained
// store is also migrating and detecting. Larger source pages still share one
// local budget and resume at the last committed cursor.
const tokenMeteringCommitChunk = 8

// A slow transport can deliver fewer than sixteen rows within four seconds.
// Keep shrinking down to one local commit chunk instead of retrying a page
// that can never finish; source query concurrency and timeout stay unchanged.
const tokenMeteringMinReadRows = 8

type tokenMeteringReadError struct {
	err          error
	queryStarted bool
	stage        string
	frontier     *tokenMeteringFrontier
}

func (e *tokenMeteringReadError) Error() string { return e.err.Error() }
func (e *tokenMeteringReadError) Unwrap() error { return e.err }

type tokenMeteringFrontier struct {
	ScanThrough, NextAt    int64
	RecentStart, RecentEnd int64
	RecentDone             bool
	RecentNextAt           int64
}

// A local budget can commit only a prefix of a successfully read source page.
// Keep its bounded suffix briefly instead of paying another shared source turn
// for the same rows. Durable cursor/lease fencing remains authoritative; losing
// this optional buffer on restart simply resumes the ordinary source query.
type tokenMeteringCachedRead struct {
	Window     TokenMeteringWindow
	Rows       []tokenMeteringRaw
	Done       bool
	ReadTiming backgroundSourceReadTiming
	// A source timeout may have delivered a valid prefix before the local
	// writer budget expired. Keep its durable failure classification beside the
	// in-memory suffix so the prefix is committed first and the retry/backoff is
	// recorded afterwards; never turn that source failure into local_unavailable.
	FailureReason        string
	NextReadRows         int
	NextReadSuccessPages int
	NextReadWorstMS      int64
	ObservedAt           int64
	ExpiresAt            time.Time
}

func (m *Monitor) takeTokenMeteringCachedRead(w TokenMeteringWindow, now int64) *tokenMeteringCachedRead {
	page := m.tokenMeteringReadCache
	if page == nil {
		return nil
	}
	if !time.Now().Before(page.ExpiresAt) || now < page.ObservedAt {
		m.tokenMeteringReadCache = nil
		return nil
	}
	old := page.Window
	if old.SourceEpoch != w.SourceEpoch || old.StartAt != w.StartAt || old.EndAt != w.EndAt {
		return nil // A reserved replay turn must not discard the forward suffix.
	}
	m.tokenMeteringReadCache = nil
	if old.CursorAt != w.CursorAt || old.CursorID != w.CursorID || old.Replay != w.Replay || w.Done ||
		tokenMeteringReadPageRows(old.ReadPageRows) != tokenMeteringReadPageRows(w.ReadPageRows) {
		return nil
	}
	return page
}

func readTokenMeteringFrontier(tx *gorm.DB, epoch string) (*tokenMeteringFrontier, error) {
	var frontier tokenMeteringFrontier
	result := tx.Raw(`SELECT s.scan_through,COALESCE(w.next_at,0) AS next_at,
		COALESCE(r.start_at,0) AS recent_start,COALESCE(r.end_at,0) AS recent_end,
		COALESCE(r.done,0) AS recent_done,COALESCE(r.next_at,0) AS recent_next_at FROM token_metering_states s
		LEFT JOIN token_metering_windows w ON w.source_epoch=s.source_epoch AND w.start_at=s.scan_through
		LEFT JOIN token_metering_windows r ON r.source_epoch=s.source_epoch AND r.start_at=(
			SELECT MAX(start_at) FROM token_metering_windows WHERE source_epoch=s.source_epoch AND start_at>s.scan_through)
		WHERE s.source_epoch=?`, epoch).Scan(&frontier)
	if result.Error != nil || result.RowsAffected != 1 {
		return nil, result.Error
	}
	return &frontier, nil
}

func (f *tokenMeteringFrontier) delay(now int64) time.Duration {
	sealed := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	var delay time.Duration
	if f.ScanThrough < sealed {
		if f.NextAt <= now {
			delay = 2 * time.Second
		} else {
			delay = min(time.Minute, time.Duration(f.NextAt-now)*time.Second)
		}
	} else {
		untilSealed := f.ScanThrough + tokenMeteringWindowSeconds + tokenMeteringLagSeconds - now
		delay = min(time.Minute, time.Duration(max(1, untilSealed))*time.Second)
	}
	if tokenMeteringNeedsRecent(f.ScanThrough, now) {
		recentDue := now + 2
		if f.RecentStart > f.ScanThrough {
			if !f.RecentDone {
				recentDue = max(recentDue, f.RecentNextAt)
			} else if f.RecentEnd >= sealed {
				recentDue = f.RecentEnd + tokenMeteringWindowSeconds + tokenMeteringLagSeconds
			}
		}
		delay = min(delay, time.Duration(max(1, recentDue-now))*time.Second)
	}
	// A historical backoff or seal boundary never sleeps across new arrivals.
	return min(delay, time.Duration(tokenMeteringTailPollSeconds)*time.Second)
}

func tokenMeteringBudgetExpired(ctx, parent context.Context, err error) bool {
	if err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) || parent.Err() != nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, sql.ErrTxDone) {
		return true // database/sql may finish the transaction on its expired context.
	}
	// SQLite can return SQLITE_INTERRUPT (9) without wrapping the context.
	// Require both the actual local deadline and that specific driver code;
	// unrelated SQL failures must never become successful budget checkpoints.
	var coded interface{ Code() int }
	return errors.As(err, &coded) && coded.Code() == 9
}

func tokenMeteringReadPageRows(rows int) int {
	if rows <= 0 || rows > tokenMeteringPageRows {
		return tokenMeteringPageRows
	}
	return max(tokenMeteringMinReadRows, rows)
}

// Three committed full pages provide a throughput sample for this window.
// Keep 20% of the unchanged four-second transport allowance in reserve and
// size from the slowest sample. A two-second tunnel round trip must not pin
// every recovered window at eight rows indefinitely. Neither a timeout prefix
// nor an uncommitted suffix contributes a successful sample.
func tokenMeteringReadFeedback(w TokenMeteringWindow, rows int, elapsed time.Duration) (int, int, int64) {
	width := tokenMeteringReadPageRows(w.ReadPageRows)
	const safeMS int64 = 3200
	if rows != width || elapsed > time.Duration(safeMS)*time.Millisecond {
		return width, 0, 0
	}
	worst := max(w.ReadWorstMS, elapsed.Milliseconds(), 1)
	samples := w.ReadSuccessPages + 1
	if samples < 3 {
		return width, samples, worst
	}
	measured := int(int64(width) * safeMS / worst)
	return max(width, min(tokenMeteringPageRows, width+max(1, width/2), measured)), 0, 0
}

func (m *Monitor) startTokenMetering(ctx context.Context) {
	if !m.cfg.TokenFactsEnabled || m.cfg.LocalSnapshotOnly {
		return
	}
	for {
		pages, err := m.runTokenMeteringRound(ctx, time.Now())
		if err != nil && !errors.Is(err, context.Canceled) {
			// Source errors can contain SQL parameters; only emit the fixed category.
			slog.Warn("token facts round incomplete", "reason", "source_or_local_failure")
		}
		if !waitSourceLifecycle(ctx, m.tokenMeteringRoundDelay(ctx, pages, err, time.Now().Unix())) {
			return
		}
	}
}

// Catch-up removes idle time between bounded rounds. Every source query still
// uses the shared low-priority gate, existing start spacing, SQL and page caps.
// A spent round budget is resumable work, including when it was spent waiting
// for the gate. Actual source/local failures retain their window backoff.
func (m *Monitor) tokenMeteringRoundDelay(ctx context.Context, pages int, runErr error, now int64) time.Duration {
	if ctx.Err() != nil || m.tokenReadStore() == nil {
		return time.Minute
	}
	if runErr != nil {
		// A bounded local writer/connection wait is a scheduling yield, not a
		// failed source window.  Claiming a window or persisting its retry
		// checkpoint can time out before any transaction work starts; treating
		// that error as an ordinary local failure would put collection to sleep
		// for a full minute while the same durable cursor is still runnable.
		// Keep the existing short suffix cadence when a page prefix was already
		// committed, and otherwise leave a small interval for the contended
		// writer to hand off.  The next claim still enforces every durable
		// next_at/lease fence, so this cannot bypass source backoff.
		var waitErr *tokenLocalWaitError
		if errors.As(runErr, &waitErr) {
			if m.tokenMeteringPartialPage.Load() || pages > 0 {
				return 100 * time.Millisecond
			}
			return 2 * time.Second
		}
		// A source failure belongs to its persisted window backoff. If a
		// forward window is runnable, do not add a global minute of
		// idle time to that independent work. A replay can spend the entire
		// round before the forward lane gets a slot. Joined local/persistence
		// errors and gate failures keep the existing slow cadence.
		sourceErr, sourceOnly := runErr.(*tokenMeteringReadError) //nolint:errorlint // Joined persistence failures must not receive the source-only fast retry cadence.
		if !sourceOnly || !sourceErr.queryStarted {
			return time.Minute
		}
		if sourceErr.frontier != nil {
			// This round already committed the failure and its retry deadline.
			// Do not queue for the same state again behind maintenance writers.
			// Later successful work can make this hint wake early; selecting the
			// next window still checks its durable next_at before any source read.
			return sourceErr.frontier.delay(now)
		}
	}
	if pages > 0 && runErr == nil {
		if m.tokenMeteringPartialPage.Load() {
			// A source/local budget yield is not a failed-source retry. Resume its committed
			// cursor promptly; the shared source gate still enforces the same
			// global start spacing, duty cooldown and query concurrency.
			return 100 * time.Millisecond
		}
		return 2 * time.Second
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	var frontier *tokenMeteringFrontier
	// Reports can occupy both snapshot readers. This tiny scheduling read
	// joins the fair local queue instead of waiting behind a whole report and
	// turning a runnable forward window into a global minute of idle time.
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		frontier, err = readTokenMeteringFrontier(tx, m.cfg.TokenFactsSourceEpoch)
		return err
	})
	if err == nil && frontier != nil {
		return frontier.delay(now)
	}
	if runErr == nil && m.cfg.TokenFactsEnabled && m.sourceAccessAllowed() && tokenMeteringBudgetExpired(ctx, parent, err) {
		// A zero-page budget yield has no failed source window to back off.
		// Contention while re-reading its schedule must not add a global idle
		// minute. The next claim still enforces every persisted next_at/lease
		// before issuing SQL, so an early wake cannot bypass source backoff.
		return 2 * time.Second
	}
	return time.Minute
}

func (m *Monitor) claimTokenMetering(ctx context.Context, now int64, completedLease ...string) (TokenMeteringState, bool, error) {
	epoch := m.cfg.TokenFactsSourceEpoch
	lookback := m.cfg.TokenFactsInitialLookbackSeconds
	if lookback == 0 {
		lookback = 86400
	}
	// This only seeds a new epoch. An existing unfinished window always resumes
	// its persisted cursor, regardless of later configuration changes.
	state := TokenMeteringState{SourceEpoch: epoch, StartAt: (now - int64(lookback)) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds, SourceMode: m.cfg.TokenFactsSourceMode}
	state.ScanThrough = state.StartAt
	claimed := false
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&state).Error; err != nil {
			return err
		}
		token := randomSecret()
		claim := tx.Model(&TokenMeteringState{}).Where("source_epoch = ?", epoch)
		if len(completedLease) > 0 && completedLease[0] != "" {
			// Only the same serialized, already-finished local owner can renew
			// a lease whose bounded release failed. A restart or another worker
			// has no such proof; a changed token always fences this owner out.
			claim = claim.Where("lease_until <= ? OR lease_token = ?", now, completedLease[0])
		} else {
			claim = claim.Where("lease_until <= ?", now)
		}
		result := claim.Updates(map[string]any{"lease_token": token, "lease_until": now + 30})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		if err := tx.First(&state, "source_epoch = ?", epoch).Error; err != nil {
			return err
		}
		if err := ensureTokenMeteringLaneStates(tx, epoch, now); err != nil {
			return err
		}
		profiles, err := m.cfg.tokenMeteringPinnedKeys(state.HMACProfiles)
		if err != nil {
			return err
		}
		if state.SourceMode != m.cfg.TokenFactsSourceMode || profiles != state.HMACProfiles {
			if state.SourceMode != m.cfg.TokenFactsSourceMode {
				state.CompleteThrough = 0
			}
			state.SourceMode = m.cfg.TokenFactsSourceMode
			state.HMACProfiles = profiles
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
		}
		// Reconcile old checkpoints too: an interrupted replay is never complete.
		if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
			return err
		}
		// An older binary may already have accumulated source-read failures.
		// Seed the new page-width checkpoint from that durable history instead
		// of replaying the same oversized page once per long retry interval.
		// Existing next_at/backoff values and all coverage remain untouched.
		if state.ReadPageRows == 0 {
			state.ReadPageRows = tokenMeteringPageRows
			if state.LastReason == "source_read_failed" || state.LastReason == "retry_exhausted" {
				var attempts int
				if err := tx.Model(&TokenMeteringWindow{}).Where("source_epoch=? AND start_at=? AND done=0 AND last_reason IN ?", epoch, state.ScanThrough, []string{"source_read_failed", "retry_exhausted"}).Select("COALESCE(MAX(attempts),0)").Scan(&attempts).Error; err != nil {
					return err
				}
				if attempts > 0 {
					state.ReadPageRows = max(tokenMeteringMinReadRows, tokenMeteringPageRows>>min(attempts, 5))
				}
			}
		}
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return state, claimed, err
}
func tokenMeteringFence(tx *gorm.DB, epoch, token string, now int64) (TokenMeteringState, error) {
	var state TokenMeteringState
	if err := tx.First(&state, "source_epoch = ?", epoch).Error; err != nil {
		return state, err
	}
	if state.LeaseToken != token || state.LeaseUntil <= now {
		return state, errTokenMeteringLease
	}
	return state, nil
}
func (m *Monitor) runTokenMeteringRound(parent context.Context, at time.Time) (processedPages int, runErr error) {
	parent = context.WithValue(parent, tokenWriteOperationKey{}, "collection")
	if !m.cfg.TokenFactsEnabled || m.cfg.LocalSnapshotOnly || m.tokenStore() == nil || m.prodDB == nil || !m.sourceAccessAllowed() {
		return 0, nil
	}
	if !m.tokenMeteringMu.TryLock() {
		return 0, nil
	}
	defer m.tokenMeteringMu.Unlock()
	m.tokenMeteringPartialPage.Store(false)
	if err := validateTokenMeteringSettings(m.cfg); err != nil {
		return 0, err
	}
	// Each lane has its own source allowance. The four-read cap and hard
	// twenty-five-second round also bound shared-pool occupancy. Local commits
	// remain independently bounded and hold no source slot.
	roundDeadline := time.Now().Add(25 * time.Second)
	roundCtx, cancelRound := context.WithDeadline(parent, roundDeadline)
	defer cancelRound()
	sourceBudgets := map[tokenMeteringLane]time.Duration{
		tokenMeteringLaneRecent:  tokenMeteringLaneBudget(tokenMeteringLaneRecent),
		tokenMeteringLaneForward: tokenMeteringLaneBudget(tokenMeteringLaneForward),
		tokenMeteringLaneReplay:  tokenMeteringLaneBudget(tokenMeteringLaneReplay),
	}
	// Source time is charged from timing.Query below. Local queue/commit time
	// must not expire a second, shorter wall clock disguised as that budget.
	ctx := roundCtx
	waited := map[tokenMeteringLane]time.Duration{}
	var sourceFailures error
	var sourceFrontier *tokenMeteringFrontier
	defer func() {
		if sourceFailures != nil {
			// Notify only after local checkpoints have finished. Cancelling the
			// epoch in the middle of a partial-page commit would lose progress.
			m.reportSourceQueryError(sourceFailures)
			if runErr == nil {
				runErr = &tokenMeteringReadError{err: sourceFailures, queryStarted: true, frontier: sourceFrontier}
			} else {
				runErr = errors.Join(sourceFailures, runErr)
			}
		}
	}()
	// Finishing the shared round budget is a checkpoint, not a failed source
	// window. Backing off here would repeatedly delay already progressing work.
	// Caller cancellation and the shorter per-query timeout remain failures.
	budgetFinished := func(err error) bool {
		return tokenMeteringBudgetExpired(ctx, parent, err)
	}
	now := at.Unix()
	state, claimed, err := m.claimTokenMetering(ctx, now, m.tokenMeteringFinishedLease)
	if err != nil || !claimed {
		return 0, err
	}
	laneRuns := []*tokenMeteringLaneRun{}
	defer func() {
		m.tokenMeteringFinishedLease = state.LeaseToken
		// Lease release belongs to the collection lane.  The previous
		// background context entered the maintenance FIFO as an anonymous
		// writer and routinely timed out behind association/detection, leaving
		// the durable lease occupied until its expiry even after a successful
		// round.  Keep the release bounded, but give the source lane the same
		// priority used by its checkpoint commits.
		cleanupBase := context.WithValue(context.Background(), tokenWriteOperationKey{}, "collection")
		cleanupCtx, cleanupCancel := context.WithTimeout(cleanupBase, 2*time.Second)
		defer cleanupCancel()
		// Expiration still guarantees recovery for a restart or another process.
		// This finished owner can resume without sleeping out its own lease.
		if err := m.tokenLocalWriteTransaction(cleanupCtx, func(tx *gorm.DB) error {
			fence := tx.Model(&TokenMeteringState{}).Where("source_epoch=? AND lease_token=?", state.SourceEpoch, state.LeaseToken).Update("lease_until", 0)
			if fence.Error != nil {
				return fence.Error
			}
			if fence.RowsAffected != 1 {
				return errTokenMeteringLease
			}
			if err := tokenMeteringReleaseLanes(tx, state.SourceEpoch, state.LeaseToken, now, laneRuns); err != nil {
				return err
			}
			return nil
		}); err != nil {
			slog.Warn("token facts lease release deferred", "deadline", errors.Is(cleanupCtx.Err(), context.DeadlineExceeded))
		}
	}()
	pages := 0
	fullReadBudgetAvailable := func(lane tokenMeteringLane) bool {
		// Only our own scheduling limits cause a successful yield. A shorter
		// caller deadline still cancels the gate/read through roundCtx and
		// must propagate to the caller rather than become a successful no-op.
		// SQLite claim/commit time does not consume the source-only budget;
		// otherwise a one-second local writer wait prevents even the first
		// healthy read. The hard round deadline still bounds all these phases.
		return sourceBudgets[lane] >= 4*time.Second && time.Until(roundDeadline) >= 4*time.Second
	}
	// Count source turns, not local-only suffix flushes, for replay fairness.
	// Buffered rows must commit before a potentially slow historical read can
	// consume their lifetime. Keep the reserved replay immediately after that
	// flush; a partial local flush does not spend its source-turn reservation.
	sourceTurn := m.tokenMeteringRounds.Load()
	lanes := tokenMeteringRoundLanes(state.ScanThrough, now, sourceTurn)
	pressure, pressureErr := m.tokenPipelinePressure(ctx)
	if cached := m.tokenMeteringReadCache; cached != nil {
		if !time.Now().Before(cached.ExpiresAt) || now < cached.ObservedAt {
			m.tokenMeteringReadCache = nil
		} else {
			lane := tokenMeteringLaneForward
			if cached.Window.Replay {
				lane = tokenMeteringLaneReplay
			} else if cached.Window.StartAt > state.ScanThrough {
				lane = tokenMeteringLaneRecent
			}
			// Flush local rows before any source query, without spending the
			// next source turn's reservation for history, recent work or replay.
			for i, scheduled := range lanes {
				if scheduled == lane {
					copy(lanes[1:i+1], lanes[:i])
					break
				}
			}
			lanes[0] = lane
		}
	}
	sourceTurnCounted := false
	for i := 0; i < 4; i++ {
		// Soft pressure reduces repair frequency, but cannot permanently block
		// the raw-ID repairs that make retained facts usable. Hard pressure still
		// stops new replay reads; an already-read suffix keeps its commit priority.
		// Use the same reserved turn as lane ordering. Reading the mutable
		// counter after the forward query would skip every repair reservation.
		replayPaused := pressure.Level == "hard" || pressure.Level == "soft" && (sourceTurn+1)%4 != 0
		if lanes[i] == tokenMeteringLaneReplay && m.tokenMeteringReadCache == nil && (replayPaused || pressureErr != nil) {
			reason := "backpressure_" + pressure.Level
			if pressureErr != nil {
				reason = "backpressure_state_unavailable"
			}
			if err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
				return tokenMeteringDeferredLane(tx, state.SourceEpoch, tokenMeteringLaneReplayRepair, reason, now)
			}); err != nil {
				return pages, err
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			if budgetFinished(err) {
				return pages, nil
			}
			return pages, err
		}
		// Read latency can increase from one page to the next. The previous
		// page cannot predict a safe shorter timeout: cancelling a healthy
		// MySQL read closes its connection and forces shared collectors to
		// reconnect. Reserve the full existing query allowance, or resume in
		// the next round before claiming/reopening another source window.
		// A cached suffix needs only a local commit, with no source read.
		if m.tokenMeteringReadCache == nil && !fullReadBudgetAvailable(lanes[i]) {
			if pages > 0 {
				m.tokenMeteringPartialPage.Store(true)
			}
			continue
		}
		// A cache flush consumes a local slot, never an extra source query.
		window, found, err := m.nextTokenMeteringWork(ctx, state.SourceEpoch, state.LeaseToken, now, lanes[i])
		if err == nil && !found && lanes[i] == tokenMeteringLaneRecent {
			window, found, err = m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
		}
		if err != nil {
			if budgetFinished(err) {
				return pages, nil
			}
			return pages, err
		}
		if !found {
			continue
		}
		actualLane := lanes[i]
		if window.Lane == tokenMeteringLaneForwardCatchup {
			actualLane = tokenMeteringLaneForward
		}
		laneRun := &tokenMeteringLaneRun{Window: window}
		laneRuns = append(laneRuns, laneRun)
		if !m.tokenMeteringDiskOK() {
			laneRun.Deferred, laneRun.Reason, laneRun.Stage = true, "local_disk_limit", "capacity"
			return pages, errTokenMeteringCapacity
		}
		pageRows := tokenMeteringReadPageRows(window.ReadPageRows)
		var timing backgroundSourceReadTiming
		var rows []tokenMeteringRaw
		var done bool
		var readErr error
		observedAt, nextPageRows := now, pageRows
		nextSuccessPages, nextWorstMS := window.ReadSuccessPages, window.ReadWorstMS
		var expiresAt time.Time
		cachedFailureReason := ""
		cached := m.takeTokenMeteringCachedRead(window, now)
		if cached != nil {
			rows, done, observedAt, timing = cached.Rows, cached.Done, cached.ObservedAt, cached.ReadTiming
			nextPageRows, expiresAt = cached.NextReadRows, cached.ExpiresAt
			nextSuccessPages, nextWorstMS = cached.NextReadSuccessPages, cached.NextReadWorstMS
			cachedFailureReason = cached.FailureReason
		} else {
			// Claiming the window may itself wait for a local writer. Recheck
			// after that wait; never pass the remaining tail as a SQL deadline.
			if !fullReadBudgetAvailable(actualLane) {
				if pages > 0 {
					m.tokenMeteringPartialPage.Store(true)
				}
				laneRun.Deferred, laneRun.Reason, laneRun.Stage = true, "budget_exhausted", "source_budget"
				continue
			}
			// Local work also consumes wall time. Stop waiting early enough to
			// preserve the full read allowance inside the unchanged hard round.
			waitBudget := max(time.Duration(0), min(tokenMeteringLaneWaitBudget(actualLane)-waited[actualLane], time.Until(roundDeadline)-4*time.Second))
			if !sourceTurnCounted {
				m.tokenMeteringRounds.Add(1)
				sourceTurnCounted = true
			}
			rows, done, readErr = m.readTokenMeteringPageWithBudgets(roundCtx, window, pageRows,
				waitBudget, 4*time.Second, &timing)
			expiresAt = time.Now().Add(30 * time.Second)
		}
		window.ReadWaitMS = timing.Wait.Milliseconds()
		window.ReadQueryMS = timing.Query.Milliseconds()
		window.ReadConnectionMS = timing.Connection.Milliseconds()
		window.ReadExecuteMS = timing.Execute.Milliseconds()
		window.ReadReceiveMS = timing.Receive.Milliseconds()
		window.ReadDecodeMS = timing.Decode.Milliseconds()
		if cached == nil {
			waited[actualLane] += timing.Wait
			sourceBudgets[actualLane] = max(time.Duration(0), sourceBudgets[actualLane]-timing.Query)
			laneRun.RowsRead += int64(len(rows))
			laneRun.Read = timing
		}
		readElapsed := timing.Query
		// Classify the source deadline before the separate local commit can
		// consume the remaining round time. A shorter transport timeout must
		// not become an ordinary budget yield while its prefix is persisted.
		readBudgetFinished := tokenMeteringBudgetExpired(roundCtx, parent, readErr)
		if errors.Is(readErr, errTokenMeteringConnectionWarm) {
			laneRun.Deferred, laneRun.Reason, laneRun.Stage = true, "connection_warmup", timing.Stage
			if parent.Err() != nil {
				return pages, parent.Err()
			}
			m.tokenMeteringPartialPage.Store(true)
			return pages, nil
		}
		var sourceErr *tokenMeteringReadError
		queryTimedOut := errors.As(readErr, &sourceErr) && sourceErr.queryStarted && errors.Is(readErr, context.DeadlineExceeded) && parent.Err() == nil
		if readErr != nil && sourceErr != nil && !sourceErr.queryStarted {
			// No SQL ran: do not poison the window's retry/backoff or shrink its
			// page size because another collector occupied the shared lane.
			// Exhausting this round's queue allowance is resumable scheduling
			// work too. Returning it as a source failure adds a global minute
			// even when a committed forward window is immediately runnable.
			if parent.Err() == nil && errors.Is(readErr, context.DeadlineExceeded) {
				laneRun.Deferred, laneRun.Reason, laneRun.Stage = true, "source_slot_wait", timing.Stage
				continue
			}
			return pages, readErr
		}
		markReadFailure := func() error {
			window.LastFailureStage = timing.Stage
			nextRows := 0
			reason := "source_read_failed"
			if sourceErr != nil && sourceErr.stage == "connection_acquire" {
				reason = "source_connection_failed"
			}
			if queryTimedOut && (sourceErr.stage == "query_execute" || sourceErr.stage == "result_receive") {
				nextRows = max(tokenMeteringMinReadRows, pageRows/2)
			}
			return m.failTokenMeteringWindowCheckpoint(ctx, state.LeaseToken, window, now, reason, &sourceFrontier, nextRows)
		}
		if readErr != nil {
			// Every decoded, validated ordered prefix is useful, including one
			// followed by a transport, decode or identity error. Persist only that
			// prefix; the failed row remains beyond the durable cursor. An error
			// never supplies EOF. Cancellation leaves the same cursor for restart.
			if len(rows) == 0 || parent.Err() != nil {
				if readBudgetFinished {
					return pages, nil
				}
				if err := markReadFailure(); err != nil {
					return pages, errors.Join(readErr, err)
				}
				if parent.Err() != nil || sourceErr == nil || !sourceErr.queryStarted {
					return pages, readErr
				}
				// Keep the four-slot/five-second limit and alternate to another
				// lane. The failed window is already fenced by its own next_at;
				// a failing replay must not consume every forward opportunity.
				sourceFailures = errors.Join(sourceFailures, readErr)
				continue
			}
		}
		facts := make([]TokenMeteringFact, 0, len(rows))
		refs := []TokenMeteringRef{}
		for _, r := range rows {
			f, rs := parseTokenMetering(m.cfg, r, observedAt)
			facts = append(facts, f)
			refs = append(refs, rs...)
		}
		// Rows are already consumed and the source slot released. Reserve a
		// separate bounded LOCAL commit so an expiring source-round deadline
		// cannot repeatedly discard the same successfully read page. Caller
		// cancellation still wins; the lease fence remains in the transaction.
		commitStarted := time.Now()
		if cached == nil && readErr == nil {
			nextPageRows, nextSuccessPages, nextWorstMS = tokenMeteringReadFeedback(window, len(rows), readElapsed)
		} else if readErr != nil {
			// Reset with any committed timeout prefix as well as the separate
			// retry checkpoint; a contended failure checkpoint cannot preserve a
			// misleading streak of successful reads.
			nextSuccessPages, nextWorstMS = 0, 0
		}
		commitCtx := context.WithValue(parent, tokenMeteringCommitTimingKey{}, &laneRun.Write)
		written, commitErr := m.commitTokenMeteringReadPage(commitCtx, state.LeaseToken, window, rows, facts, refs, done && readErr == nil, now, nextPageRows, nextSuccessPages, int(nextWorstMS))
		laneRun.RowsCommitted = int64(written)
		// The page commit has its own two-second local budget. A zero-row
		// timeout there is a real local write failure and must be checkpointed
		// as such; only the enclosing round deadline may turn it into a
		// resumable budget yield. Otherwise a failed transaction could be
		// mistaken for successful scheduling and lose its failure/backoff.
		var localWait *tokenLocalWaitError
		// Older SQLite drivers can return the deadline directly when the
		// connection/BEGIN lock is interrupted, losing the wrapper while still
		// proving that no transaction work ran.  Treat that same zero-work
		// queue/lock timeout as a resumable scheduling yield; only a callback or
		// commit that actually ran is a local failure.
		// tokenLocalWriteTransaction wraps a zero-work queue/connection/SQLite
		// lock timeout as tokenLocalWaitError.  The source-read timing above has
		// no transaction Work/Commit fields; do not accidentally use its phase
		// values to classify a real local write failure.
		localSchedulingWait := errors.As(commitErr, &localWait) || errors.Is(commitErr, context.DeadlineExceeded) || tokenSQLiteBusy(commitErr) || errors.Is(commitErr, errTokenMeteringCapacity)
		if localSchedulingWait && parent.Err() == nil {
			laneRun.Deferred, laneRun.Reason, laneRun.Stage = true, "local_budget_exhausted", laneRun.Write.Stage
			if localWait != nil {
				laneRun.Reason = "writer_wait"
			}
			if errors.Is(commitErr, errTokenMeteringCapacity) {
				laneRun.Reason = "local_capacity_exhausted"
			}
			// A local budget expiration is not a source failure. Resume the
			// durable cursor, including a zero-row queue wait, without adding
			// a failed-window backoff. The transaction wrapper has logged the
			// stage and rolled back any uncommitted work.
			failureReason := cachedFailureReason
			if readErr != nil {
				failureReason = "source_read_failed"
				if sourceErr != nil && sourceErr.stage == "connection_acquire" {
					failureReason = "source_connection_failed"
				}
			}
			if readErr != nil {
				// The source error is retained with the cached suffix and will be
				// checkpointed after that suffix commits. The local wait itself is
				// resumable scheduling, never a failed source window.
				sourceFailures = errors.Join(sourceFailures, readErr)
			}
			m.tokenMeteringPartialPage.Store(true)
			if readErr == nil || len(rows) > 0 {
				checkpoint := window
				if written > 0 {
					checkpoint.CursorAt, checkpoint.CursorID = rows[written-1].CreatedAt, rows[written-1].ID
				}
				m.tokenMeteringReadCache = &tokenMeteringCachedRead{Window: checkpoint, Rows: rows[written:], Done: done, ReadTiming: timing,
					FailureReason: failureReason, NextReadRows: nextPageRows, NextReadSuccessPages: nextSuccessPages, NextReadWorstMS: nextWorstMS, ObservedAt: observedAt, ExpiresAt: expiresAt}
			}
			return pages, nil
		}
		if err := commitErr; err != nil {
			slog.Warn("token facts local commit failed", "rows", len(rows), "elapsed_ms", time.Since(commitStarted).Milliseconds(), "deadline", errors.Is(err, context.DeadlineExceeded), "class", tokenMeteringLocalError(err))
			// A failed local transaction remains a failure. The failure record
			// rereads any earlier committed prefix; it never completes a window.
			if markErr := m.failTokenMeteringWindow(ctx, state.LeaseToken, window, now, tokenMeteringLocalError(err)); markErr != nil {
				return pages, errors.Join(err, markErr)
			}
			return pages, err
		}
		pages++
		if readErr != nil {
			if readBudgetFinished {
				return pages, nil
			}
			if err := markReadFailure(); err != nil {
				return pages, errors.Join(readErr, err)
			}
			sourceFailures = errors.Join(sourceFailures, readErr)
			continue
		}
		if written < len(rows) {
			laneRun.Deferred, laneRun.Reason, laneRun.Stage = true, "budget_exhausted", "local_commit"
			if written > 0 && time.Now().Before(expiresAt) {
				checkpoint := window
				checkpoint.CursorAt, checkpoint.CursorID = rows[written-1].CreatedAt, rows[written-1].ID
				m.tokenMeteringReadCache = &tokenMeteringCachedRead{
					Window: checkpoint, Rows: rows[written:], Done: done, ReadTiming: timing, NextReadRows: nextPageRows, NextReadSuccessPages: nextSuccessPages, NextReadWorstMS: nextWorstMS,
					FailureReason: cachedFailureReason, ObservedAt: observedAt, ExpiresAt: expiresAt,
				}
			}
			m.tokenMeteringPartialPage.Store(true)
			return pages, nil
		}
		if cachedFailureReason != "" {
			if err := m.failTokenMeteringWindow(ctx, state.LeaseToken, window, now, cachedFailureReason); err != nil {
				return pages, err
			}
			cachedFailureReason = ""
			sourceFailures = errors.Join(sourceFailures, errors.New("token source page failed after local prefix"))
		}
	}
	return pages, nil
}

// Commit a bounded prefix at a time. All chunks share the existing two-second
// local budget. A full source page must not be repeatedly rolled back merely
// because its final rows do not fit. Each transaction publishes only its own
// durable cursor; only the final chunk of an exhausted source window can mark
// that window complete. No source slot is held and caller cancellation wins.
func (m *Monitor) commitTokenMeteringReadPage(parent context.Context, token string, w TokenMeteringWindow, raw []tokenMeteringRaw, facts []TokenMeteringFact, refs []TokenMeteringRef, done bool, now int64, nextReadRows ...int) (int, error) {
	if len(raw) != len(facts) {
		return 0, errors.New("token page facts mismatch")
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if len(raw) == 0 {
		if len(nextReadRows) >= 3 {
			w.ReadPageRows = tokenMeteringReadPageRows(nextReadRows[0])
			w.ReadSuccessPages, w.ReadWorstMS = nextReadRows[1], int64(nextReadRows[2])
		}
		timing := &tokenWriteTiming{}
		err := m.commitTokenMeteringPage(context.WithValue(ctx, tokenWriteTimingKey{}, timing), token, w, raw, facts, refs, done, now)
		addTokenMeteringCommitTiming(parent, timing)
		return 0, err
	}
	written := 0
	chunkRows := max(tokenMeteringCommitChunk, min(64, int(m.tokenMeteringCommitRows.Load())))
	lastWriteDuration := time.Duration(0)
	for written < len(raw) {
		if err := parent.Err(); err != nil {
			return written, err
		}
		deadline, _ := ctx.Deadline()
		// Leave room for the next transaction rather than knowingly starting
		// another chunk with only a few milliseconds of the budget left.
		if written > 0 && time.Until(deadline) < max(250*time.Millisecond, lastWriteDuration+lastWriteDuration/2) {
			return written, nil
		}
		end := min(written+chunkRows, len(raw))
		ids := make(map[int64]bool, end-written)
		for _, row := range raw[written:end] {
			ids[row.ID] = true
		}
		chunkRefs := []TokenMeteringRef{}
		for _, ref := range refs {
			if ids[ref.LogID] {
				chunkRefs = append(chunkRefs, ref)
			}
		}
		checkpoint := w
		if end == len(raw) && len(nextReadRows) > 0 {
			checkpoint.ReadPageRows = tokenMeteringReadPageRows(nextReadRows[0])
			if len(nextReadRows) >= 3 {
				checkpoint.ReadSuccessPages, checkpoint.ReadWorstMS = nextReadRows[1], int64(nextReadRows[2])
			}
		}
		var held time.Duration
		timing := &tokenWriteTiming{}
		writeCtx := context.WithValue(ctx, tokenWriteTimingKey{}, timing)
		err := m.commitTokenMeteringPage(writeCtx, token, checkpoint, raw[written:end], facts[written:end], chunkRefs, done && end == len(raw), now, &held)
		addTokenMeteringCommitTiming(parent, timing)
		lastWriteDuration = held
		if err != nil {
			// A previously cheap size can become too expensive for a denser
			// page. Shrink after our own timed-out transaction so a zero-row
			// rollback cannot repeat forever at the remembered size. Queue-only
			// waits and caller cancellation are not measurements of write cost.
			if tokenMeteringBudgetExpired(ctx, parent, err) && (timing.Work > 500*time.Millisecond || timing.Commit > 500*time.Millisecond) {
				m.tokenMeteringCommitRows.Store(int64(max(tokenMeteringCommitChunk, chunkRows/2)))
			}
			if written > 0 && tokenMeteringBudgetExpired(ctx, parent, err) {
				return written, nil
			}
			return written, err
		}
		committedRows := end - written
		written = end
		w.CursorAt, w.CursorID = raw[end-1].CreatedAt, raw[end-1].ID
		// Start with a durable small prefix, then amortize fencing, counters
		// and commit costs when the measured transaction leaves headroom.
		// No transaction exceeds the existing 64-row write batch, and all
		// chunks still share the same two-second local deadline and FIFO lock.
		// Queue wait still consumes the shared deadline. It must not also be
		// multiplied into the next transaction's predicted write cost: a prior
		// long wait can drain before the next turn. Cancellation while queued
		// or during a transaction retains only the already committed prefix.
		// It is not a measure of this chunk's write cost:
		// treating other workers' hold time as ours keeps cheap chunks tiny,
		// multiplying fencing, commits and further queue waits under load.
		// Retained-store commits can cost 250-500ms before row cost dominates.
		// Keeping every such transaction at eight rows repeatedly pays that
		// overhead and makes a sealed window fall behind. Amortize it within
		// the same two-second page budget and 64-row cap, yielding the writer
		// after each transaction; expensive pages still shrink and checkpoint.
		// COMMIT includes the store's fixed WAL/checkpoint cost. Shrinking
		// cheap SQL because that cost is high multiplies the same commits.
		if timing.Work < 500*time.Millisecond && timing.Commit < 500*time.Millisecond && committedRows == chunkRows {
			// The first local transaction pays the fixed fence/ledger cost. A
			// conservative doubling leaves too little useful work in a two-second
			// page budget when that fixed cost is a few hundred milliseconds (for
			// example, a 8+16+32 warm-up only commits 56 rows). Grow the first
			// successful prefix more aggressively, while the measured work/commit
			// limits below still shrink immediately for a dense or slow page.
			growth := 2
			if chunkRows == tokenMeteringCommitChunk {
				growth = 4
			}
			chunkRows = min(64, chunkRows*growth)
		} else if timing.Work > 500*time.Millisecond || timing.Commit > 750*time.Millisecond {
			chunkRows = max(tokenMeteringCommitChunk, chunkRows/2)
		}
		// A page can exhaust its local budget after a safe durable prefix.
		// Its successor resumes this measured size instead of repeating the
		// 8/16/32-row warmup and its writer handoffs on every partial page.
		m.tokenMeteringCommitRows.Store(int64(chunkRows))
	}
	return written, nil
}

func (m *Monitor) nextTokenMeteringWindow(ctx context.Context, epoch, token string, now int64, replay bool) (TokenMeteringWindow, bool, error) {
	lane := tokenMeteringLaneForward
	if replay {
		lane = tokenMeteringLaneReplay
	}
	return m.nextTokenMeteringWork(ctx, epoch, token, now, lane)
}

func (m *Monitor) nextTokenMeteringWork(ctx context.Context, epoch, token string, now int64, lane tokenMeteringLane) (TokenMeteringWindow, bool, error) {
	var w TokenMeteringWindow
	found := false
	replay := lane == tokenMeteringLaneReplay
	if cached := m.tokenMeteringReadCache; cached != nil && cached.Window.SourceEpoch == epoch && now >= cached.ObservedAt && time.Now().Before(cached.ExpiresAt) {
		// This window was already reserved before the source page was read.
		// Resuming its local suffix needs no second unchanged window UPDATE or
		// writer handoff. Re-read the durable lease, cursor and retry deadline;
		// every subsequent commit still fences the owner in its transaction.
		err := m.tokenReadStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			state, err := tokenMeteringFence(tx, epoch, token, now)
			if err != nil {
				return err
			}
			start := cached.Window.StartAt
			eligible := lane == tokenMeteringLaneReplay && cached.Window.Replay ||
				lane == tokenMeteringLaneForward && start == state.ScanThrough ||
				lane == tokenMeteringLaneRecent && tokenMeteringNeedsRecent(state.ScanThrough, now)
			if !eligible {
				return nil
			}
			if err := tx.First(&w, "source_epoch=? AND start_at=?", epoch, start).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil
				}
				return err
			}
			found = !w.Done && w.NextAt <= now && w.ReadPageRows > 0 && w.Replay == cached.Window.Replay
			return nil
		})
		if err != nil || found {
			return w, found, err
		}
	}
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		state, err := tokenMeteringFence(tx, epoch, token, now)
		if err != nil {
			return err
		}
		if replay {
			var repair struct{ StartAt int64 }
			pending := tx.Raw(`SELECT r.start_at FROM token_metering_source_repairs_v1 r LEFT JOIN token_metering_windows w
			 ON w.source_epoch=r.source_epoch AND w.start_at=r.start_at WHERE r.source_epoch=? AND COALESCE(w.next_at,0)<=?
			 ORDER BY r.start_at LIMIT 1`, epoch, now).Scan(&repair)
			if pending.Error != nil {
				return pending.Error
			}
			var err error
			if pending.RowsAffected > 0 {
				err = tx.Where("source_epoch=? AND start_at=?", epoch, repair.StartAt).First(&w).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					w = TokenMeteringWindow{SourceEpoch: epoch, StartAt: repair.StartAt, EndAt: repair.StartAt + 300, CursorAt: repair.StartAt}
					err = nil
				}
			} else {
				err = tx.Where("source_epoch = ? AND (done = ? OR replay = 1 OR (last_reason <> '' AND start_at < ?)) AND start_at >= ? AND next_at <= ?", epoch, true, state.ScanThrough, now-tokenMeteringRepairSeconds, now).
					Order("replay DESC, last_scanned_at ASC, start_at ASC").First(&w).Error
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if !w.Replay {
				w.Replay = true
				if w.LastReason == "" || w.SourceComplete {
					w.CursorAt = w.StartAt
					w.CursorID = 0
				}
			}
			// Revoke the previous completeness proof before reading a single replay page.
			w.Done, w.SourceComplete = false, false
		} else if lane == tokenMeteringLaneRecent {
			var eligible bool
			w, eligible, err = tokenMeteringRecentWindow(tx, state, now)
			if err != nil || !eligible {
				return err
			}
		} else {
			end := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
			if state.ScanThrough >= end {
				return nil
			}
			err := tx.First(&w, "source_epoch = ? AND start_at = ?", epoch, state.ScanThrough).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				w = TokenMeteringWindow{SourceEpoch: epoch, StartAt: state.ScanThrough, EndAt: state.ScanThrough + tokenMeteringWindowSeconds, CursorAt: state.ScanThrough}
			}
			// Recent collection can already have finished this exact window.
			// Reuse only its durable EOF; never move across an absent or open
			// interval. A replay keeps the window open until it completes.
			if w.Done && !w.Replay && w.EndAt == state.ScanThrough+tokenMeteringWindowSeconds {
				state.ScanThrough = w.EndAt
				if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
					return err
				}
				return tx.Save(&state).Error
			}
			if w.NextAt > now {
				return nil
			}
		}
		// Page size is a checkpoint of this window. A slow or unavailable
		// historical replay must not shrink every healthy forward source page.
		// The state keeps the preferred width for the next forward window;
		// legacy windows also retain their durable failure-derived reduction.
		if w.ReadPageRows == 0 {
			w.ReadPageRows = tokenMeteringReadPageRows(state.ReadPageRows)
			if (w.LastReason == "source_read_failed" || w.LastReason == "retry_exhausted") && w.Attempts > 0 {
				w.ReadPageRows = min(w.ReadPageRows, max(tokenMeteringMinReadRows, tokenMeteringPageRows>>min(w.Attempts, 5)))
			}
		}
		tokenMeteringResumeWindow(&w, now)
		w.Lane = tokenMeteringLaneName(lane)
		w.UpdatedAt = now
		if err := tx.Save(&w).Error; err != nil {
			return err
		}
		if err := tokenMeteringLaneStateUpdate(tx, w, state.ScanThrough, now, map[string]any{
			"window_start": w.StartAt, "window_end": w.EndAt, "cursor_at": w.CursorAt, "cursor_id": w.CursorID,
			"attempt": w.Attempts, "next_at": w.NextAt, "lease_token": token, "lease_until": state.LeaseUntil,
			"running": int64(1), "last_started_at": now, "last_reason": w.LastReason,
		}); err != nil {
			return err
		}
		if w.Replay {
			if state.CompleteThrough > w.StartAt {
				state.CompleteThrough = w.StartAt
			}
			if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
				return err
			}
			if err := tx.Save(&state).Error; err != nil {
				return err
			}
		}
		found = true
		return nil
	})
	return w, found, err
}

func (m *Monitor) readTokenMeteringPage(ctx context.Context, w TokenMeteringWindow, rowLimit ...int) ([]tokenMeteringRaw, bool, error) {
	pageRows := tokenMeteringPageRows
	if len(rowLimit) > 0 {
		pageRows = tokenMeteringReadPageRows(rowLimit[0])
	}
	return m.readTokenMeteringPageWithBudgets(ctx, w, pageRows, 5*time.Second, 4*time.Second, nil)
}

func (m *Monitor) readTokenMeteringPageWithBudgets(ctx context.Context, w TokenMeteringWindow, pageRows int,
	waitBudget, queryBudget time.Duration, measured *backgroundSourceReadTiming) ([]tokenMeteringRaw, bool, error) {
	result := make([]tokenMeteringRaw, 0, pageRows)
	done := true
	var phases backgroundSourceReadTiming
	phases.Stage = "source_slot_wait"
	// Keep the SQL hint below the four-second transport allowance.  The former
	// one-second hint expired healthy indexed pages before the read-only tunnel
	// could deliver them, creating a replay backlog that looked like missing
	// anomalies.  A bounded three-second server budget still fences a slow
	// source, while the durable cursor preserves any ordered prefix.
	// Waiting has its own bounded allowance; SQL timeout, concurrency, start
	// spacing and maximum reads per round are unchanged.
	timing, err := m.withBackgroundSourceLowRead(ctx, waitBudget, queryBudget, func(queryCtx context.Context) error {
		phases.Stage = "connection_acquire"
		started := time.Now()
		conn, err := m.prodDB.Conn(queryCtx)
		phases.Connection = time.Since(started)
		if err != nil {
			return err
		}
		defer conn.Close()
		// Cold tunnel authentication can consume most of this read allowance.
		// Keep the usable pooled connection instead of starting a query which
		// cannot fit, cancelling it, and repeating the handshake next time.
		// Neither the round nor query budget is increased. No cursor is advanced.
		if deadline, ok := queryCtx.Deadline(); ok && phases.Connection >= queryBudget/2 && time.Until(deadline) < queryBudget/2 && queryCtx.Err() == nil {
			phases.Stage = "connection_warmup"
			return errTokenMeteringConnectionWarm
		}
		// Fixed indexed source scan. Never select content, token_name or credentials.
		query := "SELECT /*+ MAX_EXECUTION_TIME(" + strconv.Itoa(tokenMeteringSourceExecutionMS) + ") */ id, created_at, user_id, channel_id, SUBSTR(model_name,1,257), SUBSTR(`group`,1,129), prompt_tokens, completion_tokens, quota, is_stream, use_time, CASE WHEN LENGTH(other) <= ? THEN other ELSE NULL END, COALESCE(LENGTH(other),0), CASE WHEN LENGTH(request_id) <= 256 THEN request_id ELSE '' END FROM " + m.logSourceClause(nil, 0, "", "", "", "") +
			" WHERE type = 2 AND created_at >= ? AND created_at < ? AND (created_at > ? OR (created_at = ? AND id > ?)) ORDER BY created_at, id LIMIT ?"
		phases.Stage = "query_execute"
		started = time.Now()
		rows, err := conn.QueryContext(queryCtx, query, tokenMeteringOtherBytes, w.StartAt, w.EndAt, w.CursorAt, w.CursorAt, w.CursorID, pageRows)
		phases.Execute = time.Since(started)
		if err != nil {
			return err
		}
		defer rows.Close()
		phases.Stage = "result_receive"
		bytes := 0
		lastAt, lastID := w.CursorAt, w.CursorID
		for {
			receiveStarted := time.Now()
			hasRow := rows.Next()
			phases.Receive += time.Since(receiveStarted)
			if !hasRow {
				break
			}
			var r tokenMeteringRaw
			decodeStarted := time.Now()
			if err := rows.Scan(&r.ID, &r.CreatedAt, &r.UserID, &r.ChannelID, &r.Model, &r.Group, &r.Input, &r.Output, &r.Quota, &r.Stream, &r.UseTime, &r.Other, &r.OtherBytes, &r.RequestID); err != nil {
				phases.Decode += time.Since(decodeStarted)
				phases.Stage = "result_decode"
				return err
			}
			phases.Decode += time.Since(decodeStarted)
			size := len(r.Other.String) + len(r.Model) + len(r.Group) + len(r.RequestID) + 256
			if bytes+size > tokenMeteringPageBytes {
				done = false
				break
			}
			if r.ID <= 0 || r.CreatedAt < w.StartAt || r.CreatedAt >= w.EndAt || r.CreatedAt < lastAt || r.CreatedAt == lastAt && r.ID <= lastID {
				phases.Stage = "result_identity"
				return errors.New("invalid source row identity")
			}
			lastAt, lastID = r.CreatedAt, r.ID
			bytes += size
			result = append(result, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(result) == pageRows {
			done = false
		}
		if err := rows.Close(); err != nil {
			phases.Stage = "result_close"
			return err
		}
		return nil
	})
	timing.Connection, timing.Execute, timing.Receive, timing.Decode, timing.Stage = phases.Connection, phases.Execute, phases.Receive, phases.Decode, phases.Stage
	if measured != nil {
		*measured = timing
	}
	if err != nil && ctx.Err() == nil && !errors.Is(err, errTokenMeteringConnectionWarm) {
		// Never log the SQL error itself: it may contain request parameters.
		level, message := slog.LevelWarn, "token facts source read failed"
		if timing.Query == 0 && errors.Is(err, context.DeadlineExceeded) {
			level, message = slog.LevelInfo, "token facts source read deferred"
		}
		slog.Log(ctx, level, message, "stage", timing.Stage, "wait_ms", timing.Wait.Milliseconds(), "read_ms", timing.Query.Milliseconds(),
			"connection_ms", timing.Connection.Milliseconds(), "execute_ms", timing.Execute.Milliseconds(), "receive_ms", timing.Receive.Milliseconds(), "decode_ms", timing.Decode.Milliseconds(),
			"deadline", errors.Is(err, context.DeadlineExceeded), "received_rows", len(result), "page_rows", pageRows)
	}
	if err != nil {
		done = false
		err = &tokenMeteringReadError{err: err, queryStarted: timing.Query > 0, stage: timing.Stage}
	}
	return result, done, err
}
func (m *Monitor) failTokenMeteringWindow(ctx context.Context, token string, w TokenMeteringWindow, now int64, reason string, readPageRows ...int) error {
	return m.failTokenMeteringWindowCheckpoint(ctx, token, w, now, reason, nil, readPageRows...)
}

func (m *Monitor) failTokenMeteringWindowCheckpoint(ctx context.Context, token string, w TokenMeteringWindow, now int64, reason string, checkpoint **tokenMeteringFrontier, readPageRows ...int) error {
	// Failure persistence is required. A cancelled round gets a small independent
	// local-only budget; no source slot is held here.
	deadline, hasDeadline := ctx.Deadline()
	if ctx.Err() != nil || hasDeadline && time.Until(deadline) < time.Second {
		var cancel context.CancelFunc
		// Preserve the collection lane's writer priority when the enclosing
		// source/round budget is already spent. Failure persistence is still
		// bounded and independent, but it must not re-enter as an anonymous
		// maintenance writer behind the same backlog it is fencing.
		base := context.WithValue(context.Background(), tokenWriteOperationKey{}, "collection")
		ctx, cancel = context.WithTimeout(base, time.Second)
		defer cancel()
	}
	var frontier *tokenMeteringFrontier
	observation := w
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		state, err := tokenMeteringFence(tx, w.SourceEpoch, token, now)
		if err != nil {
			return err
		}
		if err := tx.First(&w, "source_epoch = ? AND start_at = ?", w.SourceEpoch, w.StartAt).Error; err != nil {
			return err
		}
		w.Attempts = min(w.Attempts+1, 12)
		w.ReadSuccessPages, w.ReadWorstMS = 0, 0
		w.LastReason = reason
		w.LastFailureStage = observation.LastFailureStage
		if w.LastFailureStage == "" {
			w.LastFailureStage = "local_commit"
		}
		w.UpdatedAt = now
		w.NextAt = now + tokenMeteringSourceBackoff(w.SourceEpoch, w.StartAt, w.Attempts)
		if w.Attempts >= 12 {
			// Exhausting rapid retries is not successful collection. Keep the
			// durable page cursor and continue bounded backoff until recovery.
			w.Done, w.SourceComplete = false, false
			w.LastReason = "retry_exhausted"
		}
		if len(readPageRows) > 0 && readPageRows[0] > 0 {
			w.ReadPageRows = tokenMeteringReadPageRows(readPageRows[0])
			if !w.Replay && w.StartAt == state.ScanThrough {
				state.ReadPageRows = w.ReadPageRows
			}
		}
		if err := tx.Save(&w).Error; err != nil {
			return err
		}
		state.LastFailureAt = now
		state.LastReason = w.LastReason
		state.LastFailureStage = w.LastFailureStage
		if err := tokenMeteringLaneStateUpdate(tx, w, state.ScanThrough, now, map[string]any{
			"failed": gorm.Expr("failed+1"), "last_failure_at": now, "last_reason": w.LastReason,
			"last_failure_stage": w.LastFailureStage, "attempt": w.Attempts, "next_at": w.NextAt,
			"cursor_at": w.CursorAt, "cursor_id": w.CursorID,
		}); err != nil {
			return err
		}
		if state.CompleteThrough > w.StartAt {
			state.CompleteThrough = w.StartAt
		}
		if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
			return err
		}
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		if checkpoint != nil {
			frontier, err = readTokenMeteringFrontier(tx, w.SourceEpoch)
			return err
		}
		return nil
	})
	// Publish only after COMMIT, never after a rolled-back failure checkpoint.
	if err == nil && checkpoint != nil {
		*checkpoint = frontier
	}
	return err
}
func tokenMeteringBackoff(attempt int) int64 {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	return min(int64(30)*(1<<uint(attempt-1)), int64(3600))
}

// Stable per-window jitter spreads retries after restarts without changing an
// already persisted next_at. Keep at least the base delay and cap at one hour.
func tokenMeteringSourceBackoff(epoch string, start int64, attempt int) int64 {
	base := tokenMeteringBackoff(attempt)
	h := uint64(start) ^ uint64(attempt)*0x9e3779b97f4a7c15
	for _, b := range []byte(epoch) {
		h = (h ^ uint64(b)) * 1099511628211
	}
	return min(int64(3600), base+int64(h%uint64(max(int64(1), base/4))))
}
func (m *Monitor) commitTokenMeteringPage(ctx context.Context, token string, w TokenMeteringWindow, raw []tokenMeteringRaw, facts []TokenMeteringFact, refs []TokenMeteringRef, done bool, now int64, holdTime ...*time.Duration) error {
	if !m.tokenMeteringDiskOK() {
		return errTokenMeteringCapacity
	}
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		state, err := tokenMeteringFence(tx, w.SourceEpoch, token, now)
		if err != nil {
			return err
		}
		var persisted TokenMeteringWindow
		if err := tx.First(&persisted, "source_epoch = ? AND start_at = ?", w.SourceEpoch, w.StartAt).Error; err != nil {
			return err
		}
		if persisted.CursorAt != w.CursorAt || persisted.CursorID != w.CursorID || persisted.Replay != w.Replay {
			return errors.New("token page cursor changed")
		}
		// Re-read counters under the same fence as the cursor. A second chunk
		// must not overwrite the first chunk's durable statistics.
		w.RowsRead, w.RowsCommitted = persisted.RowsRead, persisted.RowsCommitted+int64(len(raw))
		w.UpdatedAt = now
		if err := m.saveTokenMeteringFacts(tx, facts, refs, &state); err != nil {
			return err
		}
		if len(raw) > 0 {
			last := raw[len(raw)-1]
			w.CursorAt = last.CreatedAt
			w.CursorID = last.ID
		}
		w.Done, w.SourceComplete = false, false
		w.LastReason = ""
		w.LastFailureStage = ""
		w.Attempts = 0
		w.NextAt = 0
		// A committed prefix is progress, even before EOF proves coverage.
		state.LastSuccessAt = now
		state.LastReason = ""
		sealed := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
		closed := done && w.EndAt <= sealed && (w.SealStarted || w.Lane != tokenMeteringLaneRealtimeTail)
		if done && !closed {
			w.OpenObservedAt = now
			w.NextAt = now + tokenMeteringTailPollSeconds
		}
		if closed {
			wasReplay := w.Replay
			wasForward := state.ScanThrough == w.StartAt
			w.Done = true
			w.Replay = false
			w.LastScannedAt = now
			w.SourceComplete = state.SourceMode == "complete"
			w.NextAt = now + 1800
			if err := tx.Exec(`DELETE FROM token_metering_source_repairs_v1 WHERE source_epoch=? AND start_at=?`, w.SourceEpoch, w.StartAt).Error; err != nil {
				return err
			}
			if state.ScanThrough == w.StartAt {
				state.ScanThrough = w.EndAt
			}
			// Carry only the measured, fully committed page width forward.
			// EOF proves coverage, not spare throughput. Increasing here used
			// to undo a slow connection's learned width after every window.
			if wasForward && !wasReplay && w.ReadPageRows > 0 {
				state.ReadPageRows = w.ReadPageRows
			}
		}
		if err := tx.Save(&w).Error; err != nil {
			return err
		}
		if err := tokenMeteringLaneStateUpdate(tx, w, state.ScanThrough, now, map[string]any{
			"rows_committed": gorm.Expr("rows_committed+?", len(raw)), "processed": gorm.Expr("processed+?", len(raw)),
			"last_success_at": now, "last_reason": "", "last_failure_stage": "", "attempt": 0,
			"cursor_at": w.CursorAt, "cursor_id": w.CursorID, "next_at": w.NextAt,
		}); err != nil {
			return err
		}
		// An unfinished prefix cannot extend coverage. Its replay boundary was
		// already invalidated when claimed; retain that certificate rather than
		// rereading all historical windows for every small local transaction.
		if state.SourceMode != "complete" {
			state.CompleteThrough = 0
		} else if done || state.CompleteThrough > w.StartAt {
			if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
				return err
			}
		}
		return tx.Save(&state).Error
	}, holdTime...)
	if err == nil && (len(facts) > 0 || done) {
		// A newly committed fact can be eligible immediately. Wake the detector
		// instead of making it wait for the full idle poll interval.
		m.signalTokenDetectionWake()
		if len(facts) > 0 {
			m.signalTokenAssociationWake()
		}
	}
	return err
}
func recomputeTokenMeteringComplete(tx *gorm.DB, state *TokenMeteringState) error {
	if state.SourceMode != "complete" {
		state.CompleteThrough = 0
		return nil
	}
	// The certificate advances only through a bounded contiguous page. Replay
	// and failure invalidate it at their exact window before calling here.
	next := max(state.StartAt, min(state.CompleteThrough, state.ScanThrough))
	// Old binaries and an interrupted replay may have left a certificate
	// beyond an unfinished window. Seek only the partial incomplete index.
	var gap struct{ StartAt int64 }
	found := tx.Table("token_metering_windows").Select("start_at").Where("source_epoch=? AND start_at>=? AND start_at<? AND (COALESCE(done,0)=0 OR COALESCE(source_complete,0)=0 OR replay=1)", state.SourceEpoch, state.StartAt, next).Order("start_at").Limit(1).Find(&gap)
	if found.Error != nil {
		return found.Error
	}
	if found.RowsAffected > 0 {
		next = gap.StartAt
	}
	var windows []TokenMeteringWindow
	if err := tx.Select("start_at,end_at,done,source_complete,replay").Where("source_epoch = ? AND start_at >= ? AND start_at < ?", state.SourceEpoch, next, state.ScanThrough).Order("start_at").Limit(256).Find(&windows).Error; err != nil {
		return err
	}
	for _, w := range windows {
		if w.StartAt != next || !w.Done || !w.SourceComplete || w.Replay {
			break
		}
		next = w.EndAt
	}
	state.CompleteThrough = next
	return nil
}
func (m *Monitor) saveTokenMeteringFacts(tx *gorm.DB, facts []TokenMeteringFact, refs []TokenMeteringRef, state *TokenMeteringState) error {
	var usage struct{ Rows, Bytes int64 }
	if err := tx.Model(&TokenMeteringState{}).Select("COALESCE(SUM(fact_count),0) AS rows, COALESCE(SUM(fact_bytes),0) AS bytes").Scan(&usage).Error; err != nil {
		return err
	}
	maxRows, maxBytes := m.cfg.tokenMeteringLimits()
	byID := map[int64][]TokenMeteringRef{}
	for _, r := range refs {
		byID[r.LogID] = append(byID[r.LogID], r)
	}
	// A source page has unique identities. Fetch the existing page once and
	// batch the writes inside the same fenced transaction; per-row Save used
	// an UPDATE-then-INSERT probe plus separate ref writes for every new fact.
	ids := make([]int64, 0, len(facts))
	seen := map[int64]bool{}
	for _, f := range facts {
		if f.SourceEpoch != state.SourceEpoch || seen[f.LogID] {
			return errors.New("invalid token page identity")
		}
		seen[f.LogID] = true
		ids = append(ids, f.LogID)
	}
	var originals []TokenMeteringFact
	if len(ids) > 0 {
		// Comparison needs the hash and revision/bookkeeping, not a second
		// decode/copy of every retained source JSON payload in this write lane.
		if err := tx.Select("source_epoch,log_id,fact_hash,revision,first_seen_at,link_deadline,link_attempts,link_revision,link_state,link_reason,evidence_json,evidence_snapshot_at,stored_bytes,request_id,upstream_request_id,upstream_model_name,request_method,request_path,entry_event_id,entry_node,entry_route,entry_method,entry_status,entry_observed_at,missing_fields,request_length,request_length_version,request_length_meaning,request_length_observed_at,pricing_snapshot,pricing_effective_at,pricing_adapter,route_snapshot,base_path_snapshot,model_mapping_snapshot,customer_name_snapshot,customer_name_source,customer_name_observed_at").
			Where("source_epoch = ? AND log_id IN ?", state.SourceEpoch, ids).Find(&originals).Error; err != nil {
			return err
		}
	}
	oldByID := map[int64]TokenMeteringFact{}
	for _, old := range originals {
		oldByID[old.LogID] = old
	}
	changed := make([]TokenMeteringFact, 0, len(facts))
	changedIDs := make([]int64, 0, len(facts))
	changedRefs := []TokenMeteringRef{}
	lastSeen := map[int64][]int64{}
	for _, f := range facts {
		old, exists := oldByID[f.LogID]
		if exists && old.FactHash == f.FactHash {
			lastSeen[f.LastSeenAt] = append(lastSeen[f.LastSeenAt], f.LogID)
			continue
		}
		if exists {
			f.Revision = old.Revision + 1
			f.FirstSeenAt = old.FirstSeenAt
			f.LinkDeadline = old.LinkDeadline
			f.LinkAttempts = old.LinkAttempts
			f.LinkRevision = old.LinkRevision + 1
			if tokenMeteringKnownDispute(old) {
				retainTokenMeteringDispute(&f, old)
				f.StoredBytes += int64(len(f.EvidenceJSON))
			}
			usage.Bytes -= old.StoredBytes
			state.FactBytes -= old.StoredBytes
			if f.LinkDeadline <= f.LastSeenAt || f.LinkAttempts >= tokenMeteringMaxLinkAttempts && f.CreatedAt < f.LastSeenAt-900 {
				if !tokenMeteringKnownDispute(f) {
					f.LinkState = "expired"
					f.LinkReason = "changed_after_link_budget"
				}
				f.LinkNextAt = 0
			}
		} else {
			usage.Rows++
			state.FactCount++
		}
		usage.Bytes += f.StoredBytes
		state.FactBytes += f.StoredBytes
		if usage.Rows > maxRows || usage.Bytes > maxBytes {
			return errTokenMeteringCapacity
		}
		changed = append(changed, f)
		changedIDs = append(changedIDs, f.LogID)
		changedRefs = append(changedRefs, byID[f.LogID]...)
	}
	for at, unchanged := range lastSeen {
		if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND log_id IN ? AND last_seen_at < ?", state.SourceEpoch, unchanged, at).Update("last_seen_at", at).Error; err != nil {
			return err
		}
	}
	if len(changed) == 0 {
		return nil
	}
	if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(changed, 64).Error; err != nil {
		return err
	}
	if err := tx.Where("source_epoch = ? AND log_id IN ?", state.SourceEpoch, changedIDs).Delete(&TokenMeteringRef{}).Error; err != nil {
		return err
	}
	if len(changedRefs) > 0 {
		if err := tx.CreateInBatches(changedRefs, 128).Error; err != nil {
			return err
		}
		// Most references are new and unique. Find the collided keys with one
		// bounded indexed EXISTS probe per input identity inside SQLite, instead
		// of a separate GORM/driver round trip for every unique reference. EXISTS
		// stops at the first other identity even for a heavily reused hash.
		var collided []struct{ KeyID, Hash string }
		if err := tx.Table("token_metering_refs AS r").Select("DISTINCT r.key_id, r.hash").
			Where("r.source_epoch = ? AND r.log_id IN ?", state.SourceEpoch, changedIDs).
			Where("EXISTS (SELECT 1 FROM token_metering_refs AS other WHERE other.key_id = r.key_id AND other.hash = r.hash AND (other.source_epoch <> r.source_epoch OR other.log_id <> r.log_id))").
			Scan(&collided).Error; err != nil {
			return err
		}
		collidedKeys := make(map[[2]string]bool, len(collided))
		for _, key := range collided {
			collidedKeys[[2]string{key.KeyID, key.Hash}] = true
		}
		// A late duplicate consumption invalidates prior unique links, even if
		// their local evidence retry budget has already ended. This also sees
		// collisions within this new page before its cursor can be published.
		for _, ref := range changedRefs {
			if !collidedKeys[[2]string{ref.KeyID, ref.Hash}] {
				continue
			}
			delete(collidedKeys, [2]string{ref.KeyID, ref.Hash})
			var collisions []TokenMeteringRef
			if err := tx.Where("key_id = ? AND hash = ?", ref.KeyID, ref.Hash).Order("source_epoch,log_id").Limit(1001).Find(&collisions).Error; err != nil {
				return err
			}
			// Include this page even when a heavily reused identity exceeds the
			// bounded historical probe. Visit each owner only once per key.
			for _, added := range changedRefs {
				if added.KeyID == ref.KeyID && added.Hash == ref.Hash {
					collisions = append(collisions, added)
				}
			}
			visited := map[struct {
				epoch string
				id    int64
			}]bool{}
			for _, other := range collisions {
				key := struct {
					epoch string
					id    int64
				}{other.SourceEpoch, other.LogID}
				if visited[key] {
					continue
				}
				visited[key] = true
				var prior TokenMeteringFact
				if err := tx.Select("missing_fields,source_facts,link_state,link_reason").First(&prior, "source_epoch=? AND log_id=?", other.SourceEpoch, other.LogID).Error; err != nil {
					return err
				}
				if prior.LinkState == "conflict" || prior.LinkState == "ambiguous" && prior.LinkReason == "request_id_reused_by_consumptions" {
					continue
				}
				path, method := tokenMeteringSourceRoute(prior)
				missing := tokenMeteringMissingSet(prior.MissingFields, "entry_event_id", "evidence_conflict", false)
				missing = tokenMeteringMissingSet(missing, "request_length", "evidence_conflict", false)
				if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND log_id = ? AND link_state <> ?", other.SourceEpoch, other.LogID, "conflict").Updates(map[string]any{
					"link_state": "ambiguous", "link_reason": "request_id_reused_by_consumptions", "evidence_level": "unavailable",
					"request_length": nil, "length_state": "missing", "request_length_version": "", "request_length_meaning": "whole_http_request_not_body_or_token_count", "request_length_observed_at": 0,
					"entry_event_id": "", "entry_node": "", "entry_route": "", "entry_method": "", "entry_status": 0, "entry_observed_at": 0,
					"evidence_json": "", "evidence_snapshot_at": 0, "request_path": path, "request_method": method, "link_revision": gorm.Expr("link_revision + 1"),
					"missing_fields": missing,
				}).Error; err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func tokenMeteringLocalError(err error) string {
	if errors.Is(err, errTokenMeteringCapacity) {
		return "local_capacity_limit"
	}
	if errors.Is(err, errTokenMeteringLease) {
		return "lease_lost"
	}
	return "local_unavailable"
}
