package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const financeFactSyncStateID = 1

// FinanceGiftRecipient is only a collection index. It contains no request
// content and exists so later hours for a gift recipient retain exact
// usage/refund ordering after the grant hour has passed.
type FinanceGiftRecipient struct {
	SourceEpoch    string `gorm:"primaryKey;size:64;column:source_epoch"`
	UserID         int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	FirstGrantAt   int64  `gorm:"index;column:first_grant_at"`
	FirstGrantHour int64  `gorm:"index;column:first_grant_hour"`
	SourceLogID    int64  `gorm:"column:source_log_id"`
	UpdatedAt      int64  `gorm:"column:updated_at"`
}

// FinanceFactSyncState is the durable, monotonic cursor for chronological
// finance backfill. The cursor advances only after every proof for the hour
// has been published locally.
type FinanceFactSyncState struct {
	ID                int64  `gorm:"primaryKey;autoIncrement:false;column:id"`
	SourceEpoch       string `gorm:"size:64;column:source_epoch"`
	StartHourTs       int64  `gorm:"column:start_hour_ts"`
	NextHourTs        int64  `gorm:"column:next_hour_ts"`
	LastCompletedHour int64  `gorm:"column:last_completed_hour"`
	Status            string `gorm:"size:16;index;column:status"`
	FailureStreak     int64  `gorm:"column:failure_streak"`
	LastError         string `gorm:"size:512;column:last_error"`
	LastAttemptAt     int64  `gorm:"column:last_attempt_at"`
	LastSuccessAt     int64  `gorm:"column:last_success_at"`
	UpdatedAt         int64  `gorm:"column:updated_at"`
}

type financeHourSnapshot struct {
	UserFacts        []FinanceUserHourFact
	UserSourceRows   int64
	Credit           financeCreditHourFetch
	RecipientIDs     []int64
	BoundaryByUserID map[int64][]FinanceGiftBoundaryEvent
}

type syncFinanceStatus struct {
	Enabled           bool    `json:"enabled"`
	ReportEnabled     bool    `json:"report_enabled"`
	Status            string  `json:"status"`
	SourceEpoch       string  `json:"source_epoch,omitempty"`
	StartHour         int64   `json:"start_hour"`
	FinalizedThrough  int64   `json:"finalized_through"`
	NextHour          int64   `json:"next_hour"`
	LastCompletedHour int64   `json:"last_completed_hour"`
	ExpectedHours     int64   `json:"expected_hours"`
	CompletedHours    int64   `json:"completed_hours"`
	ProgressPercent   float64 `json:"progress_percent"`
	GiftRecipients    int64   `json:"gift_recipients"`
	FailureStreak     int64   `json:"failure_streak"`
	LastError         string  `json:"last_error,omitempty"`
	LastAttemptAt     int64   `json:"last_attempt_at"`
	LastSuccessAt     int64   `json:"last_success_at"`
	UpdatedAt         int64   `json:"updated_at"`
}

// FinanceFactBackfillResult is the bounded maintenance-command result. It
// contains only local cursor metadata and never includes source credentials or
// user/request content.
type FinanceFactBackfillResult struct {
	RequestedHours    int   `json:"requested_hours"`
	CompletedHours    int   `json:"completed_hours"`
	CaughtUp          bool  `json:"caught_up"`
	RunFromHour       int64 `json:"run_from_hour"`
	NextHour          int64 `json:"next_hour"`
	LastCompletedHour int64 `json:"last_completed_hour"`
	UserSourceRows    int64 `json:"user_source_rows"`
	CreditSourceRows  int64 `json:"credit_source_rows"`
	EligibleGrants    int64 `json:"eligible_grants"`
	GiftRecipients    int64 `json:"gift_recipients"`
	BoundaryEvents    int64 `json:"boundary_events"`
}

// financeFactPublishedThrough returns the exclusive end of the contiguous,
// locally published finance-fact prefix. It is read-only and deliberately
// does not call financeFactSyncState, because report rendering must never
// create or reset a collection cursor.
func (m *Monitor) financeFactPublishedThrough(ctx context.Context, startHour int64) (int64, error) {
	if m == nil || startHour < 0 || startHour%usageFactHourSeconds != 0 {
		return startHour, errors.New("invalid finance fact publication boundary")
	}
	db := m.financeFactsReadStore()
	if db == nil {
		return startHour, errors.New("finance facts store unavailable")
	}
	var state FinanceFactSyncState
	err := db.WithContext(ctx).First(&state, financeFactSyncStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return startHour, nil
	}
	if err != nil {
		return startHour, err
	}
	if state.SourceEpoch == "" || state.StartHourTs != startHour || state.NextHourTs < startHour || state.NextHourTs%usageFactHourSeconds != 0 {
		return startHour, nil
	}
	return state.NextHourTs, nil
}

// NewFinanceFactBackfill constructs the least-privilege maintenance runtime:
// it opens only the existing facts SQLite file and the read-only NewAPI source.
// The main Monitor store, encrypted upstream accounts, HTTP routes and every
// unrelated worker are deliberately outside this command's dependency graph.
func NewFinanceFactBackfill(s Settings) (*Monitor, error) {
	if err := validateUsageFactsSettings(s); err != nil {
		return nil, err
	}
	if err := validateFinanceSettings(s); err != nil {
		return nil, err
	}
	factsPath := strings.TrimSpace(s.UsageFactsStorePath)
	if factsPath == "" || sameStorePath(factsPath, strings.TrimSpace(s.StorePath)) {
		return nil, errors.New("finance backfill requires a separate existing usage facts store")
	}
	if s.LocalSnapshotOnly || !s.FinanceFactsSyncEnabled {
		return nil, errors.New("finance backfill requires explicit online read-only sync settings")
	}
	exists, err := preflightStoreIntegrity(factsPath)
	if err != nil {
		return nil, fmt.Errorf("finance facts integrity preflight: %w", err)
	}
	if !exists {
		return nil, errors.New("finance backfill refuses to create a new facts store")
	}
	m := &Monitor{
		cfg:                 s,
		chNames:             map[string]string{},
		snapCache:           map[snapshotCacheKey]cachedSnap{},
		sourceFailureNotify: make(chan struct{}, 1),
	}
	m.usageFactsIntegrityCheckedAt.Store(time.Now().Unix())
	m.usageFactsIntegrityOK.Store(true)
	initialized := false
	defer func() {
		if !initialized {
			m.Close()
		}
	}()
	if err := m.openUsageFactsStore(factsPath, true); err != nil {
		return nil, err
	}
	m.sourceLifecycleInitialized.Store(true)
	if err := m.initializeSource(); err != nil {
		return nil, err
	}
	initialized = true
	return m, nil
}

func (m *Monitor) financeFactsSyncEnabled() bool {
	return m.cfg.FinanceFactsSyncEnabled && m.usageFactsEnabled() && m.prodDB != nil
}

// financeFactSyncStatus is a local-only projection for the data-sync page.
// Reading it never claims a source lease, advances the cursor or queries
// NewAPI.
func (m *Monitor) financeFactSyncStatus(ctx context.Context, now time.Time) (syncFinanceStatus, error) {
	status := syncFinanceStatus{Enabled: m.cfg.FinanceFactsSyncEnabled, ReportEnabled: m.cfg.FinanceEnabled}
	startDate := strings.TrimSpace(m.cfg.FinanceStartDate)
	if startDate == "" {
		startDate = "2026-05-01"
	}
	startHour, err := financeStartHour(startDate)
	if err != nil {
		return status, err
	}
	status.StartHour = startHour
	status.FinalizedThrough = m.usageFactFinalizedHour(now)
	if status.FinalizedThrough > startHour {
		status.ExpectedHours = (status.FinalizedThrough - startHour) / usageFactHourSeconds
	}
	db := m.usageFactsStore()
	if db == nil {
		return status, errors.New("finance facts store unavailable")
	}
	var state FinanceFactSyncState
	err = db.WithContext(ctx).First(&state, financeFactSyncStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		status.NextHour = startHour
		if status.Enabled {
			status.Status = "waiting"
		} else {
			status.Status = "disabled"
		}
		return status, nil
	}
	if err != nil {
		return status, err
	}
	status.SourceEpoch = state.SourceEpoch
	status.NextHour = state.NextHourTs
	status.LastCompletedHour = state.LastCompletedHour
	status.FailureStreak = state.FailureStreak
	status.LastError = state.LastError
	status.LastAttemptAt = state.LastAttemptAt
	status.LastSuccessAt = state.LastSuccessAt
	status.UpdatedAt = state.UpdatedAt
	completedThrough := min(max(state.NextHourTs, startHour), status.FinalizedThrough)
	if state.StartHourTs == startHour && completedThrough > startHour {
		status.CompletedHours = (completedThrough - startHour) / usageFactHourSeconds
	}
	if status.ExpectedHours > 0 {
		status.ProgressPercent = float64(status.CompletedHours) * 100 / float64(status.ExpectedHours)
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	if epoch != "" {
		if err := db.WithContext(ctx).Model(&FinanceGiftRecipient{}).Where("source_epoch=?", epoch).Count(&status.GiftRecipients).Error; err != nil {
			return status, err
		}
	}
	switch {
	case !status.Enabled:
		status.Status = "disabled"
	case state.StartHourTs != startHour:
		status.Status = "start_changed"
	case epoch == "" || state.SourceEpoch != epoch:
		status.Status = "source_changed"
	case state.Status == "error" || state.FailureStreak > 0:
		status.Status = "error"
	case state.NextHourTs >= status.FinalizedThrough:
		status.Status = "caught_up"
	default:
		status.Status = "backfilling"
	}
	return status, nil
}

func financeStartHour(startDate string) (int64, error) {
	start, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(startDate), usageCST)
	if err != nil {
		return 0, err
	}
	return start.Unix(), nil
}

func (m *Monitor) financeGiftRecipientsForHour(ctx context.Context, sourceEpoch string, hourTs int64) ([]int64, error) {
	db := m.usageFactsStore()
	if db == nil {
		return nil, errors.New("finance facts store unavailable")
	}
	var rows []FinanceGiftRecipient
	if err := db.WithContext(ctx).Where("source_epoch=? AND first_grant_hour<=?", sourceEpoch, hourTs).
		Order("user_id").Find(&rows).Error; err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		if row.UserID <= 0 {
			return nil, errors.New("invalid finance gift recipient")
		}
		ids = append(ids, row.UserID)
	}
	return ids, nil
}

func financeSnapshotRecipientIDs(existing []int64, credit financeCreditHourFetch) ([]int64, map[int64]FinanceGiftRecipient, error) {
	seen := make(map[int64]struct{}, len(existing)+len(credit.Events))
	for _, id := range existing {
		if id <= 0 {
			return nil, nil, errors.New("invalid existing finance gift recipient")
		}
		seen[id] = struct{}{}
	}
	newRecipients := make(map[int64]FinanceGiftRecipient)
	for _, event := range credit.Events {
		if !event.EligibleTrial {
			continue
		}
		if event.TargetUserID <= 0 || event.EventAt < 0 || event.SourceLogID <= 0 {
			return nil, nil, errors.New("invalid eligible finance gift event")
		}
		seen[event.TargetUserID] = struct{}{}
		candidate := FinanceGiftRecipient{UserID: event.TargetUserID, FirstGrantAt: event.EventAt,
			FirstGrantHour: event.EventAt - event.EventAt%usageFactHourSeconds, SourceLogID: event.SourceLogID}
		prior, ok := newRecipients[event.TargetUserID]
		if !ok || candidate.FirstGrantAt < prior.FirstGrantAt || (candidate.FirstGrantAt == prior.FirstGrantAt && candidate.SourceLogID < prior.SourceLogID) {
			newRecipients[event.TargetUserID] = candidate
		}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, newRecipients, nil
}

// collectFinanceHourSnapshot reads all source components through one
// repeatable-read transaction. No local fact is written until the source
// transaction has completed successfully.
func (m *Monitor) collectFinanceHourSnapshot(ctx context.Context, hourTs int64, existingRecipients []int64) (financeHourSnapshot, map[int64]FinanceGiftRecipient, error) {
	var snapshot financeHourSnapshot
	var newRecipients map[int64]FinanceGiftRecipient
	if m.prodDB == nil || hourTs < 0 || hourTs%usageFactHourSeconds != 0 {
		return snapshot, nil, errors.New("invalid finance source snapshot request")
	}
	err := m.withUsageFactSourceQuery(ctx, func(queryCtx context.Context) error {
		tx, err := m.prodDB.BeginTx(queryCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return fmt.Errorf("begin finance read snapshot: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		snapshot.UserFacts, snapshot.UserSourceRows, err = fetchFinanceUserHourFacts(queryCtx, tx, hourTs)
		if err != nil {
			return err
		}
		snapshot.Credit, err = fetchFinanceCreditHour(queryCtx, tx, hourTs)
		if err != nil {
			return err
		}
		snapshot.RecipientIDs, newRecipients, err = financeSnapshotRecipientIDs(existingRecipients, snapshot.Credit)
		if err != nil {
			return err
		}
		snapshot.BoundaryByUserID = make(map[int64][]FinanceGiftBoundaryEvent, len(snapshot.RecipientIDs))
		for offset := 0; offset < len(snapshot.RecipientIDs); offset += financeCreditHourMaxRows {
			end := offset + financeCreditHourMaxRows
			if end > len(snapshot.RecipientIDs) {
				end = len(snapshot.RecipientIDs)
			}
			events, fetchErr := fetchFinanceGiftBoundaryEvents(queryCtx, tx, hourTs, snapshot.RecipientIDs[offset:end])
			if fetchErr != nil {
				return fetchErr
			}
			for _, event := range events {
				snapshot.BoundaryByUserID[event.UserID] = append(snapshot.BoundaryByUserID[event.UserID], event)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit finance read snapshot: %w", err)
		}
		return nil
	})
	return snapshot, newRecipients, err
}

func (m *Monitor) publishFinanceHourSnapshot(ctx context.Context, hourTs, now int64, sourceEpoch string, snapshot financeHourSnapshot, newRecipients map[int64]FinanceGiftRecipient) error {
	db := m.usageFactsStore()
	if db == nil {
		return errors.New("finance facts store unavailable")
	}
	// GORM maps the nested publication transactions below to savepoints. The
	// outer transaction is the visibility boundary: readers see either the
	// prior complete hour or the complete replacement, never a mixture of new
	// user totals and old credit/boundary evidence.
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		userState, err := replaceFinanceUserHourFacts(ctx, tx, hourTs, sourceEpoch, snapshot.UserFacts, now)
		if err != nil {
			return err
		}
		if userState.SourceRows != snapshot.UserSourceRows {
			return errors.New("finance user source row proof changed before publication")
		}
		creditState, err := replaceFinanceCreditHour(ctx, tx, hourTs, now, sourceEpoch, snapshot.Credit)
		if err != nil {
			return err
		}
		if creditState.Status != "complete" {
			return errors.New("finance credit hour is incomplete")
		}
		for _, userID := range snapshot.RecipientIDs {
			if _, err := replaceFinanceGiftBoundaryUserHour(ctx, tx, hourTs, userID, now, sourceEpoch, snapshot.BoundaryByUserID[userID]); err != nil {
				return err
			}
		}
		if len(newRecipients) > 0 {
			rows := make([]FinanceGiftRecipient, 0, len(newRecipients))
			for _, row := range newRecipients {
				row.SourceEpoch = sourceEpoch
				row.UpdatedAt = now
				rows = append(rows, row)
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i].UserID < rows[j].UserID })
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "source_epoch"}, {Name: "user_id"}},
				DoNothing: true,
			}).Create(&rows).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Monitor) recordFinanceFactFailure(ctx context.Context, state FinanceFactSyncState, err error, now int64) {
	if err == nil || m.usageFactsStore() == nil {
		return
	}
	state.ID = financeFactSyncStateID
	state.Status = "error"
	state.FailureStreak++
	state.LastAttemptAt = now
	state.UpdatedAt = now
	state.LastError = err.Error()
	if len(state.LastError) > 512 {
		state.LastError = state.LastError[:512]
	}
	_ = m.usageFactsStore().WithContext(ctx).Save(&state).Error
}

func (m *Monitor) financeFactSyncState(ctx context.Context, sourceEpoch string) (FinanceFactSyncState, error) {
	db := m.usageFactsStore()
	if db == nil {
		return FinanceFactSyncState{}, errors.New("finance facts store unavailable")
	}
	startHour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		return FinanceFactSyncState{}, err
	}
	var state FinanceFactSyncState
	err = db.WithContext(ctx).First(&state, financeFactSyncStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: sourceEpoch, StartHourTs: startHour, NextHourTs: startHour, Status: "pending", UpdatedAt: time.Now().Unix()}
		return state, db.WithContext(ctx).Create(&state).Error
	}
	if err != nil {
		return FinanceFactSyncState{}, err
	}
	if state.SourceEpoch != sourceEpoch || state.StartHourTs != startHour {
		state.SourceEpoch = sourceEpoch
		state.StartHourTs = startHour
		state.NextHourTs = startHour
		state.LastCompletedHour = 0
		state.Status = "pending"
		state.FailureStreak = 0
		state.LastError = ""
		state.UpdatedAt = time.Now().Unix()
		if err := db.WithContext(ctx).Save(&state).Error; err != nil {
			return FinanceFactSyncState{}, err
		}
	}
	return state, nil
}

func (m *Monitor) syncNextFinanceFactHour(ctx context.Context) (bool, error) {
	if !m.financeFactsSyncEnabled() {
		return false, nil
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	state, err := m.financeFactSyncState(ctx, epoch)
	if err != nil {
		return false, err
	}
	finalized := m.usageFactFinalizedHour(time.Now())
	if state.NextHourTs >= finalized {
		return false, nil
	}
	hourTs := state.NextHourTs
	existing, err := m.financeGiftRecipientsForHour(ctx, epoch, hourTs)
	if err != nil {
		m.recordFinanceFactFailure(ctx, state, err, time.Now().Unix())
		return false, err
	}
	snapshot, newRecipients, err := m.collectFinanceHourSnapshot(ctx, hourTs, existing)
	if err != nil {
		m.recordFinanceFactFailure(ctx, state, err, time.Now().Unix())
		return false, err
	}
	now := time.Now().Unix()
	if err := m.publishFinanceHourSnapshot(ctx, hourTs, now, epoch, snapshot, newRecipients); err != nil {
		m.recordFinanceFactFailure(ctx, state, err, now)
		return false, err
	}
	state.Status = "running"
	state.LastCompletedHour = hourTs
	state.NextHourTs = hourTs + usageFactHourSeconds
	state.FailureStreak = 0
	state.LastError = ""
	state.LastAttemptAt = now
	state.LastSuccessAt = now
	state.UpdatedAt = now
	if err := m.usageFactsStore().WithContext(ctx).Save(&state).Error; err != nil {
		return false, err
	}
	return true, nil
}

// RunFinanceFactBackfill runs only the finance collector. It is intended for
// a stopped local acceptance container with the same SQLite volumes mounted;
// it does not start HTTP, samplers, stability repair, usage history or upstream
// account workers. The configured source lease is still required and every
// source query remains read-only and rate-limited by the shared source gate.
func (m *Monitor) RunFinanceFactBackfill(ctx context.Context, maxHours int) (FinanceFactBackfillResult, error) {
	result := FinanceFactBackfillResult{RequestedHours: maxHours}
	if m == nil || !m.financeFactsSyncEnabled() {
		return result, errors.New("finance facts sync is not enabled")
	}
	if maxHours < 1 || maxHours > 168 {
		return result, errors.New("finance backfill max hours must be between 1 and 168")
	}
	if err := m.pingSource(ctx); err != nil {
		return result, fmt.Errorf("finance source ping: %w", err)
	}
	lease, acquired, err := m.acquireSourceLease(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire finance source lease: %w", err)
	}
	if !acquired {
		return result, errors.New("finance source lease is already held")
	}
	// The ordinary supervisor owns these runtime flags during service mode.
	// This bounded command intentionally does not start that supervisor, so it
	// publishes the equivalent state only for the lifetime of the held lease.
	m.setSourceState(sourceStateReady)
	m.sourceLeaseHeld.Store(m.cfg.sourceLeaseIsRequired())
	defer func() {
		m.sourceLeaseHeld.Store(false)
		m.setSourceState(sourceStateDisabled)
	}()
	if lease != nil {
		defer func() { _ = lease.Release() }()
	}
	initialState, err := m.financeFactSyncState(ctx, strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch))
	if err != nil {
		return result, err
	}
	result.RunFromHour = initialState.NextHourTs
	for result.CompletedHours < maxHours {
		progressed, syncErr := m.syncNextFinanceFactHour(ctx)
		if syncErr != nil {
			return result, syncErr
		}
		if !progressed {
			result.CaughtUp = true
			break
		}
		result.CompletedHours++
	}
	state, err := m.financeFactSyncState(ctx, strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch))
	if err != nil {
		return result, err
	}
	result.NextHour = state.NextHourTs
	result.LastCompletedHour = state.LastCompletedHour
	if err := m.populateFinanceFactBackfillEvidence(ctx, &result, state.SourceEpoch); err != nil {
		return result, err
	}
	return result, nil
}

func (m *Monitor) populateFinanceFactBackfillEvidence(ctx context.Context, result *FinanceFactBackfillResult, sourceEpoch string) error {
	if result == nil || result.NextHour <= result.RunFromHour {
		return nil
	}
	db := m.usageFactsStore()
	if db == nil {
		return errors.New("finance facts store unavailable")
	}
	type aggregate struct {
		SourceRows     int64
		EligibleGrants int64
	}
	var users aggregate
	if err := db.WithContext(ctx).Model(&FinanceUserHourState{}).
		Select("COALESCE(SUM(source_rows),0) AS source_rows").
		Where("source_epoch=? AND hour_ts>=? AND hour_ts<? AND status='complete'", sourceEpoch, result.RunFromHour, result.NextHour).
		Scan(&users).Error; err != nil {
		return err
	}
	var credits aggregate
	if err := db.WithContext(ctx).Model(&FinanceCreditHourState{}).
		Select("COALESCE(SUM(source_rows),0) AS source_rows,COALESCE(SUM(eligible_trial_rows),0) AS eligible_grants").
		Where("source_epoch=? AND hour_ts>=? AND hour_ts<? AND status='complete'", sourceEpoch, result.RunFromHour, result.NextHour).
		Scan(&credits).Error; err != nil {
		return err
	}
	result.UserSourceRows = users.SourceRows
	result.CreditSourceRows = credits.SourceRows
	result.EligibleGrants = credits.EligibleGrants
	if err := db.WithContext(ctx).Model(&FinanceGiftRecipient{}).Where("source_epoch=?", sourceEpoch).Count(&result.GiftRecipients).Error; err != nil {
		return err
	}
	return db.WithContext(ctx).Model(&FinanceGiftBoundaryEvent{}).
		Where("source_epoch=? AND hour_ts>=? AND hour_ts<?", sourceEpoch, result.RunFromHour, result.NextHour).
		Count(&result.BoundaryEvents).Error
}

func (m *Monitor) financeFactsWakeChannel() <-chan struct{} {
	m.financeFactsWakeOnce.Do(func() { m.financeFactsWake = make(chan struct{}, 1) })
	return m.financeFactsWake
}

func (m *Monitor) notifyFinanceFactsSync() {
	_ = m.financeFactsWakeChannel()
	select {
	case m.financeFactsWake <- struct{}{}:
	default:
	}
}

func (m *Monitor) waitFinanceFactsSync(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-m.financeFactsWakeChannel():
		return true
	case <-timer.C:
		return true
	}
}

// runFinanceFactsSync is intentionally single-threaded and alternates the two
// backfill lanes while both have work. A long main-ledger backlog therefore
// cannot starve an internal-account configuration rebuild, and saving a new
// account list wakes this worker immediately without increasing source-query
// concurrency.
func (m *Monitor) runFinanceFactsSync(ctx context.Context) {
	for {
		preferInternal := m.financeFactsPreferInternal.Load()
		var progressed bool
		var err error
		if preferInternal {
			progressed, err = m.syncNextFinanceInternalAccountBatch(ctx)
			if err == nil && !progressed {
				progressed, err = m.syncNextFinanceFactHour(ctx)
			}
		} else {
			progressed, err = m.syncNextFinanceFactHour(ctx)
			if err == nil && !progressed {
				progressed, err = m.syncNextFinanceInternalAccountBatch(ctx)
			}
		}
		m.financeFactsPreferInternal.Store(!preferInternal)
		if ctx.Err() != nil {
			return
		}
		delay := m.usageFactBackfillDelay()
		if err != nil {
			slog.Warn("经营核算事实同步暂停", "err", err)
			delay = time.Minute
		} else if !progressed {
			delay = time.Duration(m.cfg.UsageFactsSyncMinutes) * time.Minute
			if delay < time.Minute {
				delay = time.Minute
			}
		}
		if !m.waitFinanceFactsSync(ctx, delay) {
			return
		}
	}
}
