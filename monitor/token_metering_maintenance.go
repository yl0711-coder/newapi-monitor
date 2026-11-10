package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"gorm.io/gorm"
)

func (m *Monitor) startTokenMeteringMaintenance(ctx context.Context) {
	m.startTokenMeteringMaintenanceAt(ctx, time.Now)
}

// All event-time decisions share one clock. Tests can replay a retained
// snapshot without making its retention depend on the date of the test run;
// timers and operation deadlines still use elapsed wall time.
func (m *Monitor) startTokenMeteringMaintenanceAt(ctx context.Context, now func() time.Time) {
	if m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return
	}
	go m.runTokenMeteringCleanup(ctx, now, time.Duration(m.cfg.tokenProcessingPollSeconds())*time.Second)
	go func() {
		poll := time.Duration(m.cfg.tokenProcessingPollSeconds()) * time.Second
		wake := m.ensureTokenAssociationWake()
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case <-wake:
			}
			if m.tokenLocalWriteMu.collectionQueued() {
				timer.Reset(100 * time.Millisecond)
				continue
			}
			localCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			count, remaining, err := m.linkTokenMeteringBatchProgress(localCtx, now().Unix())
			cancel()
			if err != nil {
				slog.Warn("token facts local association incomplete", "committed", count, "deadline", errors.Is(err, context.DeadlineExceeded))
			}
			delay := min(poll, 15*time.Second)
			if count > 0 && remaining && (err == nil || errors.Is(err, context.DeadlineExceeded)) {
				delay = 100 * time.Millisecond
			} else if count == 0 && remaining && err == nil {
				// Collection can join after the read phase. Yielding before the
				// first write is still retained work, not an empty association lane.
				delay = 100 * time.Millisecond
			}
			timer.Reset(delay)
		}
	}()
	go m.runTokenMeteringMigration(ctx, now)
	m.startTokenMeteringHealthCounts(ctx)
	// Detection does not wait for another five-second association pass on
	// every catch-up turn. It retains its own lease and round budget, and
	// yields the single writer between small committed batches.
	go func() {
		poll := time.Duration(m.cfg.tokenProcessingPollSeconds()) * time.Second
		wake := m.ensureTokenDetectionWake()
		// Start one detection turn immediately. A retained backlog is already
		// due when the process starts; waiting a full poll interval before the
		// first bounded round makes recovery appear idle and can leave a moving
		// source boundary behind for an entire cadence.
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case <-wake:
			}
			if m.tokenLocalWriteMu.collectionQueued() {
				timer.Reset(100 * time.Millisecond)
				continue
			}
			detectCtx, detectCancel := context.WithTimeout(ctx, tokenDetectionRoundSeconds*time.Second)
			processed, remaining, err := m.runTokenDetectionRoundProgress(detectCtx, now().Unix())
			if err != nil {
				slog.Warn("token detection incomplete", "committed", processed, "deadline", errors.Is(err, context.DeadlineExceeded), "class", tokenDetectionFailureCode(err))
			}
			detectCancel()
			delay := poll
			if processed > 0 && !errors.Is(err, errTokenDetectionCapacity) {
				if errors.Is(err, context.DeadlineExceeded) || err == nil && remaining {
					// Continue committed backlog after either time or row budget
					// exhaustion. Small transactions still yield the fair writer.
					delay = 0
				} else if err == nil {
					delay = 2 * time.Second
				}
			} else if processed == 0 && remaining && err == nil {
				// A collection handoff between queue selection and the first job
				// must not insert a full idle poll into an already populated queue.
				delay = 100 * time.Millisecond
			}
			timer.Reset(delay)
		}
	}()
}

func (m *Monitor) runTokenMeteringCleanup(ctx context.Context, now func() time.Time, poll time.Duration) {
	timer := time.NewTimer(poll)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if m.tokenLocalWriteMu.collectionQueued() {
			// This short delay only retries the yield. It must not become the
			// permanent retention cadence after collection gets its turn.
			timer.Reset(100 * time.Millisecond)
			continue
		}
		// Retention has independent bounded budgets; it cannot consume the
		// association/detection deadline or immediately repeat a slow sweep.
		localCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := m.cleanupTokenMetering(localCtx, now().Unix()); err != nil {
			slog.Warn("token facts cleanup incomplete")
		}
		cancel()
		localCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
		if err := m.cleanupTokenDetection(localCtx, now().Unix()); err != nil {
			slog.Warn("token detection cleanup incomplete")
		}
		cancel()
		timer.Reset(poll)
	}
}

// Migration shares the writer with collection/linking/detection, and commits
// both facts and its restart checkpoint in small independent transactions.
func (m *Monitor) runTokenMeteringMigration(ctx context.Context, now func() time.Time) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if m.tokenLocalWriteMu.collectionQueued() {
			timer.Reset(500 * time.Millisecond)
			continue
		}
		if m.tokenLatestWorkPending(ctx, now().Unix()) {
			timer.Reset(5 * time.Second)
			continue
		}
		localCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		changed, yielded, err := m.migrateTokenMeteringBatchProgress(localCtx, now().Unix())
		budgetFinished := yielded || tokenMeteringBudgetExpired(localCtx, ctx, err)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if changed > 0 && (err == nil || budgetFinished) {
			// A committed prefix followed by exhaustion of the local budget is
			// resumable work. Reserve an interval for collection and detection
			// after a budget-limited repair turn: joining the FIFO alone does not
			// limit migration's share of sustained writes on a large old store.
			// The next turn keeps the same 3s budget and committed checkpoint.
			delay := 100 * time.Millisecond
			if budgetFinished {
				delay = 5 * time.Second
			}
			timer.Reset(delay)
			continue
		}
		if err != nil {
			slog.Warn("token facts contract migration incomplete", "committed", changed, "deadline", errors.Is(err, context.DeadlineExceeded))
			timer.Reset(30 * time.Second)
			continue
		}
		if budgetFinished {
			timer.Reset(5 * time.Second)
			continue
		}
		timer.Reset(time.Duration(m.cfg.tokenProcessingPollSeconds()) * time.Second)
	}
}

// Fixed categories preserve useful failure evidence without logging request
// payloads or SQL parameters. A missing dependency must not look like a timeout.
func tokenDetectionFailureCode(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, errTokenDetectionLease):
		return "lease_lost"
	case errors.Is(err, errTokenDetectionCapacity):
		return "capacity"
	case errors.Is(err, gorm.ErrRecordNotFound):
		return "missing_dependency"
	default:
		return "local_failure"
	}
}

// startTokenMeteringHealthCounts rebuilds the derived health counters after
// the HTTP listener is up.  The rebuild is intentionally retriable: a large
// facts store or an active detection transaction may briefly hold SQLite's
// writer, but that must not turn startup or /ready into a 503.
func (m *Monitor) startTokenMeteringHealthCounts(ctx context.Context) {
	if m == nil || m.tokenStore() == nil {
		return
	}
	go func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		schemaReady, rebuilding := false, false
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			if m.tokenLocalWriteMu.collectionQueued() {
				timer.Reset(500 * time.Millisecond)
				continue
			}
			if m.tokenLatestWorkPending(ctx, time.Now().Unix()) {
				timer.Reset(5 * time.Second)
				continue
			}
			localCtx, cancel := context.WithTimeout(context.WithValue(ctx, tokenWriteOperationKey{}, "health_rebuild"), 2*time.Second)
			if !schemaReady {
				err := m.tokenLocalWriteTransaction(localCtx, ensureTokenMeteringHealthCountsSchema)
				cancel()
				schemaReady = err == nil
				timer.Reset(time.Second)
				continue
			}
			done, err := m.maintainTokenMeteringHealthCounts(localCtx, rebuilding)
			cancel()
			if err != nil {
				slog.Warn("token health counter maintenance incomplete", "deadline", errors.Is(err, context.DeadlineExceeded))
				delay := 5 * time.Second
				if !rebuilding && errors.Is(err, context.DeadlineExceeded) {
					// A healthy-ledger audit may scan a large identity index. Do
					// not turn a slow periodic audit into a five-second retry loop
					// that repeatedly competes with real queue work and API reads.
					delay = time.Minute
				}
				timer.Reset(delay)
				continue
			}
			if done {
				if rebuilding {
					slog.Info("token health counters rebuilt")
				}
				rebuilding = false
				timer.Reset(time.Minute)
			} else {
				rebuilding = true
				// One bounded page per turn; collection, association and detection
				// receive writer opportunities even during a retained-store rebuild.
				timer.Reset(100 * time.Millisecond)
			}
		}
	}()
}

func (m *Monitor) maintainTokenMeteringHealthCounts(ctx context.Context, rebuilding bool) (bool, error) {
	force := false
	if !rebuilding {
		ready, consistent := false, false
		err := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
			return reader.Transaction(func(tx *gorm.DB) error {
				var err error
				ready, err = tokenMeteringHealthCountsReady(tx)
				if err != nil || !ready {
					return err
				}
				consistent, err = tokenMeteringHealthCountsConsistentForEpoch(tx, m.cfg.TokenFactsSourceEpoch)
				return err
			})
		})
		if err != nil {
			return false, err
		}
		if ready && consistent {
			return true, nil
		}
		force = ready
	}
	done := false
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		done, err = rebuildTokenMeteringHealthCountsBatch(tx, force)
		return err
	})
	return done && err == nil, err
}

const tokenMeteringMigrationChunk = 8

func tokenMeteringMigrationVersion(s Settings) string {
	bindings, _ := s.tokenMeteringBindings()
	body, _ := json.Marshal([]any{tokenMeteringParserVersion, "raw-request-repair-v2", tokenMeteringEffectiveAdapter(s), bindings})
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

// Each transaction reads its checkpoint and its bounded primary-key page.
// There is no filtered scan across the retained JSON store. Failed/cancelled
// chunks roll back together with their checkpoint; committed prefixes survive
// both cancellation and process restart. Missing evidence is not a repair job
// to be retried forever: it remains explicitly insufficient until source replay
// or an explicitly changed adapter/binding provides new evidence.
func (m *Monitor) migrateTokenMeteringBatch(ctx context.Context, now int64) (int, error) {
	changed, _, err := m.migrateTokenMeteringBatchProgress(ctx, now)
	return changed, err
}

// A successful checkpoint can yield at either the time or row budget. Preserve
// that outcome so maintenance grants collection/detection the same writer
// interval as a committed prefix followed by an actual deadline.
func (m *Monitor) migrateTokenMeteringBatchProgress(ctx context.Context, now int64) (int, bool, error) {
	if m == nil || m.tokenStore() == nil || !m.cfg.TokenFactsEnabled || m.cfg.LocalSnapshotOnly {
		return 0, false, nil
	}
	// A million-row report/revision migration must not postpone discovery of
	// missing raw request IDs until it finishes. Alternate bounded source-fact
	// repair and derived projections; both resume their own durable prefixes.
	if (m.tokenMeteringMigrationTurns.Add(1)-1)%2 == 0 {
		count, yielded, err := m.migrateTokenMeteringFactsBatchProgress(ctx, now)
		if count > 0 || yielded || err != nil {
			return count, yielded, err
		}
	}
	return m.migrateTokenMeteringProjectionBatchProgress(ctx, now)
}

func (m *Monitor) migrateTokenMeteringProjectionBatchProgress(ctx context.Context, now int64) (int, bool, error) {
	ctx = context.WithValue(ctx, tokenWriteOperationKey{}, "migration")
	if m == nil || m.tokenStore() == nil || !m.cfg.TokenFactsEnabled || m.cfg.LocalSnapshotOnly {
		return 0, false, nil
	}
	if m.tokenLocalWriteMu.collectionQueued() {
		return 0, true, nil
	}
	// Advance every projection once per turn. A large queue migration must not
	// starve report/counter recovery, and each prefix releases the shared writer.
	projected := 0
	for _, step := range []struct {
		table string
		run   func(*gorm.DB) error
	}{
		{tokenPendingMigrationTable, func(tx *gorm.DB) error {
			n, _, err := migrateTokenPendingBatch(tx)
			if err == nil {
				projected += n
			}
			return err
		}},
		{"token_report_state_migration_v1", func(tx *gorm.DB) error {
			_, err := migrateTokenReportStates(tx)
			if err == nil {
				projected++
			}
			return err
		}},
		{tokenReportStatusCheckpoint, func(tx *gorm.DB) error {
			n, _, err := migrateTokenStatusCounts(tx)
			if err == nil {
				projected += n
			}
			return err
		}},
	} {
		if !m.tokenReadStore().Migrator().HasTable(step.table) {
			continue
		}
		var ready bool
		if err := m.tokenReadStore().WithContext(ctx).Table(step.table).Where("id=1").Select("ready").Scan(&ready).Error; err != nil {
			return projected, false, err
		}
		if !ready {
			if m.tokenLocalWriteMu.collectionQueued() {
				return projected, true, nil
			}
			if err := m.tokenLocalWriteTransaction(ctx, step.run); err != nil {
				return projected, false, err
			}
		}
	}
	if projected > 0 {
		return projected, false, nil
	}
	if m.tokenReadStore().Migrator().HasTable("token_reference_members_checkpoint_v1") {
		var ready bool
		if err := m.tokenReadStore().WithContext(ctx).Raw("SELECT ready FROM token_reference_members_checkpoint_v1 WHERE id=1").Scan(&ready).Error; err != nil {
			return 0, false, err
		}
		if !ready {
			err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error { var err error; ready, err = migrateTokenReferenceMembers(tx); return err })
			return 0, !ready, err
		}
	}
	if m.tokenReadStore().Migrator().HasTable("token_revision_members_checkpoint_v1") {
		var pending bool
		if err := m.tokenReadStore().WithContext(ctx).Raw(`SELECT EXISTS(SELECT 1 FROM token_revision_members_checkpoint_v1 WHERE ready=0) OR EXISTS(SELECT 1 FROM token_revision_members_dirty_v1 LIMIT 1)`).Scan(&pending).Error; err != nil {
			return 0, false, err
		}
		if pending {
			var ready bool
			err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error { var err error; ready, err = migrateTokenRevisionMembers(tx); return err })
			return 0, !ready, err
		}
	}
	return m.migrateTokenMeteringFactsBatchProgress(ctx, now)
}

func (m *Monitor) migrateTokenMeteringFactsBatchProgress(ctx context.Context, now int64) (int, bool, error) {
	ctx = context.WithValue(ctx, tokenWriteOperationKey{}, "migration")
	if m.tokenLocalWriteMu.collectionQueued() {
		return 0, true, nil
	}
	version := tokenMeteringMigrationVersion(m.cfg)
	changed := 0
	scanBudget := 128
	pressure, pressureErr := m.tokenPipelinePressure(ctx)
	if pressureErr != nil || pressure.Level != "normal" {
		scanBudget = tokenMeteringMigrationChunk
	}
	var lastTurn time.Duration
	for scanned := 0; scanned < scanBudget; scanned += tokenMeteringMigrationChunk {
		if err := ctx.Err(); err != nil {
			return changed, false, err
		}
		if m.tokenLocalWriteMu.collectionQueued() {
			return changed, true, nil
		}
		if !tokenLocalWriteBudgetAvailable(ctx, lastTurn) {
			return changed, true, nil
		}
		// Continuous detection and association keep a writer queued. Joining
		// the same cancellable FIFO is necessary for this retained checkpoint
		// to make progress; an immediate miss otherwise idles for a full poll
		// indefinitely. Waiting still consumes the existing local round budget.
		committed, exhausted := 0, false
		turnStarted := time.Now()
		err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
			var state TokenMeteringState
			if err := tx.First(&state, "source_epoch=?", m.cfg.TokenFactsSourceEpoch).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					exhausted = true
					return nil
				}
				return err
			}
			if state.MigrationVersion != version {
				state.MigrationCursor = 0
			}
			var facts []TokenMeteringFact
			if err := tx.Where("source_epoch=? AND log_id>?", state.SourceEpoch, state.MigrationCursor).
				Order("log_id").Limit(tokenMeteringMigrationChunk).Find(&facts).Error; err != nil {
				return err
			}
			exhausted = len(facts) < tokenMeteringMigrationChunk
			if len(facts) == 0 && state.MigrationVersion == version {
				return nil
			}
			var delta int64
			yieldedForCollection := false
			for _, fact := range facts {
				if committed > 0 && m.tokenLocalWriteMu.collectionQueued() {
					yieldedForCollection = true
					break
				}
				state.MigrationCursor = fact.LogID
				if fact.RequestID == "" {
					knownSourceMissing := false
					for _, missing := range tokenMeteringDecodeMissing(fact.MissingFields) {
						knownSourceMissing = knownSourceMissing || missing.Name == "request_id" && (missing.Reason == "source_missing" || missing.Reason == "source_identifier_invalid")
					}
					if !knownSourceMissing {
						// The HMAC cannot recover a raw identifier. Request one bounded
						// source-window repair, persisted with this migration checkpoint.
						if err := tx.Exec(`INSERT OR IGNORE INTO token_metering_source_repairs_v1 VALUES(?,?,?)`, fact.SourceEpoch, fact.CreatedAt/300*300, now).Error; err != nil {
							return err
						}
					}
				}
				var p tokenMeteringParsed
				if len(fact.SourceFacts) == 0 || len(fact.SourceFacts) > tokenMeteringOtherBytes*2 || json.Unmarshal([]byte(fact.SourceFacts), &p) != nil {
					// Do not replace malformed source evidence with fabricated facts.
					continue
				}
				if p.Fields == nil {
					p.Fields = map[string]metering.Field{}
				}
				if tokenMeteringNeedsSourceReplay(fact, p) {
					if err := tokenMeteringRequestSourceReplay(tx, fact, now); err != nil {
						return err
					}
					continue // Retain the old parser stamp until actual source reread.
				}
				tokenMeteringDropUnverifiedContract(&p)
				p.Version, p.Adapter = tokenMeteringParserVersion, tokenMeteringEffectiveAdapter(m.cfg)
				p.PricingSupport = "operands_only_no_calculation"
				if p.Fields["billing_mode"].Value == "tiered_expr" {
					p.PricingSupport = "tiered_unsupported"
					if p.Fields["expr_b64"].State == "valid" && tokenMeteringFieldUsable(p.Fields["request_rules"]) {
						p.PricingSupport = "tiered_arithmetic_snapshot_only"
					}
				}
				normalizeTokenMetering(m.cfg, &p)
				sort.Slice(p.MissingFields, func(i, j int) bool { return p.MissingFields[i].Name < p.MissingFields[j].Name })
				body, err := json.Marshal(p)
				if err != nil {
					return err
				}
				missing := fact.MissingFields
				if fact.ParserVersion != tokenMeteringParserVersion || missing == "" {
					missing = tokenMeteringLegacyMissing(fact, p)
				}
				if string(body) == fact.SourceFacts && missing == fact.MissingFields && fact.ParserVersion == tokenMeteringParserVersion && fact.SourceEventID != "" {
					continue
				}
				var refs []TokenMeteringRef
				if err := tx.Where("source_epoch=? AND log_id=?", fact.SourceEpoch, fact.LogID).Find(&refs).Error; err != nil {
					return err
				}
				updated := fact
				updated.SourceFacts = string(body)
				updated.MissingFields = missing
				for _, ref := range refs {
					if ref.KeyID == "event-id" {
						updated.SourceEventID = ref.Hash
					}
				}
				if updated.SourceFacts == fact.SourceFacts && missing == fact.MissingFields && fact.ParserVersion == tokenMeteringParserVersion && updated.SourceEventID == fact.SourceEventID {
					continue
				}
				tokenMeteringSetFactHash(&updated, refs)
				result := tx.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=? AND revision=?", fact.SourceEpoch, fact.LogID, fact.Revision).
					Updates(map[string]any{"source_facts": updated.SourceFacts, "semantic": p.Semantic, "semantic_reason": p.SemanticReason,
						"source_event_id": updated.SourceEventID, "missing_fields": missing, "parser_version": tokenMeteringParserVersion, "revision": fact.Revision + 1, "fact_hash": updated.FactHash, "stored_bytes": updated.StoredBytes})
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return errors.New("token migration fact changed")
				}
				delta += updated.StoredBytes - fact.StoredBytes
				committed++
				if m.tokenLocalWriteMu.collectionQueued() {
					yieldedForCollection = true
					break
				}
			}
			// A collection waiter means this bounded migration prefix must hand
			// the writer back now.  The durable cursor below is the exact last
			// fact examined, so the remaining facts resume on the next turn.
			if yieldedForCollection {
				exhausted = false
			}
			bytes := any(gorm.Expr("fact_bytes + ?", delta))
			if tx.Migrator().HasTable(&tokenMeteringHealthCount{}) {
				ready, err := tokenMeteringHealthCountsReady(tx)
				if err != nil {
					return err
				}
				if ready {
					// v4 rewrote stored_bytes without adjusting the state ledger.
					// The independently maintained counter already includes this
					// transaction's changes; reconcile without scanning all facts.
					var actual int64
					if err := tx.Model(&tokenMeteringHealthCount{}).Where("source_epoch=? AND dimension='stored_bytes' AND value='all'", state.SourceEpoch).Select("COALESCE(SUM(count),0)").Scan(&actual).Error; err != nil {
						return err
					}
					bytes = actual
				}
			}
			return tx.Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).
				Updates(map[string]any{"migration_version": version, "migration_cursor": state.MigrationCursor, "fact_bytes": bytes}).Error
		})
		if err != nil {
			return changed, false, err
		}
		lastTurn = time.Since(turnStarted)
		changed += committed
		if exhausted {
			return changed, false, nil
		}
		// Give waiting collection/link/detection rounds a chance between commits.
		select {
		case <-ctx.Done():
			return changed, false, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return changed, true, nil
}
