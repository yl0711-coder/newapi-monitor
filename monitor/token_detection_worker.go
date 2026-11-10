package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"time"
)

var errTokenDetectionLease = errors.New("token detection lease lost")
var errTokenDetectionCapacity = errors.New("token detection capacity reached")
var errTokenDetectionFactRemoved = errors.New("token detection fact removed after queue read")
var errTokenDetectionPending = errors.New("token detection waiting for association")
var errTokenDetectionQueueNotReady = errors.New("token detection durable queue not ready")

func (m *Monitor) runTokenDetectionRound(ctx context.Context, now int64) (int, error) {
	processed, _, err := m.runTokenDetectionRoundProgress(ctx, now)
	return processed, err
}

// Report retained work explicitly. A round may yield before its deadline or
// exhaust its row budget; elapsed time cannot distinguish that from an idle
// queue. The scheduler must continue both cases without an extra idle poll.
func (m *Monitor) runTokenDetectionRoundProgress(ctx context.Context, now int64) (int, bool, error) {
	ctx = context.WithValue(ctx, tokenWriteOperationKey{}, "detection")
	if !m.cfg.TokenDetectionEnabled || m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return 0, false, nil
	}
	if !m.tokenDetectionMu.TryLock() {
		return 0, false, nil
	}
	defer m.tokenDetectionMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, tokenDetectionRoundSeconds*time.Second)
	defer cancel()
	p, err := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
	if err != nil {
		return 0, false, err
	}
	if err := m.ensureTokenDetectionCapacity(ctx); err != nil {
		return 0, false, err
	}
	epoch := m.cfg.TokenFactsSourceEpoch
	hash := tokenDetectionPolicyHash(p)
	leaseBytes := make([]byte, 16)
	if _, err := rand.Read(leaseBytes); err != nil {
		return 0, false, err
	}
	lease := hex.EncodeToString(leaseBytes)
	claimed := false
	err = m.tokenLocalWriteTransaction(ctx, func(db *gorm.DB) error {
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&TokenDetectionState{SourceEpoch: epoch}).Error; err != nil {
			return err
		}
		claim := db.Model(&TokenDetectionState{}).Where("source_epoch = ?", epoch)
		if m.tokenDetectionFinishedLease == "" {
			claim = claim.Where("lease_until <= ?", now)
		} else {
			// The previous serialized local round has ended even if its bounded
			// release could not obtain the writer. Renew only that exact owner;
			// a new process or a changed durable token still waits for expiration.
			claim = claim.Where("lease_until <= ? OR lease_token = ?", now, m.tokenDetectionFinishedLease)
		}
		acquired := claim.Updates(map[string]any{"lease_token": lease, "lease_until": now + 30, "policy_hash": hash})
		claimed = acquired.RowsAffected > 0
		return acquired.Error
	})
	if err != nil || !claimed {
		return 0, false, err
	}
	defer func() { m.tokenDetectionFinishedLease = lease }()
	// Queue reads do not need to reserve the writer. A retained backlog must
	// not exclude collection and association for the entire detection round.
	var jobs []TokenMeteringFact
	var windowProgress tokenDetectionWindowProgress
	runErr := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
		var err error
		jobs, err = tokenDetectionJobsBounded(reader, epoch, hash, now, m.cfg.tokenProcessingBatchRows(), m.cfg.tokenProcessingPollSeconds(), &windowProgress)
		return err
	})
	if runErr == nil {
		jobs, runErr = m.claimTokenDetectionJobs(ctx, jobs, lease, now)
	}
	processed := 0
	visited := 0
	checkpointedJobs := []TokenMeteringFact{}
	// Keep bounded-round accounting separate from last_reason.  A handoff
	// caused by the row/time budget is yielded work; deferred jobs and actual
	// SQL/data errors are different health categories.
	yielded := false
	failed := int64(0)
	deferredJobs := int64(0)
	capacityOnly := runErr == nil
	if runErr != nil {
		var waitErr *tokenLocalWaitError
		switch {
		case errors.As(runErr, &waitErr), errors.Is(runErr, context.DeadlineExceeded), errors.Is(runErr, errTokenDetectionQueueNotReady):
			yielded = true
		default:
			failed++
		}
	}
	if runErr == nil && !m.tokenMeteringDiskOK() {
		runErr = errTokenDetectionCapacity
		deferredJobs++
	}
	if runErr == nil {
		// One immediate reprepare per identity absorbs a dependency change
		// within this batch. Further changes remain durable for the next turn.
		reprepared := map[int64]bool{}
		lastDuration := time.Duration(0)
		lastWriteDuration := time.Duration(0)
		lastRows := 0
		chunkRows := m.tokenDetectionCommitRows
		if chunkRows == 0 {
			chunkRows = 4
		}
		chunkRows = min(m.cfg.tokenDetectionCommitLimit(), max(1, chunkRows))
		for start := 0; start < len(jobs); {
			if ctx.Err() != nil {
				capacityOnly = false
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					yielded = true
				} else {
					failed++
				}
				runErr = errors.Join(runErr, ctx.Err())
				break
			}
			// Preparing the next batch also spends this round's deadline. When
			// the learned batch grows, reserve its larger cost before preparing
			// it; the preceding transaction alone understates that next turn.
			nextDuration := lastDuration
			if lastRows > 0 && chunkRows > lastRows {
				nextDuration = lastDuration * time.Duration(chunkRows) / time.Duration(lastRows)
			}
			if !tokenLocalWriteBudgetAvailable(ctx, nextDuration) {
				yielded = true
				break
			}
			// A queued collection turn is a durable source-cursor opportunity.
			// Yield before taking another detection transaction; the current
			// detection prefix is already committed and the next round will read
			// the same durable queue entry again.
			if m.tokenLocalWriteMu.collectionQueued() {
				yielded = true
				break
			}
			// Short local contention consumes the existing round budget rather
			// than adding an entire idle poll interval to a retained backlog.
			committed := 0
			chunkCheckpointed := []TokenMeteringFact{}
			var deferredErr error
			end := min(start+chunkRows, len(jobs))
			batchStarted := time.Now()
			plans, planErrors, prepareErr := m.prepareTokenDetectionBatch(ctx, jobs[start:end], p, lease, now)
			if prepareErr != nil {
				runErr = errors.Join(runErr, prepareErr)
				if errors.Is(prepareErr, context.DeadlineExceeded) || ctx.Err() != nil {
					yielded = true
				} else {
					failed++
				}
				break
			}
			// A slow read can consume the reservation without changing any
			// durable result. Yield before beginning a write that cannot finish.
			nextWriteDuration := lastWriteDuration
			if lastRows > 0 && end-start > lastRows {
				nextWriteDuration = lastWriteDuration * time.Duration(end-start) / time.Duration(lastRows)
			}
			if !tokenLocalWriteBudgetAvailable(ctx, nextWriteDuration) {
				yielded = true
				break
			}
			workBudget := m.tokenDetectionWorkBudget
			if workBudget < 250*time.Millisecond || workBudget > 750*time.Millisecond {
				workBudget = 250 * time.Millisecond
			}
			workBudget = min(workBudget, time.Duration(m.cfg.tokenDetectionWorkMS())*time.Millisecond)
			// Small transactions amortize WAL bookkeeping without holding the
			// writer for a whole 250-row round. A failed job is rolled back to
			// its savepoint before a capacity retry is recorded. Count progress
			// only after the enclosing transaction commits.
			timing := &tokenWriteTiming{}
			turnStarted := time.Now()
			err := m.tokenLocalWriteTransaction(context.WithValue(ctx, tokenWriteTimingKey{}, timing), func(tx *gorm.DB) error {
				workStarted := time.Now()
				for offset, job := range jobs[start:end] {
					if offset > 0 && !tokenLocalWriteBudgetAvailable(ctx, time.Since(workStarted)/time.Duration(offset)) {
						end = start + offset
						yielded = true
						break
					}
					if err := tx.SavePoint("token_detection_job").Error; err != nil {
						return err
					}
					err := planErrors[offset]
					if err == nil {
						err = m.processTokenDetectionTx(tx, job.SourceEpoch, job.LogID, p, lease, now, plans[offset])
					}
					if errors.Is(err, errTokenDetectionInputsChanged) {
						// No decision or retry was written. Its existing durable queue
						// identity remains due and will be prepared from a fresh snapshot.
						deferredJobs++
						yielded = true
						if !reprepared[job.LogID] {
							reprepared[job.LogID] = true
							jobs = append(jobs, job)
						}
						if err := tx.RollbackTo("token_detection_job").Error; err != nil {
							return err
						}
						if err := tx.Exec("RELEASE SAVEPOINT token_detection_job").Error; err != nil {
							return err
						}
						continue
					}
					if errors.Is(err, errTokenDetectionFactRemoved) {
						// Retention can remove a fact after the queue snapshot while
						// this worker yields the writer. No decision was made for
						// that identity; keep processing the other retained jobs.
					} else if err != nil {
						if !errors.Is(err, errTokenDetectionCapacity) && !errors.Is(err, errTokenDetectionPending) {
							return err
						}
						deferredJobs++
						if err := tx.RollbackTo("token_detection_job").Error; err != nil {
							return err
						}
						if err := m.deferTokenDetectionTx(tx, job, hash, lease, now); err != nil {
							return err
						}
						if errors.Is(err, errTokenDetectionCapacity) {
							deferredErr = errors.Join(deferredErr, errTokenDetectionCapacity)
						}
					} else {
						committed++
					}
					if err := tx.Exec("RELEASE SAVEPOINT token_detection_job").Error; err != nil {
						return err
					}
					chunkCheckpointed = append(chunkCheckpointed, job)
					// Keep a bounded cheap prefix when collection arrives, so an
					// expensive fixed COMMIT is shared without holding the writer
					// for the entire learned batch ahead of the source checkpoint.
					if m.tokenLocalWriteMu.yieldBatchToCollection(workStarted) {
						end = start + offset + 1
						break
					}
					// An individual reference lookup can become expensive after
					// a previously cheap batch. Yield at the next atomic job,
					// instead of holding the writer for all remembered rows before
					// learning a smaller size. Only these committed jobs are visited.
					if time.Since(workStarted) >= workBudget {
						end = start + offset + 1
						break
					}
				}
				if len(chunkCheckpointed) > 0 {
					// Publish success with the decisions themselves. A later lease
					// release failure must not hide this durable prefix, and a round
					// that merely yielded before its first job must not advance it.
					fields := map[string]any{}
					if committed > 0 {
						fields["last_success_at"] = now
					}
					if at, id, ok := tokenDetectionRecentCommittedPrefix(windowProgress.RecentDiscovery, append(checkpointedJobs, chunkCheckpointed...)); ok && windowProgress.RecentDiscovery != nil {
						fields["recent_scan_at"], fields["recent_scan_id"] = at, id
						fields["history_scan_at"] = gorm.Expr("CASE WHEN scan_policy_hash=? THEN history_scan_at ELSE 0 END", hash)
						fields["history_scan_id"] = gorm.Expr("CASE WHEN scan_policy_hash=? THEN history_scan_id ELSE 0 END", hash)
						fields["scan_policy_hash"] = hash
					}
					if committed > 0 && windowProgress.Fresh > 0 {
						fields["fresh_window_at"] = windowProgress.Fresh
					}
					if committed > 0 && windowProgress.Correction > 0 {
						fields["correction_window_at"] = windowProgress.Correction
					}
					if len(fields) == 0 {
						return nil
					}
					saved := tx.Model(&TokenDetectionState{}).Where("source_epoch=? AND lease_token=?", epoch, lease).Updates(fields)
					if saved.Error != nil {
						return saved.Error
					}
					if saved.RowsAffected != 1 {
						return errTokenDetectionLease
					}
				}
				return nil
			})
			if err != nil {
				capacityOnly = false
				var waitErr *tokenLocalWaitError
				if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &waitErr) || tokenSQLiteBusy(err) {
					yielded = true
				} else if !errors.Is(err, errTokenDetectionCapacity) && !errors.Is(err, errTokenDetectionPending) {
					failed++
				}
				runErr = errors.Join(runErr, err)
				break
			}
			lastWriteDuration = time.Since(turnStarted)
			lastDuration = time.Since(batchStarted)
			lastRows = end - start
			checkpointedJobs = append(checkpointedJobs, chunkCheckpointed...)
			processed += committed
			runErr = errors.Join(runErr, deferredErr)
			start = end
			visited = end
			// Size SQL work separately from the fixed commit cost. Retain the
			// learned size across rounds; restarting at four rows repeatedly
			// pays WAL commits while fresh identities accumulate. The round's
			// row/time limits and FIFO handoff remain unchanged.
			if timing.Work > workBudget {
				chunkRows = max(1, chunkRows/2)
			} else if timing.Work <= 250*time.Millisecond {
				// A fixed slow commit must not collapse cheap SQL to one row per
				// transaction. Residency is bounded by the row/work caps above;
				// the next-turn guard still includes actual wait and commit time.
				chunkRows = min(m.cfg.tokenDetectionCommitLimit(), chunkRows*2)
			}
			m.tokenDetectionCommitRows = chunkRows
			// Keep the work budget adaptive for expensive evidence lookups; the
			// commit-row cap above independently bounds SQLite writer residency.
			if committed > 0 && timing.Commit > 400*time.Millisecond {
				perRow := timing.Work / time.Duration(committed)
				if perRow >= 40*time.Millisecond {
					m.tokenDetectionWorkBudget = 250 * time.Millisecond
				} else {
					m.tokenDetectionWorkBudget = min(750*time.Millisecond, max(250*time.Millisecond, perRow*20))
				}
			} else if timing.Work > workBudget {
				m.tokenDetectionWorkBudget = max(250*time.Millisecond, workBudget/2)
			}
			if start < len(jobs) && !waitSourceLifecycle(ctx, time.Millisecond) {
				break
			}
		}
	}
	if runErr == nil && visited < len(jobs) {
		// All exits with unvisited durable queue identities are bounded handoffs
		// (writer fairness, time or row budget). Preserve that fact explicitly.
		yielded = true
	}
	// SQLite may expose deadline cancellation as SQLITE_INTERRUPT rather than
	// wrapping the context error. Preserve the actual round cancellation so
	// the scheduler can resume committed partial work at its catch-up cadence.
	if runErr != nil && ctx.Err() != nil {
		runErr = errors.Join(runErr, ctx.Err())
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			yielded = true
		}
	}
	// Release has a separate bounded context so a timeout cannot pin the lease.
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer finishCancel()
	lastYielded := int64(0)
	if yielded {
		lastYielded = 1
	}
	fields := map[string]any{"lease_until": 0, "lease_token": "", "last_reason": "",
		"last_processed": int64(processed), "last_yielded": lastYielded,
		"last_failed": failed, "last_deferred": deferredJobs}
	if runErr == nil && visited == len(jobs) && windowProgress.AdvanceScans {
		fields["history_scan_at"], fields["history_scan_id"] = windowProgress.HistoryScanAt, windowProgress.HistoryScanID
		fields["scan_policy_hash"] = windowProgress.ScanPolicyHash
	}
	if windowProgress.RecentDiscovery != nil {
		windowProgress.RecentScanAt, windowProgress.RecentScanID, windowProgress.AdvanceRecentScan = tokenDetectionRecentCommittedPrefix(windowProgress.RecentDiscovery, checkpointedJobs)
	}
	if runErr == nil && visited == len(jobs) && windowProgress.AdvanceRecentScan {
		fields["recent_scan_at"], fields["recent_scan_id"] = windowProgress.RecentScanAt, windowProgress.RecentScanID
		if !windowProgress.AdvanceScans {
			// Publishing the recent policy cursor must not accidentally bless a
			// historical discovery prefix from the preceding policy version.
			fields["history_scan_at"] = gorm.Expr("CASE WHEN scan_policy_hash=? THEN history_scan_at ELSE 0 END", windowProgress.ScanPolicyHash)
			fields["history_scan_id"] = gorm.Expr("CASE WHEN scan_policy_hash=? THEN history_scan_id ELSE 0 END", windowProgress.ScanPolicyHash)
		}
		fields["scan_policy_hash"] = windowProgress.ScanPolicyHash
	}
	var deferred bool
	var deferredReason string
	// Finishing needs only retry existence, not a count of the retained queue.
	// Keep this bounded probe out of the background read queue so a counter
	// audit cannot consume the lease-release deadline before release starts.
	if err := m.tokenReadStore().WithContext(finishCtx).Raw("SELECT EXISTS(SELECT 1 FROM token_detection_retries WHERE source_epoch=? LIMIT 1)", epoch).Scan(&deferred).Error; err != nil {
		capacityOnly = false
		runErr = errors.Join(runErr, err)
	}
	if deferred {
		_ = m.tokenReadStore().WithContext(finishCtx).Raw("SELECT reason FROM token_detection_retries WHERE source_epoch=? ORDER BY next_at LIMIT 1", epoch).Scan(&deferredReason).Error
	}
	if runErr == nil && deferred {
		if deferredReason == "" {
			deferredReason = "local_capacity_exhausted"
		}
		fields["last_reason"] = deferredReason
	} else if runErr == nil {
		// Successful job transactions already published their success time.
	} else if capacityOnly && errors.Is(runErr, errTokenDetectionCapacity) {
		fields["last_reason"] = "local_capacity_exhausted"
	} else {
		fields["last_reason"] = "local_detection_incomplete"
	}
	var affected int64
	finishErr := m.tokenLocalWriteTransaction(finishCtx, func(tx *gorm.DB) error {
		saved := tx.Model(&TokenDetectionState{}).Where("source_epoch = ? AND lease_token = ?", epoch, lease).Updates(fields)
		affected = saved.RowsAffected
		if saved.Error != nil {
			return saved.Error
		}
		return tx.Exec("DELETE FROM token_detection_claims_v1 WHERE source_epoch=? AND lease_token=?", epoch, lease).Error
	})
	if finishErr != nil {
		// GORM may wrap a canceled commit as ErrTxDone and lose the original
		// deadline identity. Keep both the work failure and the finish budget
		// so the scheduler can continue durable progress without hiding faults.
		return processed, visited < len(jobs), errors.Join(runErr, finishErr, finishCtx.Err())
	}
	if affected == 0 {
		return processed, visited < len(jobs), errTokenDetectionLease
	}
	return processed, yielded || visited < len(jobs) || windowProgress.ScanMore || len(jobs) == m.cfg.tokenProcessingBatchRows(), runErr
}
func tokenDetectionJobs(db *gorm.DB, epoch, hash string, now int64) ([]TokenMeteringFact, error) {
	return tokenDetectionJobsBounded(db, epoch, hash, now, 50, 60)
}

// Health needs only a bounded sample, not four ordered scheduling lanes and
// their fairness sort. Include stale work and due retries/rechecks without
// hiding capacity backoff or changing the worker's scheduling policy.
func tokenDetectionPendingSample(db *gorm.DB, epoch, hash string, now int64, limit int) ([]TokenMeteringFact, error) {
	if db.Migrator().HasTable(tokenReportPendingMembers) {
		return tokenReportPendingSample(db, epoch, hash, now, limit)
	}
	return nil, errTokenDetectionQueueNotReady
}

func tokenDetectionIdentityFactTable(db *gorm.DB) string {
	table := "token_metering_facts AS f"
	if db.Migrator().HasIndex(&TokenMeteringFact{}, "idx_token_detection_fact_identity") {
		table += " INDEXED BY idx_token_detection_fact_identity"
	}
	return table
}

func tokenDetectionJobsBounded(db *gorm.DB, epoch, hash string, now int64, limit, cadence int, progress ...*tokenDetectionWindowProgress) ([]TokenMeteringFact, error) {
	if db.Migrator().HasIndex(tokenReportPendingMembers, "idx_token_detection_pending_fresh_v1") {
		return tokenDetectionIndexedJobs(db, epoch, hash, now, limit, cadence, progress...)
	}
	// A missing durable projection is a migration dependency, never a reason
	// to fall back to scanning the retained fact table on every turn.
	return nil, errTokenDetectionQueueNotReady
}

func tokenDetectionInterleaveJobs(lanes [][]TokenMeteringFact, now int64, limit, cadence int, latest ...bool) []TokenMeteringFact {
	// A new fact can need another decision after association completes. When
	// those recent corrections are present, reserve equal primary shares for
	// them and first evaluations. Otherwise keep the larger first-evaluation
	// share that drains an old headless backlog. Retries/reviews retain their
	// smaller independent shares; the total row/time budgets do not change.
	// Rotate the first opportunity so even a short round services every lane.
	first := int(now / int64(cadence) % int64(len(lanes)))
	if len(latest) > 0 && latest[0] {
		first = int(now / int64(max(cadence, 1)) % 2)
	}
	result := make([]TokenMeteringFact, 0, limit)
	positions := make([]int, len(lanes))
	// A correction can appear in both the changed-head and due-retry lanes
	// during the same scheduling snapshot.  Consume each durable identity once;
	// otherwise one task can occupy two budget slots and the same fact can be
	// processed twice in a round after a restart.
	seen := make(map[string]struct{}, limit)
	order := make([]int, 0, len(lanes)+4)
	for offset := range lanes {
		order = append(order, (first+offset)%len(lanes))
	}
	if len(latest) > 0 && latest[0] && len(lanes) == 4 {
		order = []int{first, 1 - first, 2, 3}
	}
	if len(lanes) == 4 {
		recentCorrection := false
		for _, job := range lanes[1] {
			if job.CreatedAt >= now-3600 && job.CreatedAt <= now {
				recentCorrection = true
				break
			}
		}
		if recentCorrection {
			order = append(order, 0, 1, 0, 1, 0, 1, 0, 1)
		} else {
			order = append(order, 0, 1, 0, 0, 0, 0, 0, 0)
		}
	}
	for len(result) < limit {
		advanced := false
		for _, lane := range order {
			if len(result) < limit && positions[lane] < len(lanes[lane]) {
				job := lanes[lane][positions[lane]]
				positions[lane]++
				advanced = true
				identity := job.SourceEpoch + "\x00" + strconv.FormatInt(job.LogID, 10)
				if _, exists := seen[identity]; exists {
					continue
				}
				seen[identity] = struct{}{}
				job.CreatedAt = 0 // Work items carry identity, not a stale fact copy.
				result = append(result, job)
			}
		}
		if !advanced {
			break
		}
	}
	return result
}
func (m *Monitor) processTokenDetection(ctx context.Context, epoch string, id int64, p tokenDetectionPolicy, lease string, now int64) error {
	if err := m.ensureTokenDetectionCapacity(ctx); err != nil {
		return err
	}
	var prepared *tokenDetectionPrepared
	if err := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
		return reader.Transaction(func(tx *gorm.DB) error {
			var err error
			prepared, err = prepareTokenDetection(tx, epoch, id, p, now)
			return err
		})
	}); err != nil {
		return err
	}
	return m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		return m.processTokenDetectionTx(tx, epoch, id, p, lease, now, prepared)
	})
}

func (m *Monitor) processTokenDetectionTx(tx *gorm.DB, epoch string, id int64, p tokenDetectionPolicy, lease string, now int64, a *tokenDetectionPrepared) error {
	maxRows, maxBytes := m.cfg.tokenDetectionLimits()
	maxReferences, maxReferenceRecords := m.cfg.tokenDetectionReferenceLimits()
	// The write fence precedes all reads; SQLite serializes this short local tx.
	fence := tx.Model(&TokenDetectionState{}).Where("source_epoch = ? AND lease_token = ? AND lease_until > ?", epoch, lease, now).Update("policy_hash", tokenDetectionPolicyHash(p))
	if fence.Error != nil {
		return fence.Error
	}
	if fence.RowsAffected != 1 {
		return errTokenDetectionLease
	}
	if err := a.validate(tx); err != nil {
		return err
	}
	fact, in, current, previous := a.Fact, a.Input, a.Current, a.Previous
	if tokenMeteringNeedsSourceReplay(fact, in.Usage) {
		if err := tokenMeteringRequestSourceReplay(tx, fact, now); err != nil {
			return err
		}
	}
	generation, complete, stage, ref := a.Generation, a.Complete, a.Stage, a.Reference
	policyHash := tokenDetectionPolicyHash(p)
	if a.Unchanged {
		if a.ClearTimer {
			if err := tx.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=?", epoch, id).Update("next_at", 0).Error; err != nil {
				return err
			}
		}
		return tx.Where("source_epoch=? AND log_id=?", epoch, id).Delete(&TokenDetectionRetry{}).Error
	}
	if ref.ParentID != "" {
		switch ref.Reason {
		case "anchor_candidate_isolated", "anchor_evidence_version_unavailable", "anchor_evidence_changed_requires_review", "anchor_source_missing":
			if err := invalidateTokenReference(tx, ref.ParentID, ref.Reason, now); err != nil {
				return err
			}
		}
	}
	if ref.Status == "statistical" {
		// A non-empty ID means tokenReferenceFrom reused an existing frozen
		// anchor. Keep that identity; hashing it again would create a child row
		// and a duplicate decision revision even though no evidence changed.
		if a.NewReference {
			row := TokenDetectionReference{ID: ref.ID, Bucket: ref.Bucket, Through: ref.Through, RecordedAt: now, ExpiresAt: now + min(int64(30*86400), m.cfg.tokenDetectionRetentionSeconds()), SnapshotJSON: tokenDetectionJSON(ref)}
			row.StoredBytes = int64(len(row.SnapshotJSON))
			var found int64
			if err := tx.Model(&TokenDetectionReference{}).Where("id = ?", row.ID).Count(&found).Error; err != nil {
				return err
			}
			if found == 0 {
				var count int64
				if err := tokenDetectionActiveReferences(tx, now).Count(&count).Error; err != nil {
					return err
				}
				if count >= maxReferences {
					return errTokenDetectionCapacity
				}
				// Compact anchor markers do not spend the live snapshot quota.
				if err := tx.Model(&TokenDetectionReference{}).Count(&count).Error; err != nil {
					return err
				}
				if count >= maxReferenceRecords {
					return errTokenDetectionCapacity
				}
				if err := tx.Create(&row).Error; err != nil {
					return err
				}
			}
		}
	}
	result, brief, body, hash := a.Frozen.Result, a.Brief, a.Body, a.Hash
	var capacity tokenDetectionCapacity
	if err := tx.First(&capacity, 1).Error; err != nil {
		return err
	}
	if previous == nil {
		if capacity.CurrentRows >= maxRows {
			return errTokenDetectionCapacity
		}
	}
	if hash != current.DecisionHash {
		if capacity.RevisionBytes+capacity.ReferenceBytes+capacity.HeadBytes+int64(len(body)+len(tokenDetectionJSON(brief))-len(current.ResultJSON)) > maxBytes {
			return errTokenDetectionCapacity
		}
		revision := TokenDetectionRevision{CompactStorage: m.tokenDB != nil, SourceEpoch: epoch, LogID: id, Revision: current.Revision + 1, DecisionHash: hash, RecordedAt: now, ExpiresAt: m.cfg.tokenDetectionExpiresAt(current.FirstEvaluatedAt, now), FrozenJSON: body, StoredBytes: int64(len(body))}
		if err := tx.Create(&revision).Error; err != nil {
			return err
		}
		if err := saveTokenRevisionMembers(tx, revision); err != nil {
			return err
		}
		current.Revision = revision.Revision
		current.DecisionHash = hash
	}
	changed := current.Revision == 1 && current.FirstEvaluatedAt == 0 || current.FactRevision != fact.Revision || current.LinkRevision != fact.LinkRevision || current.PolicyHash != policyHash || (!current.Quarantined && result.Quarantined)
	referenceBucket, referenceGeneration, err := m.tokenDetectionReferenceScope(tx, in, p, previous, generation, changed, result.Quarantined, now)
	if err != nil {
		return err
	}
	current.ReferenceBucket = referenceBucket
	current.ReferenceGeneration = referenceGeneration
	current.SourceEpoch = epoch
	current.LogID = id
	current.CreatedAt = fact.CreatedAt
	current.UserID = fact.UserID
	current.ChannelID = fact.ChannelID
	current.Model = fact.Model
	current.UserGroup = fact.UserGroup
	current.FactRevision = fact.Revision
	current.LinkRevision = fact.LinkRevision
	current.PolicyHash = policyHash
	current.Stage = stage
	current.SourceComplete = complete
	current.Bucket = ref.Bucket
	current.CacheSignal = result.CacheSignal
	current.Candidate = result.Candidate
	current.Quarantined = current.Quarantined || result.Quarantined
	assessment := a.Assessment
	// Persist the user-visible state alongside the idempotent head.  This keeps
	// retention and health queries from reparsing frozen JSON and, unlike an
	// empty legacy value, makes an unfinished/unknown head explicitly pending.
	current.DetectionState = assessment.State
	if current.DetectionState == "local_formula_consistent" && !result.Candidate {
		current.DetectionState = "not_hit"
		for _, rule := range result.Rules {
			if !rule.Eligible {
				current.DetectionState = "partially_evaluated"
				break
			}
		}
	}
	current.ResultJSON = tokenDetectionJSON(brief)
	current.EvaluatedAt = now
	current.ExpiresAt = m.cfg.tokenDetectionExpiresAt(current.FirstEvaluatedAt, now)
	if current.FirstEvaluatedAt == 0 {
		current.FirstEvaluatedAt = now
	}
	current.NextAt = 0
	// A timer cannot supply missing request identity, usage semantics or entry
	// length. Source/link/policy/coverage changes already requeue this identity.
	// Reserve staged reference expansion for requests with comparable evidence,
	// instead of repeatedly rewriting the same insufficient result while fresh
	// requests are waiting for their first evaluation.
	if stage < 3 && tokenComparable(in) {
		delays := []int64{300, 600, 3600}
		current.NextAt = max(now+60, fact.CreatedAt+delays[stage])
	}
	if err := saveTokenDetectionCurrent(tx, previous, current); err != nil {
		return err
	}
	if previous != nil && previous.ReferenceBucket != current.ReferenceBucket {
		if err := cleanupTokenDetectionBucketScopes(tx, []string{previous.ReferenceBucket}); err != nil {
			return err
		}
	}
	return tx.Where("source_epoch = ? AND log_id = ?", epoch, id).Delete(&TokenDetectionRetry{}).Error
}
func (m *Monitor) cleanupTokenDetection(ctx context.Context, now int64) error {
	ctx = context.WithValue(ctx, tokenWriteOperationKey{}, "detection_cleanup")
	if m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return nil
	}
	var authorized bool
	if err := m.tokenReadStore().WithContext(ctx).Raw(`SELECT EXISTS(SELECT 1 FROM token_metering_states
	 WHERE source_epoch=? AND cleanup_backup_id<>'' AND cleanup_verified_at>0 AND cleanup_safe_before>0
	 AND cleanup_stable_since>0 AND cleanup_verified_at-cleanup_stable_since>=1800 AND cleanup_next_at<=?)`, m.cfg.TokenFactsSourceEpoch, now).Scan(&authorized).Error; err != nil {
		return err
	}
	if !authorized {
		return nil
	}
	if err := m.cleanupTokenReferences(ctx, now); err != nil {
		return err
	}
	var position struct{ CursorAt, CursorID, NextAt int64 }
	if err := m.tokenReadStore().WithContext(ctx).Table("token_detection_cleanup_v1").Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Limit(1).Find(&position).Error; err != nil {
		return err
	}
	if position.NextAt > now {
		return nil
	}
	// Read a bounded keyset prefix before checking eligibility. Filtering first
	// could walk every protected pending row on every maintenance invocation.
	var expired []TokenDetectionCurrent
	if err := m.tokenReadStore().WithContext(ctx).Select("source_epoch,log_id,expires_at").
		Where("source_epoch=? AND expires_at<? AND (expires_at,log_id)>(?,?)", m.cfg.TokenFactsSourceEpoch, now, position.CursorAt, position.CursorID).
		Order("expires_at,log_id").Limit(m.cfg.tokenProcessingBatchRows()).Find(&expired).Error; err != nil {
		return err
	}
	if len(expired) == 0 {
		return m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
			return tx.Exec(`INSERT INTO token_detection_cleanup_v1(source_epoch,next_at,updated_at) VALUES(?,?,?)
		 ON CONFLICT(source_epoch) DO UPDATE SET cursor_at=0,cursor_id=0,next_at=excluded.next_at,updated_at=excluded.updated_at,last_reason='pass_complete'`, m.cfg.TokenFactsSourceEpoch, now+300, now).Error
		})
	}
	// Share a fixed WAL commit across several bounded identity statements.
	// The sweep still reads at most the configured row budget and keeps its
	// two-second context. Check work time/collection handoff between each
	// 32-row statement group instead of imposing one COMMIT per group: at a
	// full result store that fixed overhead can free fewer slots than arrive.
	const maxCleanupRows = 256
	const cleanupStatementRows = 32
	const cleanupWorkBudget = 250 * time.Millisecond
	chunkRows := min(maxCleanupRows, m.cfg.tokenProcessingBatchRows(), int(m.tokenDetectionCleanupRows.Load()))
	if chunkRows <= 0 {
		chunkRows = tokenCleanupChunk
	}
	var lastTurn time.Duration
	for start := 0; start < len(expired); {
		if start > 0 && m.tokenLocalWriteMu.busy() {
			return nil
		}
		if m.tokenLocalWriteMu.collectionQueued() {
			return nil
		}
		if start > 0 && !tokenLocalWriteBudgetAvailable(ctx, lastTurn) {
			return nil
		}
		turnStarted := time.Now()
		end := min(start+chunkRows, len(expired))
		timing := &tokenWriteTiming{}
		if err := m.tokenLocalWriteTransaction(context.WithValue(ctx, tokenWriteTimingKey{}, timing), func(tx *gorm.DB) error {
			workStarted := time.Now()
			for offset := start; offset < end; {
				if offset > start && (time.Since(workStarted) >= cleanupWorkBudget || m.tokenLocalWriteMu.yieldBatchToCollection(workStarted)) {
					end = offset
					break
				}
				epoch := expired[offset].SourceEpoch
				through := offset + 1
				for through < end && through-offset < cleanupStatementRows && expired[through].SourceEpoch == epoch {
					through++
				}
				ids := make([]int64, 0, through-offset)
				for _, row := range expired[offset:through] {
					ids = append(ids, row.LogID)
				}
				// Recheck expiry and retained facts under this writer fence, in
				// one bounded identity query. A renewed head or restored fact is
				// excluded. Both deletions and all ledgers still commit together.
				var heads []TokenDetectionCurrent
				if err := tx.Model(&TokenDetectionCurrent{}).Select("log_id,reference_bucket").
					Where("source_epoch=? AND log_id IN ?", epoch, ids).
					Where(tokenDetectionCleanupEligible, now, now-4*86400, now, now).Scan(&heads).Error; err != nil {
					return err
				}
				offset = through
				last := expired[through-1]
				if err := tx.Exec(`INSERT INTO token_detection_cleanup_v1(source_epoch,cursor_at,cursor_id,updated_at) VALUES(?,?,?,?)
				 ON CONFLICT(source_epoch) DO UPDATE SET cursor_at=excluded.cursor_at,cursor_id=excluded.cursor_id,updated_at=excluded.updated_at,last_reason=''`, epoch, last.ExpiresAt, last.LogID, now).Error; err != nil {
					return err
				}
				if len(heads) == 0 {
					continue
				}
				safe, scopes := make([]int64, 0, len(heads)), make([]string, 0, len(heads))
				for _, head := range heads {
					safe = append(safe, head.LogID)
					scopes = append(scopes, head.ReferenceBucket)
				}
				if err := tx.Where("source_epoch=? AND log_id IN ?", epoch, safe).Delete(&TokenDetectionRevision{}).Error; err != nil {
					return err
				}
				if err := tx.Where("source_epoch=? AND log_id IN ?", epoch, safe).Delete(&TokenDetectionCurrent{}).Error; err != nil {
					return err
				}
				if err := cleanupTokenDetectionBucketScopes(tx, scopes); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			if timing.Work > cleanupWorkBudget {
				m.tokenDetectionCleanupRows.Store(int64(max(1, chunkRows/2)))
			}
			return err
		}
		lastTurn = time.Since(turnStarted)
		start = end
		// Keep the original two-second sweep budget and fair handoff. Fixed
		// WAL commit latency must not impose a permanent eight-row throughput
		// ceiling while expired results consume the unchanged capacity limit.
		if timing.Work > cleanupWorkBudget {
			chunkRows = max(1, chunkRows/2)
		} else {
			chunkRows = min(maxCleanupRows, m.cfg.tokenProcessingBatchRows(), chunkRows*2)
		}
		m.tokenDetectionCleanupRows.Store(int64(chunkRows))
	}
	if m.tokenLocalWriteMu.collectionQueued() {
		return nil
	}
	if lastTurn > 0 && !tokenLocalWriteBudgetAvailable(ctx, lastTurn) {
		return nil
	}
	// Reference expiration changes eligibility, not retention. Frozen decisions
	// and pending repairs may still reference the payload. Retry rows are only
	// removed by successful processing of their identity, never by age alone.
	return nil
}
