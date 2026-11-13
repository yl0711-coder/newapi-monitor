package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

type tokenMeteringCandidate struct {
	EventID             string   `json:"event_id"`
	ObservationEventIDs []string `json:"observation_event_ids,omitempty"`
	Node                string   `json:"node"`
	KeyID               string   `json:"key_id"`
	EventMS             int64    `json:"event_ms"`
	Status              int      `json:"status"`
	Route               string   `json:"route"`
	Method              string   `json:"method"`
	Length              *int64   `json:"request_length"`
	LengthState         string   `json:"request_length_state"`
	LengthVersion       string   `json:"request_length_version"`
}

func tokenMeteringRefKey(key, hash string) string { return key + "\x00" + hash }

const tokenMeteringLinkCommitRows = 4
const tokenMeteringLinkMaxCommitRows = 32

// This worker never invokes a source API. Queries are batched by indexed HMAC;
// a cap is a coverage gap, not permission to choose one of the returned rows.
func (m *Monitor) linkTokenMeteringBatch(ctx context.Context, now int64) (int, error) {
	count, _, err := m.linkTokenMeteringBatchProgress(ctx, now)
	return count, err
}

func (m *Monitor) linkTokenMeteringBatchProgress(ctx context.Context, now int64) (int, bool, error) {
	ctx = context.WithValue(ctx, tokenWriteOperationKey{}, "association")
	if !m.cfg.TokenFactsEnabled || m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return 0, false, nil
	}
	// Already queued recent identities get the first budget. A failing historical
	// change-feed bootstrap must not prevent these exact-identity lookups.
	count, more, err := m.linkTokenMeteringFactsBatchProgress(ctx, now)
	if ctx.Err() != nil {
		return count, true, errors.Join(err, ctx.Err())
	}
	evidenceMore, refreshErr := m.refreshTokenMeteringEvidence(ctx, now)
	if count == 0 && err == nil && refreshErr == nil && ctx.Err() == nil {
		// An otherwise idle turn can consume newly reopened exact identities
		// immediately, including previously expired links. No budget is added.
		count, more, err = m.linkTokenMeteringFactsBatchProgress(ctx, now)
	}
	return count, more || evidenceMore, errors.Join(err, refreshErr)
}

func (m *Monitor) linkTokenMeteringFactsBatchProgress(ctx context.Context, now int64) (int, bool, error) {
	// At most three HMAC profiles per fact: keep the existing 1000-row
	// evidence/owner cap meaningful and stay below SQLite's expression limit.
	var facts []TokenMeteringFact
	refs := []TokenMeteringRef{}
	err := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
		var err error
		facts, err = tokenMeteringLinkJobs(reader, m.cfg.TokenFactsSourceEpoch, now, min(m.cfg.tokenProcessingBatchRows(), 200), m.cfg.tokenProcessingPollSeconds())
		if err != nil || len(facts) == 0 {
			return err
		}
		idsByEpoch := map[string][]int64{}
		for _, f := range facts {
			idsByEpoch[f.SourceEpoch] = append(idsByEpoch[f.SourceEpoch], f.LogID)
		}
		query, first := reader, true
		for epoch, ids := range idsByEpoch {
			if first {
				query = query.Where("source_epoch = ? AND log_id IN ?", epoch, ids)
				first = false
			} else {
				query = query.Or("source_epoch = ? AND log_id IN ?", epoch, ids)
			}
		}
		return query.Find(&refs).Error
	})
	if err != nil || len(facts) == 0 {
		return 0, false, err
	}
	evidence, owners, truncated, err := m.tokenMeteringLinkInputs(ctx, refs, facts)
	if err != nil {
		// A canceled/deadlined read is a yielded turn; do not overwrite durable
		// association state when the caller did not obtain a complete result.
		if ctx.Err() != nil {
			return 0, true, err
		}
		failed, markErr := m.markTokenMeteringEvidenceQueryFailed(ctx, facts, now)
		if markErr != nil {
			return failed, true, errors.Join(err, markErr)
		}
		return failed, true, err
	}
	updated := make([]TokenMeteringFact, len(facts))
	for index, fact := range facts {
		ownRefs := []TokenMeteringRef{}
		for _, ref := range refs {
			if ref.SourceEpoch == fact.SourceEpoch && ref.LogID == fact.LogID {
				ownRefs = append(ownRefs, ref)
			}
		}
		updated[index] = resolveTokenMeteringLink(fact, ownRefs, evidence, owners, truncated, nginxEvidenceMode(m.cfg.NginxEvidenceMode), now)
	}
	count := 0
	// A transaction per row pays both a WAL commit and the other queued
	// workers' hold times up to 200 times. Keep the same bounded read batch,
	// but publish small atomic prefixes and yield the fair writer between them.
	chunkRows := min(tokenMeteringLinkMaxCommitRows, int(m.tokenLinkCommitRows.Load()))
	if chunkRows <= 0 {
		chunkRows = tokenMeteringLinkCommitRows
	}
	var lastTurn time.Duration
	for start := 0; start < len(facts); {
		// Collection advances the only source cursor.  If it is already queued,
		// do not join the FIFO only to time out behind another maintenance
		// prefix; the durable association inputs remain eligible for the next
		// turn.
		if m.tokenLocalWriteMu.collectionQueued() {
			return count, true, nil
		}
		if start > 0 && !tokenLocalWriteBudgetAvailable(ctx, lastTurn) {
			return count, true, nil
		}
		end, committed := min(start+chunkRows, len(facts)), 0
		timing := &tokenWriteTiming{}
		turnStarted := time.Now()
		err := m.tokenLocalWriteTransaction(context.WithValue(ctx, tokenWriteTimingKey{}, timing), func(tx *gorm.DB) error {
			workStarted := time.Now()
			for index := start; index < end; index++ {
				saved, err := m.saveTokenMeteringLinkTx(tx, facts[index], updated[index], now)
				if err != nil {
					return err
				}
				if saved {
					committed++
				}
				// Share fixed commit overhead across a short cheap prefix, while
				// bounding the SQL work ahead of a queued collection checkpoint.
				if m.tokenLocalWriteMu.yieldBatchToCollection(workStarted) {
					end = index + 1
					break
				}
				if time.Since(workStarted) >= 250*time.Millisecond {
					end = index + 1
					break
				}
			}
			return nil
		})
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && (timing.Work > 250*time.Millisecond || timing.Commit > 250*time.Millisecond) {
				m.tokenLinkCommitRows.Store(int64(max(1, chunkRows/2)))
			}
			return count, true, err
		}
		lastTurn = time.Since(turnStarted)
		count += committed
		if committed > 0 {
			// Association changes the detection input revision. Let a detector
			// that is between rounds observe it without waiting for its poll.
			m.signalTokenDetectionWake()
		}
		start = end
		// A slow fixed WAL commit is not evidence that SQL work is too large.
		// Shrinking cheap successful batches only repeats that cost more often.
		// The row cap, in-transaction work limit and deadline guard stay intact.
		if timing.Work < 200*time.Millisecond {
			chunkRows = min(tokenMeteringLinkMaxCommitRows, chunkRows*2)
		} else if timing.Work > 250*time.Millisecond {
			chunkRows = max(1, chunkRows/2)
		}
		m.tokenLinkCommitRows.Store(int64(chunkRows))
		if start < len(facts) && !waitSourceLifecycle(ctx, time.Millisecond) {
			return count, true, ctx.Err()
		}
	}
	return count, len(facts) == min(m.cfg.tokenProcessingBatchRows(), 200), nil
}
func (m *Monitor) tokenMeteringLinkInputs(ctx context.Context, refs []TokenMeteringRef, factBatches ...[]TokenMeteringFact) (map[string][]NginxRequestEvidence, map[string][]TokenMeteringRef, bool, error) {
	evidence := map[string][]NginxRequestEvidence{}
	owners := map[string][]TokenMeteringRef{}
	requestIDs := []string{}
	for _, batch := range factBatches {
		for _, f := range batch {
			if f.RequestID != "" {
				requestIDs = append(requestIDs, f.RequestID)
			}
		}
	}
	if len(refs) == 0 && len(requestIDs) == 0 {
		return evidence, owners, false, nil
	}
	seen := map[string]bool{}
	ownerQuery := m.tokenReadStore().WithContext(ctx)
	var sourceQuery *gorm.DB
	if m.nginxEvidenceDB != nil && nginxEvidenceMode(m.cfg.NginxEvidenceMode) != "off" {
		sourceQuery = m.nginxEvidenceDB.WithContext(ctx)
	}
	hashesByKey := map[string][]string{}
	eventIDs := []string{}
	for _, ref := range refs {
		key := tokenMeteringRefKey(ref.KeyID, ref.Hash)
		if seen[key] {
			continue
		}
		seen[key] = true
		if ref.Version == "event-id/v1" || ref.KeyID == "event-id" {
			eventIDs = append(eventIDs, ref.Hash)
		} else {
			hashesByKey[ref.KeyID] = append(hashesByKey[ref.KeyID], ref.Hash)
		}
	}
	// A constant-false OR seed disables SQLite's multi-index optimization.
	// Group exact identities by key so a large evidence store is read through
	// its existing composite index, while retaining all collision candidates.
	firstOwner, firstSource := true, true
	for key, hashes := range hashesByKey {
		if firstOwner {
			ownerQuery = ownerQuery.Where("key_id = ? AND hash IN ?", key, hashes)
		} else {
			ownerQuery = ownerQuery.Or("key_id = ? AND hash IN ?", key, hashes)
		}
		firstOwner = false
		if sourceQuery != nil {
			if firstSource {
				sourceQuery = sourceQuery.Where("hmac_key_id = ? AND oneapi_id_hmac IN ?", key, hashes)
			} else {
				sourceQuery = sourceQuery.Or("hmac_key_id = ? AND oneapi_id_hmac IN ?", key, hashes)
			}
			firstSource = false
		}
	}
	// Event IDs are durable exact identities too.  They must participate in
	// the owner probe, otherwise a reused event ID can produce evidence rows
	// without marking the consuming facts ambiguous.  Keep the predicate
	// grouped with the HMAC identities so SQLite can use the composite
	// (key_id,hash) index; never seed the query with a constant-false branch.
	if len(eventIDs) > 0 {
		if firstOwner {
			ownerQuery = ownerQuery.Where("key_id = ? AND hash IN ?", "event-id", eventIDs)
		} else {
			ownerQuery = ownerQuery.Or("key_id = ? AND hash IN ?", "event-id", eventIDs)
		}
		firstOwner = false
	}
	if sourceQuery != nil && len(eventIDs) > 0 {
		if firstSource {
			sourceQuery = sourceQuery.Where("event_id IN ?", eventIDs)
		} else {
			sourceQuery = sourceQuery.Or("event_id IN ?", eventIDs)
		}
		firstSource = false
	}
	var allOwners []TokenMeteringRef
	if !firstOwner {
		if err := m.tokenBackgroundRead(ctx, func(*gorm.DB) error {
			return ownerQuery.Limit(1001).Find(&allOwners).Error
		}); err != nil {
			return nil, nil, false, err
		}
	}
	for _, owner := range allOwners {
		key := tokenMeteringRefKey(owner.KeyID, owner.Hash)
		owners[key] = append(owners[key], owner)
	}
	var rawOwners []TokenMeteringFact
	if len(requestIDs) > 0 {
		if err := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
			return reader.Select("source_epoch,log_id,request_id").Where("request_id IN ?", requestIDs).Limit(1001).Find(&rawOwners).Error
		}); err != nil {
			return nil, nil, false, err
		}
		for _, f := range rawOwners {
			key := tokenMeteringRefKey("request-id", f.RequestID)
			owners[key] = append(owners[key], TokenMeteringRef{SourceEpoch: f.SourceEpoch, LogID: f.LogID, KeyID: "request-id", Hash: f.RequestID})
		}
		if sourceQuery != nil {
			if firstSource {
				sourceQuery = sourceQuery.Where("oneapi_request_id IN ?", requestIDs)
			} else {
				sourceQuery = sourceQuery.Or("oneapi_request_id IN ?", requestIDs)
			}
			firstSource = false
		}
	}
	var rows []NginxRequestEvidence
	if sourceQuery != nil && !firstSource {
		if err := sourceQuery.Order("event_id").Limit(1001).Find(&rows).Error; err != nil {
			return nil, nil, false, err
		}
	}
	for _, row := range rows {
		if row.OneAPIRequestID != "" {
			key := tokenMeteringRefKey("request-id", row.OneAPIRequestID)
			evidence[key] = append(evidence[key], row)
		}
		if row.HMACKeyID != "" && row.OneAPIIDHMAC != "" {
			key := tokenMeteringRefKey(row.HMACKeyID, row.OneAPIIDHMAC)
			evidence[key] = append(evidence[key], row)
		}
		if row.EventID != "" {
			key := tokenMeteringRefKey("event-id", row.EventID)
			evidence[key] = append(evidence[key], row)
		}
	}
	return evidence, owners, len(rows) > 1000 || len(allOwners) > 1000 || len(rawOwners) > 1000, nil
}

// A source/evidence read failure is different from an empty result.  Persist
// that distinction on the affected facts so health and the detection queue can
// show a retryable association failure without pretending the source window
// itself failed.  The optimistic fields in saveTokenMeteringLinkTx fence a
// concurrent collector/replay update and keep a committed prefix durable.
func (m *Monitor) markTokenMeteringEvidenceQueryFailed(ctx context.Context, facts []TokenMeteringFact, now int64) (int, error) {
	if len(facts) == 0 {
		return 0, nil
	}
	committed := 0
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		for _, fact := range facts {
			updated := fact
			updated.LinkState = "evidence_query_failed"
			updated.LinkReason = "evidence_query_failed"
			updated.EvidenceLevel = "unavailable"
			updated.LinkAttempts++
			updated.LinkNextAt = now + tokenMeteringBackoff(updated.LinkAttempts)
			if fact.CreatedAt >= now-900 {
				updated.LinkNextAt = min(updated.LinkNextAt, now+15)
			}
			if updated.LinkAttempts >= tokenMeteringMaxLinkAttempts || (updated.LinkDeadline > 0 && now >= updated.LinkDeadline) {
				// Keep the failure visible, but do not turn a query outage into an
				// expired/terminal association. A later successful read can recover it.
				updated.LinkNextAt = max(updated.LinkNextAt, now+15)
			}
			saved, err := m.saveTokenMeteringLinkTx(tx, fact, updated, now)
			if err != nil {
				return err
			}
			if saved {
				committed++
			}
		}
		return nil
	})
	return committed, err
}
func resolveTokenMeteringLink(f TokenMeteringFact, refs []TokenMeteringRef, evidence map[string][]NginxRequestEvidence, owners map[string][]TokenMeteringRef, truncated bool, mode string, now int64) TokenMeteringFact {
	if f.RequestID != "" {
		refs = append(append([]TokenMeteringRef(nil), refs...), TokenMeteringRef{SourceEpoch: f.SourceEpoch, LogID: f.LogID, KeyID: "request-id", Hash: f.RequestID, Version: "request-id/v1"})
	}
	previous := f
	// An earlier association may have supplied these fallbacks. Rebuild from
	// source operands before applying the new exact evidence, so a correction
	// or conflict cannot leave the previous event's route masquerading as source.
	f.RequestPath, f.RequestMethod = tokenMeteringSourceRoute(f)
	f.LinkState, f.LinkReason = "waiting", "evidence_pending"
	f.EvidenceLevel = "unavailable"
	f.RequestLength = nil
	f.LengthState = "missing"
	candidates := []tokenMeteringCandidate{}
	seen := map[string]bool{}
	observations := map[string]int{}
	collision := false
	outside := false
	for _, ref := range refs {
		key := tokenMeteringRefKey(ref.KeyID, ref.Hash)
		for _, owner := range owners[key] {
			if owner.SourceEpoch != f.SourceEpoch || owner.LogID != f.LogID {
				collision = true
			}
		}
		for _, row := range evidence[key] {
			if row.OneAPIRequestID != "" && f.RequestID != "" && row.OneAPIRequestID != f.RequestID {
				collision = true
			}
			// A raw-ID match cannot override contradictory keyed identity.
			for _, expected := range refs {
				if row.HMACKeyID != "" && expected.KeyID == row.HMACKeyID && expected.Version != "event-id/v1" && row.OneAPIIDHMAC != "" && expected.Hash != row.OneAPIIDHMAC {
					collision = true
				}
			}
			if seen[row.EventID] {
				continue
			}
			seen[row.EventID] = true
			// logs.created_at may describe settlement. Allow a bounded two-hour request
			// lifetime, but never use time proximity as the identifier.
			if row.EventMS < (f.CreatedAt-7200)*1000 || row.EventMS > (f.CreatedAt+300)*1000 {
				outside = true
			}
			state := row.RequestLengthState
			if state == "" {
				state = "missing"
			}
			if key := tokenMeteringObservationIdentity(row); key != "" {
				if index, exists := observations[key]; exists {
					candidate := &candidates[index]
					if len(candidate.ObservationEventIDs) >= 20 {
						truncated = true
						continue
					}
					if len(candidate.ObservationEventIDs) == 0 {
						candidate.ObservationEventIDs = []string{candidate.EventID}
					}
					candidate.ObservationEventIDs = append(candidate.ObservationEventIDs, row.EventID)
					sort.Strings(candidate.ObservationEventIDs)
					candidate.EventID = candidate.ObservationEventIDs[0]
					continue
				}
				observations[key] = len(candidates)
			}
			candidates = append(candidates, tokenMeteringCandidate{EventID: row.EventID, Node: row.Node, KeyID: row.HMACKeyID, EventMS: row.EventMS, Status: row.Status, Route: row.Route, Method: row.Method, Length: row.RequestLength, LengthState: state, LengthVersion: row.RequestLengthVersion})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].EventID < candidates[j].EventID })
	// Keep an actual disagreement witness even when it sorts beyond the snapshot cap.
	saved := append([]tokenMeteringCandidate(nil), candidates[:min(len(candidates), 20)]...)
	if len(candidates) > 20 {
		for _, c := range candidates[20:] {
			if tokenMeteringCandidatesDisagree(candidates[0], c) || c.EventMS < (f.CreatedAt-7200)*1000 || c.EventMS > (f.CreatedAt+300)*1000 {
				saved[len(saved)-1] = c
				break
			}
		}
	}
	encoded, _ := json.Marshal(saved)
	if len(candidates) > 0 && f.EvidenceJSON != string(encoded) {
		f.EvidenceJSON = string(encoded)
		f.EvidenceSnapshotAt = now
	}
	disagrees := false
	for _, candidate := range candidates {
		if tokenMeteringCandidatesDisagree(candidates[0], candidate) {
			disagrees = true
			break
		}
	}
	switch {
	case outside:
		f.LinkState = "conflict"
		f.LinkReason = "identifier_outside_time_scope"
	case disagrees:
		f.LinkState = "conflict"
		f.LinkReason = "entry_evidence_disagrees"
	case truncated:
		f.LinkState = "ambiguous"
		f.LinkReason = "candidate_budget_exceeded"
	case collision:
		f.LinkState = "ambiguous"
		f.LinkReason = "request_id_reused_by_consumptions"
	case len(refs) == 0:
		f.LinkState = "source_missing"
		f.LinkReason = "request_id_event_id_or_key_unavailable"
	case len(candidates) == 0:
		// The source identity is durable, but no exact evidence row is present.
		// Keep it retryable until the link deadline; never substitute a nearby
		// request and never label this as a completed association.
		f.LinkReason = "exact_request_evidence_not_found"
	case len(candidates) == 1:
		c := candidates[0]
		f.LinkState = "unique"
		f.LinkReason = "unique_local_hmac"
		f.EvidenceLevel = "correlated"
		if mode == "verified" {
			f.EvidenceLevel = "exact"
		} else {
			f.LinkReason = "unique_pilot_evidence"
		}
		f.RequestLength = c.Length
		f.LengthState = c.LengthState
		f.RequestLengthVersion = c.LengthVersion
		f.RequestLengthMeaning = "whole_http_request_not_body_or_token_count"
		f.RequestLengthObservedAt = c.EventMS / 1000
		f.EntryEventID, f.EntryNode, f.EntryRoute, f.EntryMethod, f.EntryStatus, f.EntryObservedAt = c.EventID, c.Node, c.Route, c.Method, c.Status, c.EventMS/1000
		f.RequestPath = firstNonEmptyTokenString(f.RequestPath, c.Route)
		f.RequestMethod = firstNonEmptyTokenString(f.RequestMethod, c.Method)
	case len(candidates) > 1:
		f.LinkState = "ambiguous"
		f.LinkReason = "multiple_entry_events"
	}
	if f.LinkState == "unique" {
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "entry_event_id", "entry_evidence_pending", f.EntryEventID != "")
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "request_length", "entry_request_length_missing", f.RequestLength != nil && f.LengthState == "valid")
		for _, name := range []string{"request_length_version", "request_length_meaning", "request_length_observed_at"} {
			f.MissingFields = tokenMeteringMissingSet(f.MissingFields, name, "entry_request_length_missing", f.RequestLength != nil && f.LengthState == "valid" && f.RequestLengthVersion != "")
		}
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "request_path", "source_path_missing", f.RequestPath != "")
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "request_method", "source_method_missing", f.RequestMethod != "")
	} else {
		f.EntryEventID, f.EntryNode, f.EntryRoute, f.EntryMethod, f.EntryStatus, f.EntryObservedAt = "", "", "", "", 0, 0
		f.RequestLengthVersion, f.RequestLengthObservedAt = "", 0
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "entry_event_id", "exact_request_evidence_not_found", false)
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "request_length", "exact_request_evidence_not_found", false)
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "request_path", "source_path_missing", f.RequestPath != "")
		f.MissingFields = tokenMeteringMissingSet(f.MissingFields, "request_method", "source_method_missing", f.RequestMethod != "")
	}
	// Retention, missing candidates and retry exhaustion are not conflict resolution.
	if tokenMeteringKnownDispute(previous) && (previous.LinkState == "conflict" || f.LinkState != "conflict") &&
		!(f.LinkState == "unique" && tokenMeteringRepeatedObservationDisputeResolved(previous, candidates)) {
		retainTokenMeteringDispute(&f, previous)
	}
	f.LinkAttempts++
	f.LinkNextAt = now + tokenMeteringBackoff(f.LinkAttempts)
	if now < f.CreatedAt+900 {
		f.LinkNextAt = min(f.LinkNextAt, now+15)
	}
	// Successful links remain a time-bounded observation, with continued checks
	// for late conflicts; budget exhaustion never erases the last known evidence.
	if f.LinkAttempts >= tokenMeteringMaxLinkAttempts && now >= f.CreatedAt+900 || now >= f.LinkDeadline {
		f.LinkNextAt = 0
		if f.LinkState == "waiting" {
			f.LinkState = "expired"
			f.LinkReason = "exact_request_evidence_not_found"
		}
	}
	return f
}
func tokenMeteringCandidatesDisagree(a, b tokenMeteringCandidate) bool {
	return a.Status != b.Status || a.Route != b.Route || a.Method != b.Method || a.LengthState != b.LengthState || !sameTokenMeteringLength(a.Length, b.Length)
}

func firstNonEmptyTokenString(current, fallback string) string {
	if current != "" {
		return current
	}
	return fallback
}

func tokenMeteringKnownDispute(f TokenMeteringFact) bool {
	reason := strings.TrimSuffix(f.LinkReason, "_snapshot_storage_limit")
	return f.LinkState == "conflict" || (f.LinkState == "ambiguous" && (reason == "multiple_entry_events" || reason == "request_id_reused_by_consumptions"))
}

func retainTokenMeteringDispute(dst *TokenMeteringFact, src TokenMeteringFact) {
	dst.LinkState, dst.LinkReason = src.LinkState, src.LinkReason
	// A rejected unique candidate cannot supply route fallbacks after the
	// earlier dispute is restored. Only source-recorded route fields survive.
	dst.RequestPath, dst.RequestMethod = tokenMeteringSourceRoute(*dst)
	dst.MissingFields = tokenMeteringMissingSet(dst.MissingFields, "request_path", "source_path_missing", dst.RequestPath != "")
	dst.MissingFields = tokenMeteringMissingSet(dst.MissingFields, "request_method", "source_method_missing", dst.RequestMethod != "")
	dst.EvidenceJSON, dst.EvidenceSnapshotAt = src.EvidenceJSON, src.EvidenceSnapshotAt
	dst.EvidenceLevel, dst.LengthState = "unavailable", "missing"
	dst.RequestLength = nil
	dst.RequestLengthVersion, dst.RequestLengthObservedAt = "", 0
	dst.EntryEventID, dst.EntryNode, dst.EntryRoute, dst.EntryMethod, dst.EntryStatus, dst.EntryObservedAt = "", "", "", "", 0, 0
	dst.MissingFields = tokenMeteringMissingSet(dst.MissingFields, "entry_event_id", "evidence_conflict", false)
	dst.MissingFields = tokenMeteringMissingSet(dst.MissingFields, "request_length", "evidence_conflict", false)
}

func sameTokenMeteringLength(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// Retention is allowed to remove only a settled, fully associated and fully
// evaluated fact.  The predicates deliberately fail closed: a missing head,
// retry row, source window or durable detection state keeps the fact for the
// next pass instead of making the watermark look healthier by deleting work.
const tokenMeteringCleanupEligibleSQL = `
	f.link_state = 'unique' AND COALESCE(f.link_next_at,0)=0
	AND EXISTS(SELECT 1 FROM token_metering_states s WHERE s.source_epoch=f.source_epoch AND f.created_at<s.cleanup_safe_before AND f.first_seen_at<=s.cleanup_verified_at AND f.last_seen_at<=s.cleanup_verified_at)
	AND EXISTS (SELECT 1 FROM token_reference_members_checkpoint_v1 WHERE id=1 AND ready=1)
	AND NOT EXISTS (SELECT 1 FROM token_reference_members_invalid_v1 LIMIT 1)
	AND NOT EXISTS (SELECT 1 FROM token_reference_members_v1 ref WHERE ref.source_epoch=f.source_epoch AND ref.log_id=f.log_id)
	AND EXISTS (SELECT 1 FROM token_revision_members_checkpoint_v1 WHERE id=1 AND ready=1)
	AND NOT EXISTS (SELECT 1 FROM token_revision_members_dirty_v1 LIMIT 1)
	AND NOT EXISTS (SELECT 1 FROM token_revision_members_v1 ref WHERE ref.source_epoch=f.source_epoch AND ref.log_id=f.log_id)
	AND EXISTS (SELECT 1 FROM token_metering_windows w
		WHERE w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300
		AND w.done=1 AND w.source_complete=1 AND w.replay=0 AND COALESCE(w.last_reason,'')='')
	AND NOT EXISTS (SELECT 1 FROM token_detection_retries r
		WHERE r.source_epoch=f.source_epoch AND r.log_id=f.log_id)
	AND EXISTS (SELECT 1 FROM token_detection_currents c
		WHERE c.source_epoch=f.source_epoch AND c.log_id=f.log_id)
	AND NOT EXISTS (SELECT 1 FROM token_detection_currents c
		WHERE c.source_epoch=f.source_epoch AND c.log_id=f.log_id
		AND (c.expires_at>=? OR c.stage<3 OR COALESCE(c.next_at,0)>0
			OR c.fact_revision<>f.revision OR c.link_revision<>f.link_revision
			OR COALESCE(c.detection_state,'') NOT IN ('not_hit','local_formula_consistent')
			OR c.source_complete<>1 OR c.policy_hash<>?
			OR c.reference_generation<>COALESCE((SELECT generation FROM token_detection_buckets b WHERE b.id=c.reference_bucket),0)))
	AND NOT EXISTS (SELECT 1 FROM token_detection_revisions r
		WHERE r.source_epoch=f.source_epoch AND r.log_id=f.log_id AND r.expires_at>=?)`

func (m *Monitor) cleanupTokenMetering(ctx context.Context, now int64) error {
	ctx = context.WithValue(ctx, tokenWriteOperationKey{}, "facts_cleanup")
	if m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return nil
	}
	cutoff := (now - m.cfg.tokenMeteringRetentionSeconds()) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	// Do not queue a writer or scan facts before a maintenance run has recorded
	// backup, four-day reconciliation and thirty minutes of stable recovery.
	var authorized bool
	if err := m.tokenReadStore().WithContext(ctx).Raw(`SELECT EXISTS(SELECT 1 FROM token_metering_states
		WHERE cleanup_backup_id<>'' AND cleanup_verified_at>0 AND cleanup_safe_before>0
		AND cleanup_stable_since>0 AND cleanup_verified_at-cleanup_stable_since>=1800 AND cleanup_next_at<=? LIMIT 1)`, now).Scan(&authorized).Error; err != nil {
		return err
	}
	if !authorized {
		return nil
	}
	// Keep source coverage and unfinished windows independently of detail
	// retention. Age is never an EOF proof and cannot advance a source cursor.
	// Retention shares the writer with newly arriving facts and detection.
	// A 500-row sweep could occupy most of each maintenance turn at retained
	// scale. Keep expiry progressing in smaller turns without monopolizing
	// the write queue; the retention cutoff and coverage accounting are intact.
	// Keep each retention transaction below the collection wait budget even on
	// the retained store where the health/report triggers make a bulk delete
	// expensive.  The adaptive chunk still amortizes fixed WAL work.  The
	// overall sweep remains the existing bounded 128-row turn so one cleanup
	// invocation can drain a normal expired prefix; the writer is released
	// between its 16-row (or smaller learned) transactions.
	const sweepRows = 128
	const sweepTransactionRows = 16
	chunkRows := min(sweepTransactionRows, int(m.tokenCleanupCommitRows.Load()))
	if chunkRows <= 0 {
		chunkRows = tokenCleanupChunk
	}
	var lastTurn time.Duration
	for removed := 0; removed < sweepRows; {
		if m.tokenLocalWriteMu.collectionQueued() {
			return nil
		}
		if removed > 0 && !tokenLocalWriteBudgetAvailable(ctx, lastTurn) {
			return nil
		}
		committed := 0
		requested := min(chunkRows, sweepRows-removed)
		timing := &tokenWriteTiming{}
		turnStarted := time.Now()
		err := m.tokenLocalWriteTransaction(context.WithValue(ctx, tokenWriteTimingKey{}, timing), func(tx *gorm.DB) error {
			type candidate struct {
				SourceEpoch string
				LogID       int64
				CreatedAt   int64
				StoredBytes int64
			}
			var old, candidates []candidate
			eligible := tokenMeteringCleanupEligibleSQL
			policy, policyErr := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
			if policyErr != nil {
				return policyErr
			}
			eligibleArgs := []any{now, tokenDetectionPolicyHash(policy), now}
			// A legacy token-only store may not have detection tables yet. Treat
			// that absence as unknown work and keep the fact; startup migration can
			// then install the queue before a later cleanup pass removes anything.
			if !tx.Migrator().HasTable(&TokenDetectionCurrent{}) {
				eligible = "1=0"
				eligibleArgs = nil
			}
			var states []TokenMeteringState
			if err := tx.Select("source_epoch,cleanup_cursor_at,cleanup_cursor_id").Where("cleanup_backup_id<>'' AND cleanup_verified_at>0 AND cleanup_stable_since>0 AND cleanup_verified_at-cleanup_stable_since>=1800 AND cleanup_next_at<=?", now).
				Order("source_epoch").Limit(8).Find(&states).Error; err != nil {
				return err
			}
			for _, s := range states {
				var prefix []candidate
				if err := tx.Table("token_metering_facts AS f").Select("f.source_epoch,f.log_id,f.created_at,f.stored_bytes").
					Where("f.source_epoch=? AND f.created_at<? AND (f.created_at,f.log_id)>(?,?)", s.SourceEpoch, cutoff, s.CleanupCursorAt, s.CleanupCursorID).
					Order("f.created_at,f.log_id").Limit(requested).Find(&prefix).Error; err != nil {
					return err
				}
				candidates = append(candidates, prefix...)
			}
			sort.Slice(candidates, func(i, j int) bool {
				a, b := candidates[i], candidates[j]
				if a.CreatedAt != b.CreatedAt {
					return a.CreatedAt < b.CreatedAt
				}
				if a.SourceEpoch != b.SourceEpoch {
					return a.SourceEpoch < b.SourceEpoch
				}
				return a.LogID < b.LogID
			})
			candidates = candidates[:min(requested, len(candidates))]
			type removedUsage struct {
				rows, bytes int64
				ids         []int64
				lastAt      int64
				lastID      int64
			}
			usage := map[string]removedUsage{}
			epochOrder := make([]string, 0, len(candidates))
			checked := map[string][]int64{}
			for _, f := range candidates {
				if len(checked[f.SourceEpoch]) == 0 {
					epochOrder = append(epochOrder, f.SourceEpoch)
				}
				checked[f.SourceEpoch] = append(checked[f.SourceEpoch], f.LogID)
				u := usage[f.SourceEpoch]
				u.lastAt, u.lastID = f.CreatedAt, f.LogID
				usage[f.SourceEpoch] = u
			}
			// Eligibility is evaluated only after a bounded discovery prefix has
			// been materialized. Protected old facts cannot cause a full scan.
			for _, epoch := range epochOrder {
				var rows []candidate
				if err := tx.Table("token_metering_facts AS f").Select("f.source_epoch,f.log_id,f.created_at,f.stored_bytes").
					Where("f.source_epoch=? AND f.log_id IN ?", epoch, checked[epoch]).Where(eligible, eligibleArgs...).Find(&rows).Error; err != nil {
					return err
				}
				old = append(old, rows...)
			}
			for _, f := range old {
				removed := usage[f.SourceEpoch]
				removed.rows++
				removed.bytes += f.StoredBytes
				removed.ids = append(removed.ids, f.LogID)
				usage[f.SourceEpoch] = removed
			}
			// Batch a bounded transaction's identities per
			// epoch. Repeated individual DELETEs made expiry monopolize writer
			// time when collection and detection were catching up. Scalar IN
			// seeks the full primary key; GORM's composite tuple IN could scan
			// the whole epoch on older SQLite. All ledgers and reference deletes
			// still roll back together if any fact or capacity update fails.
			processedEpoch := 0
			// A cleanup page may contain facts from several source epochs. If
			// collection queues while the page is being applied, the transaction
			// deliberately commits only the epochs already completed. Report that
			// committed prefix precisely; counting len(old) here would advance the
			// outer sweep past rows that are still present and make the next turn
			// skip them.
			committed = 0
			for _, epoch := range epochOrder {
				removed := usage[epoch]
				if processedEpoch > 0 && m.tokenLocalWriteMu.collectionQueued() {
					break
				}
				// The eligibility fence proves every head/revision is expired and
				// complete. Remove the owned result and fact in the same transaction
				// so a crash cannot leave an orphan head or a false pending task.
				if len(removed.ids) > 0 {
					if err := tx.Where("source_epoch=? AND log_id IN ?", epoch, removed.ids).Delete(&TokenDetectionRevision{}).Error; err != nil {
						return err
					}
					if err := tx.Where("source_epoch=? AND log_id IN ?", epoch, removed.ids).Delete(&TokenDetectionCurrent{}).Error; err != nil {
						return err
					}
					if err := tx.Where("source_epoch = ? AND log_id IN ?", epoch, removed.ids).Delete(&TokenMeteringRef{}).Error; err != nil {
						return err
					}
					if err := tx.Where("source_epoch = ? AND log_id IN ?", epoch, removed.ids).Delete(&TokenMeteringFact{}).Error; err != nil {
						return err
					}
				}
				if err := tx.Model(&TokenMeteringState{}).Where("source_epoch = ?", epoch).
					Updates(map[string]any{"fact_count": gorm.Expr("fact_count - ?", removed.rows), "fact_bytes": gorm.Expr("fact_bytes - ?", removed.bytes), "cleanup_cursor_at": removed.lastAt, "cleanup_cursor_id": removed.lastID,
						"cleanup_deleted_rows": gorm.Expr("cleanup_deleted_rows+?", removed.rows), "cleanup_deleted_bytes": gorm.Expr("cleanup_deleted_bytes+?", removed.bytes), "last_cleanup_at": now, "cleanup_reason": ""}).Error; err != nil {
					return err
				}
				committed += len(checked[epoch])
				processedEpoch++
			}
			if len(candidates) == 0 {
				// A full pass reached the end of every eligible prefix. Resetting
				// the durable positions lets a previously protected row be found
				// when its association/detection state later becomes complete.
				for _, s := range states {
					if err := tx.Model(&TokenMeteringState{}).Where("source_epoch=?", s.SourceEpoch).Updates(map[string]any{"cleanup_cursor_at": 0, "cleanup_cursor_id": 0}).Error; err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && (timing.Work > 250*time.Millisecond || timing.Commit > 250*time.Millisecond) {
				m.tokenCleanupCommitRows.Store(int64(max(1, chunkRows/2)))
			}
			return err
		}
		lastTurn = time.Since(turnStarted)
		removed += committed
		if committed < requested {
			break
		}
		if timing.Work < 200*time.Millisecond && timing.Commit < 400*time.Millisecond {
			chunkRows = min(sweepTransactionRows, chunkRows*2)
		} else if timing.Work > 250*time.Millisecond || timing.Commit > 750*time.Millisecond {
			chunkRows = max(1, chunkRows/2)
		}
		m.tokenCleanupCommitRows.Store(int64(chunkRows))
		if removed < sweepRows && !waitSourceLifecycle(ctx, time.Millisecond) {
			return ctx.Err()
		}
	}
	return nil
}

// Yield the shared writer between committed cleanup prefixes, just as the
// collector and detector do. Cancellation rolls back only the current chunk.
const tokenCleanupChunk = 8

func validateTokenMeteringLocalQuery(s string) error {
	if strings.TrimSpace(s) != "" && !tokenMeteringName.MatchString(s) {
		return errors.New("invalid source_epoch")
	}
	return nil
}

func (m *Monitor) saveTokenMeteringLink(ctx context.Context, old, updated TokenMeteringFact, now int64) (bool, error) {
	saved := false
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		saved, err = m.saveTokenMeteringLinkTx(tx, old, updated, now)
		return err
	})
	return saved && err == nil, err
}

func (m *Monitor) saveTokenMeteringLinkTx(tx *gorm.DB, old, updated TokenMeteringFact, now int64) (bool, error) {
	var current TokenMeteringFact
	err := tx.Select("source_epoch,log_id,revision,link_revision,link_next_at,link_attempts,link_checked_at,stored_bytes,link_state,link_reason,evidence_level,request_length,length_state,request_length_version,request_length_meaning,request_length_observed_at,entry_event_id,entry_node,entry_route,entry_method,entry_status,entry_observed_at,request_path,request_method,missing_fields,evidence_json,evidence_snapshot_at").
		First(&current, "source_epoch = ? AND log_id = ?", old.SourceEpoch, old.LogID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current.Revision != old.Revision || current.LinkRevision != old.LinkRevision || current.LinkNextAt != old.LinkNextAt || current.LinkAttempts != old.LinkAttempts || current.LinkCheckedAt != old.LinkCheckedAt {
		return false, nil
	}
	delta := int64(len(updated.EvidenceJSON) - len(current.EvidenceJSON))
	if delta > 0 {
		var used int64
		if err := tx.Model(&TokenMeteringState{}).Select("COALESCE(SUM(fact_bytes),0)").Scan(&used).Error; err != nil {
			return false, err
		}
		_, maxBytes := m.cfg.tokenMeteringLimits()
		if used+delta > maxBytes {
			// Keep the last known snapshot with its original observation timestamp.
			// Current association is unavailable until the bounded local retry fits.
			updated.EvidenceJSON = current.EvidenceJSON
			updated.EvidenceSnapshotAt = current.EvidenceSnapshotAt
			delta = 0
			knownDispute := tokenMeteringKnownDispute(updated)
			if knownDispute {
				updated.LinkReason = strings.TrimSuffix(updated.LinkReason, "_snapshot_storage_limit") + "_snapshot_storage_limit"
			} else {
				updated.LinkState = "waiting"
				updated.LinkReason = "snapshot_storage_limit"
			}
			updated.EvidenceLevel = "unavailable"
			updated.RequestLength = nil
			updated.LengthState = "missing"
			if updated.LinkNextAt == 0 && !knownDispute {
				updated.LinkState = "expired"
			}
		}
	}
	// Check bookkeeping is independent of the version consumed by detection.
	linkRevision := current.LinkRevision
	if tokenMeteringEvidenceHash(current) != tokenMeteringEvidenceHash(updated) {
		linkRevision++
	}
	fields := map[string]any{"link_attempts": updated.LinkAttempts, "link_next_at": updated.LinkNextAt, "link_checked_at": now}
	if linkRevision != current.LinkRevision {
		fields["link_revision"] = linkRevision
		fields["link_state"], fields["link_reason"] = updated.LinkState, updated.LinkReason
		fields["evidence_level"], fields["request_length"], fields["length_state"] = updated.EvidenceLevel, updated.RequestLength, updated.LengthState
		fields["request_length_version"], fields["request_length_meaning"], fields["request_length_observed_at"] = updated.RequestLengthVersion, updated.RequestLengthMeaning, updated.RequestLengthObservedAt
		fields["entry_event_id"], fields["entry_node"], fields["entry_route"], fields["entry_method"], fields["entry_status"], fields["entry_observed_at"] = updated.EntryEventID, updated.EntryNode, updated.EntryRoute, updated.EntryMethod, updated.EntryStatus, updated.EntryObservedAt
		fields["request_path"], fields["request_method"], fields["missing_fields"] = updated.RequestPath, updated.RequestMethod, updated.MissingFields
		fields["evidence_json"] = updated.EvidenceJSON
	}
	if updated.EvidenceSnapshotAt != current.EvidenceSnapshotAt {
		fields["evidence_snapshot_at"] = updated.EvidenceSnapshotAt
	}
	if delta != 0 {
		fields["stored_bytes"] = current.StoredBytes + delta
	}
	// Pending retries normally change only their schedule. Omitting unchanged
	// payload/version columns avoids firing the report projection and JSON health
	// triggers for those retries; the same transaction still fences the revision.
	if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND log_id = ?", old.SourceEpoch, old.LogID).Updates(fields).Error; err != nil {
		return false, err
	}
	// Most evidence-pending retries change only their schedule. Do not rewrite
	// the shared source state (and its UpdatedAt) for an unchanged byte ledger.
	if delta != 0 {
		if err := tx.Model(&TokenMeteringState{}).Where("source_epoch = ?", old.SourceEpoch).Update("fact_bytes", gorm.Expr("fact_bytes + ?", delta)).Error; err != nil {
			return false, err
		}
	}
	return true, nil
}

// tokenMeteringEvidenceHash covers the retained association, not retry counters.
// Include event identity, length semantics and availability, even if length agrees.
func tokenMeteringEvidenceHash(f TokenMeteringFact) string {
	body, _ := json.Marshal(struct {
		State, Reason, Level, LengthState, Evidence string
		Length                                      *int64
		LengthVersion, LengthMeaning                string
		LengthObserved                              int64
		EventID, Node, Route, Method                string
		Status                                      int
		EventObserved                               int64
		RequestPath, RequestMethod, Missing         string
	}{f.LinkState, f.LinkReason, f.EvidenceLevel, f.LengthState, f.EvidenceJSON, f.RequestLength,
		f.RequestLengthVersion, f.RequestLengthMeaning, f.RequestLengthObservedAt,
		f.EntryEventID, f.EntryNode, f.EntryRoute, f.EntryMethod, f.EntryStatus, f.EntryObservedAt,
		f.RequestPath, f.RequestMethod, f.MissingFields})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
