package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"math/big"
	"net/http"
	"sort"
	"time"
)

type tokenDetectionQuery struct {
	GroupSet     bool   `json:"group_set"`
	ModelSet     bool   `json:"model_set"`
	UserSet      bool   `json:"user_set"`
	ChannelSet   bool   `json:"channel_set"`
	SourceEpoch  string `json:"source_epoch"`
	From         int64  `json:"from"`
	To           int64  `json:"to"`
	AfterAt      int64  `json:"after_at"`
	AfterID      int64  `json:"after_id"`
	NewestFirst  bool   `json:"newest_first"`
	Limit        int    `json:"limit"`
	UserID       int64  `json:"user_id"`
	ChannelID    int64  `json:"channel_id"`
	Model        string `json:"model"`
	Group        string `json:"group"`
	Status       string `json:"status"`
	Rule         string `json:"rule"`
	Evidence     string `json:"evidence_level"`
	Method       string `json:"method"`
	Unavailable  bool   `json:"unavailable"`
	Subject      string `json:"subject"`
	Unit         string `json:"unit"`
	QuotaPerUnit string `json:"quota_per_unit"`
}

// The latest bounded worker turn is intentionally a compact checkpoint
// rather than a live queue scan.  It lets operators distinguish useful
// yielding/deferment from an actual processing failure.
type tokenDetectionRoundHealth struct {
	Processed int64 `json:"processed"`
	Yielded   int64 `json:"yielded"`
	Failed    int64 `json:"failed"`
	Deferred  int64 `json:"deferred"`
}
type tokenDetectionAmounts struct {
	Subject      string  `json:"subject"`
	Unit         string  `json:"unit"`
	QuotaPerUnit string  `json:"quota_per_unit"`
	Method       string  `json:"method"`
	Count        int64   `json:"count"`
	Quota        string  `json:"quota"`
	Amount       *string `json:"amount_fraction"`
}
type tokenDetectionSummary struct {
	Dimensions             map[string]int                   `json:"unique_candidate_dimensions"`
	Observed               int64                            `json:"observed_results"`
	Candidates             int64                            `json:"candidate_requests"`
	Truncated              bool                             `json:"truncated"`
	SourceComplete         bool                             `json:"source_complete"`
	Unprocessed            int64                            `json:"unprocessed_requests"`
	Insufficient           int64                            `json:"insufficient_requests"`
	StatusCountUnavailable string                           `json:"status_count_unavailable,omitempty"`
	Amounts                map[string]tokenDetectionAmounts `json:"amounts"`
	RuleEligible           map[string]int64                 `json:"rule_eligible"`
	RuleHits               map[string]int64                 `json:"rule_hits"`
	Unavailable            map[string]int64                 `json:"unavailable_by_method_and_reason"`
	Gaps                   []string                         `json:"gaps"`
}

// An unfinished bounded count is unknown, never zero or a partial total.
func (s tokenDetectionSummary) MarshalJSON() ([]byte, error) {
	type plain tokenDetectionSummary
	if s.Truncated {
		return json.Marshal(struct {
			plain
			Observed     *int64 `json:"observed_results"`
			Candidates   *int64 `json:"candidate_requests"`
			Insufficient *int64 `json:"insufficient_requests"`
			Amounts      any    `json:"amounts"`
			RuleEligible any    `json:"rule_eligible"`
			RuleHits     any    `json:"rule_hits"`
			Dimensions   any    `json:"unique_candidate_dimensions"`
			Unavailable  any    `json:"unavailable_by_method_and_reason"`
		}{plain: plain(s)})
	}
	if s.StatusCountUnavailable == "" {
		return json.Marshal(plain(s))
	}
	return json.Marshal(struct {
		plain
		Insufficient *int64 `json:"insufficient_requests"`
	}{plain: plain(s)})
}

func tokenDetectionAddAmount(s *tokenDetectionSummary, p tokenPriceResult, method, value string) {
	// Unknown monetary identity cannot be combined into an anonymous bucket.
	// Preserve the per-request quota in evidence and report a calculation gap.
	unit, ok := new(big.Rat).SetString(p.QuotaPerUnit)
	if p.Subject == "" || p.Unit == "" || !ok || unit.Sign() <= 0 {
		s.Unavailable[method+":billing_subject_or_unit_unknown"]++
		return
	}
	key := tokenDetectionHash([]string{p.Subject, p.Unit, p.QuotaPerUnit, method})
	a, ok := s.Amounts[key]
	if !ok {
		a = tokenDetectionAmounts{Subject: p.Subject, Unit: p.Unit, QuotaPerUnit: p.QuotaPerUnit, Method: method, Quota: "0"}
	}
	sum, _ := new(big.Int).SetString(a.Quota, 10)
	v, valid := new(big.Int).SetString(value, 10)
	if !valid {
		return
	}
	sum.Add(sum, v)
	a.Count++
	a.Quota = sum.String()
	amount := new(big.Rat).Quo(new(big.Rat).SetInt(sum), unit).RatString()
	a.Amount = &amount
	s.Amounts[key] = a
}
func tokenDetectionSummarize(rows []TokenDetectionCurrent) (tokenDetectionSummary, error) {
	s := tokenDetectionSummary{Dimensions: map[string]int{}, Amounts: map[string]tokenDetectionAmounts{}, RuleEligible: map[string]int64{}, RuleHits: map[string]int64{}, Unavailable: map[string]int64{}, Gaps: []string{}}
	dimensions := map[string]map[string]bool{"customers": {}, "channels": {}, "models": {}, "groups": {}}
	for _, row := range rows {
		var r tokenDetectionResult
		if json.Unmarshal([]byte(row.ResultJSON), &r) != nil {
			return s, errors.New("local_result_invalid")
		}
		s.Observed++
		for _, rule := range r.Rules {
			if rule.Eligible {
				s.RuleEligible[rule.Name]++
			}
			if rule.Hit {
				s.RuleHits[rule.Name]++
			}
		}
		if !r.Candidate {
			continue
		}
		s.Candidates++
		dimensions["customers"][big.NewInt(row.UserID).String()] = true
		dimensions["channels"][big.NewInt(row.ChannelID).String()] = true
		dimensions["models"][row.Model] = true
		dimensions["groups"][row.UserGroup] = true
		if r.ActualQuota != nil {
			tokenDetectionAddAmount(&s, r.Price, "observed_consumption_not_loss", *r.ActualQuota)
		} else {
			s.Unavailable["observed_consumption:quota_missing"]++
		}
		if r.FormulaDifference != nil {
			tokenDetectionAddAmount(&s, r.Price, "historical_formula_v1", *r.FormulaDifference)
		} else {
			s.Unavailable["historical_formula_v1:"+r.Price.Reason]++
		}
		if r.EstimatedLoss != nil {
			tokenDetectionAddAmount(&s, r.Price, "input_upper_reference_v1", *r.EstimatedLoss)
		} else {
			s.Unavailable["input_upper_reference_v1:"+r.EstimateReason]++
		}
	}
	for key, values := range dimensions {
		s.Dimensions[key] = len(values)
	}
	return s, nil
}
func tokenDetectionRange(db *gorm.DB, q tokenDetectionQuery) *gorm.DB {
	db = db.Model(&TokenDetectionCurrent{}).Where(tokenReportSummaryColumn(db, "source_epoch")+" = ? AND "+tokenReportSummaryColumn(db, "created_at")+" >= ? AND "+tokenReportSummaryColumn(db, "created_at")+" < ?", q.SourceEpoch, q.From, q.To)
	if q.UserID > 0 || q.UserSet {
		db = db.Where(tokenReportSummaryColumn(db, "user_id")+" = ?", q.UserID)
	}
	if q.ChannelID > 0 || q.ChannelSet {
		db = db.Where(tokenReportSummaryColumn(db, "channel_id")+" = ?", q.ChannelID)
	}
	if q.Model != "" || q.ModelSet {
		db = db.Where(tokenReportSummaryColumn(db, "model")+" = ?", q.Model)
	}
	if q.Group != "" || q.GroupSet {
		db = db.Where(tokenReportSummaryColumn(db, "user_group")+" = ?", q.Group)
	}
	return tokenReportFilters(db, q)
}
func (m *Monitor) tokenDetectionReport(ctx context.Context, q tokenDetectionQuery) (map[string]any, error) {
	if m.tokenStore() == nil {
		return nil, errors.New("local store unavailable")
	}
	result := map[string]any{}
	err := m.tokenReadStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		emptyStatus, err := tokenReportStatusEmpty(tx, q)
		if err != nil {
			return err
		}
		summary, analysis, err := m.tokenDetectionReportAggregates(tx, q, emptyStatus)
		if err != nil {
			return err
		}
		if !m.cfg.TokenDetectionEnabled || m.cfg.LocalSnapshotOnly {
			summary.Gaps = append(summary.Gaps, "detection_disabled")
		}
		var state TokenMeteringState
		if err := tx.Where("source_epoch = ?", q.SourceEpoch).Limit(1).Find(&state).Error; err != nil {
			return err
		}
		if state.SourceEpoch != "" {
			if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
				return err
			}
		}
		summary.SourceComplete, err = tokenMeteringSelectedRangeComplete(tx, state, q.From, q.To)
		if err != nil {
			return err
		}
		// Stale heads remain queryable but are never counted as current coverage.
		policy, policyErr := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
		hash := ""
		if policyErr == nil {
			hash = tokenDetectionPolicyHash(policy)
		}
		pending, err := tokenReportPendingCount(tx, q, hash)
		if err != nil {
			return err
		}
		summary.Unprocessed = pending
		queueReady, err := tokenReportProjectionsReady(tx)
		if err != nil {
			return err
		}
		if !queueReady {
			summary.Gaps = append(summary.Gaps, "detection_queue_rebuilding")
		}
		// Outcome filters must not hide evidence gaps and turn an empty candidate
		// list into a false all-clear. Keep only the selected identity/range scope.
		coverageQuery := q
		coverageQuery.Status, coverageQuery.Rule, coverageQuery.Evidence = "insufficient", "", ""
		coverageQuery.Method, coverageQuery.Subject, coverageQuery.Unit, coverageQuery.QuotaPerUnit = "", "", "", ""
		coverageQuery.Unavailable = false
		summary.Insufficient, err = tokenReportInsufficientCount(tx, coverageQuery)
		if errors.Is(err, errTokenReportCountNotReady) {
			summary.StatusCountUnavailable = err.Error()
			summary.Gaps = append(summary.Gaps, err.Error())
		} else if err != nil {
			return err
		}
		if summary.Insufficient > 0 {
			summary.Gaps = append(summary.Gaps, "evidence_insufficient")
		}
		if pending > 0 {
			summary.Gaps = append(summary.Gaps, "detection_pending_or_stale")
		}
		if !summary.SourceComplete {
			summary.Gaps = append(summary.Gaps, "source_range_incomplete")
		}
		if summary.Truncated {
			summary.Gaps = append(summary.Gaps, "summary_truncated_not_full_population")
			analysis.StatusCounts, analysis.Calculable, analysis.Breakdown = nil, nil, nil
		}
		pageQuery := tokenDetectionRange(tx, q)
		order, comparison := "created_at, log_id", ">"
		if q.NewestFirst {
			order, comparison = "created_at DESC, log_id DESC", "<"
		}
		if q.AfterAt > 0 {
			pageQuery = pageQuery.Where("(created_at,log_id)"+comparison+"(?,?)", q.AfterAt, q.AfterID)
		}
		var page []TokenDetectionCurrent
		if !emptyStatus {
			if err := pageQuery.Order(order).Limit(q.Limit + 1).Find(&page).Error; err != nil {
				return err
			}
		}
		// A collected fact may not have a current head yet. Merge the durable
		// fresh projection into the unfiltered list using the same keyset; page
		// visits never create jobs or trigger production reads.
		if tokenReportIncludePending(q) {
			pending, err := tokenReportPendingPage(tx, q)
			if err != nil {
				return err
			}
			page = append(page, pending...)
			sort.Slice(page, func(i, j int) bool {
				if q.NewestFirst {
					i, j = j, i
				}
				if page[i].CreatedAt == page[j].CreatedAt {
					return page[i].LogID < page[j].LogID
				}
				return page[i].CreatedAt < page[j].CreatedAt
			})
		}
		hasMore := len(page) > q.Limit
		if hasMore {
			page = page[:q.Limit]
		}
		items := []map[string]any{}
		displays, err := m.tokenDetectionDisplays(tx, q.SourceEpoch, page)
		if err != nil {
			return err
		}
		for _, c := range page {
			var r tokenDetectionResult
			if err := json.Unmarshal([]byte(c.ResultJSON), &r); err != nil {
				return err
			}
			display := displays[c.LogID]
			if display.Stale {
				// The frozen result belongs to an older fact/link revision. Keep the
				// identity visible, but do not expose its old candidate/rule hits as
				// if they were the current decision.
				r = tokenDetectionResult{Version: tokenDetectionVersion, SourceComplete: false, Gaps: []string{"association_or_detection_pending"}}
			}
			items = append(items, map[string]any{"request": c, "result": r, "display": display})
		}
		latest, err := tokenDetectionLatestProgress(tx, q, hash)
		if err != nil {
			return err
		}
		result = map[string]any{"schema_version": tokenDetectionVersion, "range": q, "summary": summary, "analysis": analysis, "items": items, "has_more": hasMore, "source_complete_through": state.CompleteThrough, "latest_progress": latest, "read_only": true, "available": true, "upstream_overcharge_confirmed": false, "generated_at": time.Now().Unix()}
		return nil
	})
	if err == nil {
		result["entry_recent_observations"] = m.tokenRecentEntryObservations(ctx, time.Now().Unix())
	}
	return result, err
}

// A gap before the requested range does not revoke a later window's own
// source proof. Conversely, neither a later successful window nor observed
// facts can fill a hole inside the selection. Count only complete, aligned
// source windows in this bounded range; the global watermark stays unchanged.
func tokenMeteringSelectedRangeComplete(tx *gorm.DB, state TokenMeteringState, from, to int64) (bool, error) {
	if from <= 0 || to <= from || to-from > 7*86400 || state.SourceEpoch == "" || state.SourceMode != "complete" || state.StartAt > from || state.ExpiredBefore > from {
		return false, nil
	}
	first := from / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	last := (to - 1) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	expected := (last-first)/tokenMeteringWindowSeconds + 1
	var complete int64
	err := tx.Model(&TokenMeteringWindow{}).
		Where("source_epoch=? AND start_at>=? AND start_at<=?", state.SourceEpoch, first, last).
		Where("start_at % ? = 0 AND end_at=start_at+? AND done=1 AND source_complete=1 AND replay=0", tokenMeteringWindowSeconds, tokenMeteringWindowSeconds).
		Count(&complete).Error
	return err == nil && complete == expected, err
}

func (m *Monitor) serveTokenDetectionQuery(c *gin.Context) {
	var q tokenDetectionQuery
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	if c.ShouldBindJSON(&q) != nil || !tokenReportFiltersValid(q) || validateTokenMeteringLocalQuery(q.SourceEpoch) != nil || q.From <= 0 || q.To <= q.From || q.To-q.From > 7*86400 || q.AfterAt < 0 || q.AfterID < 0 || q.UserID < 0 || q.ChannelID < 0 || len(q.Model) > 256 || len(q.Group) > 128 || q.Limit < 0 || q.Limit > 100 {
		c.JSON(400, gin.H{"error": "invalid local detection query"})
		return
	}
	if q.SourceEpoch == "" {
		q.SourceEpoch = m.cfg.TokenFactsSourceEpoch
	}
	if q.Limit == 0 {
		q.Limit = 50
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
	defer cancel()
	r, err := m.tokenDetectionReport(ctx, q)
	if err != nil {
		// A busy SQLite reader or an unfinished local rebuild is a data availability gap, not an empty report and not an API outage. Keep the response shape stable so the page can render the explicit unavailable state without retry storms.
		summary := tokenDetectionSummary{Dimensions: map[string]int{}, Amounts: map[string]tokenDetectionAmounts{}, RuleEligible: map[string]int64{}, RuleHits: map[string]int64{}, Unavailable: map[string]int64{}, Gaps: []string{"local_detection_unavailable"}}
		c.JSON(http.StatusOK, gin.H{"schema_version": tokenDetectionVersion, "range": q, "summary": summary, "analysis": map[string]any{}, "items": []map[string]any{}, "has_more": false, "source_complete_through": int64(0), "read_only": true, "available": false, "generated_at": time.Now().Unix()})
		return
	}
	c.JSON(200, r)
}
func (m *Monitor) serveTokenDetectionDetail(c *gin.Context) {
	var q struct {
		SourceEpoch string `json:"source_epoch"`
		LogID       int64  `json:"log_id"`
		Revision    int64  `json:"revision"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1024)
	if c.ShouldBindJSON(&q) != nil || validateTokenMeteringLocalQuery(q.SourceEpoch) != nil || q.LogID <= 0 || q.Revision < 0 {
		c.JSON(400, gin.H{"error": "invalid request identity"})
		return
	}
	if q.SourceEpoch == "" {
		q.SourceEpoch = m.cfg.TokenFactsSourceEpoch
	}
	if m.tokenStore() == nil {
		c.JSON(503, gin.H{"error": "local store unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	query := m.tokenReadStore().WithContext(ctx).Where("source_epoch = ? AND log_id = ?", q.SourceEpoch, q.LogID)
	if q.Revision > 0 {
		query = query.Where("revision = ?", q.Revision)
	}
	var fact TokenMeteringFact
	factErr := m.tokenReadStore().WithContext(ctx).Where("source_epoch=? AND log_id=?", q.SourceEpoch, q.LogID).First(&fact).Error
	var current TokenDetectionCurrent
	currentErr := m.tokenReadStore().WithContext(ctx).Where("source_epoch=? AND log_id=?", q.SourceEpoch, q.LogID).First(&current).Error
	var row TokenDetectionRevision
	err := query.Order("revision DESC").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if factErr == nil {
			display := tokenDetectionDisplay{}
			tokenDetectionDisplayFact(&display, fact)
			tokenDetectionDisplayTransport(&display, fact)
			c.JSON(http.StatusOK, gin.H{
				"available": false, "pending": true, "source_epoch": q.SourceEpoch, "log_id": q.LogID,
				"request": fact, "association_state": fact.LinkState, "association_reason": fact.LinkReason,
				"display":         display,
				"detection_state": tokenDetectionPendingStateForFact(fact), "missing_fields": display.MissingFields,
				"current": func() any {
					if currentErr == nil {
						return current
					}
					return nil
				}(),
				"reason": "frozen_revision_not_available",
			})
			return
		}
		c.JSON(404, gin.H{"error": "frozen revision not found"})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "local evidence unavailable"})
		return
	}
	stale := map[int64]bool{}
	if factErr == nil && currentErr == nil && (q.Revision == 0 || q.Revision == current.Revision) {
		stale, err = tokenDetectionStaleIdentities(m.tokenReadStore().WithContext(ctx), q.SourceEpoch, m.tokenDetectionConfiguredPolicyHash(), []int64{q.LogID})
		if err != nil {
			c.JSON(503, gin.H{"error": "local evidence freshness unavailable"})
			return
		}
	}
	if factErr == nil && currentErr == nil && stale[q.LogID] && (q.Revision == 0 || q.Revision == current.Revision) {
		display := tokenDetectionDisplay{}
		tokenDetectionDisplayFact(&display, fact)
		tokenDetectionDisplayTransport(&display, fact)
		c.JSON(http.StatusOK, gin.H{
			"available": false, "pending": true, "source_epoch": q.SourceEpoch, "log_id": q.LogID,
			"request": fact, "association_state": fact.LinkState, "association_reason": fact.LinkReason,
			"display":         display,
			"detection_state": tokenDetectionPendingStateForFact(fact), "missing_fields": display.MissingFields,
			"current": current, "reason": "current_revision_pending",
		})
		return
	}
	var frozen tokenDetectionFrozen
	if json.Unmarshal([]byte(row.FrozenJSON), &frozen) != nil {
		c.JSON(503, gin.H{"error": "local evidence invalid"})
		return
	}
	display := tokenDetectionDisplay{}
	tokenDetectionDisplayFact(&display, frozen.Input.Fact)
	tokenDetectionDisplayTransport(&display, frozen.Input.Fact, frozen.Input.Usage)
	c.JSON(200, gin.H{"revision": row.Revision, "decision_hash": row.DecisionHash, "recorded_at": row.RecordedAt, "evidence": frozen, "assessment": tokenAssessFrozen(frozen), "display": display})
}
func (m *Monitor) serveTokenDetectionHealth(c *gin.Context) {
	p, configErr := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
	var state TokenDetectionState
	unavailable := func(reason string) {
		body := gin.H{"schema_version": tokenDetectionVersion, "enabled": m.cfg.TokenDetectionEnabled && !m.cfg.LocalSnapshotOnly, "available": false, "policy_configured": configErr == nil, "pending_sample_count": nil, "pending_sample_limit": 50, "current_results": nil, "frozen_bytes": nil, "total_payload_bytes": nil, "deferred_requests": nil, "references": nil, "active_references": nil, "round": nil, "gap_reasons": []string{reason}, "read_only": true, "upstream_overcharge_confirmed": false}
		if state.SourceEpoch != "" {
			body["state"] = state
			body["round"] = tokenDetectionRoundHealth{Processed: state.LastProcessed, Yielded: state.LastYielded, Failed: state.LastFailed, Deferred: state.LastDeferred}
		}
		c.JSON(http.StatusOK, body)
	}
	if m.tokenStore() == nil {
		unavailable("local_store_unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	hash := ""
	if configErr == nil {
		hash = tokenDetectionPolicyHash(p)
	}
	epoch := m.cfg.TokenFactsSourceEpoch
	db := m.tokenReadStore().WithContext(ctx)
	if err := db.Where("source_epoch = ?", epoch).Limit(1).Find(&state).Error; err != nil {
		unavailable("local_detection_health_unavailable")
		return
	}
	if state.SourceEpoch == "" {
		unavailable("no_detection_checkpoint")
		return
	}
	jobs, err := tokenDetectionPendingSample(db, epoch, hash, time.Now().Unix(), 50)
	if err != nil {
		unavailable("local_detection_backlog_unavailable")
		return
	}
	maxRows, maxBytes := m.cfg.tokenDetectionLimits()
	maxReferences, maxReferenceRecords := m.cfg.tokenDetectionReferenceLimits()
	var count, bytes, refs, activeRefs, deferred, refBytes, headBytes int64
	detectionStates := map[string]int64{}
	retryReasons := map[string]int64{}
	var capacity tokenDetectionCapacity
	var capacityErr error
	if db.Migrator().HasTable(&tokenDetectionCapacity{}) {
		capacityErr = db.First(&capacity, 1).Error
	} else {
		// Read-only compatibility for snapshots created before the ledger existed.
		capacityErr = db.Raw(`SELECT
			(SELECT COUNT(*) FROM token_detection_currents) AS current_rows,
			(SELECT COALESCE(SUM(stored_bytes),0) FROM token_detection_revisions) AS revision_bytes,
			(SELECT COALESCE(SUM(stored_bytes),0) FROM token_detection_references) AS reference_bytes,
			(SELECT COALESCE(SUM(length(CAST(result_json AS BLOB))),0) FROM token_detection_currents) AS head_bytes,
			(SELECT COUNT(*) FROM token_detection_references) AS reference_rows`).Scan(&capacity).Error
	}
	if capacityErr != nil {
		unavailable("local_detection_capacity_unavailable")
		return
	}
	// The capacity ledger is global because it protects the shared detection
	// store.  The health response, however, is for the configured source epoch;
	// do not let an old/parallel epoch inflate current_results or its state
	// breakdown and make the selected queue look processed when it is not.
	bytes, refs, refBytes, headBytes = capacity.RevisionBytes, capacity.ReferenceRows, capacity.ReferenceBytes, capacity.HeadBytes
	if err := tokenDetectionActiveReferences(db, time.Now().Unix()).Count(&activeRefs).Error; err != nil {
		unavailable("local_detection_capacity_unavailable")
		return
	}
	stateRows, err := tokenDetectionStateCounts(db, epoch)
	if err != nil {
		unavailable("local_detection_state_unavailable")
		return
	}
	for _, row := range stateRows {
		key := row.State
		if key == "" {
			key = "pending"
		}
		detectionStates[key] += row.Count
		count += row.Count
	}
	retryRows, err := tokenDetectionRetryCounts(db, epoch)
	if err != nil {
		unavailable("local_detection_retry_state_unavailable")
		return
	}
	for _, row := range retryRows {
		retryReasons[row.Reason] = row.Count
		deferred += row.Count
	}
	queueReady, err := tokenReportProjectionsReady(db)
	if err != nil {
		unavailable("local_detection_queue_unavailable")
		return
	}
	gaps := []string{}
	if !queueReady {
		gaps = append(gaps, "detection_queue_rebuilding")
	}
	round := tokenDetectionRoundHealth{Processed: state.LastProcessed, Yielded: state.LastYielded, Failed: state.LastFailed, Deferred: state.LastDeferred}
	c.JSON(200, gin.H{"schema_version": tokenDetectionVersion, "enabled": m.cfg.TokenDetectionEnabled && !m.cfg.LocalSnapshotOnly, "available": true, "queue_ready": queueReady, "gap_reasons": gaps, "policy_configured": configErr == nil, "policy_hash": hash, "state": state, "round": round, "pending_sample_count": len(jobs), "pending_sample_limit": 50, "current_results": count, "detection_states": detectionStates, "retry_reasons": retryReasons, "frozen_bytes": bytes, "total_payload_bytes": bytes + refBytes + headBytes, "revision_budget_exhausted": 0, "deferred_requests": deferred, "references": refs, "active_references": activeRefs, "max_results": maxRows, "max_frozen_bytes": maxBytes, "detection_workers": m.cfg.tokenDetectionWorkers(), "detection_work_budget_ms": m.cfg.tokenDetectionWorkMS(), "detection_commit_rows": m.cfg.tokenDetectionCommitLimit(), "historical_scan_rows": tokenDetectionScanRows, "processing_batch_rows": m.cfg.tokenProcessingBatchRows(), "processing_poll_seconds": m.cfg.tokenProcessingPollSeconds(), "retention_days": m.cfg.tokenDetectionRetentionSeconds() / 86400, "retention_basis": m.cfg.TokenDetectionRetentionBasis, "payload_budget_basis": "uncompressed_json", "isolated_store": m.tokenDB != nil, "max_references": maxReferences, "max_revisions": nil, "revision_limit_mode": "content_changes_with_global_storage_budget", "max_reference_records": maxReferenceRecords, "full_frontend": true, "notifications": m.cfg.TokenNotificationsEnabled && !m.cfg.LocalSnapshotOnly, "upstream_overcharge_confirmed": false})
}

type tokenDetectionStateCount struct {
	State string `gorm:"column:state"`
	Count int64  `gorm:"column:count"`
}

func tokenDetectionStateCounts(db *gorm.DB, epoch string) ([]tokenDetectionStateCount, error) {
	table := "token_detection_currents"
	if db.Migrator().HasIndex(&TokenDetectionCurrent{}, "idx_token_detection_epoch_state_v1") {
		table += " INDEXED BY idx_token_detection_epoch_state_v1"
	}
	var rows []tokenDetectionStateCount
	err := db.Table(table).Where("source_epoch=?", epoch).
		Select("COALESCE(detection_state,'pending') AS state,COUNT(*) AS count").Group("detection_state").Scan(&rows).Error
	return rows, err
}

type tokenDetectionRetryCount struct {
	Reason string `gorm:"column:reason"`
	Count  int64  `gorm:"column:count"`
}

func tokenDetectionRetryTable(db *gorm.DB) string {
	table := "token_detection_retries"
	if db.Migrator().HasIndex(&TokenDetectionRetry{}, "idx_token_detection_epoch_retry_reason_v1") {
		table += " INDEXED BY idx_token_detection_epoch_retry_reason_v1"
	}
	return table
}

func tokenDetectionRetryCounts(db *gorm.DB, epoch string) ([]tokenDetectionRetryCount, error) {
	var rows []tokenDetectionRetryCount
	err := db.Table(tokenDetectionRetryTable(db)).Where("source_epoch=?", epoch).
		Select("COALESCE(reason,'unknown') AS reason,COUNT(*) AS count").Group("reason").Scan(&rows).Error
	return rows, err
}
