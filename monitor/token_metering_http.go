package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type tokenMeteringHealth struct {
	GeneratedAt            int64                       `json:"generated_at"`
	Available              bool                        `json:"available"`
	TargetThrough          int64                       `json:"target_through"`
	FinalizationLagSeconds int64                       `json:"finalization_lag_seconds"`
	LinkReasons            map[string]int64            `json:"link_reasons"`
	EntryEvidenceHealth    nginxEvidenceHealth         `json:"entry_evidence_health"`
	PricingSupport         map[string]int64            `json:"pricing_support"`
	OtherStates            map[string]int64            `json:"other_states"`
	ClosedLinkReviews      int64                       `json:"closed_link_reviews"`
	Enabled                bool                        `json:"enabled"`
	SourceAvailable        bool                        `json:"source_available"`
	SourceEpoch            string                      `json:"source_epoch"`
	SourceDeclaration      string                      `json:"source_declaration"`
	ParserVersion          string                      `json:"parser_version"`
	LengthMeaning          string                      `json:"request_length_meaning"`
	State                  *TokenMeteringState         `json:"state"`
	Backpressure           *tokenPipelinePressure      `json:"backpressure"`
	Lanes                  []TokenMeteringLaneState    `json:"lanes"`
	Facts                  int64                       `json:"facts"`
	StoredBytes            int64                       `json:"stored_bytes"`
	PendingLinks           int64                       `json:"pending_links"`
	PendingLinksBasis      string                      `json:"pending_links_basis"`
	UnresolvedLinks        int64                       `json:"unresolved_links"`
	FailedWindows          int64                       `json:"failed_windows"`
	PendingWindows         int64                       `json:"pending_windows"`
	ScanLagSeconds         int64                       `json:"scan_lag_seconds"`
	OldestRepairAt         *int64                      `json:"oldest_repair_at"`
	LinkStates             map[string]int64            `json:"link_states"`
	LengthStates           map[string]int64            `json:"request_length_states"`
	SemanticReasons        map[string]int64            `json:"semantic_reasons"`
	FieldStates            map[string]map[string]int64 `json:"field_states"`
	GapReasons             []string                    `json:"gap_reasons"`
	RepairLookbackSeconds  int64                       `json:"repair_lookback_seconds"`
	RetentionDays          int64                       `json:"retention_days"`
	LinkRetryLimit         int                         `json:"link_retry_limit"`
	HealthCountsReady      bool                        `json:"health_counts_ready"`
	HealthCountsState      string                      `json:"health_counts_state"`
}

func (h tokenMeteringHealth) MarshalJSON() ([]byte, error) {
	type wire tokenMeteringHealth
	if h.Available {
		return json.Marshal(wire(h))
	}
	// Numeric zero is an observation, not a fallback for an unfinished rebuild
	// or a failed read. Keep configuration/checkpoints but publish unknown counts.
	return json.Marshal(struct {
		wire
		Facts             *int64 `json:"facts"`
		StoredBytes       *int64 `json:"stored_bytes"`
		PendingLinks      *int64 `json:"pending_links"`
		UnresolvedLinks   *int64 `json:"unresolved_links"`
		ClosedLinkReviews *int64 `json:"closed_link_reviews"`
		FailedWindows     *int64 `json:"failed_windows"`
		PendingWindows    *int64 `json:"pending_windows"`
		ScanLagSeconds    *int64 `json:"scan_lag_seconds"`
		LinkStates        any    `json:"link_states"`
		LinkReasons       any    `json:"link_reasons"`
		LengthStates      any    `json:"request_length_states"`
		SemanticReasons   any    `json:"semantic_reasons"`
		FieldStates       any    `json:"field_states"`
		PricingSupport    any    `json:"pricing_support"`
		OtherStates       any    `json:"other_states"`
	}{wire: wire(h)})
}

func (m *Monitor) serveTokenMeteringHealth(c *gin.Context) {
	epoch := c.Query("source_epoch")
	if err := validateTokenMeteringLocalQuery(epoch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source_epoch"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	h, err := m.tokenMeteringHealth(ctx, epoch, time.Now().Unix())
	if err != nil {
		// Health is an observability endpoint. A busy reader or an in-progress counter rebuild must be visible as unavailable data, not as an HTTP outage that makes the whole Token page retry and hide the gap.
		h.HealthCountsState = "unavailable"
		h.HealthCountsReady = false
		h.GapReasons = append(h.GapReasons, "local_health_unavailable")
		c.JSON(http.StatusOK, h)
		return
	}
	c.JSON(http.StatusOK, h)
}
func (m *Monitor) tokenMeteringHealth(ctx context.Context, epoch string, now int64) (tokenMeteringHealth, error) {
	if epoch == "" {
		epoch = m.cfg.TokenFactsSourceEpoch
	}
	h := tokenMeteringHealth{Enabled: m.cfg.TokenFactsEnabled && !m.cfg.LocalSnapshotOnly, SourceAvailable: m.sourceAccessAllowed() && m.prodDB != nil,
		SourceEpoch: epoch, SourceDeclaration: m.cfg.TokenFactsSourceMode, ParserVersion: tokenMeteringParserVersion, LengthMeaning: "whole_http_request_not_body_or_token_count",
		LinkStates: map[string]int64{}, LengthStates: map[string]int64{}, SemanticReasons: map[string]int64{}, FieldStates: map[string]map[string]int64{}, GapReasons: []string{}, RepairLookbackSeconds: tokenMeteringRepairSeconds, LinkRetryLimit: tokenMeteringMaxLinkAttempts}
	h.GeneratedAt = now
	h.PendingLinksBasis = "scheduled_initial_or_late_evidence_review"
	h.RetentionDays = m.cfg.tokenMeteringRetentionSeconds() / 86400
	h.TargetThrough = (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	h.FinalizationLagSeconds = tokenMeteringLagSeconds
	h.LinkReasons = map[string]int64{}
	h.PricingSupport, h.OtherStates = map[string]int64{}, map[string]int64{}
	if m.tokenStore() == nil {
		return h, errors.New("local store unavailable")
	}
	err := m.tokenReadStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		countsTable := tx.Migrator().HasTable(&tokenMeteringHealthCount{})
		countsAvailable := false
		if countsTable {
			var countErr error
			countsAvailable, countErr = tokenMeteringHealthCountsReady(tx)
			if countErr != nil {
				return countErr
			}
		}
		if countsAvailable {
			h.HealthCountsReady = true
			h.HealthCountsState = "ready"
		} else if countsTable {
			h.HealthCountsState = "rebuilding"
			h.GapReasons = append(h.GapReasons, "health_counts_rebuilding")
		} else {
			h.HealthCountsState = "legacy"
		}
		var state TokenMeteringState
		err := tx.First(&state, "source_epoch = ?", epoch).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			// Read-only normalization protects health queries before the worker resumes.
			if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
				return err
			}
			h.State = &state
			if state.CleanupBackupID == "" || state.CleanupVerifiedAt == 0 || state.CleanupSafeBefore == 0 {
				state.CleanupReason = "backup_and_reconciliation_required"
			} else if state.CleanupVerifiedAt-state.CleanupStableSince < 1800 || state.CleanupStableSince == 0 {
				state.CleanupReason = "stable_recovery_validation_required"
			}
			if tx.Migrator().HasTable(&TokenMeteringLaneState{}) {
				if err := tx.Where("source_epoch=?", epoch).Order("lane").Limit(3).Find(&h.Lanes).Error; err != nil {
					return err
				}
				for i := range h.Lanes {
					lane := &h.Lanes[i]
					kind := tokenMeteringLaneForward
					if lane.Lane == tokenMeteringLaneRealtimeTail {
						kind = tokenMeteringLaneRecent
					} else if lane.Lane == tokenMeteringLaneReplayRepair {
						kind = tokenMeteringLaneReplay
					}
					lane.QueueUnit, lane.MaxInFlightBatches = "windows", 1
					lane.ReadBudgetMS, lane.WriterWaitBudgetMS = tokenMeteringLaneBudget(kind).Milliseconds(), tokenMeteringLaneWaitBudget(kind).Milliseconds()
					lane.BatchRows = tokenMeteringPageRows
					if lane.LeaseUntil <= now {
						lane.Running = 0
					}
					var pending struct{ Queued, Oldest int64 }
					if err := tx.Model(&TokenMeteringWindow{}).Where("source_epoch=? AND lane=? AND (done=0 OR replay=1)", epoch, lane.Lane).
						Select("COUNT(*) AS queued,COALESCE(MIN(start_at),0) AS oldest").Scan(&pending).Error; err != nil {
						return err
					}
					lane.Queued, lane.OldestPendingAt = pending.Queued, pending.Oldest
					if lane.Lane == tokenMeteringLaneForwardCatchup && state.ScanThrough < h.TargetThrough {
						lane.Queued = max(lane.Queued, (h.TargetThrough-state.ScanThrough)/tokenMeteringWindowSeconds)
						lane.OldestPendingAt = state.ScanThrough
					}
				}
			}
			if _, err := m.cfg.tokenMeteringPinnedKeys(state.HMACProfiles); err != nil {
				h.GapReasons = append(h.GapReasons, "hmac_key_version_conflict")
			}
			h.SourceDeclaration = state.SourceMode
			h.ScanLagSeconds = max(int64(0), h.TargetThrough-state.ScanThrough)
			if state.ExpiredGapWindows > 0 {
				h.GapReasons = append(h.GapReasons, "retention_expired_uncovered_windows")
			}
			if state.LastReason != "" {
				h.GapReasons = append(h.GapReasons, state.LastReason)
			}
		} else {
			h.GapReasons = append(h.GapReasons, "no_collection_checkpoint")
		}
		if countsAvailable {
			if err := readTokenMeteringHealthCounts(tx, epoch, &h); err != nil {
				if errors.Is(err, errTokenHealthCountsInconsistent) {
					h.GapReasons = append(h.GapReasons, "health_counts_inconsistent")
				}
				return err
			}
			consistent := tokenMeteringHealthCountsConsistent(&h)
			if consistent && h.Facts == 0 {
				// A missing entire ledger also sums to zero. An indexed existence
				// probe rejects that false empty result before periodic repair runs.
				var present bool
				if err := tx.Raw("SELECT EXISTS(SELECT 1 FROM token_metering_facts WHERE source_epoch=? LIMIT 1)", epoch).Scan(&present).Error; err != nil {
					return err
				}
				consistent = !present
			}
			if !consistent {
				h.HealthCountsReady = false
				h.HealthCountsState = "unavailable"
				h.GapReasons = append(h.GapReasons, "health_counts_inconsistent")
				return errTokenHealthCountsInconsistent
			}
		} else if !countsTable {
			var size struct{ Rows, Bytes int64 }
			if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ?", epoch).Select("COUNT(*) AS rows, COALESCE(SUM(stored_bytes),0) AS bytes").Scan(&size).Error; err != nil {
				return err
			}
			h.Facts, h.StoredBytes = size.Rows, size.Bytes
			if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND link_next_at > 0", epoch).Count(&h.PendingLinks).Error; err != nil {
				return err
			}
		}
		// Existing counters without a ready marker remain rebuilding/unavailable;
		// do not mix a direct facts scan with their partial ledger.
		if err := tx.Model(&TokenMeteringWindow{}).Where("source_epoch = ? AND last_reason != ''", epoch).Count(&h.FailedWindows).Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenMeteringWindow{}).Where("source_epoch = ? AND replay = ?", epoch, true).Count(&h.PendingWindows).Error; err != nil {
			return err
		}
		if h.PendingWindows > 0 {
			h.GapReasons = append(h.GapReasons, "replay_in_progress")
		}
		if h.State != nil {
			sealed := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
			var accounted int64
			// Recent EOFs are already collected, while active replays were
			// counted above. Neither should be counted again as missing history.
			if err := tx.Model(&TokenMeteringWindow{}).Where("source_epoch=? AND start_at>=? AND end_at<=? AND start_at % ?=0 AND end_at=start_at+? AND ((done=1 AND replay=0) OR replay=1)", epoch, h.State.ScanThrough, sealed, tokenMeteringWindowSeconds, tokenMeteringWindowSeconds).Count(&accounted).Error; err != nil {
				return err
			}
			h.PendingWindows += max(int64(0), (sealed-h.State.ScanThrough)/tokenMeteringWindowSeconds-accounted)
		}
		if err := tx.Model(&TokenMeteringWindow{}).Where("source_epoch = ? AND done = ? AND start_at >= ?", epoch, true, now-tokenMeteringRepairSeconds).Select("MIN(last_scanned_at)").Scan(&h.OldestRepairAt).Error; err != nil {
			return err
		}
		if !countsAvailable && !countsTable {
			for column, target := range map[string]map[string]int64{"link_state": h.LinkStates, "link_reason": h.LinkReasons, "length_state": h.LengthStates, "semantic_reason": h.SemanticReasons} {
				if err := tokenMeteringHealthGroups(tx, epoch, column, target); err != nil {
					return err
				}
			}
			h.PricingSupport = map[string]int64{}
			h.OtherStates = map[string]int64{}
			if err := tokenMeteringHealthGroups(tx, epoch, "json_extract(source_facts, '$.pricing_support')", h.PricingSupport); err != nil {
				return err
			}
			if err := tokenMeteringHealthGroups(tx, epoch, "json_extract(source_facts, '$.other_state')", h.OtherStates); err != nil {
				return err
			}
			if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND link_next_at = 0", epoch).Count(&h.ClosedLinkReviews).Error; err != nil {
				return err
			}
			// Fixed JSON paths only. No request-controlled SQL; aggregate locally without
			// loading per-request facts or invoking a source service.
			var fieldErr error
			h.FieldStates, fieldErr = tokenMeteringHealthFields(tx, epoch)
			if fieldErr != nil {
				return fieldErr
			}
		}
		return nil
	})
	h.EntryEvidenceHealth = m.nginxEvidenceHealth(ctx, time.Unix(now, 0))
	if pressure, pressureErr := m.tokenPipelinePressure(ctx); pressureErr == nil {
		h.Backpressure = &pressure
	} else {
		h.GapReasons = append(h.GapReasons, "backpressure_state_unavailable")
	}
	if h.EntryEvidenceHealth.Enabled && !h.EntryEvidenceHealth.StoreReachable {
		h.GapReasons = append(h.GapReasons, "entry_health_unavailable")
	}
	if !h.Enabled {
		h.GapReasons = append(h.GapReasons, "collection_disabled")
	}
	if !h.SourceAvailable {
		h.GapReasons = append(h.GapReasons, "source_unavailable")
	}
	if h.SourceDeclaration != "complete" {
		h.GapReasons = append(h.GapReasons, "source_completeness_unverified")
	}
	if m.nginxEvidenceDB == nil || nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" {
		h.GapReasons = append(h.GapReasons, "local_entry_evidence_unavailable")
	}
	if h.FailedWindows > 0 {
		h.GapReasons = append(h.GapReasons, "window_failures")
	}
	// A unique association remains scheduled until its late-evidence review
	// closes. Scheduling and evidence states are independent partitions.
	h.UnresolvedLinks = h.Facts - h.LinkStates["unique"]
	h.Available = err == nil && h.State != nil && (h.HealthCountsReady || h.HealthCountsState == "legacy")
	return h, err
}
func tokenMeteringHealthGroups(tx *gorm.DB, epoch, column string, out map[string]int64) error {
	var rows []struct {
		Name  string
		Count int64
	}
	if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch = ?", epoch).Select("COALESCE(" + column + ", 'missing') AS name, COUNT(*) AS count").Group(column).Scan(&rows).Error; err != nil {
		return err
	}
	for _, r := range rows {
		key := r.Name
		if key == "" {
			key = "missing"
		}
		out[key] = r.Count
	}
	return nil
}

// One bounded local aggregate scans operands once for all scalar support counts.
func tokenMeteringHealthFields(tx *gorm.DB, epoch string) (map[string]map[string]int64, error) {
	fields := tokenHealthFields()
	states := []string{"missing", "null", "valid", "sentinel", "invalid"}
	selections := make([]string, 0, len(fields)*len(states))
	for _, field := range fields {
		for _, state := range states {
			selections = append(selections, "COALESCE(SUM(CASE WHEN COALESCE(json_extract(source_facts, '$.fields."+field+".state'),'missing') = '"+state+"' THEN 1 ELSE 0 END),0)")
		}
	}
	values := make([]int64, len(selections))
	dest := make([]any, len(values))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := tx.Raw("SELECT "+strings.Join(selections, ",")+" FROM token_metering_facts WHERE source_epoch = ?", epoch).Row().Scan(dest...); err != nil {
		return nil, err
	}
	out := map[string]map[string]int64{}
	for i, field := range fields {
		out[field] = map[string]int64{}
		for j, state := range states {
			if n := values[i*len(states)+j]; n > 0 {
				out[field][state] = n
			}
		}
	}
	return out, nil
}
