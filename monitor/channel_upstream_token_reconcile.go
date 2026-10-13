package monitor

// Read-only readiness check. Successful evidence is not a confirmed mapping:
// this code never changes bindings, money, queues, or an upstream service.

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type upstreamTokenHourCheck struct {
	HourTs         int64  `json:"hour_ts"`
	Status         string `json:"status"`
	EvidenceUnits  string `json:"evidence_units,omitempty"`
	ControlUnits   string `json:"control_units,omitempty"`
	DeltaUnits     string `json:"delta_units,omitempty"`
	ToleranceUnits string `json:"tolerance_units,omitempty"`
}

type upstreamTokenReconcileView struct {
	DayTs               int64                    `json:"day_ts"`
	Status              string                   `json:"status"`
	StableScans         int                      `json:"stable_scans"`
	ReadyHours          int                      `json:"ready_hours"`
	RoundingHours       int                      `json:"rounding_hours"`
	BlockedHours        int                      `json:"blocked_hours"`
	TokenSeen           bool                     `json:"token_seen"`
	AttributionVerified bool                     `json:"attribution_verified"` // intentionally false
	Hours               []upstreamTokenHourCheck `json:"hours"`
}

func checkSub2TokenReconciliation(account ChannelUpstreamAccount, tokenID string, day, now int64,
	cursor ChannelUpstreamTokenScanCheckpoint, pricingRows []ChannelUpstreamPricingHourEvidence,
	pricingStates []ChannelUpstreamPricingHourState, controls []ChannelUpstreamUsageHour,
	key []byte, keyID string) (upstreamTokenReconcileView, error) {
	view := upstreamTokenReconcileView{DayTs: day, Status: "evidence_missing", BlockedHours: 24, Hours: []upstreamTokenHourCheck{}}
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return view, err
	}
	id, err := canonicalUpstreamTokenID(tokenID)
	if err != nil || id == "" {
		return view, errUpstreamTokenScope
	}
	if now < day+86400 {
		view.Status = "window_open"
		return view, nil
	}
	if cursor.Envelope == "" {
		return view, nil
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), account, day, key, keyID)
	if err != nil {
		view.Status = "evidence_invalid"
		return view, nil
	}
	if cursor.Revision <= 0 || cursor.SuccessRevision != cursor.Revision || cursor.SuccessHash != channelCostEvidenceContentHash(cp.Rows) || cursor.SuccessKeyID != keyID {
		view.Status = "scan_not_completed"
		return view, nil
	}
	view.StableScans = cursor.StableScans
	if cursor.StableScans < 2 {
		view.Status = "waiting_second_scan"
		return view, nil
	}
	source, err := channelCostSourceRef(key, account.Provider, cp.AccountEpoch, sub2TokenSourceKind, id)
	if err != nil {
		return view, err
	}
	for _, row := range cp.Rows {
		if row.SourceRef == source {
			view.TokenSeen = true
		}
	}
	if len(pricingRows) > sub2TokenMaxScanRows || len(pricingStates) > 24 || len(controls) > 24 {
		return view, errors.New("令牌对账输入超过单日边界")
	}
	legacy := make(map[int64][]ChannelUpstreamPricingHourEvidence, 24)
	for hour := day; hour < day+86400; hour += 3600 {
		legacy[hour] = nil
	}
	for _, row := range pricingRows {
		if row.Domain != account.Domain || row.AccountEpoch != cp.AccountEpoch || row.Provider != account.Provider ||
			row.SemanticsVersion != upstreamPricingSemanticsVersion || row.HourTs < day || row.HourTs >= day+86400 || row.HourTs%3600 != 0 {
			return view, errUpstreamTokenScope
		}
		legacy[row.HourTs] = append(legacy[row.HourTs], row)
	}
	if err := reconcileSub2TokenShadow(cp, legacy); err != nil {
		view.Status = "pricing_evidence_changed"
		return view, nil
	}
	states := make(map[int64]ChannelUpstreamPricingHourState, 24)
	for _, row := range pricingStates {
		_, duplicate := states[row.HourTs]
		if duplicate || row.Domain != account.Domain || row.AccountEpoch != cp.AccountEpoch || row.SemanticsVersion != upstreamPricingSemanticsVersion || row.HourTs < day || row.HourTs >= day+86400 || row.HourTs%3600 != 0 {
			return view, errUpstreamTokenScope
		}
		states[row.HourTs] = row
	}
	usage := make(map[int64]ChannelUpstreamUsageHour, 24)
	for _, row := range controls {
		_, duplicate := usage[row.HourTs]
		if duplicate || row.Domain != account.Domain || row.HourTs < day || row.HourTs >= day+86400 || row.HourTs%3600 != 0 {
			return view, errUpstreamTokenScope
		}
		usage[row.HourTs] = row
	}
	view.BlockedHours = 0
	for hour := day; hour < day+86400; hour += 3600 {
		check := upstreamTokenHourCheck{HourTs: hour, Status: "pricing_unverified"}
		state, exists := states[hour]
		computed, err := pricingHourStateFromEvidence(account, hour, legacy[hour], now)
		if err != nil {
			return view, err
		}
		switch {
		case !exists || state.Status != "verified" || state.VerifiedScans < 2 || !pricingReconcileAccepted(account.Provider, state.ReconcileStatus):
		case state.ContentHash != computed.ContentHash || state.SourceRows != computed.SourceRows || state.EligibleRequests != computed.EligibleRequests ||
			state.Tokens != computed.Tokens || state.FinalQuota != computed.FinalQuota || state.EvidenceRows != computed.EvidenceRows:
			check.Status = "pricing_state_changed"
		default:
			check = reconcileSub2TokenControl(computed, state, usage[hour], hour)
		}
		if check.Status == "matched" || check.Status == "rounding_only" {
			view.ReadyHours++
		} else {
			view.BlockedHours++
		}
		if check.Status == "rounding_only" {
			view.RoundingHours++
		}
		view.Hours = append(view.Hours, check)
	}
	view.Status = "reconciliation_blocked"
	if view.ReadyHours == 24 {
		view.Status = "evidence_ready"
	}
	// Empty history is not proof that this particular configured token exists.
	if !view.TokenSeen && view.Status == "evidence_ready" {
		view.Status = "token_not_observed"
	}
	return view, nil
}

func reconcileSub2TokenControl(computed, recorded ChannelUpstreamPricingHourState, control ChannelUpstreamUsageHour, hour int64) upstreamTokenHourCheck {
	view := upstreamTokenHourCheck{HourTs: hour, Status: "control_missing"}
	if control.Domain == "" {
		return view
	}
	if control.Provider != upstreamProviderSub2API || control.BucketSeconds != 3600 || control.Provisional || control.Requests < 0 || control.Tokens < 0 || control.CostUSD < 0 {
		view.Status = "control_invalid"
		return view
	}
	units, exact := pricingLegacyQuotaAsExact(upstreamProviderSub2API, control)
	// At larger float magnitudes single micro-units are no longer distinct.
	if !exact || units < 0 || units >= 1<<53 {
		view.Status = "control_invalid"
		return view
	}
	view.EvidenceUnits, view.ControlUnits = strconv.FormatInt(computed.FinalQuota, 10), strconv.FormatInt(units, 10)
	delta := computed.FinalQuota - units // nonnegative operands avoid subtraction overflow
	view.DeltaUnits = strconv.FormatInt(delta, 10)
	tolerance := int64(0)
	if computed.EligibleRequests > 0 {
		tolerance = computed.EligibleRequests/2 + 1
	}
	view.ToleranceUnits = strconv.FormatInt(tolerance, 10)
	if pricingLegacySnapshotChanged(upstreamProviderSub2API, recorded, control) {
		view.Status = "control_changed"
		return view
	}
	if computed.EligibleRequests != control.Requests || computed.Tokens != control.Tokens || !pricingQuotaReconciled(upstreamProviderSub2API, delta, computed.EligibleRequests) {
		view.Status = "control_mismatch"
		return view
	}
	view.Status = "matched"
	if delta != 0 {
		view.Status = "rounding_only"
	}
	return view
}

// Single SQLite read snapshot, bounded to one selected day; no external I/O.
func (m *Monitor) loadUpstreamTokenReconciliation(ctx context.Context, domain string, channelID int, day, now int64) (upstreamTokenReconcileView, error) {
	view := upstreamTokenReconcileView{DayTs: day, Status: "unsupported", Hours: []upstreamTokenHourCheck{}}
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return view, err
	}
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, channelID)
		if err != nil {
			return err
		}
		config, err := readUpstreamTokenConfig(tx, account, channelID)
		if err != nil {
			return err
		}
		if config.TokenID == "" {
			view.Status = "unconfigured"
			return nil
		}
		if account.Provider == upstreamProviderNewAPI {
			proof, err := readNewAPITokenProof(tx, account, day, now, m.cfg.ChannelCostHMACKeyID)
			if err != nil {
				return err
			}
			source, err := channelCostSourceRef([]byte(m.cfg.ChannelCostHMACKey), account.Provider, newAPIUpstreamAccountEpoch(account), channelCostSourceKindNewAPIToken, config.TokenID)
			if err != nil {
				view.Status = "verification_unavailable"
				return nil
			}
			view = proof.View
			view.TokenSeen = proof.Sources[source]
			if view.Status == "evidence_ready" && !view.TokenSeen {
				view.Status = "token_not_observed"
			}
			return nil
		}
		if account.Provider != upstreamProviderSub2API || account.UsageAdapter == upstreamUsageAdapterSub2Stats {
			return nil
		}
		key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
		if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
			view.Status = "verification_unavailable"
			return nil
		}
		cursor, err := readSub2TokenStoredCursor(tx, account, day)
		if err != nil {
			return err
		}
		var pricingRows []ChannelUpstreamPricingHourEvidence
		var states []ChannelUpstreamPricingHourState
		var controls []ChannelUpstreamUsageHour
		epoch := newAPIUpstreamAccountEpoch(account)
		if cursor.Envelope != "" {
			if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", domain, epoch, upstreamPricingSemanticsVersion, day, day+86400).Limit(sub2TokenMaxScanRows + 1).Find(&pricingRows).Error; err != nil {
				return err
			}
			if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", domain, epoch, upstreamPricingSemanticsVersion, day, day+86400).Limit(25).Find(&states).Error; err != nil {
				return err
			}
			if err := tx.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, day, day+86400).Limit(25).Find(&controls).Error; err != nil {
				return err
			}
		}
		view, err = checkSub2TokenReconciliation(account, config.TokenID, day, now, cursor, pricingRows, states, controls, key, keyID)
		return err
	})
	return view, err
}

func (m *Monitor) getUpstreamTokenReconciliationHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	channelID, err := strconv.Atoi(c.Query("channel_id"))
	day, dayErr := strconv.ParseInt(c.Query("day_ts"), 10, 64)
	if err != nil || dayErr != nil || day < 0 || day > time.Now().Unix() || day != cstDayStart(day) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请指定有效渠道和中国自然日开始时间"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	view, err := m.loadUpstreamTokenReconciliation(ctx, c.Query("domain"), channelID, day, time.Now().Unix())
	if err != nil {
		upstreamTokenConfigHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, view)
}
