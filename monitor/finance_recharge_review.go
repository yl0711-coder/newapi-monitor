package monitor

// Review context explains recorded configuration, not real payment history.
// It never recommends applying the latest rate to an earlier bill. Only the
// existing rechargeTermsForBucket decides whether accounting can use a rate.
type FinanceRechargeVersionReference struct {
	Version     int64    `json:"version"`
	EffectiveAt int64    `json:"effective_at"`
	Valid       bool     `json:"valid"`
	Paid        *float64 `json:"paid,omitempty"`
	Credited    *float64 `json:"credited,omitempty"`
}

func financeRechargeReviewReference(version channelRechargeVersion) FinanceRechargeVersionReference {
	valid := version.Valid && validChannelFinanceNumber(version.Paid) && validChannelFinanceNumber(version.Credit)
	ref := FinanceRechargeVersionReference{Version: version.Version, EffectiveAt: version.EffectiveAt, Valid: valid}
	if valid {
		paid, credited := version.Paid, version.Credit
		ref.Paid, ref.Credited = &paid, &credited
	}
	return ref
}

func financeRechargeReviewVersions(versions []channelRechargeVersion, start, end int64) (*FinanceRechargeVersionReference, []FinanceRechargeVersionReference) {
	var selected *FinanceRechargeVersionReference
	var changes []FinanceRechargeVersionReference
	for _, version := range versions {
		if version.EffectiveAt >= end {
			break // An exclusive boundary belongs to the next bill bucket.
		}
		ref := financeRechargeReviewReference(version)
		if version.EffectiveAt <= start {
			selected = &ref
		} else {
			changes = append(changes, ref)
		}
	}
	return selected, changes
}

func sameFinanceRechargeReviewVersion(left, right *FinanceRechargeVersionReference) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Version == right.Version && left.EffectiveAt == right.EffectiveAt && left.Valid == right.Valid
}

func financeRechargeReviewAction(reason string, hasBoundaryRecords bool) string {
	switch reason {
	case "before_first_version", "no_history":
		if hasBoundaryRecords {
			return "confirm_historical_rate_and_review_boundary_records"
		}
		return "confirm_historical_rate_and_effective_range"
	case "invalid_effective_version":
		return "review_invalid_recorded_rate"
	case upstreamAdjustedCostBucketAmbiguous:
		return "distinguish_configuration_correction_from_real_rate_change"
	default:
		return "review_original_evidence"
	}
}
