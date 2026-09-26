package monitor

// Diagnostic metadata only. It must not change the strict/mixed selection,
// attach a cost to a channel, or interpret missing publications as zero cost.
type financeInternalPairDiagnostic struct {
	HasPublication     bool
	MissingCost        bool
	ManifestUnverified bool
}

func (d financeInternalPairDiagnostic) reason(validRows int) string {
	switch {
	case validRows > 1:
		return "ambiguous_publication"
	case !d.HasPublication:
		return "channel_cost_not_published"
	case d.MissingCost:
		return "upstream_cost_missing"
	case d.ManifestUnverified:
		return "hour_manifest_unverified"
	default:
		return "cost_publication_unverified"
	}
}

func incrementFinanceInternalReason(counts map[string]int64, reason string) map[string]int64 {
	if counts == nil {
		counts = make(map[string]int64)
	}
	if reason == "" {
		reason = "unspecified" // Legacy events remain unresolved, never dropped.
	}
	counts[reason]++
	return counts
}
