package monitor

const (
	ecsLogScopeIsolated   = "isolated"
	ecsLogScopeProduction = "production"
)

// ecsLogRuntimeEnabled is the single runtime gate used by registration,
// ingestion, discovery and archive recovery. Production deliberately needs a
// second opt-in so a copied isolated configuration cannot become live merely
// by changing one string.
func ecsLogRuntimeEnabled(s Settings) bool {
	if !s.ECSLogEnabled {
		return false
	}
	return s.ECSLogScope == ecsLogScopeIsolated ||
		s.ECSLogScope == ecsLogScopeProduction && s.ECSLogProductionEnabled
}

func ecsLogPurpose(s Settings) string {
	if s.ECSLogScope == ecsLogScopeProduction {
		return "production-v1"
	}
	return "isolated-candidate"
}
