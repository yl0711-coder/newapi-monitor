package monitor

// This file is the single SQL classification boundary shared by realtime
// metric sampling and stability backfill. Keep predicates here instead of
// copying business rules into each report query.

import "strings"

const (
	// REPLACE(CAST(JSON_EXTRACT ... AS CHAR),'"','') is portable across the
	// production MySQL source and the SQLite acceptance source.
	anomalyEndReasonSQL = "COALESCE(CASE WHEN JSON_VALID(other) THEN REPLACE(CAST(JSON_EXTRACT(other,'$.stream_status.end_reason') AS CHAR),'\"','') END,'')"
	anomalyErrCountSQL  = "COALESCE(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.stream_status.error_count') AS SIGNED) END,0)"
)

// deliveryZeroOutputSQL classifies zero textual delivery. The request path is
// authoritative; model-name keywords are only a secondary exclusion for
// no-output models routed through a text-compatible endpoint.
func deliveryZeroOutputSQL() string {
	return "(completion_tokens = 0 AND " + logChainTextCompletionPathSQL() + " AND " + logChainNoOutputModelSQL() + ")"
}

func streamDeliveryFailureSQL() string {
	return logChainStreamAnomalySQL()
}

func deliveryAnomalySQL() string {
	return "(" + deliveryZeroOutputSQL() + " OR " + streamDeliveryFailureSQL() + ")"
}

func sourceFailureTimeoutSQL() string {
	return "(content LIKE '%timeout%' OR content LIKE '%deadline%')"
}

// Failure classes are deliberately mutually exclusive. A nested upstream
// error may contain both 4xx and 5xx text; timeout wins, then 5xx, then 4xx.
func sourceFailure5xxSQL() string {
	return "(NOT " + sourceFailureTimeoutSQL() + " AND content REGEXP 'status_code=5')"
}

func sourceFailure4xxSQL() string {
	return "(NOT " + sourceFailureTimeoutSQL() + " AND NOT (content REGEXP 'status_code=5') AND content REGEXP 'status_code=4')"
}

func expandAnomalyPredicates(query string) string {
	replacements := map[string]string{
		"{{ANOM}}":       deliveryAnomalySQL(),
		"{{STREAMBAD}}":  streamDeliveryFailureSQL(),
		"{{ZERO}}":       deliveryZeroOutputSQL(),
		"{{ERR4XX}}":     sourceFailure4xxSQL(),
		"{{ERR5XX}}":     sourceFailure5xxSQL(),
		"{{ERRTIMEOUT}}": sourceFailureTimeoutSQL(),
	}
	for marker, predicate := range replacements {
		query = strings.ReplaceAll(query, marker, predicate)
	}
	return query
}
