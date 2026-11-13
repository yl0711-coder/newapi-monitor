package monitor

// This file is the single SQL classification boundary shared by realtime
// metric sampling and stability backfill. Keep predicates here instead of
// copying business rules into each report query.

import (
	"strconv"
	"strings"
)

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
	return "(completion_tokens = 0 AND " + logChainTextCompletionPathSQL() + " AND " +
		logChainNoOutputModelSQL() + " AND NOT " + logChainBenignClientGoneSQL() + ")"
}

func streamDeliveryFailureSQL() string {
	return logChainStreamAnomalySQL()
}

// customerHealthFirstByteDelaySQL reads NewAPI's observed first-data delay.
// It is used only for the customer-maintenance stability responsibility count;
// it is not used as proof that a valid model token reached the customer.
func customerHealthFirstByteDelaySQL() string {
	return "(CASE WHEN COALESCE(is_stream,0)=1 AND JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)"
}

func deliveryAnomalySQL() string {
	return "(" + deliveryZeroOutputSQL() + " OR " + streamDeliveryFailureSQL() + ")"
}

// customerHealthAttributedAnomalySQL is the type=2 subset that lowers the
// customer-maintenance stability score. Real stream failures point upstream;
// a pure client_gone only enters after the user-defined three-second grace
// window, when it has produced no content and is treated as an upstream wait.
func customerHealthAttributedAnomalySQL() string {
	slowZeroOutput := "(" + deliveryZeroOutputSQL() + " AND " +
		customerHealthFirstByteDelaySQL() + " > " + strconv.FormatInt(customerHealthSlowFirstByteMS, 10) + ")"
	return "(" + streamDeliveryFailureSQL() + " OR " + logChainActionableClientGoneSQL() + " OR " + slowZeroOutput + ")"
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

// Customer-maintenance type=5 attribution uses customer_health_classification.go.
// Do not mirror the diagnostic catalogue in SQL or alter the shared metric
// failure predicates above. SQL emits raw totals; only a complete diagnostic
// pass can publish the independent customer-health classification version.

func expandAnomalyPredicates(query string) string {
	replacements := map[string]string{
		"{{ANOM}}":       deliveryAnomalySQL(),
		"{{STREAMBAD}}":  streamDeliveryFailureSQL(),
		"{{HEALTHANOM}}": customerHealthAttributedAnomalySQL(),
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
