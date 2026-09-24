package monitor

// This file is the single SQL classification boundary shared by realtime
// metric sampling and stability backfill. Keep predicates here instead of
// copying business rules into each report query.

import (
	"sort"
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
	return "(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)"
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

// customerHealthErrorFromUpstreamSQL mirrors logChainFaultFromUpstream. NewAPI
// prefixes copied upstream responses with status_code=; messages produced by
// our own gateway do not have that prefix.
func customerHealthErrorFromUpstreamSQL() string {
	return "COALESCE(content,'') LIKE 'status_code=%'"
}

// customerHealthFaultMessagePreemptsStatusSQL lists the message rules that run
// before error_code/status attribution in logChainAttributeFault. Every one of
// these rules resolves to upstream/ours/unknown, never downstream, so a match
// must keep the error in customer-maintenance stability even when the same line
// also contains HTTP 400/413.
func customerHealthFaultMessagePreemptsStatusSQL() string {
	content := "LOWER(COALESCE(content,''))"
	fromUpstream := customerHealthErrorFromUpstreamSQL()
	noAvailable := "(" + content + " LIKE '%not supported by any configured account%' OR " +
		content + " LIKE '%no available channel for model%' OR " +
		content + " LIKE '%no available upstream in group%')"
	oursThreshold := "(NOT (" + fromUpstream + ") AND (" +
		content + " LIKE '%超过阈值%' OR " + content + " LIKE '%exceeds threshold%'))"
	allUnavailable := "(" + content + " LIKE '%all upstreams are temporarily unavailable%')"
	upstreamInternal := "((" + fromUpstream + ") AND (" +
		content + " LIKE '%数据库查询出错%' OR " +
		content + " LIKE '%database error%' OR " +
		content + " LIKE '%database query error%' OR " +
		content + " LIKE '%internal server error%'))"
	return "(" + strings.Join([]string{noAvailable, oursThreshold, allUnavailable, upstreamInternal}, " OR ") + ")"
}

func customerHealthSQLStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	return strings.Join(quoted, ",")
}

// customerHealthDownstreamErrorSQL returns the exact type=5 subset attributed
// to the downstream customer. Customer maintenance excludes only this subset;
// upstream, ours and unknown errors all continue to lower stability.
//
// The expression follows logChainAttributeFault's precedence:
// message rule -> known upstream error_code -> status code. A known error_code
// is used only for copied upstream responses, matching the Go source gate.
func customerHealthDownstreamErrorSQL() string {
	errorCode := channelTestJSONEnumSQL("$.error_code")
	allMapped := make([]string, 0, len(logChainFaultByErrorCode))
	downstream := make([]string, 0, len(logChainFaultByErrorCode))
	for code, rule := range logChainFaultByErrorCode {
		allMapped = append(allMapped, code)
		if rule.fault == faultDownstream {
			downstream = append(downstream, code)
		}
	}
	sort.Strings(allMapped)
	sort.Strings(downstream)
	fromUpstream := customerHealthErrorFromUpstreamSQL()
	knownApplied := "((" + fromUpstream + ") AND " + errorCode + " IN (" + customerHealthSQLStringList(allMapped) + "))"
	byErrorCode := "((" + fromUpstream + ") AND " + errorCode + " IN (" + customerHealthSQLStringList(downstream) + "))"
	byStatus := "((NOT " + knownApplied + ") AND COALESCE(content,'') REGEXP 'status_code=(400|413)')"
	return "(NOT " + customerHealthFaultMessagePreemptsStatusSQL() + " AND (" + byErrorCode + " OR " + byStatus + "))"
}

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
