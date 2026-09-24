package monitor

import (
	"regexp"
	"strings"
)

var (
	cwSlowStats = regexp.MustCompile(`(?i)#?\s*Query_time:\s*(\S+)\s+Lock_time:\s*(\S+)\s+Rows_sent:\s*(\S+)\s+Rows_examined:\s*(\S+)`)
	cwSQLStart  = regexp.MustCompile(`(?im)^\s*(SELECT|INSERT|UPDATE|DELETE|REPLACE|CALL|ALTER|CREATE|DROP|TRUNCATE|WITH)\b`)
)

type cwRDSErrorRule struct {
	category, fault, summary string
	any                      []string
}

var cwRDSErrorRules = []cwRDSErrorRule{
	{category: "connection_capacity", fault: "rate_limit_capacity", summary: "RDS 连接容量不足", any: []string{"too many connections", "max_connections"}},
	{category: "deadlock", fault: "unknown", summary: "RDS 记录到事务死锁", any: []string{"deadlock"}},
	{category: "transport_timeout", fault: "transport_timeout", summary: "RDS 记录到连接或执行超时", any: []string{"timeout", "timed out"}},
	{category: "connection_abort", fault: "transport_connect", summary: "RDS 记录到连接异常中止", any: []string{"aborted connection", "lost connection"}},
	{category: "authentication", fault: "auth_quota_account", summary: "RDS 拒绝了数据库认证", any: []string{"access denied for user", "authentication failed"}},
	{category: "innodb", fault: "unknown", summary: "RDS 记录到 InnoDB 异常", any: []string{"innodb"}},
	{category: "lifecycle", fault: "unknown", summary: "RDS 记录到实例生命周期事件", any: []string{"crash", "shutdown", "restarting", "ready for connections"}},
	{category: "server_error", fault: "unknown", summary: "RDS 记录到数据库错误", any: []string{"[error]", " error ", "error:"}},
}

func (p *cloudWatchEvidenceParser) parseRDSError(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	message := strings.TrimSpace(in.Message)
	if in.Fields != nil {
		message = cwField(in.Fields, "@message", "message")
	}
	if message == "" {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	lower := strings.ToLower(message)
	var rule *cwRDSErrorRule
	for index := range cwRDSErrorRules {
		for _, fragment := range cwRDSErrorRules[index].any {
			if strings.Contains(lower, fragment) {
				rule = &cwRDSErrorRules[index]
				break
			}
		}
		if rule != nil {
			break
		}
	}
	if rule == nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
	eventMS := cwRDSEventMS(in)
	if eventMS <= 0 {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out := p.base(in)
	out.Kind = cwEvidenceRDSError
	out.EventMS = eventMS
	out.Category, out.FaultClass, out.Summary = rule.category, rule.fault, rule.summary
	out.Severity = cwRDSSeverity(lower)
	return out, nil
}

func (p *cloudWatchEvidenceParser) parseRDSSlowQuery(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	message := strings.TrimSpace(in.Message)
	if in.Fields != nil {
		message = cwField(in.Fields, "@message", "message")
	}
	match := cwSlowStats.FindStringSubmatch(message)
	if len(match) != 5 {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
	querySeconds, ok, err := cwParseFloat(match[1], 0, 86400)
	if err != nil || !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	lockSeconds, ok, err := cwParseFloat(match[2], 0, 86400)
	if err != nil || !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	rowsSent, ok, err := cwParseUint64(match[3], 1<<62)
	if err != nil || !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	rowsExamined, ok, err := cwParseUint64(match[4], 1<<62)
	if err != nil || !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	eventMS := cwRDSEventMS(in)
	if eventMS <= 0 {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out := p.base(in)
	out.Kind = cwEvidenceRDSSlowQuery
	out.EventMS = eventMS
	out.Category, out.FaultClass, out.Summary = "slow_query", "unknown", "RDS 记录到慢查询"
	out.QueryTimeMS = cwInt64Pointer(int64(querySeconds*1000 + 0.5))
	out.LockTimeMS = cwInt64Pointer(int64(lockSeconds*1000 + 0.5))
	out.RowsSent, out.RowsExamined = cwUint64Pointer(rowsSent), cwUint64Pointer(rowsExamined)
	if sql := cwSQLStart.FindStringSubmatch(message); len(sql) == 2 {
		out.DBOperation = strings.ToUpper(sql[1])
		out.QueryHMAC = p.hmac("rds-slow-query", cwSQLText(message))
	}
	return out, nil
}

func cwRDSEventMS(in cloudWatchEvidenceInput) int64 {
	if in.TimestampMS > 0 {
		return in.TimestampMS
	}
	if in.Fields != nil {
		if at, ok := parseCloudWatchResultTime(cwField(in.Fields, "@timestamp")); ok {
			return at.UnixMilli()
		}
	}
	return 0
}

func cwRDSSeverity(lower string) string {
	switch {
	case strings.Contains(lower, "[error]"), strings.Contains(lower, "error:"):
		return "error"
	case strings.Contains(lower, "[warning]"), strings.Contains(lower, "warning:"):
		return "warning"
	default:
		return "unknown"
	}
}

func cwSQLText(message string) string {
	lines := strings.Split(message, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		value := strings.TrimSpace(line)
		lower := strings.ToLower(value)
		if value == "" || strings.HasPrefix(value, "#") || strings.HasPrefix(lower, "set timestamp=") || strings.HasPrefix(lower, "use ") {
			continue
		}
		kept = append(kept, value)
	}
	return strings.Join(kept, " ")
}
