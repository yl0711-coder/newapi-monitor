package monitor

import (
	"errors"
	"strings"
	"unicode/utf8"
)

func usageLogsHasDiagnosticFilter(f usageLogFilter) bool {
	return f.DiagnosticScope != "" || f.Message != ""
}

// JSON validity and scalar type guards are essential: historical other values
// include malformed JSON, nulls and non-string fields. A nested key, even null,
// wins over the legacy top-level key, matching the existing root projection.
func usageLogsRejectReasonSQL(mysql bool) string {
	stringAt := func(path string) string {
		if mysql {
			return "CASE WHEN JSON_TYPE(JSON_EXTRACT(other,'" + path + "'))='STRING' THEN JSON_UNQUOTE(JSON_EXTRACT(other,'" + path + "')) ELSE NULL END"
		}
		return "CASE WHEN JSON_TYPE(other,'" + path + "')='text' THEN JSON_EXTRACT(other,'" + path + "') ELSE NULL END"
	}
	nested := "$.admin_info.reject_reason"
	present := "JSON_TYPE(other,'" + nested + "') IS NOT NULL"
	if mysql {
		present = "JSON_CONTAINS_PATH(other,'one','" + nested + "')=1"
	}
	return "(CASE WHEN JSON_VALID(other) THEN CASE WHEN " + present + " THEN " + stringAt(nested) + " ELSE " + stringAt("$.reject_reason") + " END ELSE NULL END)"
}

func appendUsageLogDiagnosticWhere(w *usageLogWhere, f usageLogFilter, mysql bool) error {
	if !usageLogsHasDiagnosticFilter(f) {
		return nil
	}
	if f.DiagnosticScope != "" && f.DiagnosticScope != "issues" && f.DiagnosticScope != "rejected" {
		return errors.New("排障筛选无效")
	}
	if !utf8.ValidString(f.Message) || utf8.RuneCountInString(f.Message) > 128 || len(f.Message) > 512 {
		return errors.New("错误或拒绝关键词最多128个字符")
	}
	if f.Start <= 0 || f.End < f.Start || f.End-f.Start > 26*3600 {
		return errors.New("错误或拒绝信息搜索每次最多26小时，请缩小时间范围；可增加用户名、渠道或请求ID定位")
	}
	reason := usageLogsRejectReasonSQL(mysql)
	// Ignore ASCII-only blank reasons consistently with the list renderer.
	hasReason := "TRIM(REPLACE(REPLACE(REPLACE(COALESCE(" + reason + ",''),CHAR(9),''),CHAR(10),''),CHAR(13),'')) <> ''"
	switch f.DiagnosticScope {
	case "issues":
		w.add("(type = 5 OR " + hasReason + ")")
	case "rejected":
		w.add(hasReason)
	}
	if f.Message != "" {
		pattern := "%" + escapeLike(strings.ToLower(f.Message)) + "%"
		w.add("(LOWER(COALESCE(content,'')) LIKE ? ESCAPE '!' OR LOWER(COALESCE("+reason+",'')) LIKE ? ESCAPE '!')", pattern, pattern)
	}
	return nil
}
