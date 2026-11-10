package monitor

import (
	"regexp"
	"strings"
)

// Identity namespaces are deliberately separate. An unlabelled ID in a copied
// upstream response proves an upstream origin, not which provider issued it.
type LogChainDiagnosticRequestID struct {
	Value      string `json:"value"`
	Owner      string `json:"owner"`
	Source     string `json:"source"`
	ObservedAt int64  `json:"observed_at"`
	AttemptID  string `json:"attempt_id,omitempty"`
	Status     string `json:"status"`
	Reason     string `json:"reason"`
	Explicit   bool   `json:"explicit_scope,omitempty"`
}

type logChainDiagnosticContext struct {
	owner   string
	attempt string
}

var logChainDiagnosticParentheticalID = regexp.MustCompile(`(?i)\((?:(platform|newapi|oneapi|upstream|relay|provider|model[ _]provider)[ _]+)?request[ _]id\s*:\s*([^\s()]+)\s*\)`)
var logChainDiagnosticIDValue = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)

func logChainDiagnosticIDOwner(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "platform", "newapi", "oneapi":
		return "platform"
	case "relay", "upstream_relay":
		return "upstream_relay"
	case "provider", "model_provider", "model provider", "model-provider":
		return "model_provider"
	case "upstream", "upstream_response":
		return "upstream"
	default:
		return "unknown"
	}
}

func logChainDiagnosticStructuredIDOwner(key, path, source, inherited string) (string, bool) {
	switch key {
	case "request_id", "requestid":
		if path == "other" || (path == "content" && source == "cloudwatch:newapi") {
			return "platform", true
		}
		return inherited, true
	case "oneapi_request_id", "platform_request_id", "newapi_request_id":
		return "platform", true
	case "upstream_request_id":
		return "upstream", true
	case "relay_request_id", "upstream_relay_request_id":
		return "upstream_relay", true
	case "provider_request_id", "model_provider_request_id":
		return "model_provider", true
	}
	return "", false
}

func (f *LogChainDiagnosticFacts) addRequestID(value, owner, source string, at int64, attempt string, explicit bool) {
	value = logChainDiagnosticSafeText(strings.TrimSpace(value), 256)
	if !logChainDiagnosticIDValue.MatchString(value) {
		return
	}
	if owner == "" {
		owner = "unknown"
	}
	for _, id := range f.RequestIDs {
		if id.Value == value && id.Owner == owner && id.Source == source && id.AttemptID == attempt {
			return
		}
	}
	if len(f.RequestIDs) >= 32 {
		f.RequestIDsTruncated = true
		return
	}
	f.RequestIDs = append(f.RequestIDs, LogChainDiagnosticRequestID{Value: value, Owner: owner, Source: source, ObservedAt: at, AttemptID: attempt, Explicit: explicit})
}

func (f *LogChainDiagnosticFacts) parseParentheticalIDs(text, source string, at int64, ctx logChainDiagnosticContext) {
	// Text already has a 16 KiB bound. Inspect all mentions within that bound
	// so duplicates cannot conceal a later conflicting ID beyond an early cap.
	for _, match := range logChainDiagnosticParentheticalID.FindAllStringSubmatch(text, -1) {
		owner := ctx.owner
		if match[1] != "" {
			owner = logChainDiagnosticIDOwner(match[1])
		}
		f.addRequestID(match[2], owner, source, at, ctx.attempt, match[1] != "")
	}
}

var logChainDiagnosticIDFields = map[string]string{
	"request_id": "platform", "upstream_request_id": "upstream",
	"upstream_relay_request_id": "upstream_relay", "model_provider_request_id": "model_provider",
}

// Resolve only within this SQL row / request attempt. Retain every candidate
// and its provenance; never use iteration order to choose among conflicting IDs.
func (f *LogChainDiagnosticFacts) resolveRequestIDs(platformID string) {
	for field, owner := range logChainDiagnosticIDFields {
		v := f.Fields[field]
		if v.Scope != "request" || v.Value == "" {
			continue
		}
		found := false
		for _, id := range f.RequestIDs {
			found = found || (id.Value == v.Value && (id.Owner == owner || field == "upstream_request_id"))
		}
		if !found {
			f.addRequestID(v.Value, owner, v.Source, v.ObservedAt, f.value("attempt_id"), true)
		}
		delete(f.Fields, field)
	}
	attempt := f.value("attempt_id")
	attempts := map[string]bool{}
	for _, id := range f.RequestIDs {
		if id.AttemptID != "" {
			attempts[id.AttemptID] = true
		}
	}
	for _, e := range f.Errors {
		if e.AttemptID != "" {
			attempts[e.AttemptID] = true
		}
	}
	values := map[string]map[string]bool{}
	for i := range f.RequestIDs {
		id := &f.RequestIDs[i]
		id.Status, id.Reason = "bound", "本次错误记录中的请求标识，保留原始来源"
		if platformID != "" && id.Value == platformID {
			if id.Explicit && id.Owner != "unknown" && id.Owner != "platform" {
				id.Status, id.Reason = "conflict", "显式上游标识与本平台 Request ID 重合，需确认命名空间"
				continue
			}
			id.Owner, id.Reason = "platform", "与本平台日志 Request ID 一致，不能作为上游 ID"
		}
		if id.Owner == "unknown" {
			id.Status, id.Reason = "unattributed", "原文已有 ID，但未记录归属层；需补记平台/上游中转/模型商及尝试标识"
			continue
		}
		if attempt != "" && id.AttemptID != "" && attempt != id.AttemptID {
			id.Status, id.Reason = "retry_conflict", "该 ID 属于其他请求尝试，不参与本次补证"
			continue
		}
		if len(attempts) > 1 && (attempt == "" || id.AttemptID == "") {
			id.Status, id.Reason = "retry_conflict", "原文包含多个请求尝试，当前 ID 未绑定唯一尝试"
			continue
		}
		if f.RequestIDsTruncated {
			id.Status, id.Reason = "ambiguous", "ID 所在内容或候选数量超出有界解析范围，无法证明唯一"
			continue
		}
		if values[id.Owner] == nil {
			values[id.Owner] = map[string]bool{}
		}
		values[id.Owner][id.Value] = true
	}
	upstream := map[string]LogChainDiagnosticRequestID{}
	for i := range f.RequestIDs {
		id := &f.RequestIDs[i]
		if id.Status != "bound" {
			continue
		}
		if len(values[id.Owner]) > 1 {
			id.Status, id.Reason = "ambiguous", "同一归属层存在多个不同 ID，不能任取一个关联证据"
			continue
		}
		if id.Owner == "upstream" {
			id.Reason = "ID 来自本次上游响应；中转或模型商未进一步标明，不能冒称具体提供方"
		}
		for field, owner := range logChainDiagnosticIDFields {
			if owner == id.Owner && field != "upstream_request_id" {
				f.put(field, id.Value, id.Source, id.ObservedAt, "request")
			}
		}
		if id.Owner != "platform" {
			upstream[id.Value] = *id
		}
	}
	// A single known upstream ID can satisfy the generic missing-field check.
	// Multiple explicitly named namespaces remain separate, without a first ID.
	if len(upstream) == 1 {
		for _, id := range upstream {
			if !f.identityNamespaceCollision(id.Value) {
				f.put("upstream_request_id", id.Value, id.Source, id.ObservedAt, "request")
			}
		}
	}
	if platformID != "" {
		f.put("request_id", platformID, "logs.request_id", 0, "request")
	}
}

func (f *LogChainDiagnosticFacts) identityNamespaceCollision(value string) bool {
	owners := map[string]bool{}
	for _, id := range f.RequestIDs {
		if id.Value == value && id.Status == "bound" && (id.Owner == "model_provider" || id.Owner == "upstream_relay") {
			owners[id.Owner] = true
		}
	}
	return len(owners) > 1
}

func (f *LogChainDiagnosticFacts) hasUpstreamIdentity() bool {
	for _, id := range f.RequestIDs {
		if id.Status == "bound" && id.Owner != "platform" && id.Owner != "unknown" {
			return true
		}
	}
	return false
}

func (f *LogChainDiagnosticFacts) identityConflict() bool {
	if f.RequestIDsTruncated {
		return true
	}
	for _, id := range f.RequestIDs {
		if id.Status == "conflict" || id.Status == "ambiguous" || id.Status == "retry_conflict" {
			return true
		}
	}
	return false
}

func logChainCloneDiagnosticFacts(f *LogChainDiagnosticFacts) *LogChainDiagnosticFacts {
	out := &LogChainDiagnosticFacts{Fields: map[string]LogChainDiagnosticField{}}
	if f == nil {
		return out
	}
	out.Errors = append([]LogChainDiagnosticError(nil), f.Errors...)
	out.RequestIDs = append([]LogChainDiagnosticRequestID(nil), f.RequestIDs...)
	out.Truncated, out.RequestIDsTruncated = f.Truncated, f.RequestIDsTruncated
	for k, v := range f.Fields {
		out.Fields[k] = v
	}
	return out
}

func (f *LogChainDiagnosticFacts) mergeRequestIDs(extra *LogChainDiagnosticFacts) {
	for _, id := range extra.RequestIDs {
		f.addRequestID(id.Value, id.Owner, id.Source, id.ObservedAt, id.AttemptID, id.Explicit)
	}
	f.RequestIDsTruncated = f.RequestIDsTruncated || extra.RequestIDsTruncated
}

func (f *LogChainDiagnosticFacts) identityMissingReason() string {
	if len(f.RequestIDs) > 0 {
		return "原文已有 Request ID；上游归属、唯一性或请求尝试尚未确认，详见逐项 ID 状态；不能任取一个补证"
	}
	return "当前请求日志及已关联证据未记录；不能用当前配置补作历史事实"
}

func logChainDiagnosticIdentityRelation(a, b *LogChainDiagnosticFacts) (matched, conflict bool) {
	if a.identityConflict() || b.identityConflict() {
		return false, true
	}
	for _, left := range a.RequestIDs {
		if left.Status != "bound" || left.Owner == "platform" {
			continue
		}
		for _, right := range b.RequestIDs {
			if right.Status != "bound" || right.Owner == "platform" {
				continue
			}
			if left.Owner != right.Owner && left.Owner != "upstream" && right.Owner != "upstream" {
				continue // Same bytes in different known namespaces do not match.
			}
			if left.Value == right.Value {
				if left.Owner == right.Owner || (!a.identityNamespaceCollision(left.Value) && !b.identityNamespaceCollision(right.Value)) {
					matched = true
				}
			} else if left.Owner == right.Owner {
				conflict = true
			}
		}
	}
	return
}
