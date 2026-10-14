package monitor

import (
	"encoding/json"
	"math/big"
	"regexp"
	"strings"
)

type portalRequestEvidence struct {
	FRT, PromptOverwritten, Status, Reason, ErrorCount portalEvidenceField
	StreamState                                        portalEvidenceState
	EndError                                           string
	Errors                                             []string
}

type portalDurationEvidence struct {
	State portalEvidenceState `json:"state"`
	Value string              `json:"value,omitempty"`
}

func portalFirstByteEvidence(r LogRow, e portalLogEvidence) *portalDurationEvidence {
	if (r.Type != 2 && r.Type != 5) || !r.IsStream || r.streamFlagUnknown {
		return nil
	}
	d := &portalDurationEvidence{State: e.State}
	if e.State == portalEvidenceValid {
		d.State = e.Request.FRT.State
		if d.State == portalEvidenceValid {
			d.Value = e.Request.FRT.Text
		}
	}
	return d
}

func parsePortalRequestEvidence(fields map[string]json.RawMessage) portalRequestEvidence {
	r := portalRequestEvidence{FRT: portalEvidenceNumber(fields["frt"], false, false), PromptOverwritten: portalEvidenceScalar(fields["is_system_prompt_overwritten"], "bool"), StreamState: portalEvidenceMissing}
	raw, exists := fields["stream_status"]
	if !exists {
		return r
	}
	if strings.TrimSpace(string(raw)) == "null" {
		r.StreamState = portalEvidenceNull
		return r
	}
	stream, valid := portalEvidenceObject(string(raw))
	if !valid {
		r.StreamState = portalEvidenceInvalid
		return r
	}
	r.StreamState = portalEvidenceValid
	r.Status = portalEvidenceScalar(stream["status"], "string")
	r.Reason = portalEvidenceScalar(stream["end_reason"], "string")
	r.ErrorCount = portalEvidenceNumber(stream["error_count"], true, false)
	if v := portalEvidenceScalar(stream["end_error"], "string"); v.State == portalEvidenceValid {
		r.EndError = portalCustomerErrorText(v.Text)
	}
	var errors []json.RawMessage
	if json.Unmarshal(stream["errors"], &errors) == nil {
		for i, raw := range errors {
			if i >= 8 {
				r.Errors = append(r.Errors, "其余错误条目已省略")
				break
			}
			var text string
			if json.Unmarshal(raw, &text) == nil && text != "" {
				r.Errors = append(r.Errors, portalCustomerErrorText(text))
			}
		}
	}
	return r
}

var portalErrorURL = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)
var portalErrorCredential = regexp.MustCompile(`(?i)("?(?:authorization|api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|secret)"?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|(?:bearer\s+)?[^\s,;]+)`)

// Customer-visible upstream errors are not raw response payloads. Preserve a
// useful plain-text error, but suppress embedded bodies and redact credentials
// and URLs before it can enter the DTO, CSV or browser. No error text is used
// to classify success/failure or to change a charge.
func portalCustomerErrorText(text string) string {
	if len(text) > 16<<10 {
		return "错误文本过长，未展示原文"
	}
	lower := strings.ToLower(text)
	for _, key := range []string{`"instructions"`, `"input"`, `"messages"`, `"prompt"`, "prompt:", "prompt=", "instructions:", "messages:", "request_body", "request body:", "response body:"} {
		if strings.Contains(lower, key) {
			return "错误信息含请求或响应内容，已隐藏"
		}
	}
	text = portalErrorCredential.ReplaceAllString(text, "$1[已隐藏]")
	text = problemBearerPattern.ReplaceAllString(text, "Bearer [已隐藏]")
	text = problemKeyPattern.ReplaceAllString(text, "[凭据已隐藏]")
	text = portalErrorURL.ReplaceAllString(text, "[地址已隐藏]")
	runes := []rune(strings.TrimSpace(text))
	if len(runes) > 1024 {
		return string(runes[:1024]) + "…（已截断）"
	}
	return string(runes)
}

// Return only owner-visible fields with bounded/redacted error text. A missing
// status is not proof of a successful stream, including historical records.
func portalRequestDetails(r LogRow, e portalLogEvidence) []portalLogDetailItem {
	if r.Type != 2 && r.Type != 5 {
		return nil
	}
	var out []portalLogDetailItem
	add := func(label, value string, state portalEvidenceState) {
		out = append(out, portalLogDetailItem{Label: label, Value: value, State: state})
	}
	if r.streamFlagUnknown {
		add("请求方式", "未记录或标记异常，暂不判断流式结束状态", portalEvidenceMissing)
		return out
	}
	if r.IsStream {
		add("请求方式", "流式", portalEvidenceValid)
	} else {
		add("请求方式", "非流式", portalEvidenceValid)
	}
	if e.State != portalEvidenceValid {
		if r.IsStream {
			add("流式状态", "未记录或无法解析，不能判定是否完整结束", e.State)
		}
		return out
	}
	if r.IsStream {
		frt := e.Request.FRT
		if frt.State == portalEvidenceValid {
			add("首字延迟（毫秒，日志原值）", frt.Text, frt.State)
		} else if frt.State == portalEvidenceMissing || frt.State == portalEvidenceNull {
			add("首字延迟", "未记录", frt.State)
		} else {
			add("首字延迟", "记录异常", frt.State)
		}
	}
	if flag := e.Request.PromptOverwritten; flag.State == portalEvidenceValid && flag.Text == "true" {
		add("系统提示词", "已替换（不展示内容）", portalEvidenceValid)
	}
	if !r.IsStream {
		if e.Request.StreamState == portalEvidenceValid {
			add("流式状态", "记录与非流式请求标记不一致，待核验", portalEvidenceInvalid)
		} else if e.Request.StreamState == portalEvidenceInvalid {
			add("流式状态", "记录格式异常，待核验", portalEvidenceInvalid)
		}
		return out
	}
	if e.Request.StreamState == portalEvidenceMissing || e.Request.StreamState == portalEvidenceNull {
		add("流式状态", "未记录，不能判定是否完整结束", e.Request.StreamState)
		return out
	}
	if e.Request.StreamState != portalEvidenceValid {
		add("流式状态", "记录异常，不能判定是否完整结束", portalEvidenceInvalid)
		return out
	}
	status, reason, count := e.Request.Status, e.Request.Reason, e.Request.ErrorCount
	// Match rc26: non-ok stream records show original fields, not an invented
	// diagnosis based on the reason or timing. Unknown strings remain strings.
	if status.State == portalEvidenceValid {
		add("流式状态", portalCustomerErrorText(status.Text), status.State)
	}
	if status.Text != "ok" {
		if reason.State == portalEvidenceValid {
			add("流结束原因", portalCustomerErrorText(reason.Text), reason.State)
		}
		if count.State == portalEvidenceValid {
			n, _ := new(big.Rat).SetString(count.Text)
			if n.Sign() > 0 {
				add("流内错误次数", count.Text, count.State)
			}
		}
		if e.Request.EndError != "" {
			add("流结束错误", e.Request.EndError, portalEvidenceValid)
		}
		for _, text := range e.Request.Errors {
			add("流内错误", text, portalEvidenceValid)
		}
	}
	return out
}
