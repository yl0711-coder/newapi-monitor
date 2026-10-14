package monitor

import (
	"encoding/json"
	"math/big"
	"strings"
)

// Customer support uses the local Request ID. Keep upstream correlation on the
// internal row only; do not publish it through the Usage API or customer CSV.
// Work on a value copy so shared source rows and internal callers are unchanged.
func portalCustomerLogRow(r LogRow) LogRow {
	r.UpstreamRequestID = ""
	r.RequestPath = "" // rc26 displays this inside the admin-only conversion section.
	if r.SourceFieldNotice != "" {
		r.SourceFieldNotice = "IP 字段暂未取得，不影响原始用量和费用。"
	}
	return r
}

// Replace the permissive legacy subscription display with strict presence
// evidence. In particular, pre + delta is NOT evidence of a final settlement.
func populatePortalSubscription(r *LogRow, e portalLogEvidence) {
	r.BillingSource, r.SubPlan, r.SubInstance, r.SubSettlement, r.SubRemain = "", "", "", "", ""
	if e.State != portalEvidenceValid {
		return
	}
	if source := e.Strings["billing_source"]; source.State == portalEvidenceValid && source.Text == "wallet" {
		r.BillingSource = "wallet"
		return
	}
	if e.Strings["billing_source"].Text != "subscription" {
		return
	}
	r.BillingSource = "subscription"
	var lines []string
	for _, field := range []struct{ key, label string }{
		{"subscription_pre_consumed", "预扣"}, {"subscription_post_delta", "结算差额"}, {"subscription_consumed", "最终抵扣"},
	} {
		v := e.Numbers[field.key]
		if v.State != portalEvidenceValid {
			continue
		}
		n, _ := new(big.Rat).SetString(v.Text)
		if field.key == "subscription_post_delta" && n.Sign() == 0 {
			continue // official rc26 omits a recorded zero delta
		}
		lines = append(lines, field.label+"："+n.Num().String()+" 额度")
	}
	r.SubSettlement = strings.Join(lines, "\n")
	integer := func(key string) string {
		v := e.Numbers[key]
		if v.State != portalEvidenceValid {
			return ""
		}
		n, _ := new(big.Rat).SetString(v.Text)
		return n.Num().String()
	}
	if plan := integer("subscription_plan_id"); plan != "" && plan != "0" {
		r.SubPlan = strings.TrimSpace("#" + plan + " " + e.Strings["subscription_plan_title"].Text)
	}
	if instance := integer("subscription_id"); instance != "" && instance != "0" {
		r.SubInstance = "#" + instance
	}
	if remain := integer("subscription_remain"); remain != "" {
		r.SubRemain = remain + " 额度"
		if total := integer("subscription_total"); total != "" {
			r.SubRemain = remain + "/" + total + " 额度"
		}
	}
}

func portalHasToolSurcharge(e portalLogEvidence) bool {
	positive := func(f portalEvidenceField) bool {
		if f.State != portalEvidenceValid {
			return false
		}
		n, ok := new(big.Rat).SetString(f.Text)
		return ok && n.Sign() > 0
	}
	for _, tool := range e.Tools {
		if tool.Name.State == portalEvidenceValid && strings.TrimSpace(tool.Name.Text) != "" && positive(tool.Count) && positive(tool.Price) {
			return true
		}
	}
	for _, prefix := range []string{"web_search", "file_search"} {
		if e.Flags[prefix].Text == "true" && positive(e.Numbers[prefix+"_call_count"]) && positive(e.Numbers[prefix+"_price"]) {
			return true
		}
	}
	return e.Flags["image_generation_call"].Text == "true" && positive(e.Numbers["image_generation_call_price"])
}

// Only owner-visible login / user operations, not channel/admin audit data.
// Unknown actions retain the existing scrubbed content rather than inventing a
// translation or returning the raw operation descriptor.
func populatePortalCustomerOperation(r *LogRow, raw string) {
	if r.Type != 3 && r.Type != 7 {
		return
	}
	if len(raw) > portalEvidenceMaxBytes {
		return
	}
	fields, ok := portalEvidenceObject(raw)
	if !ok {
		return
	}
	if r.Type == 7 {
		for _, field := range []struct{ key, label string }{{"login_method", "登录方式"}, {"user_agent", "User Agent"}} {
			v := portalEvidenceScalar(fields[field.key], "string")
			if v.State == portalEvidenceValid && v.Text != "" {
				r.RequestDetails = append(r.RequestDetails, portalLogDetailItem{Label: field.label, Value: portalCustomerErrorText(v.Text), State: portalEvidenceValid})
			}
		}
	}
	op, ok := portalEvidenceObject(string(fields["op"]))
	if !ok {
		return
	}
	var action string
	if json.Unmarshal(op["action"], &action) != nil {
		return
	}
	templates := map[string]string{
		"login":                 "登录成功（通过 {{method}}）",
		"user.quota_add":        "增加用户额度 {{quota}}",
		"user.quota_subtract":   "减少用户额度 {{quota}}",
		"user.quota_override":   "覆盖用户额度，从 {{from}} 改为 {{to}}",
		"user.passkey_register": "注册了一个 Passkey",
		"user.passkey_delete":   "解绑了一个 Passkey",
	}
	text := templates[action]
	if text == "" || (action == "login" && r.Type != 7) {
		return
	}
	params, _ := portalEvidenceObject(string(op["params"]))
	for _, key := range []string{"method", "quota", "from", "to"} {
		placeholder := "{{" + key + "}}"
		if !strings.Contains(text, placeholder) {
			continue
		}
		v := portalEvidenceScalar(params[key], "string")
		if v.State != portalEvidenceValid {
			v = portalEvidenceNumber(params[key], false, true)
		}
		if v.State != portalEvidenceValid {
			return // retain the original fallback if structured fields are incomplete
		}
		text = strings.ReplaceAll(text, placeholder, portalCustomerErrorText(v.Text))
	}
	r.Detail = text
}
