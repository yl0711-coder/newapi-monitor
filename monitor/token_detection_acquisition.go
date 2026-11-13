package monitor

import "strings"

// Effective gaps are separate from the immutable raw-field ledger. A verified
// producer operand or exact entry link can satisfy a rule without pretending
// the corresponding field was recorded in SQL.
func tokenEffectiveMissing(f TokenMeteringFact, p tokenMeteringParsed) []tokenMeteringMissingField {
	out := []tokenMeteringMissingField{}
	for _, gap := range tokenMeteringDecodeMissing(f.MissingFields) {
		if tokenMeteringFieldUsable(tokenMeteringOperand(p, gap.Name)) {
			continue
		}
		if gap.Name == "input_tokens" && p.TotalInput != nil {
			continue
		}
		if gap.Name == "entry_event_id" && f.EntryEventID != "" || gap.Name == "request_method" && f.RequestMethod != "" || gap.Name == "request_path" && f.RequestPath != "" {
			continue
		}
		if strings.HasPrefix(gap.Name, "request_length") && f.LinkState == "unique" && f.RequestLength != nil && f.LengthState == "valid" {
			continue
		}
		out = append(out, gap)
	}
	return out
}

func tokenMissingAcquisition(f TokenMeteringFact, p tokenMeteringParsed) []tokenRuleEvidenceGap {
	plan := []tokenRuleEvidenceGap{}
	for _, gap := range tokenEffectiveMissing(f, p) {
		item := tokenRuleEvidenceGap{Name: gap.Name, Reason: gap.Reason, Rule: "request_provenance", Source: "newapi.logs.other." + gap.Name, Status: "source_not_recorded", Action: "源端按请求记录此字段及来源时间；部署后从新请求开始获取，不能回填当前配置"}
		switch {
		case strings.HasPrefix(gap.Name, "request_length") || gap.Name == "entry_event_id" || gap.Name == "request_method":
			item.Rule, item.Source, item.Status, item.Action = "input_inflation,cache_structure", "nginx access request_id/event_id", "association_"+f.LinkState, "后台近期入口采集与精确关联重试；证据到达后自动递增关联版本并重检"
			if f.LinkState == "unique" {
				item.Status, item.Action = "source_not_recorded", "同请求入口证据已唯一关联，但该字段未记录或不可用；入口日志需补记字段、语义版本和时间，部署后仅新请求可取"
			}
		case gap.Name == "input_tokens" || strings.Contains(gap.Name, "cache"):
			item.Rule = "usage_format,input_inflation,cache_structure,historical_price_difference"
		case strings.HasPrefix(gap.Name, "pricing"):
			if p.Contract != nil {
				item.Status = "raw_source_missing_contract_available"
				item.Action = "原字段未记录；已用有版本和适用时间的来源契约及本次价格快照复算，参见规则操作数"
			}
		}
		plan = append(plan, item)
	}
	return plan
}
