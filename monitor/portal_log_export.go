package monitor

import (
	"math/big"
	"strconv"
	"strings"
)

const (
	portalCSVCacheReadColumn = 12 + iota
	portalCSVCacheWriteColumn
	portalCSVCacheStatusColumn
	portalCSVModeColumn
	portalCSVFRTColumn
	portalCSVAmountKindColumn
	portalCSVSourceColumn
	portalCSVBillingColumn
	portalCSVSupplementColumn
	portalCSVColumnCount
)

func portalCSVReadableBilling(r LogRow) string {
	if r.Type != 2 {
		return ""
	}
	if d := r.UsageDetails; d != nil && !d.LegacyFormulaUnsafe && d.Billing != nil && d.Billing.State == "matched" && d.Billing.Readable != "" {
		return d.Billing.Readable
	}
	// The customer export is not an internal diagnostic dump. Keep available
	// customer-facing unit prices and limitations, never the technical formula.
	lines := []string{}
	if r.LogContent != "" {
		lines = append(lines, r.LogContent)
	}
	if d := r.UsageDetails; d != nil {
		if d.Billing != nil && d.Billing.Message != "" {
			lines = append(lines, d.Billing.Message)
		}
		if d.Notice != "" && (d.Billing == nil || d.Notice != d.Billing.Message) {
			lines = append(lines, d.Notice)
		}
	}
	return strings.Join(lines, "\n")
}

func portalCSVHeader() []string {
	// Preserve the original twelve column names/order. New customer-facing
	// fields are structured; no separate, overlapping technical text columns.
	return []string{"时间", "成员", "令牌", "分组", "类型", "模型", "用时(秒)", "输入tokens", "输出tokens", "费用(美元)", "详情", "Request ID",
		"缓存读取tokens", "缓存创建tokens（总量或分项小计）", "缓存创建数据状态", "请求方式", "首字延迟(毫秒)", "金额性质", "计费来源", "费用计算说明", "补充详情"}
}

// Append existing customer-visible expanded fields, not raw other/admin data.
// Recorded amounts retain their log type: a refund is not a consumption cost.
func portalCSVRequestDetails(r LogRow) string {
	var lines []string
	add := func(label, value string) {
		if value != "" {
			lines = append(lines, label+"："+value)
		}
	}
	add("字段说明", r.SourceFieldNotice)
	if r.HasToolSurcharge {
		add("费用标记", "原始费用已含工具调用附加费，不重复加算")
	}
	for _, item := range r.RequestDetails {
		if item.Label == "请求方式" || strings.HasPrefix(item.Label, "首字延迟") || item.State == portalEvidenceMissing || item.State == portalEvidenceNull {
			continue // separate columns, or genuinely absent metadata
		}
		add(item.Label, item.Value)
	}
	if r.Type == 5 {
		add("费用说明", "此行为错误记录，不能据此判断是否另有消费扣费；可结合相同 Request ID 的消费记录核对。")
	}
	if r.Type == 2 {
		add("推理强度", r.ReasoningEffort)
		if r.IsModelMapped {
			add("实际模型", r.UpstreamModelName)
		}
		if d := r.UsageDetails; d != nil {
			if d.CacheRead.State == portalEvidenceInvalid {
				add("缓存读取", "记录异常")
			}
			for _, item := range d.Items {
				if item.Label == "缓存读取 Tokens（日志原值）" || item.Label == "缓存创建总量（日志原值）" {
					continue // exact totals already have their own numeric columns
				}
				if !d.LegacyFormulaUnsafe && d.Billing != nil && d.Billing.State == "matched" && d.Billing.Readable != "" && (strings.Contains(item.Label, "单价") || strings.Contains(item.Label, "倍率") || item.Label == "计费方式") {
					continue // verified short explanation already shows these prices/rates
				}
				add(item.Label, item.Value)
			}
		}
	}
	if r.Type == 6 {
		add("任务 ID", r.TaskID)
		add("失败原因", r.RefundReason)
	}
	add("参数覆盖", strings.Join(r.ParamOverride, "\n"))
	add("订阅套餐", r.SubPlan)
	add("订阅实例", r.SubInstance)
	add("订阅结算", r.SubSettlement)
	add("订阅剩余", r.SubRemain)
	if r.BillingSource == "subscription" {
		add("订阅说明", "token 按倍率换算成额度/次数，请求结束后差额结算（补扣/返还）；不等同于钱包现金扣款。")
	}
	return strings.Join(lines, "\n")
}

func portalCacheCSVValue(c portalCacheDisplay) string {
	switch c.State {
	case portalEvidenceValid:
		return c.Value
	case portalEvidenceInvalid:
		return "" // numeric columns must not mix counts with explanatory text
	default:
		return ""
	}
}

func portalCacheCSVStatus(c portalCacheDisplay) string {
	if c.State != portalEvidenceValid {
		if c.State == portalEvidenceInvalid {
			return "记录异常"
		}
		return ""
	}
	if c.Source == "split_only" {
		return "仅分项小计（总量未记录）"
	}
	if c.Source == "recorded" {
		return "已记录总量"
	}
	return "口径未知"
}

func portalCSVLogEvidence(r LogRow) []string {
	fields := make([]string, portalCSVColumnCount-12)
	if d := r.UsageDetails; r.Type == 2 && d != nil {
		fields[0], fields[1], fields[2] = portalCacheCSVValue(d.CacheRead), portalCacheCSVValue(d.CacheWrite), portalCacheCSVStatus(d.CacheWrite)
	}
	if r.Type == 2 || r.Type == 5 {
		if !r.streamFlagUnknown {
			fields[portalCSVModeColumn-12] = "非流式"
			if r.IsStream {
				fields[portalCSVModeColumn-12] = "流式"
			}
		}
		if r.IsStream && !r.streamFlagUnknown {
			if v := r.FirstByteEvidence; v != nil && v.State == portalEvidenceValid {
				if n, ok := new(big.Rat).SetString(v.Value); ok && n.Sign() > 0 {
					fields[portalCSVFRTColumn-12] = v.Value
				}
			} else if v == nil && r.FirstByteMs > 0 {
				fields[portalCSVFRTColumn-12] = strconv.FormatInt(r.FirstByteMs, 10)
			}
		}
	}
	if portalLogHasRecordedAmount(r.Type) {
		kind := map[int]string{0: "未知类型金额", 2: "消费", 5: "错误记录", 6: "退款"}[r.Type]
		if r.BillingSource == "subscription" {
			fields[portalCSVSourceColumn-12] = "订阅"
			if r.Type == 2 {
				kind = "订阅抵扣"
			}
		} else if r.BillingSource == "wallet" {
			fields[portalCSVSourceColumn-12] = "钱包"
		}
		fields[portalCSVAmountKindColumn-12] = kind
	}
	fields[portalCSVBillingColumn-12] = portalCSVReadableBilling(r)
	fields[portalCSVSupplementColumn-12] = portalCSVRequestDetails(r)
	for i := range fields {
		fields[i] = csvSafe(fields[i])
	}
	return fields
}
