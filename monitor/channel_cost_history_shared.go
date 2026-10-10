package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

func normalizeChannelCostHistoricalMode(in *channelCostHistoricalBindingInput) error {
	in.AllocationMode = strings.TrimSpace(in.AllocationMode)
	if in.AllocationMode == "" {
		in.AllocationMode = "allocated" // Existing clients selected one channel.
	}
	switch in.AllocationMode {
	case "allocated":
		if in.LocalChannelID <= 0 {
			return errors.New("历史独占归属必须指定本地渠道")
		}
	case "shared":
		if in.LocalChannelID != 0 {
			return errors.New("历史共享来源不得指定或猜测本地渠道")
		}
		if in.ValidFrom == nil || in.ValidTo == nil {
			return errors.New("历史共享来源必须明确指定有限的开始和结束时间")
		}
	default:
		return errors.New("历史归属方式仅支持指定本地渠道或共享不分摊")
	}
	return nil
}

type channelCostHistoricalCostHour struct {
	HourTs            int64
	ChargeUnits       int64
	ChargeUnitsPerUSD string
	Requests          int64
}

func (m *Monitor) channelCostHistoricalCostHours(ctx context.Context, in channelCostHistoricalBindingInput) ([]channelCostHistoricalCostHour, error) {
	var hours []channelCostHistoricalCostHour
	err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamCostHourEvidence{}).
		Select("hour_ts, charge_units_per_usd, COALESCE(SUM(charge_units), 0) charge_units, COALESCE(SUM(requests), 0) requests").
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND semantics_version = ?", in.Domain, in.AccountEpoch, in.SourceRef, channelCostEvidenceSemanticsVersion).
		Scopes(channelCostHistoricalRangeScope(in, "hour_ts")).
		Group("hour_ts, charge_units_per_usd").Order("hour_ts").Scan(&hours).Error
	return hours, err
}

// Shared history confirms a finite ownership limitation, not an allocation.
// Reuse the existing channel-0 cost pool: never infer customer/test splits or
// apportion costs by request count, tokens, or today's channel configuration.
func (m *Monitor) planChannelCostSharedHistory(ctx context.Context, in channelCostHistoricalBindingInput, binding ChannelCostSourceBinding, hours, requests, queueable int64) (channelCostHistoricalBindingPlan, int, error) {
	var plan channelCostHistoricalBindingPlan
	costHours, err := m.channelCostHistoricalCostHours(ctx, in)
	if err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("读取历史来源金额证据失败")
	}
	var cost int64
	for _, hour := range costHours {
		amount, err := unitsToMicroUSDCanonical(hour.ChargeUnits, hour.ChargeUnitsPerUSD)
		if err != nil || hour.Requests < 0 {
			return plan, http.StatusConflict, errors.New("历史来源包含无效计费单位或请求数，禁止回填")
		}
		if err := addEconomicsInt64(&cost, amount); err != nil {
			return plan, http.StatusConflict, errors.New("历史来源金额超出安全范围")
		}
	}
	warnings := []string{
		"共享成本保留在上游账户，仅计一次，不按请求数或 Tokens 分摊到单个渠道",
		"共享不等于内部测试成本已剔除；缺少可靠拆分证据时，不确认单渠道成本和客户毛利",
	}
	if queueable < hours {
		warnings = append(warnings, fmt.Sprintf("%d 个小时尚未核验对平，本次仅将 %d 个小时加入重算队列", hours-queueable, queueable))
	}
	plan = channelCostHistoricalBindingPlan{
		Binding: binding, EvidenceHours: hours, EvidenceRequests: requests,
		EvidenceBilledCost: economicsMoney(cost), WillQueueHours: queueable,
		TemporalOverlapQuality: "shared", RiskWarnings: warnings,
	}
	return plan, http.StatusOK, nil
}
