package monitor

import (
	"errors"
	"slices"
)

// No currency/quota fields. This cannot create coverage, funds, a balance,
// reconciliation approval or an artificial partial-tail Token count.
type MonthlyPreparationStatistics struct {
	UserID   int64  `json:"user_id"`
	Month    string `json:"month"`
	Requests int64  `json:"requests"`
	Tokens   int64  `json:"tokens"`
	Source   string `json:"source"`
}

func monthlyPreparationStatisticsValid(a MonthlyPreparationAccount, sources map[string]bool) error {
	if len(a.MonthStatistics) > len(a.Months) {
		return errors.New("整月统计超出账号准备月份范围")
	}
	seen := map[string]bool{}
	for _, s := range a.MonthStatistics {
		if s.UserID != a.UserID || !slices.Contains(a.Months, s.Month) || seen[s.Month] ||
			s.Requests < 0 || s.Requests > 9007199254740991 || s.Tokens < 0 || s.Tokens > 9007199254740991 || !monthlyBundleRef(s.Source, sources) {
			return errors.New("整月统计的账号、月份、条数或留存来源无效")
		}
		seen[s.Month] = true
	}
	return nil
}

func monthlyResolveMonthStatistics(a MonthlyPreparationAccount, month string, v monthlyPreparationTotals) (monthlyPreparationTotals, error) {
	for _, s := range a.MonthStatistics {
		if s.Month != month {
			continue
		}
		if !v.logsComplete || (v.requestsKnown && v.requests != s.Requests) || (v.tokensKnown && v.tokens != s.Tokens) {
			return v, errors.New("整月统计与已取得的日志区间不一致；不能覆盖已知值或补齐缺失流水")
		}
		v.requests, v.tokens = s.Requests, s.Tokens
		v.requestsKnown, v.tokensKnown = true, true
		v.sources[s.Source] = true
	}
	return v, nil
}
