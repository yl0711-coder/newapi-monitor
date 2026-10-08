package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	upstreamProviderOpenOx        = "openox"
	upstreamUsageAdapterOpenOx    = "openox_usage"
	openOxPageSize                = 20 // Console page size verified against the upstream API.
	openOxMaxPages                = 50
	openOxSubscriptionCostUnknown = "subscription_cost_unallocated"
)

type openOxCredential struct {
	AccessToken string `json:"access_token"`
}

// No login/refresh or model calls: only the explicitly imported management
// session is used. Do not retain upstream error bodies (they may echo secrets).
func openOxGET(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred openOxCredential, path string, target any) error {
	if cred.AccessToken == "" {
		return &upstreamAuthError{err: fmt.Errorf("OpenOx 管理令牌为空，请重新导入")}
	}
	raw, err := doUpstreamJSON(ctx, client, http.MethodGet, row.BaseURL+path, map[string]string{"Authorization": "Bearer " + cred.AccessToken}, nil)
	if err != nil {
		var httpErr *upstreamHTTPError
		if errors.As(err, &httpErr) {
			if httpErr.Status == http.StatusUnauthorized || httpErr.Status == http.StatusForbidden {
				return &upstreamAuthError{err: fmt.Errorf("OpenOx 管理令牌已失效或无权限（HTTP %d），请重新登录上游并导入 auth_token", httpErr.Status)}
			}
			return &upstreamHTTPError{Status: httpErr.Status, RetryAt: httpErr.RetryAt}
		}
		return err
	}
	var envelope struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil || !envelope.Success || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return fmt.Errorf("OpenOx 响应结构无效或 success 不为 true")
	}
	if json.Unmarshal(envelope.Data, target) != nil {
		return fmt.Errorf("OpenOx 响应字段无法解析")
	}
	return nil
}

func readOpenOxProfile(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred openOxCredential) (upstreamBalanceResult, int64, error) {
	var profile struct {
		Spendable json.RawMessage `json:"spendable_balance"`
		User      struct {
			ID int64 `json:"id"`
		} `json:"user"`
	}
	if err := openOxGET(ctx, client, row, cred, "/api/v1/profile", &profile); err != nil {
		return upstreamBalanceResult{}, 0, err
	}
	amount, err := rawJSONNumber(profile.Spendable)
	if err != nil || amount < 0 || profile.User.ID <= 0 {
		return upstreamBalanceResult{}, 0, fmt.Errorf("OpenOx 缺少有效可用钱包余额或用户 ID")
	}
	if row.UserID > 0 && row.UserID != profile.User.ID {
		return upstreamBalanceResult{}, 0, fmt.Errorf("OpenOx 令牌账户与已绑定用户不一致，请核对账户配置")
	}
	return upstreamBalanceResult{BalanceUSD: amount, BalanceRaw: amount, BalanceUnit: 1}, profile.User.ID, nil
}

type openOxUsageItem struct {
	ID                 int64           `json:"id"`
	UserID             int64           `json:"user_id"`
	CreatedAt          string          `json:"created_at"`
	TotalTokens        *int64          `json:"total_tokens"`
	Cost               json.RawMessage `json:"cost"`
	SubscriptionCredit json.RawMessage `json:"subscription_credit_used"`
	Estimated          *bool           `json:"usage_estimated"`
}

type openOxUsagePage struct {
	Items []openOxUsageItem `json:"items"`
	Total *int              `json:"total"`
	Page  int               `json:"page"`
	Size  int               `json:"size"`
}

func fetchOpenOxUsagePage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred openOxCredential, from, to int64, page int, pacer *upstreamUsageRequestPacer) (openOxUsagePage, error) {
	result := openOxUsagePage{}
	if err := pacer.beforeRequest(ctx); err != nil {
		return result, err
	}
	query := url.Values{"page": {strconv.Itoa(page)}, "size": {strconv.Itoa(openOxPageSize)},
		"date_from": {time.Unix(from, 0).In(cstLocation).Format("2006-01-02")},
		"date_to":   {time.Unix(to-1, 0).In(cstLocation).Format("2006-01-02")}}
	if err := openOxGET(ctx, client, row, cred, "/api/v1/usage?"+query.Encode(), &result); err != nil {
		return result, err
	}
	if result.Total == nil || *result.Total < 0 || result.Items == nil || result.Page != page || result.Size != openOxPageSize || len(result.Items) > openOxPageSize {
		return result, fmt.Errorf("OpenOx 分页字段缺失或不匹配，未发布部分账单")
	}
	return result, nil
}

func openOxUsageFingerprint(page openOxUsagePage) [32]byte {
	encoded, _ := json.Marshal(page)
	return sha256.Sum256(encoded)
}

// Fetch the inclusive China calendar days, then locally select the requested
// half-open hour window. Never pretend the server supports hourly filtering.
func fetchOpenOxUsageWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred openOxCredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	if from <= 0 || from%3600 != 0 || to <= from || to-from > 48*3600 || row.UserID <= 0 {
		return upstreamUsageResult{}, fmt.Errorf("OpenOx 用量窗口或绑定账户无效")
	}
	first, err := fetchOpenOxUsagePage(ctx, client, row, cred, from, to, 1, pacer)
	if err != nil {
		return upstreamUsageResult{}, err
	}
	if *first.Total > openOxPageSize*openOxMaxPages {
		return upstreamUsageResult{}, fmt.Errorf("OpenOx 自然日记录超出单轮安全上限，未发布部分账单")
	}
	items := append([]openOxUsageItem(nil), first.Items...)
	for page := 2; len(items) < *first.Total; page++ {
		if page > openOxMaxPages || len(items) != (page-1)*openOxPageSize {
			return upstreamUsageResult{}, fmt.Errorf("OpenOx 分页提前结束，未发布部分账单")
		}
		next, fetchErr := fetchOpenOxUsagePage(ctx, client, row, cred, from, to, page, pacer)
		if fetchErr != nil {
			return upstreamUsageResult{}, fetchErr
		}
		if *next.Total != *first.Total {
			return upstreamUsageResult{}, fmt.Errorf("OpenOx 分页期间记录数变化，稍后重试")
		}
		items = append(items, next.Items...)
	}
	if len(items) != *first.Total {
		return upstreamUsageResult{}, fmt.Errorf("OpenOx 实际记录数与 total 不符")
	}
	// Re-read the head even for one page, including empty pages. Only stable
	// reads are published; retries replace hours, never append their totals.
	check, err := fetchOpenOxUsagePage(ctx, client, row, cred, from, to, 1, pacer)
	if err != nil {
		return upstreamUsageResult{}, err
	}
	if openOxUsageFingerprint(first) != openOxUsageFingerprint(check) {
		return upstreamUsageResult{}, fmt.Errorf("OpenOx 首页在采集期间变化，稍后重试")
	}
	return aggregateOpenOxUsage(row, items, from, to)
}

func aggregateOpenOxUsage(row ChannelUpstreamAccount, items []openOxUsageItem, from, to int64) (upstreamUsageResult, error) {
	hours := make([]ChannelUpstreamUsageHour, (to-from+3599)/3600)
	for i := range hours {
		hours[i] = ChannelUpstreamUsageHour{Domain: row.Domain, Provider: upstreamProviderOpenOx, SourceKind: upstreamUsageAdapterOpenOx, HourTs: from + int64(i)*3600, BucketSeconds: 3600, UnitPerUSD: 1}
		if end := hours[i].HourTs + 3600; end > to {
			hours[i].BucketSeconds = to - hours[i].HourTs
		}
	}
	seen := make(map[int64]bool, len(items))
	dayFrom, dayTo := cstDayStart(from), cstDayStart(to-1)+86400
	for _, item := range items {
		at, err := time.Parse(time.RFC3339Nano, item.CreatedAt)
		if err != nil {
			return upstreamUsageResult{}, fmt.Errorf("OpenOx created_at 缺少带时区的有效时间")
		}
		cost, costErr := rawJSONNumber(item.Cost)
		credit, creditErr := rawJSONNumber(item.SubscriptionCredit)
		if item.ID <= 0 || seen[item.ID] || item.UserID != row.UserID || item.TotalTokens == nil || *item.TotalTokens < 0 || item.Estimated == nil ||
			costErr != nil || creditErr != nil || cost < 0 || credit < 0 || credit > cost+1e-9 || at.Unix() < dayFrom || at.Unix() >= dayTo {
			return upstreamUsageResult{}, fmt.Errorf("OpenOx 明细重复、账户/日期不匹配或金额/计数字段无效")
		}
		seen[item.ID] = true
		if at.Unix() < from || at.Unix() >= to {
			continue
		}
		bucket := &hours[(at.Unix()-from)/3600]
		if *item.TotalTokens > math.MaxInt64-bucket.Tokens || math.IsInf(bucket.CostUSD+cost, 0) {
			return upstreamUsageResult{}, fmt.Errorf("OpenOx 聚合数值溢出")
		}
		bucket.Requests++
		bucket.Tokens += *item.TotalTokens // Includes cache; do not add token aliases.
		bucket.CostUSD += cost             // Do not reconstruct cost from rate/component fields.
		bucket.Quota += cost
		bucket.Provisional = bucket.Provisional || *item.Estimated
	}
	return upstreamUsageResult{Hours: hours, DataUntil: to, Adapter: upstreamUsageAdapterOpenOx}, nil
}

func validateOpenOxInput(in *channelUpstreamSaveInput) error {
	if len(in.AccessToken) > 16<<10 || strings.ContainsAny(in.AccessToken, "\r\n\t ") || strings.HasPrefix(in.AccessToken, "sk-") {
		return fmt.Errorf("请填写 OpenOx 后台 auth_token，不是模型 API Key 或 Authorization 整行")
	}
	if in.Password != "" || in.RefreshToken != "" || in.APIKey != "" || len(in.APIKeys) != 0 {
		return fmt.Errorf("OpenOx 只支持导入后台 auth_token，不使用密码或 Refresh Token")
	}
	return nil
}
