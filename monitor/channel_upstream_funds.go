package monitor

// 上游资金流水是对人工充值记账的证据补充，不是自动记账的替代品。
//
// 它只保存上游明确返回的事件，并把「到账额度」、「实付原币」、「管理员净调整」和
// 「消费退款」分开。无法证明语义或币种时保留原文并标记为未知，绝不猜测后计入汇总。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	upstreamFundKindTopup       = "topup"
	upstreamFundKindRedemption  = "redemption"
	upstreamFundKindManualTopup = "manual_topup"
	upstreamFundKindAdminAdd    = "admin_add"
	upstreamFundKindAdminSub    = "admin_subtract"
	upstreamFundKindAdminSet    = "admin_override"
	upstreamFundKindRefund      = "usage_refund"
	upstreamFundKindUnknown     = "unknown"

	upstreamFundConfidenceStructured = "structured"
	upstreamFundConfidenceField      = "field"
	upstreamFundConfidenceLegacyText = "legacy_text"
	upstreamFundConfidenceUnknown    = "unknown"
)

type ChannelUpstreamFundEvent struct {
	Domain       string `gorm:"primaryKey;size:253;column:domain;index:idx_upstream_fund_domain_time,priority:1" json:"-"`
	AccountEpoch string `gorm:"primaryKey;size:64;column:account_epoch" json:"-"`
	EventKey     string `gorm:"primaryKey;size:72;column:event_key" json:"event_key"`

	OccurredAt int64  `gorm:"column:occurred_at;index;index:idx_upstream_fund_domain_time,priority:2" json:"occurred_at"`
	Provider   string `gorm:"size:24;column:provider" json:"provider"`
	SourceType int    `gorm:"column:source_type" json:"source_type"`
	Kind       string `gorm:"size:32;column:kind;index" json:"kind"`
	Direction  string `gorm:"size:12;column:direction" json:"direction"`
	Confidence string `gorm:"size:20;column:confidence" json:"confidence"`

	AmountUSD    float64 `gorm:"column:amount_usd" json:"amount_usd"`
	AmountKnown  bool    `gorm:"column:amount_known" json:"amount_known"`
	PaidAmount   float64 `gorm:"column:paid_amount" json:"paid_amount"`
	PaidKnown    bool    `gorm:"column:paid_known" json:"paid_known"`
	PaidCurrency string  `gorm:"size:12;column:paid_currency" json:"paid_currency,omitempty"`
	BeforeUSD    float64 `gorm:"column:before_usd" json:"before_usd"`
	BeforeKnown  bool    `gorm:"column:before_known" json:"before_known"`
	AfterUSD     float64 `gorm:"column:after_usd" json:"after_usd"`
	AfterKnown   bool    `gorm:"column:after_known" json:"after_known"`

	Content       string `gorm:"type:text;column:content" json:"content"`
	RequestID     string `gorm:"size:128;column:request_id;index" json:"request_id,omitempty"`
	RawJSON       string `gorm:"type:text;column:raw_json" json:"-"`
	RawTruncated  bool   `gorm:"column:raw_truncated" json:"-"`
	ObservedCount int    `gorm:"column:observed_count" json:"observed_count"`
	FetchedAt     int64  `gorm:"column:fetched_at;index" json:"fetched_at"`
}

type UpstreamFundSyncState struct {
	Domain           string `gorm:"primaryKey;size:253;column:domain" json:"domain"`
	AccountEpoch     string `gorm:"primaryKey;size:64;column:account_epoch" json:"-"`
	Status           string `gorm:"size:24;column:status;index" json:"status"`
	NextSyncAt       int64  `gorm:"column:next_sync_at;index" json:"next_sync_at"`
	LastAttemptAt    int64  `gorm:"column:last_attempt_at" json:"last_attempt_at"`
	LastSuccessAt    int64  `gorm:"column:last_success_at" json:"last_success_at"`
	TailSyncedUntil  int64  `gorm:"column:tail_synced_until" json:"data_until"`
	CoverageFrom     int64  `gorm:"column:coverage_from" json:"coverage_from"`
	BackfillBefore   int64  `gorm:"column:backfill_before" json:"-"`
	BackfillDone     bool   `gorm:"column:backfill_done" json:"backfill_done"`
	WindowFrom       int64  `gorm:"column:window_from" json:"-"`
	WindowTo         int64  `gorm:"column:window_to" json:"-"`
	WindowBackfill   bool   `gorm:"column:window_backfill" json:"-"`
	RowsTotal        int64  `gorm:"column:rows_total" json:"rows_total"`
	ConsecutiveFails int    `gorm:"column:consecutive_fails" json:"-"`
	LastError        string `gorm:"size:512;column:last_error" json:"last_error,omitempty"`
	UpdatedAt        int64  `gorm:"column:updated_at;index" json:"-"`
}

type upstreamFundItem struct {
	EventKey     string
	OccurredAt   int64
	SourceType   int
	Kind         string
	Direction    string
	Confidence   string
	AmountUSD    float64
	AmountKnown  bool
	PaidAmount   float64
	PaidKnown    bool
	PaidCurrency string
	BeforeUSD    float64
	BeforeKnown  bool
	AfterUSD     float64
	AfterKnown   bool
	Content      string
	RequestID    string
	Raw          string
	RawTruncated bool
	Financial    bool
}

var (
	fundCreditRE = regexp.MustCompile(`(?i)(?:充值金额|到账额度|credited(?:\s+amount)?)\s*[:：]?\s*\$?\s*([0-9]+(?:\.[0-9]+)?)`)
	fundPaidRE   = regexp.MustCompile(`(?i)(?:支付金额|实付(?:金额)?|paid(?:\s+amount)?)\s*[:：]?\s*([$￥¥]?)\s*([0-9]+(?:\.[0-9]+)?)`)
	fundDollarRE = regexp.MustCompile(`\$\s*([0-9]+(?:\.[0-9]+)?)`)
)

func decodeFundObject(raw json.RawMessage) map[string]json.RawMessage {
	var out map[string]json.RawMessage
	if json.Unmarshal(raw, &out) == nil {
		return out
	}
	var inner string
	if json.Unmarshal(raw, &inner) == nil {
		_ = json.Unmarshal([]byte(inner), &out)
	}
	return out
}

func fundNumber(fields map[string]json.RawMessage, names ...string) (float64, bool) {
	for _, name := range names {
		if value, ok := fields[name]; ok {
			n, err := rawJSONNumber(value)
			if err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
				return n, true
			}
		}
	}
	return 0, false
}

func fundString(fields map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		if value, ok := fields[name]; ok {
			if s := jsonRawToString(value); s != "" {
				return s
			}
		}
	}
	return ""
}

func fundUSDFromQuota(value float64, row ChannelUpstreamAccount) (float64, bool) {
	unit := row.BalanceUnit
	if unit <= 0 || math.IsNaN(unit) || math.IsInf(unit, 0) {
		unit = defaultNewAPIQuotaPerUSD
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value / unit, true
}

func parseFundTextAmount(re *regexp.Regexp, content string, index int) (float64, bool, string) {
	m := re.FindStringSubmatch(content)
	if len(m) <= index {
		return 0, false, ""
	}
	n, err := strconv.ParseFloat(m[index], 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, false, ""
	}
	currency := ""
	if re == fundPaidRE && len(m) > 1 {
		switch m[1] {
		case "¥", "￥":
			currency = "CNY"
		case "$":
			currency = "USD"
		}
	}
	return n, true, currency
}

func decodeUpstreamFundItem(row ChannelUpstreamAccount, logType int, itemJSON json.RawMessage) (upstreamFundItem, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(itemJSON, &fields); err != nil {
		return upstreamFundItem{}, fmt.Errorf("NewAPI 资金日志条目无效: %w", err)
	}
	created, ok := fundNumber(fields, "created_at", "createdAt")
	if !ok || created <= 0 || created != math.Trunc(created) {
		return upstreamFundItem{}, fmt.Errorf("NewAPI 资金日志缺少有效 created_at")
	}
	stable, err := stableUpstreamErrorEventKey(fields)
	if err != nil {
		return upstreamFundItem{}, err
	}
	raw := string(itemJSON)
	truncated := false
	if len(raw) > upstreamErrorLogRawMax {
		raw, truncated = raw[:upstreamErrorLogRawMax], true
	}
	item := upstreamFundItem{
		EventKey: strconv.Itoa(logType) + "-" + stable, OccurredAt: int64(created), SourceType: logType,
		Kind: upstreamFundKindUnknown, Direction: "info", Confidence: upstreamFundConfidenceUnknown,
		Content:   boundedUpstreamErrorField(fundString(fields, "content", "message", "remark"), upstreamErrorLogContentMax),
		RequestID: boundedUpstreamErrorField(fundString(fields, "request_id", "requestId"), 128),
		Raw:       raw, RawTruncated: truncated,
	}
	quota, quotaKnown := fundNumber(fields, "quota")
	switch logType {
	case 1:
		item.Financial, item.Direction, item.Kind = true, "credit", upstreamFundKindTopup
		lower := strings.ToLower(item.Content)
		if strings.Contains(item.Content, "兑换码") || strings.Contains(lower, "redeem") {
			item.Kind = upstreamFundKindRedemption
		} else if strings.Contains(item.Content, "手动") || strings.Contains(item.Content, "管理员") || strings.Contains(lower, "manual") {
			item.Kind = upstreamFundKindManualTopup
		}
		if amount, found, _ := parseFundTextAmount(fundCreditRE, item.Content, 1); found {
			item.AmountUSD, item.AmountKnown, item.Confidence = amount, true, upstreamFundConfidenceLegacyText
		} else if quotaKnown && quota != 0 {
			item.AmountUSD, item.AmountKnown = fundUSDFromQuota(quota, row)
			item.Confidence = upstreamFundConfidenceField
		} else if amount, found, _ := parseFundTextAmount(fundDollarRE, item.Content, 1); found {
			item.AmountUSD, item.AmountKnown, item.Confidence = amount, true, upstreamFundConfidenceLegacyText
		}
		if paid, found, currency := parseFundTextAmount(fundPaidRE, item.Content, 2); found {
			item.PaidAmount, item.PaidKnown, item.PaidCurrency = paid, true, currency
		}
	case 3:
		other := decodeFundObject(fields["other"])
		op := decodeFundObject(other["op"])
		action := strings.ToLower(fundString(op, "action"))
		params := decodeFundObject(op["params"])
		if len(params) == 0 {
			params = decodeFundObject(other["params"])
		}
		switch action {
		case "user.quota_add":
			item.Financial, item.Kind, item.Direction, item.Confidence = true, upstreamFundKindAdminAdd, "credit", upstreamFundConfidenceStructured
			if v, found := fundNumber(params, "quota", "amount"); found {
				item.AmountUSD, item.AmountKnown = fundUSDFromQuota(v, row)
			}
		case "user.quota_subtract":
			item.Financial, item.Kind, item.Direction, item.Confidence = true, upstreamFundKindAdminSub, "debit", upstreamFundConfidenceStructured
			if v, found := fundNumber(params, "quota", "amount"); found {
				item.AmountUSD, item.AmountKnown = fundUSDFromQuota(math.Abs(v), row)
			}
		case "user.quota_override":
			item.Financial, item.Kind, item.Direction, item.Confidence = true, upstreamFundKindAdminSet, "info", upstreamFundConfidenceStructured
			from, fromOK := fundNumber(params, "from", "before", "old")
			to, toOK := fundNumber(params, "to", "after", "new")
			if fromOK {
				item.BeforeUSD, item.BeforeKnown = fundUSDFromQuota(from, row)
			}
			if toOK {
				item.AfterUSD, item.AfterKnown = fundUSDFromQuota(to, row)
			}
			if item.BeforeKnown && item.AfterKnown {
				delta := item.AfterUSD - item.BeforeUSD
				item.AmountUSD, item.AmountKnown = math.Abs(delta), true
				if delta > 0 {
					item.Direction = "credit"
				} else if delta < 0 {
					item.Direction = "debit"
				}
			}
		default:
			// 兼容早期没有结构化 op 的 NewAPI，但只在文本明确出现“额度”时保留，
			// 避免把改分组、改角色等普通管理日志误当资金。
			if strings.Contains(item.Content, "额度") || strings.Contains(strings.ToLower(item.Content), "quota") {
				item.Financial, item.Kind, item.Confidence = true, upstreamFundKindAdminSet, upstreamFundConfidenceLegacyText
				amounts := fundDollarRE.FindAllStringSubmatch(item.Content, 3)
				if len(amounts) >= 2 {
					before, e1 := strconv.ParseFloat(amounts[0][1], 64)
					after, e2 := strconv.ParseFloat(amounts[1][1], 64)
					if e1 == nil && e2 == nil {
						item.BeforeUSD, item.BeforeKnown, item.AfterUSD, item.AfterKnown = before, true, after, true
						delta := after - before
						item.AmountUSD, item.AmountKnown = math.Abs(delta), true
						if delta > 0 {
							item.Direction = "credit"
						} else if delta < 0 {
							item.Direction = "debit"
						}
					}
				}
			}
		}
	case 6:
		item.Financial, item.Kind, item.Direction = true, upstreamFundKindRefund, "credit"
		if quotaKnown {
			item.AmountUSD, item.AmountKnown = fundUSDFromQuota(math.Abs(quota), row)
			item.Confidence = upstreamFundConfidenceField
		}
	}
	return item, nil
}

type upstreamFundWindowResult struct {
	Rows        []ChannelUpstreamFundEvent
	Truncated   bool
	SuggestedTo int64
}

func narrowedUpstreamFundWindow(from, to, splitAt int64, backfill bool) (int64, int64) {
	if splitAt <= from || splitAt >= to {
		splitAt = from + (to-from)/2
	}
	// Tail 按时间向前走，先取左半段后推进右水位。历史补全是从现有
	// coverage 向过去倒着走，必须先取与已覆盖边界相邻的右半段。如果两者
	// 都取左半段，回填水位会跳过 [splitAt,to) 并永久漏账。
	if backfill {
		return splitAt, to
	}
	return from, splitAt
}

func (m *Monitor) syncUpstreamFundWindow(ctx context.Context, row ChannelUpstreamAccount, cred newAPICredential, from, to, now int64) (upstreamFundWindowResult, error) {
	if to <= from {
		return upstreamFundWindowResult{}, fmt.Errorf("上游资金流水窗口无效")
	}
	pacer := newUpstreamUsageRequestPacer(18, upstreamUsageRequestInterval)
	epoch := newAPIUpstreamAccountEpoch(row)
	grouped := map[string]*ChannelUpstreamFundEvent{}
	for _, logType := range []int{1, 3, 6} {
		decode := func(raw json.RawMessage) (upstreamFundItem, error) { return decodeUpstreamFundItem(row, logType, raw) }
		first, err := fetchNewAPILogPageWithType(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, 1, pacer, logType, decode)
		if err != nil {
			return upstreamFundWindowResult{}, err
		}
		pages := int((first.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
		if pages > 4 {
			if to-from <= 1 {
				return upstreamFundWindowResult{}, fmt.Errorf("单秒资金/管理日志过密，无法安全完整采集")
			}
			return upstreamFundWindowResult{Truncated: true, SuggestedTo: from + (to-from)/2}, nil
		}
		expected := int(first.Total)
		if expected > upstreamUsagePageSize {
			expected = upstreamUsagePageSize
		}
		if len(first.Items) != expected {
			return upstreamFundWindowResult{}, fmt.Errorf("NewAPI 资金日志首页数量异常")
		}
		items := append([]upstreamFundItem{}, first.Items...)
		for page := 2; page <= pages; page++ {
			got, fetchErr := fetchNewAPILogPageWithType(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, page, pacer, logType, decode)
			if fetchErr != nil {
				return upstreamFundWindowResult{}, fetchErr
			}
			if got.Total != first.Total {
				return upstreamFundWindowResult{}, fmt.Errorf("NewAPI 资金日志扫描期间 total 变化")
			}
			items = append(items, got.Items...)
		}
		if int64(len(items)) != first.Total {
			return upstreamFundWindowResult{}, fmt.Errorf("NewAPI 资金日志分页不完整")
		}
		if pages > 1 {
			probe, probeErr := fetchNewAPILogPageWithType(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, 1, pacer, logType, decode)
			if probeErr != nil {
				return upstreamFundWindowResult{}, probeErr
			}
			if probe.Total != first.Total || probe.Fingerprint != first.Fingerprint {
				return upstreamFundWindowResult{}, fmt.Errorf("NewAPI 资金日志扫描期间首页已变化")
			}
		}
		for _, item := range items {
			if !item.Financial {
				continue
			}
			if existing := grouped[item.EventKey]; existing != nil {
				existing.ObservedCount++
				continue
			}
			grouped[item.EventKey] = &ChannelUpstreamFundEvent{
				Domain: row.Domain, AccountEpoch: epoch, EventKey: item.EventKey, OccurredAt: item.OccurredAt,
				Provider: row.Provider, SourceType: item.SourceType, Kind: item.Kind, Direction: item.Direction,
				Confidence: item.Confidence, AmountUSD: item.AmountUSD, AmountKnown: item.AmountKnown,
				PaidAmount: item.PaidAmount, PaidKnown: item.PaidKnown, PaidCurrency: item.PaidCurrency,
				BeforeUSD: item.BeforeUSD, BeforeKnown: item.BeforeKnown, AfterUSD: item.AfterUSD, AfterKnown: item.AfterKnown,
				Content: item.Content, RequestID: item.RequestID, RawJSON: item.Raw, RawTruncated: item.RawTruncated,
				ObservedCount: 1, FetchedAt: now,
			}
		}
	}
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := upstreamFundWindowResult{Rows: make([]ChannelUpstreamFundEvent, 0, len(keys))}
	for _, key := range keys {
		out.Rows = append(out.Rows, *grouped[key])
	}
	return out, nil
}

func (m *Monitor) persistUpstreamFundEvents(ctx context.Context, rows []ChannelUpstreamFundEvent) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += 100 {
		end := start + 100
		if end > len(rows) {
			end = len(rows)
		}
		if err := m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "domain"}, {Name: "account_epoch"}, {Name: "event_key"}},
			DoUpdates: clause.Assignments(map[string]any{
				"occurred_at": gorm.Expr("excluded.occurred_at"), "provider": gorm.Expr("excluded.provider"),
				"source_type": gorm.Expr("excluded.source_type"), "kind": gorm.Expr("excluded.kind"),
				"direction": gorm.Expr("excluded.direction"), "confidence": gorm.Expr("excluded.confidence"),
				"amount_usd": gorm.Expr("excluded.amount_usd"), "amount_known": gorm.Expr("excluded.amount_known"),
				"paid_amount": gorm.Expr("excluded.paid_amount"), "paid_known": gorm.Expr("excluded.paid_known"),
				"paid_currency": gorm.Expr("excluded.paid_currency"), "before_usd": gorm.Expr("excluded.before_usd"),
				"before_known": gorm.Expr("excluded.before_known"), "after_usd": gorm.Expr("excluded.after_usd"),
				"after_known": gorm.Expr("excluded.after_known"), "content": gorm.Expr("excluded.content"),
				"request_id": gorm.Expr("excluded.request_id"), "raw_json": gorm.Expr("excluded.raw_json"),
				"raw_truncated": gorm.Expr("excluded.raw_truncated"), "fetched_at": gorm.Expr("excluded.fetched_at"),
				"observed_count": gorm.Expr("MAX(observed_count, excluded.observed_count)"),
			}),
		}).Create(rows[start:end]).Error; err != nil {
			return fmt.Errorf("保存上游资金流水失败: %w", err)
		}
	}
	return nil
}

func upstreamFundsBackfillDays(s Settings) int {
	days := s.UpstreamFundsBackfillDays
	if days < 1 {
		return 90
	}
	if days > 365 {
		return 365
	}
	return days
}

func (m *Monitor) loadFundState(ctx context.Context, row ChannelUpstreamAccount) (UpstreamFundSyncState, error) {
	epoch := newAPIUpstreamAccountEpoch(row)
	var state UpstreamFundSyncState
	err := m.storeDB.WithContext(ctx).First(&state, "domain = ? AND account_epoch = ?", row.Domain, epoch).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return UpstreamFundSyncState{Domain: row.Domain, AccountEpoch: epoch}, nil
	}
	return state, err
}

func (m *Monitor) saveFundState(ctx context.Context, state *UpstreamFundSyncState, now int64) error {
	state.UpdatedAt = now
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}, {Name: "account_epoch"}}, UpdateAll: true}).Create(state).Error
}

func (m *Monitor) failFundState(ctx context.Context, state *UpstreamFundSyncState, now int64, cause error) error {
	state.Status = upstreamStatusError
	state.LastAttemptAt = now
	state.ConsecutiveFails++
	state.LastError = boundedUpstreamErrorField(cause.Error(), 512)
	var authErr *upstreamAuthError
	if errors.As(cause, &authErr) {
		state.Status = upstreamStatusReconnect
		state.NextSyncAt = upstreamAccountIsolatedUntil
	} else {
		backoff := 2 * time.Minute << min(state.ConsecutiveFails-1, 6)
		if backoff > 2*time.Hour {
			backoff = 2 * time.Hour
		}
		state.NextSyncAt = now + int64(backoff.Seconds())
		if retry := upstreamRetryAt(cause); retry > state.NextSyncAt {
			state.NextSyncAt = retry
		}
	}
	if err := m.saveFundState(ctx, state, now); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (m *Monitor) syncOneUpstreamFunds(ctx context.Context, domain string, background bool) (UpstreamFundSyncState, error) {
	var release func()
	var err error
	if background {
		release, err = m.tryAcquireUpstreamAccountBackground(domain)
	} else {
		release, err = m.acquireUpstreamAccountAdmin(ctx, domain)
	}
	if err != nil {
		return UpstreamFundSyncState{Domain: domain}, err
	}
	defer release()
	var row ChannelUpstreamAccount
	if err = m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		return UpstreamFundSyncState{Domain: domain}, err
	}
	state, err := m.loadFundState(ctx, row)
	if err != nil {
		return state, err
	}
	now := time.Now().Unix()
	if !row.Enabled || !row.UsageSyncEnabled {
		return state, fmt.Errorf("该上游账户未授权使用日志同步")
	}
	if !upstreamErrorLogDomainAllowed(m.cfg.UpstreamFundsDomains, row.Domain) {
		return state, fmt.Errorf("该上游未加入资金流水灰度白名单")
	}
	if row.Provider != upstreamProviderNewAPI {
		state.Status = upstreamStatusUnsupported
		state.LastAttemptAt = now
		state.NextSyncAt = 0
		state.LastError = upstreamProviderName(row.Provider) + "资金明细接口尚未完成契约验证，未进行猜测采集"
		return state, m.saveFundState(ctx, &state, now)
	}
	credential, err := m.credentialForAccount(row)
	if err != nil {
		return state, m.failFundState(ctx, &state, now, &upstreamAuthError{err: err})
	}
	cred, ok := credential.(newAPICredential)
	if !ok {
		return state, m.failFundState(ctx, &state, now, &upstreamAuthError{err: fmt.Errorf("凭据类型不匹配")})
	}
	from, to, isBackfill := state.WindowFrom, state.WindowTo, state.WindowBackfill
	if to <= from {
		if state.TailSyncedUntil == 0 {
			from, to = now-6*3600, now+1
		} else if state.TailSyncedUntil < now-120 {
			from, to = state.TailSyncedUntil-120, now+1
		} else if !state.BackfillDone {
			before := state.BackfillBefore
			if before <= 0 {
				before = state.CoverageFrom
			}
			target := now - int64(upstreamFundsBackfillDays(m.cfg))*86400
			if before <= target {
				state.BackfillDone = true
			} else {
				to = before
				from = to - 86400
				if from < target {
					from = target
				}
				isBackfill = true
			}
		}
		if to <= from {
			from, to = state.TailSyncedUntil-120, now+1
			isBackfill = false
		}
	}
	result, err := m.syncUpstreamFundWindow(ctx, row, cred, from, to, now)
	if err != nil {
		return state, m.failFundState(ctx, &state, now, err)
	}
	if result.Truncated {
		state.Status = upstreamStatusPending
		state.LastAttemptAt = now
		state.ConsecutiveFails = 0
		state.LastError = "窗口过密，已缩小后续传"
		state.WindowFrom, state.WindowTo = narrowedUpstreamFundWindow(from, to, result.SuggestedTo, isBackfill)
		state.WindowBackfill = isBackfill
		state.NextSyncAt = now + 30
		err = m.saveFundState(ctx, &state, now)
		return state, err
	}
	if err = m.persistUpstreamFundEvents(ctx, result.Rows); err != nil {
		return state, m.failFundState(ctx, &state, now, err)
	}
	if isBackfill {
		state.BackfillBefore = from
		if state.CoverageFrom == 0 || from < state.CoverageFrom {
			state.CoverageFrom = from
		}
	} else {
		state.TailSyncedUntil = to
		if state.CoverageFrom == 0 {
			state.CoverageFrom = from
		}
		if state.BackfillBefore == 0 {
			state.BackfillBefore = from
		}
	}
	target := now - int64(upstreamFundsBackfillDays(m.cfg))*86400
	if state.BackfillBefore > 0 && state.BackfillBefore <= target {
		state.BackfillDone = true
	}
	state.Status = upstreamStatusOK
	state.LastAttemptAt = now
	state.LastSuccessAt = now
	state.ConsecutiveFails = 0
	state.LastError = ""
	state.WindowFrom = 0
	state.WindowTo = 0
	state.WindowBackfill = false
	if err = m.storeDB.WithContext(ctx).Model(&ChannelUpstreamFundEvent{}).Where("domain = ? AND account_epoch = ?", row.Domain, state.AccountEpoch).Count(&state.RowsTotal).Error; err != nil {
		return state, m.failFundState(ctx, &state, now, err)
	}
	if state.BackfillDone {
		state.NextSyncAt = now + 1800
	} else {
		state.NextSyncAt = now + 60
	}
	if err = m.saveFundState(ctx, &state, now); err != nil {
		return state, err
	}
	return state, nil
}

func (m *Monitor) syncDueUpstreamFunds(ctx context.Context) {
	if !m.cfg.UpstreamFundsSyncEnabled || len(m.cfg.UpstreamFundsDomains) == 0 {
		return
	}
	roundCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	now := time.Now().Unix()
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.WithContext(roundCtx).Where("enabled = ? AND usage_sync_enabled = ?", true, true).Order("domain ASC").Find(&rows).Error; err != nil {
		slog.Warn("读取上游资金流水账户失败", "err", err)
		return
	}
	type dueRow struct {
		row  ChannelUpstreamAccount
		last int64
	}
	eligible := make([]dueRow, 0, len(rows))
	for _, row := range rows {
		if !upstreamErrorLogDomainAllowed(m.cfg.UpstreamFundsDomains, row.Domain) {
			continue
		}
		state, err := m.loadFundState(roundCtx, row)
		if err != nil {
			continue
		}
		if state.Status == upstreamStatusUnsupported || state.NextSyncAt > now {
			continue
		}
		eligible = append(eligible, dueRow{row, state.LastAttemptAt})
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].last != eligible[j].last {
			return eligible[i].last < eligible[j].last
		}
		return eligible[i].row.Domain < eligible[j].row.Domain
	})
	for i, item := range eligible {
		if i >= 2 || roundCtx.Err() != nil {
			return
		}
		if _, err := m.syncOneUpstreamFunds(roundCtx, item.row.Domain, true); err != nil && !errors.Is(err, errUpstreamAccountBusy) {
			slog.Warn("上游资金流水同步未完成", "domain", item.row.Domain, "err", err)
		}
	}
}

type upstreamFundSummary struct {
	CreditedUSD               float64 `json:"credited_usd"`
	DebitedUSD                float64 `json:"debited_usd"`
	RefundedUSD               float64 `json:"refunded_usd"`
	UnknownAmountEvents       int64   `json:"unknown_amount_events"`
	PaidUnknownCurrencyEvents int64   `json:"paid_unknown_currency_events"`
	EventOccurrences          int64   `json:"event_occurrences"`
}

type upstreamFundPaidTotal struct {
	Currency string  `json:"currency"`
	Amount   float64 `json:"amount"`
}

func (m *Monitor) getChannelUpstreamFundsHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if domain == "" || len(domain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	var row ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "该主域名尚未配置上游账户"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取上游账户失败"})
		return
	}
	now := time.Now().Unix()
	from := now - 30*86400
	to := now + 1
	if raw := c.Query("from"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			from = parsed
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "from 无效"})
			return
		}
	}
	if raw := c.Query("to"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			to = parsed
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "to 无效"})
			return
		}
	}
	if to <= from || to-from > 366*86400 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "查询时间范围无效（最多 366 天）"})
		return
	}
	state, _ := m.loadFundState(ctx, row)
	var events []ChannelUpstreamFundEvent
	epoch := newAPIUpstreamAccountEpoch(row)
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND occurred_at >= ? AND occurred_at < ?", domain, epoch, from, to).Order("occurred_at DESC,event_key DESC").Limit(500).Find(&events).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取资金流水失败"})
		return
	}
	// 明细最多返回 500 条，但汇总必须覆盖整个查询区间，不能被页面展示上限截断。
	// observed_count 保留同秒完全相同事件的重数；早期零值按 1 处理。
	summary := upstreamFundSummary{}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamFundEvent{}).
		Select(`
			COALESCE(SUM(CASE WHEN amount_known AND kind <> ? AND direction = 'credit' THEN amount_usd * CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END ELSE 0 END), 0) AS credited_usd,
			COALESCE(SUM(CASE WHEN amount_known AND kind <> ? AND direction = 'debit' THEN amount_usd * CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END ELSE 0 END), 0) AS debited_usd,
			COALESCE(SUM(CASE WHEN amount_known AND kind = ? THEN amount_usd * CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END ELSE 0 END), 0) AS refunded_usd,
			COALESCE(SUM(CASE WHEN NOT amount_known THEN CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END ELSE 0 END), 0) AS unknown_amount_events,
			COALESCE(SUM(CASE WHEN paid_known AND paid_currency = '' THEN CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END ELSE 0 END), 0) AS paid_unknown_currency_events,
			COALESCE(SUM(CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END), 0) AS event_occurrences`,
			upstreamFundKindRefund, upstreamFundKindRefund, upstreamFundKindRefund).
		Where("domain = ? AND account_epoch = ? AND occurred_at >= ? AND occurred_at < ?", domain, epoch, from, to).
		Scan(&summary).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "汇总资金流水失败"})
		return
	}
	var paidTotals []upstreamFundPaidTotal
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamFundEvent{}).
		Select("paid_currency AS currency, SUM(paid_amount * CASE WHEN observed_count > 0 THEN observed_count ELSE 1 END) AS amount").
		Where("domain = ? AND account_epoch = ? AND occurred_at >= ? AND occurred_at < ? AND paid_known = ? AND paid_currency <> ''", domain, epoch, from, to, true).
		Group("paid_currency").Order("paid_currency ASC").Scan(&paidTotals).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "汇总上游实付原币失败"})
		return
	}
	capability := "supported"
	reason := ""
	if row.Provider != upstreamProviderNewAPI {
		capability = "pending_adapter"
		reason = upstreamProviderName(row.Provider) + "资金明细接口待契约验证"
	} else if !row.UsageSyncEnabled {
		capability = "account_off"
		reason = "账户未授权读取上游日志"
	} else if !m.cfg.UpstreamFundsSyncEnabled {
		capability = "global_off"
		reason = "资金流水灰度开关未开启"
	} else if !upstreamErrorLogDomainAllowed(m.cfg.UpstreamFundsDomains, domain) {
		capability = "not_allowed"
		reason = "该主域名未加入资金流水白名单"
	}
	limitations := []string{}
	if row.Provider == upstreamProviderNewAPI {
		limitations = append(limitations, "NewAPI 普通用户的 self 日志可能看不到管理员记在操作人名下的额度调整；这类缺口必须继续保留人工记账或后续用余额快照对账发现。")
		limitations = append(limitations, "实付金额没有明确币种时保留为原币未知，不与美元到账额度相加。")
	}
	c.JSON(http.StatusOK, gin.H{"domain": domain, "capability": capability, "capability_reason": reason, "limitations": limitations, "state": state, "from": from, "to": to, "limited": len(events) >= 500, "summary": summary, "paid_totals": paidTotals, "events": events})
}

func (m *Monitor) syncChannelUpstreamFundsHandler(c *gin.Context) {
	if !m.cfg.UpstreamFundsSyncEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "上游资金流水同步处于灰度关闭状态"})
		return
	}
	if c.Request.ContentLength > 4096 {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "请求过大"})
		return
	}
	var in channelUpstreamSyncInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式无效"})
		return
	}
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	if in.Domain == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 75*time.Second)
	defer cancel()
	state, err := m.syncOneUpstreamFunds(ctx, in.Domain, false)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error(), "state": state})
		return
	}
	c.JSON(http.StatusOK, gin.H{"state": state})
}
