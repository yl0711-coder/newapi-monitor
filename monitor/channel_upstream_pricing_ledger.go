package monitor

// 上游计价账本是现有上游消费汇总的独立影子证据层。
// 它不修改任何中转站、不保存原始日志/凭据/用户内容，也不参与现有消费展示。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	upstreamPricingSemanticsVersion = 2
	// The shadow ledger has its own lock but still shares host pacing and SQLite.
	// Bound every turn so a pathological upstream/hour remains cheap and
	// restartable instead of monopolizing either shared resource.
	upstreamPricingMaxRequestsPerRun       = 20
	upstreamPricingMaxCheckpointDimensions = 2048
	upstreamPricingMaxCheckpointBytes      = 2 << 20
)

const (
	pricingRatioMissing = "missing"
	pricingRatioNull    = "null"
	pricingRatioValid   = "valid"
	pricingRatioInvalid = "invalid"
)

type newAPIPricingAttributes struct {
	GroupName            string
	ModelName            string
	TokenID              int64
	GroupRatio           string
	GroupRatioCanonical  string
	GroupRatioState      string
	UserGroupRatio       string
	UserRatioCanonical   string
	UserGroupRatioState  string
	EffectiveRatio       string
	EffectiveRatioSource string
	DiscountRatio        string
	DiscountCanonical    string
	DiscountRatioState   string
	EvidenceCapability   string
	BillingMode          string
	OtherValid           bool
}

// newAPIPricingUsageItem is intentionally separate from newAPIUsageItem.
// The pricing fields are large and are parsed only by the gray-listed shadow
// ledger; the stable usage worker keeps its original lightweight item shape.
type newAPIPricingUsageItem struct {
	CreatedAt        int64
	QuotaExact       int64
	QuotaExactKnown  bool
	PromptTokens     int64
	CompletionTokens int64
	TokensExactKnown bool
	Pricing          newAPIPricingAttributes
}

type pricingRatioValue struct {
	Text      string
	Canonical string
	State     string
	rat       *big.Rat
}

// ChannelUpstreamPricingHourEvidence stores only the minimum, aggregated
// billing evidence needed to explain an upstream charge. It deliberately has
// no request ID, token name, raw `other`, prompt, response, IP, or credential.
type ChannelUpstreamPricingHourEvidence struct {
	Domain               string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch         string `gorm:"primaryKey;size:64;column:account_epoch"`
	HourTs               int64  `gorm:"primaryKey;column:hour_ts"`
	SemanticsVersion     int    `gorm:"primaryKey;column:semantics_version"`
	DimensionHash        string `gorm:"primaryKey;size:64;column:dimension_hash"`
	Provider             string `gorm:"size:24;column:provider"`
	SourceGroup          string `gorm:"size:191;column:source_group"`
	ModelName            string `gorm:"size:191;column:model_name"`
	GroupRatio           string `gorm:"size:80;column:group_ratio"`
	GroupRatioState      string `gorm:"size:16;column:group_ratio_state"`
	UserGroupRatio       string `gorm:"size:80;column:user_group_ratio"`
	UserGroupRatioState  string `gorm:"size:16;column:user_group_ratio_state"`
	EffectiveRatio       string `gorm:"size:80;column:effective_ratio"`
	EffectiveRatioSource string `gorm:"size:32;column:effective_ratio_source"`
	DiscountRatio        string `gorm:"size:80;column:discount_ratio"`
	DiscountRatioState   string `gorm:"size:16;column:discount_ratio_state"`
	EvidenceCapability   string `gorm:"size:32;column:evidence_capability"`
	BillingMode          string `gorm:"size:64;column:billing_mode"`
	OtherValid           bool   `gorm:"column:other_valid"`
	SourceRows           int64  `gorm:"column:source_rows"`
	EligibleRequests     int64  `gorm:"column:eligible_requests"`
	PromptTokens         int64  `gorm:"column:prompt_tokens"`
	CompletionTokens     int64  `gorm:"column:completion_tokens"`
	FinalQuota           int64  `gorm:"column:final_quota"`
	NonPositiveQuotaRows int64  `gorm:"column:nonpositive_quota_rows"`
	FirstSourceAt        int64  `gorm:"column:first_source_at"`
	LastSourceAt         int64  `gorm:"column:last_source_at"`
	FetchedAt            int64  `gorm:"column:fetched_at;index"`
}

func (ChannelUpstreamPricingHourEvidence) TableName() string {
	return "channel_upstream_pricing_hour_evidence"
}

type ChannelUpstreamPricingHourState struct {
	Domain           string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch     string `gorm:"primaryKey;size:64;column:account_epoch"`
	HourTs           int64  `gorm:"primaryKey;column:hour_ts"`
	SemanticsVersion int    `gorm:"primaryKey;column:semantics_version"`
	Status           string `gorm:"size:24;column:status;index"`
	ContentHash      string `gorm:"size:64;column:content_hash"`
	VerifiedScans    int    `gorm:"column:verified_scans"`
	SourceRows       int64  `gorm:"column:source_rows"`
	EligibleRequests int64  `gorm:"column:eligible_requests"`
	Tokens           int64  `gorm:"column:tokens"`
	FinalQuota       int64  `gorm:"column:final_quota"`
	EvidenceRows     int64  `gorm:"column:evidence_rows"`
	ReconcileStatus  string `gorm:"size:24;column:reconcile_status;index"`
	LegacyRequests   int64  `gorm:"column:legacy_requests"`
	LegacyTokens     int64  `gorm:"column:legacy_tokens"`
	LegacyQuota      int64  `gorm:"column:legacy_quota"`
	LegacyFetchedAt  int64  `gorm:"column:legacy_fetched_at"`
	RequestDelta     int64  `gorm:"column:request_delta"`
	TokenDelta       int64  `gorm:"column:token_delta"`
	QuotaDelta       int64  `gorm:"column:quota_delta"`
	LastError        string `gorm:"size:512;column:last_error"`
	CompletedAt      int64  `gorm:"column:completed_at"`
	ReconciledAt     int64  `gorm:"column:reconciled_at"`
	UpdatedAt        int64  `gorm:"column:updated_at;index"`
}

func (ChannelUpstreamPricingHourState) TableName() string {
	return "channel_upstream_pricing_hour_states"
}

type ChannelUpstreamPricingObservedState struct {
	Domain               string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch         string `gorm:"primaryKey;size:64;column:account_epoch"`
	SourceGroup          string `gorm:"primaryKey;size:191;column:source_group"`
	ModelName            string `gorm:"primaryKey;size:191;column:model_name"`
	SemanticsVersion     int    `gorm:"primaryKey;column:semantics_version"`
	Status               string `gorm:"size:24;column:status;index"`
	StateHash            string `gorm:"size:64;column:state_hash"`
	GroupRatio           string `gorm:"size:80;column:group_ratio"`
	GroupRatioState      string `gorm:"size:16;column:group_ratio_state"`
	UserGroupRatio       string `gorm:"size:80;column:user_group_ratio"`
	UserGroupRatioState  string `gorm:"size:16;column:user_group_ratio_state"`
	EffectiveRatio       string `gorm:"size:80;column:effective_ratio"`
	EffectiveRatioSource string `gorm:"size:32;column:effective_ratio_source"`
	DiscountRatio        string `gorm:"size:80;column:discount_ratio"`
	DiscountRatioState   string `gorm:"size:16;column:discount_ratio_state"`
	EvidenceCapability   string `gorm:"size:32;column:evidence_capability"`
	FirstObservedHour    int64  `gorm:"column:first_observed_hour"`
	LastObservedHour     int64  `gorm:"column:last_observed_hour;index"`
	EvidenceRequests     int64  `gorm:"column:evidence_requests"`
	UpdatedAt            int64  `gorm:"column:updated_at"`
}

func (ChannelUpstreamPricingObservedState) TableName() string {
	return "channel_upstream_pricing_observed_states"
}

type ChannelUpstreamPricingChangeEvent struct {
	ID                int64  `gorm:"primaryKey;autoIncrement;column:id"`
	EventKey          string `gorm:"size:64;uniqueIndex;column:event_key"`
	Domain            string `gorm:"size:253;index;column:domain"`
	AccountEpoch      string `gorm:"size:64;index;column:account_epoch"`
	SourceGroup       string `gorm:"size:191;column:source_group"`
	ModelName         string `gorm:"size:191;column:model_name"`
	SemanticsVersion  int    `gorm:"column:semantics_version"`
	PreviousStateHash string `gorm:"size:64;column:previous_state_hash"`
	CurrentStateHash  string `gorm:"size:64;column:current_state_hash"`
	PreviousRatio     string `gorm:"size:80;column:previous_ratio"`
	CurrentRatio      string `gorm:"size:80;column:current_ratio"`
	FirstObservedHour int64  `gorm:"column:first_observed_hour;index"`
	CreatedAt         int64  `gorm:"column:created_at"`
}

func (ChannelUpstreamPricingChangeEvent) TableName() string {
	return "channel_upstream_pricing_change_events"
}

// ChannelUpstreamPricingSyncState is intentionally independent from the
// existing usage cursor. Pricing failures may never make a healthy usage bill
// stale or move its history cursor.
type ChannelUpstreamPricingSyncState struct {
	Domain              string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch        string `gorm:"primaryKey;size:64;column:account_epoch"`
	SemanticsVersion    int    `gorm:"primaryKey;column:semantics_version"`
	Status              string `gorm:"size:24;column:status;index"`
	TailThroughHour     int64  `gorm:"column:tail_through_hour"`
	TailNextSyncAt      int64  `gorm:"column:tail_next_sync_at;index"`
	BackfillStartHour   int64  `gorm:"column:backfill_start_hour"`
	BackfillNextHour    int64  `gorm:"column:backfill_next_hour"`
	BackfillTargetHour  int64  `gorm:"column:backfill_target_hour"`
	BackfillDone        bool   `gorm:"column:backfill_done"`
	BackfillNextSyncAt  int64  `gorm:"column:backfill_next_sync_at;index"`
	ConsecutiveFailures int    `gorm:"column:consecutive_failures"`
	LastError           string `gorm:"size:512;column:last_error"`
	Progress            string `gorm:"size:256;column:progress"`
	VerifiedHours       int64  `gorm:"column:verified_hours"`
	PendingHours        int64  `gorm:"column:pending_hours"`
	MismatchHours       int64  `gorm:"column:mismatch_hours"`
	LastAttemptAt       int64  `gorm:"column:last_attempt_at"`
	LastSuccessAt       int64  `gorm:"column:last_success_at"`
	UpdatedAt           int64  `gorm:"column:updated_at;index"`
}

// ChannelUpstreamPricingPageCheckpoint is an independent, durable staging
// cursor for one dense pricing hour. AggregatesJSON contains only the same
// redacted group/model/rate aggregates as the final evidence table; it never
// stores raw log records, request content, IPs or credentials.
type ChannelUpstreamPricingPageCheckpoint struct {
	Domain               string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch         string `gorm:"primaryKey;size:64;column:account_epoch"`
	SemanticsVersion     int    `gorm:"primaryKey;column:semantics_version"`
	HourTs               int64  `gorm:"primaryKey;column:hour_ts"`
	Provider             string `gorm:"size:24;column:provider"`
	WindowSeconds        int64  `gorm:"column:window_seconds"`
	NextPage             int    `gorm:"column:next_page"`
	Total                int64  `gorm:"column:total"`
	SourceRows           int64  `gorm:"column:source_rows"`
	FirstPageFingerprint string `gorm:"size:64;column:first_page_fingerprint"`
	AggregatesJSON       string `gorm:"type:text;column:aggregates_json"`
	UpdatedAt            int64  `gorm:"column:updated_at;index"`
}

func (ChannelUpstreamPricingPageCheckpoint) TableName() string {
	return "channel_upstream_pricing_page_checkpoints"
}

func (ChannelUpstreamPricingSyncState) TableName() string {
	return "channel_upstream_pricing_sync_states"
}

func rawJSONInt64Exact(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("缺少整数")
	}
	text := strings.TrimSpace(string(raw))
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		text = strings.TrimSpace(text)
	}
	if text == "" || strings.ContainsAny(text, ".eE") {
		return 0, fmt.Errorf("不是精确整数")
	}
	value, ok := new(big.Int).SetString(text, 10)
	if !ok || !value.IsInt64() {
		return 0, fmt.Errorf("整数无效或溢出")
	}
	return value.Int64(), nil
}

func optionalRawJSONInt64Exact(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, true
	}
	value, err := rawJSONInt64Exact(raw)
	return value, err == nil
}

func canonicalPricingRatio(raw json.RawMessage, present bool) pricingRatioValue {
	if !present {
		return pricingRatioValue{State: pricingRatioMissing}
	}
	if len(raw) == 0 || string(raw) == "null" {
		return pricingRatioValue{State: pricingRatioNull}
	}
	text := strings.TrimSpace(string(raw))
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return pricingRatioValue{State: pricingRatioInvalid}
		}
		text = strings.TrimSpace(text)
	}
	value, ok := new(big.Rat).SetString(text)
	if !ok || value.Sign() < 0 {
		return pricingRatioValue{State: pricingRatioInvalid}
	}
	// 上游倍率实际精度远低于 30 位。保留足够的十进制精度并去尾零，
	// 同时用 RatString 参与 hash，避免 0.29 与 0.290000 被当成两个倍率。
	display := strings.TrimRight(strings.TrimRight(value.FloatString(30), "0"), ".")
	if display == "" {
		display = "0"
	}
	return pricingRatioValue{Text: display, Canonical: value.RatString(), State: pricingRatioValid, rat: value}
}

func boundedPricingDimension(value string) string {
	value = strings.TrimSpace(value)
	if utf8.RuneCountInString(value) <= 191 {
		return value
	}
	runes := []rune(value)
	return string(runes[:191])
}

func decodeNewAPIPricingOther(raw json.RawMessage) (group, user pricingRatioValue, billingMode string, valid bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return canonicalPricingRatio(nil, false), canonicalPricingRatio(nil, false), "", true
	}
	otherJSON := raw
	if raw[0] == '"' {
		var encoded string
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return pricingRatioValue{State: pricingRatioInvalid}, pricingRatioValue{State: pricingRatioInvalid}, "", false
		}
		if strings.TrimSpace(encoded) == "" {
			return canonicalPricingRatio(nil, false), canonicalPricingRatio(nil, false), "", true
		}
		otherJSON = []byte(encoded)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(otherJSON, &fields); err != nil || fields == nil {
		return pricingRatioValue{State: pricingRatioInvalid}, pricingRatioValue{State: pricingRatioInvalid}, "", false
	}
	groupRaw, groupPresent := fields["group_ratio"]
	userRaw, userPresent := fields["user_group_ratio"]
	group = canonicalPricingRatio(groupRaw, groupPresent)
	user = canonicalPricingRatio(userRaw, userPresent)
	if modeRaw, ok := fields["billing_mode"]; ok && len(modeRaw) > 0 && string(modeRaw) != "null" {
		_ = json.Unmarshal(modeRaw, &billingMode)
		billingMode = boundedPricingDimension(billingMode)
	}
	return group, user, billingMode, true
}

func effectiveNewAPIPricingRatio(group, user pricingRatioValue) (string, string) {
	if user.State == pricingRatioValid && user.rat != nil && user.rat.Sign() > 0 {
		return user.Text, "user_group_ratio"
	}
	if group.State == pricingRatioValid {
		return group.Text, "group_ratio"
	}
	return "", "unknown"
}

func decodeNewAPIUsageItem(itemJSON json.RawMessage) (newAPIPricingUsageItem, error) {
	var raw struct {
		CreatedAt        json.RawMessage `json:"created_at"`
		Quota            json.RawMessage `json:"quota"`
		PromptTokens     json.RawMessage `json:"prompt_tokens"`
		CompletionTokens json.RawMessage `json:"completion_tokens"`
		ModelName        string          `json:"model_name"`
		GroupName        string          `json:"group"`
		TokenID          json.RawMessage `json:"token_id"`
		Other            json.RawMessage `json:"other"`
	}
	if err := json.Unmarshal(itemJSON, &raw); err != nil {
		return newAPIPricingUsageItem{}, fmt.Errorf("NewAPI 使用日志条目无效: %w", err)
	}
	created, err := rawJSONNumber(raw.CreatedAt)
	if err != nil || created < 0 || created != math.Trunc(created) || created > math.MaxInt64 {
		return newAPIPricingUsageItem{}, fmt.Errorf("NewAPI 使用日志缺少有效 created_at")
	}
	_, err = rawJSONNumber(raw.Quota)
	if err != nil {
		return newAPIPricingUsageItem{}, fmt.Errorf("NewAPI 使用日志缺少有效 quota")
	}
	quotaExact, quotaExactErr := rawJSONInt64Exact(raw.Quota)
	prompt, promptExact := optionalRawJSONInt64Exact(raw.PromptTokens)
	completion, completionExact := optionalRawJSONInt64Exact(raw.CompletionTokens)
	if !promptExact {
		parsed, _ := rawJSONNumber(raw.PromptTokens)
		prompt = int64(parsed)
	}
	if !completionExact {
		parsed, _ := rawJSONNumber(raw.CompletionTokens)
		completion = int64(parsed)
	}
	tokenID, _ := rawJSONInt64Exact(raw.TokenID)
	groupRatio, userGroupRatio, billingMode, otherValid := decodeNewAPIPricingOther(raw.Other)
	effectiveRatio, effectiveSource := effectiveNewAPIPricingRatio(groupRatio, userGroupRatio)
	return newAPIPricingUsageItem{
		CreatedAt: int64(created), QuotaExact: quotaExact, QuotaExactKnown: quotaExactErr == nil,
		PromptTokens: prompt, CompletionTokens: completion, TokensExactKnown: promptExact && completionExact,
		Pricing: newAPIPricingAttributes{
			GroupName: boundedPricingDimension(raw.GroupName), ModelName: boundedPricingDimension(raw.ModelName), TokenID: tokenID,
			GroupRatio: groupRatio.Text, GroupRatioCanonical: groupRatio.Canonical, GroupRatioState: groupRatio.State,
			UserGroupRatio: userGroupRatio.Text, UserRatioCanonical: userGroupRatio.Canonical, UserGroupRatioState: userGroupRatio.State,
			EffectiveRatio: effectiveRatio, EffectiveRatioSource: effectiveSource,
			DiscountRatioState: pricingRatioMissing, EvidenceCapability: "full_rate",
			BillingMode: billingMode, OtherValid: otherValid,
		},
	}, nil
}

func newAPIUpstreamAccountEpoch(row ChannelUpstreamAccount) string {
	base := strings.TrimRight(strings.ToLower(strings.TrimSpace(row.BaseURL)), "/")
	if parsed, err := url.Parse(base); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		parsed.RawQuery, parsed.Fragment = "", ""
		base = parsed.String()
	}
	payload := strings.Join([]string{strings.ToLower(strings.TrimSpace(row.Provider)), base, strconv.FormatInt(row.UserID, 10), strings.ToLower(strings.TrimSpace(row.Account))}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func pricingDimensionHash(attributes newAPIPricingAttributes) string {
	parts := []string{
		attributes.GroupName, attributes.ModelName,
		attributes.GroupRatioState, attributes.GroupRatioCanonical,
		attributes.UserGroupRatioState, attributes.UserRatioCanonical,
		attributes.EffectiveRatioSource, attributes.EffectiveRatio, attributes.BillingMode,
		attributes.DiscountRatioState, attributes.DiscountCanonical, attributes.EvidenceCapability,
		strconv.FormatBool(attributes.OtherValid),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func pricingSourceContentHash(items []newAPIPricingUsageItem, from, to int64) (string, error) {
	rows := make([]string, 0, len(items))
	for _, item := range items {
		if item.CreatedAt < from || item.CreatedAt >= to {
			continue
		}
		if !item.QuotaExactKnown {
			return "", fmt.Errorf("NewAPI 计价账本要求 quota 为精确 int64")
		}
		if !item.TokensExactKnown {
			return "", fmt.Errorf("NewAPI 计价账本要求 token 数为精确 int64")
		}
		rows = append(rows, strings.Join([]string{
			strconv.FormatInt(item.CreatedAt, 10), strconv.FormatInt(item.QuotaExact, 10),
			strconv.FormatInt(item.PromptTokens, 10), strconv.FormatInt(item.CompletionTokens, 10),
			pricingDimensionHash(item.Pricing),
		}, "|"))
	}
	sort.Strings(rows)
	sum := sha256.Sum256([]byte(strings.Join(rows, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

func buildNewAPIPricingHour(row ChannelUpstreamAccount, items []newAPIPricingUsageItem, hourTs int64, fetchedAt int64) ([]ChannelUpstreamPricingHourEvidence, ChannelUpstreamPricingHourState, error) {
	if hourTs%3600 != 0 {
		return nil, ChannelUpstreamPricingHourState{}, fmt.Errorf("计价账本只允许发布整小时")
	}
	epoch := newAPIUpstreamAccountEpoch(row)
	byDimension := make(map[string]*ChannelUpstreamPricingHourEvidence)
	state := ChannelUpstreamPricingHourState{
		Domain: row.Domain, AccountEpoch: epoch, HourTs: hourTs,
		SemanticsVersion: upstreamPricingSemanticsVersion, Status: "observed",
		CompletedAt: fetchedAt, UpdatedAt: fetchedAt,
	}
	for _, item := range items {
		if item.CreatedAt < hourTs || item.CreatedAt >= hourTs+3600 {
			continue
		}
		if !item.QuotaExactKnown || !item.TokensExactKnown {
			return nil, ChannelUpstreamPricingHourState{}, fmt.Errorf("NewAPI 计价字段不是精确整数")
		}
		state.SourceRows++
		dimensionHash := pricingDimensionHash(item.Pricing)
		evidence := byDimension[dimensionHash]
		if evidence == nil {
			evidence = &ChannelUpstreamPricingHourEvidence{
				Domain: row.Domain, AccountEpoch: epoch, HourTs: hourTs,
				SemanticsVersion: upstreamPricingSemanticsVersion, DimensionHash: dimensionHash,
				Provider: row.Provider, SourceGroup: item.Pricing.GroupName, ModelName: item.Pricing.ModelName,
				GroupRatio:      item.Pricing.GroupRatio,
				GroupRatioState: item.Pricing.GroupRatioState, UserGroupRatio: item.Pricing.UserGroupRatio,
				UserGroupRatioState: item.Pricing.UserGroupRatioState, EffectiveRatio: item.Pricing.EffectiveRatio,
				EffectiveRatioSource: item.Pricing.EffectiveRatioSource, BillingMode: item.Pricing.BillingMode,
				DiscountRatio: item.Pricing.DiscountRatio, DiscountRatioState: item.Pricing.DiscountRatioState,
				EvidenceCapability: item.Pricing.EvidenceCapability,
				OtherValid:         item.Pricing.OtherValid, FirstSourceAt: item.CreatedAt, LastSourceAt: item.CreatedAt,
				FetchedAt: fetchedAt,
			}
			byDimension[dimensionHash] = evidence
		}
		evidence.SourceRows++
		if item.CreatedAt < evidence.FirstSourceAt {
			evidence.FirstSourceAt = item.CreatedAt
		}
		if item.CreatedAt > evidence.LastSourceAt {
			evidence.LastSourceAt = item.CreatedAt
		}
		if item.QuotaExact <= 0 {
			evidence.NonPositiveQuotaRows++
			continue
		}
		if item.QuotaExact > math.MaxInt64-evidence.FinalQuota || item.QuotaExact > math.MaxInt64-state.FinalQuota {
			return nil, ChannelUpstreamPricingHourState{}, fmt.Errorf("NewAPI 计价 quota 小时合计溢出")
		}
		if item.PromptTokens < 0 || item.CompletionTokens < 0 || item.PromptTokens > math.MaxInt64-item.CompletionTokens {
			return nil, ChannelUpstreamPricingHourState{}, fmt.Errorf("NewAPI 计价 token 无效或溢出")
		}
		itemTokens := item.PromptTokens + item.CompletionTokens
		if itemTokens > math.MaxInt64-state.Tokens {
			return nil, ChannelUpstreamPricingHourState{}, fmt.Errorf("NewAPI 计价 token 小时合计溢出")
		}
		evidence.EligibleRequests++
		evidence.PromptTokens += item.PromptTokens
		evidence.CompletionTokens += item.CompletionTokens
		evidence.FinalQuota += item.QuotaExact
		state.EligibleRequests++
		state.Tokens += itemTokens
		state.FinalQuota += item.QuotaExact
	}
	evidenceRows := make([]ChannelUpstreamPricingHourEvidence, 0, len(byDimension))
	for _, evidence := range byDimension {
		evidenceRows = append(evidenceRows, *evidence)
	}
	sort.Slice(evidenceRows, func(i, j int) bool { return evidenceRows[i].DimensionHash < evidenceRows[j].DimensionHash })
	state.EvidenceRows = int64(len(evidenceRows))
	state.ContentHash = pricingEvidenceContentHash(evidenceRows)
	return evidenceRows, state, nil
}

func pricingEvidenceContentHash(evidence []ChannelUpstreamPricingHourEvidence) string {
	rows := make([]string, 0, len(evidence))
	for _, row := range evidence {
		rows = append(rows, strings.Join([]string{
			row.DimensionHash, strconv.FormatInt(row.SourceRows, 10), strconv.FormatInt(row.EligibleRequests, 10),
			strconv.FormatInt(row.PromptTokens, 10), strconv.FormatInt(row.CompletionTokens, 10),
			strconv.FormatInt(row.FinalQuota, 10), strconv.FormatInt(row.NonPositiveQuotaRows, 10),
			strconv.FormatInt(row.FirstSourceAt, 10), strconv.FormatInt(row.LastSourceAt, 10),
		}, "|"))
	}
	sort.Strings(rows)
	sum := sha256.Sum256([]byte(strings.Join(rows, "\n")))
	return hex.EncodeToString(sum[:])
}

func pricingObservedStateHash(status string, evidence ChannelUpstreamPricingHourEvidence) string {
	parts := []string{status, evidence.GroupRatioState, evidence.GroupRatio, evidence.UserGroupRatioState, evidence.UserGroupRatio,
		evidence.EffectiveRatioSource, evidence.EffectiveRatio, evidence.DiscountRatioState, evidence.DiscountRatio, evidence.EvidenceCapability}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func pricingChangeEventKey(previous ChannelUpstreamPricingObservedState, currentHash string, hour int64) string {
	parts := []string{previous.Domain, previous.AccountEpoch, previous.SourceGroup, previous.ModelName, previous.StateHash, currentHash, strconv.FormatInt(hour, 10), strconv.Itoa(upstreamPricingSemanticsVersion)}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func updateUpstreamPricingObservedStatesTx(tx *gorm.DB, state ChannelUpstreamPricingHourState, evidence []ChannelUpstreamPricingHourEvidence, now int64) error {
	if state.Status != "verified" || (state.ReconcileStatus != "matched" && state.ReconcileStatus != "source_verified") {
		return nil
	}
	type key struct{ group, model string }
	grouped := make(map[key][]ChannelUpstreamPricingHourEvidence)
	for _, row := range evidence {
		if row.EligibleRequests <= 0 {
			continue
		}
		k := key{row.SourceGroup, row.ModelName}
		grouped[k] = append(grouped[k], row)
	}
	for k, rows := range grouped {
		selected := rows[0]
		status := "confirmed"
		stateHashes := make(map[string]bool)
		var evidenceRequests int64
		confirmable := true
		for _, candidate := range rows {
			// The hourly bill remains valid reconciliation evidence even when an
			// upstream version omits or corrupts `other`, but an unknown ratio must
			// never become the account's confirmed current pricing state. Treat one
			// invalid dimension in the same group/model/hour as ambiguity instead of
			// selecting the valid-looking subset.
			if !candidate.OtherValid ||
				(candidate.EvidenceCapability != "full_rate" && candidate.EvidenceCapability != "effective_rate" && candidate.EvidenceCapability != "discount_only") ||
				((candidate.EvidenceCapability == "full_rate" || candidate.EvidenceCapability == "effective_rate") && (candidate.EffectiveRatioSource == "unknown" || candidate.EffectiveRatio == "")) ||
				(candidate.EvidenceCapability == "discount_only" && (candidate.DiscountRatioState != pricingRatioValid || candidate.DiscountRatio == "")) {
				confirmable = false
			}
			stateHashes[pricingObservedStateHash("confirmed", candidate)] = true
			if candidate.EligibleRequests > math.MaxInt64-evidenceRequests {
				return fmt.Errorf("NewAPI 计价证据请求数溢出")
			}
			evidenceRequests += candidate.EligibleRequests
		}
		if !confirmable || len(stateHashes) != 1 {
			// Multiple effective ratios in the same group/model/hour are valid
			// evidence, but they cannot define one current rate. Keep the hourly
			// evidence and leave any previously confirmed current value untouched.
			continue
		}
		currentHash := pricingObservedStateHash(status, selected)
		var previous ChannelUpstreamPricingObservedState
		lookup := tx.Where("domain = ? AND account_epoch = ? AND source_group = ? AND model_name = ? AND semantics_version = ?",
			state.Domain, state.AccountEpoch, k.group, k.model, upstreamPricingSemanticsVersion).First(&previous)
		if lookup.Error != nil && !errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
			return lookup.Error
		}
		if lookup.Error == nil && state.HourTs < previous.LastObservedHour {
			continue
		}
		firstObserved := state.HourTs
		if lookup.Error == nil {
			if previous.StateHash == currentHash {
				firstObserved = previous.FirstObservedHour
			}
			if previous.StateHash != "" && previous.StateHash != currentHash && state.HourTs > previous.LastObservedHour {
				event := ChannelUpstreamPricingChangeEvent{
					EventKey: pricingChangeEventKey(previous, currentHash, state.HourTs), Domain: state.Domain,
					AccountEpoch: state.AccountEpoch, SourceGroup: k.group, ModelName: k.model,
					SemanticsVersion: upstreamPricingSemanticsVersion, PreviousStateHash: previous.StateHash,
					CurrentStateHash: currentHash, PreviousRatio: pricingObservedDisplay(previous),
					CurrentRatio: pricingEvidenceDisplay(selected), FirstObservedHour: state.HourTs, CreatedAt: now,
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&event).Error; err != nil {
					return err
				}
			}
		}
		observed := ChannelUpstreamPricingObservedState{
			Domain: state.Domain, AccountEpoch: state.AccountEpoch, SourceGroup: k.group, ModelName: k.model,
			SemanticsVersion: upstreamPricingSemanticsVersion, Status: status, StateHash: currentHash,
			GroupRatio: selected.GroupRatio, GroupRatioState: selected.GroupRatioState,
			UserGroupRatio: selected.UserGroupRatio, UserGroupRatioState: selected.UserGroupRatioState,
			EffectiveRatio: selected.EffectiveRatio, EffectiveRatioSource: selected.EffectiveRatioSource,
			DiscountRatio: selected.DiscountRatio, DiscountRatioState: selected.DiscountRatioState,
			EvidenceCapability: selected.EvidenceCapability,
			FirstObservedHour:  firstObserved, LastObservedHour: state.HourTs,
			EvidenceRequests: evidenceRequests, UpdatedAt: now,
		}
		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&observed).Error; err != nil {
			return err
		}
	}
	return nil
}

func pricingEvidenceDisplay(row ChannelUpstreamPricingHourEvidence) string {
	if row.EvidenceCapability == "discount_only" {
		return row.DiscountRatio
	}
	return row.EffectiveRatio
}

func pricingObservedDisplay(row ChannelUpstreamPricingObservedState) string {
	if row.EvidenceCapability == "discount_only" {
		return row.DiscountRatio
	}
	return row.EffectiveRatio
}

func legacyQuotaAsExact(value float64) (int64, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) || value < math.MinInt64 || value > math.MaxInt64 {
		return 0, false
	}
	return int64(value), true
}

func pricingLegacyQuotaAsExact(provider string, legacy ChannelUpstreamUsageHour) (int64, bool) {
	if provider == upstreamProviderNewAPI {
		return legacyQuotaAsExact(legacy.Quota)
	}
	value := legacy.CostUSD * 1_000_000
	if math.IsNaN(value) || math.IsInf(value, 0) || value < math.MinInt64 || value > math.MaxInt64 {
		return 0, false
	}
	return int64(math.Round(value)), true
}

func pricingReconcileAccepted(provider, status string) bool {
	if status == "matched" {
		return true
	}
	return provider == upstreamProviderAICodeWith && status == "source_verified"
}

// persistNewAPIPricingHour atomically replaces one complete shadow hour,
// records its reconciliation proof, advances no cursor itself, and never
// changes ChannelUpstreamUsageHour.
func (m *Monitor) persistNewAPIPricingHour(ctx context.Context, account ChannelUpstreamAccount, hourTs int64, evidence []ChannelUpstreamPricingHourEvidence, state ChannelUpstreamPricingHourState, now int64) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var previous ChannelUpstreamPricingHourState
		previousErr := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?",
			state.Domain, state.AccountEpoch, state.HourTs, state.SemanticsVersion).First(&previous).Error
		if previousErr != nil && !errors.Is(previousErr, gorm.ErrRecordNotFound) {
			return previousErr
		}
		state.VerifiedScans = 1
		if previousErr == nil && previous.ContentHash == state.ContentHash && previous.SourceRows == state.SourceRows &&
			previous.EligibleRequests == state.EligibleRequests && previous.Tokens == state.Tokens && previous.FinalQuota == state.FinalQuota {
			state.VerifiedScans = previous.VerifiedScans + 1
			if state.VerifiedScans >= 2 {
				state.Status = "verified"
			}
		}
		var legacy ChannelUpstreamUsageHour
		legacyErr := tx.Where("domain = ? AND hour_ts = ?", account.Domain, hourTs).First(&legacy).Error
		switch {
		case account.Provider == upstreamProviderAICodeWith:
			// AICodeWith's established usage bill is a natural-day aggregate,
			// while the details evidence is timestamped hourly. Require two
			// identical source scans here; day-total reconciliation is performed
			// independently and must never invent an hourly allocation.
			state.ReconcileStatus = "source_verified"
		case errors.Is(legacyErr, gorm.ErrRecordNotFound):
			state.ReconcileStatus = "legacy_missing"
		case legacyErr != nil:
			return legacyErr
		default:
			legacyQuota, comparable := pricingLegacyQuotaAsExact(account.Provider, legacy)
			if !comparable {
				state.ReconcileStatus = "legacy_inexact"
				break
			}
			state.LegacyRequests, state.LegacyTokens, state.LegacyQuota = legacy.Requests, legacy.Tokens, legacyQuota
			state.LegacyFetchedAt = legacy.FetchedAt
			state.RequestDelta = state.EligibleRequests - legacy.Requests
			state.TokenDelta = state.Tokens - legacy.Tokens
			state.QuotaDelta = state.FinalQuota - legacyQuota
			state.ReconciledAt = now
			if state.RequestDelta == 0 && state.TokenDelta == 0 && state.QuotaDelta == 0 {
				state.ReconcileStatus = "matched"
			} else {
				state.ReconcileStatus = "mismatch"
			}
		}
		if err := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?",
			state.Domain, state.AccountEpoch, state.HourTs, state.SemanticsVersion).Delete(&ChannelUpstreamPricingHourEvidence{}).Error; err != nil {
			return err
		}
		for i := range evidence {
			evidence[i].FetchedAt = now
			if err := tx.Create(&evidence[i]).Error; err != nil {
				return err
			}
		}
		state.UpdatedAt = now
		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&state).Error; err != nil {
			return err
		}
		if err := updateUpstreamPricingObservedStatesTx(tx, state, evidence, now); err != nil {
			return err
		}
		return tx.Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?",
			state.Domain, state.AccountEpoch, state.SemanticsVersion, state.HourTs).Delete(&ChannelUpstreamPricingPageCheckpoint{}).Error
	})
}

func newPricingSyncState(account ChannelUpstreamAccount, now int64, backfillDays int) ChannelUpstreamPricingSyncState {
	closedThrough := now - now%3600
	start := closedThrough - int64(backfillDays)*86400
	if start < 0 {
		start = 0
	}
	start -= start % 3600
	return ChannelUpstreamPricingSyncState{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account),
		SemanticsVersion: upstreamPricingSemanticsVersion, Status: "pending",
		BackfillStartHour: start, BackfillNextHour: start, BackfillTargetHour: closedThrough,
		TailNextSyncAt: now, BackfillNextSyncAt: now, UpdatedAt: now,
	}
}

func pricingSyncRetryAt(now int64, failures int) int64 {
	if failures < 1 {
		failures = 1
	}
	shift := failures - 1
	if shift > 5 {
		shift = 5
	}
	return now + int64(60*(1<<shift))
}

func pricingSyncIntervalSeconds(s Settings) int64 {
	minutes := upstreamUsageSyncMinutes(s)
	return int64(minutes * 60)
}

func pricingLedgerDomainAllowed(domains []string, domain string) bool {
	domain = strings.ToLower(strings.TrimSpace(domain))
	for _, configured := range domains {
		if strings.ToLower(strings.TrimSpace(configured)) == domain {
			return true
		}
	}
	return false
}

func pricingLedgerProviderSupported(provider string) bool {
	switch provider {
	case upstreamProviderNewAPI, upstreamProviderSub2API, upstreamProviderAICodeWith:
		return true
	default:
		return false
	}
}

func pricingLedgerCapabilityLabel(provider string) string {
	switch provider {
	case upstreamProviderNewAPI:
		return "标准/用户/生效倍率证据"
	case upstreamProviderSub2API:
		return "逐请求生效倍率证据"
	case upstreamProviderAICodeWith:
		return "折扣证据（不含基础倍率）"
	default:
		return "不支持"
	}
}

func pricingCheckpointProgress(checkpoint ChannelUpstreamPricingPageCheckpoint) string {
	pageCount := int((checkpoint.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
	completedPages := checkpoint.NextPage - 1
	if completedPages > pageCount {
		completedPages = pageCount
	}
	return fmt.Sprintf("%s 小时已安全保存 %d/%d 条（%d/%d 页）",
		time.Unix(checkpoint.HourTs, 0).In(cstLocation).Format("2006-01-02 15:00"),
		checkpoint.SourceRows, checkpoint.Total, completedPages, pageCount)
}

func addPricingCounter(target *int64, value int64) error {
	if value < 0 || *target < 0 || value > math.MaxInt64-*target {
		return fmt.Errorf("计价证据聚合溢出")
	}
	*target += value
	return nil
}

func mergePricingEvidenceRows(target map[string]*ChannelUpstreamPricingHourEvidence, rows []ChannelUpstreamPricingHourEvidence) error {
	for _, incoming := range rows {
		mapKey := pricingEvidenceMapKey(incoming)
		existing := target[mapKey]
		if existing == nil {
			copyRow := incoming
			target[mapKey] = &copyRow
			continue
		}
		if existing.Domain != incoming.Domain || existing.AccountEpoch != incoming.AccountEpoch || existing.HourTs != incoming.HourTs ||
			existing.SourceGroup != incoming.SourceGroup || existing.ModelName != incoming.ModelName ||
			existing.GroupRatio != incoming.GroupRatio || existing.GroupRatioState != incoming.GroupRatioState ||
			existing.UserGroupRatio != incoming.UserGroupRatio || existing.UserGroupRatioState != incoming.UserGroupRatioState ||
			existing.EffectiveRatio != incoming.EffectiveRatio || existing.EffectiveRatioSource != incoming.EffectiveRatioSource ||
			existing.DiscountRatio != incoming.DiscountRatio || existing.DiscountRatioState != incoming.DiscountRatioState ||
			existing.EvidenceCapability != incoming.EvidenceCapability ||
			existing.BillingMode != incoming.BillingMode || existing.OtherValid != incoming.OtherValid {
			return fmt.Errorf("计价证据维度摘要冲突")
		}
		for _, pair := range []struct{ target, value *int64 }{
			{&existing.SourceRows, &incoming.SourceRows}, {&existing.EligibleRequests, &incoming.EligibleRequests},
			{&existing.PromptTokens, &incoming.PromptTokens}, {&existing.CompletionTokens, &incoming.CompletionTokens},
			{&existing.FinalQuota, &incoming.FinalQuota}, {&existing.NonPositiveQuotaRows, &incoming.NonPositiveQuotaRows},
		} {
			if err := addPricingCounter(pair.target, *pair.value); err != nil {
				return err
			}
		}
		if incoming.FirstSourceAt < existing.FirstSourceAt {
			existing.FirstSourceAt = incoming.FirstSourceAt
		}
		if incoming.LastSourceAt > existing.LastSourceAt {
			existing.LastSourceAt = incoming.LastSourceAt
		}
		if incoming.FetchedAt > existing.FetchedAt {
			existing.FetchedAt = incoming.FetchedAt
		}
	}
	return nil
}

func pricingEvidenceMapKey(row ChannelUpstreamPricingHourEvidence) string {
	return strconv.FormatInt(row.HourTs, 10) + ":" + row.DimensionHash
}

func pricingEvidenceMapToSlice(rows map[string]*ChannelUpstreamPricingHourEvidence) []ChannelUpstreamPricingHourEvidence {
	out := make([]ChannelUpstreamPricingHourEvidence, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].HourTs != out[j].HourTs {
			return out[i].HourTs < out[j].HourTs
		}
		return out[i].DimensionHash < out[j].DimensionHash
	})
	return out
}

func pricingHourStateFromEvidence(account ChannelUpstreamAccount, hourTs int64, evidence []ChannelUpstreamPricingHourEvidence, now int64) (ChannelUpstreamPricingHourState, error) {
	state := ChannelUpstreamPricingHourState{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), HourTs: hourTs,
		SemanticsVersion: upstreamPricingSemanticsVersion, Status: "observed", CompletedAt: now, UpdatedAt: now,
		EvidenceRows: int64(len(evidence)), ContentHash: pricingEvidenceContentHash(evidence),
	}
	for _, row := range evidence {
		if err := addPricingCounter(&state.SourceRows, row.SourceRows); err != nil {
			return ChannelUpstreamPricingHourState{}, err
		}
		if err := addPricingCounter(&state.EligibleRequests, row.EligibleRequests); err != nil {
			return ChannelUpstreamPricingHourState{}, err
		}
		if row.PromptTokens > math.MaxInt64-row.CompletionTokens {
			return ChannelUpstreamPricingHourState{}, fmt.Errorf("计价证据 token 溢出")
		}
		if err := addPricingCounter(&state.Tokens, row.PromptTokens+row.CompletionTokens); err != nil {
			return ChannelUpstreamPricingHourState{}, err
		}
		if err := addPricingCounter(&state.FinalQuota, row.FinalQuota); err != nil {
			return ChannelUpstreamPricingHourState{}, err
		}
	}
	return state, nil
}

func (m *Monitor) savePricingPageCheckpoint(ctx context.Context, checkpoint *ChannelUpstreamPricingPageCheckpoint, evidence map[string]*ChannelUpstreamPricingHourEvidence) error {
	if len(evidence) > upstreamPricingMaxCheckpointDimensions {
		return fmt.Errorf("计价证据断点维度超过安全上限（%d）", upstreamPricingMaxCheckpointDimensions)
	}
	encoded, err := json.Marshal(pricingEvidenceMapToSlice(evidence))
	if err != nil {
		return err
	}
	if len(encoded) > upstreamPricingMaxCheckpointBytes {
		return fmt.Errorf("计价证据断点超过安全大小（%d bytes）", upstreamPricingMaxCheckpointBytes)
	}
	checkpoint.AggregatesJSON = string(encoded)
	checkpoint.UpdatedAt = time.Now().Unix()
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(checkpoint).Error
}

func (m *Monitor) deletePricingPageCheckpoint(ctx context.Context, domain, epoch string, hourTs int64) error {
	return m.storeDB.WithContext(ctx).
		Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?", domain, epoch, upstreamPricingSemanticsVersion, hourTs).
		Delete(&ChannelUpstreamPricingPageCheckpoint{}).Error
}

func decodePricingCheckpointEvidence(checkpoint ChannelUpstreamPricingPageCheckpoint) (map[string]*ChannelUpstreamPricingHourEvidence, error) {
	if len(checkpoint.AggregatesJSON) > upstreamPricingMaxCheckpointBytes {
		return nil, fmt.Errorf("计价证据断点超过安全大小（%d bytes）", upstreamPricingMaxCheckpointBytes)
	}
	var rows []ChannelUpstreamPricingHourEvidence
	if err := json.Unmarshal([]byte(checkpoint.AggregatesJSON), &rows); err != nil {
		return nil, fmt.Errorf("计价证据断点损坏: %w", err)
	}
	if len(rows) > upstreamPricingMaxCheckpointDimensions {
		return nil, fmt.Errorf("计价证据断点维度超过安全上限（%d）", upstreamPricingMaxCheckpointDimensions)
	}
	out := make(map[string]*ChannelUpstreamPricingHourEvidence, len(rows))
	for i := range rows {
		row := rows[i]
		if err := validatePricingCheckpointEvidenceRow(checkpoint, row); err != nil {
			return nil, fmt.Errorf("计价证据断点第 %d 行无效: %w", i+1, err)
		}
		mapKey := pricingEvidenceMapKey(row)
		if _, duplicate := out[mapKey]; duplicate {
			return nil, fmt.Errorf("计价证据断点维度重复")
		}
		copyRow := row
		out[mapKey] = &copyRow
	}
	var sourceRows int64
	for _, row := range out {
		if err := addPricingCounter(&sourceRows, row.SourceRows); err != nil {
			return nil, err
		}
	}
	if sourceRows != checkpoint.SourceRows {
		return nil, fmt.Errorf("计价证据断点行数不一致")
	}
	return out, nil
}

func validatePricingCheckpointEvidenceRow(checkpoint ChannelUpstreamPricingPageCheckpoint, row ChannelUpstreamPricingHourEvidence) error {
	windowSeconds := checkpoint.WindowSeconds
	if windowSeconds == 0 {
		windowSeconds = 3600
	}
	provider := checkpoint.Provider
	if provider == "" {
		provider = upstreamProviderNewAPI
	}
	if row.Domain != checkpoint.Domain || row.AccountEpoch != checkpoint.AccountEpoch || row.SemanticsVersion != checkpoint.SemanticsVersion ||
		row.HourTs < checkpoint.HourTs || row.HourTs >= checkpoint.HourTs+windowSeconds || row.HourTs%3600 != 0 {
		return fmt.Errorf("账户或时间窗口边界不一致")
	}
	if row.Provider != provider || !pricingLedgerProviderSupported(row.Provider) || len(row.DimensionHash) != 64 {
		return fmt.Errorf("来源或维度摘要无效")
	}
	for _, dimension := range []string{row.SourceGroup, row.ModelName, row.BillingMode} {
		if !utf8.ValidString(dimension) || boundedPricingDimension(dimension) != dimension {
			return fmt.Errorf("维度文本无效")
		}
	}
	if len(row.GroupRatio) > 80 || len(row.UserGroupRatio) > 80 || len(row.EffectiveRatio) > 80 || len(row.EffectiveRatioSource) > 32 || len(row.DiscountRatio) > 80 {
		return fmt.Errorf("倍率字段过长")
	}
	groupCanonical, err := persistedPricingRatioCanonical(row.GroupRatioState, row.GroupRatio)
	if err != nil {
		return err
	}
	userCanonical, err := persistedPricingRatioCanonical(row.UserGroupRatioState, row.UserGroupRatio)
	if err != nil {
		return err
	}
	discountCanonical, err := persistedPricingRatioCanonical(row.DiscountRatioState, row.DiscountRatio)
	if err != nil {
		return err
	}
	attributes := newAPIPricingAttributes{
		GroupName: row.SourceGroup, ModelName: row.ModelName,
		GroupRatio: row.GroupRatio, GroupRatioCanonical: groupCanonical, GroupRatioState: row.GroupRatioState,
		UserGroupRatio: row.UserGroupRatio, UserRatioCanonical: userCanonical, UserGroupRatioState: row.UserGroupRatioState,
		EffectiveRatio: row.EffectiveRatio, EffectiveRatioSource: row.EffectiveRatioSource, BillingMode: row.BillingMode,
		DiscountRatio: row.DiscountRatio, DiscountCanonical: discountCanonical, DiscountRatioState: row.DiscountRatioState,
		EvidenceCapability: row.EvidenceCapability,
		OtherValid:         row.OtherValid,
	}
	groupValue := pricingRatioValue{Text: row.GroupRatio, Canonical: groupCanonical, State: row.GroupRatioState}
	if groupCanonical != "" {
		groupValue.rat, _ = new(big.Rat).SetString(groupCanonical)
	}
	userValue := pricingRatioValue{Text: row.UserGroupRatio, Canonical: userCanonical, State: row.UserGroupRatioState}
	if userCanonical != "" {
		userValue.rat, _ = new(big.Rat).SetString(userCanonical)
	}
	switch row.Provider {
	case upstreamProviderNewAPI:
		expectedRatio, expectedSource := effectiveNewAPIPricingRatio(groupValue, userValue)
		if row.EffectiveRatio != expectedRatio || row.EffectiveRatioSource != expectedSource || row.EvidenceCapability != "full_rate" {
			return fmt.Errorf("生效倍率口径不一致")
		}
	case upstreamProviderSub2API:
		full := row.EvidenceCapability == "effective_rate" && row.GroupRatioState == pricingRatioMissing && row.EffectiveRatio != "" && row.EffectiveRatioSource == "rate_multiplier"
		costOnly := row.EvidenceCapability == "cost_only"
		if !full && !costOnly {
			return fmt.Errorf("Sub2API 倍率证据不完整")
		}
	case upstreamProviderAICodeWith:
		if row.DiscountRatioState != pricingRatioValid || row.EvidenceCapability != "discount_only" || row.EffectiveRatio != "" {
			return fmt.Errorf("AICodeWith 折扣证据不完整")
		}
	}
	if pricingDimensionHash(attributes) != row.DimensionHash {
		return fmt.Errorf("维度摘要校验失败")
	}
	partitionValid := row.EligibleRequests <= math.MaxInt64-row.NonPositiveQuotaRows
	if row.Provider == upstreamProviderNewAPI {
		partitionValid = partitionValid && row.EligibleRequests+row.NonPositiveQuotaRows == row.SourceRows
	} else {
		partitionValid = partitionValid && row.EligibleRequests == row.SourceRows && row.NonPositiveQuotaRows <= row.SourceRows
	}
	if row.SourceRows <= 0 || row.EligibleRequests < 0 || row.NonPositiveQuotaRows < 0 || !partitionValid || row.PromptTokens < 0 ||
		row.CompletionTokens < 0 || row.FinalQuota < 0 {
		return fmt.Errorf("聚合计数无效")
	}
	if row.FirstSourceAt < row.HourTs || row.LastSourceAt < row.FirstSourceAt || row.LastSourceAt >= row.HourTs+3600 {
		return fmt.Errorf("来源时间越界")
	}
	return nil
}

func persistedPricingRatioCanonical(state, display string) (string, error) {
	switch state {
	case pricingRatioValid:
		value, ok := new(big.Rat).SetString(display)
		if !ok || value.Sign() < 0 {
			return "", fmt.Errorf("倍率数值无效")
		}
		return value.RatString(), nil
	case pricingRatioMissing, pricingRatioNull, pricingRatioInvalid:
		if display != "" {
			return "", fmt.Errorf("非有效倍率不应包含数值")
		}
		return "", nil
	default:
		return "", fmt.Errorf("倍率状态无效")
	}
}

// fetchNewAPIPricingHour reads one complete hour through an independent
// durable page checkpoint. A normal request-budget yield returns complete=false
// without publishing evidence or incrementing failure counters.
func (m *Monitor) fetchNewAPIPricingHour(ctx context.Context, account ChannelUpstreamAccount, credential newAPICredential, hourTs int64, pacer *upstreamUsageRequestPacer, now int64) ([]ChannelUpstreamPricingHourEvidence, ChannelUpstreamPricingHourState, string, bool, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	var checkpoint ChannelUpstreamPricingPageCheckpoint
	loadErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?", account.Domain, epoch, upstreamPricingSemanticsVersion, hourTs).First(&checkpoint).Error
	if loadErr != nil && !errors.Is(loadErr, gorm.ErrRecordNotFound) {
		return nil, ChannelUpstreamPricingHourState{}, "", false, loadErr
	}
	hasCheckpoint := loadErr == nil
	if hasCheckpoint && (checkpoint.NextPage < 2 || checkpoint.Total < 0 || checkpoint.SourceRows < 0 || checkpoint.SourceRows > checkpoint.Total || len(checkpoint.FirstPageFingerprint) != 64) {
		if err := m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, hourTs); err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		hasCheckpoint = false
	}
	client := m.channelUpstreamHTTPClient()
	var first newAPIUsagePage[newAPIPricingUsageItem]
	firstLoaded, checkpointVerified := false, false
	if hasCheckpoint {
		page, err := fetchNewAPIPricingPage(ctx, client, account, credential, hourTs, hourTs+3600, 1, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return nil, ChannelUpstreamPricingHourState{}, pricingCheckpointProgress(checkpoint), false, nil
			}
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		first, firstLoaded = page, true
		if page.Total == checkpoint.Total && hex.EncodeToString(page.Fingerprint[:]) == checkpoint.FirstPageFingerprint {
			checkpointVerified = true
		} else {
			if err := m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, hourTs); err != nil {
				return nil, ChannelUpstreamPricingHourState{}, "", false, err
			}
			return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("NewAPI 计价日志断点期间首页已变化，窗口将重试")
		}
	}
	evidenceMap := make(map[string]*ChannelUpstreamPricingHourEvidence)
	if !hasCheckpoint {
		if !firstLoaded {
			page, err := fetchNewAPIPricingPage(ctx, client, account, credential, hourTs, hourTs+3600, 1, pacer)
			if err != nil {
				return nil, ChannelUpstreamPricingHourState{}, "", false, err
			}
			first = page
		}
		expectedFirst := int(first.Total)
		if expectedFirst > upstreamUsagePageSize {
			expectedFirst = upstreamUsagePageSize
		}
		if len(first.Items) != expectedFirst {
			return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("NewAPI 计价日志首页数量异常（got=%d want=%d）", len(first.Items), expectedFirst)
		}
		pageEvidence, pageState, err := buildNewAPIPricingHour(account, first.Items, hourTs, now)
		if err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		if pageState.SourceRows != int64(len(first.Items)) {
			return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("NewAPI 计价日志返回了窗口外条目")
		}
		if err := mergePricingEvidenceRows(evidenceMap, pageEvidence); err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		checkpoint = ChannelUpstreamPricingPageCheckpoint{
			Domain: account.Domain, AccountEpoch: epoch, SemanticsVersion: upstreamPricingSemanticsVersion,
			HourTs: hourTs, Provider: upstreamProviderNewAPI, WindowSeconds: 3600,
			NextPage: 2, Total: first.Total, SourceRows: pageState.SourceRows,
			FirstPageFingerprint: hex.EncodeToString(first.Fingerprint[:]),
		}
		if err := m.savePricingPageCheckpoint(ctx, &checkpoint, evidenceMap); err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
	} else {
		var err error
		evidenceMap, err = decodePricingCheckpointEvidence(checkpoint)
		if err != nil {
			// A corrupt local staging row must not become a permanent retry loop.
			// Delete only this independent shadow checkpoint; the published legacy
			// usage aggregate and any verified pricing hours remain untouched.
			if deleteErr := m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, hourTs); deleteErr != nil {
				return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("%v; 清理损坏断点失败: %w", err, deleteErr)
			}
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
	}
	pageCount := int((checkpoint.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
	for checkpoint.NextPage <= pageCount {
		pageNumber := checkpoint.NextPage
		page, err := fetchNewAPIPricingPage(ctx, client, account, credential, hourTs, hourTs+3600, pageNumber, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return nil, ChannelUpstreamPricingHourState{}, pricingCheckpointProgress(checkpoint), false, nil
			}
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		if err := validateNewAPIUsagePage(page, checkpoint.Total, pageNumber, pageCount); err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		pageEvidence, pageState, err := buildNewAPIPricingHour(account, page.Items, hourTs, now)
		if err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		if pageState.SourceRows != int64(len(page.Items)) {
			return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("NewAPI 计价日志返回了窗口外条目")
		}
		if err := mergePricingEvidenceRows(evidenceMap, pageEvidence); err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		checkpoint.SourceRows += pageState.SourceRows
		checkpoint.NextPage++
		if err := m.savePricingPageCheckpoint(ctx, &checkpoint, evidenceMap); err != nil {
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
	}
	if checkpoint.SourceRows != checkpoint.Total {
		return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("NewAPI 计价日志分页数量不完整（got=%d want=%d）", checkpoint.SourceRows, checkpoint.Total)
	}
	if pageCount > 1 && !checkpointVerified {
		probe, err := fetchNewAPIPricingPage(ctx, client, account, credential, hourTs, hourTs+3600, 1, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return nil, ChannelUpstreamPricingHourState{}, pricingCheckpointProgress(checkpoint), false, nil
			}
			return nil, ChannelUpstreamPricingHourState{}, "", false, err
		}
		if probe.Total != checkpoint.Total || hex.EncodeToString(probe.Fingerprint[:]) != checkpoint.FirstPageFingerprint {
			if err := m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, hourTs); err != nil {
				return nil, ChannelUpstreamPricingHourState{}, "", false, err
			}
			return nil, ChannelUpstreamPricingHourState{}, "", false, fmt.Errorf("NewAPI 计价日志扫描期间首页已变化，窗口将重试")
		}
	}
	evidence := pricingEvidenceMapToSlice(evidenceMap)
	state, err := pricingHourStateFromEvidence(account, hourTs, evidence, now)
	if err != nil {
		return nil, ChannelUpstreamPricingHourState{}, "", false, err
	}
	return evidence, state, "", true, nil
}

func (m *Monitor) loadOrCreatePricingSyncState(ctx context.Context, account ChannelUpstreamAccount, now int64) (ChannelUpstreamPricingSyncState, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	var state ChannelUpstreamPricingSyncState
	err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ?", account.Domain, epoch, upstreamPricingSemanticsVersion).First(&state).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = newPricingSyncState(account, now, upstreamUsageBackfillDays(m.cfg))
		if err := m.storeDB.WithContext(ctx).Create(&state).Error; err != nil {
			return state, err
		}
		return state, nil
	}
	return state, err
}

func (m *Monitor) savePricingSyncState(ctx context.Context, state *ChannelUpstreamPricingSyncState) error {
	state.UpdatedAt = time.Now().Unix()
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(state).Error
}

func (m *Monitor) refreshPricingSyncCounts(ctx context.Context, state *ChannelUpstreamPricingSyncState) error {
	var counts struct {
		Verified int64
		Pending  int64
		Mismatch int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamPricingHourState{}).
		Select(`COALESCE(SUM(CASE WHEN status='verified' AND reconcile_status IN ('matched','source_verified') THEN 1 ELSE 0 END),0) verified,
			COALESCE(SUM(CASE WHEN NOT (status='verified' AND reconcile_status IN ('matched','source_verified')) THEN 1 ELSE 0 END),0) pending,
			COALESCE(SUM(CASE WHEN reconcile_status IN ('mismatch','legacy_inexact','legacy_missing') THEN 1 ELSE 0 END),0) mismatch`).
		Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts >= ? AND hour_ts < ?",
			state.Domain, state.AccountEpoch, state.SemanticsVersion, state.BackfillStartHour, state.BackfillTargetHour).
		Scan(&counts).Error; err != nil {
		return err
	}
	state.VerifiedHours, state.PendingHours, state.MismatchHours = counts.Verified, counts.Pending, counts.Mismatch
	return nil
}

func (m *Monitor) pricingTailHourDue(ctx context.Context, account ChannelUpstreamAccount, closedThrough int64) (int64, bool, error) {
	start := closedThrough - 3*3600
	if start < 0 {
		start = 0
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	for hour := start; hour < closedThrough; hour += 3600 {
		var legacy ChannelUpstreamUsageHour
		legacyErr := m.storeDB.WithContext(ctx).Where("domain = ? AND hour_ts = ?", account.Domain, hour).First(&legacy).Error
		if errors.Is(legacyErr, gorm.ErrRecordNotFound) {
			continue
		}
		if legacyErr != nil {
			return 0, false, legacyErr
		}
		var observed ChannelUpstreamPricingHourState
		observedErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?",
			account.Domain, epoch, upstreamPricingSemanticsVersion, hour).First(&observed).Error
		if errors.Is(observedErr, gorm.ErrRecordNotFound) {
			return hour, true, nil
		}
		if observedErr != nil {
			return 0, false, observedErr
		}
		if observed.Status != "verified" || !pricingReconcileAccepted(account.Provider, observed.ReconcileStatus) ||
			(account.Provider != upstreamProviderAICodeWith && observed.LegacyFetchedAt != legacy.FetchedAt) {
			return hour, true, nil
		}
	}
	return 0, false, nil
}

func pricingReconcileBlockMessage(state ChannelUpstreamPricingHourState) string {
	return fmt.Sprintf("小时 %s 未对平（%s；请求差 %d，Token 差 %d，quota 差 %d）",
		time.Unix(state.HourTs, 0).In(cstLocation).Format("2006-01-02 15:00"),
		state.ReconcileStatus, state.RequestDelta, state.TokenDelta, state.QuotaDelta)
}

func (m *Monitor) syncStoredNewAPIPricing(ctx context.Context, domain string) (ChannelUpstreamPricingSyncState, error) {
	m.upstreamPricingMu.Lock()
	defer m.upstreamPricingMu.Unlock()
	now := time.Now().Unix()
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&account, "domain = ?", domain).Error; err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	if !account.Enabled || !account.UsageSyncEnabled || account.Provider != upstreamProviderNewAPI {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("该账户未启用 NewAPI 使用日志同步")
	}
	if !m.cfg.UpstreamPricingLedgerEnabled || !pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, account.Domain) {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("该域名未进入计价账本灰度名单")
	}
	credentialAny, err := m.credentialForAccount(account)
	if err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	credential, ok := credentialAny.(newAPICredential)
	if !ok {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("NewAPI 凭据格式无效")
	}
	state, err := m.loadOrCreatePricingSyncState(ctx, account, now)
	if err != nil {
		return state, err
	}
	closedThrough := now - now%3600
	// Backfill is a continuous archive cursor, not a one-time fixed target. If
	// the process is stopped for more than the three-hour tail overlap, extend
	// the target to the newest closed hour so the middle gap is still consumed
	// sequentially after restart.
	if closedThrough > state.BackfillTargetHour {
		state.BackfillTargetHour = closedThrough
		state.BackfillDone = state.BackfillNextHour >= state.BackfillTargetHour
		if !state.BackfillDone && state.BackfillNextSyncAt == 0 {
			state.BackfillNextSyncAt = now
		}
	}
	state.LastAttemptAt = now
	pacer := newUpstreamUsageRequestPacer(upstreamPricingMaxRequestsPerRun, upstreamUsageRequestInterval)
	runHour := func(hourTs int64) (ChannelUpstreamPricingHourState, string, bool, error) {
		evidence, hourState, progress, complete, fetchErr := m.fetchNewAPIPricingHour(ctx, account, credential, hourTs, pacer, now)
		if fetchErr != nil {
			return ChannelUpstreamPricingHourState{}, "", false, fetchErr
		}
		if !complete {
			return ChannelUpstreamPricingHourState{}, progress, false, nil
		}
		if persistErr := m.persistNewAPIPricingHour(ctx, account, hourTs, evidence, hourState, now); persistErr != nil {
			return ChannelUpstreamPricingHourState{}, "", false, persistErr
		}
		var published ChannelUpstreamPricingHourState
		queryErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?",
			account.Domain, newAPIUpstreamAccountEpoch(account), hourTs, upstreamPricingSemanticsVersion).First(&published).Error
		return published, "", true, queryErr
	}
	previousFailures := state.ConsecutiveFailures
	completedRead := false
	blocked := false
	if closedThrough >= 3600 && (state.TailNextSyncAt == 0 || state.TailNextSyncAt <= now) {
		var tailHour int64
		var due bool
		tailHour, due, err = m.pricingTailHourDue(ctx, account, closedThrough)
		if err == nil && due {
			var published ChannelUpstreamPricingHourState
			var progress string
			var complete bool
			published, progress, complete, err = runHour(tailHour)
			if err == nil && !complete {
				state.Status, state.Progress, state.LastError = "paging", progress, ""
				state.TailNextSyncAt, state.BackfillNextSyncAt = now+60, now+60
				blocked = true
			} else if err == nil {
				completedRead = true
				state.Progress = ""
				switch {
				case published.Status != "verified":
					state.Status, state.LastError = "pending_verification", "已完整读取，等待下一轮一致性复核"
					state.TailNextSyncAt, state.BackfillNextSyncAt = now+60, now+60
					blocked = true
				case published.ReconcileStatus != "matched":
					state.Status, state.LastError = "reconcile_blocked", pricingReconcileBlockMessage(published)
					retryAt := now + pricingSyncIntervalSeconds(m.cfg)
					state.TailNextSyncAt, state.BackfillNextSyncAt = retryAt, retryAt
					blocked = true
				default:
					if tailHour+3600 > state.TailThroughHour {
						state.TailThroughHour = tailHour + 3600
					}
					state.TailNextSyncAt = now + pricingSyncIntervalSeconds(m.cfg)
				}
			}
		} else if err == nil {
			state.TailNextSyncAt = now + pricingSyncIntervalSeconds(m.cfg)
		}
	}
	if err == nil && !blocked && !state.BackfillDone && (state.BackfillNextSyncAt == 0 || state.BackfillNextSyncAt <= now) {
		limit := m.cfg.UpstreamPricingBackfillHoursPerRun
		if limit < 1 {
			limit = 1
		}
		if limit > 6 {
			limit = 6
		}
		for completed := 0; completed < limit && state.BackfillNextHour < state.BackfillTargetHour; completed++ {
			var already ChannelUpstreamPricingHourState
			alreadyErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?",
				account.Domain, newAPIUpstreamAccountEpoch(account), upstreamPricingSemanticsVersion, state.BackfillNextHour).First(&already).Error
			if alreadyErr == nil && already.Status == "verified" && already.ReconcileStatus == "matched" {
				state.BackfillNextHour += 3600
				continue
			}
			if alreadyErr != nil && !errors.Is(alreadyErr, gorm.ErrRecordNotFound) {
				err = alreadyErr
				break
			}
			published, progress, complete, runErr := runHour(state.BackfillNextHour)
			if runErr != nil {
				err = runErr
				break
			}
			if !complete {
				state.Status, state.Progress, state.LastError = "paging", progress, ""
				state.BackfillNextSyncAt = now + 60
				blocked = true
				break
			}
			completedRead = true
			state.Progress = ""
			// A closed historical hour must produce the same complete content
			// twice and match the already published legacy aggregate before its
			// independent cursor may advance.
			if published.Status != "verified" || published.ReconcileStatus != "matched" {
				if published.Status != "verified" {
					state.Status, state.LastError = "pending_verification", "已完整读取，等待下一轮一致性复核"
					state.BackfillNextSyncAt = now + 60
				} else {
					state.Status, state.LastError = "reconcile_blocked", pricingReconcileBlockMessage(published)
					state.BackfillNextSyncAt = now + pricingSyncIntervalSeconds(m.cfg)
				}
				blocked = true
				break
			}
			state.BackfillNextHour += 3600
		}
		state.BackfillDone = state.BackfillNextHour >= state.BackfillTargetHour
		if state.BackfillDone {
			state.BackfillNextSyncAt = 0
		} else if !blocked {
			state.BackfillNextSyncAt = now + 60
		}
	}
	if err == nil && !blocked {
		state.LastError, state.Progress, state.ConsecutiveFailures = "", "", 0
		if state.BackfillDone {
			state.Status = "ok"
		} else {
			state.Status = "backfilling"
		}
	}
	if err != nil {
		state.Status = "error"
		state.LastError = sanitizeUpstreamErrorWithSecrets(err, upstreamCredentialSecrets(credential)...)
		state.ConsecutiveFailures = previousFailures + 1
		retryAt := pricingSyncRetryAt(now, state.ConsecutiveFailures)
		if upstreamAt := upstreamRetryAt(err); upstreamAt > retryAt {
			retryAt = upstreamAt
		}
		state.TailNextSyncAt = retryAt
		state.BackfillNextSyncAt = retryAt
	}
	if countErr := m.refreshPricingSyncCounts(ctx, &state); countErr != nil && err == nil {
		err = countErr
		state.Status = "error"
		state.LastError = sanitizeUpstreamError(countErr)
		state.ConsecutiveFailures = previousFailures + 1
		retryAt := pricingSyncRetryAt(now, state.ConsecutiveFailures)
		state.TailNextSyncAt = retryAt
		state.BackfillNextSyncAt = retryAt
	}
	if err == nil && completedRead {
		state.LastSuccessAt = now
		state.ConsecutiveFailures = 0
	}
	if saveErr := m.savePricingSyncState(ctx, &state); saveErr != nil {
		return state, saveErr
	}
	return state, err
}

func (m *Monitor) syncDueUpstreamPricing(ctx context.Context) {
	if !m.cfg.UpstreamPricingLedgerEnabled {
		return
	}
	now := time.Now().Unix()
	domains := append([]string(nil), m.cfg.UpstreamPricingLedgerDomains...)
	candidates := make([]upstreamPricingDueAccount, 0, len(domains))
	for _, domain := range domains {
		domain = strings.ToLower(strings.TrimSpace(domain))
		if domain == "" {
			continue
		}
		var account ChannelUpstreamAccount
		if err := m.storeDB.WithContext(ctx).Where("domain = ? AND enabled = ? AND usage_sync_enabled = ?", domain, true, true).First(&account).Error; err != nil || !pricingLedgerProviderSupported(account.Provider) {
			continue
		}
		epoch := newAPIUpstreamAccountEpoch(account)
		var state ChannelUpstreamPricingSyncState
		err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ?", domain, epoch, upstreamPricingSemanticsVersion).First(&state).Error
		due := errors.Is(err, gorm.ErrRecordNotFound)
		if err == nil {
			due = state.TailNextSyncAt == 0 || state.TailNextSyncAt <= now ||
				(!state.BackfillDone && (state.BackfillNextSyncAt == 0 || state.BackfillNextSyncAt <= now))
		}
		if due {
			nextDue := int64(0)
			if err == nil {
				nextDue = state.TailNextSyncAt
				if !state.BackfillDone && (nextDue == 0 || (state.BackfillNextSyncAt > 0 && state.BackfillNextSyncAt < nextDue)) {
					nextDue = state.BackfillNextSyncAt
				}
			}
			candidates = append(candidates, upstreamPricingDueAccount{Domain: domain, NextDueAt: nextDue, LastAttemptAt: state.LastAttemptAt})
		}
	}
	if len(candidates) == 0 {
		return
	}
	candidates = sortUpstreamPricingDueAccounts(candidates)
	syncCtx, cancel := context.WithTimeout(ctx, upstreamPricingOperationTimeout(m.cfg))
	_, syncErr := m.syncStoredUpstreamPricing(syncCtx, candidates[0].Domain)
	cancel()
	if syncErr != nil {
		// 影子账本故障只记录自身状态，绝不能污染既有消费汇总健康。
		slog.Warn("上游计价账本同步失败", "domain", candidates[0].Domain, "err", sanitizeUpstreamError(syncErr))
	}
	// 首期保持全局单并发，但按到期时间和最近尝试排序，不让固定域名占满每一轮。
}

func (m *Monitor) syncStoredUpstreamPricing(ctx context.Context, domain string) (ChannelUpstreamPricingSyncState, error) {
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Select("domain", "provider").First(&account, "domain = ?", domain).Error; err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	switch account.Provider {
	case upstreamProviderNewAPI:
		return m.syncStoredNewAPIPricing(ctx, domain)
	case upstreamProviderSub2API:
		return m.syncStoredSub2Pricing(ctx, domain)
	case upstreamProviderAICodeWith:
		return m.syncStoredAICodeWithPricing(ctx, domain)
	default:
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("上游类型 %s 不支持计价证据", upstreamProviderName(account.Provider))
	}
}

func upstreamPricingOperationTimeout(s Settings) time.Duration {
	// The shadow worker is restartable at page granularity. Bound its lock hold
	// time independently so failures or a dense upstream hour cannot starve the
	// primary balance/usage lanes.
	timeout := time.Duration(upstreamPricingMaxRequestsPerRun-1)*(upstreamGuardMinInterval+upstreamGuardMaxJitter) + 2*upstreamSyncTimeout(s)
	if timeout < 20*time.Second {
		return 20 * time.Second
	}
	if timeout > 45*time.Second {
		return 45 * time.Second
	}
	return timeout
}

type upstreamPricingDueAccount struct {
	Domain        string
	NextDueAt     int64
	LastAttemptAt int64
}

func sortUpstreamPricingDueAccounts(accounts []upstreamPricingDueAccount) []upstreamPricingDueAccount {
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].NextDueAt != accounts[j].NextDueAt {
			return accounts[i].NextDueAt < accounts[j].NextDueAt
		}
		if accounts[i].LastAttemptAt != accounts[j].LastAttemptAt {
			return accounts[i].LastAttemptAt < accounts[j].LastAttemptAt
		}
		return accounts[i].Domain < accounts[j].Domain
	})
	return accounts
}
