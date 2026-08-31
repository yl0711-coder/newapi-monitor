package monitor

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	upstreamProviderNewAPI     = "newapi"
	upstreamProviderSub2API    = "sub2api"
	upstreamProviderAICodeWith = "aicodewith"

	upstreamStatusPending     = "pending"
	upstreamStatusOK          = "ok"
	upstreamStatusError       = "error"
	upstreamStatusReconnect   = "reconnect"
	upstreamStatusDisabled    = "disabled"
	upstreamStatusUnsupported = "unsupported"

	maxChannelUpstreamBody    = 128 << 10
	maxUpstreamResponseBody   = 1 << 20
	maxAICodeWithKeyNameRunes = 64
	defaultNewAPIQuotaPerUSD  = 500000.0
	upstreamCredentialVersion = 1
	// 配置页面按行动态添加 Key，不以业务套餐数作人为限制。
	// 这里的 64 只是请求体/密文和单轮上游访问的安全边界，而不是 UI 固定槽位。
	maxAICodeWithAPIKeys = 64
	// 管理端一次只验证少量新增/替换 Key。更大的集合通过多次原子保存逐步加入，
	// 避免一个浏览器请求为了账户级 8 秒节流持续数分钟。
	maxAICodeWithKeyChangesPerSave = 4
)

// ChannelUpstreamAccount 是归并主域名对应的上游面板账户。
// Credential 只保存 AES-256-GCM 密文；任何 API 响应都不会返回该字段。
// BalanceKnown 明确区分“真实余额为 0”和“从未成功读取”，同步失败不会把旧余额覆盖成 0。
type ChannelUpstreamAccount struct {
	Domain            string  `gorm:"primaryKey;size:253;column:domain"`
	Provider          string  `gorm:"size:24;index"`
	BaseURL           string  `gorm:"size:512;column:base_url"`
	Account           string  `gorm:"size:320"`
	UserID            int64   `gorm:"column:user_id"`
	Credential        string  `gorm:"type:text"`
	CredentialVersion int     `gorm:"column:credential_version"`
	Enabled           bool    `gorm:"column:enabled"`
	BalanceUSD        float64 `gorm:"column:balance_usd"`
	BalanceKnown      bool    `gorm:"column:balance_known"`
	BalanceRaw        float64 `gorm:"column:balance_raw"`
	BalanceUnit       float64 `gorm:"column:balance_unit"`
	UnitAssumed       bool    `gorm:"column:unit_assumed"`
	Status            string  `gorm:"size:24;index"`
	LastError         string  `gorm:"size:512;column:last_error"`
	LastAttemptAt     int64   `gorm:"column:last_attempt_at"`
	LastSuccessAt     int64   `gorm:"column:last_success_at;index"`
	NextSyncAt        int64   `gorm:"column:next_sync_at;index"`
	ConsecutiveFails  int     `gorm:"column:consecutive_fails"`
	CreatedAt         int64   `gorm:"column:created_at"`
	UpdatedAt         int64   `gorm:"column:updated_at;index"`
	UpdatedBy         string  `gorm:"size:128;column:updated_by"`
	// 使用日志同步必须由管理员显式打开。它和余额快照独立：余额可用于预警，
	// 使用日志才可用于某个日期范围内的上游消费汇总。
	UsageSyncEnabled      bool   `gorm:"column:usage_sync_enabled"`
	UsageStatus           string `gorm:"size:24;column:usage_status"`
	UsageLastError        string `gorm:"size:512;column:usage_last_error"`
	UsageLastAttemptAt    int64  `gorm:"column:usage_last_attempt_at"`
	UsageLastSuccessAt    int64  `gorm:"column:usage_last_success_at;index"`
	UsageNextSyncAt       int64  `gorm:"column:usage_next_sync_at;index"`
	UsageConsecutiveFails int    `gorm:"column:usage_consecutive_fails"`
	// UsageAdapter records the verified provider capability selected for this
	// account. In particular, older Sub2API releases may only expose the daily
	// stats endpoint; remembering the fallback prevents probing a missing route
	// on every low-frequency sync.
	UsageAdapter string `gorm:"size:32;column:usage_adapter"`
	// 当天尾部刷新与历史回填分别退避。历史某一天异常不能拖慢当天数据，
	// 也不能在每次尾部刷新时无节制重试同一个高流量窗口。
	UsageBackfillLastAttemptAt    int64  `gorm:"column:usage_backfill_last_attempt_at"`
	UsageBackfillLastSuccessAt    int64  `gorm:"column:usage_backfill_last_success_at"`
	UsageBackfillNextSyncAt       int64  `gorm:"column:usage_backfill_next_sync_at;index"`
	UsageBackfillConsecutiveFails int    `gorm:"column:usage_backfill_consecutive_fails"`
	UsageBackfillLastError        string `gorm:"size:512;column:usage_backfill_last_error"`
	UsageBackfillProgress         string `gorm:"size:256;column:usage_backfill_progress"`
	// UsageBackfillCursor 是尚待补齐的时间窗口起点；0 表示初始化。NewAPI
	// 使用小时游标，只有完整小时原子发布后才前移；按日提供账单的供应商仍使用自然日游标。
	// UsageBackfillDone 只说明配置范围内的历史已完成，不表示今天的实时性。
	UsageBackfillCursor int64 `gorm:"column:usage_backfill_cursor"`
	UsageBackfillDone   bool  `gorm:"column:usage_backfill_done"`
	UsageDataUntil      int64 `gorm:"column:usage_data_until"`
}

// ChannelUpstreamUsageHour 是上游账户账单的本地脱敏汇总。BucketSeconds=3600
// 表示小时级聚合；AICodeWith 以及旧版 Sub2API 兼容接口只给出中国自然日聚合，
// 因此用 BucketSeconds=86400（当天尾部为已覆盖秒数）保留真实粒度，不伪造小时分布。
// 不保留 API Key、Cookie、请求体、响应体、用户内容或远端原始日志 ID；页面按日期范围
// 仅查询这里，绝不因用户刷新而访问上游。按供应商真实粒度重算能处理延迟入账，
// 又不依赖不可靠的跨版本日志 ID 去重。
type ChannelUpstreamUsageHour struct {
	Domain        string  `gorm:"primaryKey;size:253;column:domain"`
	HourTs        int64   `gorm:"primaryKey;column:hour_ts"`
	BucketSeconds int64   `gorm:"column:bucket_seconds"`
	Requests      int64   `gorm:"column:requests"`
	Tokens        int64   `gorm:"column:tokens"`
	Quota         float64 `gorm:"column:quota"`
	CostUSD       float64 `gorm:"column:cost_usd"`
	FetchedAt     int64   `gorm:"column:fetched_at;index"`
	Provider      string  `gorm:"size:24;column:provider"`
}

// NewAPIUsageBackfillCheckpoint 是 NewAPI 高密度历史小时的脱敏分页断点。
// 它只保存累计计数和首页指纹，不保存上游原始日志、用户内容或凭据。每页完成后
// 与 NextPage 一起原子落库；达到单轮请求预算或进程重启后可从下一页继续。
// 公共 ChannelUpstreamUsageHour 仍只会在整个小时复核完成后一次性发布。
type NewAPIUsageBackfillCheckpoint struct {
	Domain               string  `gorm:"primaryKey;size:253;column:domain"`
	WindowFrom           int64   `gorm:"column:window_from"`
	WindowTo             int64   `gorm:"column:window_to"`
	NextPage             int     `gorm:"column:next_page"`
	Total                int64   `gorm:"column:total"`
	SourceRows           int64   `gorm:"column:source_rows"`
	Requests             int64   `gorm:"column:requests"`
	Tokens               int64   `gorm:"column:tokens"`
	Quota                float64 `gorm:"column:quota"`
	FirstPageFingerprint string  `gorm:"size:64;column:first_page_fingerprint"`
	UpdatedAt            int64   `gorm:"column:updated_at;index"`
}

// NewAPIUsageBackfillSegment records one fully verified child window of a
// historical hour. Slow upstreams can time out while evaluating a whole hour;
// the worker then reads bounded five-minute children, but the public hourly
// aggregate is still published only after these rows form one continuous,
// gap-free cover of the parent hour. No source log or credential is retained.
type NewAPIUsageBackfillSegment struct {
	Domain               string  `gorm:"primaryKey;size:253;column:domain"`
	HourFrom             int64   `gorm:"primaryKey;column:hour_from"`
	SegmentFrom          int64   `gorm:"primaryKey;column:segment_from"`
	SegmentTo            int64   `gorm:"column:segment_to"`
	Status               string  `gorm:"size:16;column:status;index"`
	Total                int64   `gorm:"column:total"`
	SourceRows           int64   `gorm:"column:source_rows"`
	Requests             int64   `gorm:"column:requests"`
	Tokens               int64   `gorm:"column:tokens"`
	Quota                float64 `gorm:"column:quota"`
	FirstPageFingerprint string  `gorm:"size:64;column:first_page_fingerprint"`
	UpdatedAt            int64   `gorm:"column:updated_at;index"`
}

// AICodeWithKeySyncState keeps scheduling and health isolated per credential.
// The secret remains inside ChannelUpstreamAccount.Credential; this table only
// stores the opaque local slot and the remote numeric identity returned by the
// usage endpoint.
type AICodeWithKeySyncState struct {
	Domain                   string `gorm:"primaryKey;size:253;column:domain"`
	SlotID                   string `gorm:"primaryKey;size:96;column:slot_id"`
	CredentialSetVersion     string `gorm:"size:64;column:credential_set_version;index"`
	Ordinal                  int    `gorm:"column:ordinal"`
	Status                   string `gorm:"size:24;column:status;index"`
	SourceKeyID              int64  `gorm:"column:source_key_id"`
	LastError                string `gorm:"size:512;column:last_error"`
	LastAttemptAt            int64  `gorm:"column:last_attempt_at"`
	LastSuccessAt            int64  `gorm:"column:last_success_at"`
	NextSyncAt               int64  `gorm:"column:next_sync_at;index"`
	ConsecutiveFails         int    `gorm:"column:consecutive_fails"`
	TailRoundID              string `gorm:"size:96;column:tail_round_id;index"`
	BackfillCursor           int64  `gorm:"column:backfill_cursor"`
	BackfillRoundID          string `gorm:"size:96;column:backfill_round_id;index"`
	BackfillDone             bool   `gorm:"column:backfill_done"`
	BackfillLastSuccessAt    int64  `gorm:"column:backfill_last_success_at"`
	BackfillNextSyncAt       int64  `gorm:"column:backfill_next_sync_at;index"`
	BackfillConsecutiveFails int    `gorm:"column:backfill_consecutive_fails"`
	BackfillLastError        string `gorm:"size:512;column:backfill_last_error"`
	UpdatedAt                int64  `gorm:"column:updated_at;index"`
}

// AICodeWithUsageStage contains one key's result for an account-level round.
// Public ChannelUpstreamUsageHour rows are replaced only after every key in
// the frozen credential-set version has completed the same round.
type AICodeWithUsageStage struct {
	Domain               string  `gorm:"primaryKey;size:253;column:domain"`
	RoundID              string  `gorm:"primaryKey;size:96;column:round_id"`
	SlotID               string  `gorm:"primaryKey;size:96;column:slot_id"`
	HourTs               int64   `gorm:"primaryKey;column:hour_ts"`
	CredentialSetVersion string  `gorm:"size:64;column:credential_set_version;index"`
	BucketSeconds        int64   `gorm:"column:bucket_seconds"`
	Requests             int64   `gorm:"column:requests"`
	Tokens               int64   `gorm:"column:tokens"`
	Quota                float64 `gorm:"column:quota"`
	CostUSD              float64 `gorm:"column:cost_usd"`
	FetchedAt            int64   `gorm:"column:fetched_at"`
}

type AICodeWithUsageRound struct {
	Domain               string `gorm:"primaryKey;size:253;column:domain"`
	Kind                 string `gorm:"primaryKey;size:16;column:kind"`
	RoundID              string `gorm:"size:96;column:round_id;uniqueIndex"`
	CredentialSetVersion string `gorm:"size:64;column:credential_set_version;index"`
	WindowFrom           int64  `gorm:"column:window_from"`
	WindowTo             int64  `gorm:"column:window_to"`
	CompletedKeys        int    `gorm:"column:completed_keys"`
	TotalKeys            int    `gorm:"column:total_keys"`
	Status               string `gorm:"size:24;column:status;index"`
	CreatedAt            int64  `gorm:"column:created_at"`
	UpdatedAt            int64  `gorm:"column:updated_at;index"`
}

// ChannelUpstreamAccountView 是渠道管理页可见的脱敏状态。
type ChannelUpstreamAccountView struct {
	Configured                    bool                              `json:"configured"`
	Enabled                       bool                              `json:"enabled"`
	Provider                      string                            `json:"provider,omitempty"`
	ProviderName                  string                            `json:"provider_name,omitempty"`
	BaseURL                       string                            `json:"base_url,omitempty"`
	AccountMasked                 string                            `json:"account_masked,omitempty"`
	APIKeyCount                   int                               `json:"api_key_count,omitempty"`
	APIKeySlots                   []AICodeWithKeySlotView           `json:"api_key_slots,omitempty"`
	BalanceUSD                    *float64                          `json:"balance_usd,omitempty"`
	Currency                      string                            `json:"currency,omitempty"`
	UnitAssumed                   bool                              `json:"unit_assumed,omitempty"`
	Status                        string                            `json:"status,omitempty"`
	LastError                     string                            `json:"last_error,omitempty"`
	LastAttemptAt                 int64                             `json:"last_attempt_at,omitempty"`
	LastSuccessAt                 int64                             `json:"last_success_at,omitempty"`
	NextSyncAt                    int64                             `json:"next_sync_at,omitempty"`
	Assessment                    *ChannelUpstreamBalanceAssessment `json:"assessment,omitempty"`
	UsageSyncEnabled              bool                              `json:"usage_sync_enabled,omitempty"`
	UsageWorkerEnabled            bool                              `json:"usage_worker_enabled"`
	UsageStatus                   string                            `json:"usage_status,omitempty"`
	UsageEffectiveStatus          string                            `json:"usage_effective_status,omitempty"`
	UsageTailPhase                string                            `json:"usage_tail_phase,omitempty"`
	UsageHistoryPhase             string                            `json:"usage_history_phase,omitempty"`
	UsageFresh                    bool                              `json:"usage_fresh"`
	UsageLagSeconds               int64                             `json:"usage_lag_seconds,omitempty"`
	UsageFreshnessLimitSeconds    int64                             `json:"usage_freshness_limit_seconds,omitempty"`
	UsageLastError                string                            `json:"usage_last_error,omitempty"`
	UsageLastSuccessAt            int64                             `json:"usage_last_success_at,omitempty"`
	UsageNextSyncAt               int64                             `json:"usage_next_sync_at,omitempty"`
	UsageDataUntil                int64                             `json:"usage_data_until,omitempty"`
	UsageBackfillDone             bool                              `json:"usage_backfill_done,omitempty"`
	UsageConsecutiveFails         int                               `json:"usage_consecutive_fails,omitempty"`
	UsageBackfillLastSuccessAt    int64                             `json:"usage_backfill_last_success_at,omitempty"`
	UsageBackfillNextSyncAt       int64                             `json:"usage_backfill_next_sync_at,omitempty"`
	UsageBackfillConsecutiveFails int                               `json:"usage_backfill_consecutive_fails,omitempty"`
	UsageBackfillLastError        string                            `json:"usage_backfill_last_error,omitempty"`
	UsageBackfillProgress         string                            `json:"usage_backfill_progress,omitempty"`
	UsageLastAttemptAt            int64                             `json:"usage_last_attempt_at,omitempty"`
	UsageBackfillLastAttemptAt    int64                             `json:"usage_backfill_last_attempt_at,omitempty"`
	UsageBackfillCursor           int64                             `json:"usage_backfill_cursor,omitempty"`
	UsageAdapter                  string                            `json:"usage_adapter,omitempty"`
	UsageAdapterName              string                            `json:"usage_adapter_name,omitempty"`
	UsageGranularity              string                            `json:"usage_granularity,omitempty"`
	PricingLedgerWorkerEnabled    bool                              `json:"pricing_ledger_worker_enabled"`
	PricingLedgerEligible         bool                              `json:"pricing_ledger_eligible"`
	PricingLedgerCapability       string                            `json:"pricing_ledger_capability,omitempty"`
	PricingLedgerStatus           string                            `json:"pricing_ledger_status,omitempty"`
	PricingTailThroughHour        int64                             `json:"pricing_tail_through_hour,omitempty"`
	PricingBackfillStartHour      int64                             `json:"pricing_backfill_start_hour,omitempty"`
	PricingBackfillNextHour       int64                             `json:"pricing_backfill_next_hour,omitempty"`
	PricingBackfillTargetHour     int64                             `json:"pricing_backfill_target_hour,omitempty"`
	PricingBackfillTotalHours     int64                             `json:"pricing_backfill_total_hours,omitempty"`
	PricingBackfillDone           bool                              `json:"pricing_backfill_done,omitempty"`
	PricingLastAttemptAt          int64                             `json:"pricing_last_attempt_at,omitempty"`
	PricingLastSuccessAt          int64                             `json:"pricing_last_success_at,omitempty"`
	PricingLastError              string                            `json:"pricing_last_error,omitempty"`
	PricingProgress               string                            `json:"pricing_progress,omitempty"`
	PricingVerifiedHours          int64                             `json:"pricing_verified_hours,omitempty"`
	PricingPendingHours           int64                             `json:"pricing_pending_hours,omitempty"`
	PricingMismatchHours          int64                             `json:"pricing_mismatch_hours,omitempty"`
	ErrorLogWorkerEnabled         bool                              `json:"error_log_worker_enabled"`
	ErrorLogSelected              bool                              `json:"error_log_selected"`
	ErrorLogStatus                string                            `json:"error_log_status,omitempty"`
	ErrorLogLastAttemptAt         int64                             `json:"error_log_last_attempt_at,omitempty"`
	ErrorLogLastSuccessAt         int64                             `json:"error_log_last_success_at,omitempty"`
	ErrorLogNextSyncAt            int64                             `json:"error_log_next_sync_at,omitempty"`
	ErrorLogCoverageFrom          int64                             `json:"error_log_coverage_from,omitempty"`
	ErrorLogSyncedUntil           int64                             `json:"error_log_synced_until,omitempty"`
	ErrorLogWindowFrom            int64                             `json:"error_log_window_from,omitempty"`
	ErrorLogWindowTo              int64                             `json:"error_log_window_to,omitempty"`
	ErrorLogRowsTotal             int64                             `json:"error_log_rows_total,omitempty"`
	ErrorLogConsecutiveFails      int                               `json:"error_log_consecutive_fails,omitempty"`
	ErrorLogLastError             string                            `json:"error_log_last_error,omitempty"`
	ErrorLogUnresolvedFields      string                            `json:"error_log_unresolved_fields,omitempty"`
}

// AICodeWithKeySlotView is the non-secret identity of one configured key.
// SlotID is generated locally and is safe to round-trip through the UI; the
// actual key and the upstream api_key_id are never returned by an API.
type AICodeWithKeySlotView struct {
	SlotID                   string `json:"slot_id"`
	Name                     string `json:"name,omitempty"`
	Label                    string `json:"label"`
	Status                   string `json:"status,omitempty"`
	LastError                string `json:"last_error,omitempty"`
	LastSuccessAt            int64  `json:"last_success_at,omitempty"`
	NextSyncAt               int64  `json:"next_sync_at,omitempty"`
	ConsecutiveFails         int    `json:"consecutive_fails,omitempty"`
	BackfillDone             bool   `json:"backfill_done,omitempty"`
	BackfillLastError        string `json:"backfill_last_error,omitempty"`
	BackfillLastSuccessAt    int64  `json:"backfill_last_success_at,omitempty"`
	BackfillNextSyncAt       int64  `json:"backfill_next_sync_at,omitempty"`
	BackfillConsecutiveFails int    `json:"backfill_consecutive_fails,omitempty"`
}

type channelUpstreamSaveInput struct {
	Domain            string                       `json:"domain"`
	Provider          string                       `json:"provider"`
	BaseURL           string                       `json:"base_url"`
	Enabled           *bool                        `json:"enabled"`
	UserID            int64                        `json:"user_id"`
	AccessToken       string                       `json:"access_token"`
	APIKey            string                       `json:"api_key"`
	APIKeys           []string                     `json:"api_keys"`
	AddAPIKeys        []string                     `json:"add_api_keys"`
	AddAPIKeySlots    []aicodeWithKeyAdditionInput `json:"add_api_key_slots"`
	RenameAPIKeySlots []aicodeWithKeyRenameInput   `json:"rename_api_key_slots"`
	RemoveAPIKeyIDs   []string                     `json:"remove_api_key_ids"`
	Email             string                       `json:"email"`
	Password          string                       `json:"password"`
	RefreshToken      string                       `json:"refresh_token"`
	UsageSyncEnabled  *bool                        `json:"usage_sync_enabled"`
}

type channelUpstreamSyncInput struct {
	Domain string `json:"domain"`
}

type channelUpstreamConfigView struct {
	Domain           string                     `json:"domain"`
	Provider         string                     `json:"provider,omitempty"`
	BaseURL          string                     `json:"base_url,omitempty"`
	Enabled          bool                       `json:"enabled"`
	UsageSyncEnabled bool                       `json:"usage_sync_enabled"`
	UserID           int64                      `json:"user_id,omitempty"`
	Email            string                     `json:"email,omitempty"`
	Account          ChannelUpstreamAccountView `json:"account"`
}

type newAPICredential struct {
	AccessToken string `json:"access_token"`
}

type sub2APICredential struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
}

type aiCodeWithCredential struct {
	APIKey  string                    `json:"api_key,omitempty"` // 只用于兼容早期单 Key 凭据。
	APIKeys []string                  `json:"api_keys,omitempty"`
	Slots   []aiCodeWithKeyCredential `json:"slots,omitempty"`
}

type aiCodeWithKeyCredential struct {
	SlotID string `json:"slot_id"`
	Name   string `json:"name,omitempty"`
	Secret string `json:"secret"`
}

type aicodeWithKeyAdditionInput struct {
	Name   string `json:"name"`
	APIKey string `json:"api_key"`
}

type aicodeWithKeyRenameInput struct {
	SlotID string `json:"slot_id"`
	Name   string `json:"name"`
}

type upstreamBalanceResult struct {
	BalanceUSD  float64
	BalanceRaw  float64
	BalanceUnit float64
	UnitAssumed bool
}

type upstreamHTTPError struct {
	Status  int
	Message string
	RetryAt int64
}

func (e *upstreamHTTPError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("上游返回 HTTP %d", e.Status)
	}
	return fmt.Sprintf("上游返回 HTTP %d：%s", e.Status, e.Message)
}

type upstreamStoredSyncError struct {
	message string
	retryAt int64
}

func (e *upstreamStoredSyncError) Error() string { return e.message }

type upstreamAuthError struct{ err error }

func (e *upstreamAuthError) Error() string { return e.err.Error() }
func (e *upstreamAuthError) Unwrap() error { return e.err }

func upstreamProviderName(provider string) string {
	switch provider {
	case upstreamProviderNewAPI:
		return "NewAPI"
	case upstreamProviderSub2API:
		return "Sub2API"
	case upstreamProviderAICodeWith:
		return "AICodeWith（春秋）"
	default:
		return provider
	}
}

func upstreamUsageAdapterName(provider, adapter string) string {
	switch adapter {
	case upstreamUsageAdapterNewAPILog:
		return "NewAPI 分页日志"
	case upstreamUsageAdapterSub2Trend:
		return "Sub2API 小时汇总"
	case upstreamUsageAdapterSub2Stats:
		return "Sub2API 单日汇总（兼容模式）"
	case upstreamUsageAdapterAICodeWith:
		return "AICodeWith 按 Key 日账单"
	}
	switch provider {
	case upstreamProviderNewAPI:
		return "NewAPI 分页日志"
	case upstreamProviderSub2API:
		return "Sub2API 账户用量"
	case upstreamProviderAICodeWith:
		return "AICodeWith 按 Key 日账单"
	default:
		return ""
	}
}

func upstreamUsageGranularity(provider, adapter string) string {
	if provider == upstreamProviderAICodeWith || adapter == upstreamUsageAdapterSub2Stats {
		return "day"
	}
	return "hour"
}

func upstreamSyncMinutes(s Settings) int {
	if s.UpstreamSyncMinutes <= 0 {
		return 5
	}
	if s.UpstreamSyncMinutes > 1440 {
		return 1440
	}
	return s.UpstreamSyncMinutes
}

func upstreamUsageSyncMinutes(s Settings) int {
	minutes := s.UpstreamUsageSyncMinutes
	if minutes <= 0 {
		minutes = 20
	}
	if minutes < 15 {
		minutes = 15
	}
	if minutes > 1440 {
		minutes = 1440
	}
	return minutes
}

func upstreamUsageBackfillDays(s Settings) int {
	days := s.UpstreamUsageBackfillDays
	if days <= 0 {
		days = 90
	}
	if days > 180 {
		days = 180
	}
	return days
}

func upstreamMaxConcurrency(s Settings) int {
	if s.UpstreamMaxConcurrency < 1 {
		return 1
	}
	if s.UpstreamMaxConcurrency > 2 {
		return 2
	}
	return s.UpstreamMaxConcurrency
}

func upstreamSyncTimeout(s Settings) time.Duration {
	seconds := s.UpstreamSyncTimeoutSec
	if seconds <= 0 {
		seconds = 15
	}
	if seconds < 3 {
		seconds = 3
	}
	if seconds > 60 {
		seconds = 60
	}
	return time.Duration(seconds) * time.Second
}

func upstreamSaveTimeout(s Settings, provider string, keyCount int) time.Duration {
	timeout := upstreamSyncTimeout(s) + 5*time.Second
	if provider == upstreamProviderAICodeWith && keyCount > 1 {
		// 保存只验证一把旧基准 Key 和最多四把变更 Key，并遵守
		// AICodeWith 账户级请求间隔；把可预知的等待纳入管理请求 deadline。
		timeout += time.Duration(keyCount-1) * aiCodeWithUsageRequestInterval
	}
	if timeout > 2*time.Minute {
		return 2 * time.Minute
	}
	return timeout
}

func sanitizeUpstreamError(err error) string {
	if err == nil {
		return ""
	}
	s := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\r", " "), "\n", " "))
	if len([]rune(s)) > 240 {
		s = string([]rune(s)[:240]) + "…"
	}
	return s
}

// sanitizeUpstreamErrorWithSecrets 在错误进入日志、本地库或 API 响应前移除凭据。
// 部分异常上游会把请求中的 token 或密码原样拼回错误消息，不能信任其错误正文。
func sanitizeUpstreamErrorWithSecrets(err error, secrets ...string) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return sanitizeUpstreamError(errors.New(message))
}

func normalizeAICodeWithAPIKeys(single string, values []string) ([]string, error) {
	all := append([]string(nil), values...)
	if strings.TrimSpace(single) != "" {
		all = append(all, single)
	}
	seen := make(map[string]bool, len(all))
	out := make([]string, 0, len(all))
	for _, value := range all {
		key := strings.TrimSpace(value)
		if key == "" || seen[key] {
			continue
		}
		if !strings.HasPrefix(key, "sk-acw-") || len(key) > 2048 {
			return nil, fmt.Errorf("AICodeWith API Key 格式应为 sk-acw-...")
		}
		seen[key] = true
		out = append(out, key)
	}
	if len(out) > maxAICodeWithAPIKeys {
		return nil, fmt.Errorf("AICodeWith API Key 最多配置 %d 把", maxAICodeWithAPIKeys)
	}
	sort.Strings(out)
	return out, nil
}

func newAICodeWithSlotID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "acw_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

func normalizeAICodeWithKeyName(value string) (string, error) {
	name := strings.TrimSpace(value)
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > maxAICodeWithKeyNameRunes {
		return "", fmt.Errorf("AICodeWith Key 名称最多 %d 个字符", maxAICodeWithKeyNameRunes)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("AICodeWith Key 名称不能包含控制字符")
		}
	}
	if strings.HasPrefix(strings.ToLower(name), "sk-acw-") {
		return "", fmt.Errorf("AICodeWith Key 名称不能填写密钥内容")
	}
	return name, nil
}

func normalizeAICodeWithKeyAdditions(values []aicodeWithKeyAdditionInput) ([]aicodeWithKeyAdditionInput, error) {
	out := make([]aicodeWithKeyAdditionInput, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		keys, err := normalizeAICodeWithAPIKeys("", []string{value.APIKey})
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return nil, fmt.Errorf("新增 AICodeWith Key 时必须填写密钥")
		}
		if seen[keys[0]] {
			return nil, fmt.Errorf("新增的 AICodeWith Key 重复")
		}
		name, err := normalizeAICodeWithKeyName(value.Name)
		if err != nil {
			return nil, err
		}
		seen[keys[0]] = true
		out = append(out, aicodeWithKeyAdditionInput{Name: name, APIKey: keys[0]})
	}
	return out, nil
}

func normalizeAICodeWithKeyRenames(values []aicodeWithKeyRenameInput) ([]aicodeWithKeyRenameInput, error) {
	out := make([]aicodeWithKeyRenameInput, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		id := strings.TrimSpace(value.SlotID)
		if id == "" || len(id) > 96 || !strings.HasPrefix(id, "acw_") {
			return nil, fmt.Errorf("待改名的 AICodeWith Key 标识无效")
		}
		if seen[id] {
			return nil, fmt.Errorf("AICodeWith Key 名称更新重复")
		}
		name, err := normalizeAICodeWithKeyName(value.Name)
		if err != nil {
			return nil, err
		}
		seen[id] = true
		out = append(out, aicodeWithKeyRenameInput{SlotID: id, Name: name})
	}
	return out, nil
}

func normalizeAICodeWithCredential(cred aiCodeWithCredential) (aiCodeWithCredential, error) {
	seenIDs := make(map[string]bool)
	seenSecrets := make(map[string]bool)
	slots := make([]aiCodeWithKeyCredential, 0, len(cred.Slots)+len(cred.APIKeys)+1)
	appendSlot := func(slot aiCodeWithKeyCredential) error {
		secret := strings.TrimSpace(slot.Secret)
		if secret == "" {
			return nil
		}
		if !strings.HasPrefix(secret, "sk-acw-") || len(secret) > 2048 {
			return fmt.Errorf("AICodeWith API Key 格式应为 sk-acw-...")
		}
		if seenSecrets[secret] {
			return nil
		}
		id := strings.TrimSpace(slot.SlotID)
		if id == "" {
			var err error
			id, err = newAICodeWithSlotID()
			if err != nil {
				return fmt.Errorf("生成 Key 标识失败: %w", err)
			}
		}
		if len(id) > 96 || !strings.HasPrefix(id, "acw_") || seenIDs[id] {
			return fmt.Errorf("AICodeWith Key 标识无效")
		}
		name, err := normalizeAICodeWithKeyName(slot.Name)
		if err != nil {
			return err
		}
		seenIDs[id], seenSecrets[secret] = true, true
		slots = append(slots, aiCodeWithKeyCredential{SlotID: id, Name: name, Secret: secret})
		return nil
	}
	for _, slot := range cred.Slots {
		if err := appendSlot(slot); err != nil {
			return aiCodeWithCredential{}, err
		}
	}
	legacy, err := normalizeAICodeWithAPIKeys(cred.APIKey, cred.APIKeys)
	if err != nil {
		return aiCodeWithCredential{}, err
	}
	for _, secret := range legacy {
		if err := appendSlot(aiCodeWithKeyCredential{Secret: secret}); err != nil {
			return aiCodeWithCredential{}, err
		}
	}
	if len(slots) > maxAICodeWithAPIKeys {
		return aiCodeWithCredential{}, fmt.Errorf("AICodeWith API Key 最多配置 %d 把", maxAICodeWithAPIKeys)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].SlotID < slots[j].SlotID })
	return aiCodeWithCredential{Slots: slots}, nil
}

func aiCodeWithCredentialKeys(cred aiCodeWithCredential) ([]string, error) {
	normalized, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(normalized.Slots))
	for _, slot := range normalized.Slots {
		keys = append(keys, slot.Secret)
	}
	return keys, nil
}

func applyAICodeWithKeyChanges(existing aiCodeWithCredential, additions, removals []string) (aiCodeWithCredential, error) {
	structured := make([]aicodeWithKeyAdditionInput, 0, len(additions))
	for _, secret := range additions {
		structured = append(structured, aicodeWithKeyAdditionInput{APIKey: secret})
	}
	return applyAICodeWithSlotChanges(existing, structured, nil, removals)
}

func applyAICodeWithSlotChanges(existing aiCodeWithCredential, additions []aicodeWithKeyAdditionInput, renames []aicodeWithKeyRenameInput, removals []string) (aiCodeWithCredential, error) {
	current, err := normalizeAICodeWithCredential(existing)
	if err != nil {
		return aiCodeWithCredential{}, err
	}
	remove := make(map[string]bool, len(removals))
	for _, id := range removals {
		id = strings.TrimSpace(id)
		if id == "" || len(id) > 96 || !strings.HasPrefix(id, "acw_") {
			return aiCodeWithCredential{}, fmt.Errorf("待删除的 AICodeWith Key 标识无效")
		}
		remove[id] = true
	}
	rename := make(map[string]string, len(renames))
	for _, update := range renames {
		id := strings.TrimSpace(update.SlotID)
		if id == "" || len(id) > 96 || !strings.HasPrefix(id, "acw_") {
			return aiCodeWithCredential{}, fmt.Errorf("待改名的 AICodeWith Key 标识无效")
		}
		if _, exists := rename[id]; exists {
			return aiCodeWithCredential{}, fmt.Errorf("AICodeWith Key 名称更新重复")
		}
		name, nameErr := normalizeAICodeWithKeyName(update.Name)
		if nameErr != nil {
			return aiCodeWithCredential{}, nameErr
		}
		rename[id] = name
	}
	remaining := make([]aiCodeWithKeyCredential, 0, len(current.Slots)+len(additions))
	found := make(map[string]bool, len(remove))
	renamed := make(map[string]bool, len(rename))
	for _, slot := range current.Slots {
		if remove[slot.SlotID] {
			found[slot.SlotID] = true
			continue
		}
		if name, ok := rename[slot.SlotID]; ok {
			slot.Name = name
			renamed[slot.SlotID] = true
		}
		remaining = append(remaining, slot)
	}
	for id := range remove {
		if !found[id] {
			return aiCodeWithCredential{}, fmt.Errorf("待删除的 AICodeWith Key 不存在")
		}
	}
	for id := range rename {
		if remove[id] {
			return aiCodeWithCredential{}, fmt.Errorf("不能同时删除并改名同一把 AICodeWith Key")
		}
		if !renamed[id] {
			return aiCodeWithCredential{}, fmt.Errorf("待改名的 AICodeWith Key 不存在")
		}
	}
	for _, addition := range additions {
		keys, keyErr := normalizeAICodeWithAPIKeys("", []string{addition.APIKey})
		if keyErr != nil {
			return aiCodeWithCredential{}, keyErr
		}
		if len(keys) == 0 {
			continue
		}
		name, nameErr := normalizeAICodeWithKeyName(addition.Name)
		if nameErr != nil {
			return aiCodeWithCredential{}, nameErr
		}
		remaining = append(remaining, aiCodeWithKeyCredential{Name: name, Secret: keys[0]})
	}
	result, err := normalizeAICodeWithCredential(aiCodeWithCredential{Slots: remaining})
	if err != nil {
		return aiCodeWithCredential{}, err
	}
	if len(result.Slots) == 0 {
		return aiCodeWithCredential{}, fmt.Errorf("AICodeWith 账户至少保留一把 API Key")
	}
	return result, nil
}

func upstreamCredentialSecrets(credential any) []string {
	switch cred := credential.(type) {
	case newAPICredential:
		return []string{cred.AccessToken}
	case *newAPICredential:
		if cred != nil {
			return []string{cred.AccessToken}
		}
	case sub2APICredential:
		return []string{cred.AccessToken, cred.RefreshToken}
	case *sub2APICredential:
		if cred != nil {
			return []string{cred.AccessToken, cred.RefreshToken}
		}
	case aiCodeWithCredential:
		keys, _ := aiCodeWithCredentialKeys(cred)
		return keys
	case *aiCodeWithCredential:
		if cred != nil {
			keys, _ := aiCodeWithCredentialKeys(*cred)
			return keys
		}
	}
	return nil
}

func maskUpstreamAccount(provider, account string, userID int64) string {
	switch provider {
	case upstreamProviderNewAPI:
		if userID > 0 {
			return fmt.Sprintf("用户 ID %d", userID)
		}
	case upstreamProviderSub2API:
		parts := strings.Split(strings.TrimSpace(account), "@")
		if len(parts) == 2 {
			name := []rune(parts[0])
			if len(name) > 2 {
				return string(name[:2]) + "***@" + parts[1]
			}
			return "***@" + parts[1]
		}
	case upstreamProviderAICodeWith:
		parts := strings.Split(account, ":")
		if len(parts) == 3 && parts[0] == "keys" {
			if count, err := strconv.Atoi(parts[1]); err == nil && count > 0 {
				return fmt.Sprintf("%d 把 API Key", count)
			}
		}
		return "API Key 已配置"
	}
	return "已配置"
}

func aiCodeWithAccountKeyCount(account string) int {
	parts := strings.Split(account, ":")
	if len(parts) != 3 || parts[0] != "keys" {
		return 0
	}
	count, err := strconv.Atoi(parts[1])
	if err != nil || count <= 0 || count > maxAICodeWithAPIKeys {
		return 0
	}
	return count
}

func aiCodeWithKeyIdentity(apiKeys []string) string {
	canonical := append([]string(nil), apiKeys...)
	sort.Strings(canonical)
	sum := sha256.Sum256([]byte(strings.Join(canonical, "\x00")))
	return fmt.Sprintf("keys:%d:%x", len(canonical), sum[:8])
}

func aiCodeWithCredentialIdentity(cred aiCodeWithCredential) (string, error) {
	keys, err := aiCodeWithCredentialKeys(cred)
	if err != nil {
		return "", err
	}
	if len(keys) == 0 {
		return "", fmt.Errorf("AICodeWith API Key 为空")
	}
	return aiCodeWithKeyIdentity(keys), nil
}

func aiCodeWithCredentialSetVersion(cred aiCodeWithCredential) (string, error) {
	normalized, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, slot := range normalized.Slots {
		_, _ = h.Write([]byte(slot.SlotID))
		_, _ = h.Write([]byte{0})
		sum := sha256.Sum256([]byte(slot.Secret))
		_, _ = h.Write(sum[:])
	}
	return fmt.Sprintf("acwv1-%x", h.Sum(nil)[:12]), nil
}

func (m *Monitor) aicodeWithSlotViews(ctx context.Context, row ChannelUpstreamAccount) []AICodeWithKeySlotView {
	if row.Provider != upstreamProviderAICodeWith {
		return nil
	}
	var states []AICodeWithKeySyncState
	_ = m.storeDB.WithContext(ctx).Where("domain = ?", row.Domain).Find(&states).Error
	return m.aicodeWithSlotViewsFromStates(row, states)
}

func (m *Monitor) aicodeWithSlotViewsFromStates(row ChannelUpstreamAccount, states []AICodeWithKeySyncState) []AICodeWithKeySlotView {
	if row.Provider != upstreamProviderAICodeWith {
		return nil
	}
	credential, err := m.credentialForAccount(row)
	if err != nil {
		return nil
	}
	cred, ok := credential.(aiCodeWithCredential)
	if !ok {
		return nil
	}
	normalized, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return nil
	}
	byID := make(map[string]AICodeWithKeySyncState, len(states))
	for _, state := range states {
		byID[state.SlotID] = state
	}
	views := make([]AICodeWithKeySlotView, 0, len(normalized.Slots))
	for i, slot := range normalized.Slots {
		state := byID[slot.SlotID]
		label := slot.Name
		if label == "" {
			label = fmt.Sprintf("Key %d", i+1)
		}
		views = append(views, AICodeWithKeySlotView{
			SlotID: slot.SlotID, Name: slot.Name, Label: label, Status: state.Status,
			LastError: state.LastError, LastSuccessAt: state.LastSuccessAt, NextSyncAt: state.NextSyncAt,
			ConsecutiveFails: state.ConsecutiveFails, BackfillDone: state.BackfillDone,
			BackfillLastError: state.BackfillLastError, BackfillLastSuccessAt: state.BackfillLastSuccessAt,
			BackfillNextSyncAt: state.BackfillNextSyncAt, BackfillConsecutiveFails: state.BackfillConsecutiveFails,
		})
	}
	return views
}

func upstreamAccountView(row ChannelUpstreamAccount) ChannelUpstreamAccountView {
	view := ChannelUpstreamAccountView{
		Configured: true, Enabled: row.Enabled, Provider: row.Provider,
		ProviderName: upstreamProviderName(row.Provider), BaseURL: row.BaseURL,
		AccountMasked: maskUpstreamAccount(row.Provider, row.Account, row.UserID),
		Currency:      "USD", UnitAssumed: row.UnitAssumed, Status: row.Status,
		LastError: row.LastError, LastAttemptAt: row.LastAttemptAt,
		LastSuccessAt: row.LastSuccessAt, NextSyncAt: row.NextSyncAt,
		UsageSyncEnabled: row.UsageSyncEnabled, UsageStatus: row.UsageStatus,
		UsageLastError: row.UsageLastError, UsageLastSuccessAt: row.UsageLastSuccessAt,
		UsageNextSyncAt: row.UsageNextSyncAt,
		UsageDataUntil:  row.UsageDataUntil, UsageBackfillDone: row.UsageBackfillDone,
		UsageLastAttemptAt:            row.UsageLastAttemptAt,
		UsageBackfillLastAttemptAt:    row.UsageBackfillLastAttemptAt,
		UsageBackfillCursor:           row.UsageBackfillCursor,
		UsageAdapter:                  row.UsageAdapter,
		UsageAdapterName:              upstreamUsageAdapterName(row.Provider, row.UsageAdapter),
		UsageGranularity:              upstreamUsageGranularity(row.Provider, row.UsageAdapter),
		UsageConsecutiveFails:         row.UsageConsecutiveFails,
		UsageBackfillLastSuccessAt:    row.UsageBackfillLastSuccessAt,
		UsageBackfillNextSyncAt:       row.UsageBackfillNextSyncAt,
		UsageBackfillConsecutiveFails: row.UsageBackfillConsecutiveFails,
		UsageBackfillLastError:        row.UsageBackfillLastError,
		UsageBackfillProgress:         row.UsageBackfillProgress,
	}
	if row.Provider == upstreamProviderAICodeWith {
		view.APIKeyCount = aiCodeWithAccountKeyCount(row.Account)
	}
	if row.BalanceKnown {
		balance := row.BalanceUSD
		view.BalanceUSD = &balance
	}
	return view
}

// decorateUpstreamUsageHealth turns persisted worker state into an
// authoritative presentation state. A successful run is not green forever:
// once its published watermark exceeds the bounded freshness window it is
// explicitly stale. The global gray switch is also visible instead of being
// mistaken for an account-level failure.
func decorateUpstreamUsageHealth(view *ChannelUpstreamAccountView, row ChannelUpstreamAccount, s Settings, now int64) {
	view.UsageWorkerEnabled = s.UpstreamUsageSyncEnabled
	limit := int64((2*upstreamUsageSyncMinutes(s) + 5) * 60)
	view.UsageFreshnessLimitSeconds = limit
	if row.UsageDataUntil > 0 && now > row.UsageDataUntil {
		view.UsageLagSeconds = now - row.UsageDataUntil
	}
	view.UsageFresh = row.UsageDataUntil > 0 && view.UsageLagSeconds <= limit
	switch {
	case !s.UpstreamUsageSyncEnabled:
		view.UsageEffectiveStatus = "global_off"
	case !row.Enabled || !row.UsageSyncEnabled:
		view.UsageEffectiveStatus = upstreamStatusDisabled
	case row.UsageStatus == upstreamStatusError || row.UsageStatus == upstreamStatusReconnect || row.UsageStatus == upstreamStatusUnsupported:
		view.UsageEffectiveStatus = row.UsageStatus
	case row.UsageLastAttemptAt == 0 && row.UsageLastSuccessAt == 0:
		// A newly enabled account can still carry the persisted legacy value
		// "disabled" until the scheduler selects it for the first time. Expose
		// that as a queue state instead of incorrectly telling operators that
		// the account is not enabled.
		view.UsageEffectiveStatus = "queued"
	case row.UsageStatus == upstreamStatusOK && !view.UsageFresh:
		view.UsageEffectiveStatus = "stale"
	default:
		view.UsageEffectiveStatus = row.UsageStatus
	}
	view.UsageTailPhase = view.UsageEffectiveStatus
	view.UsageHistoryPhase = upstreamUsageHistoryPhase(row, s)
}

// upstreamUsageHistoryPhase is deliberately independent from the realtime
// usage Tail. A historical page timeout must not make a fresh current-day
// watermark look unavailable, and a never-scheduled account must not be
// presented as if it were already backfilling.
func upstreamUsageHistoryPhase(row ChannelUpstreamAccount, s Settings) string {
	switch {
	case !s.UpstreamUsageSyncEnabled:
		return "global_off"
	case !row.Enabled || !row.UsageSyncEnabled:
		return upstreamStatusDisabled
	case row.UsageStatus == upstreamStatusError || row.UsageStatus == upstreamStatusReconnect || row.UsageStatus == upstreamStatusUnsupported:
		return "blocked"
	case row.UsageBackfillDone:
		return "complete"
	case row.UsageBackfillLastError != "":
		return "retry"
	case row.UsageBackfillLastAttemptAt == 0 && row.UsageBackfillLastSuccessAt == 0:
		return "queued"
	case row.UsageBackfillProgress != "":
		return "paging"
	default:
		return "backfilling"
	}
}

func (m *Monitor) channelUpstreamAccountView(row ChannelUpstreamAccount) ChannelUpstreamAccountView {
	view := upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, m.cfg, time.Now().Unix())
	return view
}

// syncAICodeWithBalanceSnapshot 用一把已经在保存时逐把验证过的 Key 读取账户级余额。
// AICodeWith 的 balance 是账户级快照，后台每 5 分钟重复请求所有 Key 不会增加信息量，
// 却会让同步耗时随 Key 数线性增长。首选 Key 若明确失效(401/403)，才依次尝试后续
// Key；网络、限流等账户级故障不会放大成最多 64 次请求。每把 Key 的独立有效性和
// 账单仍由使用量任务校验。
func syncAICodeWithBalanceSnapshot(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred aiCodeWithCredential) (upstreamBalanceResult, aiCodeWithCredential, error) {
	normalized, err := normalizeAICodeWithCredential(cred)
	keys, keyErr := aiCodeWithCredentialKeys(normalized)
	if err == nil {
		err = keyErr
	}
	if err != nil || len(keys) == 0 {
		if err == nil {
			err = fmt.Errorf("AICodeWith API Key 为空，请重新连接")
		}
		return upstreamBalanceResult{}, cred, &upstreamAuthError{err: err}
	}
	pacer := newUpstreamUsageRequestPacer(len(normalized.Slots), aiCodeWithUsageRequestInterval)
	for index := range normalized.Slots {
		if paceErr := pacer.beforeRequest(ctx); paceErr != nil {
			return upstreamBalanceResult{}, normalized, paceErr
		}
		result, _, syncErr := syncAICodeWithBalance(ctx, client, row, aiCodeWithCredential{Slots: normalized.Slots[index : index+1]})
		if syncErr == nil {
			return result, normalized, nil
		}
		var authErr *upstreamAuthError
		if !errors.As(syncErr, &authErr) {
			return upstreamBalanceResult{}, normalized, syncErr
		}
	}
	return upstreamBalanceResult{}, normalized, &upstreamAuthError{err: fmt.Errorf("AICodeWith 已配置的 %d 把 API Key 均无法读取余额，请检查或替换失效 Key", len(normalized.Slots))}
}

func (m *Monitor) loadChannelUpstreamViews(ctx context.Context) (map[string]ChannelUpstreamAccountView, error) {
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Find(&rows).Error; err != nil {
		return nil, err
	}
	var pricingStates []ChannelUpstreamPricingSyncState
	if m.cfg.UpstreamPricingLedgerEnabled {
		if err := m.storeDB.WithContext(ctx).Find(&pricingStates).Error; err != nil {
			return nil, err
		}
	}
	stateByAccount := make(map[string]ChannelUpstreamPricingSyncState, len(pricingStates))
	for _, state := range pricingStates {
		stateByAccount[state.Domain+"\x00"+state.AccountEpoch] = state
	}
	var keyStates []AICodeWithKeySyncState
	if err := m.storeDB.WithContext(ctx).Find(&keyStates).Error; err != nil {
		return nil, err
	}
	keyStatesByDomain := make(map[string][]AICodeWithKeySyncState)
	for _, state := range keyStates {
		keyStatesByDomain[state.Domain] = append(keyStatesByDomain[state.Domain], state)
	}
	var errorLogStates []UpstreamErrorLogSyncState
	if m.cfg.UpstreamErrorLogSyncEnabled {
		if err := m.storeDB.WithContext(ctx).Find(&errorLogStates).Error; err != nil {
			return nil, err
		}
	}
	errorLogStateByDomain := make(map[string]UpstreamErrorLogSyncState, len(errorLogStates))
	for _, state := range errorLogStates {
		errorLogStateByDomain[state.Domain] = state
	}
	out := make(map[string]ChannelUpstreamAccountView, len(rows))
	for _, row := range rows {
		view := m.channelUpstreamAccountView(row)
		// Key names and per-key checkpoints are local, non-secret operational
		// state. Including them here lets the unified sync page explain which
		// AICodeWith key is waiting or retrying without contacting the upstream.
		view.APIKeySlots = m.aicodeWithSlotViewsFromStates(row, keyStatesByDomain[row.Domain])
		view.PricingLedgerWorkerEnabled = m.cfg.UpstreamPricingLedgerEnabled
		view.PricingLedgerEligible = pricingLedgerAccountSupported(row) && pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, row.Domain)
		view.PricingLedgerCapability = pricingLedgerCapabilityLabel(row.Provider)
		key := row.Domain + "\x00" + newAPIUpstreamAccountEpoch(row)
		state, hasState := stateByAccount[key]
		switch {
		case !m.cfg.UpstreamPricingLedgerEnabled:
			view.PricingLedgerStatus = "global_off"
		case !view.PricingLedgerEligible:
			view.PricingLedgerStatus = "not_selected"
		case !row.Enabled || !row.UsageSyncEnabled:
			view.PricingLedgerStatus = upstreamStatusDisabled
		case !hasState:
			view.PricingLedgerStatus = upstreamStatusPending
		default:
			view.PricingLedgerStatus = state.Status
		}
		if hasState {
			view.PricingTailThroughHour = state.TailThroughHour
			view.PricingBackfillStartHour = state.BackfillStartHour
			view.PricingBackfillNextHour = state.BackfillNextHour
			view.PricingBackfillTargetHour = state.BackfillTargetHour
			if state.BackfillTargetHour > state.BackfillStartHour {
				view.PricingBackfillTotalHours = (state.BackfillTargetHour - state.BackfillStartHour) / 3600
			}
			view.PricingBackfillDone = state.BackfillDone
			view.PricingLastAttemptAt = state.LastAttemptAt
			view.PricingLastSuccessAt = state.LastSuccessAt
			view.PricingLastError = state.LastError
			view.PricingProgress = state.Progress
			view.PricingVerifiedHours = state.VerifiedHours
			view.PricingPendingHours = state.PendingHours
			view.PricingMismatchHours = state.MismatchHours
		}
		view.ErrorLogWorkerEnabled = m.cfg.UpstreamErrorLogSyncEnabled
		view.ErrorLogSelected = upstreamErrorLogDomainAllowed(m.cfg.UpstreamErrorLogDomains, row.Domain)
		errorState, hasErrorState := errorLogStateByDomain[row.Domain]
		switch {
		case !view.ErrorLogWorkerEnabled:
			view.ErrorLogStatus = "global_off"
		case !view.ErrorLogSelected:
			view.ErrorLogStatus = "not_selected"
		case !row.Enabled || !row.UsageSyncEnabled:
			view.ErrorLogStatus = upstreamStatusDisabled
		case !hasErrorState:
			view.ErrorLogStatus = upstreamStatusPending
		default:
			view.ErrorLogStatus = errorState.Status
		}
		if hasErrorState {
			view.ErrorLogLastAttemptAt = errorState.LastAttemptAt
			view.ErrorLogLastSuccessAt = errorState.LastSuccessAt
			view.ErrorLogNextSyncAt = errorState.NextSyncAt
			view.ErrorLogCoverageFrom = errorState.CoverageFrom
			view.ErrorLogSyncedUntil = errorState.SyncedUntil
			view.ErrorLogWindowFrom = errorState.WindowFrom
			view.ErrorLogWindowTo = errorState.WindowTo
			view.ErrorLogRowsTotal = errorState.RowsTotal
			view.ErrorLogConsecutiveFails = errorState.ConsecutiveFails
			view.ErrorLogLastError = errorState.LastError
			view.ErrorLogUnresolvedFields = errorState.UnresolvedFields
		}
		out[row.Domain] = view
	}
	return out, nil
}

func (m *Monitor) upstreamCredentialSecret() (string, error) {
	secret := strings.TrimSpace(m.cfg.UpstreamCredentialSecret)
	if secret == "" {
		secret = strings.TrimSpace(m.cfg.SessionSecret)
	}
	if secret == "" {
		return "", fmt.Errorf("未配置凭据加密密钥")
	}
	return secret, nil
}

func (m *Monitor) upstreamAEAD() (cipher.AEAD, error) {
	secret, err := m.upstreamCredentialSecret()
	if err != nil {
		return nil, err
	}
	return upstreamAEADForSecret(secret)
}

func upstreamAEADForSecret(secret string) (cipher.AEAD, error) {
	key := sha256.Sum256([]byte("newapi-monitor/channel-upstream/v1\x00" + secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// migrateLegacyUpstreamCredentialEncryption preserves accounts created before
// MONITOR_UPSTREAM_CREDENTIAL_SECRET was split from MONITOR_SESSION_SECRET.
// A row already decryptable by the new key is left untouched. Otherwise the
// complete set is decrypted with the legacy session key and re-sealed with the
// new key in one SQLite transaction; any ambiguous/corrupt row aborts startup
// instead of silently turning configured upstreams into reconnect state.
func (m *Monitor) migrateLegacyUpstreamCredentialEncryption() error {
	newSecret := strings.TrimSpace(m.cfg.UpstreamCredentialSecret)
	legacySecret := strings.TrimSpace(m.cfg.SessionSecret)
	if newSecret == "" || legacySecret == "" || newSecret == legacySecret || m.storeDB == nil {
		return nil
	}
	newAEAD, err := upstreamAEADForSecret(newSecret)
	if err != nil {
		return err
	}
	legacyAEAD, err := upstreamAEADForSecret(legacySecret)
	if err != nil {
		return err
	}
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		var rows []ChannelUpstreamAccount
		if err := tx.Where("credential_version = ? AND credential <> ''", upstreamCredentialVersion).Find(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			payload, err := base64.RawURLEncoding.DecodeString(row.Credential)
			if err != nil || len(payload) <= newAEAD.NonceSize() || newAEAD.NonceSize() != legacyAEAD.NonceSize() {
				return fmt.Errorf("上游 %s 的凭据密文无效，拒绝自动换钥", row.Domain)
			}
			aad := upstreamCredentialAAD(row.Domain, row.Provider)
			if _, err := newAEAD.Open(nil, payload[:newAEAD.NonceSize()], payload[newAEAD.NonceSize():], aad); err == nil {
				continue
			}
			plain, err := legacyAEAD.Open(nil, payload[:legacyAEAD.NonceSize()], payload[legacyAEAD.NonceSize():], aad)
			if err != nil {
				return fmt.Errorf("上游 %s 的凭据既不能用新密钥也不能用旧会话密钥解密，拒绝启动", row.Domain)
			}
			nonce := make([]byte, newAEAD.NonceSize())
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				return err
			}
			sealed := newAEAD.Seal(nil, nonce, plain, aad)
			for i := range plain {
				plain[i] = 0
			}
			rotated := make([]byte, 0, len(nonce)+len(sealed))
			rotated = append(rotated, nonce...)
			rotated = append(rotated, sealed...)
			encoded := base64.RawURLEncoding.EncodeToString(rotated)
			result := tx.Model(&ChannelUpstreamAccount{}).
				Where("domain = ? AND credential = ?", row.Domain, row.Credential).
				Updates(map[string]any{"credential": encoded, "updated_at": time.Now().Unix()})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("上游 %s 凭据换钥发生并发修改，拒绝提交", row.Domain)
			}
		}
		return nil
	})
}

// migrateAICodeWithCredentialSlots turns the legacy anonymous key array into
// stable opaque slots before the UI or scheduler can observe it. It is local,
// transactional per account, and performs no upstream request.
func (m *Monitor) migrateAICodeWithCredentialSlots() error {
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.Where("provider = ?", upstreamProviderAICodeWith).Find(&rows).Error; err != nil {
		return err
	}
	for i := range rows {
		row := rows[i]
		var raw aiCodeWithCredential
		if err := m.openUpstreamCredential(row, &raw); err != nil {
			return fmt.Errorf("%s 凭据无法迁移为 Key 槽位: %w", row.Domain, err)
		}
		normalized, err := normalizeAICodeWithCredential(raw)
		if err != nil {
			return fmt.Errorf("%s 凭据无法迁移为 Key 槽位: %w", row.Domain, err)
		}
		identity, err := aiCodeWithCredentialIdentity(normalized)
		if err != nil {
			return err
		}
		row.Account = identity
		if err := m.sealUpstreamAccountCredential(&row, normalized); err != nil {
			return err
		}
		if err := m.persistAICodeWithAccountChange(context.Background(), &row, normalized, false, false); err != nil {
			return fmt.Errorf("%s Key 槽位迁移失败: %w", row.Domain, err)
		}
	}
	return nil
}

// migrateAICodeWithContractLedgerUnit reverses the short-lived CNY/7 rollout.
// AICodeWith's CNY field names its internal ledger currency, while the agreed
// business accounting basis is 1:1. The predicate on balance_unit makes the
// correction idempotent and confines it to rows written by that rollout.
func (m *Monitor) migrateAICodeWithContractLedgerUnit() error {
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.Where("provider = ? AND balance_unit > ? AND balance_unit < ?", upstreamProviderAICodeWith, 6.999, 7.001).Find(&rows).Error; err != nil {
		return err
	}
	for i := range rows {
		row := rows[i]
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", row.Domain).
				UpdateColumn("cost_usd", gorm.Expr("cost_usd * ?", row.BalanceUnit)).Error; err != nil {
				return err
			}
			if err := tx.Model(&AICodeWithUsageStage{}).Where("domain = ?", row.Domain).
				UpdateColumn("cost_usd", gorm.Expr("cost_usd * ?", row.BalanceUnit)).Error; err != nil {
				return err
			}
			updates := map[string]any{"balance_unit": 1.0, "unit_assumed": false}
			if row.BalanceKnown {
				updates["balance_usd"] = row.BalanceRaw
			}
			return tx.Model(&ChannelUpstreamAccount{}).
				Where("domain = ? AND provider = ? AND balance_unit > ? AND balance_unit < ?", row.Domain, upstreamProviderAICodeWith, 6.999, 7.001).
				Updates(updates).Error
		}); err != nil {
			return fmt.Errorf("%s AICodeWith 1:1 账面单位修正失败: %w", row.Domain, err)
		}
	}
	return nil
}

func upstreamCredentialAAD(domain, provider string) []byte {
	return []byte("channel-upstream:" + domain + ":" + provider + ":v1")
}

func (m *Monitor) sealUpstreamCredential(domain, provider string, credential any) (string, error) {
	plain, err := json.Marshal(credential)
	if err != nil {
		return "", err
	}
	aead, err := m.upstreamAEAD()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := aead.Seal(nil, nonce, plain, upstreamCredentialAAD(domain, provider))
	payload := make([]byte, 0, len(nonce)+len(sealed))
	payload = append(payload, nonce...)
	payload = append(payload, sealed...)
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func (m *Monitor) openUpstreamCredential(row ChannelUpstreamAccount, out any) error {
	if row.CredentialVersion != upstreamCredentialVersion || row.Credential == "" {
		return fmt.Errorf("凭据版本无效，请重新连接")
	}
	payload, err := base64.RawURLEncoding.DecodeString(row.Credential)
	if err != nil {
		return fmt.Errorf("凭据密文无效，请重新连接")
	}
	aead, err := m.upstreamAEAD()
	if err != nil {
		return err
	}
	if len(payload) <= aead.NonceSize() {
		return fmt.Errorf("凭据密文无效，请重新连接")
	}
	plain, err := aead.Open(nil, payload[:aead.NonceSize()], payload[aead.NonceSize():], upstreamCredentialAAD(row.Domain, row.Provider))
	if err != nil {
		return fmt.Errorf("无法解密凭据，请检查 MONITOR_UPSTREAM_CREDENTIAL_SECRET 或重新连接")
	}
	if err := json.Unmarshal(plain, out); err != nil {
		return fmt.Errorf("凭据内容无效，请重新连接")
	}
	return nil
}

func normalizeUpstreamBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 512 {
		return "", fmt.Errorf("上游地址无效")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("上游地址必须是完整且不含账号、查询参数或片段的 URL")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !(strings.EqualFold(u.Hostname(), "localhost") || ip != nil && ip.IsLoopback()) {
			return "", fmt.Errorf("上游地址必须使用 HTTPS")
		}
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func upstreamEndpoint(baseURL, endpoint string) string {
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(endpoint, "/")
}

// aicodeWithEndpoint keeps legacy account records working after AICodeWith
// moved its console API from aicodewith.com to aicodewith.ai. The rewrite is
// deliberately limited to the exact former official host; the shared HTTP
// client must continue rejecting redirects so credentials are never forwarded
// to a host selected by an upstream response.
func aicodeWithEndpoint(baseURL, endpoint string) string {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err == nil && strings.EqualFold(u.Hostname(), "aicodewith.com") {
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort("aicodewith.ai", port)
		} else {
			u.Host = "aicodewith.ai"
		}
		baseURL = u.String()
	}
	return upstreamEndpoint(baseURL, endpoint)
}

func newUpstreamHTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 8
	transport.MaxIdleConnsPerHost = 2
	transport.IdleConnTimeout = 60 * time.Second
	transport.TLSHandshakeTimeout = 8 * time.Second
	transport.ResponseHeaderTimeout = timeout
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			// API 端点不应重定向；禁止携带令牌跟随跳转，避免跨主机泄露 Authorization。
			return http.ErrUseLastResponse
		},
	}
}

func (m *Monitor) channelUpstreamHTTPClient() *http.Client {
	client := m.upstreamClient
	if client == nil {
		// 仅兼容直接构造 Monitor 的单元测试；生产实例均由 New 初始化并复用连接池。
		client = newUpstreamHTTPClient(upstreamSyncTimeout(m.cfg))
	}
	return installUpstreamHostGuardWithConcurrency(client, m.storeDB, upstreamMaxConcurrency(m.cfg))
}

func upstreamResponseMessage(body []byte) string {
	var payload struct {
		Message string          `json:"message"`
		Error   json.RawMessage `json:"error"`
		Detail  string          `json:"detail"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	message := payload.Message
	if message == "" && len(payload.Error) > 0 && string(payload.Error) != "null" {
		if payload.Error[0] == '"' {
			_ = json.Unmarshal(payload.Error, &message)
		} else {
			var nested struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			}
			if json.Unmarshal(payload.Error, &nested) == nil {
				message = nested.Message
				if message == "" {
					message = nested.Type
				}
			}
		}
	}
	if message == "" {
		message = payload.Detail
	}
	return sanitizeUpstreamError(errors.New(message))
}

func doUpstreamJSON(ctx context.Context, client *http.Client, method, endpoint string, headers map[string]string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "NexusAPI-Monitor/1.0")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("连接上游失败: %w", err)
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxUpstreamResponseBody+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("读取上游响应失败: %w", err)
	}
	if len(data) > maxUpstreamResponseBody {
		return nil, fmt.Errorf("上游响应超过安全上限")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		retryAt := int64(0)
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAt = parseUpstreamRetryAfter(resp.Header.Get("Retry-After"), time.Now()).Unix()
		}
		return nil, &upstreamHTTPError{Status: resp.StatusCode, Message: upstreamResponseMessage(data), RetryAt: retryAt}
	}
	return data, nil
}

func rawJSONNumber(raw json.RawMessage) (float64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("缺少数值")
	}
	var number json.Number
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		number = json.Number(strings.TrimSpace(s))
	} else {
		if err := json.Unmarshal(raw, &number); err != nil {
			return 0, err
		}
	}
	value, err := number.Float64()
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("数值无效")
	}
	return value, nil
}

func decodeNewAPIData(body []byte, out any) error {
	var envelope struct {
		Success bool            `json:"success"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("NewAPI 响应格式无效")
	}
	if !envelope.Success {
		message := strings.TrimSpace(envelope.Message)
		if message == "" {
			message = "请求失败"
		}
		return fmt.Errorf("NewAPI：%s", sanitizeUpstreamError(errors.New(message)))
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("NewAPI 数据格式无效")
	}
	return nil
}

func syncNewAPIBalance(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential) (upstreamBalanceResult, newAPICredential, error) {
	if strings.TrimSpace(cred.AccessToken) == "" {
		return upstreamBalanceResult{}, cred, &upstreamAuthError{err: fmt.Errorf("NewAPI 访问令牌为空，请重新连接")}
	}
	headers := map[string]string{
		"Authorization": "Bearer " + cred.AccessToken,
		"New-Api-User":  strconv.FormatInt(row.UserID, 10),
	}
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/user/self"), headers, nil)
	if err != nil {
		var statusErr *upstreamHTTPError
		if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
			return upstreamBalanceResult{}, cred, &upstreamAuthError{err: err}
		}
		return upstreamBalanceResult{}, cred, err
	}
	var profile struct {
		Quota json.RawMessage `json:"quota"`
	}
	if err := decodeNewAPIData(body, &profile); err != nil {
		return upstreamBalanceResult{}, cred, err
	}
	quota, err := rawJSONNumber(profile.Quota)
	if err != nil {
		return upstreamBalanceResult{}, cred, fmt.Errorf("NewAPI 未返回有效账户余额")
	}
	unit, assumed := defaultNewAPIQuotaPerUSD, true
	if statusBody, statusErr := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/status"), nil, nil); statusErr == nil {
		var status struct {
			QuotaPerUnit json.RawMessage `json:"quota_per_unit"`
		}
		if decodeNewAPIData(statusBody, &status) == nil {
			if parsed, parseErr := rawJSONNumber(status.QuotaPerUnit); parseErr == nil && parsed > 0 {
				unit, assumed = parsed, false
			}
		}
	}
	if unit <= 0 {
		return upstreamBalanceResult{}, cred, fmt.Errorf("NewAPI 额度换算单位无效")
	}
	return upstreamBalanceResult{BalanceUSD: quota / unit, BalanceRaw: quota, BalanceUnit: unit, UnitAssumed: assumed}, cred, nil
}

func decodeAICodeWithBalance(body []byte) (float64, string, error) {
	var envelope struct {
		Balance          json.RawMessage `json:"balance"`
		AvailableBalance json.RawMessage `json:"available_balance"`
		RemainingBalance json.RawMessage `json:"remaining_balance"`
		Currency         string          `json:"currency"`
		Data             json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, "", fmt.Errorf("AICodeWith 余额响应格式无效")
	}
	if len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		var nested struct {
			Balance          json.RawMessage `json:"balance"`
			AvailableBalance json.RawMessage `json:"available_balance"`
			RemainingBalance json.RawMessage `json:"remaining_balance"`
			Currency         string          `json:"currency"`
		}
		if err := json.Unmarshal(envelope.Data, &nested); err != nil {
			return 0, "", fmt.Errorf("AICodeWith 余额数据格式无效")
		}
		if len(nested.Balance) > 0 {
			envelope.Balance = nested.Balance
		}
		if len(envelope.Balance) == 0 && len(nested.AvailableBalance) > 0 {
			envelope.AvailableBalance = nested.AvailableBalance
		}
		if len(envelope.Balance) == 0 && len(envelope.AvailableBalance) == 0 && len(nested.RemainingBalance) > 0 {
			envelope.RemainingBalance = nested.RemainingBalance
		}
		if nested.Currency != "" {
			envelope.Currency = nested.Currency
		}
	}
	currency := strings.ToUpper(strings.TrimSpace(envelope.Currency))
	if currency == "" {
		// 兼容春秋早期未返回 currency 的响应；当时接口金额口径为 USD。
		currency = "USD"
	}
	if currency != "USD" && currency != "CNY" {
		return 0, "", fmt.Errorf("AICodeWith 余额币种不受支持（%s）", sanitizeUpstreamError(errors.New(currency)))
	}
	raw := envelope.Balance
	if len(raw) == 0 {
		raw = envelope.AvailableBalance
	}
	if len(raw) == 0 {
		raw = envelope.RemainingBalance
	}
	balance, err := rawJSONNumber(raw)
	if err != nil {
		return 0, "", fmt.Errorf("AICodeWith 未返回有效余额")
	}
	return balance, currency, nil
}

func syncAICodeWithBalance(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred aiCodeWithCredential) (upstreamBalanceResult, aiCodeWithCredential, error) {
	return syncAICodeWithBalanceWithPacer(ctx, client, row, cred, nil)
}

func syncAICodeWithBalanceWithPacer(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred aiCodeWithCredential, pacer *upstreamUsageRequestPacer) (upstreamBalanceResult, aiCodeWithCredential, error) {
	normalized, keyErr := normalizeAICodeWithCredential(cred)
	keys, keysErr := aiCodeWithCredentialKeys(normalized)
	if keyErr == nil {
		keyErr = keysErr
	}
	if keyErr != nil {
		return upstreamBalanceResult{}, cred, &upstreamAuthError{err: keyErr}
	}
	if len(keys) == 0 {
		return upstreamBalanceResult{}, cred, &upstreamAuthError{err: fmt.Errorf("AICodeWith API Key 为空，请重新连接")}
	}
	var balanceRaw, balanceUnit float64
	var balanceCurrency string
	for index, apiKey := range keys {
		if err := pacer.beforeRequest(ctx); err != nil {
			return upstreamBalanceResult{}, cred, err
		}
		body, err := doUpstreamJSON(ctx, client, http.MethodGet, aicodeWithEndpoint(row.BaseURL, "/api/v1/balance"), map[string]string{
			"Authorization": "Bearer " + apiKey,
		}, nil)
		if err != nil {
			var statusErr *upstreamHTTPError
			if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
				return upstreamBalanceResult{}, cred, &upstreamAuthError{err: fmt.Errorf("第 %d 把 AICodeWith API Key: %w", index+1, err)}
			}
			return upstreamBalanceResult{}, cred, fmt.Errorf("第 %d 把 AICodeWith API Key: %w", index+1, err)
		}
		currentRaw, currentCurrency, err := decodeAICodeWithBalance(body)
		if err != nil {
			return upstreamBalanceResult{}, cred, fmt.Errorf("第 %d 把 AICodeWith API Key: %w", index+1, err)
		}
		// AICodeWith 的 CNY 是其站内账面额度币种。业务计价合同按 1:1
		// 对账，不能把它当作人民币兑美元再除以汇率；币种仍严格校验，
		// 余额与按 Key 使用金额则都以相同的 1:1 单位持久化。
		currentUnit := 1.0
		if index == 0 {
			balanceRaw, balanceUnit, balanceCurrency = currentRaw, currentUnit, currentCurrency
		} else if currentCurrency != balanceCurrency || math.Abs(currentRaw-balanceRaw) > 0.0001 {
			return upstreamBalanceResult{}, cred, fmt.Errorf("多把 AICodeWith API Key 返回的账户余额不一致，不能合并为同一主域名账户")
		}
	}
	return upstreamBalanceResult{
		BalanceUSD: balanceRaw / balanceUnit, BalanceRaw: balanceRaw, BalanceUnit: balanceUnit,
	}, normalized, nil
}

func aicodeWithSaveValidationCredential(existing *aiCodeWithCredential, next aiCodeWithCredential) (aiCodeWithCredential, int, error) {
	normalizedNext, err := normalizeAICodeWithCredential(next)
	if err != nil {
		return aiCodeWithCredential{}, 0, err
	}
	existingBySlot := make(map[string]aiCodeWithKeyCredential)
	if existing != nil {
		normalizedExisting, normalizeErr := normalizeAICodeWithCredential(*existing)
		if normalizeErr != nil {
			return aiCodeWithCredential{}, 0, normalizeErr
		}
		for _, slot := range normalizedExisting.Slots {
			existingBySlot[slot.SlotID] = slot
		}
	}
	changed := make([]aiCodeWithKeyCredential, 0, len(normalizedNext.Slots))
	unchanged := make([]aiCodeWithKeyCredential, 0, len(normalizedNext.Slots))
	for _, slot := range normalizedNext.Slots {
		previous, ok := existingBySlot[slot.SlotID]
		if ok && previous.Secret == slot.Secret {
			unchanged = append(unchanged, slot)
			continue
		}
		changed = append(changed, slot)
	}
	if len(changed) > maxAICodeWithKeyChangesPerSave {
		return aiCodeWithCredential{}, 0, fmt.Errorf("每次最多新增或替换 %d 把 AICodeWith API Key，请分批保存", maxAICodeWithKeyChangesPerSave)
	}
	if len(changed) == 0 {
		return aiCodeWithCredential{}, 0, nil
	}
	validation := make([]aiCodeWithKeyCredential, 0, len(changed)+1)
	// 用一把仍保留的已验证 Key 作为账户余额基准；不重扫其余旧 Key。
	if len(unchanged) > 0 {
		validation = append(validation, unchanged[0])
	}
	validation = append(validation, changed...)
	return aiCodeWithCredential{Slots: validation}, len(validation), nil
}

func decodeSub2APIData(body []byte, out any) error {
	var envelope struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("Sub2API 响应格式无效")
	}
	if envelope.Code != 0 {
		message := strings.TrimSpace(envelope.Message)
		if message == "" {
			message = fmt.Sprintf("错误码 %d", envelope.Code)
		}
		return fmt.Errorf("Sub2API：%s", sanitizeUpstreamError(errors.New(message)))
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("Sub2API 数据格式无效")
	}
	return nil
}

func loginSub2API(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, password string) (sub2APICredential, error) {
	body, err := doUpstreamJSON(ctx, client, http.MethodPost, upstreamEndpoint(row.BaseURL, "/api/v1/auth/login"), nil, map[string]string{
		"email": row.Account, "password": password,
	})
	if err != nil {
		return sub2APICredential{}, err
	}
	var auth struct {
		AccessToken  string          `json:"access_token"`
		RefreshToken string          `json:"refresh_token"`
		ExpiresIn    json.RawMessage `json:"expires_in"`
		Requires2FA  bool            `json:"requires_2fa"`
	}
	if err := decodeSub2APIData(body, &auth); err != nil {
		return sub2APICredential{}, err
	}
	if auth.Requires2FA {
		return sub2APICredential{}, fmt.Errorf("Sub2API 账号启用了二次验证，当前连接方式暂不支持")
	}
	if auth.AccessToken == "" || auth.RefreshToken == "" {
		return sub2APICredential{}, fmt.Errorf("Sub2API 登录未返回完整令牌")
	}
	expiresIn, err := rawJSONNumber(auth.ExpiresIn)
	if err != nil || expiresIn <= 0 {
		expiresIn = 3600
	}
	return sub2APICredential{AccessToken: auth.AccessToken, RefreshToken: auth.RefreshToken, ExpiresAt: time.Now().Unix() + int64(expiresIn)}, nil
}

func refreshSub2API(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential) (sub2APICredential, error) {
	if cred.RefreshToken == "" {
		return cred, &upstreamAuthError{err: fmt.Errorf("Sub2API 刷新令牌为空，请重新连接")}
	}
	body, err := doUpstreamJSON(ctx, client, http.MethodPost, upstreamEndpoint(row.BaseURL, "/api/v1/auth/refresh"), nil, map[string]string{
		"refresh_token": cred.RefreshToken,
	})
	if err != nil {
		var statusErr *upstreamHTTPError
		if errors.As(err, &statusErr) && statusErr.Status >= 400 && statusErr.Status < 500 {
			return cred, &upstreamAuthError{err: err}
		}
		return cred, err
	}
	var auth struct {
		AccessToken  string          `json:"access_token"`
		RefreshToken string          `json:"refresh_token"`
		ExpiresIn    json.RawMessage `json:"expires_in"`
	}
	if err := decodeSub2APIData(body, &auth); err != nil {
		return cred, err
	}
	if auth.AccessToken == "" || auth.RefreshToken == "" {
		return cred, fmt.Errorf("Sub2API 刷新未返回完整令牌")
	}
	expiresIn, err := rawJSONNumber(auth.ExpiresIn)
	if err != nil || expiresIn <= 0 {
		expiresIn = 3600
	}
	return sub2APICredential{AccessToken: auth.AccessToken, RefreshToken: auth.RefreshToken, ExpiresAt: time.Now().Unix() + int64(expiresIn)}, nil
}

// importSub2APISession supports Sub2API deployments whose interactive login is
// protected by Turnstile or another browser-only challenge. The administrator
// completes that challenge on the upstream site and imports only its refresh
// token; Monitor immediately rotates it through the upstream's official refresh
// endpoint and never persists the supplied token verbatim.
func importSub2APISession(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, refreshToken string) (sub2APICredential, error) {
	refreshToken = strings.TrimSpace(refreshToken)
	if refreshToken == "" {
		return sub2APICredential{}, fmt.Errorf("Sub2API Refresh Token 为空")
	}
	return refreshSub2API(ctx, client, row, sub2APICredential{RefreshToken: refreshToken})
}

func sub2APIProfile(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential) (upstreamBalanceResult, error) {
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/v1/user/profile"), map[string]string{
		"Authorization": "Bearer " + cred.AccessToken,
	}, nil)
	if err != nil {
		return upstreamBalanceResult{}, err
	}
	var profile struct {
		Balance json.RawMessage `json:"balance"`
	}
	if err := decodeSub2APIData(body, &profile); err != nil {
		return upstreamBalanceResult{}, err
	}
	balance, err := rawJSONNumber(profile.Balance)
	if err != nil {
		return upstreamBalanceResult{}, fmt.Errorf("Sub2API 未返回有效账户余额")
	}
	return upstreamBalanceResult{BalanceUSD: balance, BalanceRaw: balance, BalanceUnit: 1}, nil
}

func syncSub2APIBalance(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential) (upstreamBalanceResult, sub2APICredential, error) {
	refreshed := false
	if cred.AccessToken == "" || cred.ExpiresAt <= time.Now().Add(2*time.Minute).Unix() {
		var err error
		cred, err = refreshSub2API(ctx, client, row, cred)
		if err != nil {
			return upstreamBalanceResult{}, cred, err
		}
		refreshed = true
	}
	result, err := sub2APIProfile(ctx, client, row, cred)
	if err == nil {
		return result, cred, nil
	}
	var statusErr *upstreamHTTPError
	if !refreshed && errors.As(err, &statusErr) && statusErr.Status == http.StatusUnauthorized {
		updated, refreshErr := refreshSub2API(ctx, client, row, cred)
		if refreshErr != nil {
			return upstreamBalanceResult{}, cred, refreshErr
		}
		cred = updated
		result, err = sub2APIProfile(ctx, client, row, cred)
		if err == nil {
			return result, cred, nil
		}
	}
	if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
		return upstreamBalanceResult{}, cred, &upstreamAuthError{err: err}
	}
	return upstreamBalanceResult{}, cred, err
}

func (m *Monitor) syncUpstreamCredential(ctx context.Context, row ChannelUpstreamAccount, credential any) (upstreamBalanceResult, any, error) {
	client := m.channelUpstreamHTTPClient()
	switch row.Provider {
	case upstreamProviderNewAPI:
		cred, ok := credential.(newAPICredential)
		if !ok {
			return upstreamBalanceResult{}, credential, fmt.Errorf("NewAPI 凭据格式无效")
		}
		result, updated, err := syncNewAPIBalance(ctx, client, row, cred)
		return result, updated, err
	case upstreamProviderSub2API:
		cred, ok := credential.(sub2APICredential)
		if !ok {
			return upstreamBalanceResult{}, credential, fmt.Errorf("Sub2API 凭据格式无效")
		}
		result, updated, err := syncSub2APIBalance(ctx, client, row, cred)
		return result, updated, err
	case upstreamProviderAICodeWith:
		cred, ok := credential.(aiCodeWithCredential)
		if !ok {
			return upstreamBalanceResult{}, credential, fmt.Errorf("AICodeWith 凭据格式无效")
		}
		result, updated, err := syncAICodeWithBalance(ctx, client, row, cred)
		return result, updated, err
	default:
		return upstreamBalanceResult{}, credential, fmt.Errorf("不支持的中转站类型")
	}
}

func nextUpstreamSyncAt(s Settings, domain string, now int64, failures int) int64 {
	minutes := upstreamSyncMinutes(s)
	if failures > 0 {
		shift := failures
		if shift > 4 {
			shift = 4
		}
		minutes *= 1 << shift
		if minutes > 60 {
			minutes = 60
		}
	}
	base := int64(minutes * 60)
	hash := sha256.Sum256([]byte(domain + ":" + strconv.FormatInt(now/base, 10)))
	jitterMax := base / 10
	if jitterMax > 30 {
		jitterMax = 30
	}
	var jitter int64
	if jitterMax > 0 {
		// The configured interval is a safety floor. Jitter may spread due
		// accounts later, but must never make an upstream request due early.
		jitter = int64(hash[0]) % (jitterMax + 1)
	}
	return now + base + jitter
}

func applyUpstreamSyncResult(row *ChannelUpstreamAccount, result upstreamBalanceResult, err error, now int64, s Settings, secrets ...string) {
	row.LastAttemptAt = now
	if err == nil {
		row.BalanceUSD = result.BalanceUSD
		row.BalanceRaw = result.BalanceRaw
		row.BalanceUnit = result.BalanceUnit
		row.UnitAssumed = result.UnitAssumed
		row.BalanceKnown = true
		row.Status = upstreamStatusOK
		row.LastError = ""
		row.LastSuccessAt = now
		row.ConsecutiveFails = 0
		row.NextSyncAt = nextUpstreamSyncAt(s, row.Domain, now, 0)
		return
	}
	row.ConsecutiveFails++
	row.LastError = sanitizeUpstreamErrorWithSecrets(err, secrets...)
	var authErr *upstreamAuthError
	if errors.As(err, &authErr) {
		row.Status = upstreamStatusReconnect
		row.NextSyncAt = upstreamAccountIsolatedUntil
	} else {
		row.Status = upstreamStatusError
		row.NextSyncAt = nextUpstreamSyncAt(s, row.Domain, now, row.ConsecutiveFails)
	}
	if retryAt := upstreamRetryAt(err); retryAt > row.NextSyncAt {
		row.NextSyncAt = retryAt
	}
}

func (m *Monitor) credentialForAccount(row ChannelUpstreamAccount) (any, error) {
	switch row.Provider {
	case upstreamProviderNewAPI:
		var cred newAPICredential
		return cred, m.openUpstreamCredential(row, &cred)
	case upstreamProviderSub2API:
		var cred sub2APICredential
		return cred, m.openUpstreamCredential(row, &cred)
	case upstreamProviderAICodeWith:
		var cred aiCodeWithCredential
		return cred, m.openUpstreamCredential(row, &cred)
	default:
		return nil, fmt.Errorf("不支持的中转站类型")
	}
}

func (m *Monitor) persistSyncedUpstreamAccount(ctx context.Context, row *ChannelUpstreamAccount, credential any) error {
	sealed, err := m.sealUpstreamCredential(row.Domain, row.Provider, credential)
	if err != nil {
		return err
	}
	row.Credential = sealed
	row.CredentialVersion = upstreamCredentialVersion
	return m.persistUpstreamAccount(ctx, row)
}

// persistUpstreamAccountIdentityChange makes the account identity and its local
// usage namespace one SQLite commit. A provider/base URL/account change must
// never become visible while rows attributed to the previous identity remain.
// The caller prepares (or preserves) the sealed credential before entering the
// transaction; no network access or secret handling occurs while SQLite is held.
func (m *Monitor) persistUpstreamAccountIdentityChange(ctx context.Context, row *ChannelUpstreamAccount, clearUsage, recoverErrorLogAuth bool) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "domain"}},
			UpdateAll: true,
		}).Create(row).Error; err != nil {
			return err
		}
		if clearUsage {
			if err := tx.Where("domain = ?", row.Domain).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillCheckpoint{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillSegment{}).Error; err != nil {
				return err
			}
		}
		return reconcileUpstreamErrorLogAccountChange(tx, row.Domain, clearUsage, recoverErrorLogAuth, row.UpdatedAt)
	})
}

func (m *Monitor) persistAICodeWithAccountChange(ctx context.Context, row *ChannelUpstreamAccount, cred aiCodeWithCredential, clearUsage, recoverErrorLogAuth bool) error {
	normalized, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return err
	}
	setVersion, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}}, UpdateAll: true}).Create(row).Error; err != nil {
			return err
		}
		var oldStates []AICodeWithKeySyncState
		if err := tx.Where("domain = ?", row.Domain).Find(&oldStates).Error; err != nil {
			return err
		}
		oldByID := make(map[string]AICodeWithKeySyncState, len(oldStates))
		oldVersion := ""
		for _, state := range oldStates {
			oldByID[state.SlotID] = state
			if oldVersion == "" {
				oldVersion = state.CredentialSetVersion
			}
		}
		versionChanged := oldVersion != "" && oldVersion != setVersion
		activeIDs := make([]string, 0, len(normalized.Slots))
		for i, slot := range normalized.Slots {
			activeIDs = append(activeIDs, slot.SlotID)
			state, exists := oldByID[slot.SlotID]
			if !exists || versionChanged {
				state = AICodeWithKeySyncState{Domain: row.Domain, SlotID: slot.SlotID, Status: upstreamStatusPending}
			}
			state.CredentialSetVersion, state.Ordinal, state.UpdatedAt = setVersion, i+1, now
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "domain"}, {Name: "slot_id"}}, UpdateAll: true,
			}).Create(&state).Error; err != nil {
				return err
			}
		}
		deleteQuery := tx.Where("domain = ?", row.Domain)
		if len(activeIDs) > 0 {
			deleteQuery = deleteQuery.Where("slot_id NOT IN ?", activeIDs)
		}
		if err := deleteQuery.Delete(&AICodeWithKeySyncState{}).Error; err != nil {
			return err
		}
		if versionChanged || len(oldStates) == 0 {
			if err := tx.Where("domain = ?", row.Domain).Delete(&AICodeWithUsageStage{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&AICodeWithUsageRound{}).Error; err != nil {
				return err
			}
		}
		if clearUsage {
			if err := tx.Where("domain = ?", row.Domain).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillCheckpoint{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillSegment{}).Error; err != nil {
				return err
			}
		}
		return reconcileUpstreamErrorLogAccountChange(tx, row.Domain, clearUsage, recoverErrorLogAuth, row.UpdatedAt)
	})
}

// reconcileUpstreamErrorLogAccountChange keeps the independently scheduled
// error-evidence lane consistent with an administrator's account change.
//
// A provider/base URL/account identity change invalidates both the cursor and
// evidence rows: retaining either would mix two upstream accounts under one
// domain. A replacement credential for the same NewAPI identity is different:
// it is the explicit recovery action after a 401/403, so preserve the cursor
// and evidence but reopen the isolated scheduler gate. This runs in the same
// SQLite transaction as the account update; the UI can never observe a new
// credential with an old permanent isolation state.
func reconcileUpstreamErrorLogAccountChange(tx *gorm.DB, domain string, clearIdentity, recoverAuth bool, now int64) error {
	if clearIdentity {
		if err := tx.Where("domain = ?", domain).Delete(&ChannelUpstreamErrorLog{}).Error; err != nil {
			return err
		}
		return tx.Where("domain = ?", domain).Delete(&UpstreamErrorLogSyncState{}).Error
	}
	if !recoverAuth {
		return nil
	}
	return tx.Model(&UpstreamErrorLogSyncState{}).
		Where("domain = ? AND (status = ? OR next_sync_at = ?)", domain, upstreamStatusReconnect, upstreamAccountIsolatedUntil).
		Updates(map[string]any{
			"status": upstreamStatusPending, "next_sync_at": int64(0),
			"consecutive_fails": 0, "last_error": "", "updated_at": now,
		}).Error
}

func (m *Monitor) sealUpstreamAccountCredential(row *ChannelUpstreamAccount, credential any) error {
	sealed, err := m.sealUpstreamCredential(row.Domain, row.Provider, credential)
	if err != nil {
		return err
	}
	row.Credential = sealed
	row.CredentialVersion = upstreamCredentialVersion
	return nil
}

func (m *Monitor) persistUpstreamAccount(ctx context.Context, row *ChannelUpstreamAccount) error {
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "domain"}},
		UpdateAll: true,
	}).Create(row).Error
}

func (m *Monitor) syncStoredUpstreamAccount(ctx context.Context, domain string) (ChannelUpstreamAccount, error) {
	return m.syncStoredUpstreamAccountWithPriority(ctx, domain, false)
}

func (m *Monitor) syncStoredUpstreamAccountBackground(ctx context.Context, domain string) (ChannelUpstreamAccount, error) {
	return m.syncStoredUpstreamAccountWithPriority(ctx, domain, true)
}

func (m *Monitor) syncStoredUpstreamAccountWithPriority(ctx context.Context, domain string, background bool) (ChannelUpstreamAccount, error) {
	var release func()
	var err error
	if background {
		release, err = m.tryAcquireUpstreamAccountBackground(domain)
	} else {
		release, err = m.acquireUpstreamAccountAdmin(ctx, domain)
	}
	if err != nil {
		return ChannelUpstreamAccount{Domain: domain}, err
	}
	defer release()
	var row ChannelUpstreamAccount
	if loadErr := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; loadErr != nil {
		return row, loadErr
	}
	if !row.Enabled {
		return row, fmt.Errorf("该上游账户已停用自动同步")
	}
	credential, err := m.credentialForAccount(row)
	now := time.Now().Unix()
	if err != nil {
		err = &upstreamAuthError{err: err}
		applyUpstreamSyncResult(&row, upstreamBalanceResult{}, err, now, m.cfg)
		// 解密失败时保留原密文，避免临时密钥配置错误把可恢复凭据覆盖成空值。
		if persistErr := m.persistUpstreamAccount(ctx, &row); persistErr != nil {
			return row, persistErr
		}
		return row, &upstreamStoredSyncError{message: row.LastError}
	}
	originalSecrets := upstreamCredentialSecrets(credential)
	var result upstreamBalanceResult
	if cred, ok := credential.(aiCodeWithCredential); ok && row.Provider == upstreamProviderAICodeWith {
		// 保存配置时已逐把验证；周期余额是账户级快照，只需一把 Key。
		result, credential, err = syncAICodeWithBalanceSnapshot(ctx, m.channelUpstreamHTTPClient(), row, cred)
	} else {
		result, credential, err = m.syncUpstreamCredential(ctx, row, credential)
	}
	allSecrets := append([]string{}, originalSecrets...)
	allSecrets = append(allSecrets, upstreamCredentialSecrets(credential)...)
	applyUpstreamSyncResult(&row, result, err, now, m.cfg, allSecrets...)
	if persistErr := m.persistSyncedUpstreamAccount(ctx, &row, credential); persistErr != nil {
		return row, persistErr
	}
	if err != nil {
		return row, &upstreamStoredSyncError{message: row.LastError, retryAt: upstreamRetryAt(err)}
	}
	return row, nil
}

func validateChannelUpstreamInput(in *channelUpstreamSaveInput) error {
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	in.Provider = strings.ToLower(strings.TrimSpace(in.Provider))
	in.Email = strings.TrimSpace(in.Email)
	in.AccessToken = strings.TrimSpace(in.AccessToken)
	in.APIKey = strings.TrimSpace(in.APIKey)
	in.RefreshToken = strings.TrimSpace(in.RefreshToken)
	if in.Domain == "" || len(in.Domain) > 253 || normalizeChannelBaseDomain(in.Domain) != in.Domain {
		return fmt.Errorf("主域名无效")
	}
	baseURL, err := normalizeUpstreamBaseURL(in.BaseURL)
	if err != nil {
		return err
	}
	in.BaseURL = baseURL
	parsedURL, _ := url.Parse(baseURL)
	if normalizeChannelBaseDomain(parsedURL.Hostname()) != in.Domain {
		return fmt.Errorf("站点地址必须属于所配置的主域名")
	}
	switch in.Provider {
	case upstreamProviderNewAPI:
		if in.UserID <= 0 {
			return fmt.Errorf("NewAPI 用户 ID 必须大于 0")
		}
	case upstreamProviderSub2API:
		address, err := mail.ParseAddress(in.Email)
		if err != nil || !strings.EqualFold(address.Address, in.Email) || len(in.Email) > 320 {
			return fmt.Errorf("Sub2API 登录邮箱无效")
		}
		if in.Password != "" && in.RefreshToken != "" {
			return fmt.Errorf("Sub2API 登录密码和 Refresh Token 只能填写一种")
		}
		if len(in.RefreshToken) > 16<<10 {
			return fmt.Errorf("Sub2API Refresh Token 过长")
		}
	case upstreamProviderAICodeWith:
		keys, err := normalizeAICodeWithAPIKeys(in.APIKey, in.APIKeys)
		if err != nil {
			return err
		}
		additions, err := normalizeAICodeWithAPIKeys("", in.AddAPIKeys)
		if err != nil {
			return err
		}
		structuredAdditions, err := normalizeAICodeWithKeyAdditions(in.AddAPIKeySlots)
		if err != nil {
			return err
		}
		renames, err := normalizeAICodeWithKeyRenames(in.RenameAPIKeySlots)
		if err != nil {
			return err
		}
		seenRemoval := make(map[string]bool, len(in.RemoveAPIKeyIDs))
		removals := make([]string, 0, len(in.RemoveAPIKeyIDs))
		for _, id := range in.RemoveAPIKeyIDs {
			id = strings.TrimSpace(id)
			if id == "" || len(id) > 96 || !strings.HasPrefix(id, "acw_") {
				return fmt.Errorf("待删除的 AICodeWith Key 标识无效")
			}
			if !seenRemoval[id] {
				seenRemoval[id] = true
				removals = append(removals, id)
			}
		}
		in.APIKey = ""
		in.APIKeys = keys
		in.AddAPIKeys = additions
		in.AddAPIKeySlots = structuredAdditions
		in.RenameAPIKeySlots = renames
		in.RemoveAPIKeyIDs = removals
	default:
		return fmt.Errorf("当前只支持 NewAPI、Sub2API 和 AICodeWith")
	}
	return nil
}

func (m *Monitor) channelDomainExists(ctx context.Context, domain string) (bool, error) {
	var count int64
	err := m.storeDB.WithContext(ctx).Model(&ChannelSnap{}).Where("base_domain = ?", domain).Count(&count).Error
	return count > 0, err
}

func (m *Monitor) getChannelUpstreamHandler(c *gin.Context) {
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if domain == "" || len(domain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	var row ChannelUpstreamAccount
	err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusOK, channelUpstreamConfigView{Domain: domain, Enabled: true})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取上游账户配置失败"})
		return
	}
	view := channelUpstreamConfigView{
		Domain: domain, Provider: row.Provider, BaseURL: row.BaseURL, Enabled: row.Enabled,
		UsageSyncEnabled: row.UsageSyncEnabled, UserID: row.UserID, Account: m.channelUpstreamAccountView(row),
	}
	view.Account.APIKeySlots = m.aicodeWithSlotViews(ctx, row)
	if row.Provider == upstreamProviderSub2API {
		view.Email = row.Account
	}
	c.JSON(http.StatusOK, view)
}

func (m *Monitor) saveChannelUpstreamHandler(c *gin.Context) {
	if !m.upstreamCredentialPersistent {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "生产环境必须固定配置 MONITOR_SESSION_SECRET 或 MONITOR_UPSTREAM_CREDENTIAL_SECRET 后才能保存上游凭据"})
		return
	}
	if c.Request.ContentLength > maxChannelUpstreamBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelUpstreamBody)
	var in channelUpstreamSaveInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效"})
		return
	}
	if err := validateChannelUpstreamInput(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	requestSecrets := []string{in.AccessToken, in.APIKey, in.Password, in.RefreshToken}
	requestSecrets = append(requestSecrets, in.APIKeys...)
	requestSecrets = append(requestSecrets, in.AddAPIKeys...)
	for _, addition := range in.AddAPIKeySlots {
		requestSecrets = append(requestSecrets, addition.APIKey)
	}
	validationKeyBudget := len(in.APIKeys) + len(in.AddAPIKeys) + len(in.AddAPIKeySlots)
	if in.Provider == upstreamProviderAICodeWith {
		// 一把旧 Key 基准 + 每次最多四把变更 Key。
		validationKeyBudget = maxAICodeWithKeyChangesPerSave + 1
	}
	exists, err := m.channelDomainExists(c.Request.Context(), in.Domain)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "核对主域名失败"})
		return
	}
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该主域名不属于当前渠道快照"})
		return
	}

	releaseAccount, err := m.acquireUpstreamAccountAdmin(c.Request.Context(), in.Domain)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该上游账户正在执行其他操作，请稍后重试"})
		return
	}
	defer releaseAccount()
	// Waiting for an unrelated account no longer consumes this operation's
	// validation deadline. The timeout starts only after this domain is ours.
	ctx, cancel := context.WithTimeout(c.Request.Context(), upstreamSaveTimeout(m.cfg, in.Provider, validationKeyBudget))
	defer cancel()
	var existing ChannelUpstreamAccount
	existingErr := m.storeDB.WithContext(ctx).First(&existing, "domain = ?", in.Domain).Error
	if existingErr != nil && !errors.Is(existingErr, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取现有配置失败"})
		return
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	now := time.Now().Unix()
	row := ChannelUpstreamAccount{
		Domain: in.Domain, Provider: in.Provider, BaseURL: in.BaseURL, Enabled: enabled,
		Status: upstreamStatusPending, CreatedAt: now, UpdatedAt: now, UpdatedBy: c.GetString("uname"),
	}
	if existingErr == nil {
		// CreatedAt 属于主域名配置本身；余额和最近成功时间属于具体账户身份，
		// 必须等 provider/base URL/账号全部确认相同后才能继承。
		row.CreatedAt = existing.CreatedAt
	}
	if in.UsageSyncEnabled != nil {
		row.UsageSyncEnabled = *in.UsageSyncEnabled
	} else if existingErr == nil {
		row.UsageSyncEnabled = existing.UsageSyncEnabled
	}
	var credential any
	var existingAICodeWithCredential *aiCodeWithCredential
	preserveSealedCredential := false
	credentialUpdated := in.AccessToken != "" || len(in.APIKeys) > 0 || len(in.AddAPIKeys) > 0 || len(in.AddAPIKeySlots) > 0 || len(in.RemoveAPIKeyIDs) > 0 || in.Password != "" || in.RefreshToken != ""
	credentialMetadataChanged := len(in.RenameAPIKeySlots) > 0
	sameIdentity := existingErr == nil && existing.Provider == in.Provider && existing.BaseURL == in.BaseURL
	credentialSetChanged := false
	switch in.Provider {
	case upstreamProviderNewAPI:
		row.UserID = in.UserID
		row.Account = strconv.FormatInt(in.UserID, 10)
		sameIdentity = sameIdentity && existing.UserID == in.UserID
		if in.AccessToken != "" {
			credential = newAPICredential{AccessToken: in.AccessToken}
		} else if sameIdentity && !row.Enabled {
			row.Credential, row.CredentialVersion = existing.Credential, existing.CredentialVersion
			preserveSealedCredential = true
		} else if sameIdentity {
			credential, err = m.credentialForAccount(existing)
		} else {
			err = fmt.Errorf("首次连接或变更 NewAPI 账户时必须填写用户访问令牌")
		}
	case upstreamProviderSub2API:
		row.Account = strings.ToLower(in.Email)
		sameIdentity = sameIdentity && strings.EqualFold(existing.Account, row.Account)
		if in.RefreshToken != "" {
			client := m.channelUpstreamHTTPClient()
			credential, err = importSub2APISession(ctx, client, row, in.RefreshToken)
		} else if in.Password != "" {
			client := m.channelUpstreamHTTPClient()
			credential, err = loginSub2API(ctx, client, row, in.Password)
		} else if sameIdentity && !row.Enabled {
			row.Credential, row.CredentialVersion = existing.Credential, existing.CredentialVersion
			preserveSealedCredential = true
		} else if sameIdentity {
			credential, err = m.credentialForAccount(existing)
		} else {
			err = fmt.Errorf("首次连接或变更 Sub2API 账户时必须填写登录密码或 Refresh Token")
		}
	case upstreamProviderAICodeWith:
		additions := make([]aicodeWithKeyAdditionInput, 0, len(in.APIKeys)+len(in.AddAPIKeys)+len(in.AddAPIKeySlots))
		for _, secret := range in.APIKeys {
			additions = append(additions, aicodeWithKeyAdditionInput{APIKey: secret})
		}
		for _, secret := range in.AddAPIKeys {
			additions = append(additions, aicodeWithKeyAdditionInput{APIKey: secret})
		}
		additions = append(additions, in.AddAPIKeySlots...)
		if sameIdentity && existing.Account != "" {
			var currentAny any
			currentAny, err = m.credentialForAccount(existing)
			if err == nil {
				current, ok := currentAny.(aiCodeWithCredential)
				if !ok {
					err = fmt.Errorf("AICodeWith 凭据格式无效")
				} else {
					existingCopy := current
					existingAICodeWithCredential = &existingCopy
				}
				if err == nil && (len(additions) > 0 || len(in.RenameAPIKeySlots) > 0 || len(in.RemoveAPIKeyIDs) > 0) {
					var changed aiCodeWithCredential
					changed, err = applyAICodeWithSlotChanges(current, additions, in.RenameAPIKeySlots, in.RemoveAPIKeyIDs)
					credential = changed
					credentialSetChanged = err == nil && (len(additions) > 0 || len(in.RemoveAPIKeyIDs) > 0)
				} else if err == nil {
					credential, err = normalizeAICodeWithCredential(current)
				}
			}
		} else if len(additions) > 0 && len(in.RemoveAPIKeyIDs) == 0 {
			credential, err = applyAICodeWithSlotChanges(aiCodeWithCredential{}, additions, nil, nil)
			credentialSetChanged = err == nil
			sameIdentity = false
		} else {
			sameIdentity = false
			err = fmt.Errorf("首次连接或变更 AICodeWith 账户时必须填写 API Key")
		}
		if err == nil {
			var identity string
			identity, err = aiCodeWithCredentialIdentity(credential.(aiCodeWithCredential))
			row.Account = identity
			if credentialSetChanged {
				row.UsageStatus = upstreamStatusPending
				row.UsageLastError = ""
				row.UsageNextSyncAt = 0
				row.UsageBackfillCursor = 0
				row.UsageBackfillDone = false
				row.UsageBackfillNextSyncAt = 0
			}
		}
	}
	if sameIdentity {
		row.BalanceUSD, row.BalanceKnown, row.BalanceRaw = existing.BalanceUSD, existing.BalanceKnown, existing.BalanceRaw
		row.BalanceUnit, row.UnitAssumed = existing.BalanceUnit, existing.UnitAssumed
		row.LastSuccessAt = existing.LastSuccessAt
		row.UsageStatus, row.UsageLastError = existing.UsageStatus, existing.UsageLastError
		row.UsageLastAttemptAt, row.UsageLastSuccessAt = existing.UsageLastAttemptAt, existing.UsageLastSuccessAt
		row.UsageNextSyncAt, row.UsageBackfillCursor, row.UsageBackfillDone, row.UsageDataUntil = existing.UsageNextSyncAt, existing.UsageBackfillCursor, existing.UsageBackfillDone, existing.UsageDataUntil
		row.UsageAdapter = existing.UsageAdapter
		row.UsageConsecutiveFails = existing.UsageConsecutiveFails
		row.UsageBackfillLastAttemptAt, row.UsageBackfillLastSuccessAt = existing.UsageBackfillLastAttemptAt, existing.UsageBackfillLastSuccessAt
		row.UsageBackfillNextSyncAt, row.UsageBackfillConsecutiveFails = existing.UsageBackfillNextSyncAt, existing.UsageBackfillConsecutiveFails
		row.UsageBackfillLastError = existing.UsageBackfillLastError
		row.UsageBackfillProgress = existing.UsageBackfillProgress
		// A 401/403 deliberately isolates automatic usage requests until an
		// administrator supplies credentials again. Saving a replacement secret
		// for the same account is that explicit recovery action: retain all local
		// usage/cursor state, but make tail and history eligible to run again.
		usageAuthIsolated := existing.UsageStatus == upstreamStatusReconnect ||
			existing.UsageNextSyncAt == upstreamAccountIsolatedUntil ||
			existing.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil
		if credentialUpdated && usageAuthIsolated {
			row.UsageStatus, row.UsageLastError = upstreamStatusPending, ""
			row.UsageNextSyncAt, row.UsageConsecutiveFails = 0, 0
			row.UsageBackfillNextSyncAt, row.UsageBackfillConsecutiveFails = 0, 0
			row.UsageBackfillLastError = ""
			row.UsageBackfillProgress = ""
		}
	}
	if credentialSetChanged {
		row.UsageStatus, row.UsageLastError = upstreamStatusPending, ""
		row.UsageNextSyncAt, row.UsageConsecutiveFails = 0, 0
		row.UsageBackfillCursor, row.UsageBackfillDone = 0, false
		row.UsageBackfillNextSyncAt, row.UsageBackfillConsecutiveFails = 0, 0
		row.UsageBackfillLastError = ""
		row.UsageBackfillProgress = ""
	}
	if !row.UsageSyncEnabled {
		row.UsageStatus, row.UsageNextSyncAt = upstreamStatusDisabled, 0
	}
	// 密码从这里起不再被引用；它从未进入持久化模型、日志或响应。
	in.Password = ""
	in.RefreshToken = ""
	in.AccessToken = ""
	in.APIKey = ""
	in.APIKeys = nil
	in.AddAPIKeys = nil
	in.AddAPIKeySlots = nil
	in.RenameAPIKeySlots = nil
	in.RemoveAPIKeyIDs = nil
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": sanitizeUpstreamErrorWithSecrets(err, requestSecrets...)})
		return
	}
	labelOnlyChange := row.Provider == upstreamProviderAICodeWith && sameIdentity && credentialMetadataChanged && !credentialSetChanged &&
		row.Enabled == existing.Enabled && row.UsageSyncEnabled == existing.UsageSyncEnabled
	if labelOnlyChange {
		updated := existing
		updated.UpdatedAt, updated.UpdatedBy = now, row.UpdatedBy
		cred := credential.(aiCodeWithCredential)
		if sealErr := m.sealUpstreamAccountCredential(&updated, cred); sealErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "保存 Key 名称失败"})
			return
		}
		if persistErr := m.persistAICodeWithAccountChange(ctx, &updated, cred, false, false); persistErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "保存 Key 名称失败"})
			return
		}
		view := m.channelUpstreamAccountView(updated)
		view.APIKeySlots = m.aicodeWithSlotViews(ctx, updated)
		c.JSON(http.StatusOK, gin.H{"account": view})
		return
	}
	if !row.Enabled {
		row.Status, row.NextSyncAt = upstreamStatusDisabled, 0
		if !preserveSealedCredential {
			if sealErr := m.sealUpstreamAccountCredential(&row, credential); sealErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "保存上游配置失败"})
				return
			}
		}
		clearUsage := existingErr == nil && !sameIdentity && !(in.Provider == upstreamProviderAICodeWith && existing.Provider == upstreamProviderAICodeWith && existing.BaseURL == in.BaseURL)
		var persistErr error
		recoverErrorLogAuth := row.Provider == upstreamProviderNewAPI && sameIdentity && credentialUpdated
		if cred, ok := credential.(aiCodeWithCredential); ok && row.Provider == upstreamProviderAICodeWith && !preserveSealedCredential {
			persistErr = m.persistAICodeWithAccountChange(ctx, &row, cred, clearUsage, false)
		} else {
			persistErr = m.persistUpstreamAccountIdentityChange(ctx, &row, clearUsage, recoverErrorLogAuth)
		}
		if persistErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "保存上游配置失败"})
			return
		}
		view := m.channelUpstreamAccountView(row)
		view.APIKeySlots = m.aicodeWithSlotViews(ctx, row)
		c.JSON(http.StatusOK, gin.H{"account": view})
		return
	}
	var result upstreamBalanceResult
	updatedCredential := credential
	var syncErr error
	if row.Provider == upstreamProviderAICodeWith {
		fullCredential := credential.(aiCodeWithCredential)
		validationCredential, validationCount, validationErr := aicodeWithSaveValidationCredential(existingAICodeWithCredential, fullCredential)
		if validationErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": sanitizeUpstreamErrorWithSecrets(validationErr, requestSecrets...)})
			return
		}
		if validationCount == 0 {
			// 删除、改名或开关操作沿用最近完整余额；后台余额快照会按原频率更新。
			result = upstreamBalanceResult{BalanceUSD: row.BalanceUSD, BalanceRaw: row.BalanceRaw, BalanceUnit: row.BalanceUnit, UnitAssumed: row.UnitAssumed}
		} else {
			pacer := newUpstreamUsageRequestPacer(validationCount, m.aiCodeWithRequestInterval())
			result, _, syncErr = syncAICodeWithBalanceWithPacer(ctx, m.channelUpstreamHTTPClient(), row, validationCredential, pacer)
		}
	} else {
		result, updatedCredential, syncErr = m.syncUpstreamCredential(ctx, row, credential)
	}
	credentialSecrets := append([]string{}, requestSecrets...)
	credentialSecrets = append(credentialSecrets, upstreamCredentialSecrets(credential)...)
	credential = updatedCredential
	credentialSecrets = append(credentialSecrets, upstreamCredentialSecrets(credential)...)
	// Adding/removing an AICodeWith key is an atomic credential-set change.
	// A failed validation must not persist a poisoned set that would prevent all
	// keys from completing the same publish round.
	if row.Provider == upstreamProviderAICodeWith && credentialSetChanged && syncErr != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "AICodeWith Key 验证失败，原配置未修改: " + sanitizeUpstreamErrorWithSecrets(syncErr, credentialSecrets...)})
		return
	}
	applyUpstreamSyncResult(&row, result, syncErr, now, m.cfg, credentialSecrets...)
	if err := m.sealUpstreamAccountCredential(&row, credential); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存上游配置失败"})
		return
	}
	clearUsage := existingErr == nil && !sameIdentity && !(in.Provider == upstreamProviderAICodeWith && existing.Provider == upstreamProviderAICodeWith && existing.BaseURL == in.BaseURL)
	var persistErr error
	recoverErrorLogAuth := row.Provider == upstreamProviderNewAPI && sameIdentity && credentialUpdated
	if cred, ok := credential.(aiCodeWithCredential); ok && row.Provider == upstreamProviderAICodeWith {
		persistErr = m.persistAICodeWithAccountChange(ctx, &row, cred, clearUsage, false)
	} else {
		persistErr = m.persistUpstreamAccountIdentityChange(ctx, &row, clearUsage, recoverErrorLogAuth)
	}
	if persistErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存上游配置失败"})
		return
	}
	view := m.channelUpstreamAccountView(row)
	view.APIKeySlots = m.aicodeWithSlotViews(ctx, row)
	response := gin.H{"account": view}
	if syncErr != nil {
		response["sync_error"] = row.LastError
	}
	c.JSON(http.StatusOK, response)
}

type channelUpstreamSyncOperation func(context.Context, string) (ChannelUpstreamAccount, error)

func (m *Monitor) serveChannelUpstreamSync(c *gin.Context, timeout time.Duration, operation channelUpstreamSyncOperation, emptyResultError string, lastError func(ChannelUpstreamAccount) string) {
	if c.Request.ContentLength > maxChannelUpstreamBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "请求内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelUpstreamBody)
	var in channelUpstreamSyncInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式无效"})
		return
	}
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()
	row, err := operation(ctx, in.Domain)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "该主域名尚未配置上游账户"})
		return
	}
	if err != nil && row.Domain == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": emptyResultError})
		return
	}
	response := gin.H{"account": m.channelUpstreamAccountView(row)}
	if err != nil {
		response["sync_error"] = lastError(row)
		if retryAt := upstreamRetryAt(err); retryAt > 0 {
			response["retry_at"] = retryAt
		}
	}
	c.JSON(http.StatusOK, response)
}

func (m *Monitor) syncChannelUpstreamHandler(c *gin.Context) {
	m.serveChannelUpstreamSync(c, upstreamSyncTimeout(m.cfg)+3*time.Second, m.syncStoredUpstreamAccount,
		"同步上游余额失败", func(row ChannelUpstreamAccount) string { return row.LastError })
}

func (m *Monitor) startChannelUpstreamSync(ctx context.Context) {
	// 已审批的倍率版本是本地持久化承诺，必须在到点后生效。
	// 这条 lane 只读写 Monitor SQLite，无上游 I/O，因此先于余额、日志和
	// 计价采集闸门启动；上游全部停采时也不会丢失已排程任务。
	m.startChannelFinanceActivationLane(ctx, 7*time.Second)
	if !m.cfg.UpstreamSyncEnabled && !m.cfg.UpstreamUsageSyncEnabled && !m.cfg.UpstreamPricingLedgerEnabled && !m.cfg.UpstreamErrorLogSyncEnabled {
		slog.Info("上游余额、消费账单、计价证据与错误日志采集均已关闭")
		return
	}

	var configured int64
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Count(&configured).Error; err != nil {
		slog.Warn("读取上游账户配置失败，上游同步未启动", "err", err)
		return
	}
	if configured > 0 && !m.upstreamCredentialPersistent {
		slog.Error("上游同步未启动：凭据密钥未固定，请配置 MONITOR_SESSION_SECRET 或 MONITOR_UPSTREAM_CREDENTIAL_SECRET")
		return
	}
	if !m.cfg.UpstreamSyncEnabled {
		slog.Info("上游余额同步已关闭，消费账单同步不受影响")
	}
	if !m.cfg.UpstreamUsageSyncEnabled {
		slog.Info("上游使用日志同步处于灰度关闭状态，余额同步不受影响")
	}
	if !m.cfg.UpstreamErrorLogSyncEnabled {
		slog.Info("上游错误日志采集处于灰度关闭状态，其余同步不受影响")
	}
	// Keep the lanes independent. A slow balance provider must not delay usage
	// freshness or pricing evidence until the whole balance batch finishes.
	// Request-level host/global protection still bounds aggregate traffic, and
	// account gates serialize operations that share one credential.
	if m.cfg.UpstreamSyncEnabled {
		goSourceEpoch(ctx, func(laneCtx context.Context) {
			runUpstreamPeriodicLane(laneCtx, 8*time.Second, time.Minute, m.syncDueUpstreamAccounts)
		})
	}
	if m.cfg.UpstreamUsageSyncEnabled {
		goSourceEpoch(ctx, func(laneCtx context.Context) {
			runUpstreamPeriodicLane(laneCtx, 9*time.Second, upstreamUsageSchedulerInterval, m.syncDueUpstreamUsage)
		})
	}
	if m.cfg.UpstreamPricingLedgerEnabled {
		goSourceEpoch(ctx, func(laneCtx context.Context) {
			runUpstreamPeriodicLane(laneCtx, 10*time.Second, time.Minute, m.syncDueUpstreamPricing)
		})
	}
	if m.cfg.UpstreamErrorLogSyncEnabled {
		goSourceEpoch(ctx, func(laneCtx context.Context) {
			// 同步器内部仍有 5 分钟节流和失败退避；每分钟只做到期检查。
			runUpstreamPeriodicLane(laneCtx, 11*time.Second, time.Minute, m.syncDueUpstreamErrorLogs)
		})
	}
	goSourceEpoch(ctx, func(cleanupCtx context.Context) {
		<-cleanupCtx.Done()
		m.channelUpstreamHTTPClient().CloseIdleConnections()
	})
}

func (m *Monitor) startChannelFinanceActivationLane(ctx context.Context, initialDelay time.Duration) bool {
	if !m.cfg.ChannelCostClosureEnabled {
		return false
	}
	return goSourceEpoch(ctx, func(laneCtx context.Context) {
		runUpstreamPeriodicLane(laneCtx, initialDelay, time.Minute, m.syncDueChannelFinanceActivations)
	})
}

func (m *Monitor) syncDueChannelFinanceActivations(ctx context.Context) {
	if !m.cfg.ChannelCostClosureEnabled {
		return
	}
	const maxPerTick = 8
	now := time.Now().Unix()
	for i := 0; i < maxPerTick; i++ {
		applied, err := m.applyOneDueChannelFinanceActivation(ctx, now)
		if err != nil {
			// SQLite busy/临时错误不消耗持久化槽位，下一分钟继续。
			slog.Warn("待生效渠道倍率本轮未应用，将自动重试", "err", err)
			return
		}
		if applied {
			continue
		}
		// A false result can mean either an empty queue, or that one corrupt /
		// orphan head was isolated. Continue only when another due slot exists;
		// this prevents one bad domain from delaying healthy domains by a minute.
		var due int64
		if err := m.storeDB.WithContext(ctx).Model(&ChannelFinanceActivationSlot{}).
			Where("effective_at <= ?", now).Limit(1).Count(&due).Error; err != nil {
			slog.Warn("读取待生效渠道倍率队列失败，将自动重试", "err", err)
			return
		}
		if due == 0 {
			return
		}
	}
}

func runUpstreamPeriodicLane(ctx context.Context, initialDelay, interval time.Duration, run func(context.Context)) {
	if initialDelay < 0 {
		initialDelay = 0
	}
	if interval <= 0 {
		interval = time.Minute
	}
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		run(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run(ctx)
		}
	}
}

func (m *Monitor) syncDueUpstreamAccounts(ctx context.Context) {
	now := time.Now().Unix()
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Where("enabled = ? AND (next_sync_at = 0 OR next_sync_at <= ?)", true, now).
		Order("next_sync_at ASC").Limit(50).Find(&rows).Error; err != nil {
		slog.Warn("读取待同步上游账户失败", "err", err)
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		syncCtx, cancel := context.WithTimeout(ctx, upstreamSyncTimeout(m.cfg)+2*time.Second)
		synced, err := m.syncStoredUpstreamAccountBackground(syncCtx, row.Domain)
		cancel()
		if err != nil {
			if errors.Is(err, errUpstreamAccountBusy) {
				continue
			}
			message := synced.LastError
			if message == "" {
				// 只有本地读取/持久化失败时可能没有上游错误正文；该错误不含明文凭据。
				message = sanitizeUpstreamError(err)
			}
			slog.Warn("上游余额同步失败", "domain", row.Domain, "provider", row.Provider, "err", message)
		}
	}
}
