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
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	upstreamProviderNewAPI  = "newapi"
	upstreamProviderSub2API = "sub2api"

	upstreamStatusPending     = "pending"
	upstreamStatusOK          = "ok"
	upstreamStatusError       = "error"
	upstreamStatusReconnect   = "reconnect"
	upstreamStatusDisabled    = "disabled"
	upstreamStatusUnsupported = "unsupported"

	maxChannelUpstreamBody    = 32 << 10
	maxUpstreamResponseBody   = 1 << 20
	defaultNewAPIQuotaPerUSD  = 500000.0
	upstreamCredentialVersion = 1
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
	// 当天尾部刷新与历史回填分别退避。历史某一天异常不能拖慢当天数据，
	// 也不能在每次尾部刷新时无节制重试同一个高流量窗口。
	UsageBackfillLastAttemptAt    int64  `gorm:"column:usage_backfill_last_attempt_at"`
	UsageBackfillLastSuccessAt    int64  `gorm:"column:usage_backfill_last_success_at"`
	UsageBackfillNextSyncAt       int64  `gorm:"column:usage_backfill_next_sync_at;index"`
	UsageBackfillConsecutiveFails int    `gorm:"column:usage_backfill_consecutive_fails"`
	UsageBackfillLastError        string `gorm:"size:512;column:usage_backfill_last_error"`
	// UsageBackfillCursor 是尚待补齐的自然日（CST）起点；0 表示初始化。
	// UsageBackfillDone 只说明配置范围内的历史已完成，不表示今天的实时性。
	UsageBackfillCursor int64 `gorm:"column:usage_backfill_cursor"`
	UsageBackfillDone   bool  `gorm:"column:usage_backfill_done"`
	UsageDataUntil      int64 `gorm:"column:usage_data_until"`
}

// ChannelUpstreamUsageHour 是上游账户日志按小时的本地脱敏汇总。
// 不保留 API Key、Cookie、请求体、响应体、用户内容或远端原始日志 ID；页面按日期范围
// 仅查询这里，绝不因用户刷新而访问上游。按小时重算能处理上游延迟入库而不依赖不可靠的
// 跨版本日志 ID 去重。
type ChannelUpstreamUsageHour struct {
	Domain    string  `gorm:"primaryKey;size:253;column:domain"`
	HourTs    int64   `gorm:"primaryKey;column:hour_ts"`
	Requests  int64   `gorm:"column:requests"`
	Tokens    int64   `gorm:"column:tokens"`
	Quota     float64 `gorm:"column:quota"`
	CostUSD   float64 `gorm:"column:cost_usd"`
	FetchedAt int64   `gorm:"column:fetched_at;index"`
	Provider  string  `gorm:"size:24;column:provider"`
}

// ChannelUpstreamAccountView 是渠道管理页可见的脱敏状态。
type ChannelUpstreamAccountView struct {
	Configured                    bool                              `json:"configured"`
	Enabled                       bool                              `json:"enabled"`
	Provider                      string                            `json:"provider,omitempty"`
	ProviderName                  string                            `json:"provider_name,omitempty"`
	BaseURL                       string                            `json:"base_url,omitempty"`
	AccountMasked                 string                            `json:"account_masked,omitempty"`
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
	UsageStatus                   string                            `json:"usage_status,omitempty"`
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
}

type channelUpstreamSaveInput struct {
	Domain           string `json:"domain"`
	Provider         string `json:"provider"`
	BaseURL          string `json:"base_url"`
	Enabled          *bool  `json:"enabled"`
	UserID           int64  `json:"user_id"`
	AccessToken      string `json:"access_token"`
	Email            string `json:"email"`
	Password         string `json:"password"`
	UsageSyncEnabled *bool  `json:"usage_sync_enabled"`
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
	default:
		return provider
	}
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
		minutes = 30
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
	}
	return "已配置"
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
		UsageConsecutiveFails:         row.UsageConsecutiveFails,
		UsageBackfillLastSuccessAt:    row.UsageBackfillLastSuccessAt,
		UsageBackfillNextSyncAt:       row.UsageBackfillNextSyncAt,
		UsageBackfillConsecutiveFails: row.UsageBackfillConsecutiveFails,
		UsageBackfillLastError:        row.UsageBackfillLastError,
	}
	if row.BalanceKnown {
		balance := row.BalanceUSD
		view.BalanceUSD = &balance
	}
	return view
}

func (m *Monitor) loadChannelUpstreamViews(ctx context.Context) (map[string]ChannelUpstreamAccountView, error) {
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]ChannelUpstreamAccountView, len(rows))
	for _, row := range rows {
		out[row.Domain] = upstreamAccountView(row)
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
	return installUpstreamHostGuard(client, m.storeDB)
}

func upstreamResponseMessage(body []byte) string {
	var payload struct {
		Message string `json:"message"`
		Error   string `json:"error"`
		Detail  string `json:"detail"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	message := payload.Message
	if message == "" {
		message = payload.Error
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

func (m *Monitor) persistUpstreamAccount(ctx context.Context, row *ChannelUpstreamAccount) error {
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "domain"}},
		UpdateAll: true,
	}).Create(row).Error
}

func (m *Monitor) syncStoredUpstreamAccount(ctx context.Context, domain string) (ChannelUpstreamAccount, error) {
	m.upstreamSyncMu.Lock()
	defer m.upstreamSyncMu.Unlock()
	var row ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		return row, err
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
	result, credential, err = m.syncUpstreamCredential(ctx, row, credential)
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
	default:
		return fmt.Errorf("当前只支持 NewAPI 和 Sub2API")
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
		UsageSyncEnabled: row.UsageSyncEnabled, UserID: row.UserID, Account: upstreamAccountView(row),
	}
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
	requestSecrets := []string{in.AccessToken, in.Password}
	ctx, cancel := context.WithTimeout(c.Request.Context(), upstreamSyncTimeout(m.cfg)+5*time.Second)
	defer cancel()
	exists, err := m.channelDomainExists(ctx, in.Domain)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "核对主域名失败"})
		return
	}
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该主域名不属于当前渠道快照"})
		return
	}

	m.upstreamSyncMu.Lock()
	defer m.upstreamSyncMu.Unlock()
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
	preserveSealedCredential := false
	credentialUpdated := in.AccessToken != "" || in.Password != ""
	sameIdentity := existingErr == nil && existing.Provider == in.Provider && existing.BaseURL == in.BaseURL
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
		if in.Password != "" {
			client := m.channelUpstreamHTTPClient()
			credential, err = loginSub2API(ctx, client, row, in.Password)
		} else if sameIdentity && !row.Enabled {
			row.Credential, row.CredentialVersion = existing.Credential, existing.CredentialVersion
			preserveSealedCredential = true
		} else if sameIdentity {
			credential, err = m.credentialForAccount(existing)
		} else {
			err = fmt.Errorf("首次连接或变更 Sub2API 账户时必须填写登录密码")
		}
	}
	if sameIdentity {
		row.BalanceUSD, row.BalanceKnown, row.BalanceRaw = existing.BalanceUSD, existing.BalanceKnown, existing.BalanceRaw
		row.BalanceUnit, row.UnitAssumed = existing.BalanceUnit, existing.UnitAssumed
		row.LastSuccessAt = existing.LastSuccessAt
		row.UsageStatus, row.UsageLastError = existing.UsageStatus, existing.UsageLastError
		row.UsageLastAttemptAt, row.UsageLastSuccessAt = existing.UsageLastAttemptAt, existing.UsageLastSuccessAt
		row.UsageNextSyncAt, row.UsageBackfillCursor, row.UsageBackfillDone, row.UsageDataUntil = existing.UsageNextSyncAt, existing.UsageBackfillCursor, existing.UsageBackfillDone, existing.UsageDataUntil
		row.UsageConsecutiveFails = existing.UsageConsecutiveFails
		row.UsageBackfillLastAttemptAt, row.UsageBackfillLastSuccessAt = existing.UsageBackfillLastAttemptAt, existing.UsageBackfillLastSuccessAt
		row.UsageBackfillNextSyncAt, row.UsageBackfillConsecutiveFails = existing.UsageBackfillNextSyncAt, existing.UsageBackfillConsecutiveFails
		row.UsageBackfillLastError = existing.UsageBackfillLastError
		// A 401/403 deliberately isolates automatic usage requests until an
		// administrator supplies credentials again. Saving a replacement secret
		// for the same account is that explicit recovery action: retain all local
		// usage/cursor state, but make tail and history eligible to run again.
		usageAuthIsolated := existing.UsageStatus == upstreamStatusReconnect ||
			existing.UsageNextSyncAt == upstreamAccountIsolatedUntil ||
			existing.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil
		if credentialUpdated && row.UsageSyncEnabled && usageAuthIsolated {
			row.UsageStatus, row.UsageLastError = upstreamStatusPending, ""
			row.UsageNextSyncAt, row.UsageConsecutiveFails = 0, 0
			row.UsageBackfillNextSyncAt, row.UsageBackfillConsecutiveFails = 0, 0
			row.UsageBackfillLastError = ""
		}
	}
	if !row.UsageSyncEnabled {
		row.UsageStatus, row.UsageNextSyncAt = upstreamStatusDisabled, 0
	}
	// 密码从这里起不再被引用；它从未进入持久化模型、日志或响应。
	in.Password = ""
	in.AccessToken = ""
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": sanitizeUpstreamErrorWithSecrets(err, requestSecrets...)})
		return
	}
	if !row.Enabled {
		row.Status, row.NextSyncAt = upstreamStatusDisabled, 0
		var persistErr error
		if preserveSealedCredential {
			persistErr = m.persistUpstreamAccount(ctx, &row)
		} else {
			persistErr = m.persistSyncedUpstreamAccount(ctx, &row, credential)
		}
		if persistErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "保存上游配置失败"})
			return
		}
		// 账户身份变更后，即使管理员先将账户停用再保存，也不能继续保留
		// 旧账户的本地小时汇总；它们不再属于当前主域名配置。
		if existingErr == nil && !sameIdentity {
			if clearErr := m.storeDB.WithContext(ctx).Where("domain = ?", row.Domain).Delete(&ChannelUpstreamUsageHour{}).Error; clearErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "保存账户后清理旧使用汇总失败"})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{"account": upstreamAccountView(row)})
		return
	}
	result, updatedCredential, syncErr := m.syncUpstreamCredential(ctx, row, credential)
	credentialSecrets := append([]string{}, requestSecrets...)
	credentialSecrets = append(credentialSecrets, upstreamCredentialSecrets(credential)...)
	credential = updatedCredential
	credentialSecrets = append(credentialSecrets, upstreamCredentialSecrets(credential)...)
	applyUpstreamSyncResult(&row, result, syncErr, now, m.cfg, credentialSecrets...)
	if err := m.persistSyncedUpstreamAccount(ctx, &row, credential); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存上游配置失败"})
		return
	}
	// 更换 provider、站点或账户后，旧账户的脱敏小时汇总不能再归因给新账户。
	// 这里只清理 Monitor 本地汇总，绝不修改主站或上游数据。
	if existingErr == nil && !sameIdentity {
		if clearErr := m.storeDB.WithContext(ctx).Where("domain = ?", row.Domain).Delete(&ChannelUpstreamUsageHour{}).Error; clearErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "保存账户后清理旧使用汇总失败"})
			return
		}
	}
	response := gin.H{"account": upstreamAccountView(row)}
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
	response := gin.H{"account": upstreamAccountView(row)}
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
	if !m.cfg.UpstreamSyncEnabled {
		slog.Info("上游账户同步已关闭")
		return
	}

	var configured int64
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Count(&configured).Error; err != nil {
		slog.Warn("读取上游账户配置失败，余额同步未启动", "err", err)
		return
	}
	if configured > 0 && !m.upstreamCredentialPersistent {
		slog.Error("上游余额同步未启动：凭据密钥未固定，请配置 MONITOR_SESSION_SECRET 或 MONITOR_UPSTREAM_CREDENTIAL_SECRET")
		return
	}
	goSourceEpoch(ctx, func(ctx context.Context) {
		defer m.channelUpstreamHTTPClient().CloseIdleConnections()
		timer := time.NewTimer(8 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.syncDueUpstreamAccounts(ctx)
			m.syncDueUpstreamUsage(ctx)
		}
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.syncDueUpstreamAccounts(ctx)
				m.syncDueUpstreamUsage(ctx)
			}
		}
	})
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
		synced, err := m.syncStoredUpstreamAccount(syncCtx, row.Domain)
		cancel()
		if err != nil {
			message := synced.LastError
			if message == "" {
				// 只有本地读取/持久化失败时可能没有上游错误正文；该错误不含明文凭据。
				message = sanitizeUpstreamError(err)
			}
			slog.Warn("上游余额同步失败", "domain", row.Domain, "provider", row.Provider, "err", message)
		}
	}
}
