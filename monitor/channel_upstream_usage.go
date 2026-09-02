package monitor

// 上游使用日志同步是一个低频、显式启用的旁路任务。它只把日志汇总写入
// Monitor SQLite；不向 NewAPI 主库写入，也不会因管理页查询访问上游。

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	maxUpstreamUsagePages = 50
	upstreamUsagePageSize = 100
	// One due account may need several offset pages and one historical split.
	// Bound and pace that whole tail+history operation so a dense day cannot
	// turn a low-frequency scheduler into a burst against the upstream WAF.
	upstreamUsageMaxRequestsPerRun = 60
	upstreamUsageRequestInterval   = 200 * time.Millisecond
	// Re-read a short overlap on every tail pass so delayed log commits are
	// corrected without re-reading the whole open day every 30 minutes.
	upstreamUsageTailOverlap = 3 * time.Hour
	// AICodeWith 的按 Key 账单接口最多接受 31 个中国自然日。
	// 历史每轮一次请求批量补齐，避免把按天聚合接口退化为逐日请求。
	aiCodeWithUsageMaxDays = 31
	// 文档约定每把 Key 最多 10 次/分钟。多 Key 账户内的所有请求也共用
	// 同一个 8 秒节流器，避免逐 Key 新建节流器后瞬时打满上游账户限额。
	aiCodeWithUsageMaxRequestsPerRun = 2
	aiCodeWithUsageRequestInterval   = 8 * time.Second
	aiCodeWithKeysPerTurn            = 4
	// The scheduler selects one bounded batch per turn (one account by default,
	// at most two distinct hosts after explicit rollout). A short cadence removes
	// the old one-minute idle gap; per-account next_sync_at and host protection
	// still enforce the actual request frequency.
	upstreamUsageSchedulerInterval = 10 * time.Second
	upstreamUsageAdapterNewAPILog  = "newapi_log"
	upstreamUsageAdapterSub2Trend  = "sub2api_trend"
	upstreamUsageAdapterSub2Stats  = "sub2api_stats"
	upstreamUsageAdapterAICodeWith = "aicodewith_key"
	// Tail 与历史补数是两条调度泳道。管理员手动同步仍走 auto，后台必须
	// 显式选择一条，防止一个历史密集小时占满整轮而拖延其他账户的 Tail。
	upstreamUsageLaneAuto    upstreamUsageLane = "auto"
	upstreamUsageLaneTail    upstreamUsageLane = "tail"
	upstreamUsageLaneHistory upstreamUsageLane = "history"
	// 历史是可恢复的低优先级工作。20 次请求/45 秒足够连续推进，又能在
	// Tail 到期前及时让出调度器；断点会保留在 SQLite，下轮继续。
	upstreamUsageHistoryMaxRequestsPerRun = 20
	upstreamUsageHistoryOperationTimeout  = 45 * time.Second
	// A whole-hour NewAPI query may be slow even when small time ranges are
	// healthy. After a persisted timeout the history lane switches that hour to
	// durable five-minute children. The same 20-request/45-second lane budget
	// still applies, so recovery cannot turn into an upstream request burst.
	newAPIBackfillAdaptiveSegment    = 5 * time.Minute
	newAPIBackfillAdaptiveMinSegment = 10 * time.Second
	// Tail 持续有流量时，每完成 4 个 Tail 批次允许 1 个历史批次；但只要
	// 最老 Tail 已逾期超过 5 分钟，就继续优先追新，不为历史让路。
	upstreamUsageHistoryFairnessTailBatches = 4
	upstreamUsageTailOverdueGuard           = 5 * time.Minute
)

type upstreamUsageLane string

func (m *Monitor) aiCodeWithRequestInterval() time.Duration {
	if m != nil && m.upstreamAICodeWithInterval > 0 {
		return m.upstreamAICodeWithInterval
	}
	return aiCodeWithUsageRequestInterval
}

type upstreamUsageResult struct {
	Hours     []ChannelUpstreamUsageHour
	DataUntil int64
	Adapter   string
	// SourceKeyID is populated only by per-Key providers. It is an independent
	// control identity from the upstream response and prevents two configured
	// credentials that resolve to the same remote Key from being double-counted.
	SourceKeyID int64
}

// cstDayStart 返回给定 Unix 时刻所在中国自然日的起点。使用自然日切片可让历史
// 补全不随服务器时区漂移；当前日滚动覆盖最近数小时，修正上游延迟入库。
func cstDayStart(ts int64) int64 {
	t := time.Unix(ts, 0).In(cstLocation)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, cstLocation).Unix()
}

func nextUpstreamUsageSyncAt(s Settings, domain string, now int64, failures int) int64 {
	return nextUpstreamUsageScheduledAt(s, domain+":tail", now, failures)
}

func nextUpstreamUsageSyncAtForProvider(s Settings, provider, domain string, now int64, failures int) int64 {
	// AICodeWith 是多 Key 账户级账单接口，且线上已经观察到 429。即使其他
	// 适配器提速到 20 分钟，也为该提供方保留至少 30 分钟的 Tail 间隔。
	if provider == upstreamProviderAICodeWith && upstreamUsageSyncMinutes(s) < 30 {
		s.UpstreamUsageSyncMinutes = 30
	}
	return nextUpstreamUsageSyncAt(s, domain, now, failures)
}

func nextUpstreamUsageBackfillAt(s Settings, domain string, now int64, failures int) int64 {
	return nextUpstreamUsageScheduledAt(s, domain+":backfill", now, failures)
}

func nextUpstreamUsageScheduledAt(s Settings, scheduleKey string, now int64, failures int) int64 {
	minutes := upstreamUsageSyncMinutes(s)
	if failures > 0 {
		shift := failures
		if shift > 3 {
			shift = 3
		}
		minutes *= 1 << shift
		if minutes > 240 {
			minutes = 240
		}
	}
	base := int64(minutes * 60)
	// 基于域名和时桶的确定性单向抖动，既可测试又避免各上游在同一秒
	// 被同时请求；不会缩短配置的最小安全间隔。
	h := sha256Sum(scheduleKey + ":usage:" + strconv.FormatInt(now/base, 10))
	jitterMax := base / 10
	if jitterMax > 45 {
		jitterMax = 45
	}
	jitter := int64(0)
	if jitterMax > 0 {
		jitter = int64(h[0]) % (jitterMax + 1)
	}
	return now + base + jitter
}

var upstreamUsageTransientRetryDelays = [...]time.Duration{
	2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour,
}

var upstreamUsageLocalStoreRetryDelays = [...]time.Duration{
	10 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute,
}

func upstreamUsageRetryDelay(delays []time.Duration, failures int) time.Duration {
	index := failures - 1
	if index < 0 {
		index = 0
	}
	if index >= len(delays) {
		index = len(delays) - 1
	}
	return delays[index]
}

func isUpstreamUsageLocalStoreBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database is busy") || strings.Contains(message, "sqlite_busy")
}

func isUpstreamUsageTransientFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var statusErr *upstreamHTTPError
	if errors.As(err, &statusErr) {
		return statusErr.Status == http.StatusRequestTimeout || statusErr.Status == http.StatusTooEarly || statusErr.Status >= 500
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "timeout") || strings.Contains(message, "deadline exceeded") ||
		strings.Contains(message, "connection reset") || strings.Contains(message, "connection refused") ||
		strings.Contains(message, "unexpected eof") || strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "no such host")
}

func upstreamUsageFailureRetryAt(s Settings, domain string, now int64, failures int, err error, scheduleKey string) int64 {
	if retryAt := upstreamRetryAt(err); retryAt > now {
		return retryAt
	}
	if isUpstreamUsageLocalStoreBusy(err) {
		return now + int64(upstreamUsageRetryDelay(upstreamUsageLocalStoreRetryDelays[:], failures)/time.Second)
	}
	if isUpstreamUsageTransientFailure(err) {
		return now + int64(upstreamUsageRetryDelay(upstreamUsageTransientRetryDelays[:], failures)/time.Second)
	}
	var statusErr *upstreamHTTPError
	if errors.As(err, &statusErr) && statusErr.Status == http.StatusTooManyRequests {
		return now + int64(upstreamRetryAfterDefault/time.Second)
	}
	// 响应格式或适配器错误通常不是秒级自愈，保持低频但有上限的探测。
	return nextUpstreamUsageScheduledAt(s, domain+":"+scheduleKey+":other", now, max(1, failures))
}

func upstreamUsageOperationTimeout(s Settings) time.Duration {
	// Leave enough wall-clock budget for a fast upstream to consume the full
	// guarded request allowance. Slow responses are still bounded by the hard
	// two-minute operation cap and each request's own HTTP timeout.
	pacingBudget := time.Duration(upstreamUsageMaxRequestsPerRun-1) * (upstreamGuardMinInterval + upstreamGuardMaxJitter)
	timeout := pacingBudget + 2*upstreamSyncTimeout(s)
	if timeout < 30*time.Second {
		return 30 * time.Second
	}
	if timeout > 2*time.Minute {
		return 2 * time.Minute
	}
	return timeout
}

func upstreamUsageOperationTimeoutForLane(s Settings, lane upstreamUsageLane) time.Duration {
	if lane == upstreamUsageLaneHistory {
		return upstreamUsageHistoryOperationTimeout
	}
	return upstreamUsageOperationTimeout(s)
}

// sha256Sum 隔离使用日志调度的 hash 细节，避免把凭据混入调度输入。
func sha256Sum(s string) [32]byte { return sha256.Sum256([]byte(s)) }

type newAPIUsageItem struct {
	CreatedAt        int64
	Quota            float64
	PromptTokens     int64
	CompletionTokens int64
}

type newAPIUsagePage[T any] struct {
	Items       []T
	Total       int64
	Fingerprint [32]byte
}

type upstreamUsageWindowTooDense struct {
	total int64
}

func (e *upstreamUsageWindowTooDense) Error() string {
	return fmt.Sprintf("上游使用日志窗口超过单次安全上限（至少 %d 条）", e.total)
}

type upstreamUsageRequestPacer struct {
	maxRequests int
	interval    time.Duration
	calls       int
	next        time.Time
}

type upstreamUsageRunBudgetExhausted struct{ max int }

func (e *upstreamUsageRunBudgetExhausted) Error() string {
	return fmt.Sprintf("上游使用日志单轮请求达到安全上限（%d 次）", e.max)
}

func newUpstreamUsageRequestPacer(maxRequests int, interval time.Duration) *upstreamUsageRequestPacer {
	return &upstreamUsageRequestPacer{maxRequests: maxRequests, interval: interval}
}

func (p *upstreamUsageRequestPacer) beforeRequest(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.maxRequests > 0 && p.calls >= p.maxRequests {
		return &upstreamUsageRunBudgetExhausted{max: p.maxRequests}
	}
	if wait := time.Until(p.next); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	p.calls++
	p.next = time.Now().Add(p.interval)
	return nil
}

func fetchNewAPIUsagePage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pageNumber int, pacer *upstreamUsageRequestPacer) (newAPIUsagePage[newAPIUsageItem], error) {
	return fetchNewAPIUsagePageWithDecoder(ctx, client, row, cred, from, to, pageNumber, pacer, decodeLegacyNewAPIUsageItem)
}

func fetchNewAPIPricingPage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pageNumber int, pacer *upstreamUsageRequestPacer) (newAPIUsagePage[newAPIPricingUsageItem], error) {
	return fetchNewAPIUsagePageWithDecoder(ctx, client, row, cred, from, to, pageNumber, pacer, decodeNewAPIUsageItem)
}

// fetchNewAPIUsagePageWithDecoder 取一页上游**消费**日志（type=2）。
// 保留这个名字与签名：既有调用方（计价台账、回填、趋势）都在用，不动它们。
func fetchNewAPIUsagePageWithDecoder[T any](ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pageNumber int, pacer *upstreamUsageRequestPacer, decodeItem func(json.RawMessage) (T, error)) (newAPIUsagePage[T], error) {
	return fetchNewAPILogPageWithType(ctx, client, row, cred, from, to, pageNumber, pacer, 2, decodeItem)
}

// fetchNewAPILogPageWithType 是上面那个函数的一般化：把写死的 type 抽成参数。
//
// 抽出来是为了让上游**错误**日志（type=5，见 channel_upstream_errorlog.go）
// 能复用同一套分页/限速/认证/页指纹，而不是另写一份——
// 另写必然漏掉其中几条（半开区间的 to-1、页指纹防漏页、401/403 转换）。
//
// logType 只由调用方以常量传入，不接受用户输入，无注入面。
func fetchNewAPILogPageWithType[T any](ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pageNumber int, pacer *upstreamUsageRequestPacer, logType int, decodeItem func(json.RawMessage) (T, error)) (newAPIUsagePage[T], error) {
	if err := pacer.beforeRequest(ctx); err != nil {
		return newAPIUsagePage[T]{}, err
	}
	if pageNumber <= 0 {
		return newAPIUsagePage[T]{}, fmt.Errorf("NewAPI 使用日志页码无效")
	}
	query := url.Values{}
	query.Set("p", strconv.Itoa(pageNumber))
	query.Set("page_size", strconv.Itoa(upstreamUsagePageSize))
	query.Set("type", strconv.Itoa(logType))
	query.Set("start_timestamp", strconv.FormatInt(from, 10))
	// NewAPI's existing filter is inclusive. Use to-1 so every Monitor window
	// remains the documented half-open interval [from,to), including splits.
	query.Set("end_timestamp", strconv.FormatInt(to-1, 10))
	headers := map[string]string{"Authorization": "Bearer " + cred.AccessToken, "New-Api-User": strconv.FormatInt(row.UserID, 10)}
	// 上游账户保存的是普通用户访问令牌，只能读取该账户自己的消费日志。
	// /api/log/ 是管理员全站日志接口，普通用户凭据会得到 403；不得为了同步
	// 上游账单而要求或保存管理员令牌。
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/log/self")+"?"+query.Encode(), headers, nil)
	if err != nil {
		var statusErr *upstreamHTTPError
		if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
			return newAPIUsagePage[T]{}, &upstreamAuthError{err: err}
		}
		return newAPIUsagePage[T]{}, err
	}
	var data struct {
		Items []json.RawMessage `json:"items"`
		Total json.RawMessage   `json:"total"`
	}
	if err := decodeNewAPIData(body, &data); err != nil {
		return newAPIUsagePage[T]{}, err
	}
	total, err := rawJSONNumber(data.Total)
	if err != nil || total < 0 || total != float64(int64(total)) {
		return newAPIUsagePage[T]{}, fmt.Errorf("NewAPI 使用日志缺少有效 total")
	}
	fingerprint, err := canonicalUsagePageFingerprint(data.Items)
	if err != nil {
		return newAPIUsagePage[T]{}, fmt.Errorf("NewAPI 使用日志条目无效: %w", err)
	}
	page := newAPIUsagePage[T]{
		Items:       make([]T, 0, len(data.Items)),
		Total:       int64(total),
		Fingerprint: fingerprint,
	}
	for _, itemJSON := range data.Items {
		item, decodeErr := decodeItem(itemJSON)
		if decodeErr != nil {
			return newAPIUsagePage[T]{}, decodeErr
		}
		page.Items = append(page.Items, item)
	}
	return page, nil
}

// decodeLegacyNewAPIUsageItem intentionally preserves the old, lightweight
// usage aggregation parser. The pricing ledger is gray-off by default, so its
// big.Rat/other parsing must not add CPU, allocation or compatibility risk to
// the already stable usage worker.
func decodeLegacyNewAPIUsageItem(itemJSON json.RawMessage) (newAPIUsageItem, error) {
	var raw struct {
		CreatedAt        json.RawMessage `json:"created_at"`
		Quota            json.RawMessage `json:"quota"`
		PromptTokens     json.RawMessage `json:"prompt_tokens"`
		CompletionTokens json.RawMessage `json:"completion_tokens"`
	}
	if err := json.Unmarshal(itemJSON, &raw); err != nil {
		return newAPIUsageItem{}, fmt.Errorf("NewAPI 使用日志条目无效: %w", err)
	}
	created, err := rawJSONNumber(raw.CreatedAt)
	if err != nil {
		return newAPIUsageItem{}, fmt.Errorf("NewAPI 使用日志缺少有效 created_at")
	}
	quota, err := rawJSONNumber(raw.Quota)
	if err != nil {
		return newAPIUsageItem{}, fmt.Errorf("NewAPI 使用日志缺少有效 quota")
	}
	prompt, _ := rawJSONNumber(raw.PromptTokens)
	completion, _ := rawJSONNumber(raw.CompletionTokens)
	return newAPIUsageItem{CreatedAt: int64(created), Quota: quota, PromptTokens: int64(prompt), CompletionTokens: int64(completion)}, nil
}

func canonicalUsagePageFingerprint(raw []json.RawMessage) ([32]byte, error) {
	// NewAPI 未修改版本不暴露真实日志 ID：响应里的 id 会被重写为
	// 页内序号。因此这里对完整首页条目做规范化指纹，用于扫描后复核。
	normalized := make([]any, 0, len(raw))
	for _, item := range raw {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(item))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return [32]byte{}, err
		}
		normalized = append(normalized, value)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// fetchNewAPIUsageItems 兼容未修改 NewAPI 的 OFFSET 分页协议。旧接口不暴露
// 真实 ID，所以 Monitor 通过 total、每页数量与扫描后首页指纹三重复核。
// 任一复核失败都整窗口放弃，不覆盖已有 SQLite 数据。
func fetchNewAPIUsageItems(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) ([]newAPIUsageItem, error) {
	first, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer)
	if err != nil {
		return nil, err
	}
	if first.Total > int64(maxUpstreamUsagePages*upstreamUsagePageSize) {
		return nil, &upstreamUsageWindowTooDense{total: first.Total}
	}
	expectedFirst := int(first.Total)
	if expectedFirst > upstreamUsagePageSize {
		expectedFirst = upstreamUsagePageSize
	}
	if len(first.Items) != expectedFirst {
		return nil, fmt.Errorf("NewAPI 使用日志首页数量异常（got=%d want=%d）", len(first.Items), expectedFirst)
	}
	items := append(make([]newAPIUsageItem, 0, first.Total), first.Items...)
	pageCount := int((first.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
	for pageNumber := 2; pageNumber <= pageCount; pageNumber++ {
		page, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, pageNumber, pacer)
		if err != nil {
			return nil, err
		}
		if page.Total != first.Total {
			return nil, fmt.Errorf("NewAPI 使用日志扫描期间 total 变化（%d -> %d）", first.Total, page.Total)
		}
		expected := upstreamUsagePageSize
		if pageNumber == pageCount && first.Total%upstreamUsagePageSize != 0 {
			expected = int(first.Total % upstreamUsagePageSize)
		}
		if len(page.Items) != expected {
			return nil, fmt.Errorf("NewAPI 使用日志第 %d 页数量异常（got=%d want=%d）", pageNumber, len(page.Items), expected)
		}
		items = append(items, page.Items...)
	}
	if pageCount > 1 {
		probe, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer)
		if err != nil {
			return nil, err
		}
		if probe.Total != first.Total || probe.Fingerprint != first.Fingerprint {
			return nil, fmt.Errorf("NewAPI 使用日志扫描期间首页已变化，窗口将重试")
		}
	}
	return items, nil
}

func fetchNewAPIUsageItemsSplit(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) ([]newAPIUsageItem, error) {
	items, err := fetchNewAPIUsageItems(ctx, client, row, cred, from, to, pacer)
	var dense *upstreamUsageWindowTooDense
	if !errors.As(err, &dense) {
		return items, err
	}
	if to-from <= 1 {
		return nil, fmt.Errorf("单秒上游使用日志超过 %d 条，无法安全拆分", maxUpstreamUsagePages*upstreamUsagePageSize)
	}
	mid := from + (to-from)/2
	left, err := fetchNewAPIUsageItemsSplit(ctx, client, row, cred, from, mid, pacer)
	if err != nil {
		return nil, err
	}
	right, err := fetchNewAPIUsageItemsSplit(ctx, client, row, cred, mid, to, pacer)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

// fetchNewAPIUsageWindow 读取一个不超过 24 小时的窗口。任何分页异常都会返回错误，
// 调用方不会覆盖既有本地汇总，因此“部分页成功”绝不会表现为完整消费数据。
func fetchNewAPIUsageWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64) (upstreamUsageResult, error) {
	return fetchNewAPIUsageWindowWithPacer(ctx, client, row, cred, from, to, nil)
}

func fetchNewAPIUsageWindowWithPacer(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	if cred.AccessToken == "" {
		return upstreamUsageResult{}, &upstreamAuthError{err: fmt.Errorf("NewAPI 访问令牌为空，请重新连接")}
	}
	if to <= from || to-from > 26*3600 {
		return upstreamUsageResult{}, fmt.Errorf("上游日志同步窗口无效")
	}
	buckets := map[int64]*ChannelUpstreamUsageHour{}
	items, err := fetchNewAPIUsageItemsSplit(ctx, client, row, cred, from, to, pacer)
	if err != nil {
		return upstreamUsageResult{}, err
	}
	for _, item := range items {
		// Upstream versions may apply filters differently; retain a local range
		// check before persisting the aggregate.
		if item.CreatedAt < from || item.CreatedAt >= to || item.Quota <= 0 {
			continue
		}
		hour := item.CreatedAt - item.CreatedAt%3600
		bucket := buckets[hour]
		if bucket == nil {
			bucket = &ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, BucketSeconds: 3600, Provider: row.Provider}
			buckets[hour] = bucket
		}
		bucket.Requests++
		bucket.Quota += item.Quota
		bucket.Tokens += item.PromptTokens + item.CompletionTokens
	}
	unit := row.BalanceUnit
	if unit <= 0 {
		unit = defaultNewAPIQuotaPerUSD
	}
	// 为每个已完整读取的小时保留零值桶，后续才能区分“零消费”与“尚未同步”。
	for hour := from - from%3600; hour+3600 <= to; hour += 3600 {
		if buckets[hour] == nil {
			buckets[hour] = &ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, BucketSeconds: 3600, Provider: row.Provider}
		}
	}
	out := make([]ChannelUpstreamUsageHour, 0, len(buckets))
	for _, bucket := range buckets {
		bucket.CostUSD = bucket.Quota / unit
		out = append(out, *bucket)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HourTs < out[j].HourTs })
	return upstreamUsageResult{Hours: out, DataUntil: to, Adapter: upstreamUsageAdapterNewAPILog}, nil
}

func newAPIBackfillProgress(checkpoint NewAPIUsageBackfillCheckpoint) string {
	pageCount := int((checkpoint.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
	completedPages := checkpoint.NextPage - 1
	if completedPages > pageCount {
		completedPages = pageCount
	}
	if completedPages < 1 && checkpoint.Total > 0 {
		completedPages = 1
	}
	return fmt.Sprintf("%s 小时已安全保存 %d/%d 条（%d/%d 页）",
		time.Unix(checkpoint.WindowFrom, 0).In(cstLocation).Format("2006-01-02 15:00"),
		checkpoint.SourceRows, checkpoint.Total, completedPages, pageCount)
}

func accumulateNewAPIBackfillPage(checkpoint *NewAPIUsageBackfillCheckpoint, items []newAPIUsageItem) {
	checkpoint.SourceRows += int64(len(items))
	for _, item := range items {
		if item.CreatedAt < checkpoint.WindowFrom || item.CreatedAt >= checkpoint.WindowTo || item.Quota <= 0 {
			continue
		}
		checkpoint.Requests++
		checkpoint.Quota += item.Quota
		checkpoint.Tokens += item.PromptTokens + item.CompletionTokens
	}
}

func validateNewAPIUsagePage[T any](page newAPIUsagePage[T], total int64, pageNumber, pageCount int) error {
	if page.Total != total {
		return fmt.Errorf("NewAPI 使用日志扫描期间 total 变化（%d -> %d）", total, page.Total)
	}
	expected := upstreamUsagePageSize
	if pageNumber == pageCount && total%upstreamUsagePageSize != 0 {
		expected = int(total % upstreamUsagePageSize)
	}
	if len(page.Items) != expected {
		return fmt.Errorf("NewAPI 使用日志第 %d 页数量异常（got=%d want=%d）", pageNumber, len(page.Items), expected)
	}
	return nil
}

func (m *Monitor) saveNewAPIUsageBackfillCheckpoint(ctx context.Context, checkpoint *NewAPIUsageBackfillCheckpoint) error {
	checkpoint.UpdatedAt = time.Now().Unix()
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "domain"}},
		UpdateAll: true,
	}).Create(checkpoint).Error
}

// syncNewAPIUsageBackfillWindow imports one closed historical hour with a
// durable page cursor. A request-budget yield is a normal scheduling event:
// completed pages stay only in the checkpoint and public hourly data remains
// untouched until the final total/fingerprint verification succeeds.
func (m *Monitor) syncNewAPIUsageBackfillWindowDirect(ctx context.Context, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer, requireStableProbe bool) (upstreamUsageResult, string, bool, error) {
	if to <= from || to-from > 3600 || from-from%3600 != (to-1)-(to-1)%3600 {
		return upstreamUsageResult{}, "", false, fmt.Errorf("NewAPI 历史补数窗口必须位于同一小时且不超过一小时")
	}
	var checkpoint NewAPIUsageBackfillCheckpoint
	loadErr := m.storeDB.WithContext(ctx).First(&checkpoint, "domain = ?", row.Domain).Error
	if loadErr != nil && !errors.Is(loadErr, gorm.ErrRecordNotFound) {
		return upstreamUsageResult{}, "", false, loadErr
	}
	hasCheckpoint := loadErr == nil
	if hasCheckpoint && (checkpoint.WindowFrom != from || checkpoint.WindowTo != to || checkpoint.NextPage < 2 || checkpoint.Total < 0 || checkpoint.SourceRows < 0 || checkpoint.SourceRows > checkpoint.Total || len(checkpoint.FirstPageFingerprint) != 64) {
		if err := m.storeDB.WithContext(ctx).Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillCheckpoint{}).Error; err != nil {
			return upstreamUsageResult{}, "", false, err
		}
		hasCheckpoint = false
		checkpoint = NewAPIUsageBackfillCheckpoint{}
	}

	client := m.channelUpstreamHTTPClient()
	var first newAPIUsagePage[newAPIUsageItem]
	firstLoaded := false
	checkpointVerified := false
	if hasCheckpoint {
		page, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return upstreamUsageResult{}, newAPIBackfillProgress(checkpoint), false, nil
			}
			return upstreamUsageResult{}, "", false, err
		}
		first, firstLoaded = page, true
		fingerprint := hex.EncodeToString(page.Fingerprint[:])
		if page.Total == checkpoint.Total && fingerprint == checkpoint.FirstPageFingerprint {
			checkpointVerified = true
		} else {
			if err := m.storeDB.WithContext(ctx).Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillCheckpoint{}).Error; err != nil {
				return upstreamUsageResult{}, "", false, err
			}
			hasCheckpoint = false
			checkpoint = NewAPIUsageBackfillCheckpoint{}
		}
	}

	if !hasCheckpoint {
		if !firstLoaded {
			page, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer)
			if err != nil {
				var exhausted *upstreamUsageRunBudgetExhausted
				if errors.As(err, &exhausted) {
					return upstreamUsageResult{}, fmt.Sprintf("等待下一轮安全额度继续 %s 小时", time.Unix(from, 0).In(cstLocation).Format("2006-01-02 15:00")), false, nil
				}
				return upstreamUsageResult{}, "", false, err
			}
			first = page
		}
		expectedFirst := int(first.Total)
		if expectedFirst > upstreamUsagePageSize {
			expectedFirst = upstreamUsagePageSize
		}
		if len(first.Items) != expectedFirst {
			return upstreamUsageResult{}, "", false, fmt.Errorf("NewAPI 使用日志首页数量异常（got=%d want=%d）", len(first.Items), expectedFirst)
		}
		checkpoint = NewAPIUsageBackfillCheckpoint{
			Domain: row.Domain, WindowFrom: from, WindowTo: to, NextPage: 2,
			Total: first.Total, FirstPageFingerprint: hex.EncodeToString(first.Fingerprint[:]),
		}
		accumulateNewAPIBackfillPage(&checkpoint, first.Items)
		if err := m.saveNewAPIUsageBackfillCheckpoint(ctx, &checkpoint); err != nil {
			return upstreamUsageResult{}, "", false, err
		}
	}

	pageCount := int((checkpoint.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
	for checkpoint.NextPage <= pageCount {
		pageNumber := checkpoint.NextPage
		page, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, pageNumber, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return upstreamUsageResult{}, newAPIBackfillProgress(checkpoint), false, nil
			}
			return upstreamUsageResult{}, "", false, err
		}
		if err := validateNewAPIUsagePage(page, checkpoint.Total, pageNumber, pageCount); err != nil {
			return upstreamUsageResult{}, "", false, err
		}
		accumulateNewAPIBackfillPage(&checkpoint, page.Items)
		checkpoint.NextPage++
		if err := m.saveNewAPIUsageBackfillCheckpoint(ctx, &checkpoint); err != nil {
			return upstreamUsageResult{}, "", false, err
		}
	}
	if checkpoint.SourceRows != checkpoint.Total {
		return upstreamUsageResult{}, "", false, fmt.Errorf("NewAPI 使用日志分页数量不完整（got=%d want=%d）", checkpoint.SourceRows, checkpoint.Total)
	}
	if (pageCount > 1 || requireStableProbe) && !checkpointVerified {
		probe, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return upstreamUsageResult{}, newAPIBackfillProgress(checkpoint), false, nil
			}
			return upstreamUsageResult{}, "", false, err
		}
		if probe.Total != checkpoint.Total || hex.EncodeToString(probe.Fingerprint[:]) != checkpoint.FirstPageFingerprint {
			if deleteErr := m.storeDB.WithContext(ctx).Where("domain = ?", row.Domain).Delete(&NewAPIUsageBackfillCheckpoint{}).Error; deleteErr != nil {
				return upstreamUsageResult{}, "", false, deleteErr
			}
			return upstreamUsageResult{}, "", false, fmt.Errorf("NewAPI 使用日志扫描期间首页已变化，窗口将重试")
		}
	}

	unit := row.BalanceUnit
	if unit <= 0 {
		unit = defaultNewAPIQuotaPerUSD
	}
	hour := ChannelUpstreamUsageHour{
		Domain: row.Domain, HourTs: from, BucketSeconds: to - from,
		Requests: checkpoint.Requests, Tokens: checkpoint.Tokens, Quota: checkpoint.Quota,
		CostUSD: checkpoint.Quota / unit, Provider: row.Provider,
	}
	return upstreamUsageResult{Hours: []ChannelUpstreamUsageHour{hour}, DataUntil: to, Adapter: upstreamUsageAdapterNewAPILog}, "", true, nil
}

const (
	newAPIBackfillSegmentPending  = "pending"
	newAPIBackfillSegmentComplete = "complete"
)

func isNewAPIBackfillTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var statusErr *upstreamHTTPError
	if errors.As(err, &statusErr) && (statusErr.Status == http.StatusRequestTimeout || statusErr.Status == http.StatusGatewayTimeout || statusErr.Status == 524) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "timeout") || strings.Contains(message, "deadline exceeded") ||
		strings.Contains(message, "awaiting headers") || strings.Contains(message, "请求超时")
}

func newAPIBackfillNeedsAdaptiveSegments(row ChannelUpstreamAccount) bool {
	return row.UsageBackfillConsecutiveFails > 0 && isNewAPIBackfillTimeout(errors.New(row.UsageBackfillLastError))
}

func newAPIBackfillSegmentProgress(from int64, complete, total int, current string) string {
	progress := fmt.Sprintf("%s 小时慢查询降档已完成 %d/%d 个子窗口",
		time.Unix(from, 0).In(cstLocation).Format("2006-01-02 15:00"), complete, total)
	if strings.TrimSpace(current) != "" {
		progress += "；" + current
	}
	return progress
}

func newAPIBackfillOperationBudgetYield(ctx context.Context, from int64, complete, total int) (upstreamUsageResult, string, bool, error) {
	return upstreamUsageResult{}, newAPIBackfillSegmentProgress(from, complete, total, "本轮历史读取时间预算已用完，断点已保留"), false, nil
}

func (m *Monitor) saveNewAPIUsageBackfillSegment(ctx context.Context, segment NewAPIUsageBackfillSegment) error {
	segment.UpdatedAt = time.Now().Unix()
	segment.Status = newAPIBackfillSegmentComplete
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "domain"}, {Name: "hour_from"}, {Name: "segment_from"}},
			UpdateAll: true,
		}).Create(&segment).Error; err != nil {
			return err
		}
		return tx.Where("domain = ? AND window_from = ? AND window_to = ?", segment.Domain, segment.SegmentFrom, segment.SegmentTo).
			Delete(&NewAPIUsageBackfillCheckpoint{}).Error
	})
}

func (m *Monitor) ensureNewAPIUsageBackfillSegmentPlan(ctx context.Context, domain string, from, to int64) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ?", domain, from).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			step := int64(newAPIBackfillAdaptiveSegment / time.Second)
			now := time.Now().Unix()
			for segmentFrom := from; segmentFrom < to; segmentFrom += step {
				segment := NewAPIUsageBackfillSegment{
					Domain: domain, HourFrom: from, SegmentFrom: segmentFrom, SegmentTo: min(to, segmentFrom+step),
					Status: newAPIBackfillSegmentPending, UpdatedAt: now,
				}
				if err := tx.Create(&segment).Error; err != nil {
					return err
				}
			}
		}
		// Switching topology and discarding an unfinished parent accumulation is
		// one SQLite commit. A child-page checkpoint is deliberately retained.
		return tx.Where("domain = ? AND window_from = ? AND window_to = ?", domain, from, to).
			Delete(&NewAPIUsageBackfillCheckpoint{}).Error
	})
}

func nextNewAPIBackfillSegmentSize(seconds int64) int64 {
	switch {
	case seconds > 60:
		return 60
	case seconds > int64(newAPIBackfillAdaptiveMinSegment/time.Second):
		return int64(newAPIBackfillAdaptiveMinSegment / time.Second)
	default:
		return 0
	}
}

func (m *Monitor) splitNewAPIUsageBackfillSegment(ctx context.Context, segment NewAPIUsageBackfillSegment) error {
	step := nextNewAPIBackfillSegmentSize(segment.SegmentTo - segment.SegmentFrom)
	if step == 0 {
		return fmt.Errorf("NewAPI 历史补数最小子窗口仍然超时")
	}
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Where("domain = ? AND hour_from = ? AND segment_from = ? AND status = ?", segment.Domain, segment.HourFrom, segment.SegmentFrom, newAPIBackfillSegmentPending).
			Delete(&NewAPIUsageBackfillSegment{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("NewAPI 历史补数子窗口状态已变化")
		}
		if err := tx.Where("domain = ? AND window_from = ? AND window_to = ?", segment.Domain, segment.SegmentFrom, segment.SegmentTo).
			Delete(&NewAPIUsageBackfillCheckpoint{}).Error; err != nil {
			return err
		}
		now := time.Now().Unix()
		for childFrom := segment.SegmentFrom; childFrom < segment.SegmentTo; childFrom += step {
			child := NewAPIUsageBackfillSegment{
				Domain: segment.Domain, HourFrom: segment.HourFrom, SegmentFrom: childFrom,
				SegmentTo: min(segment.SegmentTo, childFrom+step), Status: newAPIBackfillSegmentPending, UpdatedAt: now,
			}
			if err := tx.Create(&child).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func validateNewAPIUsageBackfillSegments(segments []NewAPIUsageBackfillSegment, from, to int64, requireComplete bool) error {
	expected := from
	for _, segment := range segments {
		if segment.HourFrom != from || segment.SegmentFrom != expected || segment.SegmentTo <= segment.SegmentFrom || segment.SegmentTo > to ||
			(segment.Status != newAPIBackfillSegmentPending && segment.Status != newAPIBackfillSegmentComplete) {
			return fmt.Errorf("NewAPI 历史补数子窗口覆盖不连续或证据无效")
		}
		if segment.Status == newAPIBackfillSegmentComplete && (segment.Total < 0 || segment.SourceRows != segment.Total || len(segment.FirstPageFingerprint) != 64) {
			return fmt.Errorf("NewAPI 历史补数子窗口完整性证据无效")
		}
		if requireComplete && segment.Status != newAPIBackfillSegmentComplete {
			return fmt.Errorf("NewAPI 历史补数子窗口尚未全部完成")
		}
		expected = segment.SegmentTo
	}
	if expected != to {
		return fmt.Errorf("NewAPI 历史补数子窗口尚未完整覆盖父小时")
	}
	return nil
}

func (m *Monitor) syncNewAPIUsageBackfillWindowSegmented(ctx context.Context, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, string, bool, error) {
	var stored []NewAPIUsageBackfillSegment
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND hour_from = ?", row.Domain, from).
		Order("segment_from ASC").Find(&stored).Error; err != nil {
		return upstreamUsageResult{}, "", false, err
	}
	if len(stored) == 0 {
		return upstreamUsageResult{}, "", false, fmt.Errorf("NewAPI 历史补数降档计划缺失")
	}
	if err := validateNewAPIUsageBackfillSegments(stored, from, to, false); err != nil {
		return upstreamUsageResult{}, "", false, err
	}
	completed := 0
	for index := range stored {
		segment := stored[index]
		if segment.Status == newAPIBackfillSegmentComplete {
			completed++
			continue
		}

		result, progress, complete, err := m.syncNewAPIUsageBackfillWindowDirect(ctx, row, cred, segment.SegmentFrom, segment.SegmentTo, pacer, true)
		if err != nil {
			// The lane's 45-second parent deadline is a cooperative yield, not
			// evidence that this source interval is slow. Preserve topology and
			// checkpoints; only a request-local timeout may split the interval.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return newAPIBackfillOperationBudgetYield(ctx, from, completed, len(stored))
			}
			if isNewAPIBackfillTimeout(err) && segment.SegmentTo-segment.SegmentFrom > int64(newAPIBackfillAdaptiveMinSegment/time.Second) {
				if splitErr := m.splitNewAPIUsageBackfillSegment(ctx, segment); splitErr != nil {
					return upstreamUsageResult{}, "", false, splitErr
				}
				return upstreamUsageResult{}, newAPIBackfillSegmentProgress(from, completed, len(stored), "子窗口再次超时，已持久降档"), false, nil
			}
			return upstreamUsageResult{}, "", false, err
		}
		if !complete {
			return upstreamUsageResult{}, newAPIBackfillSegmentProgress(from, completed, len(stored), progress), false, nil
		}
		if len(result.Hours) != 1 || result.Hours[0].HourTs != segment.SegmentFrom || result.Hours[0].BucketSeconds != segment.SegmentTo-segment.SegmentFrom {
			return upstreamUsageResult{}, "", false, fmt.Errorf("NewAPI 历史补数子窗口汇总无效")
		}
		var checkpoint NewAPIUsageBackfillCheckpoint
		if err := m.storeDB.WithContext(ctx).First(&checkpoint, "domain = ? AND window_from = ? AND window_to = ?", row.Domain, segment.SegmentFrom, segment.SegmentTo).Error; err != nil {
			return upstreamUsageResult{}, "", false, err
		}
		segment.Total, segment.SourceRows = checkpoint.Total, checkpoint.SourceRows
		segment.Requests, segment.Tokens, segment.Quota = result.Hours[0].Requests, result.Hours[0].Tokens, result.Hours[0].Quota
		segment.FirstPageFingerprint = checkpoint.FirstPageFingerprint
		if err := m.saveNewAPIUsageBackfillSegment(ctx, segment); err != nil {
			return upstreamUsageResult{}, "", false, err
		}
		segment.Status = newAPIBackfillSegmentComplete
		stored[index] = segment
		completed++
	}

	if err := validateNewAPIUsageBackfillSegments(stored, from, to, true); err != nil {
		return upstreamUsageResult{}, "", false, err
	}
	hour := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: from, BucketSeconds: to - from, Provider: row.Provider}
	for _, segment := range stored {
		hour.Requests += segment.Requests
		hour.Tokens += segment.Tokens
		hour.Quota += segment.Quota
	}
	unit := row.BalanceUnit
	if unit <= 0 {
		unit = defaultNewAPIQuotaPerUSD
	}
	hour.CostUSD = hour.Quota / unit
	return upstreamUsageResult{Hours: []ChannelUpstreamUsageHour{hour}, DataUntil: to, Adapter: upstreamUsageAdapterNewAPILog}, "", true, nil
}

func (m *Monitor) syncNewAPIUsageBackfillWindow(ctx context.Context, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, string, bool, error) {
	var checkpoint NewAPIUsageBackfillCheckpoint
	checkpointErr := m.storeDB.WithContext(ctx).First(&checkpoint, "domain = ?", row.Domain).Error
	if checkpointErr != nil && !errors.Is(checkpointErr, gorm.ErrRecordNotFound) {
		return upstreamUsageResult{}, "", false, checkpointErr
	}
	var segmentCount int64
	if err := m.storeDB.WithContext(ctx).Model(&NewAPIUsageBackfillSegment{}).
		Where("domain = ? AND hour_from = ?", row.Domain, from).Count(&segmentCount).Error; err != nil {
		return upstreamUsageResult{}, "", false, err
	}
	checkpointIsChild := checkpointErr == nil && checkpoint.WindowFrom >= from && checkpoint.WindowTo <= to && (checkpoint.WindowFrom != from || checkpoint.WindowTo != to)
	if segmentCount > 0 || checkpointIsChild || newAPIBackfillNeedsAdaptiveSegments(row) {
		if err := m.ensureNewAPIUsageBackfillSegmentPlan(ctx, row.Domain, from, to); err != nil {
			return upstreamUsageResult{}, "", false, err
		}
		return m.syncNewAPIUsageBackfillWindowSegmented(ctx, row, cred, from, to, pacer)
	}
	result, progress, complete, err := m.syncNewAPIUsageBackfillWindowDirect(ctx, row, cred, from, to, pacer, false)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return newAPIBackfillOperationBudgetYield(ctx, from, 0, 1)
	}
	if !isNewAPIBackfillTimeout(err) {
		return result, progress, complete, err
	}
	if planErr := m.ensureNewAPIUsageBackfillSegmentPlan(ctx, row.Domain, from, to); planErr != nil {
		return upstreamUsageResult{}, "", false, planErr
	}
	return upstreamUsageResult{}, newAPIBackfillSegmentProgress(from, 0, int((to-from)/int64(newAPIBackfillAdaptiveSegment/time.Second)), "整小时查询超时，已持久降档"), false, nil
}

func (m *Monitor) syncNewAPIUsage(ctx context.Context, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	return fetchNewAPIUsageWindowWithPacer(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, pacer)
}

type sub2APIUsageMetric struct {
	Requests int64
	Tokens   int64
	CostUSD  float64
}

func decodeSub2APIUsageMetric(requestsRaw, tokensRaw, costRaw json.RawMessage) (sub2APIUsageMetric, error) {
	requests, err := rawJSONNumber(requestsRaw)
	if err != nil || requests < 0 || requests != math.Trunc(requests) || requests > math.MaxInt64 {
		return sub2APIUsageMetric{}, fmt.Errorf("缺少有效 total_requests")
	}
	tokens, err := rawJSONNumber(tokensRaw)
	if err != nil || tokens < 0 || tokens != math.Trunc(tokens) || tokens > math.MaxInt64 {
		return sub2APIUsageMetric{}, fmt.Errorf("缺少有效 total_tokens")
	}
	cost, err := rawJSONNumber(costRaw)
	if err != nil || cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return sub2APIUsageMetric{}, fmt.Errorf("缺少有效 total_actual_cost")
	}
	return sub2APIUsageMetric{Requests: int64(requests), Tokens: int64(tokens), CostUSD: cost}, nil
}

func validateSub2APIUsageWindow(from, to int64) (string, error) {
	if from != cstDayStart(from) || to <= from || to > from+86400 {
		return "", fmt.Errorf("Sub2API 使用量必须按单个中国自然日同步")
	}
	return time.Unix(from, 0).In(cstLocation).Format("2006-01-02"), nil
}

func sub2APIUsageHeaders(cred sub2APICredential) map[string]string {
	return map[string]string{"Authorization": "Bearer " + cred.AccessToken}
}

func wrapSub2APIUsageHTTPError(err error) error {
	if err == nil {
		return nil
	}
	var statusErr *upstreamHTTPError
	if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
		return &upstreamAuthError{err: err}
	}
	return err
}

// fetchSub2APIUsageTrend reads a single account-scoped aggregate query. It does
// not download raw prompts, responses, API keys, IPs, or individual log rows.
func fetchSub2APIUsageTrend(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	day, err := validateSub2APIUsageWindow(from, to)
	if err != nil {
		return upstreamUsageResult{}, err
	}
	if strings.TrimSpace(cred.AccessToken) == "" {
		return upstreamUsageResult{}, &upstreamAuthError{err: fmt.Errorf("Sub2API 访问令牌为空，请重新连接")}
	}
	if err := pacer.beforeRequest(ctx); err != nil {
		return upstreamUsageResult{}, err
	}
	query := url.Values{}
	query.Set("start_date", day)
	query.Set("end_date", day)
	query.Set("timezone", "Asia/Shanghai")
	query.Set("granularity", "hour")
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/v1/usage/dashboard/trend")+"?"+query.Encode(), sub2APIUsageHeaders(cred), nil)
	if err != nil {
		return upstreamUsageResult{}, wrapSub2APIUsageHTTPError(err)
	}
	var data struct {
		Trend []struct {
			Date        string          `json:"date"`
			Requests    json.RawMessage `json:"requests"`
			TotalTokens json.RawMessage `json:"total_tokens"`
			ActualCost  json.RawMessage `json:"actual_cost"`
		} `json:"trend"`
		StartDate   string `json:"start_date"`
		EndDate     string `json:"end_date"`
		Granularity string `json:"granularity"`
	}
	if err := decodeSub2APIData(body, &data); err != nil {
		return upstreamUsageResult{}, fmt.Errorf("Sub2API 小时用量响应无效: %w", err)
	}
	if data.StartDate != day || data.EndDate != day || data.Granularity != "hour" {
		return upstreamUsageResult{}, fmt.Errorf("Sub2API 用量返回的统计范围与请求不一致")
	}
	buckets := make(map[int64]ChannelUpstreamUsageHour, len(data.Trend))
	for _, item := range data.Trend {
		hour, parseErr := time.ParseInLocation("2006-01-02 15:04", strings.TrimSpace(item.Date), cstLocation)
		if parseErr != nil || hour.Minute() != 0 || hour.Second() != 0 || hour.Unix() < from || hour.Unix() >= from+86400 {
			return upstreamUsageResult{}, fmt.Errorf("Sub2API 用量包含越界或无效小时")
		}
		if _, duplicate := buckets[hour.Unix()]; duplicate {
			return upstreamUsageResult{}, fmt.Errorf("Sub2API 用量包含重复小时")
		}
		metric, metricErr := decodeSub2APIUsageMetric(item.Requests, item.TotalTokens, item.ActualCost)
		if metricErr != nil {
			return upstreamUsageResult{}, fmt.Errorf("Sub2API %s 用量无效: %w", item.Date, metricErr)
		}
		buckets[hour.Unix()] = ChannelUpstreamUsageHour{
			Domain: row.Domain, HourTs: hour.Unix(), BucketSeconds: 3600,
			Requests: metric.Requests, Tokens: metric.Tokens, Quota: metric.CostUSD,
			CostUSD: metric.CostUSD, Provider: row.Provider,
		}
	}
	hours := make([]ChannelUpstreamUsageHour, 0, int(math.Ceil(float64(to-from)/3600)))
	for hour := from; hour < to; hour += 3600 {
		seconds := int64(3600)
		if hour+seconds > to {
			seconds = to - hour
		}
		bucket, ok := buckets[hour]
		if !ok {
			bucket = ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, Provider: row.Provider}
		}
		bucket.BucketSeconds = seconds
		hours = append(hours, bucket)
	}
	return upstreamUsageResult{Hours: hours, DataUntil: to, Adapter: upstreamUsageAdapterSub2Trend}, nil
}

// fetchSub2APIUsageStats is the compatibility path for older Sub2API sites
// without the hourly trend route. It stores one truthful natural-day bucket
// instead of inventing an hourly distribution.
func fetchSub2APIUsageStats(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	day, err := validateSub2APIUsageWindow(from, to)
	if err != nil {
		return upstreamUsageResult{}, err
	}
	if strings.TrimSpace(cred.AccessToken) == "" {
		return upstreamUsageResult{}, &upstreamAuthError{err: fmt.Errorf("Sub2API 访问令牌为空，请重新连接")}
	}
	if err := pacer.beforeRequest(ctx); err != nil {
		return upstreamUsageResult{}, err
	}
	query := url.Values{}
	query.Set("start_date", day)
	query.Set("end_date", day)
	query.Set("timezone", "Asia/Shanghai")
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/v1/usage/stats")+"?"+query.Encode(), sub2APIUsageHeaders(cred), nil)
	if err != nil {
		return upstreamUsageResult{}, wrapSub2APIUsageHTTPError(err)
	}
	var data struct {
		TotalRequests   json.RawMessage `json:"total_requests"`
		TotalTokens     json.RawMessage `json:"total_tokens"`
		TotalActualCost json.RawMessage `json:"total_actual_cost"`
	}
	if err := decodeSub2APIData(body, &data); err != nil {
		return upstreamUsageResult{}, fmt.Errorf("Sub2API 单日用量响应无效: %w", err)
	}
	metric, err := decodeSub2APIUsageMetric(data.TotalRequests, data.TotalTokens, data.TotalActualCost)
	if err != nil {
		return upstreamUsageResult{}, fmt.Errorf("Sub2API %s 单日用量无效: %w", day, err)
	}
	bucket := ChannelUpstreamUsageHour{
		Domain: row.Domain, HourTs: from, BucketSeconds: to - from,
		Requests: metric.Requests, Tokens: metric.Tokens, Quota: metric.CostUSD,
		CostUSD: metric.CostUSD, Provider: row.Provider,
	}
	return upstreamUsageResult{Hours: []ChannelUpstreamUsageHour{bucket}, DataUntil: to, Adapter: upstreamUsageAdapterSub2Stats}, nil
}

func sub2APIUsageRouteMissing(err error) bool {
	var statusErr *upstreamHTTPError
	return errors.As(err, &statusErr) && (statusErr.Status == http.StatusNotFound || statusErr.Status == http.StatusMethodNotAllowed)
}

func fetchSub2APIUsageWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, from, to int64, pacer *upstreamUsageRequestPacer, adapter string) (upstreamUsageResult, error) {
	if adapter == upstreamUsageAdapterSub2Stats {
		return fetchSub2APIUsageStats(ctx, client, row, cred, from, to, pacer)
	}
	result, err := fetchSub2APIUsageTrend(ctx, client, row, cred, from, to, pacer)
	if err == nil || !sub2APIUsageRouteMissing(err) {
		return result, err
	}
	return fetchSub2APIUsageStats(ctx, client, row, cred, from, to, pacer)
}

func syncSub2APIUsage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, from, to int64, pacer *upstreamUsageRequestPacer, adapter string) (upstreamUsageResult, sub2APICredential, error) {
	refreshed := false
	if cred.AccessToken == "" || cred.ExpiresAt <= time.Now().Add(2*time.Minute).Unix() {
		var err error
		cred, err = refreshSub2API(ctx, client, row, cred)
		if err != nil {
			return upstreamUsageResult{}, cred, err
		}
		refreshed = true
	}
	result, err := fetchSub2APIUsageWindow(ctx, client, row, cred, from, to, pacer, adapter)
	if err == nil {
		return result, cred, nil
	}
	var statusErr *upstreamHTTPError
	if !refreshed && errors.As(err, &statusErr) && statusErr.Status == http.StatusUnauthorized {
		updated, refreshErr := refreshSub2API(ctx, client, row, cred)
		if refreshErr != nil {
			return upstreamUsageResult{}, cred, refreshErr
		}
		cred = updated
		result, err = fetchSub2APIUsageWindow(ctx, client, row, cred, from, to, pacer, adapter)
		if err == nil {
			return result, cred, nil
		}
	}
	return upstreamUsageResult{}, cred, err
}

type aiCodeWithUsageMetric struct {
	Cost     float64
	Tokens   int64
	Requests int64
}

func decodeAICodeWithUsageMetric(costRaw, tokensRaw, requestsRaw json.RawMessage) (aiCodeWithUsageMetric, error) {
	cost, err := rawJSONNumber(costRaw)
	if err != nil || cost < 0 {
		return aiCodeWithUsageMetric{}, fmt.Errorf("缺少有效 cost")
	}
	tokens, err := rawJSONNumber(tokensRaw)
	if err != nil || tokens < 0 || tokens != math.Trunc(tokens) || tokens > math.MaxInt64 {
		return aiCodeWithUsageMetric{}, fmt.Errorf("缺少有效 total_tokens")
	}
	requests, err := rawJSONNumber(requestsRaw)
	if err != nil || requests < 0 || requests != math.Trunc(requests) || requests > math.MaxInt64 {
		return aiCodeWithUsageMetric{}, fmt.Errorf("缺少有效 requests")
	}
	return aiCodeWithUsageMetric{Cost: cost, Tokens: int64(tokens), Requests: int64(requests)}, nil
}

func fetchAICodeWithUsageWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, apiKey string, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	if strings.TrimSpace(apiKey) == "" {
		return upstreamUsageResult{}, &upstreamAuthError{err: fmt.Errorf("AICodeWith API Key 为空，请重新连接")}
	}
	if to <= from || from != cstDayStart(from) {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量必须按中国自然日同步")
	}
	lastDay := cstDayStart(to - 1)
	days := int((lastDay-from)/86400) + 1
	if days <= 0 || days > aiCodeWithUsageMaxDays {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量窗口最多 %d 天", aiCodeWithUsageMaxDays)
	}
	if err := pacer.beforeRequest(ctx); err != nil {
		return upstreamUsageResult{}, err
	}
	startText := time.Unix(from, 0).In(cstLocation).Format("2006-01-02")
	endText := time.Unix(lastDay, 0).In(cstLocation).Format("2006-01-02")
	query := url.Values{}
	query.Set("start", startText)
	query.Set("end", endText)
	query.Set("group_by", "day")
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, aicodeWithEndpoint(row.BaseURL, "/api/v1/api-keys/usage")+"?"+query.Encode(), map[string]string{
		"Authorization": "Bearer " + apiKey,
	}, nil)
	if err != nil {
		var statusErr *upstreamHTTPError
		if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
			return upstreamUsageResult{}, &upstreamAuthError{err: err}
		}
		return upstreamUsageResult{}, err
	}
	var envelope struct {
		Data struct {
			APIKeyID int64 `json:"api_key_id"`
			Period   struct {
				Start string `json:"start"`
				End   string `json:"end"`
			} `json:"period"`
			GroupBy string `json:"group_by"`
			Summary struct {
				Cost        json.RawMessage `json:"cost"`
				TotalTokens json.RawMessage `json:"total_tokens"`
				Requests    json.RawMessage `json:"requests"`
			} `json:"summary"`
			Daily []struct {
				Date        string          `json:"date"`
				Cost        json.RawMessage `json:"cost"`
				TotalTokens json.RawMessage `json:"total_tokens"`
				Requests    json.RawMessage `json:"requests"`
			} `json:"daily"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量响应格式无效")
	}
	if envelope.Data.GroupBy != "day" || envelope.Data.Period.Start != startText || envelope.Data.Period.End != endText {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量返回的统计范围与请求不一致")
	}
	if envelope.Data.APIKeyID <= 0 {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量缺少有效 api_key_id")
	}
	summary, err := decodeAICodeWithUsageMetric(envelope.Data.Summary.Cost, envelope.Data.Summary.TotalTokens, envelope.Data.Summary.Requests)
	if err != nil {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量 summary 无效: %w", err)
	}
	byDay := make(map[int64]aiCodeWithUsageMetric, len(envelope.Data.Daily))
	var summed aiCodeWithUsageMetric
	for _, item := range envelope.Data.Daily {
		day, parseErr := time.ParseInLocation("2006-01-02", item.Date, cstLocation)
		if parseErr != nil || day.Format("2006-01-02") != item.Date || day.Unix() < from || day.Unix() > lastDay {
			return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量包含越界或无效日期")
		}
		if _, duplicate := byDay[day.Unix()]; duplicate {
			return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量包含重复日期")
		}
		metric, metricErr := decodeAICodeWithUsageMetric(item.Cost, item.TotalTokens, item.Requests)
		if metricErr != nil {
			return upstreamUsageResult{}, fmt.Errorf("AICodeWith %s 使用量无效: %w", item.Date, metricErr)
		}
		byDay[day.Unix()] = metric
		summed.Cost += metric.Cost
		summed.Tokens += metric.Tokens
		summed.Requests += metric.Requests
	}
	// summary 是服务端对同一 Key/同一范围的独立控制总数。对不上时整窗口放弃，
	// 不用部分 daily 覆盖已有 SQLite 账单。
	if math.Abs(summed.Cost-summary.Cost) > 0.0001 || summed.Tokens != summary.Tokens || summed.Requests != summary.Requests {
		return upstreamUsageResult{}, fmt.Errorf("AICodeWith 使用量分日合计与 summary 不一致")
	}
	hours := make([]ChannelUpstreamUsageHour, 0, days)
	unit := row.BalanceUnit
	if unit <= 0 {
		// 已保存的春秋账户会从余额响应持久化真实换算单位。早期仅支持
		// USD 的账户可能没有单位，继续按 1 兼容；新 CNY 账户不会走这里。
		unit = 1
	}
	for day := from; day <= lastDay; day += 86400 {
		bucketTo := day + 86400
		if bucketTo > to {
			bucketTo = to
		}
		metric := byDay[day]
		hours = append(hours, ChannelUpstreamUsageHour{
			Domain: row.Domain, HourTs: day, BucketSeconds: bucketTo - day,
			Requests: metric.Requests, Tokens: metric.Tokens, Quota: metric.Cost,
			CostUSD: metric.Cost / unit, Provider: row.Provider,
		})
	}
	return upstreamUsageResult{Hours: hours, DataUntil: to, Adapter: upstreamUsageAdapterAICodeWith, SourceKeyID: envelope.Data.APIKeyID}, nil
}

func (m *Monitor) syncAICodeWithUsage(ctx context.Context, row ChannelUpstreamAccount, cred aiCodeWithCredential, from, to int64, pacers map[string]*upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	keys, err := aiCodeWithCredentialKeys(cred)
	if err != nil || len(keys) == 0 {
		if err == nil {
			err = fmt.Errorf("AICodeWith API Key 为空，请重新连接")
		}
		return upstreamUsageResult{}, &upstreamAuthError{err: err}
	}
	combined := make(map[int64]ChannelUpstreamUsageHour)
	seenKeyIDs := make(map[int64]bool, len(keys))
	for index, apiKey := range keys {
		pacer := pacers[apiKey]
		if pacer == nil {
			pacer = newUpstreamUsageRequestPacer(aiCodeWithUsageMaxRequestsPerRun, aiCodeWithUsageRequestInterval)
			pacers[apiKey] = pacer
		}
		result, fetchErr := fetchAICodeWithUsageWindow(ctx, m.channelUpstreamHTTPClient(), row, apiKey, from, to, pacer)
		if fetchErr != nil {
			return upstreamUsageResult{}, fmt.Errorf("第 %d 把 AICodeWith API Key: %w", index+1, fetchErr)
		}
		if seenKeyIDs[result.SourceKeyID] {
			return upstreamUsageResult{}, fmt.Errorf("多把 AICodeWith 凭据解析为同一个 api_key_id，拒绝重复累计")
		}
		seenKeyIDs[result.SourceKeyID] = true
		for _, bucket := range result.Hours {
			current, exists := combined[bucket.HourTs]
			if exists && current.BucketSeconds != bucket.BucketSeconds {
				return upstreamUsageResult{}, fmt.Errorf("AICodeWith 多 Key 账单覆盖范围不一致")
			}
			if !exists {
				current = ChannelUpstreamUsageHour{
					Domain: row.Domain, HourTs: bucket.HourTs, BucketSeconds: bucket.BucketSeconds,
					Provider: row.Provider,
				}
			}
			current.Requests += bucket.Requests
			current.Tokens += bucket.Tokens
			current.Quota += bucket.Quota
			current.CostUSD += bucket.CostUSD
			combined[bucket.HourTs] = current
		}
	}
	hours := make([]ChannelUpstreamUsageHour, 0, len(combined))
	for _, bucket := range combined {
		hours = append(hours, bucket)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].HourTs < hours[j].HourTs })
	return upstreamUsageResult{Hours: hours, DataUntil: to, Adapter: upstreamUsageAdapterAICodeWith}, nil
}

func newAICodeWithRoundID(domain, kind, version string, from, to int64) string {
	h := sha256Sum(strings.Join([]string{domain, kind, version, strconv.FormatInt(from, 10), strconv.FormatInt(to, 10), strconv.FormatInt(time.Now().UnixNano(), 10)}, "\x00"))
	return fmt.Sprintf("acwr_%x", h[:16])
}

func (m *Monitor) ensureAICodeWithUsageRound(ctx context.Context, row ChannelUpstreamAccount, version, kind string, from, to int64, total int, now int64) (AICodeWithUsageRound, error) {
	var round AICodeWithUsageRound
	err := m.storeDB.WithContext(ctx).First(&round, "domain = ? AND kind = ?", row.Domain, kind).Error
	if err == nil && round.CredentialSetVersion == version && round.Status == upstreamStatusPending {
		return round, nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return round, err
	}
	oldRoundID := round.RoundID
	round = AICodeWithUsageRound{
		Domain: row.Domain, Kind: kind, RoundID: newAICodeWithRoundID(row.Domain, kind, version, from, to),
		CredentialSetVersion: version, WindowFrom: from, WindowTo: to, TotalKeys: total,
		Status: upstreamStatusPending, CreatedAt: now, UpdatedAt: now,
	}
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if oldRoundID != "" {
			if err := tx.Where("domain = ? AND round_id = ?", row.Domain, oldRoundID).Delete(&AICodeWithUsageStage{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("domain = ? AND kind = ?", row.Domain, kind).Delete(&AICodeWithUsageRound{}).Error; err != nil {
			return err
		}
		return tx.Create(&round).Error
	})
	return round, err
}

func (m *Monitor) stageAICodeWithKeyResult(ctx context.Context, round AICodeWithUsageRound, state *AICodeWithKeySyncState, result upstreamUsageResult, now int64) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if result.SourceKeyID <= 0 {
			return fmt.Errorf("AICodeWith 使用量缺少有效 api_key_id")
		}
		var duplicate int64
		if err := tx.Model(&AICodeWithKeySyncState{}).
			Where("domain = ? AND credential_set_version = ? AND slot_id <> ? AND source_key_id = ?", state.Domain, round.CredentialSetVersion, state.SlotID, result.SourceKeyID).
			Count(&duplicate).Error; err != nil {
			return err
		}
		if duplicate > 0 {
			return fmt.Errorf("多把 AICodeWith 凭据解析为同一个 api_key_id，拒绝重复累计")
		}
		if err := tx.Where("domain = ? AND round_id = ? AND slot_id = ?", state.Domain, round.RoundID, state.SlotID).Delete(&AICodeWithUsageStage{}).Error; err != nil {
			return err
		}
		for _, bucket := range result.Hours {
			stage := AICodeWithUsageStage{
				Domain: state.Domain, RoundID: round.RoundID, SlotID: state.SlotID,
				HourTs: bucket.HourTs, CredentialSetVersion: round.CredentialSetVersion,
				BucketSeconds: bucket.BucketSeconds, Requests: bucket.Requests, Tokens: bucket.Tokens,
				Quota: bucket.Quota, CostUSD: bucket.CostUSD, FetchedAt: now,
			}
			if err := tx.Create(&stage).Error; err != nil {
				return err
			}
		}
		state.SourceKeyID, state.UpdatedAt = result.SourceKeyID, now
		if round.Kind == "tail" {
			state.Status, state.LastError = upstreamStatusOK, ""
			state.LastAttemptAt, state.LastSuccessAt, state.ConsecutiveFails, state.NextSyncAt = now, now, 0, 0
			state.TailRoundID = round.RoundID
		} else {
			state.BackfillRoundID = round.RoundID
			state.BackfillLastSuccessAt, state.BackfillConsecutiveFails, state.BackfillNextSyncAt, state.BackfillLastError = now, 0, 0, ""
		}
		return tx.Save(state).Error
	})
}

func (m *Monitor) recordAICodeWithKeyFailure(ctx context.Context, round AICodeWithUsageRound, state *AICodeWithKeySyncState, err error, secret string, now int64) error {
	sanitized := sanitizeUpstreamErrorWithSecrets(err, secret)
	if round.Kind == "backfill" {
		state.UpdatedAt = now
		state.BackfillLastError = sanitized
		state.BackfillConsecutiveFails++
		state.BackfillNextSyncAt = upstreamUsageFailureRetryAt(m.cfg, round.Domain, now, state.BackfillConsecutiveFails, err, round.Kind+":"+state.SlotID)
		var authErr *upstreamAuthError
		if errors.As(err, &authErr) {
			// 凭据失效不是历史车道专属问题；只有这种证据才同时隔离 Tail。
			state.Status, state.LastError = upstreamStatusReconnect, sanitized
			state.LastAttemptAt = now
			state.ConsecutiveFails++
			state.NextSyncAt = upstreamAccountIsolatedUntil
			state.BackfillNextSyncAt = upstreamAccountIsolatedUntil
		}
		return m.storeDB.WithContext(ctx).Save(state).Error
	}
	state.Status = upstreamStatusError
	state.LastAttemptAt, state.UpdatedAt = now, now
	state.LastError = sanitized
	state.ConsecutiveFails++
	state.NextSyncAt = upstreamUsageFailureRetryAt(m.cfg, round.Domain, now, state.ConsecutiveFails, err, round.Kind+":"+state.SlotID)
	var authErr *upstreamAuthError
	if errors.As(err, &authErr) {
		state.Status, state.NextSyncAt = upstreamStatusReconnect, upstreamAccountIsolatedUntil
	}
	return m.storeDB.WithContext(ctx).Save(state).Error
}

func (m *Monitor) publishAICodeWithRound(ctx context.Context, row *ChannelUpstreamAccount, round AICodeWithUsageRound, now int64) (bool, error) {
	var completed int64
	roundColumn := "tail_round_id"
	if round.Kind == "backfill" {
		roundColumn = "backfill_round_id"
	}
	if err := m.storeDB.WithContext(ctx).Model(&AICodeWithKeySyncState{}).
		Where("domain = ? AND credential_set_version = ? AND "+roundColumn+" = ?", row.Domain, round.CredentialSetVersion, round.RoundID).
		Count(&completed).Error; err != nil {
		return false, err
	}
	if completed != int64(round.TotalKeys) {
		return false, nil
	}
	var staged []AICodeWithUsageStage
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND round_id = ?", row.Domain, round.RoundID).Order("hour_ts ASC").Find(&staged).Error; err != nil {
		return false, err
	}
	aggregated := make(map[int64]ChannelUpstreamUsageHour)
	for _, part := range staged {
		bucket := aggregated[part.HourTs]
		if bucket.Domain == "" {
			bucket = ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: part.HourTs, BucketSeconds: part.BucketSeconds, Provider: upstreamProviderAICodeWith}
		} else if bucket.BucketSeconds != part.BucketSeconds {
			return false, fmt.Errorf("AICodeWith 多 Key 账单覆盖范围不一致")
		}
		bucket.Requests += part.Requests
		bucket.Tokens += part.Tokens
		bucket.Quota += part.Quota
		bucket.CostUSD += part.CostUSD
		bucket.FetchedAt = now
		aggregated[part.HourTs] = bucket
	}
	return true, m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current AICodeWithUsageRound
		if err := tx.First(&current, "domain = ? AND kind = ?", row.Domain, round.Kind).Error; err != nil {
			return err
		}
		if current.RoundID != round.RoundID || current.CredentialSetVersion != round.CredentialSetVersion {
			return fmt.Errorf("AICodeWith 凭据集合已变更，拒绝发布旧批次")
		}
		if err := tx.Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", row.Domain, round.WindowFrom, round.WindowTo).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			return err
		}
		hours := make([]int64, 0, len(aggregated))
		for hour := range aggregated {
			hours = append(hours, hour)
		}
		sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
		for _, hour := range hours {
			bucket := aggregated[hour]
			if err := tx.Create(&bucket).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("domain = ? AND round_id = ?", row.Domain, round.RoundID).Delete(&AICodeWithUsageStage{}).Error; err != nil {
			return err
		}
		if err := tx.Where("domain = ? AND kind = ?", row.Domain, round.Kind).Delete(&AICodeWithUsageRound{}).Error; err != nil {
			return err
		}
		if round.Kind == "backfill" && round.WindowTo >= cstDayStart(now) {
			// Keep the per-key diagnostic state in the same transaction as the
			// final account-level history publication. The account row remains the
			// scheduling authority, while these fields make the sync-status page
			// accurately report every key as complete.
			if err := tx.Model(&AICodeWithKeySyncState{}).
				Where("domain = ? AND credential_set_version = ?", row.Domain, round.CredentialSetVersion).
				Updates(map[string]any{
					"backfill_done":              true,
					"backfill_last_error":        "",
					"backfill_next_sync_at":      int64(0),
					"backfill_consecutive_fails": 0,
					"updated_at":                 now,
				}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Monitor) processAICodeWithRound(ctx context.Context, row *ChannelUpstreamAccount, normalized aiCodeWithCredential, version, kind string, from, to, now int64, budget int) (bool, int64, int, error) {
	var states []AICodeWithKeySyncState
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND credential_set_version = ?", row.Domain, version).Order("ordinal ASC").Find(&states).Error; err != nil {
		return false, 0, 0, err
	}
	if len(states) != len(normalized.Slots) {
		if err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("domain = ?", row.Domain).Delete(&AICodeWithKeySyncState{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&AICodeWithUsageStage{}).Error; err != nil {
				return err
			}
			if err := tx.Where("domain = ?", row.Domain).Delete(&AICodeWithUsageRound{}).Error; err != nil {
				return err
			}
			for i, slot := range normalized.Slots {
				state := AICodeWithKeySyncState{Domain: row.Domain, SlotID: slot.SlotID, CredentialSetVersion: version, Ordinal: i + 1, Status: upstreamStatusPending, UpdatedAt: now}
				if err := tx.Create(&state).Error; err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return false, 0, 0, err
		}
		states = nil
		if err := m.storeDB.WithContext(ctx).Where("domain = ? AND credential_set_version = ?", row.Domain, version).Order("ordinal ASC").Find(&states).Error; err != nil {
			return false, 0, 0, err
		}
		if len(states) != len(normalized.Slots) {
			return false, 0, 0, fmt.Errorf("AICodeWith Key 同步状态与凭据集合不一致")
		}
	}
	round, err := m.ensureAICodeWithUsageRound(ctx, *row, version, kind, from, to, len(states), now)
	if err != nil {
		return false, 0, 0, err
	}
	secretByID := make(map[string]string, len(normalized.Slots))
	for _, slot := range normalized.Slots {
		secretByID[slot.SlotID] = slot.Secret
	}
	processed := 0
	// 单个账户无论有多少 Key 都共享同一个请求节流器；否则每把 Key 各自的
	// “第一个请求”会同时发出，实际仍可能触发账户级 429。
	roundPacer := newUpstreamUsageRequestPacer(max(1, budget), m.aiCodeWithRequestInterval())
	for _, i := range selectAICodeWithKeyStatesForTurn(states, round, kind, now, budget) {
		state := &states[i]
		result, fetchErr := fetchAICodeWithUsageWindow(ctx, m.channelUpstreamHTTPClient(), *row, secretByID[state.SlotID], round.WindowFrom, round.WindowTo, roundPacer)
		if fetchErr == nil {
			fetchErr = m.stageAICodeWithKeyResult(ctx, round, state, result, now)
		}
		if errors.Is(fetchErr, context.Canceled) {
			return false, 0, processed, fetchErr
		}
		if fetchErr != nil {
			if persistErr := m.recordAICodeWithKeyFailure(ctx, round, state, fetchErr, secretByID[state.SlotID], now); persistErr != nil {
				return false, 0, processed, persistErr
			}
		}
		processed++
		// 认证错误只隔离当前 Key，继续验证其他 Key；而限流、主机熔断、
		// 超时和 5xx 属于账户/主机级信号，本轮立即止步，避免一个故障
		// 被剩余 Key 放大为请求风暴。未处理 Key 保持 pending，下轮续跑。
		var authErr *upstreamAuthError
		if fetchErr != nil && !errors.As(fetchErr, &authErr) &&
			(upstreamRetryAt(fetchErr) > now || isUpstreamUsageTransientFailure(fetchErr)) {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	published, err := m.publishAICodeWithRound(ctx, row, round, now)
	if err != nil {
		return false, 0, processed, err
	}
	if published {
		// round.WindowTo 在第一轮创建时即被冻结。多 Key 分轮期间即使 now
		// 已向后推进，也只能发布这个已被所有 Key 完整覆盖的水位，不能把
		// 尚未读取的几分钟虚报为已同步。
		if kind == "tail" {
			row.UsageDataUntil = round.WindowTo
		}
		row.UsageAdapter = upstreamUsageAdapterAICodeWith
		return true, int64(round.TotalKeys), processed, nil
	}
	var done int64
	column := "tail_round_id"
	if kind == "backfill" {
		column = "backfill_round_id"
	}
	if err := m.storeDB.WithContext(ctx).Model(&AICodeWithKeySyncState{}).
		Where("domain = ? AND credential_set_version = ? AND "+column+" = ?", row.Domain, version, round.RoundID).Count(&done).Error; err != nil {
		return false, 0, processed, err
	}
	return false, done, processed, nil
}

// selectAICodeWithKeyStatesForTurn is deliberately pure. The durable state is
// ordered by Ordinal, so every process restart resumes at the first unfinished
// key while one worker turn remains strictly bounded. A large account can
// never expand one two-minute operation into N tail + N history requests.
func selectAICodeWithKeyStatesForTurn(states []AICodeWithKeySyncState, round AICodeWithUsageRound, kind string, now int64, budget int) []int {
	if budget <= 0 {
		return nil
	}
	selected := make([]int, 0, min(budget, len(states)))
	for i := range states {
		completedRound, nextAt := states[i].TailRoundID, states[i].NextSyncAt
		if kind == "backfill" {
			completedRound, nextAt = states[i].BackfillRoundID, states[i].BackfillNextSyncAt
		}
		if completedRound == round.RoundID || nextAt == upstreamAccountIsolatedUntil || nextAt > now {
			continue
		}
		selected = append(selected, i)
		if len(selected) == budget {
			break
		}
	}
	return selected
}

// aiCodeWithRoundWaitState summarizes only unfinished keys in the current
// durable round. When every unfinished key is auth-isolated, the account can
// never make progress with its current credential set and must leave the
// global due queue until an administrator replaces at least one key.
func (m *Monitor) aiCodeWithRoundWaitState(ctx context.Context, domain, version, kind string, now int64) (allIsolated bool, retryAt int64, err error) {
	var round AICodeWithUsageRound
	if err = m.storeDB.WithContext(ctx).First(&round, "domain = ? AND kind = ?", domain, kind).Error; err != nil {
		return false, 0, err
	}
	var states []AICodeWithKeySyncState
	if err = m.storeDB.WithContext(ctx).Where("domain = ? AND credential_set_version = ?", domain, version).Find(&states).Error; err != nil {
		return false, 0, err
	}
	pending := 0
	allIsolated = true
	for i := range states {
		completedRound, nextAt := states[i].TailRoundID, states[i].NextSyncAt
		if kind == "backfill" {
			completedRound, nextAt = states[i].BackfillRoundID, states[i].BackfillNextSyncAt
		}
		if completedRound == round.RoundID {
			continue
		}
		pending++
		if nextAt == upstreamAccountIsolatedUntil {
			continue
		}
		allIsolated = false
		candidate := nextAt
		if candidate == 0 || candidate <= now {
			candidate = now + 15
		}
		if retryAt == 0 || candidate < retryAt {
			retryAt = candidate
		}
	}
	if pending == 0 {
		return false, 0, nil
	}
	return allIsolated, retryAt, nil
}

func isolateAICodeWithUsageAccount(row *ChannelUpstreamAccount) error {
	err := &upstreamAuthError{err: errors.New("AICodeWith 所有未完成 Key 均需重新连接")}
	row.UsageStatus = upstreamStatusReconnect
	row.UsageLastError = err.Error()
	row.UsageNextSyncAt = upstreamAccountIsolatedUntil
	row.UsageBackfillLastError = err.Error()
	row.UsageBackfillNextSyncAt = upstreamAccountIsolatedUntil
	return err
}

func (m *Monitor) syncStoredAICodeWithUsage(ctx context.Context, row *ChannelUpstreamAccount, cred aiCodeWithCredential, now int64, lane upstreamUsageLane) error {
	normalized, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return err
	}
	version, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		return err
	}
	total := len(normalized.Slots)
	budget := aiCodeWithKeysPerTurn
	today := cstDayStart(now)
	if lane != upstreamUsageLaneHistory && (row.UsageNextSyncAt == 0 || row.UsageNextSyncAt <= now) {
		published, done, used, roundErr := m.processAICodeWithRound(ctx, row, normalized, version, "tail", today, now, now, budget)
		budget -= used
		row.UsageLastAttemptAt = now
		if roundErr != nil {
			if errors.Is(roundErr, context.Canceled) {
				return roundErr
			}
			row.UsageStatus, row.UsageLastError = upstreamStatusError, sanitizeUpstreamError(roundErr)
			row.UsageConsecutiveFails++
			row.UsageNextSyncAt = upstreamUsageFailureRetryAt(m.cfg, row.Domain, now, row.UsageConsecutiveFails, roundErr, "tail")
			return roundErr
		}
		if !published {
			allIsolated, retryAt, waitErr := m.aiCodeWithRoundWaitState(ctx, row.Domain, version, "tail", now)
			if waitErr != nil {
				return waitErr
			}
			if allIsolated {
				return isolateAICodeWithUsageAccount(row)
			}
			row.UsageStatus, row.UsageLastError = upstreamStatusPending, fmt.Sprintf("AICodeWith 当日 Key 进度 %d/%d，已完成 Key 不会重复请求", done, total)
			row.UsageNextSyncAt = retryAt
			if row.UsageNextSyncAt == 0 {
				row.UsageNextSyncAt = now + 15
			}
			return nil
		}
		row.UsageStatus, row.UsageLastError = upstreamStatusOK, ""
		row.UsageLastSuccessAt, row.UsageConsecutiveFails = now, 0
		row.UsageNextSyncAt = nextUpstreamUsageSyncAtForProvider(m.cfg, row.Provider, row.Domain, now, 0)
	}
	if lane == upstreamUsageLaneTail {
		return nil
	}
	if row.UsageBackfillCursor == 0 {
		row.UsageBackfillCursor = cstDayStart(now - int64(upstreamUsageBackfillDays(m.cfg))*86400)
	}
	if row.UsageBackfillCursor >= today {
		row.UsageBackfillDone, row.UsageBackfillNextSyncAt = true, 0
		return nil
	}
	if budget <= 0 || (row.UsageBackfillNextSyncAt > now && row.UsageBackfillNextSyncAt != upstreamAccountIsolatedUntil) || row.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil {
		return nil
	}
	to := row.UsageBackfillCursor + aiCodeWithUsageMaxDays*86400
	if to > today {
		to = today
	}
	published, done, _, roundErr := m.processAICodeWithRound(ctx, row, normalized, version, "backfill", row.UsageBackfillCursor, to, now, budget)
	row.UsageBackfillLastAttemptAt = now
	if roundErr != nil {
		if errors.Is(roundErr, context.Canceled) {
			return roundErr
		}
		row.UsageBackfillLastError = sanitizeUpstreamError(roundErr)
		row.UsageBackfillConsecutiveFails++
		row.UsageBackfillNextSyncAt = upstreamUsageFailureRetryAt(m.cfg, row.Domain, now, row.UsageBackfillConsecutiveFails, roundErr, "history")
		return roundErr
	}
	if !published {
		allIsolated, retryAt, waitErr := m.aiCodeWithRoundWaitState(ctx, row.Domain, version, "backfill", now)
		if waitErr != nil {
			return waitErr
		}
		if allIsolated {
			return isolateAICodeWithUsageAccount(row)
		}
		row.UsageBackfillLastError = fmt.Sprintf("AICodeWith 历史 Key 进度 %d/%d", done, total)
		row.UsageBackfillNextSyncAt = retryAt
		if row.UsageBackfillNextSyncAt == 0 {
			row.UsageBackfillNextSyncAt = now + 15
		}
		return nil
	}
	row.UsageBackfillCursor, row.UsageBackfillLastSuccessAt = to, now
	row.UsageBackfillConsecutiveFails, row.UsageBackfillLastError = 0, ""
	row.UsageBackfillDone = to >= today
	if row.UsageBackfillDone {
		row.UsageBackfillNextSyncAt = 0
	} else {
		row.UsageBackfillNextSyncAt = now + 15
	}
	return nil
}

// persistUpstreamUsageWindow 仅在完整读取窗口后替换同一窗口的小时桶。事务失败时保留
// 原数据，确保页面既不会出现半窗口，也不会把上游故障解释为零消费。
func (m *Monitor) persistUpstreamUsageWindow(ctx context.Context, domain string, from, to int64, hours []ChannelUpstreamUsageHour, now int64) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return persistUpstreamUsageWindowTx(tx, domain, from, to, hours, now)
	})
}

func persistUpstreamUsageWindowTx(tx *gorm.DB, domain string, from, to int64, hours []ChannelUpstreamUsageHour, now int64) error {
	if err := tx.Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, from, to).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		return err
	}
	for i := range hours {
		hours[i].FetchedAt = now
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}, {Name: "hour_ts"}}, UpdateAll: true}).Create(&hours[i]).Error; err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) persistNewAPIUsageBackfillWindow(ctx context.Context, domain string, from, to int64, hours []ChannelUpstreamUsageHour, now int64) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := persistUpstreamUsageWindowTx(tx, domain, from, to, hours, now); err != nil {
			return err
		}
		if err := tx.Where("domain = ?", domain).Delete(&NewAPIUsageBackfillCheckpoint{}).Error; err != nil {
			return err
		}
		return tx.Where("domain = ? AND hour_from = ?", domain, from).Delete(&NewAPIUsageBackfillSegment{}).Error
	})
}

func applyUpstreamUsageResult(row *ChannelUpstreamAccount, result upstreamUsageResult, err error, now int64, s Settings, secrets ...string) {
	row.UsageLastAttemptAt = now
	if err == nil {
		row.UsageStatus, row.UsageLastError = upstreamStatusOK, ""
		row.UsageLastSuccessAt, row.UsageDataUntil = now, result.DataUntil
		if result.Adapter != "" {
			row.UsageAdapter = result.Adapter
		}
		row.UsageConsecutiveFails = 0
		row.UsageNextSyncAt = nextUpstreamUsageSyncAtForProvider(s, row.Provider, row.Domain, now, 0)
		return
	}
	row.UsageLastError = sanitizeUpstreamErrorWithSecrets(err, secrets...)
	row.UsageConsecutiveFails++
	var authErr *upstreamAuthError
	if errors.As(err, &authErr) {
		row.UsageStatus = upstreamStatusReconnect
		row.UsageNextSyncAt = upstreamAccountIsolatedUntil
	} else {
		row.UsageStatus = upstreamStatusError
		row.UsageNextSyncAt = upstreamUsageFailureRetryAt(s, row.Domain, now, row.UsageConsecutiveFails, err, "tail")
	}
}

type upstreamUsageSyncPlan struct {
	tailFrom     int64
	tailTo       int64
	backfillFrom int64
	backfillTo   int64
}

// planUpstreamUsageSync treats the open-day tail and history as two due lanes.
// History may run by itself while a healthy tail is waiting for its normal
// interval, but it must never bypass a tail error/auth/rate-limit backoff.
func planUpstreamUsageSync(row ChannelUpstreamAccount, now int64, backfillDays int) upstreamUsageSyncPlan {
	today := cstDayStart(now)
	plan := upstreamUsageSyncPlan{}
	tailDue := row.UsageNextSyncAt == 0 || row.UsageNextSyncAt <= now
	if tailDue {
		plan.tailFrom, plan.tailTo = today, now
		if row.Provider != upstreamProviderAICodeWith && row.Provider != upstreamProviderSub2API && row.UsageDataUntil > today {
			plan.tailFrom = row.UsageDataUntil - int64(upstreamUsageTailOverlap/time.Second)
			if plan.tailFrom < today {
				plan.tailFrom = today
			}
			plan.tailFrom -= plan.tailFrom % 3600
		}
	}
	backfill := row.UsageBackfillCursor
	if backfill == 0 {
		backfill = cstDayStart(now - int64(backfillDays)*86400)
	}
	historyDue := !row.UsageBackfillDone && backfill < today &&
		(row.UsageBackfillNextSyncAt == 0 || row.UsageBackfillNextSyncAt <= now)
	tailBlocked := (row.UsageStatus == upstreamStatusError || row.UsageStatus == upstreamStatusReconnect) && !tailDue
	if historyDue && !tailBlocked {
		plan.backfillFrom = backfill
		plan.backfillTo = backfill + 86400
		if row.Provider == upstreamProviderNewAPI {
			// NewAPI exposes mutable OFFSET pages. Import one closed hour at a
			// time so a dense day can yield and resume without rescanning it.
			plan.backfillTo = backfill + 3600
		}
		if row.Provider == upstreamProviderAICodeWith {
			plan.backfillTo = backfill + aiCodeWithUsageMaxDays*86400
			if plan.backfillTo > today {
				plan.backfillTo = today
			}
		}
	}
	return plan
}

func planUpstreamUsageSyncForLane(row ChannelUpstreamAccount, now int64, backfillDays int, lane upstreamUsageLane) upstreamUsageSyncPlan {
	plan := planUpstreamUsageSync(row, now, backfillDays)
	switch lane {
	case upstreamUsageLaneTail:
		plan.backfillFrom, plan.backfillTo = 0, 0
	case upstreamUsageLaneHistory:
		plan.tailFrom, plan.tailTo = 0, 0
	}
	return plan
}

func applyUpstreamUsageBackfillResult(row *ChannelUpstreamAccount, err error, now int64, s Settings, secrets ...string) {
	row.UsageBackfillLastAttemptAt = now
	if err == nil {
		row.UsageBackfillLastSuccessAt = now
		row.UsageBackfillLastError = ""
		row.UsageBackfillProgress = ""
		row.UsageBackfillConsecutiveFails = 0
		row.UsageBackfillNextSyncAt = nextUpstreamUsageBackfillAt(s, row.Domain, now, 0)
		return
	}
	row.UsageBackfillLastError = sanitizeUpstreamErrorWithSecrets(err, secrets...)
	row.UsageBackfillProgress = ""
	row.UsageBackfillConsecutiveFails++
	var authErr *upstreamAuthError
	if errors.As(err, &authErr) {
		// Tail and history share one account credential. Once either lane proves
		// it invalid, isolate the whole account until an administrator replaces
		// the credential; otherwise a healthy-looking tail would retry a known
		// bad secret at its next normal deadline.
		row.UsageStatus = upstreamStatusReconnect
		row.UsageLastError = row.UsageBackfillLastError
		row.UsageNextSyncAt = upstreamAccountIsolatedUntil
		row.UsageBackfillNextSyncAt = upstreamAccountIsolatedUntil
	} else {
		row.UsageBackfillNextSyncAt = upstreamUsageFailureRetryAt(s, row.Domain, now, row.UsageBackfillConsecutiveFails, err, "history")
	}
}

func applyUpstreamUsageBackfillYield(row *ChannelUpstreamAccount, progress string, now int64) {
	row.UsageBackfillLastAttemptAt = now
	row.UsageBackfillConsecutiveFails = 0
	row.UsageBackfillLastError = ""
	row.UsageBackfillProgress = progress
	// A budget yield is healthy progress, not an upstream failure. Continue
	// soon enough to drain history while still leaving a quiet gap between turns.
	row.UsageBackfillNextSyncAt = now + 15
}

func legacyNewAPIBackfillBudgetError() string {
	return (&upstreamUsageRunBudgetExhausted{max: upstreamUsageMaxRequestsPerRun}).Error()
}

func normalizeLegacyNewAPIBackfillBudgetState(row *ChannelUpstreamAccount, now int64) {
	if row == nil || row.Provider != upstreamProviderNewAPI || row.UsageBackfillDone ||
		row.UsageBackfillLastError != legacyNewAPIBackfillBudgetError() {
		return
	}
	// Older images classified a healthy per-run request budget yield as a
	// failure and left the account behind exponential backoff. The checkpoint
	// implementation can safely resume that same hour, so make the inherited
	// state immediately eligible without weakening real upstream failures.
	row.UsageBackfillConsecutiveFails = 0
	row.UsageBackfillLastError = ""
	row.UsageBackfillProgress = "等待断点续传"
	row.UsageBackfillNextSyncAt = now
}

func coupleUpstreamUsageHistoryRetryToTail(row *ChannelUpstreamAccount) {
	if !row.UsageBackfillDone && row.UsageBackfillNextSyncAt < row.UsageNextSyncAt {
		row.UsageBackfillNextSyncAt = row.UsageNextSyncAt
	}
}

func (m *Monitor) syncStoredUpstreamUsage(ctx context.Context, domain string) (ChannelUpstreamAccount, error) {
	return m.syncStoredUpstreamUsageWithPriority(ctx, domain, false, upstreamUsageLaneAuto)
}

func (m *Monitor) syncStoredUpstreamUsageLaneBackground(ctx context.Context, domain string, lane upstreamUsageLane) (ChannelUpstreamAccount, error) {
	return m.syncStoredUpstreamUsageWithPriority(ctx, domain, true, lane)
}

func upstreamUsageLocalCommitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// The read budget is exhausted, but durable checkpoints and the next local
		// schedule still need a bounded SQLite commit. This detached context is
		// local-only and must never be used for another upstream request.
		return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	}
	return ctx, func() {}
}

func (m *Monitor) syncStoredUpstreamUsageWithPriority(ctx context.Context, domain string, background bool, lane upstreamUsageLane) (ChannelUpstreamAccount, error) {
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
	if !row.Enabled || !row.UsageSyncEnabled {
		return row, fmt.Errorf("该上游账户未启用使用日志同步")
	}
	now := time.Now().Unix()
	normalizeLegacyNewAPIBackfillBudgetState(&row, now)
	if row.Provider != upstreamProviderNewAPI && row.Provider != upstreamProviderSub2API && row.Provider != upstreamProviderAICodeWith {
		err := fmt.Errorf("%s 暂未验证公开使用日志接口，未自动读取日志", upstreamProviderName(row.Provider))
		row.UsageStatus = upstreamStatusUnsupported
		row.UsageLastAttemptAt = now
		row.UsageLastError = err.Error()
		row.UsageNextSyncAt = 0
		if persistErr := m.persistUpstreamAccount(ctx, &row); persistErr != nil {
			return row, fmt.Errorf("保存不支持状态失败: %w", persistErr)
		}
		return row, err
	}
	credential, err := m.credentialForAccount(row)
	if err != nil {
		applyUpstreamUsageResult(&row, upstreamUsageResult{}, &upstreamAuthError{err: err}, now, m.cfg)
		if persistErr := m.persistUpstreamAccount(ctx, &row); persistErr != nil {
			return row, fmt.Errorf("保存凭据错误状态失败: %w", persistErr)
		}
		return row, err
	}
	if cred, ok := credential.(aiCodeWithCredential); ok && row.Provider == upstreamProviderAICodeWith {
		err = m.syncStoredAICodeWithUsage(ctx, &row, cred, now, lane)
		if persistErr := m.persistUpstreamAccount(ctx, &row); persistErr != nil {
			return row, persistErr
		}
		if err != nil {
			return row, &upstreamStoredSyncError{message: sanitizeUpstreamErrorWithSecrets(err, upstreamCredentialSecrets(cred)...), retryAt: upstreamRetryAt(err)}
		}
		return row, nil
	}
	secrets := upstreamCredentialSecrets(credential)
	var syncUsage func(context.Context, int64, int64, *upstreamUsageRequestPacer) (upstreamUsageResult, error)
	switch cred := credential.(type) {
	case newAPICredential:
		if row.Provider != upstreamProviderNewAPI {
			return row, fmt.Errorf("%s 凭据与供应商不匹配", upstreamProviderName(row.Provider))
		}
		syncUsage = func(callCtx context.Context, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
			return m.syncNewAPIUsage(callCtx, row, cred, from, to, pacer)
		}
	case sub2APICredential:
		if row.Provider != upstreamProviderSub2API {
			return row, fmt.Errorf("%s 凭据与供应商不匹配", upstreamProviderName(row.Provider))
		}
		current := cred
		adapter := row.UsageAdapter
		syncUsage = func(callCtx context.Context, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
			result, updated, syncErr := syncSub2APIUsage(callCtx, m.channelUpstreamHTTPClient(), row, current, from, to, pacer, adapter)
			current = updated
			credential = current
			secrets = upstreamCredentialSecrets(current)
			if syncErr == nil && result.Adapter != "" {
				adapter = result.Adapter
			}
			return result, syncErr
		}
	default:
		return row, fmt.Errorf("%s 凭据格式无效", upstreamProviderName(row.Provider))
	}
	plan := planUpstreamUsageSyncForLane(row, now, upstreamUsageBackfillDays(m.cfg), lane)
	requestBudget := upstreamUsageMaxRequestsPerRun
	if lane == upstreamUsageLaneHistory {
		requestBudget = upstreamUsageHistoryMaxRequestsPerRun
	}
	pacer := newUpstreamUsageRequestPacer(requestBudget, upstreamUsageRequestInterval)
	operationRetryAt := int64(0)
	if row.UsageBackfillCursor == 0 {
		row.UsageBackfillCursor = cstDayStart(now - int64(upstreamUsageBackfillDays(m.cfg))*86400)
	}
	// Tail is the primary freshness contract. When both lanes are due it runs
	// first; only a successful, atomically persisted tail permits history.
	result := upstreamUsageResult{DataUntil: row.UsageDataUntil}
	tailRan := plan.tailTo > plan.tailFrom
	historyRan := false
	if tailRan {
		result, err = syncUsage(ctx, plan.tailFrom, plan.tailTo, pacer)
		if errors.Is(err, context.Canceled) {
			return row, err
		}
		operationRetryAt = upstreamRetryAt(err)
		if err == nil {
			err = m.persistUpstreamUsageWindow(ctx, row.Domain, plan.tailFrom, plan.tailTo, result.Hours, now)
		}
		if errors.Is(err, context.Canceled) {
			return row, err
		}
	}
	if tailRan {
		applyUpstreamUsageResult(&row, result, err, now, m.cfg, secrets...)
		if err != nil {
			// Couple only the retry deadline, not the health counters: a due
			// history lane must not reselect this account before tail recovery.
			coupleUpstreamUsageHistoryRetryToTail(&row)
		}
	}
	if err == nil && plan.backfillTo > plan.backfillFrom {
		historyRan = true
		if row.Provider == upstreamProviderNewAPI {
			cursor := plan.backfillFrom
			today := cstDayStart(now)
			for cursor < today {
				windowTo := cursor + 3600
				history, progress, complete, historyErr := m.syncNewAPIUsageBackfillWindow(ctx, row, credential.(newAPICredential), cursor, windowTo, pacer)
				if errors.Is(historyErr, context.Canceled) {
					return row, historyErr
				}
				if retryAt := upstreamRetryAt(historyErr); retryAt > operationRetryAt {
					operationRetryAt = retryAt
				}
				if historyErr != nil {
					applyUpstreamUsageBackfillResult(&row, historyErr, now, m.cfg, secrets...)
					err = historyErr
					break
				}
				if !complete {
					applyUpstreamUsageBackfillYield(&row, progress, now)
					break
				}
				if historyErr = m.persistNewAPIUsageBackfillWindow(ctx, row.Domain, cursor, windowTo, history.Hours, now); historyErr != nil {
					if errors.Is(historyErr, context.Canceled) {
						return row, historyErr
					}
					applyUpstreamUsageBackfillResult(&row, historyErr, now, m.cfg, secrets...)
					err = historyErr
					break
				}
				applyUpstreamUsageBackfillResult(&row, nil, now, m.cfg, secrets...)
				cursor = windowTo
				row.UsageBackfillCursor = cursor
				row.UsageBackfillDone = cursor >= today
				if row.UsageBackfillDone {
					row.UsageBackfillNextSyncAt = 0
					break
				}
			}
		} else {
			history, historyErr := syncUsage(ctx, plan.backfillFrom, plan.backfillTo, pacer)
			if errors.Is(historyErr, context.Canceled) {
				return row, historyErr
			}
			if retryAt := upstreamRetryAt(historyErr); retryAt > operationRetryAt {
				operationRetryAt = retryAt
			}
			if historyErr == nil {
				historyErr = m.persistUpstreamUsageWindow(ctx, row.Domain, plan.backfillFrom, plan.backfillTo, history.Hours, now)
			}
			if errors.Is(historyErr, context.Canceled) {
				return row, historyErr
			}
			if historyErr != nil {
				// Tail health and schedule remain successful. Only history receives an
				// independent exponential backoff, so today's spend stays fresh.
				applyUpstreamUsageBackfillResult(&row, historyErr, now, m.cfg, secrets...)
				err = historyErr
			} else {
				applyUpstreamUsageBackfillResult(&row, nil, now, m.cfg, secrets...)
				row.UsageBackfillCursor = plan.backfillTo
				row.UsageBackfillDone = plan.backfillTo >= cstDayStart(now)
				if row.UsageBackfillDone {
					row.UsageBackfillNextSyncAt = 0
				}
			}
		}
	} else if err == nil && tailRan {
		row.UsageBackfillDone = row.UsageBackfillCursor >= cstDayStart(now)
	}
	persistCtx, persistCancel := upstreamUsageLocalCommitContext(ctx)
	defer persistCancel()
	var persistErr error
	if row.Provider == upstreamProviderSub2API {
		// Sub2API rotates refresh tokens. Persist the newest token even when the
		// subsequent usage query fails, otherwise the next run may replay a
		// consumed refresh token and isolate an otherwise healthy account.
		persistErr = m.persistSyncedUpstreamAccount(persistCtx, &row, credential)
	} else {
		persistErr = m.persistUpstreamAccount(persistCtx, &row)
	}
	if persistErr != nil {
		return row, persistErr
	}
	if err != nil {
		message := row.UsageBackfillLastError
		if tailRan && !historyRan {
			message = row.UsageLastError
		}
		if message == "" {
			message = sanitizeUpstreamErrorWithSecrets(err, secrets...)
		}
		return row, &upstreamStoredSyncError{message: message, retryAt: operationRetryAt}
	}
	return row, nil
}

func (m *Monitor) syncDueUpstreamUsage(ctx context.Context) {
	if !m.cfg.UpstreamUsageSyncEnabled {
		return
	}
	now := time.Now().Unix()
	limit := upstreamMaxConcurrency(m.cfg)
	// 读取较宽的本地候选集，真正发起的同步仍受 limit 严格限制。这样排序
	// 最前的账户若正被管理员保存/其他 lane 占用，可以跳过它继续服务后续
	// Tail，不会因为默认并发 1 而每 10 秒重复撞同一个 busy gate。
	const candidateLimit = 50
	// Tail 是新鲜度合同：默认先跑 Tail；只有连续批次配额已满且
	// 最老 Tail 仍在宽限内，才允许一个有界历史批次让路。
	rows, err := m.loadDueUpstreamUsageAccountsForLane(ctx, now, candidateLimit, upstreamUsageLaneTail)
	if err != nil {
		slog.Warn("读取待同步上游使用日志失败", "err", err)
		return
	}
	lane := upstreamUsageLaneTail
	if len(rows) > 0 && shouldRunUpstreamUsageHistoryFairness(now, rows[0], m.upstreamUsageTailBatches.Load()) {
		historyRows, historyErr := m.loadDueUpstreamUsageAccountsForLane(ctx, now, candidateLimit, upstreamUsageLaneHistory)
		if historyErr != nil {
			slog.Warn("读取待补全上游历史日志失败", "err", historyErr)
			return
		}
		if len(historyRows) > 0 {
			lane, rows = upstreamUsageLaneHistory, historyRows
		}
	}
	if len(rows) == 0 {
		lane = upstreamUsageLaneHistory
		rows, err = m.loadDueUpstreamUsageAccountsForLane(ctx, now, candidateLimit, lane)
		if err != nil {
			slog.Warn("读取待补全上游历史日志失败", "err", err)
			return
		}
	}
	attempted := m.runDueUpstreamUsageAccounts(ctx, rows, lane, limit)
	if lane == upstreamUsageLaneHistory {
		// 即使候选恰好 busy，也只消耗本次公平令牌；下轮先回到 Tail。
		m.upstreamUsageTailBatches.Store(0)
	} else if attempted > 0 {
		m.upstreamUsageTailBatches.Add(1)
	}
}

func shouldRunUpstreamUsageHistoryFairness(now int64, oldestTail ChannelUpstreamAccount, completedTailBatches uint64) bool {
	if completedTailBatches < upstreamUsageHistoryFairnessTailBatches || oldestTail.UsageNextSyncAt <= 0 {
		return false
	}
	return now-oldestTail.UsageNextSyncAt <= int64(upstreamUsageTailOverdueGuard/time.Second)
}

func (m *Monitor) loadDueUpstreamUsageAccounts(ctx context.Context, now int64, limit int) ([]ChannelUpstreamAccount, error) {
	// 兼容既有测试/调用者：合并视图仍按 Tail 优先；无 Tail 才返回历史。
	rows, err := m.loadDueUpstreamUsageAccountsForLane(ctx, now, limit, upstreamUsageLaneTail)
	if err != nil || len(rows) > 0 {
		return rows, err
	}
	return m.loadDueUpstreamUsageAccountsForLane(ctx, now, limit, upstreamUsageLaneHistory)
}

func (m *Monitor) loadDueUpstreamUsageAccountsForLane(ctx context.Context, now int64, limit int, lane upstreamUsageLane) ([]ChannelUpstreamAccount, error) {
	if limit <= 0 || limit > 50 {
		limit = 1
	}
	var rows []ChannelUpstreamAccount
	if lane == upstreamUsageLaneTail {
		err := m.storeDB.WithContext(ctx).Where(`enabled = ? AND usage_sync_enabled = ?
			AND (usage_next_sync_at = 0 OR usage_next_sync_at <= ?)`, true, true, now).
			Order("usage_next_sync_at ASC, domain ASC").Limit(limit).Find(&rows).Error
		return rows, err
	}
	legacyBudgetError := legacyNewAPIBackfillBudgetError()
	err := m.storeDB.WithContext(ctx).Where(`enabled = ? AND usage_sync_enabled = ? AND (
		(usage_backfill_done = ? AND (usage_backfill_next_sync_at = 0 OR usage_backfill_next_sync_at <= ?)
			AND usage_status = ?)
		OR (provider = ? AND usage_backfill_done = ? AND usage_backfill_last_error = ?)
	)`, true, true, false, now, upstreamStatusOK,
		upstreamProviderNewAPI, false, legacyBudgetError).
		Order("usage_backfill_next_sync_at ASC, domain ASC").
		Limit(limit).Find(&rows).Error
	return rows, err
}

type dueUpstreamUsageResult struct {
	row    ChannelUpstreamAccount
	synced ChannelUpstreamAccount
	err    error
}

func (m *Monitor) runDueUpstreamUsageAccounts(ctx context.Context, rows []ChannelUpstreamAccount, lane upstreamUsageLane, limit int) int {
	if limit < 1 {
		limit = 1
	}
	if limit > 2 {
		limit = 2
	}
	results := make(chan dueUpstreamUsageResult, limit)
	next, active, attempted := 0, 0, 0
	launch := func(row ChannelUpstreamAccount) {
		active++
		go func() {
			syncCtx, cancel := context.WithTimeout(ctx, upstreamUsageOperationTimeoutForLane(m.cfg, lane))
			defer cancel()
			synced, syncErr := m.syncStoredUpstreamUsageLaneBackground(syncCtx, row.Domain, lane)
			results <- dueUpstreamUsageResult{row: row, synced: synced, err: syncErr}
		}()
	}
	fill := func() {
		for next < len(rows) && attempted+active < limit && ctx.Err() == nil {
			launch(rows[next])
			next++
		}
	}
	fill()
	for active > 0 {
		result := <-results
		active--
		if errors.Is(result.err, errUpstreamAccountBusy) {
			// busy 是本地调度让路，不消耗本轮真实同步名额。
			fill()
			continue
		}
		attempted++
		if result.err != nil {
			message := result.synced.UsageLastError
			if lane == upstreamUsageLaneHistory || message == "" {
				message = result.synced.UsageBackfillLastError
			}
			slog.Warn("上游使用日志同步失败", "domain", result.row.Domain, "provider", result.row.Provider, "lane", lane, "err", message)
		}
		fill()
	}
	return attempted
}

// syncChannelUpstreamUsageHandler 是管理员明确触发的单账户日志同步。它只请求
// 已开启日志同步且经过适配的 NewAPI/Sub2API/AICodeWith 账户；不接受任何 URL、凭据或时间范围，避免把该接口
// 变成任意外部请求代理。
func (m *Monitor) syncChannelUpstreamUsageHandler(c *gin.Context) {
	if !m.cfg.UpstreamUsageSyncEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "上游消费同步处于灰度关闭状态"})
		return
	}
	m.serveChannelUpstreamSync(c, upstreamUsageOperationTimeout(m.cfg), m.syncStoredUpstreamUsage,
		"同步上游使用日志失败", func(row ChannelUpstreamAccount) string {
			if row.UsageLastError != "" {
				return row.UsageLastError
			}
			return row.UsageBackfillLastError
		})
}
