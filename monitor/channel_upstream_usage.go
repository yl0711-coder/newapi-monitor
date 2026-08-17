package monitor

// 上游使用日志同步是一个低频、显式启用的旁路任务。它只把日志汇总写入
// Monitor SQLite；不向 NewAPI 主库写入，也不会因管理页查询访问上游。

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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
)

type upstreamUsageResult struct {
	Hours     []ChannelUpstreamUsageHour
	DataUntil int64
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

// sha256Sum 隔离使用日志调度的 hash 细节，避免把凭据混入调度输入。
func sha256Sum(s string) [32]byte { return sha256.Sum256([]byte(s)) }

type newAPIUsageItem struct {
	CreatedAt        int64
	Quota            float64
	PromptTokens     int64
	CompletionTokens int64
}

type newAPIUsagePage struct {
	Items       []newAPIUsageItem
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
		return fmt.Errorf("上游使用日志单轮请求达到安全上限（%d 次）", p.maxRequests)
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

func fetchNewAPIUsagePage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pageNumber int, pacer *upstreamUsageRequestPacer) (newAPIUsagePage, error) {
	if err := pacer.beforeRequest(ctx); err != nil {
		return newAPIUsagePage{}, err
	}
	if pageNumber <= 0 {
		return newAPIUsagePage{}, fmt.Errorf("NewAPI 使用日志页码无效")
	}
	query := url.Values{}
	query.Set("p", strconv.Itoa(pageNumber))
	query.Set("page_size", strconv.Itoa(upstreamUsagePageSize))
	query.Set("type", "2")
	query.Set("start_timestamp", strconv.FormatInt(from, 10))
	// NewAPI's existing filter is inclusive. Use to-1 so every Monitor window
	// remains the documented half-open interval [from,to), including splits.
	query.Set("end_timestamp", strconv.FormatInt(to-1, 10))
	headers := map[string]string{"Authorization": "Bearer " + cred.AccessToken, "New-Api-User": strconv.FormatInt(row.UserID, 10)}
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/log/")+"?"+query.Encode(), headers, nil)
	if err != nil {
		var statusErr *upstreamHTTPError
		if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
			return newAPIUsagePage{}, &upstreamAuthError{err: err}
		}
		return newAPIUsagePage{}, err
	}
	var data struct {
		Items []json.RawMessage `json:"items"`
		Total json.RawMessage   `json:"total"`
	}
	if err := decodeNewAPIData(body, &data); err != nil {
		return newAPIUsagePage{}, err
	}
	total, err := rawJSONNumber(data.Total)
	if err != nil || total < 0 || total != float64(int64(total)) {
		return newAPIUsagePage{}, fmt.Errorf("NewAPI 使用日志缺少有效 total")
	}
	fingerprint, err := canonicalUsagePageFingerprint(data.Items)
	if err != nil {
		return newAPIUsagePage{}, fmt.Errorf("NewAPI 使用日志条目无效: %w", err)
	}
	page := newAPIUsagePage{
		Items:       make([]newAPIUsageItem, 0, len(data.Items)),
		Total:       int64(total),
		Fingerprint: fingerprint,
	}
	for _, itemJSON := range data.Items {
		var raw struct {
			CreatedAt        json.RawMessage `json:"created_at"`
			Quota            json.RawMessage `json:"quota"`
			PromptTokens     json.RawMessage `json:"prompt_tokens"`
			CompletionTokens json.RawMessage `json:"completion_tokens"`
		}
		if err := json.Unmarshal(itemJSON, &raw); err != nil {
			return newAPIUsagePage{}, fmt.Errorf("NewAPI 使用日志条目无效: %w", err)
		}
		created, err := rawJSONNumber(raw.CreatedAt)
		if err != nil {
			return newAPIUsagePage{}, fmt.Errorf("NewAPI 使用日志缺少有效 created_at")
		}
		quota, err := rawJSONNumber(raw.Quota)
		if err != nil {
			return newAPIUsagePage{}, fmt.Errorf("NewAPI 使用日志缺少有效 quota")
		}
		prompt, _ := rawJSONNumber(raw.PromptTokens)
		completion, _ := rawJSONNumber(raw.CompletionTokens)
		page.Items = append(page.Items, newAPIUsageItem{
			CreatedAt: int64(created), Quota: quota,
			PromptTokens: int64(prompt), CompletionTokens: int64(completion),
		})
	}
	return page, nil
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
			bucket = &ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, Provider: row.Provider}
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
			buckets[hour] = &ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, Provider: row.Provider}
		}
	}
	out := make([]ChannelUpstreamUsageHour, 0, len(buckets))
	for _, bucket := range buckets {
		bucket.CostUSD = bucket.Quota / unit
		out = append(out, *bucket)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HourTs < out[j].HourTs })
	return upstreamUsageResult{Hours: out, DataUntil: to}, nil
}

func (m *Monitor) syncNewAPIUsage(ctx context.Context, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	return fetchNewAPIUsageWindowWithPacer(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, pacer)
}

// persistUpstreamUsageWindow 仅在完整读取窗口后替换同一窗口的小时桶。事务失败时保留
// 原数据，确保页面既不会出现半窗口，也不会把上游故障解释为零消费。
func (m *Monitor) persistUpstreamUsageWindow(ctx context.Context, domain string, from, to int64, hours []ChannelUpstreamUsageHour, now int64) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
	})
}

func applyUpstreamUsageResult(row *ChannelUpstreamAccount, result upstreamUsageResult, err error, now int64, s Settings, secrets ...string) {
	row.UsageLastAttemptAt = now
	if err == nil {
		row.UsageStatus, row.UsageLastError = upstreamStatusOK, ""
		row.UsageLastSuccessAt, row.UsageDataUntil = now, result.DataUntil
		row.UsageConsecutiveFails = 0
		row.UsageNextSyncAt = nextUpstreamUsageSyncAt(s, row.Domain, now, 0)
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
		row.UsageNextSyncAt = nextUpstreamUsageSyncAt(s, row.Domain, now, row.UsageConsecutiveFails)
	}
	if retryAt := upstreamRetryAt(err); retryAt > row.UsageNextSyncAt {
		row.UsageNextSyncAt = retryAt
	}
}

type upstreamUsageSyncPlan struct {
	tailFrom     int64
	tailTo       int64
	backfillFrom int64
	backfillTo   int64
}

// planUpstreamUsageSync always schedules the open-day tail independently of
// historical work. A long backfill can therefore never hide today's spend.
func planUpstreamUsageSync(row ChannelUpstreamAccount, now int64, backfillDays int) upstreamUsageSyncPlan {
	today := cstDayStart(now)
	plan := upstreamUsageSyncPlan{tailFrom: today, tailTo: now}
	if row.UsageDataUntil > today {
		plan.tailFrom = row.UsageDataUntil - int64(upstreamUsageTailOverlap/time.Second)
		if plan.tailFrom < today {
			plan.tailFrom = today
		}
		plan.tailFrom -= plan.tailFrom % 3600
	}
	backfill := row.UsageBackfillCursor
	if backfill == 0 {
		backfill = cstDayStart(now - int64(backfillDays)*86400)
	}
	if backfill < today && (row.UsageBackfillNextSyncAt == 0 || row.UsageBackfillNextSyncAt <= now) {
		plan.backfillFrom = backfill
		plan.backfillTo = backfill + 86400
	}
	return plan
}

func applyUpstreamUsageBackfillResult(row *ChannelUpstreamAccount, err error, now int64, s Settings, secrets ...string) {
	row.UsageBackfillLastAttemptAt = now
	if err == nil {
		row.UsageBackfillLastSuccessAt = now
		row.UsageBackfillLastError = ""
		row.UsageBackfillConsecutiveFails = 0
		row.UsageBackfillNextSyncAt = nextUpstreamUsageBackfillAt(s, row.Domain, now, 0)
		return
	}
	row.UsageBackfillLastError = sanitizeUpstreamErrorWithSecrets(err, secrets...)
	row.UsageBackfillConsecutiveFails++
	var authErr *upstreamAuthError
	if errors.As(err, &authErr) {
		row.UsageBackfillNextSyncAt = upstreamAccountIsolatedUntil
	} else {
		row.UsageBackfillNextSyncAt = nextUpstreamUsageBackfillAt(s, row.Domain, now, row.UsageBackfillConsecutiveFails)
	}
	if retryAt := upstreamRetryAt(err); retryAt > row.UsageBackfillNextSyncAt {
		row.UsageBackfillNextSyncAt = retryAt
	}
}

func (m *Monitor) syncStoredUpstreamUsage(ctx context.Context, domain string) (ChannelUpstreamAccount, error) {
	m.upstreamSyncMu.Lock()
	defer m.upstreamSyncMu.Unlock()
	var row ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		return row, err
	}
	if !row.Enabled || !row.UsageSyncEnabled {
		return row, fmt.Errorf("该上游账户未启用使用日志同步")
	}
	now := time.Now().Unix()
	if row.Provider != upstreamProviderNewAPI {
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
	cred, ok := credential.(newAPICredential)
	if !ok {
		return row, fmt.Errorf("NewAPI 凭据格式无效")
	}
	secrets := upstreamCredentialSecrets(cred)
	plan := planUpstreamUsageSync(row, now, upstreamUsageBackfillDays(m.cfg))
	pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, upstreamUsageRequestInterval)
	operationRetryAt := int64(0)
	if row.UsageBackfillCursor == 0 {
		row.UsageBackfillCursor = cstDayStart(now - int64(upstreamUsageBackfillDays(m.cfg))*86400)
	}
	// Tail is the primary freshness contract. It runs first on every due pass;
	// only a successful, atomically persisted tail permits additional history.
	result := upstreamUsageResult{DataUntil: row.UsageDataUntil}
	if plan.tailTo > plan.tailFrom {
		result, err = m.syncNewAPIUsage(ctx, row, cred, plan.tailFrom, plan.tailTo, pacer)
		operationRetryAt = upstreamRetryAt(err)
		if err == nil {
			err = m.persistUpstreamUsageWindow(ctx, row.Domain, plan.tailFrom, plan.tailTo, result.Hours, now)
		}
	}
	applyUpstreamUsageResult(&row, result, err, now, m.cfg, secrets...)
	if err == nil && plan.backfillTo > plan.backfillFrom {
		history, historyErr := m.syncNewAPIUsage(ctx, row, cred, plan.backfillFrom, plan.backfillTo, pacer)
		if retryAt := upstreamRetryAt(historyErr); retryAt > operationRetryAt {
			operationRetryAt = retryAt
		}
		if historyErr == nil {
			historyErr = m.persistUpstreamUsageWindow(ctx, row.Domain, plan.backfillFrom, plan.backfillTo, history.Hours, now)
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
	} else if err == nil {
		row.UsageBackfillDone = row.UsageBackfillCursor >= cstDayStart(now)
	}
	if persistErr := m.persistUpstreamAccount(ctx, &row); persistErr != nil {
		return row, persistErr
	}
	if err != nil {
		message := row.UsageLastError
		if message == "" {
			message = row.UsageBackfillLastError
		}
		return row, &upstreamStoredSyncError{message: message, retryAt: operationRetryAt}
	}
	return row, nil
}

func (m *Monitor) syncDueUpstreamUsage(ctx context.Context) {
	now := time.Now().Unix()
	var rows []ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Where("enabled = ? AND usage_sync_enabled = ? AND (usage_next_sync_at = 0 OR usage_next_sync_at <= ?)", true, true, now).Order("usage_next_sync_at ASC").Limit(1).Find(&rows).Error; err != nil {
		slog.Warn("读取待同步上游使用日志失败", "err", err)
		return
	}
	for _, row := range rows {
		syncCtx, cancel := context.WithTimeout(ctx, upstreamUsageOperationTimeout(m.cfg))
		synced, err := m.syncStoredUpstreamUsage(syncCtx, row.Domain)
		cancel()
		if err != nil {
			message := synced.UsageLastError
			if message == "" {
				message = synced.UsageBackfillLastError
			}
			slog.Warn("上游使用日志同步失败", "domain", row.Domain, "provider", row.Provider, "err", message)
		}
	}
}

// syncChannelUpstreamUsageHandler 是管理员明确触发的单账户日志同步。它仍只请求
// 已开启日志同步的 NewAPI 账户；不接受任何 URL、凭据或时间范围，避免把该接口
// 变成任意外部请求代理。
func (m *Monitor) syncChannelUpstreamUsageHandler(c *gin.Context) {
	m.serveChannelUpstreamSync(c, upstreamUsageOperationTimeout(m.cfg), m.syncStoredUpstreamUsage,
		"同步上游使用日志失败", func(row ChannelUpstreamAccount) string {
			if row.UsageLastError != "" {
				return row.UsageLastError
			}
			return row.UsageBackfillLastError
		})
}
