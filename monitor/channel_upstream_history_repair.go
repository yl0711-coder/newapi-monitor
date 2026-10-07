package monitor

// A deliberately narrow historical-prefix repair for Sub2API accounts. It
// never rewinds the live/history cursors and never treats an empty response as
// proof of zero spend. Other providers need their own identity/coverage proof.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type upstreamHistoricalDayInput struct {
	Domain         string `json:"domain"`
	Day            string `json:"day"`
	ExpectedDigest string `json:"expected_digest"`
}

type upstreamHistoricalDayView struct {
	Domain        string  `json:"domain"`
	Day           string  `json:"day"`
	Provider      string  `json:"provider"`
	Adapter       string  `json:"adapter"`
	FirstStoredAt int64   `json:"first_stored_at"`
	AnchorDay     string  `json:"anchor_day"`
	AnchorMatched bool    `json:"anchor_matched"`
	Buckets       int     `json:"buckets"`
	Requests      int64   `json:"requests"`
	Tokens        int64   `json:"tokens"`
	CostUSD       float64 `json:"cost_usd"`
	Digest        string  `json:"digest"`
	Applied       bool    `json:"applied"`
}

// One immutable receipt per repaired day. It is inserted in the same
// transaction as the published buckets, so a successful repair is auditable
// even after later report-cache refreshes or process restarts.
type ChannelUpstreamHistoricalRepair struct {
	Domain      string  `gorm:"primaryKey;size:253;column:domain"`
	DayTs       int64   `gorm:"primaryKey;column:day_ts"`
	Provider    string  `gorm:"size:24;column:provider"`
	SourceEpoch string  `gorm:"size:64;column:source_epoch"`
	Adapter     string  `gorm:"size:32;column:adapter"`
	Digest      string  `gorm:"size:64;column:digest"`
	Requests    int64   `gorm:"column:requests"`
	Tokens      int64   `gorm:"column:tokens"`
	CostUSD     float64 `gorm:"column:cost_usd"`
	Actor       string  `gorm:"size:128;column:actor"`
	CreatedAt   int64   `gorm:"column:created_at;index"`
}

var errUpstreamHistoricalDayConflict = errors.New("历史补采范围或预览依据已变化，请重新预览")

func parseClosedHistoricalDay(day string, now time.Time) (int64, int64, error) {
	parsed, err := time.ParseInLocation("2006-01-02", day, cstLocation)
	if err != nil || parsed.Format("2006-01-02") != day {
		return 0, 0, fmt.Errorf("请输入有效的中国自然日 YYYY-MM-DD")
	}
	from := parsed.Unix()
	to := from + 86400
	if to > cstDayStart(now.Unix()) || from < cstDayStart(now.AddDate(-1, 0, 0).Unix()) {
		return 0, 0, fmt.Errorf("仅支持过去一年内已经结束的自然日")
	}
	return from, to, nil
}

func historicalSub2ResultView(row ChannelUpstreamAccount, day string, firstStoredAt int64, result upstreamUsageResult) (upstreamHistoricalDayView, error) {
	from, to, err := parseClosedHistoricalDay(day, time.Now())
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if result.DataUntil != to || (result.Adapter != upstreamUsageAdapterSub2Trend && result.Adapter != upstreamUsageAdapterSub2Stats) {
		return upstreamHistoricalDayView{}, fmt.Errorf("上游返回的自然日范围或适配类型无效")
	}
	wantBuckets, step := 24, int64(3600)
	if result.Adapter == upstreamUsageAdapterSub2Stats {
		wantBuckets, step = 1, 86400
	}
	if len(result.Hours) != wantBuckets {
		return upstreamHistoricalDayView{}, fmt.Errorf("上游返回的自然日桶数不完整")
	}
	view := upstreamHistoricalDayView{Domain: row.Domain, Day: day, Provider: row.Provider, Adapter: result.Adapter, FirstStoredAt: firstStoredAt, Buckets: len(result.Hours)}
	for i, bucket := range result.Hours {
		if bucket.Domain != row.Domain || bucket.Provider != row.Provider || bucket.HourTs != from+int64(i)*step || bucket.BucketSeconds != step ||
			bucket.Requests < 0 || bucket.Tokens < 0 || bucket.CostUSD < 0 || math.IsNaN(bucket.CostUSD) || math.IsInf(bucket.CostUSD, 0) ||
			bucket.Quota < 0 || math.IsNaN(bucket.Quota) || math.IsInf(bucket.Quota, 0) {
			return upstreamHistoricalDayView{}, fmt.Errorf("上游返回的历史账单边界或金额无效")
		}
		if err := addEconomicsInt64(&view.Requests, bucket.Requests); err != nil {
			return upstreamHistoricalDayView{}, err
		}
		if err := addEconomicsInt64(&view.Tokens, bucket.Tokens); err != nil {
			return upstreamHistoricalDayView{}, err
		}
		view.CostUSD += bucket.CostUSD
	}
	// A blank old day is not evidence that the source retained its history.
	if view.Requests == 0 && view.Tokens == 0 && view.CostUSD == 0 {
		return upstreamHistoricalDayView{}, fmt.Errorf("上游返回空历史日，不能据此补成零消费")
	}
	if math.IsInf(view.CostUSD, 0) || math.IsNaN(view.CostUSD) {
		return upstreamHistoricalDayView{}, fmt.Errorf("上游历史金额溢出")
	}
	canonical, err := json.Marshal(struct {
		Epoch         string
		FirstStoredAt int64
		Day           string
		Adapter       string
		Hours         []ChannelUpstreamUsageHour
	}{newAPIUpstreamAccountEpoch(row), firstStoredAt, day, result.Adapter, result.Hours})
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	sum := sha256.Sum256(canonical)
	view.Digest = hex.EncodeToString(sum[:])
	return view, nil
}

func loadSub2HistoricalAnchor(ctx context.Context, db *gorm.DB, row ChannelUpstreamAccount, firstStoredAt int64) (upstreamHistoricalDayView, []ChannelUpstreamUsageHour, error) {
	var firstNonzero int64
	if err := db.WithContext(ctx).Raw(`SELECT COALESCE(MIN(hour_ts),0) FROM channel_upstream_usage_hours
		WHERE domain = ? AND (requests > 0 OR tokens > 0 OR cost_usd > 0)`, row.Domain).Scan(&firstNonzero).Error; err != nil {
		return upstreamHistoricalDayView{}, nil, err
	}
	if firstNonzero == 0 {
		return upstreamHistoricalDayView{}, nil, fmt.Errorf("本地没有可核对的非零账单日，拒绝历史补采")
	}
	anchorFrom := cstDayStart(firstNonzero)
	var hours []ChannelUpstreamUsageHour
	if err := db.WithContext(ctx).Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", row.Domain, anchorFrom, anchorFrom+86400).
		Order("hour_ts ASC").Find(&hours).Error; err != nil {
		return upstreamHistoricalDayView{}, nil, err
	}
	adapter := upstreamUsageAdapterSub2Trend
	if len(hours) == 1 && hours[0].BucketSeconds == 86400 {
		adapter = upstreamUsageAdapterSub2Stats
	}
	day := time.Unix(anchorFrom, 0).In(cstLocation).Format("2006-01-02")
	view, err := historicalSub2ResultView(row, day, firstStoredAt, upstreamUsageResult{Hours: hours, DataUntil: anchorFrom + 86400, Adapter: adapter})
	return view, hours, err
}

func historicalSub2AnchorMatches(stored, fetched upstreamHistoricalDayView, storedHours, fetchedHours []ChannelUpstreamUsageHour) bool {
	if stored.Day != fetched.Day || stored.Adapter != fetched.Adapter || stored.Requests != fetched.Requests || stored.Tokens != fetched.Tokens ||
		math.Abs(stored.CostUSD-fetched.CostUSD) > 0.000001 || len(storedHours) != len(fetchedHours) {
		return false
	}
	for i := range storedHours {
		a, b := storedHours[i], fetchedHours[i]
		if a.HourTs != b.HourTs || a.BucketSeconds != b.BucketSeconds || a.Requests != b.Requests || a.Tokens != b.Tokens ||
			math.Abs(a.CostUSD-b.CostUSD) > 0.000001 {
			return false
		}
	}
	return true
}

// The trend adapter inserts zero buckets for omitted hours. A 24-bucket trend
// is therefore not proof that an old day is complete; require the separate
// day-total endpoint to agree before publishing any historical prefix.
func verifySub2HistoricalDailyTotal(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, from, to int64, pacer *upstreamUsageRequestPacer, trend upstreamHistoricalDayView) error {
	if trend.Adapter != upstreamUsageAdapterSub2Trend {
		return nil
	}
	daily, err := fetchSub2APIUsageStats(ctx, client, row, cred, from, to, pacer)
	if err != nil {
		return fmt.Errorf("无法独立核对历史日汇总: %s", sanitizeUpstreamErrorWithSecrets(err, upstreamCredentialSecrets(cred)...))
	}
	dayView, err := historicalSub2ResultView(row, trend.Day, trend.FirstStoredAt, daily)
	if err != nil || dayView.Requests != trend.Requests || dayView.Tokens != trend.Tokens || math.Abs(dayView.CostUSD-trend.CostUSD) > 0.000001 {
		return fmt.Errorf("历史小时账单与上游自然日汇总不一致，拒绝补采")
	}
	return nil
}

func (m *Monitor) inspectSub2HistoricalDay(ctx context.Context, domain, day, expectedDigest, actor string) (upstreamHistoricalDayView, error) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	from, to, err := parseClosedHistoricalDay(day, time.Now())
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if domain == "" {
		return upstreamHistoricalDayView{}, fmt.Errorf("请选择已配置的上游主域名")
	}
	if expectedDigest != "" && strings.TrimSpace(actor) == "" {
		return upstreamHistoricalDayView{}, fmt.Errorf("提交补采缺少可审计的管理员身份")
	}
	release, err := m.acquireUpstreamAccountAdmin(ctx, domain)
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	defer release()
	var row ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if row.Provider != upstreamProviderSub2API || !row.Enabled || !row.UsageSyncEnabled || row.UsageStatus == upstreamStatusReconnect {
		return upstreamHistoricalDayView{}, fmt.Errorf("仅允许已启用且认证正常的 Sub2API 账户补采")
	}
	if (row.UsageStatus == upstreamStatusError && row.UsageNextSyncAt > time.Now().Unix()) || row.UsageBackfillNextSyncAt > time.Now().Unix() {
		return upstreamHistoricalDayView{}, fmt.Errorf("上游账户仍处于同步退避期，不能通过历史补采绕过限频")
	}
	var firstStoredAt int64
	if err := m.storeDB.WithContext(ctx).Raw("SELECT COALESCE(MIN(hour_ts),0) FROM channel_upstream_usage_hours WHERE domain = ?", domain).Scan(&firstStoredAt).Error; err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if firstStoredAt == 0 || to > firstStoredAt {
		return upstreamHistoricalDayView{}, errUpstreamHistoricalDayConflict
	}
	var archived int64
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamUsageArchive{}).
		Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, from, to).Count(&archived).Error; err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if archived != 0 {
		return upstreamHistoricalDayView{}, fmt.Errorf("该自然日存在旧账户归档账单，不能作为当前账户缺口自动补入")
	}
	credential, err := m.credentialForAccount(row)
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	cred, ok := credential.(sub2APICredential)
	if !ok || cred.AccessToken == "" || (cred.ExpiresAt > 0 && cred.ExpiresAt <= time.Now().Add(time.Minute).Unix()) {
		return upstreamHistoricalDayView{}, fmt.Errorf("Sub2API 访问令牌不可用；先让正常同步刷新后再预览")
	}
	// No refresh-token call here: a supposedly read-only preview must not
	// consume a single-use credential needed by the live collector.
	storedAnchor, storedAnchorHours, err := loadSub2HistoricalAnchor(ctx, m.storeDB, row, firstStoredAt)
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	anchorFrom, anchorTo, err := parseClosedHistoricalDay(storedAnchor.Day, time.Now())
	if err != nil {
		return upstreamHistoricalDayView{}, fmt.Errorf("锚点账单日期无效: %w", err)
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamUsageArchive{}).
		Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, from, anchorTo).Count(&archived).Error; err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if archived != 0 {
		return upstreamHistoricalDayView{}, fmt.Errorf("目标日与锚点之间存在旧账户归档账单，拒绝跨账户补采")
	}
	pacer := newUpstreamUsageRequestPacer(4, upstreamUsageRequestInterval)
	remoteAnchor, err := fetchSub2APIUsageWindow(ctx, m.channelUpstreamHTTPClient(), row, cred, anchorFrom, anchorTo, pacer, row.UsageAdapter)
	if err != nil {
		return upstreamHistoricalDayView{}, fmt.Errorf("核对已存账单日失败: %s", sanitizeUpstreamErrorWithSecrets(err, upstreamCredentialSecrets(cred)...))
	}
	fetchedAnchor, err := historicalSub2ResultView(row, storedAnchor.Day, firstStoredAt, remoteAnchor)
	if err != nil || !historicalSub2AnchorMatches(storedAnchor, fetchedAnchor, storedAnchorHours, remoteAnchor.Hours) {
		return upstreamHistoricalDayView{}, fmt.Errorf("已存非零账单日与当前上游账户不一致，拒绝历史补采")
	}
	result, err := fetchSub2APIUsageWindow(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, pacer, row.UsageAdapter)
	if err != nil {
		return upstreamHistoricalDayView{}, fmt.Errorf("读取历史账单失败: %s", sanitizeUpstreamErrorWithSecrets(err, upstreamCredentialSecrets(cred)...))
	}
	view, err := historicalSub2ResultView(row, day, firstStoredAt, result)
	if err != nil {
		return upstreamHistoricalDayView{}, err
	}
	if err := verifySub2HistoricalDailyTotal(ctx, m.channelUpstreamHTTPClient(), row, cred, from, to, pacer, view); err != nil {
		return upstreamHistoricalDayView{}, err
	}
	view.AnchorDay, view.AnchorMatched = storedAnchor.Day, true
	combined := sha256.Sum256([]byte(view.Digest + ":" + storedAnchor.Digest + ":" + fetchedAnchor.Digest))
	view.Digest = hex.EncodeToString(combined[:])
	if expectedDigest == "" {
		return view, nil
	}
	if len(expectedDigest) != 64 || subtle.ConstantTimeCompare([]byte(expectedDigest), []byte(view.Digest)) != 1 {
		return upstreamHistoricalDayView{}, errUpstreamHistoricalDayConflict
	}
	// Recheck both the source identity and the still-empty historical prefix in
	// the same SQLite transaction that publishes the complete day. A normal
	// tail/history worker never changes its cursors as a result of this repair.
	if err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current ChannelUpstreamAccount
		if err := tx.First(&current, "domain = ?", domain).Error; err != nil {
			return err
		}
		if newAPIUpstreamAccountEpoch(current) != newAPIUpstreamAccountEpoch(row) || !current.Enabled || !current.UsageSyncEnabled {
			return errUpstreamHistoricalDayConflict
		}
		var earliest, existing int64
		if err := tx.Raw("SELECT COALESCE(MIN(hour_ts),0) FROM channel_upstream_usage_hours WHERE domain = ?", domain).Scan(&earliest).Error; err != nil {
			return err
		}
		if err := tx.Model(&ChannelUpstreamUsageHour{}).Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, from, to).Count(&existing).Error; err != nil {
			return err
		}
		if earliest != firstStoredAt || existing != 0 {
			return errUpstreamHistoricalDayConflict
		}
		if err := tx.Model(&ChannelUpstreamUsageArchive{}).
			Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, from, anchorTo).Count(&archived).Error; err != nil {
			return err
		}
		if archived != 0 {
			return errUpstreamHistoricalDayConflict
		}
		currentAnchor, _, err := loadSub2HistoricalAnchor(ctx, tx, current, firstStoredAt)
		if err != nil || currentAnchor.Digest != storedAnchor.Digest {
			return errUpstreamHistoricalDayConflict
		}
		var receipts int64
		if err := tx.Model(&ChannelUpstreamHistoricalRepair{}).Where("domain = ? AND day_ts = ?", domain, from).Count(&receipts).Error; err != nil {
			return err
		}
		if receipts != 0 {
			return errUpstreamHistoricalDayConflict
		}
		now := time.Now().Unix()
		if err := persistUpstreamUsageWindowTx(tx, domain, from, to, result.Hours, now); err != nil {
			return err
		}
		return tx.Create(&ChannelUpstreamHistoricalRepair{
			Domain: domain, DayTs: from, Provider: row.Provider, SourceEpoch: newAPIUpstreamAccountEpoch(row),
			Adapter: result.Adapter, Digest: view.Digest, Requests: view.Requests, Tokens: view.Tokens,
			CostUSD: view.CostUSD, Actor: strings.TrimSpace(actor), CreatedAt: now,
		}).Error
	}); err != nil {
		return upstreamHistoricalDayView{}, err
	}
	view.Applied = true
	return view, nil
}

func (m *Monitor) serveSub2HistoricalDay(c *gin.Context, apply bool) {
	if !m.cfg.UpstreamUsageSyncEnabled || !m.cfg.UpstreamUsageHistoryRepairEnabled || m.cfg.LocalSnapshotOnly {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "历史账单单日修复处于灰度关闭状态"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelUpstreamBody)
	var input upstreamHistoricalDayInput
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式无效"})
		return
	}
	allowed := false
	for _, configured := range m.cfg.UpstreamUsageHistoryRepairDomains {
		if strings.EqualFold(strings.TrimSpace(configured), strings.TrimSpace(input.Domain)) {
			allowed = true
			break
		}
	}
	if !allowed {
		c.JSON(http.StatusForbidden, gin.H{"error": "该上游不在历史账单修复白名单"})
		return
	}
	if _, _, err := parseClosedHistoricalDay(input.Day, time.Now()); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if apply && input.ExpectedDigest == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "提交补采前必须提供本次预览摘要"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
	defer cancel()
	expected := ""
	if apply {
		expected = input.ExpectedDigest
	}
	view, err := m.inspectSub2HistoricalDay(ctx, input.Domain, input.Day, expected, c.GetString("uname"))
	if err == nil {
		c.JSON(http.StatusOK, view)
		return
	}
	status := http.StatusBadGateway
	if errors.Is(err, errUpstreamHistoricalDayConflict) {
		status = http.StatusConflict
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	c.JSON(status, gin.H{"error": err.Error()})
}

func (m *Monitor) previewSub2HistoricalDayHandler(c *gin.Context) {
	m.serveSub2HistoricalDay(c, false)
}

func (m *Monitor) applySub2HistoricalDayHandler(c *gin.Context) {
	m.serveSub2HistoricalDay(c, true)
}
