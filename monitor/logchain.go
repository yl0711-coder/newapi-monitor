package monitor

// 客户排障链路（P1）。回答"某客户某条请求走了哪个上游、上游返回了什么"。
//
// 与客户自助面（portal.go / LogRow）的根本区别：
//   - 本文件产出渠道 ID / 渠道名 / 上游主域名，属经营内部信息，仅 view 组（管理员及以上）可读，
//     绝不可挂到 portal 或 public 面。
//   - content 原文直出，不过 scrubContent。scrubContent 见 content 含"渠道"二字即整段清空，
//     而 new-api 的上游错误原文常形如"渠道 xxx (#12) 返回错误：..."——排障恰恰要看这句。
//
// 数据来源：生产 logs 表（含 type=5 错误），不走本地事实表——事实表口径是 type IN (2,6)，
// 排除了错误日志。channel_id → base_domain 的补全查本地 channel_snaps（生产库与本地库
// 是两个连接，无法在单条 SQL 里 join，因此分两步）。
//
// 已知盲区（见 serveLogChainRequests 返回的 blind_spots，不要在 UI 上假装没有）：
//  1. 未打到渠道即被拒的请求（限流/无可用渠道/分组无权限）不在 logs 里，
//     只在 stability_reject_hours 的小时聚合中，且该表无 user_id，定位不到具体客户。
//  2. new-api 换渠道重试会落多条 type=5，本层无法把它们归并成一次客户请求。
//  3. 从不采集请求/响应正文。

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	logChainDefaultLimit   = 50
	logChainMaxLimit       = 200
	logChainMaxDays        = 31   // 单次查询最大跨度，防全表扫
	logChainQueryTimeoutMS = 8000 // 生产库 MAX_EXECUTION_TIME 上限
	logChainMaxDomainChans = 500  // 域名反查渠道 ID 的条数上限，防 IN 列表爆炸
)

// LogChainRow 一条请求的排障视图。含渠道与上游主域名——仅管理员面可见。
type LogChainRow struct {
	ID        int64  `json:"id"`
	CreatedAt int64  `json:"created_at"`
	Type      int    `json:"type"`
	TypeName  string `json:"type_name"`
	RequestID string `json:"request_id,omitempty"`

	// 客户侧
	UserID    int64  `json:"user_id"`
	Member    string `json:"member"`
	Group     string `json:"group"`
	TokenName string `json:"token_name"`

	// 上游侧：channel_id 来自 logs，其余由本地 channel_snaps 补全
	ChannelID         int64  `json:"channel_id"`
	ChannelName       string `json:"channel_name,omitempty"`
	ChannelVendor     string `json:"channel_vendor,omitempty"`
	UpstreamDomain    string `json:"upstream_domain,omitempty"`
	ChannelStatus     int    `json:"channel_status,omitempty"`
	ChannelDeleted    bool   `json:"channel_deleted,omitempty"`
	ChannelUnresolved bool   `json:"channel_unresolved,omitempty"` // 快照查不到该渠道

	// 请求侧
	ModelName         string `json:"model_name"`
	UpstreamModelName string `json:"upstream_model_name,omitempty"` // 模型映射后上游实际收到的名字
	IsModelMapped     bool   `json:"is_model_mapped,omitempty"`
	PromptTokens      int64  `json:"prompt_tokens"`
	CompletionTokens  int64  `json:"completion_tokens"`
	CacheReadTokens   int64  `json:"cache_read_tokens,omitempty"`
	UseTime           int64  `json:"use_time"`
	IsStream          bool   `json:"is_stream"`
	FirstByteMs       int64  `json:"first_byte_ms,omitempty"`
	RequestPath       string `json:"request_path,omitempty"`

	// CostUSD 仅消费(type=2)有意义，同 LogRow 口径：其它类型 quota 恒为 0，折美元会误导。
	CostUSD float64 `json:"cost_usd"`

	// Content 是 logs.content 原文，未经 scrubContent。错误(type=5)的上游返回全在这里。
	Content string `json:"content,omitempty"`
}

// logChainScope 已校验的查询范围。所有字段都来自用户输入但已收敛到安全区间。
type logChainScope struct {
	FromTs    int64
	ToTs      int64
	UserID    int64
	ChannelID int64
	Domain    string
	Model     string
	Group     string
	TokenName string
	RequestID string
	Keyword   string
	ErrorOnly bool
	LogType   int
	BeforeID  int64
	Limit     int
}

// parseLogChainScope 解析并收敛查询参数。时间窗按 CST 自然日左闭右开，与事实层口径一致。
// 任何越界值都收敛而非报错，只有语义矛盾（from/to 只给一个、日期格式错）才拒绝。
func parseLogChainScope(c *gin.Context, now time.Time) (logChainScope, error) {
	now = now.In(cstLocation)
	s := logChainScope{Limit: logChainDefaultLimit}

	days, _ := strconv.Atoi(c.DefaultQuery("days", "1"))
	if days < 1 {
		days = 1
	}
	if days > logChainMaxDays {
		days = logChainMaxDays
	}
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, cstLocation).AddDate(0, 0, -days+1)
	to := now.Add(time.Second) // 含当前秒，避免刚发生的请求查不到

	fromText, toText := strings.TrimSpace(c.Query("from")), strings.TrimSpace(c.Query("to"))
	if fromText != "" || toText != "" {
		if fromText == "" || toText == "" {
			return logChainScope{}, errors.New("from 和 to 必须同时提供")
		}
		f, err := time.ParseInLocation("2006-01-02", fromText, cstLocation)
		if err != nil {
			return logChainScope{}, errors.New("from 日期格式应为 YYYY-MM-DD")
		}
		t, err := time.ParseInLocation("2006-01-02", toText, cstLocation)
		if err != nil {
			return logChainScope{}, errors.New("to 日期格式应为 YYYY-MM-DD")
		}
		t = t.AddDate(0, 0, 1) // to 当天整日纳入
		if t.Before(f) {
			return logChainScope{}, errors.New("to 不能早于 from")
		}
		// 跨度硬上限：宁可截断也不放行全表扫。
		if t.Sub(f) > time.Duration(logChainMaxDays)*24*time.Hour {
			f = t.AddDate(0, 0, -logChainMaxDays)
		}
		from, to = f, t
	}
	s.FromTs, s.ToTs = from.Unix(), to.Unix()

	s.UserID, _ = strconv.ParseInt(strings.TrimSpace(c.Query("user_id")), 10, 64)
	s.ChannelID, _ = strconv.ParseInt(strings.TrimSpace(c.Query("channel_id")), 10, 64)
	s.Domain = strings.ToLower(strings.TrimSpace(c.Query("domain")))
	s.Model = strings.TrimSpace(c.Query("model"))
	s.Group = strings.TrimSpace(c.Query("group"))
	s.TokenName = strings.TrimSpace(c.Query("token_name"))
	s.RequestID = strings.TrimSpace(c.Query("request_id"))
	s.Keyword = strings.TrimSpace(c.Query("keyword"))
	s.ErrorOnly = c.Query("error_only") == "true"
	if t, err := strconv.Atoi(strings.TrimSpace(c.Query("type"))); err == nil && t >= 1 && t <= 6 {
		s.LogType = t
	}
	if s.ErrorOnly && s.LogType != 0 && s.LogType != 5 {
		return logChainScope{}, errors.New("error_only 与 type 冲突：error_only=true 时 type 只能为 5")
	}
	if b, err := strconv.ParseInt(strings.TrimSpace(c.Query("before_id")), 10, 64); err == nil && b > 0 {
		s.BeforeID = b
	}
	if l, err := strconv.Atoi(strings.TrimSpace(c.Query("limit"))); err == nil && l > 0 {
		s.Limit = l
	}
	if s.Limit > logChainMaxLimit {
		s.Limit = logChainMaxLimit
	}
	return s, nil
}

// resolveDomainChannelIDs 把上游主域名反查成渠道 ID 列表。base_domain 是 channel_snaps
// 的索引列，且渠道删除后快照保留，因此历史请求也能按域名筛到。
// 命中数超过上限时返回 truncated=true，由调用方明确告知前端结果不完整——不静默截断。
func (m *Monitor) resolveDomainChannelIDs(ctx context.Context, domain string) (ids []int64, truncated bool, err error) {
	if domain == "" {
		return nil, false, nil
	}
	var rows []struct{ ID int64 }
	q := m.storeDB.WithContext(ctx).Raw(
		"SELECT id FROM channel_snaps WHERE base_domain = ? ORDER BY id LIMIT ?",
		domain, logChainMaxDomainChans+1)
	if err := q.Scan(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("按域名反查渠道失败: %w", err)
	}
	if len(rows) > logChainMaxDomainChans {
		rows = rows[:logChainMaxDomainChans]
		truncated = true
	}
	ids = make([]int64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	return ids, truncated, nil
}

// logChainWhere 拼生产库 WHERE。全部用户可控值参数化，无拼接注入。
// domainChans 为 nil 表示未按域名筛；非 nil 且为空表示该域名无对应渠道（调用方应直接返回空集）。
func logChainWhere(s logChainScope, domainChans []int64) (string, []any) {
	where := "created_at >= ? AND created_at < ?"
	args := []any{s.FromTs, s.ToTs}

	// 与既有口径一致：排除渠道测试流量，只看真实客户请求。
	where += " AND NOT (" + channelTestLogPredicateSQL() + ")"

	switch {
	case s.ErrorOnly:
		where += " AND type = 5"
	case s.LogType > 0:
		where += " AND type = ?"
		args = append(args, s.LogType)
	default:
		// 排障默认只看消费与错误：充值/管理/系统日志与"请求走了哪个上游"无关，
		// 混进来会把错误行挤出首页。要看它们请显式传 type。
		where += " AND type IN (2,5)"
	}
	if s.UserID > 0 {
		where += " AND user_id = ?"
		args = append(args, s.UserID)
	}
	if s.ChannelID > 0 {
		where += " AND channel_id = ?"
		args = append(args, s.ChannelID)
	}
	if len(domainChans) > 0 {
		inSQL, inArgs := usageIn("channel_id", domainChans)
		where += " AND " + inSQL
		args = append(args, inArgs...)
	}
	if s.Model != "" {
		where += " AND model_name = ?"
		args = append(args, s.Model)
	}
	if s.Group != "" {
		where += " AND `group` = ?"
		args = append(args, s.Group)
	}
	if s.TokenName != "" {
		where += " AND token_name LIKE ? ESCAPE '!'"
		args = append(args, "%"+escapeLike(s.TokenName)+"%")
	}
	if s.RequestID != "" { // logs.request_id 有独立索引 idx_logs_request_id
		where += " AND request_id = ?"
		args = append(args, s.RequestID)
	}
	if s.Keyword != "" {
		where += " AND content LIKE ? ESCAPE '!'"
		args = append(args, "%"+escapeLike(s.Keyword)+"%")
	}
	if s.BeforeID > 0 { // 游标翻页：id 近似时间序，倒序取更早的，不用深 OFFSET
		where += " AND id < ?"
		args = append(args, s.BeforeID)
	}
	return where, args
}

// queryLogChain 查生产 logs 取一页排障明细。多取一行判断 has_more，不做 COUNT(*)。
func (m *Monitor) queryLogChain(ctx context.Context, s logChainScope, domainChans []int64) ([]LogChainRow, bool, error) {
	if m.prodDB == nil {
		return nil, false, errors.New("生产库未连接：本地快照只读模式无法查询请求明细")
	}
	cctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if err := m.acquireInteractiveUsageDetailGate(cctx); err != nil {
		return nil, false, fmt.Errorf("等待日志查询槽位失败: %w", err)
	}
	defer m.releaseUsageDetailGate()

	where, args := logChainWhere(s, domainChans)
	// COALESCE 全列：历史版本与迁移数据可能留 NULL，直接 Scan 进 int64 会让整页返回 500。
	q := "SELECT /*+ MAX_EXECUTION_TIME(" + strconv.Itoa(logChainQueryTimeoutMS) + ") */" +
		" id, created_at, COALESCE(type,0), COALESCE(user_id,0), COALESCE(username,'')," +
		" COALESCE(`group`,''), COALESCE(token_name,''), COALESCE(channel_id,0)," +
		" COALESCE(model_name,''), COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0)," +
		" COALESCE(use_time,0), COALESCE(is_stream,0), COALESCE(quota,0)," +
		" COALESCE(content,''), COALESCE(other,''), COALESCE(request_id,'')" +
		" FROM logs WHERE " + where +
		" ORDER BY id DESC LIMIT " + strconv.Itoa(s.Limit+1)

	rows, err := m.prodDB.QueryContext(cctx, q, args...)
	if err != nil {
		return nil, false, fmt.Errorf("排障日志查询失败: %w", err)
	}
	defer rows.Close()

	out := make([]LogChainRow, 0, s.Limit+1)
	for rows.Next() {
		var r LogChainRow
		var quota int64
		var isStream int
		var content, other string
		if err := rows.Scan(&r.ID, &r.CreatedAt, &r.Type, &r.UserID, &r.Member,
			&r.Group, &r.TokenName, &r.ChannelID, &r.ModelName, &r.PromptTokens,
			&r.CompletionTokens, &r.UseTime, &isStream, &quota,
			&content, &other, &r.RequestID); err != nil {
			return nil, false, err
		}
		r.TypeName = logTypeName(r.Type)
		r.IsStream = isStream != 0
		// 同 LogRow：非消费类型 quota 恒为 0，折美元会得 $0.00 误导对账。
		if r.Type == 2 {
			r.CostUSD = float64(quota) / quotaPerUSD
		}
		// 关键差异：不过 scrubContent。管理员面要看含"渠道 xxx"字样的上游错误原文。
		r.Content = content
		if o := parseLogOther(other); o != nil {
			if r.IsStream && o.FRT > 0 {
				r.FirstByteMs = int64(o.FRT)
			}
			r.CacheReadTokens = int64(o.CacheTokens)
			r.RequestPath = o.RequestPath
			if o.IsModelMapped && o.UpstreamModelName != "" {
				r.IsModelMapped = true
				r.UpstreamModelName = o.UpstreamModelName
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > s.Limit
	if hasMore {
		out = out[:s.Limit]
	}
	return out, hasMore, nil
}

// attachChannelSnaps 用本地 channel_snaps 补全渠道名/厂商/上游主域名。
// 生产库与本地库是两个连接，不能在一条 SQL 里 join，故分两步。
//
// 查不到快照时标 ChannelUnresolved 而不是留空：留空会被读成"没有上游域名"，
// 而真实含义是"我们的快照没覆盖到这个渠道"，两者排障动作完全不同。
func (m *Monitor) attachChannelSnaps(ctx context.Context, rows []LogChainRow) error {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(rows))
	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		if r.ChannelID <= 0 {
			continue // channel_id=0：未打到渠道即失败，无快照可补
		}
		if _, ok := seen[r.ChannelID]; ok {
			continue
		}
		seen[r.ChannelID] = struct{}{}
		ids = append(ids, r.ChannelID)
	}
	if len(ids) == 0 {
		return nil
	}
	var snaps []ChannelSnap
	if err := m.storeDB.WithContext(ctx).
		Select("id", "name", "vendor", "base_domain", "status", "deleted_at").
		Where("id IN ?", ids).Find(&snaps).Error; err != nil {
		return fmt.Errorf("读取渠道快照失败: %w", err)
	}
	byID := make(map[int64]ChannelSnap, len(snaps))
	for _, s := range snaps {
		byID[int64(s.ID)] = s
	}
	for i := range rows {
		if rows[i].ChannelID <= 0 {
			continue
		}
		s, ok := byID[rows[i].ChannelID]
		if !ok {
			rows[i].ChannelUnresolved = true
			continue
		}
		rows[i].ChannelName = s.Name
		rows[i].ChannelVendor = s.Vendor
		rows[i].UpstreamDomain = s.BaseDomain
		rows[i].ChannelStatus = s.Status
		rows[i].ChannelDeleted = s.DeletedAt > 0
	}
	return nil
}

// logChainBlindSpots 是本接口结构性答不了的问题。随响应一起返回，让前端必须显式面对：
// 排障工具最危险的失效方式是"查不到"被读成"没发生过"。
func logChainBlindSpots() []string {
	return []string{
		"未打到渠道即被拒的请求（限流/无可用渠道/分组无权限）不在本结果内：这类记录不写 logs，" +
			"只在 stability_reject_hours 的小时聚合里，且该表无 user_id，无法定位到具体客户。" +
			"客户报“请求根本发不出去”时，本接口查不到属预期，不代表没发生。",
		"new-api 换渠道重试会落多条 type=5：本接口按条列出，但无法归并成一次客户请求，" +
			"看到 N 条错误不等于失败 N 次。",
		"从不采集请求/响应正文：“回答内容质量不对”这类问题本接口答不了。",
	}
}

// serveLogChainRequests GET /logchain/requests
// 管理员排障：按客户/渠道/上游域名/模型筛请求，看上游返回的错误原文。
func (m *Monitor) serveLogChainRequests(c *gin.Context) {
	scope, err := parseLogChainScope(c, time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx := c.Request.Context()

	// 按域名筛：先本地反查渠道 ID。域名无对应渠道时直接返回空集，不去打生产库。
	var domainChans []int64
	domainTruncated := false
	if scope.Domain != "" {
		domainChans, domainTruncated, err = m.resolveDomainChannelIDs(ctx, scope.Domain)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if len(domainChans) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"ok": true, "rows": []LogChainRow{}, "has_more": false,
				"scope": logChainScopeEcho(scope), "blind_spots": logChainBlindSpots(),
				"note": "该上游主域名在本地渠道快照中没有对应渠道",
			})
			return
		}
	}

	rows, hasMore, err := m.queryLogChain(ctx, scope, domainChans)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := m.attachChannelSnaps(ctx, rows); err != nil {
		// 渠道补全失败不吞整页：明细本身有效，标注补全缺失即可。
		c.JSON(http.StatusOK, gin.H{
			"ok": true, "rows": rows, "has_more": hasMore,
			"scope": logChainScopeEcho(scope), "blind_spots": logChainBlindSpots(),
			"channel_enrich_error": err.Error(),
		})
		return
	}
	resp := gin.H{
		"ok": true, "rows": rows, "has_more": hasMore,
		"scope": logChainScopeEcho(scope), "blind_spots": logChainBlindSpots(),
	}
	if hasMore && len(rows) > 0 {
		resp["next_before_id"] = rows[len(rows)-1].ID
	}
	if domainTruncated {
		resp["domain_channels_truncated"] = true
		resp["note"] = fmt.Sprintf("该域名下渠道数超过 %d，仅取前 %d 个渠道的请求，结果不完整",
			logChainMaxDomainChans, logChainMaxDomainChans)
	}
	c.JSON(http.StatusOK, resp)
}

// logChainScopeEcho 回显生效范围。用户传的值可能被收敛过（跨度截断、limit 上限），
// 不回显的话前端会以为筛选条件按原样生效了。
func logChainScopeEcho(s logChainScope) gin.H {
	h := gin.H{
		"from_ts": s.FromTs,
		"to_ts":   s.ToTs,
		"from":    time.Unix(s.FromTs, 0).In(cstLocation).Format("2006-01-02 15:04:05"),
		"to":      time.Unix(s.ToTs, 0).In(cstLocation).Format("2006-01-02 15:04:05"),
		"limit":   s.Limit,
	}
	if s.UserID > 0 {
		h["user_id"] = s.UserID
	}
	if s.ChannelID > 0 {
		h["channel_id"] = s.ChannelID
	}
	if s.Domain != "" {
		h["domain"] = s.Domain
	}
	if s.Model != "" {
		h["model"] = s.Model
	}
	if s.Group != "" {
		h["group"] = s.Group
	}
	if s.TokenName != "" {
		h["token_name"] = s.TokenName
	}
	if s.RequestID != "" {
		h["request_id"] = s.RequestID
	}
	if s.Keyword != "" {
		h["keyword"] = s.Keyword
	}
	if s.ErrorOnly {
		h["error_only"] = true
	}
	if s.LogType > 0 {
		h["type"] = s.LogType
	}
	return h
}
