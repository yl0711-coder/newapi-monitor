package monitor

// usage.go:「用户用量」——盯一组指定的 new-api 用户(按邮箱 / 用户ID 添加),
// 按需对生产库 logs 表做窗口化聚合:消费矩阵(用户×日)与单用户详情(每日/分组/模型),费用=quota/500000 美元。
//
// 与采样器的边界:采样器是【常驻周期】查询,这里是【按需】查询——只在打开页面 / 点查询时执行,
// 全部限定时间范围并命中索引(idx_logs_user_id / idx_created_at_type / idx_logs_group / idx_logs_model_name),
// 且同一时刻只放行一条聚合(usageMu 串行化),不给生产库常驻负担。生产库全程只读。
// 名单存本地 sqlite(tracked_users);鉴权沿用全站约定:管理员可看,仅超管可改名单。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// usageTZOffsetSec 天粒度按东八区(CST)切日,与团队运营时区一致(日志本身是 unix 秒,无时区)。
	usageTZOffsetSec = 8 * 3600
	maxUsageDays     = 90  // 单次查询时间范围上限,约束单次扫描量(产品要求:最多90天)
	maxUsageDimRows  = 300 // 分组/模型维度返回上限
)

var usageCST = time.FixedZone("CST", usageTZOffsetSec)

// usageDayExprMySQL 把 created_at(unix 秒)折算成 CST 日序号(自 epoch 起第几天)。
// MySQL 整除用 DIV;测试里(sqlite)用 usageDayExpr 字段覆盖为 '/'(sqlite 整型相除即整除)。
const usageDayExprMySQL = "(created_at + 28800) DIV 86400"

// ---- 聚合查询(生产库,只读、窗口化、走索引) ----

// dayExpr 返回日桶 SQL 表达式;测试用 usageDayExpr 覆盖成 sqlite 兼容写法。
func (m *Monitor) dayExpr() string {
	if m.usageDayExpr != "" {
		return m.usageDayExpr
	}
	return usageDayExprMySQL
}

// UsageDaily 某 CST 自然日的合计。
type UsageDaily struct {
	Date             string  `json:"date"`
	Requests         int64   `json:"requests"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Tokens           int64   `json:"tokens"`
	CostUSD          float64 `json:"cost_usd"`
}

// UsageDailyModel 某 CST 自然日、某模型的合计(供每日消耗按模型堆叠展示用)。
type UsageDailyModel struct {
	Date     string  `json:"date"`
	Model    string  `json:"model"`
	Requests int64   `json:"requests"`
	Tokens   int64   `json:"tokens"`
	CostUSD  float64 `json:"cost_usd"`
}

// UsageDim 某维度取值(分组 / 模型 / 用户)的合计。
type UsageDim struct {
	Key      string  `json:"key"`
	Requests int64   `json:"requests"`
	Tokens   int64   `json:"tokens"`
	CostUSD  float64 `json:"cost_usd"`
}

// UsageStats 一次用户用量查询的完整结果(详情页专用:单用户的每日/分组/模型)。
type UsageStats struct {
	From         string            `json:"from"`
	To           string            `json:"to"`
	Summary      UsageDim          `json:"summary"`
	Daily        []UsageDaily      `json:"daily"`
	DailyByModel []UsageDailyModel `json:"daily_by_model"`
	ByGroup      []UsageDim        `json:"by_group"`
	ByModel      []UsageDim        `json:"by_model"`
}

// usageIn 生成 "<col> IN (?,?,…)" 片段与参数(ids 已由调用方保证非空;col 只传代码内常量,勿传用户输入)。
func usageIn(col string, ids []int64) (string, []any) {
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args[i] = id
	}
	return col + " IN (" + strings.Join(ph, ",") + ")", args
}

// computeUsageStats 对 [fromTs, toTs) 内、指定用户集合的消费日志(type=2)做三路聚合(每日/分组/模型)。
// tokenID>0 时再按令牌过滤(单用户详情下钻单令牌;与 user_id 双条件,隔离不依赖 token 归属校验)。
// 串行化(usageMu):同一时刻最多一条聚合在生产库上跑,叠加连接池上限双保险。
func (m *Monitor) computeUsageStats(ctx context.Context, ids []int64, fromTs, toTs, tokenID int64) (*UsageStats, error) {
	if len(ids) == 0 {
		return &UsageStats{}, nil // 名单为空不该走到这;防御:不拼 "IN ()" 非法 SQL
	}
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	inSQL, inArgs := usageIn("user_id", ids)
	where := "type = 2 AND created_at >= ? AND created_at < ? AND " + inSQL
	args := append([]any{fromTs, toTs}, inArgs...)
	if tokenID > 0 {
		where += " AND token_id = ?"
		args = append(args, tokenID)
	}

	st := &UsageStats{
		From: time.Unix(fromTs, 0).In(usageCST).Format("2006-01-02"),
		To:   time.Unix(toTs-1, 0).In(usageCST).Format("2006-01-02"),
	}

	// 1) 每日:日桶 = CST 日序号,回来再折成日期文本。
	dailyQ := "SELECT " + m.dayExpr() + " AS day_idx, COUNT(*)," +
		" CAST(COALESCE(SUM(prompt_tokens),0) AS SIGNED), CAST(COALESCE(SUM(completion_tokens),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(quota),0) AS SIGNED)" +
		" FROM logs WHERE " + where + " GROUP BY day_idx ORDER BY day_idx"
	rows, err := m.prodDB.QueryContext(cctx, dailyQ, args...)
	if err != nil {
		return nil, fmt.Errorf("按日聚合失败: %w", err)
	}
	for rows.Next() {
		var dayIdx, quota int64
		var d UsageDaily
		if err := rows.Scan(&dayIdx, &d.Requests, &d.PromptTokens, &d.CompletionTokens, &quota); err != nil {
			rows.Close()
			return nil, err
		}
		d.Date = time.Unix(dayIdx*86400-usageTZOffsetSec, 0).In(usageCST).Format("2006-01-02")
		d.Tokens = d.PromptTokens + d.CompletionTokens
		d.CostUSD = float64(quota) / quotaPerUSD
		st.Daily = append(st.Daily, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, d := range st.Daily { // 汇总卡直接由日聚合累加,不再多查一遍
		st.Summary.Requests += d.Requests
		st.Summary.Tokens += d.Tokens
		st.Summary.CostUSD += d.CostUSD
	}

	// 1b) 按日×模型:供「每日消耗」图按模型堆叠展示;LIMIT 是防御性上限(90天×50模型/天),
	// 正常场景远远够用,前端再按 by_model 排序做 top-N + 其他归并。
	dailyModelQ := "SELECT " + m.dayExpr() + " AS day_idx, COALESCE(model_name,''), COUNT(*)," +
		" CAST(COALESCE(SUM(prompt_tokens+completion_tokens),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(quota),0) AS SIGNED)" +
		" FROM logs WHERE " + where + " GROUP BY day_idx, model_name ORDER BY day_idx" +
		" LIMIT " + strconv.Itoa(maxUsageDays*50)
	dmRows, err := m.prodDB.QueryContext(cctx, dailyModelQ, args...)
	if err != nil {
		return nil, fmt.Errorf("按日按模型聚合失败: %w", err)
	}
	for dmRows.Next() {
		var dayIdx, quota int64
		var dm UsageDailyModel
		if err := dmRows.Scan(&dayIdx, &dm.Model, &dm.Requests, &dm.Tokens, &quota); err != nil {
			dmRows.Close()
			return nil, err
		}
		dm.Date = time.Unix(dayIdx*86400-usageTZOffsetSec, 0).In(usageCST).Format("2006-01-02")
		dm.CostUSD = float64(quota) / quotaPerUSD
		st.DailyByModel = append(st.DailyByModel, dm)
	}
	dmRows.Close()
	if err := dmRows.Err(); err != nil {
		return nil, err
	}

	// 2/3) 按分组 / 模型。列名 group 是保留字,必须反引号。
	// (曾有第三路 GROUP BY user_id:前端改成矩阵+单用户详情后无人消费,纯耗生产库,已删。)
	dims := []struct {
		col  string
		dst  *[]UsageDim
		desc string
	}{
		{"COALESCE(`group`,'')", &st.ByGroup, "按分组"},
		{"COALESCE(model_name,'')", &st.ByModel, "按模型"},
	}
	for _, dim := range dims {
		q := "SELECT " + dim.col + " AS k, COUNT(*)," +
			" CAST(COALESCE(SUM(prompt_tokens+completion_tokens),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(quota),0) AS SIGNED)" +
			" FROM logs WHERE " + where +
			" GROUP BY k ORDER BY SUM(quota) DESC LIMIT " + strconv.Itoa(maxUsageDimRows)
		rows, err := m.prodDB.QueryContext(cctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("%s聚合失败: %w", dim.desc, err)
		}
		for rows.Next() {
			var r UsageDim
			var quota int64
			if err := rows.Scan(&r.Key, &r.Requests, &r.Tokens, &quota); err != nil {
				rows.Close()
				return nil, err
			}
			r.CostUSD = float64(quota) / quotaPerUSD
			*dim.dst = append(*dim.dst, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// ---- 列表页矩阵数据(前端渲染为 行=用户 × 列=日期,格=当日消费) ----

// UsageMatrixUser 矩阵列头(一个被盯用户)+ 区间合计。
type UsageMatrixUser struct {
	UserID       int64    `json:"user_id"`
	Username     string   `json:"username"`
	Email        string   `json:"email"`
	GroupID      int64    `json:"group_id"`
	GroupName    string   `json:"group_name"`
	Note         string   `json:"note"`
	TotalUSD     float64  `json:"total_usd"`
	BalanceUSD   *float64 `json:"balance_usd"`    // 主站当前余额(users.quota 折美元);null=主站已删/取不到
	TotalUsedUSD *float64 `json:"total_used_usd"` // 主站累计总消耗(users.used_quota 折美元;终身值,不受90天窗口影响);null=已删/取不到
}

// UsageMatrixCell 稀疏格:某用户某天的消费(无消费的天不出格)。
type UsageMatrixCell struct {
	UserID   int64   `json:"user_id"`
	Date     string  `json:"date"`
	Requests int64   `json:"requests"`
	CostUSD  float64 `json:"cost_usd"`
}

// UsageMatrix 列表页数据:days 连续日期(新→旧)+ 用户(按累计总消耗降序,稳定)+ 稀疏格。
type UsageMatrix struct {
	From  string            `json:"from"`
	To    string            `json:"to"`
	Days  []string          `json:"days"`
	Users []UsageMatrixUser `json:"users"`
	Cells []UsageMatrixCell `json:"cells"`
}

// computeUsageMatrix 一条 GROUP BY user_id×日 的聚合,窗口与索引约束同 computeUsageStats。
func (m *Monitor) computeUsageMatrix(ctx context.Context, ids []int64, fromTs, toTs int64) (*UsageMatrix, error) {
	mx := &UsageMatrix{
		From: time.Unix(fromTs, 0).In(usageCST).Format("2006-01-02"),
		To:   time.Unix(toTs-1, 0).In(usageCST).Format("2006-01-02"),
	}
	// 连续日期轴(新→旧):有没有消费都出行,老板看的是"每个人每天"的完整节奏
	for ts := toTs - 86400; ts >= fromTs; ts -= 86400 {
		mx.Days = append(mx.Days, time.Unix(ts, 0).In(usageCST).Format("2006-01-02"))
	}
	if len(ids) == 0 {
		return mx, nil
	}
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	inSQL, inArgs := usageIn("user_id", ids)
	q := "SELECT user_id, " + m.dayExpr() + " AS day_idx, COUNT(*), CAST(COALESCE(SUM(quota),0) AS SIGNED)" +
		" FROM logs WHERE type = 2 AND created_at >= ? AND created_at < ? AND " + inSQL +
		" GROUP BY user_id, day_idx"
	rows, err := m.prodDB.QueryContext(cctx, q, append([]any{fromTs, toTs}, inArgs...)...)
	if err != nil {
		return nil, fmt.Errorf("矩阵聚合失败: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var uid, dayIdx, reqs, quota int64
		if err := rows.Scan(&uid, &dayIdx, &reqs, &quota); err != nil {
			return nil, err
		}
		mx.Cells = append(mx.Cells, UsageMatrixCell{
			UserID:   uid,
			Date:     time.Unix(dayIdx*86400-usageTZOffsetSec, 0).In(usageCST).Format("2006-01-02"),
			Requests: reqs,
			CostUSD:  float64(quota) / quotaPerUSD,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mx, nil // 用户列(含合计与排序)由 handler 结合名单组装
}

// parseUsageRange 解析 from/to(YYYY-MM-DD,CST 自然日,含端点),空则默认近 7 天;越界自动收敛。
func parseUsageRange(fromStr, toStr string, now time.Time) (fromTs, toTs int64, err error) {
	today := now.In(usageCST).Truncate(0)
	y, mo, d := today.Date()
	todayStart := time.Date(y, mo, d, 0, 0, 0, 0, usageCST)

	to := todayStart
	if toStr != "" {
		if to, err = time.ParseInLocation("2006-01-02", toStr, usageCST); err != nil {
			return 0, 0, fmt.Errorf("结束日期格式应为 YYYY-MM-DD")
		}
	}
	from := to.AddDate(0, 0, -6) // 默认近 7 天(含今天)
	if fromStr != "" {
		if from, err = time.ParseInLocation("2006-01-02", fromStr, usageCST); err != nil {
			return 0, 0, fmt.Errorf("开始日期格式应为 YYYY-MM-DD")
		}
	}
	if from.After(to) {
		from, to = to, from
	}
	// 含两端点共 N 天 ⇔ 零点差 (N-1)*24h;用 >= 卡在恰好 maxUsageDays 天(超一天即 91 天会被拒)
	if to.Sub(from) >= time.Duration(maxUsageDays)*24*time.Hour {
		return 0, 0, fmt.Errorf("时间范围过大,最长 %d 天", maxUsageDays)
	}
	return from.Unix(), to.AddDate(0, 0, 1).Unix(), nil // to 含当天 → 上界取次日 0 点(开区间)
}

// ---- HTTP 处理器 ----

// serveUsageMatrix GET /usage/matrix?from=&to=(管理员):列表页矩阵数据(前端渲染为 行=用户 × 列=日期)。
// 用户 label 取邮箱(缺则用户名/#id)并按【累计总消耗】降序(稳定,不随日期区间变);零消费用户仍保留。
func (m *Monitor) serveUsageMatrix(c *gin.Context) {
	if !m.Enabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	tracked, err := m.listTracked()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	tracked, balances, usedTotals := m.refreshTrackedLabels(c.Request.Context(), tracked) // 身份标签校准 + 取当前余额与累计总消耗
	mx, err := m.computeUsageMatrix(c.Request.Context(), idsOf(tracked), fromTs, toTs)
	if err != nil {
		slog.Warn("用户用量矩阵聚合失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "统计查询失败,请稍后重试(细节见服务端日志)"})
		return
	}
	totals := map[int64]float64{}
	for _, cell := range mx.Cells {
		totals[cell.UserID] += cell.CostUSD
	}
	gm := m.groupNameMap()
	for _, u := range tracked {
		mu := UsageMatrixUser{UserID: u.UserID, Username: u.Username, Email: u.Email,
			GroupID: u.GroupID, GroupName: gm[u.GroupID], Note: u.Note, TotalUSD: totals[u.UserID]}
		if b, ok := balances[u.UserID]; ok {
			bv := b
			mu.BalanceUSD = &bv
		}
		if uq, ok := usedTotals[u.UserID]; ok {
			uv := uq
			mu.TotalUsedUSD = &uv
		}
		mx.Users = append(mx.Users, mu)
	}
	// 排序按【累计总消耗】降序(终身值,与所选日期区间无关)——切换时间范围顺序不变,大客户恒在前;
	// 同值(如都为0/已删)按用户名兜底,保证顺序完全稳定。
	usedOf := func(u UsageMatrixUser) float64 {
		if u.TotalUsedUSD != nil {
			return *u.TotalUsedUSD
		}
		return 0
	}
	sort.SliceStable(mx.Users, func(i, j int) bool {
		ui, uj := usedOf(mx.Users[i]), usedOf(mx.Users[j])
		if ui != uj {
			return ui > uj
		}
		return mx.Users[i].Username < mx.Users[j].Username
	})
	m.portalWarmFromMatrix(mx, tracked, fromTs, toTs) // 写穿透预热:管理端每次刷新顺手把各组客户端缓存灌成最新
	c.JSON(http.StatusOK, gin.H{"enabled": true, "matrix": mx, "empty": len(tracked) == 0})
}

// userBalanceUSD 取单个用户的主站当前余额(users.quota 折美元);查不到/出错返回 nil(前端显示 —)。
func (m *Monitor) userBalanceUSD(ctx context.Context, id int64) *float64 {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var quota int64
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COALESCE(quota,0) FROM users WHERE id = ?", id).Scan(&quota); err != nil {
		slog.Warn("查询用户余额失败", "err", err, "user_id", id)
		return nil
	}
	b := float64(quota) / quotaPerUSD
	return &b
}

// userUsedUSD 取单个用户的主站累计总消耗(users.used_quota 折美元;终身值);查不到/出错返回 nil。
func (m *Monitor) userUsedUSD(ctx context.Context, id int64) *float64 {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var used int64
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COALESCE(used_quota,0) FROM users WHERE id = ?", id).Scan(&used); err != nil {
		slog.Warn("查询用户累计消耗失败", "err", err, "user_id", id)
		return nil
	}
	u := float64(used) / quotaPerUSD
	return &u
}

// TokenUsage 单个令牌在时间范围内的用量。MaskedKey 永远是脱敏串,服务端绝不返回明文 key。
type TokenUsage struct {
	TokenID      int64    `json:"token_id"` // 主站 tokens.id;0=老日志无token_id(不可下钻)
	Owner        string   `json:"owner"`    // 令牌所属用户(展示名:用户名/邮箱/#ID)
	Name         string   `json:"name"`
	MaskedKey    string   `json:"masked_key"`
	Group        string   `json:"group"` // 令牌绑定的分组(计价档);空=跟随用户默认分组/已删
	Requests     int64    `json:"requests"`
	Tokens       int64    `json:"tokens"`
	CostUSD      float64  `json:"cost_usd"`
	TotalCostUSD *float64 `json:"total_cost_usd"` // 累计总消耗(tokens.used_quota 折美元;创建至今终身值,不受日期范围影响);null=令牌已不可查(硬删/老日志无token_id)
	Deleted      bool     `json:"deleted"`        // 已删除令牌(软删有消费仍显示/硬删兜底行);前端沉底+标记,与现存令牌分区
}

// tokenMetaOf 取单令牌元数据(名称/脱敏key/分组/累计/是否已删),强制归属校验(id+user_id 双条件)。
// 查不到(硬删/不属于该用户)返回 nil,不报错——令牌详情页此时只展示日志侧数据。
func (m *Monitor) tokenMetaOf(ctx context.Context, uid, tokenID int64) *TokenUsage {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var name, key, group string
	var used int64
	var deleted bool
	q := "SELECT COALESCE(name,''), COALESCE(`key`,''), COALESCE(`group`,''), CAST(COALESCE(used_quota,0) AS SIGNED), (deleted_at IS NOT NULL)" +
		" FROM tokens WHERE id = ? AND user_id = ?"
	if err := m.prodDB.QueryRowContext(cctx, q, tokenID, uid).Scan(&name, &key, &group, &used, &deleted); err != nil {
		return nil
	}
	total := float64(used) / quotaPerUSD
	return &TokenUsage{TokenID: tokenID, Name: name, MaskedKey: maskTokenKey(key), Group: group, TotalCostUSD: &total, Deleted: deleted}
}

// maskTokenKey 与 new-api 的 MaskTokenKey 同风格。tokens.key 不含 sk- 前缀,
// 客户实际用的是 sk-<key>,故脱敏串带 sk- 前缀以便客户辨认,同时绝不暴露完整 key。
func maskTokenKey(key string) string {
	n := len(key)
	switch {
	case n == 0:
		return ""
	case n <= 4:
		return strings.Repeat("*", n)
	case n <= 8:
		return "sk-" + key[:2] + "****" + key[n-2:]
	default:
		return "sk-" + key[:4] + "**********" + key[n-4:]
	}
}

// computeUserTokenUsage 列出某用户的全部现存令牌(即使范围内零用量),叠加 [fromTs,toTs) 消费日志的按令牌聚合;
// 已删除但范围内有用量的令牌也保留一行(名称/key 尽量回查,查不到回退日志名)。
// 每行带累计总消耗(tokens.used_quota,创建至今终身值);生产库只读;key 只在服务端脱敏后返回,明文永不出库。
// 排序:现存令牌在前、已删除沉底,区内按范围费用降序。
func (m *Monitor) computeUserTokenUsage(ctx context.Context, uid, fromTs, toTs int64) ([]TokenUsage, error) {
	m.usageMu.Lock() // 与其它大聚合共用串行闸:同一时刻生产库最多跑一条聚合(调用方未持锁,不会重入)
	defer m.usageMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 令牌所属用户:logs.user_id 即令牌拥有者,故整批令牌都归 uid;解析其展示名(用户名→邮箱→#ID)
	owner := fmt.Sprintf("#%d", uid)
	var oname, oemail string
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COALESCE(username,''), COALESCE(email,'') FROM users WHERE id = ?", uid).Scan(&oname, &oemail); err == nil {
		if oname != "" {
			owner = oname
		} else if oemail != "" {
			owner = oemail
		}
	}

	type agg struct {
		logName                 string
		requests, tokens, quota int64
	}
	byTok := map[int64]*agg{}
	var ids []int64

	q := "SELECT token_id, COALESCE(MAX(token_name),''), COUNT(*)," +
		" CAST(COALESCE(SUM(prompt_tokens+completion_tokens),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(quota),0) AS SIGNED)" +
		" FROM logs WHERE type = 2 AND user_id = ? AND created_at >= ? AND created_at < ?" +
		" GROUP BY token_id"
	rows, err := m.prodDB.QueryContext(cctx, q, uid, fromTs, toTs)
	if err != nil {
		return nil, fmt.Errorf("按令牌聚合失败: %w", err)
	}
	for rows.Next() {
		var tid, reqs, toks, quota int64
		var name string
		if err := rows.Scan(&tid, &name, &reqs, &toks, &quota); err != nil {
			rows.Close()
			return nil, err
		}
		byTok[tid] = &agg{logName: name, requests: reqs, tokens: toks, quota: quota}
		if tid > 0 {
			ids = append(ids, tid)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// tokens 表:该用户全部现存令牌(零用量也要展示)+ 范围内出现过的已删令牌(软删,名称/key 仍可回查)。
	// used_quota = 令牌创建至今累计消耗;deleted_at 判定软删;key、group 是保留字,MySQL 需反引号。
	type tokInfo struct {
		name, mask, group string
		usedQuota         int64
		deleted           bool
	}
	infoByID := map[int64]*tokInfo{}
	kq := "SELECT id, COALESCE(name,''), COALESCE(`key`,''), COALESCE(`group`,''), CAST(COALESCE(used_quota,0) AS SIGNED), (deleted_at IS NOT NULL)" +
		" FROM tokens WHERE (user_id = ? AND deleted_at IS NULL)"
	kargs := []any{uid}
	if len(ids) > 0 {
		inSQL, inArgs := usageIn("id", ids)
		kq += " OR " + inSQL
		kargs = append(kargs, inArgs...)
	}
	krows, err := m.prodDB.QueryContext(cctx, kq, kargs...)
	if err != nil {
		return nil, fmt.Errorf("查询令牌信息失败: %w", err)
	}
	for krows.Next() {
		var id, used int64
		var name, key, group string
		var deleted bool
		if err := krows.Scan(&id, &name, &key, &group, &used, &deleted); err != nil {
			krows.Close()
			return nil, err
		}
		infoByID[id] = &tokInfo{name: name, mask: maskTokenKey(key), group: group, usedQuota: used, deleted: deleted}
	}
	krows.Close()
	if err := krows.Err(); err != nil {
		return nil, err
	}

	out := make([]TokenUsage, 0, len(infoByID)+1)
	// tokens 表里的每个令牌都出一行(现存令牌零用量补零;软删且范围内有用量的也在此列)
	for tid, info := range infoByID {
		a := byTok[tid]
		if a == nil {
			a = &agg{}
		}
		delete(byTok, tid)
		name := info.name
		if name == "" {
			name = a.logName
		}
		if name == "" {
			name = "(未命名)"
		}
		total := float64(info.usedQuota) / quotaPerUSD
		out = append(out, TokenUsage{
			TokenID:      tid,
			Owner:        owner,
			Name:         name,
			MaskedKey:    info.mask,
			Group:        info.group,
			Requests:     a.requests,
			Tokens:       a.tokens,
			CostUSD:      float64(a.quota) / quotaPerUSD,
			TotalCostUSD: &total,
			Deleted:      info.deleted,
		})
	}
	// 剩下的是 tokens 表查不到的:硬删令牌/老日志 token_id=0 → 回退日志名,key/分组/累计留空,归入已删除区
	for tid, a := range byTok {
		name := a.logName
		if name == "" {
			name = "(未命名)"
		}
		out = append(out, TokenUsage{
			TokenID:  tid,
			Owner:    owner,
			Name:     name,
			Requests: a.requests,
			Tokens:   a.tokens,
			CostUSD:  float64(a.quota) / quotaPerUSD,
			Deleted:  true,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Deleted != out[j].Deleted {
			return !out[i].Deleted // 现存令牌在前,已删除沉底(前端按此分区渲染)
		}
		if out[i].CostUSD != out[j].CostUSD {
			return out[i].CostUSD > out[j].CostUSD
		}
		ti, tj := 0.0, 0.0
		if out[i].TotalCostUSD != nil {
			ti = *out[i].TotalCostUSD
		}
		if out[j].TotalCostUSD != nil {
			tj = *out[j].TotalCostUSD
		}
		if ti != tj {
			return ti > tj
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// LogRow 一条日志(逐条明细,给客户端「使用日志」查看/导出用);只含元数据,不含请求/响应内容,也不含渠道等内部信息。
type LogRow struct {
	ID               int64   `json:"id"`
	CreatedAt        int64   `json:"created_at"`
	Member           string  `json:"member"` // 成员用户名(日志写入时记录)
	Type             int     `json:"type"`   // 1充值 2消费 3管理 4系统 5错误 6退款
	TokenName        string  `json:"token_name"`
	ModelName        string  `json:"model_name"`
	Group            string  `json:"group"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	UseTime          int64   `json:"use_time"`      // 总耗时(秒)
	IsStream         bool    `json:"is_stream"`     // 流式请求
	FirstByteMs      int64   `json:"first_byte_ms"` // 首字延迟(毫秒);仅流式且有值时>0
	CostUSD          float64 `json:"cost_usd"`      // 费用(美元);仅消费(type=2)有值,其它类型 quota 恒为0不代表费用,置0且前端/CSV留空
	Detail           string  `json:"detail"`        // 详情摘要(消费=计价摘要/退款=退款文案/其它=content)
}

// logTypeName 日志类型码 → 中文名(与 new-api LogType 常量一致)。
func logTypeName(t int) string {
	switch t {
	case 1:
		return "充值"
	case 2:
		return "消费"
	case 3:
		return "管理"
	case 4:
		return "系统"
	case 5:
		return "错误"
	case 6:
		return "退款"
	default:
		return "其它"
	}
}

// logOther 日志 other JSON 里【仅】我们要用的安全字段:首字延迟 + 计价摘要所需的价格/倍率。
// 渠道等内部字段(channel_id/channel_name/admin_info…)不在此结构 → 天然不解析、不外传。
type logOther struct {
	FRT                   float64  `json:"frt"`
	ModelPrice            *float64 `json:"model_price"`
	ModelRatio            *float64 `json:"model_ratio"`
	GroupRatio            *float64 `json:"group_ratio"`
	UserGroupRatio        *float64 `json:"user_group_ratio"`
	CacheTokens           float64  `json:"cache_tokens"`
	CacheRatio            *float64 `json:"cache_ratio"`
	CacheCreationTokens   float64  `json:"cache_creation_tokens"`
	CacheCreationRatio    *float64 `json:"cache_creation_ratio"`
	CacheCreationTokens5m float64  `json:"cache_creation_tokens_5m"`
	CacheCreationRatio5m  *float64 `json:"cache_creation_ratio_5m"`
	CacheCreationTokens1h float64  `json:"cache_creation_tokens_1h"`
	CacheCreationRatio1h  *float64 `json:"cache_creation_ratio_1h"`
	Image                 bool     `json:"image"`
	ImageRatio            *float64 `json:"image_ratio"`
	ViolationFeeCode      string   `json:"violation_fee_code"`
	BillingMode           string   `json:"billing_mode"` // "tiered_expr"=阶梯计费(此时 model_ratio/model_price 均为0,不能当标准单价展示)
}

func parseLogOther(s string) *logOther {
	if s == "" {
		return nil
	}
	var o logOther
	// 容错:单个字段类型漂移(如上游改版把 frt 发成字符串)时 Unmarshal 报错但已解析的字段仍有效,
	// 保留部分结果而不是整行降级;完全不是 JSON 时得到零值结构,行为与 nil 等价(详情回退 content)。
	_ = json.Unmarshal([]byte(s), &o)
	return &o
}

// fmtPriceUSD/trimNum:与 new-api(线上 classic 主题 formatCompactDisplayPrice)一致——美元符号+去尾零数字。
func fmtPriceUSD(v float64) string { return "$" + trimNum(v, 6) }
func trimNum(v float64, digits int) string {
	s := strconv.FormatFloat(v, 'f', digits, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// buildLogDetail 拼「详情」,逐行对齐 new-api 线上(classic 主题 renderPriceSimpleCore segments 模式):
// 消费=多行(首行 分组/专属倍率,再 输入价、缓存读、5m/1h/缓存创建、图片输入;【不含输出价】,与线上一致);
// 退款=固定文案;其余类型及无价格信息的消费=回退到原始 content。行以 \n 分隔,前端首行深色其余灰。
func buildLogDetail(logType int, o *logOther, content string) string {
	if logType == 6 {
		return "异步任务退款"
	}
	if logType != 2 || o == nil {
		return scrubContent(content) // 充值/管理/系统回退 content:先剔除内部信息(纵深防御)
	}
	if o.ViolationFeeCode != "" {
		return "违规费 " + o.ViolationFeeCode
	}
	// 阶梯计费:model_ratio/model_price 均为 0,按标准单价展示会显示"$0/1M"误导;
	// 我们不复刻线上阶梯专用渲染,回退 content(无则标注),避免展示错误单价。
	if o.BillingMode == "tiered_expr" {
		if content != "" {
			return content
		}
		if o.UserGroupRatio != nil && *o.UserGroupRatio > 0 {
			return "阶梯计费 · 专属倍率 " + trimNum(*o.UserGroupRatio, 6) + "x"
		}
		if o.GroupRatio != nil {
			return "阶梯计费 · 分组倍率 " + trimNum(*o.GroupRatio, 6) + "x"
		}
		return "阶梯计费"
	}
	var lines []string
	// 首行:专属倍率(user_group_ratio 有效时)优先,否则分组倍率
	if o.UserGroupRatio != nil && *o.UserGroupRatio > 0 {
		lines = append(lines, "专属倍率 "+trimNum(*o.UserGroupRatio, 6)+"x")
	} else if o.GroupRatio != nil {
		lines = append(lines, "分组倍率 "+trimNum(*o.GroupRatio, 6)+"x")
	}
	switch {
	case o.ModelPrice != nil && *o.ModelPrice > 0: // 按次计费
		lines = append(lines, "模型价格 "+fmtPriceUSD(*o.ModelPrice))
	case o.ModelRatio != nil: // 按量:倍率1 = $2/1M
		in := *o.ModelRatio * 2.0
		lines = append(lines, "输入 "+fmtPriceUSD(in)+" / 1M tokens")
		if o.CacheTokens != 0 && o.CacheRatio != nil {
			lines = append(lines, "缓存读 "+fmtPriceUSD(in**o.CacheRatio)+" / 1M tokens")
		}
		hasSplit := o.CacheCreationTokens5m > 0 || o.CacheCreationTokens1h > 0
		if hasSplit && o.CacheCreationTokens5m > 0 && o.CacheCreationRatio5m != nil {
			lines = append(lines, "5m缓存创建 "+fmtPriceUSD(in**o.CacheCreationRatio5m)+" / 1M tokens")
		}
		if hasSplit && o.CacheCreationTokens1h > 0 && o.CacheCreationRatio1h != nil {
			lines = append(lines, "1h缓存创建 "+fmtPriceUSD(in**o.CacheCreationRatio1h)+" / 1M tokens")
		}
		if !hasSplit && o.CacheCreationTokens != 0 && o.CacheCreationRatio != nil {
			lines = append(lines, "缓存创建 "+fmtPriceUSD(in**o.CacheCreationRatio)+" / 1M tokens")
		}
		if o.Image && o.ImageRatio != nil {
			lines = append(lines, "图片输入 "+fmtPriceUSD(in**o.ImageRatio)+" / 1M tokens")
		}
	}
	if len(lines) == 0 {
		return scrubContent(content) // 无价格信息的消费(老格式)→ 原始 content(同样先剔内部信息)
	}
	return strings.Join(lines, "\n")
}

// scrubContent 纵深防御:回退展示的 new-api 日志 content 里若含"渠道"字样(如系统日志
// "查看渠道密钥信息 (渠道ID: N)"),整条隐去——正常客户日志不会有这类内部文案,
// 唯一来源是误把管理员账号加进客户组;宁可少显也绝不把渠道信息漏给客户。
func scrubContent(content string) string {
	if strings.Contains(content, "渠道") {
		return ""
	}
	return content
}

// logFilterWhere 拼日志筛选的公共 WHERE(不含游标/排序/上限);全部用户可控值参数化,无注入。
// 查看(queryGroupLogs)与计数(countGroupLogs)共用,保证两者筛选口径完全一致。logType=0 表示全部类型。
func logFilterWhere(ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName string) (string, []any) {
	inSQL, inArgs := usageIn("user_id", ids)
	where := "created_at >= ? AND created_at < ? AND " + inSQL
	args := append([]any{fromTs, toTs}, inArgs...)
	if logType > 0 { // 仅看某类型(充值1/消费2/管理3/系统4;错误5/退款6 不对客户展示,由参数层挡)
		where += " AND type = ?"
		args = append(args, logType)
	} else { // 全部:排除错误(5)与退款(6),产品要求不在客户使用日志里展示
		where += " AND type NOT IN (5,6)"
	}
	if memberUID > 0 { // 仅看某成员
		where += " AND user_id = ?"
		args = append(args, memberUID)
	}
	if model != "" { // 仅看某模型(精确匹配,与聚合的 by_model key 一致)
		where += " AND model_name = ?"
		args = append(args, model)
	}
	if group != "" { // 仅看某分组(精确匹配,与聚合的 by_group key 一致)
		where += " AND `group` = ?"
		args = append(args, group)
	}
	if tokenName != "" { // 令牌名模糊匹配(参数化+通配符转义,防注入/防 %_ 泛匹配拖慢查询)
		where += " AND token_name LIKE ? ESCAPE '!'"
		args = append(args, "%"+escapeLike(tokenName)+"%")
	}
	return where, args
}

// escapeLike 转义 LIKE 模式里的通配符,使用户输入按字面匹配。
// ESCAPE 字符选 '!'(非反斜杠):反斜杠在 MySQL 与 sqlite 的字符串字面量语义不同,'!' 两边行为一致。
func escapeLike(s string) string {
	return strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(s)
}

// countGroupLogs 数一组成员在当前筛选下的日志总条数(供前端算总页数)。只在翻页首页调用一次,翻页时前端复用。
func (m *Monitor) countGroupLogs(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	where, args := logFilterWhere(ids, fromTs, toTs, memberUID, logType, model, group, tokenName)
	var n int64
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COUNT(*) FROM logs WHERE "+where, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("日志计数失败: %w", err)
	}
	return n, nil
}

// queryGroupLogs 查一组成员的日志,按 id 倒序游标分页;窗口化、走索引、只读、串行(usageMu)。
// 全部用户可控值参数化;memberUID 需调用方已校验属本组;limit 由调用方控上限(分页 pageSize+1 / 导出 cap,超限判定在导出侧用 COUNT 探测)。
// 取 content+other 拼「详情」与首字(only 安全字段);花费/首字/详情按 new-api 的可展示/计时类型口径填。
func (m *Monitor) queryGroupLogs(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName string, beforeID int64, limit int) ([]LogRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	m.usageMu.Lock() // 与其它大聚合共用串行闸:同一时刻生产库最多一条重查询
	defer m.usageMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 25*time.Second) // 导出可能取到 5 万行,给足超时
	defer cancel()

	where, args := logFilterWhere(ids, fromTs, toTs, memberUID, logType, model, group, tokenName)
	if beforeID > 0 { // 游标:取比上次末尾更早的(id 近似时间序,倒序翻页,不用深 OFFSET)
		where += " AND id < ?"
		args = append(args, beforeID)
	}
	q := "SELECT id, created_at, COALESCE(username,''), COALESCE(token_name,''), COALESCE(model_name,''), COALESCE(`group`,''), prompt_tokens, completion_tokens, use_time, quota, type, is_stream, COALESCE(content,''), COALESCE(other,'')" +
		" FROM logs WHERE " + where + " ORDER BY id DESC LIMIT " + strconv.Itoa(limit)
	rows, err := m.prodDB.QueryContext(cctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("日志查询失败: %w", err)
	}
	defer rows.Close()
	out := make([]LogRow, 0, limit)
	for rows.Next() {
		var r LogRow
		var quota int64
		var isStream int
		var content, other string
		if err := rows.Scan(&r.ID, &r.CreatedAt, &r.Member, &r.TokenName, &r.ModelName, &r.Group, &r.PromptTokens, &r.CompletionTokens, &r.UseTime, &quota, &r.Type, &isStream, &content, &other); err != nil {
			return nil, err
		}
		r.IsStream = isStream != 0
		o := parseLogOther(other)
		// 费用仅消费(type=2)有意义:充值/管理/系统在 new-api 里 quota 恒为 0(金额只写在 content),
		// 折美元会得 $0.00 误导客户对账,故非消费不给 CostUSD,前端/CSV 费用列留空。
		if r.Type == 2 {
			r.CostUSD = float64(quota) / quotaPerUSD
		}
		if r.IsStream && o != nil && o.FRT > 0 {
			r.FirstByteMs = int64(o.FRT)
		}
		r.Detail = buildLogDetail(r.Type, o, content)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// serveUsageStats GET /usage/stats?from=&to=&user_id=[&token_id=](管理员):对名单(或其中一人/其单个令牌)做每日/分组/模型聚合。
func (m *Monitor) serveUsageStats(c *gin.Context) {
	if !m.Enabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	tracked, err := m.listTracked()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	inList := map[int64]bool{}
	for _, u := range tracked {
		inList[u.UserID] = true
	}
	ids := idsOf(tracked)
	isGroup := false
	var tokenID int64
	// 可选其一:user_id=单用户详情;group_id=公司详情(聚合整组成员,0=未分组成员)
	if f := strings.TrimSpace(c.Query("user_id")); f != "" {
		id, err := strconv.ParseInt(f, 10, 64)
		if err != nil || !inList[id] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不在名单内"})
			return
		}
		ids = []int64{id}
		// 令牌详情:仅在单用户详情下有效;聚合强制 user_id+token_id 双条件,越权令牌只会查出空
		if t := strings.TrimSpace(c.Query("token_id")); t != "" {
			tokenID, err = strconv.ParseInt(t, 10, 64)
			if err != nil || tokenID <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "token_id 不合法"})
				return
			}
		}
	} else if g := strings.TrimSpace(c.Query("group_id")); g != "" {
		gid, err := strconv.ParseInt(g, 10, 64)
		if err != nil || gid < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "group_id 不合法"})
			return
		}
		isGroup = true
		ids = ids[:0]
		for _, u := range tracked {
			if u.GroupID == gid {
				ids = append(ids, u.UserID)
			}
		}
	}
	if len(ids) == 0 { // 名单为空:不查生产库,直接空结果
		c.JSON(http.StatusOK, gin.H{"enabled": true, "stats": &UsageStats{}, "empty": true})
		return
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	st, err := m.computeUsageStats(c.Request.Context(), ids, fromTs, toTs, tokenID)
	if err != nil {
		slog.Warn("用户用量详情聚合失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "统计查询失败,请稍后重试(细节见服务端日志)"})
		return
	}
	resp := gin.H{"enabled": true, "stats": st}
	if isGroup { // 公司详情:成员数 + 余额合计 + 累计总消耗合计(主键 IN 的 SUM)
		resp["members"] = len(ids)
		resp["balance_usd"] = m.sumBalanceUSD(c.Request.Context(), ids)
		resp["total_used_usd"] = m.sumUsedUSD(c.Request.Context(), ids)
	} else if tokenID > 0 { // 令牌详情:元数据(名称/脱敏key/分组/累计;硬删查不到则为 null,前端用点击时的名字兜底)
		resp["token"] = m.tokenMetaOf(c.Request.Context(), ids[0], tokenID)
	} else if len(ids) == 1 { // 单用户详情:个人余额 + 累计总消耗(实时取,null=已删/取不到)+ 各令牌用量
		resp["balance_usd"] = m.userBalanceUSD(c.Request.Context(), ids[0])
		resp["total_used_usd"] = m.userUsedUSD(c.Request.Context(), ids[0])
		if toks, err := m.computeUserTokenUsage(c.Request.Context(), ids[0], fromTs, toTs); err != nil {
			slog.Warn("单用户令牌用量聚合失败", "err", err, "user_id", ids[0])
		} else {
			resp["by_token"] = toks
		}
	}
	c.JSON(http.StatusOK, resp)
}

// sumBalanceUSD 一组用户的主站余额合计(users.quota 求和折美元);空组/出错返回 nil。
func (m *Monitor) sumBalanceUSD(ctx context.Context, ids []int64) *float64 {
	if len(ids) == 0 {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	inSQL, args := usageIn("id", ids)
	var quota int64
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COALESCE(SUM(quota),0) FROM users WHERE "+inSQL, args...).Scan(&quota); err != nil {
		slog.Warn("查询分组余额合计失败", "err", err)
		return nil
	}
	b := float64(quota) / quotaPerUSD
	return &b
}

// sumUsedUSD 一组用户的主站累计总消耗合计(users.used_quota 求和折美元);空组/出错返回 nil。
func (m *Monitor) sumUsedUSD(ctx context.Context, ids []int64) *float64 {
	if len(ids) == 0 {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	inSQL, args := usageIn("id", ids)
	var used int64
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COALESCE(SUM(used_quota),0) FROM users WHERE "+inSQL, args...).Scan(&used); err != nil {
		slog.Warn("查询分组累计消耗合计失败", "err", err)
		return nil
	}
	u := float64(used) / quotaPerUSD
	return &u
}
