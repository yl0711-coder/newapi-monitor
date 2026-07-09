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
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	// usageTZOffsetSec 天粒度按东八区(CST)切日,与团队运营时区一致(日志本身是 unix 秒,无时区)。
	usageTZOffsetSec = 8 * 3600
	maxTrackedUsers  = 500 // 名单上限,防误加成全量扫描
	maxUsageDays     = 90  // 单次查询时间范围上限,约束单次扫描量(产品要求:最多90天)
	maxUsageDimRows  = 300 // 分组/模型维度返回上限
)

var usageCST = time.FixedZone("CST", usageTZOffsetSec)

// usageDayExprMySQL 把 created_at(unix 秒)折算成 CST 日序号(自 epoch 起第几天)。
// MySQL 整除用 DIV;测试里(sqlite)用 usageDayExpr 字段覆盖为 '/'(sqlite 整型相除即整除)。
const usageDayExprMySQL = "(created_at + 28800) DIV 86400"

// TrackedUser 被盯的 new-api 用户(名单存本地 sqlite,主键=new-api user_id,天然去重)。
// GroupID 归属的客户分组(customer_groups.id),0=未分组——分组是监控本地的客户管理元数据,与主站无关。
type TrackedUser struct {
	UserID   int64  `gorm:"primaryKey;column:user_id" json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	GroupID  int64  `json:"group_id"`
	Note     string `gorm:"size:200" json:"note"` // 备注:记用户状态/联系人等,监控本地元数据
	AddedAt  int64  `json:"added_at"`
}

// CustomerGroup 客户分组(公司):监控本地的客户管理实体,name 唯一。
type CustomerGroup struct {
	ID        int64  `gorm:"primaryKey" json:"id"`
	Name      string `gorm:"uniqueIndex;size:64" json:"name"`
	Note      string `gorm:"size:500" json:"note"`
	Stage     string `gorm:"size:16;default:active" json:"stage"` // trial=试用 / active=正式 / churned=已流失
	TrialEnd  int64  `json:"trial_end"`                           // 试用到期(unix 秒,0=无);仅 trial 有意义
	CreatedAt int64  `json:"created_at"`
}

// FollowUpLog 跟进记录(时间线,追加式);跟进落到【人】,按 user_id 归档。
type FollowUpLog struct {
	ID        int64  `gorm:"primaryKey" json:"id"`
	UserID    int64  `gorm:"index" json:"user_id"`
	Text      string `gorm:"size:500" json:"text"`
	Author    string `gorm:"size:64" json:"author"`
	CreatedAt int64  `json:"created_at"`
}

// UsageSettings 客户跟进阈值(单行,id=1;缺省用 defaultUsageSettings)。
type UsageSettings struct {
	ID              int64   `gorm:"primaryKey" json:"-"`
	DormantDays     int     `json:"dormant_days"`      // 正式客户连续无消费达此天数→疑似流失
	DropPct         int     `json:"drop_pct"`          // 近7天 vs 前7天 消费降幅≥此%→消费下滑
	LowBalanceUSD   float64 `json:"low_balance_usd"`   // 余额低于此→催充值
	TrialLowUSD     float64 `json:"trial_low_usd"`     // 试用近7天消费低于此→用不起来
	TrialHighUSD    float64 `json:"trial_high_usd"`    // 试用近7天消费高于此→转化时机
	TrialExpiryDays int     `json:"trial_expiry_days"` // 试用到期剩余≤此天→到期跟进
}

func defaultUsageSettings() UsageSettings {
	return UsageSettings{ID: 1, DormantDays: 7, DropPct: 50, LowBalanceUSD: 5, TrialLowUSD: 1, TrialHighUSD: 20, TrialExpiryDays: 7}
}

// loadUsageSettings 读阈值;无记录/字段为0则用默认补齐(防老库/半配)。
func (m *Monitor) loadUsageSettings() UsageSettings {
	var s UsageSettings
	if err := m.storeDB.First(&s, 1).Error; err != nil {
		return defaultUsageSettings()
	}
	d := defaultUsageSettings()
	if s.DormantDays <= 0 {
		s.DormantDays = d.DormantDays
	}
	if s.DropPct <= 0 {
		s.DropPct = d.DropPct
	}
	if s.LowBalanceUSD <= 0 {
		s.LowBalanceUSD = d.LowBalanceUSD
	}
	if s.TrialLowUSD <= 0 {
		s.TrialLowUSD = d.TrialLowUSD
	}
	if s.TrialHighUSD <= 0 {
		s.TrialHighUSD = d.TrialHighUSD
	}
	if s.TrialExpiryDays <= 0 {
		s.TrialExpiryDays = d.TrialExpiryDays
	}
	return s
}

const followUpWindowDays = 30 // 跟进判断固定回看窗口(独立于页面显示范围)

const maxCustomerGroups = 200 // 分组数量护栏

// ---- 名单 CRUD(本地库) ----

func (m *Monitor) listTracked() ([]TrackedUser, error) {
	var rows []TrackedUser
	err := m.storeDB.Order("added_at").Find(&rows).Error
	return rows, err
}

// resolveNewAPIUser 去生产库 users 表把输入解析成用户:一条等值查询同时匹配 ID/用户名/邮箱
// (username 在 new-api 是唯一索引,和邮箱一样可靠)。多命中时数字输入按 ID 优先消歧,仍撞则报错让用 ID。
func (m *Monitor) resolveNewAPIUser(ctx context.Context, input string) (*TrackedUser, error) {
	in := strings.TrimSpace(input)
	if in == "" {
		return nil, fmt.Errorf("请输入用户ID、用户名或邮箱")
	}
	var asID int64
	if id, e := strconv.ParseInt(in, 10, 64); e == nil && id > 0 {
		asID = id
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := m.prodDB.QueryContext(cctx,
		"SELECT id, COALESCE(username,''), COALESCE(email,'') FROM users WHERE id = ? OR username = ? OR email = ? LIMIT 3",
		asID, in, in)
	if err != nil {
		// 驱动错误可能含内网 DB 地址/schema 细节:细节进日志,给浏览器的信息脱敏
		slog.Warn("查询主站用户失败", "err", err)
		return nil, fmt.Errorf("查询主站用户失败,请稍后重试(细节见服务端日志)")
	}
	defer rows.Close()
	var found []TrackedUser
	for rows.Next() {
		var u TrackedUser
		if err := rows.Scan(&u.UserID, &u.Username, &u.Email); err != nil {
			return nil, err
		}
		found = append(found, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	switch {
	case len(found) == 0:
		return nil, fmt.Errorf("主站没有找到该用户(%s)", input)
	case len(found) == 1:
		return &found[0], nil
	}
	// 多命中:纯数字输入优先当 ID(如用户名恰叫 "123" 与 ID=123 撞车)
	if asID > 0 {
		for i := range found {
			if found[i].UserID == asID {
				return &found[i], nil
			}
		}
	}
	return nil, fmt.Errorf("该输入匹配到多个用户(用户名/邮箱撞车),请改用用户ID添加")
}

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

// UsageDim 某维度取值(分组 / 模型 / 用户)的合计。
type UsageDim struct {
	Key      string  `json:"key"`
	Requests int64   `json:"requests"`
	Tokens   int64   `json:"tokens"`
	CostUSD  float64 `json:"cost_usd"`
}

// UsageStats 一次用户用量查询的完整结果(详情页专用:单用户的每日/分组/模型)。
type UsageStats struct {
	From    string       `json:"from"`
	To      string       `json:"to"`
	Summary UsageDim     `json:"summary"`
	Daily   []UsageDaily `json:"daily"`
	ByGroup []UsageDim   `json:"by_group"`
	ByModel []UsageDim   `json:"by_model"`
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
// 串行化(usageMu):同一时刻最多一条聚合在生产库上跑,叠加连接池上限双保险。
func (m *Monitor) computeUsageStats(ctx context.Context, ids []int64, fromTs, toTs int64) (*UsageStats, error) {
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
	UserID     int64    `json:"user_id"`
	Username   string   `json:"username"`
	Email      string   `json:"email"`
	GroupID    int64    `json:"group_id"`
	GroupName  string   `json:"group_name"`
	Note       string   `json:"note"`
	TotalUSD   float64  `json:"total_usd"`
	BalanceUSD *float64 `json:"balance_usd"` // 主站当前余额(users.quota 折美元);null=主站已删/取不到
}

// UsageMatrixCell 稀疏格:某用户某天的消费(无消费的天不出格)。
type UsageMatrixCell struct {
	UserID   int64   `json:"user_id"`
	Date     string  `json:"date"`
	Requests int64   `json:"requests"`
	CostUSD  float64 `json:"cost_usd"`
}

// UsageMatrix 列表页数据:days 连续日期(新→旧)+ 用户(按区间消费降序)+ 稀疏格。
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
	// 含两端点共 N 天 ⇔ 零点差 (N-1)*24h;用 >= 卡在恰好 190 天(> 会放行 191 天)
	if to.Sub(from) >= time.Duration(maxUsageDays)*24*time.Hour {
		return 0, 0, fmt.Errorf("时间范围过大,最长 %d 天", maxUsageDays)
	}
	return from.Unix(), to.AddDate(0, 0, 1).Unix(), nil // to 含当天 → 上界取次日 0 点(开区间)
}

// ---- HTTP 处理器 ----

// groupNameMap 分组 id→name(本地库,量小全取)。
func (m *Monitor) groupNameMap() map[int64]string {
	var gs []CustomerGroup
	m.storeDB.Find(&gs)
	out := map[int64]string{}
	for _, g := range gs {
		out[g.ID] = g.Name
	}
	return out
}

// trackedUserView 名单项+冗余分组名(前端免二次拼)。
type trackedUserView struct {
	TrackedUser
	GroupName string `json:"group_name"`
}

func (m *Monitor) trackedViews(rows []TrackedUser) []trackedUserView {
	gm := m.groupNameMap()
	out := make([]trackedUserView, 0, len(rows))
	for _, u := range rows {
		out = append(out, trackedUserView{TrackedUser: u, GroupName: gm[u.GroupID]})
	}
	return out
}

// listTrackedUsers GET /usage/users(管理员):返回名单(含分组名)。
func (m *Monitor) listTrackedUsers(c *gin.Context) {
	rows, err := m.listTracked()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"users": m.trackedViews(rows)})
}

// ---- 客户分组 CRUD(name 唯一;删除=解散,成员回未分组) ----

// listGroups GET /usage/groups(管理员):分组列表+人数。
func (m *Monitor) listGroups(c *gin.Context) {
	var gs []CustomerGroup
	if err := m.storeDB.Order("created_at").Find(&gs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	type row struct {
		CustomerGroup
		Members int64 `json:"members"`
	}
	out := make([]row, 0, len(gs))
	for _, g := range gs {
		var n int64
		m.storeDB.Model(&TrackedUser{}).Where("group_id = ?", g.ID).Count(&n)
		out = append(out, row{CustomerGroup: g, Members: n})
	}
	c.JSON(http.StatusOK, gin.H{"groups": out})
}

// normalizeGroupInput 清洗分组输入:名称必填≤64,备注≤500。
func normalizeGroupInput(name, note string) (string, string, error) {
	name = strings.TrimSpace(name)
	note = strings.TrimSpace(note)
	if name == "" {
		return "", "", fmt.Errorf("分组名称不能为空")
	}
	if len(name) > 64 {
		return "", "", fmt.Errorf("分组名称过长(≤64字节)")
	}
	if len(note) > 500 {
		note = note[:500]
	}
	return name, note, nil
}

// createGroup POST /usage/groups(仅超管):{name, note, stage?, trial_end?};stage 缺省 active。
func (m *Monitor) createGroup(c *gin.Context) {
	var in struct {
		Name     string `json:"name"`
		Note     string `json:"note"`
		Stage    string `json:"stage"`
		TrialEnd int64  `json:"trial_end"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	name, note, err := normalizeGroupInput(in.Name, in.Note)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	stage := in.Stage
	if stage != "trial" && stage != "active" && stage != "churned" {
		stage = "active"
	}
	trialEnd := in.TrialEnd
	if stage != "trial" {
		trialEnd = 0
	}
	var count int64
	if err := m.storeDB.Model(&CustomerGroup{}).Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取分组失败,请重试"})
		return
	}
	if count >= maxCustomerGroups {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("分组已达上限 %d 个", maxCustomerGroups)})
		return
	}
	g := CustomerGroup{Name: name, Note: note, Stage: stage, TrialEnd: trialEnd, CreatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&g).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "创建失败:分组名可能已存在"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "group": g})
}

// updateGroup POST /usage/groups/update(仅超管):{id, name, note}。
func (m *Monitor) updateGroup(c *gin.Context) {
	var in struct {
		ID   int64
		Name string
		Note string
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	name, note, err := normalizeGroupInput(in.Name, in.Note)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).Updates(map[string]any{"name": name, "note": note})
	if res.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "保存失败:分组名可能已存在"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "分组不存在"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// deleteGroup POST /usage/groups/delete(仅超管):解散——成员回未分组,不删用户。
func (m *Monitor) deleteGroup(c *gin.Context) {
	var in struct {
		ID int64 `json:"id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&TrackedUser{}).Where("group_id = ?", in.ID).Update("group_id", 0).Error; err != nil {
			return err
		}
		return tx.Delete(&CustomerGroup{}, in.ID).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// setUserNote POST /usage/users/note(仅超管):{user_id, note};清空 note 传空串。
func (m *Monitor) setUserNote(c *gin.Context) {
	var in struct {
		UserID int64  `json:"user_id"`
		Note   string `json:"note"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	note := strings.TrimSpace(in.Note)
	if len(note) > 200 {
		note = note[:200]
	}
	res := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", in.UserID).Update("note", note)
	if res.Error != nil || res.RowsAffected == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "用户不在名单内"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// setUserGroup POST /usage/users/group(仅超管):{user_id, group_id};group_id=0 为移出分组。
func (m *Monitor) setUserGroup(c *gin.Context) {
	var in struct {
		UserID  int64 `json:"user_id"`
		GroupID int64 `json:"group_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 || in.GroupID < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id/group_id required"})
		return
	}
	if in.GroupID > 0 {
		var n int64
		m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.GroupID).Count(&n)
		if n == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "分组不存在"})
			return
		}
	}
	res := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", in.UserID).Update("group_id", in.GroupID)
	if res.Error != nil || res.RowsAffected == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "用户不在名单内"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// addTrackedUser POST /usage/users(仅超管):{input: 邮箱或用户ID} → 解析主站用户后入名单。
func (m *Monitor) addTrackedUser(c *gin.Context) {
	if !m.Enabled() { // 与 matrix/stats 同一守卫:无生产库连接时干净拒绝,而非 nil 解引用
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接主站数据库,无法解析用户"})
		return
	}
	var in struct {
		Input   string `json:"input"`
		GroupID int64  `json:"group_id"` // 可选:添加同时归入分组;0=未分组
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if in.GroupID > 0 {
		var n int64
		m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.GroupID).Count(&n)
		if n == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "所选分组不存在"})
			return
		}
	}
	// 名单上限是约束生产库 IN 扫描宽度的护栏:计数出错必须拒绝,不能当 0 放行
	var count int64
	if err := m.storeDB.Model(&TrackedUser{}).Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取名单失败,请重试"})
		return
	}
	if count >= maxTrackedUsers {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("名单已达上限 %d 个", maxTrackedUsers)})
		return
	}
	u, err := m.resolveNewAPIUser(c.Request.Context(), in.Input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u.AddedAt = time.Now().Unix()
	u.GroupID = in.GroupID
	if err := m.storeDB.Save(u).Error; err != nil { // 主键=user_id,重复添加=幂等更新(含改组)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "user": u})
}

// deleteTrackedUser POST /usage/users/delete(仅超管):{user_id} → 移出名单(不动主站)。
func (m *Monitor) deleteTrackedUser(c *gin.Context) {
	var in struct {
		UserID int64 `json:"user_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	if err := m.storeDB.Delete(&TrackedUser{}, "user_id = ?", in.UserID).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// trackedLabel 展示名:用户名优先(需求:显示用户名),缺则邮箱,再缺回退 #id。
func trackedLabel(u TrackedUser) string {
	if u.Username != "" {
		return u.Username
	}
	if u.Email != "" {
		return u.Email
	}
	return "#" + strconv.FormatInt(u.UserID, 10)
}

// refreshTrackedLabels 按 id 去生产库 users 表把名单的 username/email 刷新成当前值(主键 IN 查询,代价可忽略),
// 并顺路取回各用户【当前余额】(users.quota 折美元;实时值不落库)。主站已删的用户不在余额表 → 前端显示 —。
// 名单存的是添加时的快照——主站改邮箱/账号易主后,矩阵会把今天的消费记在旧身份上;
// 这里每次查询顺手校准,变化的顺手回写本地库(自愈缓存);失败则退回快照+空余额,绝不阻断统计。
func (m *Monitor) refreshTrackedLabels(ctx context.Context, tracked []TrackedUser) ([]TrackedUser, map[int64]float64) {
	balances := map[int64]float64{}
	if len(tracked) == 0 {
		return tracked, balances
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	inSQL, args := usageIn("id", idsOf(tracked))
	rows, err := m.prodDB.QueryContext(cctx, "SELECT id, COALESCE(username,''), COALESCE(email,''), COALESCE(quota,0) FROM users WHERE "+inSQL, args...)
	if err != nil {
		slog.Warn("刷新检测用户标签失败,沿用快照", "err", err)
		return tracked, balances
	}
	defer rows.Close()
	fresh := map[int64]TrackedUser{}
	for rows.Next() {
		var u TrackedUser
		var quota int64
		if err := rows.Scan(&u.UserID, &u.Username, &u.Email, &quota); err != nil {
			slog.Warn("刷新检测用户标签失败,沿用快照", "err", err)
			return tracked, map[int64]float64{}
		}
		fresh[u.UserID] = u
		balances[u.UserID] = float64(quota) / quotaPerUSD
	}
	if err := rows.Err(); err != nil {
		slog.Warn("刷新检测用户标签失败,沿用快照", "err", err)
		return tracked, map[int64]float64{}
	}
	for i, u := range tracked {
		f, ok := fresh[u.UserID]
		if !ok {
			continue // 主站已删的用户:保留快照当历史名
		}
		if f.Username != u.Username || f.Email != u.Email {
			tracked[i].Username, tracked[i].Email = f.Username, f.Email
			upd := tracked[i]
			if err := m.storeDB.Save(&upd).Error; err != nil {
				slog.Warn("回写检测用户标签失败", "err", err, "user_id", u.UserID)
			}
		}
	}
	return tracked, balances
}

func idsOf(tracked []TrackedUser) []int64 {
	ids := make([]int64, 0, len(tracked))
	for _, u := range tracked {
		ids = append(ids, u.UserID)
	}
	return ids
}

// serveUsageMatrix GET /usage/matrix?from=&to=(管理员):列表页矩阵数据(前端渲染为 行=用户 × 列=日期)。
// 用户 label 取邮箱(缺则用户名/#id)并按区间消费降序;零消费用户排最后仍保留。
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
	tracked, balances := m.refreshTrackedLabels(c.Request.Context(), tracked) // 身份标签校准到主站当前值 + 取当前余额
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
		mx.Users = append(mx.Users, mu)
	}
	sort.SliceStable(mx.Users, func(i, j int) bool { return mx.Users[i].TotalUSD > mx.Users[j].TotalUSD })
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

// serveUsageStats GET /usage/stats?from=&to=&user_id=(管理员):对名单(或其中一人)做每日/分组/模型聚合。
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
	// 可选其一:user_id=单用户详情;group_id=公司详情(聚合整组成员,0=未分组成员)
	if f := strings.TrimSpace(c.Query("user_id")); f != "" {
		id, err := strconv.ParseInt(f, 10, 64)
		if err != nil || !inList[id] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不在名单内"})
			return
		}
		ids = []int64{id}
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
	st, err := m.computeUsageStats(c.Request.Context(), ids, fromTs, toTs)
	if err != nil {
		slog.Warn("用户用量详情聚合失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "统计查询失败,请稍后重试(细节见服务端日志)"})
		return
	}
	resp := gin.H{"enabled": true, "stats": st}
	if isGroup { // 公司详情:成员数 + 余额合计(一条 SUM,主键 IN)
		resp["members"] = len(ids)
		resp["balance_usd"] = m.sumBalanceUSD(c.Request.Context(), ids)
	} else if len(ids) == 1 { // 单用户详情:个人余额(实时取,null=已删/取不到)
		resp["balance_usd"] = m.userBalanceUSD(c.Request.Context(), ids[0])
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
