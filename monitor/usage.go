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
// Portal* = 客户端(独立域名报表页)登录账号:一组一账号,双密码并存——
// PortalPwAdmin(我方配置,永久有效)/ PortalPwUser(客户自改,可选),登录任一匹配即可;都只存 bcrypt 哈希。
type CustomerGroup struct {
	ID            int64  `gorm:"primaryKey" json:"id"`
	Name          string `gorm:"uniqueIndex;size:64" json:"name"`
	Note          string `gorm:"size:500" json:"note"`
	Stage         string `gorm:"size:16;default:active" json:"stage"` // trial=试用 / active=正式 / churned=已流失
	TrialEnd      int64  `json:"trial_end"`                           // 试用到期(unix 秒,0=无);仅 trial 有意义
	PortalEmail   string `gorm:"size:128;index" json:"portal_email"`  // 客户端登录邮箱;空=未开通(跨组唯一由 handler 校验)
	PortalPwAdmin string `gorm:"size:128" json:"-"`                   // 我方配置密码 bcrypt;不回显
	PortalPwUser  string `gorm:"size:128" json:"-"`                   // 客户自改密码 bcrypt;不回显
	CreatedAt     int64  `json:"created_at"`
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
		Members         int64 `json:"members"`
		PortalSet       bool  `json:"portal_set"`         // 已开通客户端账号
		PortalUserPwSet bool  `json:"portal_user_pw_set"` // 客户自改过密码
	}
	out := make([]row, 0, len(gs))
	for _, g := range gs {
		var n int64
		m.storeDB.Model(&TrackedUser{}).Where("group_id = ?", g.ID).Count(&n)
		out = append(out, row{CustomerGroup: g, Members: n, PortalSet: g.PortalEmail != "", PortalUserPwSet: g.PortalPwUser != ""})
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
func (m *Monitor) refreshTrackedLabels(ctx context.Context, tracked []TrackedUser) ([]TrackedUser, map[int64]float64, map[int64]float64) {
	balances := map[int64]float64{}
	used := map[int64]float64{}
	if len(tracked) == 0 {
		return tracked, balances, used
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	inSQL, args := usageIn("id", idsOf(tracked))
	// used_quota 与 quota 同表同行,SELECT 多取一列即得累计总消耗,无额外往返/扫描
	rows, err := m.prodDB.QueryContext(cctx, "SELECT id, COALESCE(username,''), COALESCE(email,''), COALESCE(quota,0), COALESCE(used_quota,0) FROM users WHERE "+inSQL, args...)
	if err != nil {
		slog.Warn("刷新检测用户标签失败,沿用快照", "err", err)
		return tracked, balances, used
	}
	defer rows.Close()
	fresh := map[int64]TrackedUser{}
	for rows.Next() {
		var u TrackedUser
		var quota, usedQ int64
		if err := rows.Scan(&u.UserID, &u.Username, &u.Email, &quota, &usedQ); err != nil {
			slog.Warn("刷新检测用户标签失败,沿用快照", "err", err)
			return tracked, map[int64]float64{}, map[int64]float64{}
		}
		fresh[u.UserID] = u
		balances[u.UserID] = float64(quota) / quotaPerUSD
		used[u.UserID] = float64(usedQ) / quotaPerUSD
	}
	if err := rows.Err(); err != nil {
		slog.Warn("刷新检测用户标签失败,沿用快照", "err", err)
		return tracked, map[int64]float64{}, map[int64]float64{}
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
	return tracked, balances, used
}

func idsOf(tracked []TrackedUser) []int64 {
	ids := make([]int64, 0, len(tracked))
	for _, u := range tracked {
		ids = append(ids, u.UserID)
	}
	return ids
}

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
	Owner     string  `json:"owner"` // 令牌所属用户(展示名:用户名/邮箱/#ID)
	Name      string  `json:"name"`
	MaskedKey string  `json:"masked_key"`
	Group     string  `json:"group"` // 令牌绑定的分组(计价档);空=跟随用户默认分组/已删
	Requests  int64   `json:"requests"`
	Tokens    int64   `json:"tokens"`
	CostUSD   float64 `json:"cost_usd"`
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

// computeUserTokenUsage 按令牌聚合某用户在 [fromTs,toTs) 的消费日志,并联 tokens 表取名称 + 脱敏 key。
// 生产库只读;key 只在服务端脱敏后返回,明文永不出库。按费用降序。
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

	// tokens 表取当前名称 + 脱敏 key + 分组(令牌可能已删除,取不到则回退日志里的名字、key/分组留空)
	nameByID := map[int64]string{}
	maskByID := map[int64]string{}
	groupByID := map[int64]string{}
	if len(ids) > 0 {
		inSQL, inArgs := usageIn("id", ids)
		// key、group 都是保留字,MySQL 需反引号
		kq := "SELECT id, COALESCE(name,''), COALESCE(`key`,''), COALESCE(`group`,'') FROM tokens WHERE " + inSQL
		krows, err := m.prodDB.QueryContext(cctx, kq, inArgs...)
		if err != nil {
			return nil, fmt.Errorf("查询令牌信息失败: %w", err)
		}
		for krows.Next() {
			var id int64
			var name, key, group string
			if err := krows.Scan(&id, &name, &key, &group); err != nil {
				krows.Close()
				return nil, err
			}
			nameByID[id] = name
			maskByID[id] = maskTokenKey(key)
			groupByID[id] = group
		}
		krows.Close()
		if err := krows.Err(); err != nil {
			return nil, err
		}
	}

	out := make([]TokenUsage, 0, len(byTok))
	for tid, a := range byTok {
		name := nameByID[tid]
		if name == "" {
			name = a.logName // 令牌已删/无 token_id → 用日志里的名字
		}
		if name == "" {
			name = "(未命名)"
		}
		out = append(out, TokenUsage{
			Owner:     owner,
			Name:      name,
			MaskedKey: maskByID[tid], // 已删或老日志(token_id=0)→ 空,前端显示"—"
			Group:     groupByID[tid],
			Requests:  a.requests,
			Tokens:    a.tokens,
			CostUSD:   float64(a.quota) / quotaPerUSD,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CostUSD != out[j].CostUSD {
			return out[i].CostUSD > out[j].CostUSD
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
	if isGroup { // 公司详情:成员数 + 余额合计 + 累计总消耗合计(主键 IN 的 SUM)
		resp["members"] = len(ids)
		resp["balance_usd"] = m.sumBalanceUSD(c.Request.Context(), ids)
		resp["total_used_usd"] = m.sumUsedUSD(c.Request.Context(), ids)
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
