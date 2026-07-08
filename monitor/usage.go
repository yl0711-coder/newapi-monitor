package monitor

// usage.go:「用户用量」——盯一组指定的 new-api 用户(按邮箱 / 用户ID 添加),
// 按需对生产库 logs 表做窗口化聚合:每日总量、按分组、按模型、按用户,以及费用(quota/500000 美元)。
//
// 与采样器的边界:采样器是【常驻周期】查询,这里是【按需】查询——只在打开页面 / 点查询时执行,
// 全部限定时间范围并命中索引(idx_logs_user_id / idx_created_at_type / idx_logs_group / idx_logs_model_name),
// 且同一时刻只放行一条聚合(usageMu 串行化),不给生产库常驻负担。生产库全程只读。
// 名单存本地 sqlite(tracked_users);鉴权沿用全站约定:管理员可看,仅超管可改名单。

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// usageTZOffsetSec 天粒度按东八区(CST)切日,与团队运营时区一致(日志本身是 unix 秒,无时区)。
	usageTZOffsetSec = 8 * 3600
	maxTrackedUsers  = 500 // 名单上限,防误加成全量扫描
	maxUsageDays     = 190 // 单次查询时间范围上限(约6个月),约束单次扫描量
	maxUsageDimRows  = 300 // 分组/模型维度返回上限
)

var usageCST = time.FixedZone("CST", usageTZOffsetSec)

// usageDayExprMySQL 把 created_at(unix 秒)折算成 CST 日序号(自 epoch 起第几天)。
// MySQL 整除用 DIV;测试里(sqlite)用 usageDayExpr 字段覆盖为 '/'(sqlite 整型相除即整除)。
const usageDayExprMySQL = "(created_at + 28800) DIV 86400"

// TrackedUser 被盯的 new-api 用户(名单存本地 sqlite,主键=new-api user_id,天然去重)。
type TrackedUser struct {
	UserID   int64  `gorm:"primaryKey;column:user_id" json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	AddedAt  int64  `json:"added_at"`
}

// ---- 名单 CRUD(本地库) ----

func (m *Monitor) listTracked() ([]TrackedUser, error) {
	var rows []TrackedUser
	err := m.storeDB.Order("added_at").Find(&rows).Error
	return rows, err
}

// classifyUserInput 判定输入是 user_id(纯数字)还是邮箱(含@);两者都不是则报错。
func classifyUserInput(input string) (userID int64, email string, err error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return 0, "", fmt.Errorf("请输入邮箱或用户ID")
	}
	if id, e := strconv.ParseInt(s, 10, 64); e == nil && id > 0 {
		return id, "", nil
	}
	if strings.Contains(s, "@") {
		return 0, s, nil
	}
	return 0, "", fmt.Errorf("无法识别:请输入纯数字用户ID,或含 @ 的完整邮箱")
}

// resolveNewAPIUser 去生产库 users 表把输入解析成用户(只读、走主键/邮箱等值查询)。
func (m *Monitor) resolveNewAPIUser(ctx context.Context, input string) (*TrackedUser, error) {
	id, email, err := classifyUserInput(input)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var rows *sql.Rows
	if id > 0 {
		rows, err = m.prodDB.QueryContext(cctx, "SELECT id, COALESCE(username,''), COALESCE(email,'') FROM users WHERE id = ?", id)
	} else {
		rows, err = m.prodDB.QueryContext(cctx, "SELECT id, COALESCE(username,''), COALESCE(email,'') FROM users WHERE email = ? LIMIT 2", email)
	}
	if err != nil {
		return nil, fmt.Errorf("查询主站用户失败: %w", err)
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
	case len(found) > 1:
		return nil, fmt.Errorf("该邮箱匹配到多个用户,请改用用户ID添加")
	}
	return &found[0], nil
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

// UsageStats 一次用户用量查询的完整结果。
type UsageStats struct {
	From    string       `json:"from"`
	To      string       `json:"to"`
	Summary UsageDim     `json:"summary"`
	Daily   []UsageDaily `json:"daily"`
	ByGroup []UsageDim   `json:"by_group"`
	ByModel []UsageDim   `json:"by_model"`
	ByUser  []UsageDim   `json:"by_user"`
}

// usageIn 生成 "user_id IN (?,?,…)" 片段与参数(ids 已由调用方保证非空)。
func usageIn(ids []int64) (string, []any) {
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args[i] = id
	}
	return "user_id IN (" + strings.Join(ph, ",") + ")", args
}

// computeUsageStats 对 [fromTs, toTs) 内、指定用户集合的消费日志(type=2)做四路聚合。
// 串行化(usageMu):同一时刻最多一条聚合在生产库上跑,叠加连接池上限(2)双保险。
func (m *Monitor) computeUsageStats(ctx context.Context, ids []int64, fromTs, toTs int64) (*UsageStats, error) {
	if len(ids) == 0 {
		return &UsageStats{}, nil // 名单为空不该走到这;防御:不拼 "IN ()" 非法 SQL
	}
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	inSQL, inArgs := usageIn(ids)
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

	// 2/3/4) 按分组 / 模型 / 用户。列名 group 是保留字,必须反引号。
	dims := []struct {
		col  string
		dst  *[]UsageDim
		desc string
	}{
		{"COALESCE(`group`,'')", &st.ByGroup, "按分组"},
		{"COALESCE(model_name,'')", &st.ByModel, "按模型"},
		{"user_id", &st.ByUser, "按用户"},
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
	if to.Sub(from) > time.Duration(maxUsageDays)*24*time.Hour {
		return 0, 0, fmt.Errorf("时间范围过大,最长 %d 天", maxUsageDays)
	}
	return from.Unix(), to.AddDate(0, 0, 1).Unix(), nil // to 含当天 → 上界取次日 0 点(开区间)
}

// ---- HTTP 处理器 ----

// listTrackedUsers GET /usage/users(管理员):返回名单。
func (m *Monitor) listTrackedUsers(c *gin.Context) {
	rows, err := m.listTracked()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"users": rows})
}

// addTrackedUser POST /usage/users(仅超管):{input: 邮箱或用户ID} → 解析主站用户后入名单。
func (m *Monitor) addTrackedUser(c *gin.Context) {
	var in struct {
		Input string `json:"input"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var count int64
	m.storeDB.Model(&TrackedUser{}).Count(&count)
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
	if err := m.storeDB.Save(u).Error; err != nil { // 主键=user_id,重复添加=幂等更新
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

// serveUsageStats GET /usage/stats?from=&to=&user_id=(管理员):对名单(或其中一人)做四路聚合。
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
	label := map[int64]string{}
	ids := make([]int64, 0, len(tracked))
	for _, u := range tracked {
		name := u.Username
		if name == "" {
			name = u.Email
		}
		if name == "" {
			name = "#" + strconv.FormatInt(u.UserID, 10)
		}
		label[u.UserID] = name
		ids = append(ids, u.UserID)
	}
	// 可选:只看名单内某一个用户
	if f := strings.TrimSpace(c.Query("user_id")); f != "" {
		id, err := strconv.ParseInt(f, 10, 64)
		if err != nil || label[id] == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不在名单内"})
			return
		}
		ids = []int64{id}
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	for i := range st.ByUser { // user_id → 可读名(用户名/邮箱)
		if id, e := strconv.ParseInt(st.ByUser[i].Key, 10, 64); e == nil && label[id] != "" {
			st.ByUser[i].Key = label[id] + " (#" + st.ByUser[i].Key + ")"
		}
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "stats": st})
}
