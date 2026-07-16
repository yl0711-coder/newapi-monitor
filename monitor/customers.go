package monitor

// customers.go:「客户管理」域——被盯用户名单 + 客户分组(公司)+ 跟进阈值,全存监控本地 sqlite,
// 与主站无关(只在解析/刷新用户时对生产库 users 表做主键级只读点查)。
// 从 usage.go 按域拆出:这里管"盯谁、怎么分组";usage.go 管"生产库用量聚合与日志查询"。

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const maxTrackedUsers = 500 // 名单上限,防误加成全量扫描

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

// groupNameMap 分组 id→name(本地库,量小全取)。
func (m *Monitor) groupNameMap() map[int64]string {
	var gs []CustomerGroup
	warnReadErr("groupNameMap", m.storeDB.Find(&gs))
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
		ID   int64  `json:"id"`
		Name string `json:"name"`
		Note string `json:"note"`
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
