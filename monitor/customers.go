package monitor

// customers.go:「客户管理」域——被盯用户名单 + 客户分组(公司)+ 跟进阈值,全存监控本地 sqlite,
// 与主站无关(只在解析/刷新用户时对生产库 users 表做主键级只读点查)。
// 从 usage.go 按域拆出:这里管"盯谁、怎么分组";usage.go 管"生产库用量聚合与日志查询"。

import (
	"context"
	"errors"
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
	ID   int64  `gorm:"primaryKey" json:"id"`
	Name string `gorm:"uniqueIndex;size:64" json:"name"`
	Note string `gorm:"size:500" json:"note"`
	// Stage/TrialEnd 是旧版客户状态功能留下的兼容列。状态功能已从 Monitor 退出，
	// 但保留数据库列可避免 SQLite 破坏性迁移；接口不再返回、写入或据此改变业务判断。
	Stage         string `gorm:"size:16;default:active" json:"-"`
	TrialEnd      int64  `json:"-"`
	PortalEmail   string `gorm:"size:128;index" json:"portal_email"` // 客户端登录邮箱;空=未开通(跨组唯一由 handler 校验)
	PortalPwAdmin string `gorm:"size:128" json:"-"`                  // 我方配置密码 bcrypt;不回显
	PortalPwUser  string `gorm:"size:128" json:"-"`                  // 客户自改密码 bcrypt;不回显
	PortalAuthVer int64  `gorm:"not null;default:0" json:"-"`        // 登录凭证版本;账号/密码变化时递增,立即废止旧会话
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
	ID            int64   `gorm:"primaryKey" json:"-"`
	DormantDays   int     `json:"dormant_days"`    // 连续无消费达此天数→疑似流失
	DropPct       int     `json:"drop_pct"`        // 近7天 vs 前7天 消费降幅≥此%→消费下滑
	LowBalanceUSD float64 `json:"low_balance_usd"` // 余额低于此→催充值
	// 以下三列仅为旧数据库兼容保留，状态功能退出后不再参与判断或通过接口暴露。
	TrialLowUSD     float64 `json:"-"`
	TrialHighUSD    float64 `json:"-"`
	TrialExpiryDays int     `json:"-"`
}

func defaultUsageSettings() UsageSettings {
	return UsageSettings{ID: 1, DormantDays: 7, DropPct: 50, LowBalanceUSD: 5}
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

// ---- 客户分组 CRUD(name 唯一;只允许删除无成员且 Portal 未启用的误建公司) ----

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

// createGroup POST /usage/groups(仅超管):{name, note}。
func (m *Monitor) createGroup(c *gin.Context) {
	var in struct {
		Name string `json:"name"`
		Note string `json:"note"`
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
	var count int64
	if err := m.storeDB.Model(&CustomerGroup{}).Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取分组失败,请重试"})
		return
	}
	if count >= maxCustomerGroups {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("分组已达上限 %d 个", maxCustomerGroups)})
		return
	}
	// 兼容列固定写入 active/0；新版业务不再读取客户状态。
	g := CustomerGroup{Name: name, Note: note, Stage: "active", TrialEnd: 0, CreatedAt: time.Now().Unix()}
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

// deleteGroup POST /usage/groups/delete(仅超管):只删除无成员且 Portal 已停用的误建公司。
func (m *Monitor) deleteGroup(c *gin.Context) {
	var in struct {
		ID        int64  `json:"id"`
		RequestID string `json:"request_id"`
		Reason    string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	meta, err := usageMemberMutationMetaFromGin(c, in.RequestID, in.Reason)
	if err == nil {
		_, err = m.removeCustomerGroup(c.Request.Context(), in.ID, meta)
	}
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errCustomerGroupHasMembers) || errors.Is(err, errCustomerGroupPortalEnabled) ||
			errors.Is(err, errUsageMemberRequestConflict) {
			status = http.StatusConflict
		} else if errors.Is(err, gorm.ErrRecordNotFound) || strings.Contains(err.Error(), "幂等键") {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
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

// setUserGroup POST /usage/users/group(仅超管):显式纠正用户当前所属公司。
// 它不修改 tracked revision，也不搬迁或重算 facts。
func (m *Monitor) setUserGroup(c *gin.Context) {
	var in struct {
		UserID    int64  `json:"user_id"`
		GroupID   int64  `json:"group_id"`
		RequestID string `json:"request_id"`
		Reason    string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 || in.GroupID < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id/group_id required"})
		return
	}
	meta, err := usageMemberMutationMetaFromGin(c, in.RequestID, in.Reason)
	var result usageMemberMutationResult
	if err == nil {
		result, err = m.correctUsageMemberCompany(c.Request.Context(), in.UserID, in.GroupID, meta)
	}
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errUsageMemberRequestConflict) {
			status = http.StatusConflict
		} else if errors.Is(err, errUsageMemberControlIntegrity) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "action": result.Action, "tracked_revision": result.TrackedRevision, "replayed": result.Replayed})
}

// addTrackedUser POST /usage/users(仅超管):{input: 邮箱或用户ID} → 解析主站用户后入名单。
func (m *Monitor) addTrackedUser(c *gin.Context) {
	if !m.Enabled() { // 与 matrix/stats 同一守卫:无生产库连接时干净拒绝,而非 nil 解引用
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接主站数据库,无法解析用户"})
		return
	}
	var in struct {
		Input     string `json:"input"`
		GroupID   int64  `json:"group_id"` // 可选:添加同时归入公司;0=未分组
		RequestID string `json:"request_id"`
		Reason    string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	u, err := m.resolveNewAPIUser(c.Request.Context(), in.Input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	meta, err := usageMemberMutationMetaFromGin(c, in.RequestID, in.Reason)
	var result usageMemberMutationResult
	if err == nil {
		result, err = m.addUsageMember(c.Request.Context(), *u, in.GroupID, meta)
	}
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, errUsageMemberDifferentCompany), errors.Is(err, errUsageMemberRequestConflict):
			status = http.StatusConflict
		case errors.Is(err, errUsageMemberControlIntegrity):
			status = http.StatusServiceUnavailable
		case strings.Contains(err.Error(), "不存在"), strings.Contains(err.Error(), "上限"), strings.Contains(err.Error(), "幂等键"):
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "user": result.User, "action": result.Action,
		"active": result.Active, "tracked_revision": result.TrackedRevision, "replayed": result.Replayed})
}

// deleteTrackedUser POST /usage/users/delete(仅超管):{user_id} → 移出名单(不动主站)。
func (m *Monitor) deleteTrackedUser(c *gin.Context) {
	var in struct {
		UserID    int64  `json:"user_id"`
		RequestID string `json:"request_id"`
		Reason    string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	meta, err := usageMemberMutationMetaFromGin(c, in.RequestID, in.Reason)
	var result usageMemberMutationResult
	if err == nil {
		result, err = m.removeUsageMember(c.Request.Context(), in.UserID, meta)
	}
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errUsageMemberNotActive) || strings.Contains(err.Error(), "幂等键") {
			status = http.StatusBadRequest
		} else if errors.Is(err, errUsageMemberRequestConflict) {
			status = http.StatusConflict
		} else if errors.Is(err, errUsageMemberControlIntegrity) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	// 不撤销已发布事实读取。页面立即与当前 TrackedUser 取交集，
	// 已删成员不会再显示；后台会在下一轮低频任务中发布新名单版本。
	c.JSON(http.StatusOK, gin.H{"ok": true, "action": result.Action, "active": result.Active,
		"tracked_revision": result.TrackedRevision, "replayed": result.Replayed})
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

// refreshTrackedLabelsFromLocalSnapshot 仅在来源 users 表暂时不可读时使用 Monitor
// 本地已有的资料快照兜底。它不触发来源库查询、不写主站；没有快照的成员仍按本地名单
// 展示，余额和累计消耗保持空值，避免把缺失资料伪装为实时值。
func (m *Monitor) refreshTrackedLabelsFromLocalSnapshot(ctx context.Context, tracked []TrackedUser) ([]TrackedUser, map[int64]int64, map[int64]int64, bool) {
	balances, used := map[int64]int64{}, map[int64]int64{}
	if m.storeDB == nil || len(tracked) == 0 {
		return tracked, balances, used, false
	}
	var snapshots []UsageUserSnapshot
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if err := m.storeDB.WithContext(qctx).Where("user_id IN ?", idsOf(tracked)).Find(&snapshots).Error; err != nil {
		return tracked, balances, used, false
	}
	byID := make(map[int64]UsageUserSnapshot, len(snapshots))
	for _, snap := range snapshots {
		if snap.Exists {
			byID[snap.UserID] = snap
		}
	}
	if len(byID) == 0 {
		return tracked, balances, used, false
	}
	for i := range tracked {
		snap, ok := byID[tracked[i].UserID]
		if !ok {
			continue
		}
		if snap.Username != "" {
			tracked[i].Username = snap.Username
		}
		if snap.Email != "" {
			tracked[i].Email = snap.Email
		}
		balances[snap.UserID] = snap.BalanceQuota
		used[snap.UserID] = snap.UsedQuota
	}
	return tracked, balances, used, true
}

// refreshTrackedLabels 按 id 去生产库 users 表把名单的 username/email 刷新成当前值(主键 IN 查询,代价可忽略),
// 并顺路取回各用户【当前余额】与【累计消耗】的原始整数 quota(实时值不落库)。
// 金额只在响应边界折美元，避免汇总、比较阶段引入浮点误差。主站已删的用户不在结果表 → 前端显示 —。
// 名单存的是添加时的快照——主站改邮箱/账号易主后,矩阵会把今天的消费记在旧身份上;
// 这里每次查询顺手校准,变化的顺手回写本地库(自愈缓存);来源失败时才读取本地已有资料快照，
// 不会让一条 users 点查错误拖垮用量页面，也不会用短时缓存覆盖正常情况下的实时余额。
func (m *Monitor) refreshTrackedLabels(ctx context.Context, tracked []TrackedUser) ([]TrackedUser, map[int64]int64, map[int64]int64) {
	balances := map[int64]int64{}
	used := map[int64]int64{}
	if len(tracked) == 0 {
		return tracked, balances, used
	}
	fallback := func(err error) ([]TrackedUser, map[int64]int64, map[int64]int64) {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return tracked, balances, used
		}
		if localTracked, localBalances, localUsed, ok := m.refreshTrackedLabelsFromLocalSnapshot(ctx, tracked); ok {
			slog.Warn("刷新检测用户标签失败,使用本地资料快照", "err", err)
			return localTracked, localBalances, localUsed
		}
		slog.Warn("刷新检测用户标签失败,沿用本地名单", "err", err)
		return tracked, balances, used
	}
	if m.prodDB == nil {
		return fallback(errors.New("生产库未连接"))
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	inSQL, args := usageIn("id", idsOf(tracked))
	// used_quota 与 quota 同表同行,SELECT 多取一列即得累计总消耗,无额外往返或扫描。
	rows, err := m.prodDB.QueryContext(cctx, "SELECT id, COALESCE(username,''), COALESCE(email,''), COALESCE(quota,0), COALESCE(used_quota,0) FROM users WHERE "+inSQL, args...)
	if err != nil {
		return fallback(err)
	}
	defer rows.Close()
	fresh := map[int64]TrackedUser{}
	for rows.Next() {
		var u TrackedUser
		var quota, usedQ int64
		if err := rows.Scan(&u.UserID, &u.Username, &u.Email, &quota, &usedQ); err != nil {
			return fallback(err)
		}
		fresh[u.UserID] = u
		balances[u.UserID] = quota
		used[u.UserID] = usedQ
	}
	if err := rows.Err(); err != nil {
		return fallback(err)
	}
	for i, u := range tracked {
		f, ok := fresh[u.UserID]
		if !ok {
			continue // 主站已删的用户:保留快照当历史名。
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
