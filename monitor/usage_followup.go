package monitor

// usage_followup.go:「客户跟进」——判断【落到人】(逐个用户按消费状态判是否需跟进),
// 但列表【按公司(分组)归拢】:某公司有人需跟进就列出该公司 + 需跟进人数,展开看具体是谁、为什么;
// 跟进记录 / 看消费都是针对那个人。未分组的需跟进用户归到「未分组」桶。
//
// 边界同前:只对生产库多一条固定 30 天窗口的按需聚合(复用 computeUsageMatrix,走索引、串行闸),
// 逐日消费 + 余额 + 公司状态(试用/正式)+ 阈值 在 Go 里算,全本地存储、不给生产库加常驻负担。

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
)

// FollowUpMember 一个需跟进的用户(人)。
type FollowUpMember struct {
	UserID       int64    `json:"user_id"`
	Username     string   `json:"username"`
	Email        string   `json:"email"`
	Spend30USD   float64  `json:"spend_30d_usd"`
	BalanceUSD   *float64 `json:"balance_usd"`
	DaysIdle     int      `json:"days_idle"`
	LastActive   string   `json:"last_active"`
	Reasons      []string `json:"reasons"`
	Actions      []string `json:"actions"`
	Urgency      int      `json:"urgency"`
	Level        string   `json:"level"`          // 分级:urgent=紧急(30天内有消费的客户出状况) / info=长期沉默(30天全无消费)
	LastFollowUp int64    `json:"last_follow_up"` // 该用户上次跟进时间
}

// FollowUpCompany 按公司(分组)归拢:含公司级提示 + 需跟进成员。
type FollowUpCompany struct {
	GroupID        int64            `json:"group_id"` // 0 = 未分组
	GroupName      string           `json:"group_name"`
	Stage          string           `json:"stage"`
	CompanyReasons []string         `json:"company_reasons"` // 公司级(试用到期 / 空分组)
	Members        []FollowUpMember `json:"members"`         // 需跟进的成员(人)
	Spend30USD     float64          `json:"spend_30d_usd"`   // 公司近30天合计(展示)
	Urgency        int              `json:"urgency"`
}

const ungroupedName = "未分组"

// computeFollowUps 逐用户判是否需跟进,再按公司归拢。
func (m *Monitor) computeFollowUps(ctx context.Context, nowUnix int64) ([]FollowUpCompany, error) {
	tracked, err := m.listTracked()
	if err != nil {
		return nil, err
	}
	var groups []CustomerGroup
	if err := m.storeDB.Order("id").Find(&groups).Error; err != nil {
		return nil, err
	}
	groupByID := map[int64]CustomerGroup{}
	for _, g := range groups {
		groupByID[g.ID] = g
	}
	if len(tracked) == 0 && len(groups) == 0 {
		return []FollowUpCompany{}, nil
	}

	allIDs := idsOf(tracked)
	tracked, balances, _ := m.refreshTrackedLabels(ctx, tracked) // 跟进判断不用累计总消耗,忽略第三返回

	// 固定 30 天窗口(不受页面显示范围影响),按 CST 切日
	toTs := followUpDayStart(nowUnix+usageTZOffsetSec) + 86400
	fromTs := toTs - int64(followUpWindowDays)*86400
	mx := &UsageMatrix{}
	if len(allIDs) > 0 {
		if mx, err = m.computeUsageMatrix(ctx, allIDs, fromTs, toTs); err != nil {
			return nil, err
		}
	}
	// 逐(用户,日序号)消费;dayIdx 0=最早 … 29=今天
	dateOfIdx := make([]string, followUpWindowDays)
	idxOfDate := map[string]int{}
	for i := 0; i < followUpWindowDays; i++ {
		dateOfIdx[i] = time.Unix(fromTs+int64(i)*86400, 0).In(usageCST).Format("2006-01-02")
		idxOfDate[dateOfIdx[i]] = i
	}
	spendByUserDay := map[int64]map[int]float64{}
	for _, c := range mx.Cells {
		di, ok := idxOfDate[c.Date]
		if !ok {
			continue
		}
		if spendByUserDay[c.UserID] == nil {
			spendByUserDay[c.UserID] = map[int]float64{}
		}
		spendByUserDay[c.UserID][di] += c.CostUSD
	}

	st := m.loadUsageSettings()
	lastFollow := m.lastFollowUpByUser()
	todayIdx := followUpWindowDays - 1

	type bucket struct {
		comp    *FollowUpCompany
		members []FollowUpMember
	}
	buckets := map[int64]*bucket{}
	getBucket := func(gid int64) *bucket {
		if b, ok := buckets[gid]; ok {
			return b
		}
		name := ungroupedName
		stage := "active"
		if g, ok := groupByID[gid]; ok {
			name = g.Name
			if g.Stage != "" {
				stage = g.Stage
			}
		}
		b := &bucket{comp: &FollowUpCompany{GroupID: gid, GroupName: name, Stage: stage}}
		buckets[gid] = b
		return b
	}

	// 逐用户判定
	for _, u := range tracked {
		stage := "active"
		if g, ok := groupByID[u.GroupID]; ok && g.Stage != "" {
			stage = g.Stage
		}
		if stage == "churned" { // 已流失公司:不再提醒其成员
			continue
		}
		days := spendByUserDay[u.UserID]
		var spend30, last7, prev7 float64
		lastActiveIdx := -1
		for di := 0; di < followUpWindowDays; di++ {
			v := days[di]
			spend30 += v
			if v > 0 {
				lastActiveIdx = di
			}
			if di > todayIdx-7 {
				last7 += v
			} else if di > todayIdx-14 {
				prev7 += v
			}
		}
		mem := FollowUpMember{UserID: u.UserID, Username: u.Username, Email: u.Email, Spend30USD: spend30, LastFollowUp: lastFollow[u.UserID]}
		if b, ok := balances[u.UserID]; ok {
			bv := b
			mem.BalanceUSD = &bv
		}
		if lastActiveIdx >= 0 {
			mem.LastActive = dateOfIdx[lastActiveIdx]
			mem.DaysIdle = todayIdx - lastActiveIdx
		} else {
			mem.DaysIdle = followUpWindowDays
		}
		add := func(reason, action string, urgency int) {
			mem.Reasons = append(mem.Reasons, reason)
			mem.Actions = append(mem.Actions, action)
			mem.Urgency += urgency
		}
		if stage == "trial" {
			if last7 < st.TrialLowUSD {
				add(fmt.Sprintf("试用消耗低(近7天 %s)", fmtUSD2(last7)), "沟通:是不是用不起来/有问题", 30)
			}
			if last7 >= st.TrialHighUSD {
				add(fmt.Sprintf("试用消耗高(近7天 %s)", fmtUSD2(last7)), "转化时机:确认付费/谈转正", 60)
			}
		} else {
			if mem.DaysIdle >= st.DormantDays {
				add(fmt.Sprintf("连续 %d 天无消费", mem.DaysIdle), "疑似流失:去沟通问原因", 50)
			} else if prev7 > 0 && last7 < prev7*(1-float64(st.DropPct)/100) {
				drop := int((1 - last7/prev7) * 100)
				add(fmt.Sprintf("消费下滑(近7天降 %d%%)", drop), "关注、了解原因", 35)
			}
			if mem.BalanceUSD != nil && *mem.BalanceUSD < st.LowBalanceUSD {
				add(fmt.Sprintf("余额低(%s)", fmtUSD2(*mem.BalanceUSD)), "催充值,避免断服流失", 45)
			}
		}
		if len(mem.Reasons) > 0 {
			mem.Urgency += int(spend30)
			// 分级:30天内有过消费的客户出状况=紧急(该马上催);30天全无消费=长期沉默(低优先级,页面折叠、不进红徽章)
			if spend30 > 0 {
				mem.Level = "urgent"
			} else {
				mem.Level = "info"
			}
			b := getBucket(u.GroupID)
			b.members = append(b.members, mem)
			b.comp.Spend30USD += spend30
		}
	}

	// 公司级信号:试用到期临近 / 空分组
	for _, g := range groups {
		if g.Stage == "churned" {
			continue
		}
		var count int64
		m.storeDB.Model(&TrackedUser{}).Where("group_id = ?", g.ID).Count(&count)
		if count == 0 {
			getBucket(g.ID).comp.CompanyReasons = append(getBucket(g.ID).comp.CompanyReasons, "分组内没有用户(把该公司的用户加进来)")
		}
		if g.Stage == "trial" && g.TrialEnd > 0 {
			daysLeft := int((g.TrialEnd - nowUnix) / 86400)
			if daysLeft <= st.TrialExpiryDays {
				b := getBucket(g.ID)
				if daysLeft < 0 {
					b.comp.CompanyReasons = append(b.comp.CompanyReasons, "试用已到期(尽快转正或收回额度)")
				} else {
					b.comp.CompanyReasons = append(b.comp.CompanyReasons, fmt.Sprintf("试用还剩 %d 天到期", daysLeft))
				}
			}
		}
	}

	var out []FollowUpCompany
	for _, b := range buckets {
		if len(b.members) == 0 && len(b.comp.CompanyReasons) == 0 {
			continue
		}
		sort.SliceStable(b.members, func(i, j int) bool { return b.members[i].Urgency > b.members[j].Urgency })
		b.comp.Members = b.members
		b.comp.Urgency = int(b.comp.Spend30USD) + len(b.comp.CompanyReasons)*20
		for _, mm := range b.members {
			b.comp.Urgency += mm.Urgency
		}
		out = append(out, *b.comp)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Urgency > out[j].Urgency })
	return out, nil
}

func fmtUSD2(v float64) string {
	return "$" + strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), ".")
}

// followUpDayStart 把(已 +8h 的)秒对齐到当日 0 点(仍是 +8h 语义),配合 -usageTZOffsetSec 还原。
func followUpDayStart(shifted int64) int64 { return shifted/86400*86400 - usageTZOffsetSec }

// lastFollowUpByUser 各用户最近一次跟进时间。
func (m *Monitor) lastFollowUpByUser() map[int64]int64 {
	type row struct {
		UserID int64
		Last   int64
	}
	var rows []row
	m.storeDB.Model(&FollowUpLog{}).Select("user_id, MAX(created_at) AS last").Group("user_id").Scan(&rows)
	out := map[int64]int64{}
	for _, r := range rows {
		out[r.UserID] = r.Last
	}
	return out
}

// ---- HTTP ----

// serveFollowUps GET /usage/followups(管理员):待跟进(按公司归拢的需跟进成员)。
func (m *Monitor) serveFollowUps(c *gin.Context) {
	if !m.Enabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	items, err := m.computeFollowUps(c.Request.Context(), time.Now().Unix())
	if err != nil {
		slog.Warn("待跟进计算失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "待跟进查询失败,请稍后重试"})
		return
	}
	total := 0
	for _, co := range items {
		total += len(co.Members)
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "companies": items, "member_total": total, "window_days": followUpWindowDays})
}

// listFollowLogs GET /usage/followups/log?user_id=(管理员):某用户的跟进记录(新→旧)。
func (m *Monitor) listFollowLogs(c *gin.Context) {
	uid, _ := strconv.ParseInt(strings.TrimSpace(c.Query("user_id")), 10, 64)
	if uid <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	var logs []FollowUpLog
	m.storeDB.Where("user_id = ?", uid).Order("created_at DESC").Limit(100).Find(&logs)
	c.JSON(http.StatusOK, gin.H{"logs": logs})
}

// addFollowLog POST /usage/followups/log(仅超管):{user_id, text}。
func (m *Monitor) addFollowLog(c *gin.Context) {
	var in struct {
		UserID int64  `json:"user_id"`
		Text   string `json:"text"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	text := strings.TrimSpace(in.Text)
	if text == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "跟进内容不能为空"})
		return
	}
	if len(text) > 500 {
		text = text[:500]
	}
	name, _, _ := m.currentUser(c)
	lg := FollowUpLog{UserID: in.UserID, Text: text, Author: clip(name, 64), CreatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&lg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "log": lg})
}

// setGroupStage POST /usage/groups/stage(仅超管):{id, stage, trial_end}。
func (m *Monitor) setGroupStage(c *gin.Context) {
	var in struct {
		ID       int64  `json:"id"`
		Stage    string `json:"stage"`
		TrialEnd int64  `json:"trial_end"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	if in.Stage != "trial" && in.Stage != "active" && in.Stage != "churned" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "stage 非法"})
		return
	}
	upd := map[string]any{"stage": in.Stage, "trial_end": in.TrialEnd}
	if in.Stage != "trial" {
		upd["trial_end"] = 0
	}
	res := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).Updates(upd)
	if res.Error != nil || res.RowsAffected == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "分组不存在"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// getUsageSettings GET /usage/settings(管理员)。
func (m *Monitor) getUsageSettings(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"settings": m.loadUsageSettings()})
}

// saveUsageSettings POST /usage/settings(仅超管):阈值,单行 upsert。
func (m *Monitor) saveUsageSettings(c *gin.Context) {
	var in UsageSettings
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	in.ID = 1
	if err := m.storeDB.Save(&in).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "settings": m.loadUsageSettings()})
}
