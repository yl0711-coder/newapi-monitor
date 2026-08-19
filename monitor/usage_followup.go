package monitor

// usage_followup.go:「客户跟进」——判断【落到人】(逐个用户按消费状态判是否需跟进),
// 但列表【按公司(分组)归拢】:某公司有人需跟进就列出该公司 + 需跟进人数,展开看具体是谁、为什么;
// 跟进记录 / 看消费都是针对那个人。未分组的需跟进用户归到「未分组」桶。
//
// 事实读启用后复用本地 30 天消费矩阵与资料快照，不访问生产 users/logs/tokens；
// 只有完整 30 天已发布时才执行阈值判断，覆盖不足则显式返回“暂不可用”，避免把缺失日当成零消费。

import (
	"context"
	"fmt"
	"log/slog"
	"math"
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
	CompanyReasons []string         `json:"company_reasons"` // 公司级(当前仅空分组)
	Members        []FollowUpMember `json:"members"`         // 需跟进的成员(人)
	Spend30USD     float64          `json:"spend_30d_usd"`   // 公司近30天合计(展示)
	Urgency        int              `json:"urgency"`
}

// followUpComputation 把“业务上真的没有待跟进”与“30 天事实尚未
// 完整发布、暂时不能判断”分开。后者不得用缺失日的默认零值生成误导性结论。
type followUpComputation struct {
	Companies     []FollowUpCompany
	Available     bool
	RangePartial  bool
	Message       string
	RequestedFrom string
	RequestedTo   string
	CoverageFrom  string
	CoverageTo    string
	RequestedDays int
	AvailableDays int
}

const ungroupedName = "未分组"

// classifyMember 单个用户的跟进判定(纯函数,便于单测):由按日消费算出 30 天合计/近7天/前7天/静默天数,
// 对所有关注客户使用同一套客观阈值,产出 reasons/actions/urgency/level。
// days 键为日序号(0=最早 … followUpWindowDays-1=最近闭合日)。eligibleFromIdx
// 是该成员来源 floor 之后的首个完整自然日；注册前的零不能被当作
// “本人无消费”。返回 (成员, 是否需要跟进)。
func classifyMember(u TrackedUser, days map[int]float64, balance *float64, lastFollowUp int64, dateOfIdx []string, eligibleFromIdx int, st UsageSettings) (FollowUpMember, bool) {
	todayIdx := followUpWindowDays - 1
	eligibleFromIdx = max(0, min(eligibleFromIdx, followUpWindowDays))
	eligibleDays := followUpWindowDays - eligibleFromIdx
	var spend30, last7, prev7 float64
	lastActiveIdx := -1
	for di := eligibleFromIdx; di < followUpWindowDays; di++ {
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
	mem := FollowUpMember{UserID: u.UserID, Username: u.Username, Email: u.Email, Spend30USD: spend30, BalanceUSD: balance, LastFollowUp: lastFollowUp}
	if lastActiveIdx >= 0 {
		mem.LastActive = dateOfIdx[lastActiveIdx]
		mem.DaysIdle = todayIdx - lastActiveIdx
	} else {
		mem.DaysIdle = eligibleDays
	}
	add := func(reason, action string, urgency int) {
		mem.Reasons = append(mem.Reasons, reason)
		mem.Actions = append(mem.Actions, action)
		mem.Urgency += urgency
	}
	if eligibleDays >= st.DormantDays && mem.DaysIdle >= st.DormantDays {
		add(fmt.Sprintf("连续 %d 天无消费", mem.DaysIdle), "疑似流失:去沟通问原因", 50)
	} else if eligibleDays >= 14 && prev7 > 0 && last7 < prev7*(1-float64(st.DropPct)/100) {
		drop := int((1 - last7/prev7) * 100)
		add(fmt.Sprintf("消费下滑(近7天降 %d%%)", drop), "关注、了解原因", 35)
	}
	if mem.BalanceUSD != nil && *mem.BalanceUSD < st.LowBalanceUSD {
		add(fmt.Sprintf("余额低(%s)", fmtUSD2(*mem.BalanceUSD)), "催充值,避免断服流失", 45)
	}
	if len(mem.Reasons) == 0 {
		return mem, false
	}
	mem.Urgency += int(spend30)
	// 分级:30天内有过消费的客户出状况=紧急(该马上催);30天全无消费=长期沉默(低优先级,页面折叠、不进红徽章)
	if spend30 > 0 {
		mem.Level = "urgent"
	} else {
		mem.Level = "info"
	}
	return mem, true
}

// computeFollowUpsWithCoverage 只在完整 30 天窗口可读时执行跟进判定。
// 跟进规则同时依赖“30 天无消费”和“近 7 天 / 前 7 天”，因此不能像
// 普通矩阵那样展示一个左侧被裁剪的窗口；否则未发布日会被误当成零消费。
func (m *Monitor) computeFollowUpsWithCoverage(ctx context.Context, nowUnix int64) (followUpComputation, error) {
	// Follow-up rules use the latest 30 *complete* CST calendar days.  The
	// current day cannot be part of the decision because the facts watermark is
	// intentionally behind wall clock and will never reach tomorrow 00:00 while
	// this request is running.  Including today therefore made the feature
	// permanently partial in production and could also classify late-arriving
	// traffic as zero consumption.
	toTs := followUpDayStart(nowUnix + usageTZOffsetSec)
	fromTs := toTs - int64(followUpWindowDays)*usageFactDaySeconds
	requestedRange := newUsageMatrixRange(fromTs, toTs)
	var selectedIDs []int64
	if m.usageFactsReadRequested() {
		tracked, memberCoverage, err := m.listTrackedForUsageReadCoverage(ctx)
		if err != nil {
			return followUpComputation{}, err
		}
		if !memberCoverage.Complete {
			return followUpComputation{
				Companies: []FollowUpCompany{}, RequestedFrom: requestedRange.From, RequestedTo: requestedRange.To,
				RequestedDays: followUpWindowDays,
				Message: fmt.Sprintf("待跟进判断需要完整成员集合；当前已签收 %d/%d 个成员，补全后将自动恢复",
					memberCoverage.Published, memberCoverage.Active),
			}, nil
		}
		selectedIDs = idsOf(tracked)
	}
	readRange, err := m.resolveUsageAggregateReadRangeForMembers(ctx, fromTs, toTs, selectedIDs)
	if err != nil {
		return followUpComputation{}, err
	}
	result := followUpComputation{
		Companies:     []FollowUpCompany{},
		RequestedFrom: requestedRange.From,
		RequestedTo:   requestedRange.To,
		RequestedDays: followUpWindowDays,
		RangePartial:  readRange.Partial,
	}
	if readRange.Available {
		coverage := newUsageMatrixRange(readRange.From, readRange.To)
		result.CoverageFrom = coverage.From
		result.CoverageTo = coverage.To
		result.AvailableDays = int((readRange.To - readRange.From) / usageFactDaySeconds)
	}

	// 旧的直读来源模式没有发布左界；一旦运维已要求 facts 切读，则只有
	// 完整 30 天服务窗口才能继续。此分支在读名单/资料前返回，也保证了
	// 无发布窗口时不会为了降级而访问生产 users/logs。
	if m.usageFactsReadRequested() && (!readRange.Available || readRange.Partial) {
		if readRange.Available {
			result.Message = fmt.Sprintf("待跟进判断需要完整 %d 天；当前已发布 %s 至 %s（%d/%d 天），历史补全后将自动恢复",
				followUpWindowDays, result.CoverageFrom, result.CoverageTo, result.AvailableDays, result.RequestedDays)
		} else {
			result.Message = readRange.Message
			if result.Message == "" {
				result.Message = fmt.Sprintf("待跟进判断需要完整 %d 天，当前历史消费尚未完整发布", followUpWindowDays)
			}
		}
		return result, nil
	}
	items, err := m.computeFollowUpsCompleteWindow(ctx, nowUnix)
	if err != nil {
		return result, err
	}
	result.Companies = items
	result.Available = true
	return result, nil
}

// computeFollowUps 保留原有内部调用契约；覆盖不足时返回空结果但不伪造判断。
// HTTP 处理器使用上面的详细结果向页面显式报告 available=false。
func (m *Monitor) computeFollowUps(ctx context.Context, nowUnix int64) ([]FollowUpCompany, error) {
	result, err := m.computeFollowUpsWithCoverage(ctx, nowUnix)
	return result.Companies, err
}

// computeFollowUpsCompleteWindow 逐用户判是否需跟进(判定逻辑在 classifyMember),
// 再按公司归拢。调用方已保证 facts 服务版覆盖完整 30 天。
func (m *Monitor) computeFollowUpsCompleteWindow(ctx context.Context, nowUnix int64) ([]FollowUpCompany, error) {
	tracked, err := m.listTrackedForUsageRead(ctx)
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
	// 跟进页与用户用量共享同一条读取边界：启用事实读后，只读本地资料快照和
	// 本地事实，不能因为后台跟进计算又退回生产 users/logs。
	tracked, balances, _ := m.refreshTrackedLabelsForRead(ctx, tracked) // 跟进判断不用累计总消耗,忽略第三返回
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 固定读取最近 30 个已闭合 CST 自然日；今天的实时消费不参与
	// “连续 30 天无消费”和两段 7 天趋势判定。
	toTs := followUpDayStart(nowUnix + usageTZOffsetSec)
	fromTs := toTs - int64(followUpWindowDays)*usageFactDaySeconds
	eligibleFromByUser := make(map[int64]int, len(allIDs))
	if m.usageFactsReadRequested() && len(allIDs) > 0 {
		var published []UsageFactPublishedMember
		if err := m.usageFactsStore().WithContext(ctx).
			Select("user_id", "source_floor_hour").
			Where("user_id IN ?", allIDs).
			Find(&published).Error; err != nil {
			return nil, err
		}
		for _, row := range published {
			if row.SourceFloorHour <= 0 {
				continue
			}
			// Only a complete CST day can participate in a daily inactivity
			// rule. If observability starts mid-day, that partial day is not
			// evidence that the member was inactive for the whole day.
			firstCompleteDay := usageFactDayStart(row.SourceFloorHour)
			if row.SourceFloorHour > firstCompleteDay {
				firstCompleteDay += usageFactDaySeconds
			}
			idx := int((firstCompleteDay - fromTs) / usageFactDaySeconds)
			eligibleFromByUser[row.UserID] = max(0, min(idx, followUpWindowDays))
		}
	}
	mx := &UsageMatrix{}
	if len(allIDs) > 0 {
		err = m.loadUsageAggregateJSON(
			ctx,
			m.usageFactCacheKey(adminUsageAggregateKey("matrix", portalMemberFingerprint(tracked), 0, 0, fromTs, toTs)),
			usageAggregateTTL(toTs, time.Unix(nowUnix, 0)),
			false,
			mx,
			func() (any, error) {
				result, err := m.computeUsageMatrixForRead(ctx, allIDs, fromTs, toTs)
				if result != nil {
					result.Users = nil
				}
				return result, err
			},
		)
		if err != nil {
			return nil, err
		}
	}
	// 逐(用户,日序号)消费;dayIdx 0=最早 … 29=今天
	dateOfIdx := make([]string, followUpWindowDays)
	idxOfDate := map[string]int{}
	for i := 0; i < followUpWindowDays; i++ {
		dateOfIdx[i] = time.Unix(fromTs+int64(i)*usageFactDaySeconds, 0).In(usageCST).Format("2006-01-02")
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
		// 跟进规则继续使用消费毛额，退款/净额展示能力不能悄悄改变既有客户活跃判断。
		spendByUserDay[c.UserID][di] += float64(c.ConsumeQuota) / quotaPerUSD
	}

	st := m.loadUsageSettings()
	lastFollow := m.lastFollowUpByUser()

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
		if g, ok := groupByID[gid]; ok {
			name = g.Name
		}
		b := &bucket{comp: &FollowUpCompany{GroupID: gid, GroupName: name}}
		buckets[gid] = b
		return b
	}

	// 逐用户判定(具体规则在 classifyMember)
	for _, u := range tracked {
		var balance *float64
		if b, ok := balances[u.UserID]; ok {
			bv := float64(b) / quotaPerUSD
			balance = &bv
		}
		mem, need := classifyMember(u, spendByUserDay[u.UserID], balance, lastFollow[u.UserID], dateOfIdx,
			eligibleFromByUser[u.UserID], st)
		if !need {
			continue
		}
		b := getBucket(u.GroupID)
		b.members = append(b.members, mem)
		b.comp.Spend30USD += mem.Spend30USD
	}

	// 公司级信号:空分组。
	for _, g := range groups {
		var count int64
		m.storeDB.Model(&TrackedUser{}).Where("group_id = ?", g.ID).Count(&count)
		if count == 0 {
			getBucket(g.ID).comp.CompanyReasons = append(getBucket(g.ID).comp.CompanyReasons, "分组内没有用户(把该公司的用户加进来)")
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

func followUpResponse(result followUpComputation) gin.H {
	total := 0
	for _, co := range result.Companies {
		total += len(co.Members)
	}
	resp := gin.H{
		"enabled":        true,
		"available":      result.Available,
		"companies":      result.Companies,
		"member_total":   total,
		"window_days":    followUpWindowDays,
		"requested_days": result.RequestedDays,
		"available_days": result.AvailableDays,
		"range_partial":  result.RangePartial,
		"requested_from": result.RequestedFrom,
		"requested_to":   result.RequestedTo,
		"coverage_from":  result.CoverageFrom,
		"coverage_to":    result.CoverageTo,
	}
	if result.Message != "" {
		resp["message"] = result.Message
	}
	return resp
}

// serveFollowUps GET /usage/followups(管理员):待跟进(按公司归拢的需跟进成员)。
func (m *Monitor) serveFollowUps(c *gin.Context) {
	m.serveFollowUpsAt(c, time.Now().Unix())
}

// serveFollowUpsAt 使窗口边界可用固定时钟做回归验证。
func (m *Monitor) serveFollowUpsAt(c *gin.Context, nowUnix int64) {
	if !m.usageReadServingEnabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "available": false})
		return
	}
	result, err := m.computeFollowUpsWithCoverage(c.Request.Context(), nowUnix)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		// facts 切读已被显式请求后，本地读故障只能是这一个数据域
		// 暂不可用。继续返回 200 让页面保留其他功能，严禁退回生产 logs。
		if m.usageFactsReadRequested() {
			slog.Warn("待跟进本地事实读取失败，已局部降级", "err", err)
			result.Available = false
			result.Companies = []FollowUpCompany{}
			result.Message = m.usageFactsUnavailableMessage(err, "待跟进判断")
			c.JSON(http.StatusOK, followUpResponse(result))
			return
		}
		slog.Warn("待跟进计算失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "待跟进查询失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, followUpResponse(result))
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
	if in.DormantDays <= 0 || in.DropPct <= 0 || in.DropPct >= 100 || in.LowBalanceUSD <= 0 || math.IsNaN(in.LowBalanceUSD) || math.IsInf(in.LowBalanceUSD, 0) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "跟进阈值无效"})
		return
	}
	// 只更新仍在使用的三项，保留旧库兼容列且不再让客户端改写它们。
	values := map[string]any{"dormant_days": in.DormantDays, "drop_pct": in.DropPct, "low_balance_usd": in.LowBalanceUSD}
	if err := m.storeDB.Model(&UsageSettings{}).Where("id = ?", 1).Assign(values).FirstOrCreate(&UsageSettings{ID: 1}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "settings": m.loadUsageSettings()})
}
