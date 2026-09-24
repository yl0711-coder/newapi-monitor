package monitor

// 客户维护的取数层。一律读本地 storeDB，不碰生产库、不占 usageDetailGate。

import (
	"context"
	"sort"
	"strconv"
	"time"
)

// customerHealthQueryTimeout 单条本地查询预算。
//
// 本地 SQLite 正常在毫秒级完成。给 3 秒是为了在采样写入并发时等锁，
// 而不是立刻报 SQLITE_BUSY；超过这个数说明库本身有问题，宁可报错也不挂住页面。
const customerHealthQueryTimeout = 3 * time.Second

// customerHealthCompany 是名单里的一家公司及其成员。
type customerHealthCompany struct {
	groupID int64
	name    string
	members []CustomerHealthMember
}

// customerHealthUserFacts 一个用户今天的分渠道用量。
type customerHealthUserFacts struct {
	success       int64
	anomaly       int64
	failed        int64
	healthAnomaly int64
	healthFailed  int64
	channels      map[int]customerHealthChannelFacts
	models        map[string]int64
}

// customerHealthChannelFacts 某用户在某渠道上的今日用量。
type customerHealthChannelFacts struct {
	success       int64
	anomaly       int64
	failed        int64
	healthAnomaly int64
	healthFailed  int64
	// failedScopes 只记录会影响稳定率的 type=5 错误所在模型/分组。
	// 下游错误已在来源聚合时排除；异常(type=2)仍不能借同渠道别人的错误原文定责。
	failedScopes map[customerHealthProblemScope]int64
}

type customerHealthProblemScope struct {
	modelName string
	grp       string
}

// customerHealthUsageIndex 今日全量用户事实。
//
// 为什么把**所有**用户都读进来而不只读名单内的：判断"其他客户用这个渠道
// 有没有问题"必须看名单之外的客户。只读名单内的话，这个问题永远答不了。
type customerHealthUsageIndex struct {
	byUser map[int64]*customerHealthUserFacts
	// policyReady=false means this window still contains aggregates written
	// before the current customer-health responsibility policy.
	policyReady bool
	// channelTotals 每个渠道的全站今日用量，用于同渠道横向比较。
	channelTotals map[int]customerHealthChannelFacts
	// channelUsers 每个渠道今天有哪些用户在用，用于判断是否只有这一家。
	channelUsers map[int]map[int64]struct{}
}

// customerHealthProblemIndex 今日问题签名，按渠道归集。
type customerHealthProblemIndex struct {
	byChannel map[int][]customerHealthProblem
}

// customerHealthProblem 一条问题签名的精简形态。
type customerHealthProblem struct {
	channelID int
	modelName string
	grp       string
	code      string
	message   string
	count     int64
}

// customerHealthCompanies 读独立客户维护名单：公司 + 成员。
//
// 只返回**有成员**的公司。空公司在稳定性页上没有意义，展示出来只会让人
// 以为"这家公司今天没请求"，而实际是还没加人。
func (m *Monitor) customerHealthCompanies(ctx context.Context) ([]customerHealthCompany, error) {
	qctx, cancel := context.WithTimeout(ctx, customerHealthQueryTimeout)
	defer cancel()
	var groups []CustomerHealthGroup
	if err := m.storeDB.WithContext(qctx).Order("name ASC").Find(&groups).Error; err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}
	var users []CustomerHealthMember
	if err := m.storeDB.WithContext(qctx).Where("group_id > 0").Find(&users).Error; err != nil {
		return nil, err
	}
	membersByGroup := make(map[int64][]CustomerHealthMember, len(groups))
	for _, u := range users {
		membersByGroup[u.GroupID] = append(membersByGroup[u.GroupID], u)
	}
	out := make([]customerHealthCompany, 0, len(groups))
	for _, g := range groups {
		members := membersByGroup[g.ID]
		if len(members) == 0 {
			continue
		}
		sort.Slice(members, func(i, j int) bool { return members[i].UserID < members[j].UserID })
		out = append(out, customerHealthCompany{groupID: g.ID, name: g.Name, members: members})
	}
	return out, nil
}

// customerHealthUsage 读今日分钟事实并按用户/渠道归集。
func (m *Monitor) customerHealthUsage(ctx context.Context, fromTs, toTs int64) (*customerHealthUsageIndex, error) {
	qctx, cancel := context.WithTimeout(ctx, customerHealthQueryTimeout)
	defer cancel()
	index := &customerHealthUsageIndex{
		byUser:        map[int64]*customerHealthUserFacts{},
		policyReady:   true,
		channelTotals: map[int]customerHealthChannelFacts{},
		channelUsers:  map[int]map[int64]struct{}{},
	}
	type aggRow struct {
		UserID        int64
		ChannelID     int
		ModelName     string
		Grp           string
		Success       int64
		Anomaly       int64
		Failed        int64
		HealthAnomaly int64
		HealthFailed  int64
		HealthVersion int
	}
	var rows []aggRow
	// 按 用户 × 渠道 × 模型 × 分组 聚合。模型/分组虽不直接展示，但它们是
	// 收紧责任方证据的必要边界；丢掉后会把同渠道其它模型的故障借给当前客户。
	err := m.storeDB.WithContext(qctx).
		Model(&CapacityUserMinuteSample{}).
		Select("user_id, channel_id, model_name, grp, COALESCE(SUM(success),0) AS success, "+
			"COALESCE(SUM(anomaly),0) AS anomaly, COALESCE(SUM(failed),0) AS failed, "+
			"COALESCE(SUM(customer_health_anomaly),0) AS health_anomaly, "+
			"COALESCE(SUM(customer_health_failed),0) AS health_failed, "+
			"COALESCE(MIN(customer_health_version),0) AS health_version").
		Where("bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?",
			fromTs, toTs, stabilityTrafficClassificationVersion).
		Group("user_id, channel_id, model_name, grp").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r.HealthVersion != customerHealthStabilityPolicyVersion {
			index.policyReady = false
		}
		facts := index.byUser[r.UserID]
		if facts == nil {
			facts = &customerHealthUserFacts{
				channels: map[int]customerHealthChannelFacts{}, models: map[string]int64{},
			}
			index.byUser[r.UserID] = facts
		}
		facts.success += r.Success
		facts.anomaly += r.Anomaly
		facts.failed += r.Failed
		facts.healthAnomaly += r.HealthAnomaly
		facts.healthFailed += r.HealthFailed
		facts.models[r.ModelName] += r.Success + r.Anomaly + r.Failed
		ch := facts.channels[r.ChannelID]
		if ch.failedScopes == nil {
			ch.failedScopes = map[customerHealthProblemScope]int64{}
		}
		ch.success += r.Success
		ch.anomaly += r.Anomaly
		ch.failed += r.Failed
		ch.healthAnomaly += r.HealthAnomaly
		ch.healthFailed += r.HealthFailed
		if r.HealthFailed > 0 {
			ch.failedScopes[customerHealthProblemScope{modelName: r.ModelName, grp: r.Grp}] += r.HealthFailed
		}
		facts.channels[r.ChannelID] = ch

		total := index.channelTotals[r.ChannelID]
		total.success += r.Success
		total.anomaly += r.Anomaly
		total.failed += r.Failed
		total.healthAnomaly += r.HealthAnomaly
		total.healthFailed += r.HealthFailed
		index.channelTotals[r.ChannelID] = total
		if index.channelUsers[r.ChannelID] == nil {
			index.channelUsers[r.ChannelID] = map[int64]struct{}{}
		}
		index.channelUsers[r.ChannelID][r.UserID] = struct{}{}
	}

	return index, nil
}

// customerHealthProblems 读今日问题签名。
//
// StabilityProblemSample 没有 user_id，所以归因只能到“渠道 × 模型 × 分组”这一层。
// 这不是缺陷而是数据边界：要逐条精确归因得查生产 logs，而那会占用客户
// 排障的闸门（见 customer_health.go 顶部说明）。
func (m *Monitor) customerHealthProblems(ctx context.Context, fromTs, toTs int64, channelIDs []int64) (*customerHealthProblemIndex, error) {
	qctx, cancel := context.WithTimeout(ctx, customerHealthQueryTimeout)
	defer cancel()
	index := &customerHealthProblemIndex{byChannel: map[int][]customerHealthProblem{}}
	if len(channelIDs) == 0 || toTs <= fromTs {
		return index, nil
	}
	type problemRow struct {
		ChannelID int
		ModelName string
		Grp       string
		Code      string
		Message   string
		Count     int64
	}
	var rows []problemRow
	in, inArgs := usageIn("channel_id", channelIDs)
	args := append([]any{fromTs, toTs, stabilityTrafficClassificationVersion, "newapi"}, inArgs...)
	args = append(args, customerHealthMaxProblemsPerScope)
	// 先聚合，再用窗口函数给每个“渠道＋模型＋分组”单独排名。不能先全局
	// LIMIT，也不能只按渠道排名：高噪声渠道/热门模型都不应饿死其它精确范围。
	// 只读当前判据版本的 newapi 错误；旁路 nginx 证据和旧判据不能借来定责。
	err := m.storeDB.WithContext(qctx).Raw(`WITH grouped AS (
 SELECT channel_id,model_name,grp,code,message,COALESCE(SUM(count),0) AS total_count
 FROM stability_problem_samples
 WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ? AND source = ? AND `+in+`
 GROUP BY channel_id,model_name,grp,code,message
), ranked AS (
 SELECT channel_id,model_name,grp,code,message,total_count,
        ROW_NUMBER() OVER (PARTITION BY channel_id,model_name,grp
          ORDER BY total_count DESC,model_name ASC,grp ASC,code ASC,message ASC) AS rn
 FROM grouped
)
SELECT channel_id,model_name,grp,code,message,total_count AS count
FROM ranked WHERE rn <= ? ORDER BY channel_id ASC,rn ASC`, args...).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		index.byChannel[r.ChannelID] = append(index.byChannel[r.ChannelID], customerHealthProblem{
			channelID: r.ChannelID, modelName: r.ModelName, grp: r.Grp,
			code: r.Code, message: r.Message, count: r.Count,
		})
	}
	return index, nil
}

// customerHealthRelevantChannels 只返回维护名单内公司今天实际使用的渠道。
func customerHealthRelevantChannels(companies []customerHealthCompany, usage *customerHealthUsageIndex) []int64 {
	seen := map[int]struct{}{}
	for _, company := range companies {
		for _, member := range company.members {
			if facts := usage.byUser[member.UserID]; facts != nil {
				for channelID := range facts.channels {
					if channelID > 0 {
						seen[channelID] = struct{}{}
					}
				}
			}
		}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, int64(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// attachCustomerHealthSpend 给一行补上今日消耗金额。
//
// ★ 只读本地事实，绝不退回生产库 ★
// 用户用量页在事实层未启用时会退回 computeUsageMatrix 直查生产 logs，
// 那条路占 usageGate（容量 1）、与客户 Portal 同泳道。本页是常开概览页，
// 每次刷新都那么干会让客户查自己日志排队。所以本页取不到就如实说取不到，
// 不借那条路补数——这是"新增功能不得妨碍既有功能"的直接应用。
//
// 口径与用户用量页的今日总消费同源：都是 loadUsageLiveProjection 的
// 「已封口小时事实 + (当前累计 - 封口后锚点)」双水位（见 usage-live-projection.md）。
func (m *Monitor) attachCustomerHealthSpend(ctx context.Context, row *CustomerHealthRow, fromTs, toTs int64, now time.Time) {
	rows := []CustomerHealthRow{*row}
	m.attachCustomerHealthSpendBatch(ctx, rows, fromTs, toTs, now)
	*row = rows[0]
}

// attachCustomerHealthSpendBatch 一次读取整页成员金额，再按公司独立判断完整性。
func (m *Monitor) attachCustomerHealthSpendBatch(ctx context.Context, rows []CustomerHealthRow, fromTs, toTs int64, now time.Time) {
	ids := customerHealthRowUserIDs(rows)
	if len(ids) == 0 {
		for i := range rows {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "该公司没有可统计的成员。"
		}
		return
	}
	// 事实层没启用时 usageFactsStore() 可能返回 nil，直接查会 panic。
	if !m.usageFactsReadEnabled() || m.usageFactsStore() == nil {
		if m.cfg.CustomerHealthSourceEnabled {
			m.attachCustomerHealthCollectedSpendBatch(ctx, rows, ids, fromTs, toTs)
			return
		}
		for i := range rows {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "本地用量事实层未启用，今日消耗金额不可知（不代表没有消耗）。"
		}
		return
	}
	batch, err := m.loadUsageLiveProjectionBatch(ctx, ids, fromTs, toTs, now)
	if err != nil {
		for i := range rows {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "今日消耗金额暂时读不到（本地事实库查询失败），不代表没有消耗。"
		}
		return
	}
	fallback := make([]int, 0, len(rows))
	for i := range rows {
		net := make(map[int64]int64, len(rows[i].Members))
		through := now.Unix()
		complete := batch != nil
		for _, member := range rows[i].Members {
			value, okValue := int64(0), false
			memberThrough, okThrough := int64(0), false
			if batch != nil {
				value, okValue = batch.TodayNetByUser[member.UserID]
				memberThrough, okThrough = batch.ThroughByUser[member.UserID]
			}
			if !okValue || !okThrough {
				complete = false
				break
			}
			net[member.UserID] = value
			if memberThrough < through {
				through = memberThrough
			}
		}
		if complete {
			applyCustomerHealthSpend(&rows[i], net, customerHealthSpendLive,
				"今日消耗＝已封口小时事实＋实时累计差额，口径与用户用量页今日总消费一致；"+
					"已更新至 "+time.Unix(through, 0).In(cstLocation).Format("15:04")+"（CST）。")
		} else {
			fallback = append(fallback, i)
		}
	}
	if len(fallback) > 0 {
		m.attachCustomerHealthFinalizedSpendBatch(ctx, rows, fallback, fromTs)
	}
}

func customerHealthRowUserIDs(rows []CustomerHealthRow) []int64 {
	seen := map[int64]struct{}{}
	for _, row := range rows {
		for _, member := range row.Members {
			if member.UserID > 0 {
				seen[member.UserID] = struct{}{}
			}
		}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// attachCustomerHealthCollectedSpendBatch 读取 logchain-only 独立采集已写入
// 本地 SQLite 的用户分钟净 quota。整页只执行一次聚合查询。
func (m *Monitor) attachCustomerHealthCollectedSpendBatch(ctx context.Context, rows []CustomerHealthRow, ids []int64, dayTs, dayEnd int64) {
	from := m.customerHealthSourceFrom.Load()
	through := m.customerHealthSourceThrough.Load()
	if from != dayTs || through <= dayTs {
		for i := range rows {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "客户维护只读采集正在追赶今日数据，连续覆盖尚未建立；金额暂不展示。"
		}
		return
	}
	if through > dayEnd {
		through = dayEnd
	}
	net := make(map[int64]int64, len(ids))
	for _, id := range ids {
		net[id] = 0
	}
	in, args := usageIn("user_id", ids)
	queryArgs := []any{dayTs, through, stabilityTrafficClassificationVersion}
	queryArgs = append(queryArgs, args...)
	type rowNet struct {
		UserID int64 `gorm:"column:user_id"`
		Net    int64 `gorm:"column:net"`
	}
	var netRows []rowNet
	qctx, cancel := context.WithTimeout(ctx, customerHealthQueryTimeout)
	defer cancel()
	err := m.storeDB.WithContext(qctx).Raw(`SELECT user_id,
CAST(COALESCE(SUM(quota),0)-COALESCE(SUM(refund_quota),0) AS INTEGER) AS net
FROM capacity_user_minute_samples
WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ? AND `+in+`
GROUP BY user_id`, queryArgs...).Scan(&netRows).Error
	if err != nil {
		for i := range rows {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "客户维护本地净额查询失败，金额暂不展示（不代表没有消耗）。"
		}
		return
	}
	for _, item := range netRows {
		net[item.UserID] = item.Net
	}
	for i := range rows {
		applyCustomerHealthSpend(&rows[i], net, customerHealthSpendSampled,
			"8204 独立只读采集已连续覆盖今日 00:00 至 "+
				time.Unix(through, 0).In(cstLocation).Format("15:04")+
				"（CST）；金额来自生产 logs 的只读聚合并仅写本地 SQLite，已扣退款。")
	}
}

// attachCustomerHealthFinalizedSpendBatch 为实时投影未就绪的公司批量读取已封口金额。
func (m *Monitor) attachCustomerHealthFinalizedSpendBatch(ctx context.Context, rows []CustomerHealthRow, indexes []int, dayTs int64) {
	finalizedThrough := m.usageFactsReadyThrough.Load()
	if finalizedThrough <= dayTs {
		for _, i := range indexes {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "今日尚无已封口的用量事实，金额不可知（不代表没有消耗）。"
		}
		return
	}
	qctx, cancel := context.WithTimeout(ctx, customerHealthQueryTimeout)
	defer cancel()
	// usage_hour_facts is intentionally sparse: a fully verified member with no
	// requests/refunds has no row. Publication, not row existence, proves that a
	// missing aggregate is a legitimate zero. Unpublished members must remain nil.
	membership, err := m.currentPublishedUsageMembership(qctx, nil)
	if err != nil {
		for _, i := range indexes {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "今日消耗金额暂时读不到（成员发布状态查询失败），不代表没有消耗。"
		}
		return
	}
	publishedIDs := make([]int64, 0, len(membership.Members))
	net := make(map[int64]int64, len(membership.Members))
	publishedByGroup := make(map[int64][]int64)
	for _, member := range membership.Members {
		publishedIDs = append(publishedIDs, member.UserID)
		net[member.UserID] = 0
		publishedByGroup[member.GroupID] = append(publishedByGroup[member.GroupID], member.UserID)
	}
	actual, err := usageFactsTodayFinalizedNetByUser(m.usageFactsStore().WithContext(qctx), publishedIDs, dayTs, finalizedThrough)
	if err != nil {
		for _, i := range indexes {
			rows[i].SpendState = customerHealthSpendUnavailable
			rows[i].SpendNote = "今日消耗金额暂时读不到（已封口事实查询失败），不代表没有消耗。"
		}
		return
	}
	for userID, quota := range actual {
		net[userID] = quota
	}
	for _, i := range indexes {
		companyNet := make(map[int64]int64, len(publishedByGroup[rows[i].GroupID]))
		for _, id := range publishedByGroup[rows[i].GroupID] {
			companyNet[id] = net[id]
		}
		applyCustomerHealthSpend(&rows[i], companyNet, customerHealthSpendFinalized,
			"实时水位暂不可用，此处只统计到已封口小时 "+
				time.Unix(finalizedThrough, 0).In(cstLocation).Format("15:04")+
				"（CST）；该时刻之后的消耗未计入。")
	}
}

// applyCustomerHealthSpend 把 quota 折成美元写进行与成员。
//
// 折算沿用仓库既有常量 quotaPerUSD，不另立一个换算系数——两处各写一份
// 迟早漂移，页面之间金额对不上。
//
// ★ 两条不能改的规则 ★
//
//  1. 只给取到的成员写值。没在 net 里的成员保持 nil。补 0 会让
//     "这个人今天没花钱"和"这个人的数取不到"看起来一样。
//
//  2. 有任何一个成员取不到，公司合计就必须是 nil（状态 partial）。
//     本页承诺的是"该公司所有用户今日消耗"，把取到的几个加起来当合计，
//     会给出一个看起来合理但偏小的数字，且页面上无从察觉它不完整。
//     典型触发场景：某成员刚加进名单，用量事实还没为它发布完成。
func applyCustomerHealthSpend(row *CustomerHealthRow, net map[int64]int64, state, note string) {
	resolved := 0
	total := int64(0)
	for i := range row.Members {
		q, ok := net[row.Members[i].UserID]
		if !ok {
			row.Members[i].TodaySpendUSD = nil
			continue
		}
		usd := float64(q) / quotaPerUSD
		row.Members[i].TodaySpendUSD = &usd
		total += q
		resolved++
	}
	missing := len(row.Members) - resolved
	if missing > 0 {
		// 合计留空，并说清缺了几个人——只说"不完整"会让人无从判断严重程度。
		row.TodaySpendUSD = nil
		row.SpendState = customerHealthSpendPartial
		row.SpendNote = "公司合计不可给出：" + strconv.Itoa(len(row.Members)) + " 个成员中有 " +
			strconv.Itoa(missing) + " 个今日消耗取不到（多为刚加入名单、用量事实尚未发布完成），" +
			"已取到的成员金额单独列出。合计要等全部成员都有事实后才给，避免给出偏小的数。"
		return
	}
	totalUSD := float64(total) / quotaPerUSD
	row.TodaySpendUSD = &totalUSD
	row.SpendState = state
	row.SpendNote = note
}

// sortCustomerHealthRows 排序：先按最需要关注的排。
//
// 顺序依据：有稳定率的排在前（nil 表示今天没请求，放最后），稳定率低的
// 在前，再按公司名。这样一打开页面，最该看的公司就在第一行。
func sortCustomerHealthRows(rows []CustomerHealthRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if (a.StabilityPct == nil) != (b.StabilityPct == nil) {
			return b.StabilityPct == nil
		}
		if a.StabilityPct != nil && b.StabilityPct != nil && *a.StabilityPct != *b.StabilityPct {
			return *a.StabilityPct < *b.StabilityPct
		}
		return a.Company < b.Company
	})
}
