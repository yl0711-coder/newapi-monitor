package monitor

// 客户维护：盯住已加入名单的公司，看它们今天用得顺不顺。
//
// ★ 与客户排障的分工 ★
// 本页只回答"稳不稳、大概谁的问题"，不列具体请求日志——要看某一条请求
// 发生了什么，去客户排障。两边共用同一套判据（异常口径、归因三分），
// 但本页一律读**本地事实**，不打生产库。
// 客户维护名单也有独立的本地表，不与用户用量的 TrackedUser/CustomerGroup
// 共用增删改记录；两边只通过 user_id 关联同一用户的事实。
//
// 为什么不打生产库：客户排障走 usageDetailGate（容量 1），与客户 Portal
// 查自己日志是同一条泳道。本页是常开的概览页，若每次刷新都查生产 logs，
// 客户查自己日志就要排队——这正是"新增功能不得妨碍既有功能"要挡的。
//
// 数据来源：
//   CapacityUserMinuteSample   分钟 × 用户 × 渠道 × 模型 × 分组，已分好
//                              Success / Anomaly / Failed
//   StabilityProblemSample     分钟 × 渠道 × 模型 × 分组 + code/message
//                              （无 user_id，故归因到"渠道/模型"这一层）

import (
	"context"
	"time"
)

// customerHealthRedThreshold 稳定率低于该值这一行标红。
//
// 用户口径：低于 90% 标红。这个阈值只影响展示，不参与任何计算，
// 也不写库——改它不会让历史数据的判定发生漂移。
const customerHealthRedThreshold = 90.0

// customerHealthScopeGapThresholdPct 判断问题是否明显集中在当前公司的差值阈值。
// 比较同一渠道上“当前公司失败记录占比 - 其他账号失败记录占比”；达到 7 个
// 百分点才提示更像该公司问题，否则提示更像渠道共同问题。
const customerHealthScopeGapThresholdPct = 7.0

// customerHealthStabilityPolicyVersion versions the customer-maintenance-only
// responsibility filter. It is intentionally separate from the global
// delivery classification version: changing who should lower a customer's
// stability must not rewrite the stability dashboard's historical meaning.
const customerHealthStabilityPolicyVersion = 3

// customerHealthSlowFirstByteMS 是客户维护稳定性采用的首字延迟边界。
// other.frt 单位为毫秒；严格大于 3 秒才计入稳定性，恰好 3 秒仍不计入。
const customerHealthSlowFirstByteMS int64 = 3000

// customerHealthMaxProblemsPerScope 归因取样上限（每个渠道＋模型＋分组分别计算）。
//
// 旧实现对全站统一 LIMIT 2000，一个高噪声渠道可能吃完整个额度，让其它渠道
// 完全没有归因证据。只按渠道拆仍不够：同一渠道的热门模型也会饿死低频模型。
// 现在先限定客户实际使用的渠道，再给归因实际匹配的每个精确范围各取 Top N。
const customerHealthMaxProblemsPerScope = 50

// CustomerHealthMember 公司下的一个用户。
//
// 多个用户名统一按一个公司算（用户明确要求）：Total 等指标是公司下所有
// 成员相加，不逐人展示——逐人展示会让"这个公司稳不稳"这个问题变模糊。
type CustomerHealthMember struct {
	// The customer-health membership is intentionally separate from the
	// general usage watch list.  UserID remains the primary key so a user can
	// only be assigned to one customer-health company at a time, matching the
	// old list semantics while allowing the two domains to evolve separately.
	UserID   int64  `gorm:"primaryKey;column:user_id" json:"user_id"`
	GroupID  int64  `gorm:"index;column:group_id" json:"-"`
	Username string `gorm:"size:255" json:"username"`
	Email    string `gorm:"size:255" json:"-"`
	Note     string `gorm:"size:200" json:"-"`
	AddedAt  int64  `json:"-"`

	// TodaySpendUSD 该成员今日消耗（美元，已扣退款）。
	// ★ nil = 取不到，不是 0 ★ 见 CustomerHealthRow.TodaySpendUSD 的说明。
	TodaySpendUSD *float64 `gorm:"-" json:"today_spend_usd"`
}

// CustomerHealthGroup is the customer-maintenance company list.  It is
// deliberately independent from CustomerGroup, which belongs to the usage
// and customer-portal domain.  Existing installations are copied once by
// migrateLegacyCustomerHealthTables when this table is first introduced.
type CustomerHealthGroup struct {
	ID        int64  `gorm:"primaryKey" json:"id"`
	Name      string `gorm:"uniqueIndex;size:64" json:"name"`
	Note      string `gorm:"size:500" json:"note"`
	CreatedAt int64  `json:"created_at"`
}

// CustomerHealthMigrationState records the one-time legacy copy.  Keeping a
// durable marker prevents later edits to the usage tables from silently
// changing the independent customer-health list.
type CustomerHealthMigrationState struct {
	ID         uint  `gorm:"primaryKey;autoIncrement:false"`
	Version    int   `gorm:"not null"`
	MigratedAt int64 `gorm:"column:migrated_at"`
}

// CustomerHealthPrimaryModel is one of the one or two models selected by the
// customer-maintenance primary-model policy across all tracked members.
type CustomerHealthPrimaryModel struct {
	Name     string  `json:"name"`
	Requests int64   `json:"requests"`
	SharePct float64 `json:"share_pct"`
}

// CustomerHealthRow 一个公司今天的稳定性。
type CustomerHealthRow struct {
	GroupID int64                  `json:"group_id"`
	Company string                 `json:"company"`
	Members []CustomerHealthMember `json:"members"`
	// MetricsReady 表示日志记录数/记录稳定率已建立可信的连续来源覆盖。
	// false 时数值字段只是 Go 零值，前端必须显示“—”，不得解释为真实零。
	MetricsReady bool   `json:"metrics_ready"`
	MetricsNote  string `json:"metrics_note"`

	// Total = Success + Anomaly + Failed，即已进入渠道的日志记录数。
	// NewAPI 换渠道重试会为同一个外部请求写多条 type=5，最后还可能再写一条
	// type=2；因此这里明确是记录/渠道尝试口径，不冒充去重后的用户请求数。
	// ★ 不含前置拒绝 ★ 那类请求不写 logs，本地事实里也没有；
	// 要看被拒量去问题预警。页面列名和展开指标保持这一口径。
	Total   int64 `json:"total"`
	Success int64 `json:"success"`
	Anomaly int64 `json:"anomaly"`
	Failed  int64 `json:"failed"`
	// StabilityAnomaly counts only anomalies attributed to upstream or ours.
	// StabilityFailed counts every error except downstream/customer errors.
	// Raw Anomaly/Failed remain above so the filtered stability can be audited.
	StabilityAnomaly int64 `json:"stability_anomaly"`
	StabilityFailed  int64 `json:"stability_failed"`

	// PrimaryModels is derived from all members together. The most-used model is
	// always returned; when two models both exceed 40%, both are returned.
	PrimaryModels []CustomerHealthPrimaryModel `json:"primary_models"`

	// StabilityPct 记录稳定率百分比。**nil = 今天还没有任何日志记录**，
	// 不是 0——0% 会被读成"全挂了"。缺失绝不显示为零。
	StabilityPct *float64 `json:"stability_pct"`

	// Fault 主要原因三分：upstream / ours / downstream / unknown。
	// 沿用 logchain_fault.go 的同一套取值，不另立一套词汇。
	Fault string `json:"fault"`
	// FaultConfidence 归因置信度，沿用 faultConf* 取值。
	// 本页归因基于渠道级事实推断，通常不会是 high。
	FaultConfidence string `json:"fault_confidence"`
	// Reason 人能读的主要原因说明。
	Reason string `json:"reason"`

	// ChannelScope 回答"其他客户用这个渠道有没有问题"。
	// 取值见 customerHealthScope* 常量。
	ChannelScope string `json:"channel_scope"`
	// ScopeNote 上述判断的依据说明，页面直接展示，不让人猜。
	ScopeNote string `json:"scope_note"`

	// TodaySpendUSD 该公司所有成员今日消耗合计（美元，已扣退款）。
	//
	// ★ nil = 取不到，绝不写 0 ★ 本地事实层未启用、水位未就绪、成员未全部
	// 签收时都取不到。显示 $0.00 会被读成"这家今天没花钱"，而实际是"不知道"。
	// 这是系统定位宪法「缺失绝不显示为零」在本字段上的形态。
	TodaySpendUSD *float64 `json:"today_spend_usd"`
	// SpendState 金额的可信状态，取值见 customerHealthSpend* 常量。
	SpendState string `json:"spend_state"`
	// SpendNote 金额口径/为什么取不到的说明，页面必须展示。
	SpendNote string `json:"spend_note"`
}

// CustomerHealthReport 整页响应。
type CustomerHealthReport struct {
	// Day 是本次统计的自然日（CST），形如 2026-09-15。
	Day string `json:"day"`
	// FromTs/ToTs 统计窗口（含 From、不含 To）。
	FromTs int64 `json:"from_ts"`
	ToTs   int64 `json:"to_ts"`
	// GeneratedAt 出报时刻，让人能判断数据新旧。
	GeneratedAt int64  `json:"generated_at"`
	TimeZone    string `json:"time_zone"`
	// RedThreshold 标红阈值，由后端下发，前端不得自行硬编码。
	RedThreshold float64 `json:"red_threshold"`
	// Collection 说明本页本地事实采集是否开启、是否已经连续覆盖到某个时刻。
	// 0 请求只有在 Ready=true 时才可理解为覆盖区间内的真实零。
	Collection CustomerHealthCollectionStatus `json:"collection"`
	Rows       []CustomerHealthRow            `json:"rows"`
	// Notes 字段保留为空，兼容旧接口字段；客户维护页不再展示长篇口径说明。
	Notes []string `json:"notes"`
}

type CustomerHealthCollectionStatus struct {
	Mode          string `json:"mode"`
	Enabled       bool   `json:"enabled"`
	Running       bool   `json:"running"`
	Ready         bool   `json:"ready"`
	FromTs        int64  `json:"from_ts"`
	ThroughTs     int64  `json:"through_ts"`
	LastSuccessAt int64  `json:"last_success_at"`
	LastFailureAt int64  `json:"last_failure_at"`
	Note          string `json:"note"`
}

// 当前公司相对同渠道其他账号是否明显更差。四态分离，不合并成布尔——
// "更像该公司问题"和"无法判断"是完全不同的结论，合并会让人对着错误方向排查。
const (
	customerHealthScopeOnlyThis = "only_this_customer" // 当前公司失败率至少高 7 个百分点
	customerHealthScopeShared   = "all_customers"      // 差值不足 7 个百分点，更像渠道问题
	customerHealthScopeSoleUser = "sole_user"          // 该渠道今天只有这个客户在用
	customerHealthScopeUnknown  = "unknown"            // 证据不足
	customerHealthScopeHealthy  = "no_problem"         // 该公司今天没问题，无需比对
)

// 今日消耗金额的可信状态。三态分离而不是一个布尔：
// "已到实时水位"和"只到封口小时"都是可用金额，但口径不同；
// "取不到"必须与它们彻底分开，否则会被当成 0。
const (
	// customerHealthSpendLive 已封口小时事实 + 实时累计差额，口径与用户用量页
	// 的今日总消费一致（usage-live-projection.md 的双水位）。
	customerHealthSpendLive = "live"
	// customerHealthSpendFinalized 只到已封口小时。实时投影 fail-closed 时的退路，
	// 是真实数字但不含封口之后的消耗，必须在页面上说明截止到几点。
	customerHealthSpendFinalized = "finalized"
	// customerHealthSpendSampled 是 logchain-only 验收环境从生产 logs 只读采集、
	// 写入本地分钟事实后得到的净额。它有明确连续右水位，但不冒充 Usage Facts 发布版。
	customerHealthSpendSampled = "sampled"
	// customerHealthSpendUnavailable 取不到。金额一律 nil。
	customerHealthSpendUnavailable = "unavailable"
	// customerHealthSpendPartial 只取到部分成员。
	//
	// ★ 这一档存在的理由 ★ 本页承诺的是"该公司**所有**用户今日消耗"。
	// 若某个成员的金额取不到（典型场景：刚加进名单、事实还没为它发布），
	// 把取到的那几个加起来当公司合计，会给出一个**看起来合理但偏小**的数字，
	// 而且没有任何迹象表明它不完整——这比不给数更危险。
	// 所以此档下公司合计一律 nil，只保留已取到成员的各自金额。
	customerHealthSpendPartial = "partial"
)

// customerHealthDayRange 返回指定时刻所在自然日（CST）的 [from, to)。
func customerHealthDayRange(now time.Time) (int64, int64, string) {
	local := now.In(cstLocation)
	start := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, cstLocation)
	return start.Unix(), start.AddDate(0, 0, 1).Unix(), start.Format("2006-01-02")
}

// customerHealthNotes 保留旧接口形状，但不再向客户维护页下发长篇说明。
// 具体数据口径由列名、展开明细和客户排障页承载，避免同一页重复堆叠解释文字。
func customerHealthNotes() []string {
	return nil
}

// customerHealthStability 计算稳定率。
//
// 口径：(Total - StabilityAnomaly - StabilityFailed) / Total。
// Total 仍是全部日志记录；归因为客户/下游的错误，以及证据不足而未归到
// upstream/ours 的异常，只是不降低稳定率，不能从总记录数里悄悄消失。
//
// 没有任何请求时返回 nil：0% 会被读成"全挂了"，而实际是"今天还没用"。
func customerHealthStability(total, stabilityAnomaly, stabilityFailed int64) *float64 {
	if total <= 0 {
		return nil
	}
	unstable := stabilityAnomaly + stabilityFailed
	if unstable < 0 {
		unstable = 0
	}
	if unstable > total {
		unstable = total
	}
	pct := float64(total-unstable) * 100 / float64(total)
	return &pct
}

// buildCustomerHealthReport 汇总所有公司今天的稳定性。
func (m *Monitor) buildCustomerHealthReport(ctx context.Context, now time.Time) (CustomerHealthReport, error) {
	fromTs, toTs, day := customerHealthDayRange(now)
	report := CustomerHealthReport{
		Day: day, FromTs: fromTs, ToTs: toTs,
		GeneratedAt: now.Unix(), TimeZone: "Asia/Shanghai",
		RedThreshold: customerHealthRedThreshold,
		Collection:   m.customerHealthCollectionStatus(fromTs),
		Rows:         []CustomerHealthRow{},
		Notes:        customerHealthNotes(),
	}
	companies, err := m.customerHealthCompanies(ctx)
	if err != nil {
		return CustomerHealthReport{}, err
	}
	if len(companies) == 0 {
		return report, nil
	}
	usageToTs := toTs
	metricsReady := true
	if m.cfg.CustomerHealthSourceEnabled {
		metricsReady = report.Collection.Ready
		if !metricsReady {
			usageToTs = fromTs
		} else if report.Collection.ThroughTs < usageToTs {
			// 独立采集重启后 SQLite 里可能留有右水位之后的旧行。只统计本轮已经
			// 从 00:00 连续证明到的区间，保证页面数字与显示的水位完全一致。
			usageToTs = report.Collection.ThroughTs
		}
	}
	usage, err := m.customerHealthUsage(ctx, fromTs, usageToTs)
	if err != nil {
		return CustomerHealthReport{}, err
	}
	if metricsReady && !usage.policyReady {
		metricsReady = false
		report.Collection.Ready = false
		report.Collection.Note = "客户维护的新稳定性归因口径正在回算今日数据，完成前不展示不完整指标"
	}
	problems := &customerHealthProblemIndex{byChannel: map[int][]customerHealthProblem{}}
	if metricsReady {
		// 问题证据必须与已证明的用量右水位一致；并且只读维护客户实际用到的渠道。
		// 覆盖未就绪时不做这条无用查询，避免采集追赶期间与 SQLite 写入争锁。
		problems, err = m.customerHealthProblems(ctx, fromTs, usageToTs,
			customerHealthRelevantChannels(companies, usage))
		if err != nil {
			return CustomerHealthReport{}, err
		}
	}
	for _, company := range companies {
		row := buildCustomerHealthRow(company, usage, problems)
		row.MetricsReady = metricsReady
		if m.cfg.CustomerHealthSourceEnabled {
			row.MetricsNote = report.Collection.Note
		}
		if !metricsReady {
			row.Total, row.Success, row.Anomaly, row.Failed = 0, 0, 0, 0
			row.StabilityAnomaly, row.StabilityFailed = 0, 0
			row.PrimaryModels = []CustomerHealthPrimaryModel{}
			row.StabilityPct = nil
			row.Fault, row.FaultConfidence = "", ""
			row.Reason = ""
			row.ChannelScope, row.ScopeNote = customerHealthScopeUnknown, ""
		}
		report.Rows = append(report.Rows, row)
	}
	// 金额事实一次批量读取，再按公司独立做完整性判定。一家公司缺成员不会
	// 传染其它公司，同时消除“每家公司一组 SQLite 查询”的 N+1。
	m.attachCustomerHealthSpendBatch(ctx, report.Rows, fromTs, toTs, now)
	sortCustomerHealthRows(report.Rows)
	return report, nil
}
