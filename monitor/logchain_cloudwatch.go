package monitor

// logchain_cloudwatch.go：客户排障的 CloudWatch 按需证据查询。
//
// ★ 只在人工展开某一条请求并点击时才执行 ★
// 不在 /logchain/requests 里自动为整页补证据：Logs Insights 按扫描字节计费，
// 自动全查会让每次翻页都产生 AWS 费用，并把现有排障查询拖慢。
//
// ★ 只用 FilterLogEvents，绝不用 Logs Insights ★
// 按需查一定带精确 Request ID，而「搜索所有日志流」不按扫描量计费；
// Insights 才按字节计费。用错 API 不会报错，只会在账单上出现。
//
// 本文件不落库、不缓存到磁盘、不回传任何原始标识：证据里的 ID 全部是分域 HMAC。

import (
	"context"
	"time"
)

// logChainCloudWatchWindow 是按需查询的单侧时间窗。
// 文档建议「已知精确 Request ID → 报障时间前后各 2 分钟」。
// 不放大：窗口每扩一倍，FilterLogEvents 要翻的页也跟着涨。
const logChainCloudWatchWindow = 2 * time.Minute

// logChainCloudWatchTotalBudget 是一次点击的总预算，覆盖全部来源之和。
// 文档给单个排障任务 20~30 秒；取 30 秒后仍留有余量让前端拿到结构化结果，
// 而不是把浏览器挂到超时。单来源超时仍由阶段 1 的 25 秒兜底。
const logChainCloudWatchTotalBudget = 30 * time.Second

// logChainCloudWatchSourceStatus 是单个来源的观察结果。
//
// ★ empty 与 unavailable 必须分开 ★
// 「查了，没有」和「没查到/查不了」是完全不同的事实。混成一个值会让人
// 把数据源故障读成"确认没有这条请求"，这正是「缺失绝不显示为零」要禁止的。
type logChainCloudWatchSourceStatus struct {
	Source cloudWatchLogSourceID `json:"source"`
	// LogGroup / Region 只回显后端实际用了哪个白名单来源，供人复核查询范围。
	// 前端不能提交这两个字段，见 logChainCloudWatchRequest。
	LogGroup  string `json:"log_group"`
	Region    string `json:"region"`
	Sensitive bool   `json:"sensitive"`
	// Status 取值：found / empty / unavailable / access_denied / throttled /
	// timeout / disabled / invalid / cancelled / skipped。
	Status string `json:"status"`
	// Linkage 是关联等级。exact 只给能用 Request ID 精确匹配的来源；
	// CloudFront 与 NewAPI 的 Request ID 尚未统一透传，只能是候选。
	Linkage     string `json:"linkage"`
	Events      int    `json:"events"`
	Parsed      uint64 `json:"parsed"`
	ParseFailed uint64 `json:"parse_failed"`
	Truncated   bool   `json:"truncated"`
	// Partial 表示同一来源需要执行多次固定查询时，已有查询成功但至少一次
	// 失败或因总预算未执行。它避免“只要命中过一条就把其余缺口藏掉”。
	Partial bool `json:"partial,omitempty"`
	// Note 是固定文案，不含任何日志原文或 AWS 原始错误消息。
	Note string `json:"note,omitempty"`
}

type logChainCloudWatchEvidence struct {
	Source   cloudWatchLogSourceID          `json:"source"`
	Evidence []cloudWatchStructuredEvidence `json:"evidence"`
}

type logChainCloudWatchResult struct {
	Enabled bool `json:"enabled"`
	// SensitiveRequested / SensitiveIncluded 分开：页面必须显示"这次到底读没读
	// 客户 IP 与 User-Agent"，请求了但被拒不能显示成读了。
	SensitiveRequested bool                             `json:"sensitive_requested"`
	SensitiveIncluded  bool                             `json:"sensitive_included"`
	FromUnix           int64                            `json:"from_unix"`
	ToUnix             int64                            `json:"to_unix"`
	Sources            []logChainCloudWatchSourceStatus `json:"sources"`
	Evidence           []logChainCloudWatchEvidence     `json:"evidence"`
	// Caveats 是固定边界说明，避免页面把候选关联读成确定结论。
	Caveats []string `json:"caveats"`
}

// logChainCloudWatchPlan 是一次按需查询要读的来源。顺序即页面展示顺序，
// 按请求生命周期从入口到应用排列。
type logChainCloudWatchPlan struct {
	Source  cloudWatchLogSourceID
	Kind    cloudWatchFixedFilterKind
	Linkage string
	Note    string
}

// logChainCloudWatchPlans 固定来源清单。
//
// Worker 两个来源用 oneapi_request_id 精确匹配，可标 exact。
// CloudFront 只能按同一 Request ID 值试查：当前 NewAPI Request ID 并未透传到
// CloudFront，所以命中也只能算候选；查不到更不能推断"客户没到达"。
func logChainCloudWatchPlans(includeSensitive bool) []logChainCloudWatchPlan {
	plans := []logChainCloudWatchPlan{
		{Source: cwSourceCloudFrontAccess, Kind: cwFilterCloudFrontRequestID, Linkage: "ambiguous",
			Note: "CloudFront 与 NewAPI 的 Request ID 尚未统一透传，此处只能作候选；日志另有投递延迟，查不到不等于请求没到达。"},
		{Source: cwSourceWorkerNginx, Kind: cwFilterWorkerNginxID, Linkage: "exact",
			Note: "按 NewAPI Request ID 精确匹配入口日志流。"},
		{Source: cwSourceWorkerNewAPI, Kind: cwFilterWorkerNewAPIID, Linkage: "exact",
			Note: "按 NewAPI Request ID 精确匹配应用日志流；未写入 logs 表的路由前拒绝会出现在这里。"},
	}
	if includeSensitive {
		plans = append(plans, logChainCloudWatchPlan{Source: cwSourceCloudFrontDiagnostic, Kind: cwFilterCloudFrontRequestID,
			Linkage: "ambiguous", Note: "客户网络诊断日志，含客户 IP 与 User-Agent；页面只显示脱敏摘要与 HMAC。"})
	}
	return plans
}

func logChainCloudWatchCaveats() []string {
	return []string{
		"这是单条请求的链路证据，不是统计口径；拒绝量与趋势仍看「问题预警」。",
		"exact 表示按 NewAPI Request ID 精确匹配；候选（ambiguous）不等于就是这条请求，不能据此定责。",
		"某个来源显示「查不到」与「查不了」含义不同：前者是该窗口内没有匹配事件，后者是数据源不可用，都不代表请求没有发生。",
	}
}

// lookupLogChainCloudWatchEvidence 执行一次按需查询。
//
// 每个来源独立成败：某一路失败只标记该来源状态，绝不让整次查询失败，
// 也绝不影响已经从生产 logs 取回的排障明细。
func (m *Monitor) lookupLogChainCloudWatchEvidence(ctx context.Context, requestID string, at time.Time, wantSensitive, allowSensitive bool) logChainCloudWatchResult {
	includeSensitive := wantSensitive && allowSensitive
	from, to := at.Add(-logChainCloudWatchWindow), at.Add(logChainCloudWatchWindow)
	out := logChainCloudWatchResult{
		Enabled: true, SensitiveRequested: wantSensitive, SensitiveIncluded: includeSensitive,
		FromUnix: from.Unix(), ToUnix: to.Unix(), Caveats: logChainCloudWatchCaveats(),
	}
	// ★ 开关必须先判 ★
	// 未启用与"密钥没配好"是两回事。若让密钥校验先失败，关闭状态会显示成
	// unavailable，看页面的人会去排查 AWS 权限和网络，而真实原因只是功能没开。
	if !m.cloudWatchEvidenceAvailable() {
		out.Enabled = false
		for _, plan := range logChainCloudWatchPlans(includeSensitive) {
			out.Sources = append(out.Sources, m.cloudWatchSourceStatus(plan, "disabled",
				"CloudWatch 按需证据未启用。"))
		}
		return out
	}
	parser, err := m.cloudWatchEvidenceParser(includeSensitive)
	if err != nil {
		for _, plan := range logChainCloudWatchPlans(includeSensitive) {
			out.Sources = append(out.Sources, m.cloudWatchSourceStatus(plan, "unavailable",
				"证据脱敏密钥未按要求配置，已拒绝读取，避免回传未脱敏标识。"))
		}
		return out
	}
	// ★ 必须有整次点击的总预算 ★
	// 来源是串行查的，而每个来源自身就有 25 秒总超时。AWS 不可达时 3~4 个来源
	// 会累加到 75~100 秒：浏览器早已放弃，用户只看到长时间卡住，还白排队占用闸门。
	// 超出预算后剩余来源直接标 skipped，如实说明"本次没查"，不假装查过。
	budgetCtx, cancelBudget := context.WithTimeout(ctx, logChainCloudWatchTotalBudget)
	defer cancelBudget()
	for _, plan := range logChainCloudWatchPlans(includeSensitive) {
		if budgetCtx.Err() != nil {
			out.Sources = append(out.Sources, m.cloudWatchSourceStatus(plan, "skipped",
				"本次查询已达整体时间预算，该来源未查询；可重试或缩小时间范围。"))
			continue
		}
		status, evidence := m.lookupOneCloudWatchSource(budgetCtx, parser, plan, requestID, from, to)
		out.Sources = append(out.Sources, status)
		if len(evidence) > 0 {
			out.Evidence = append(out.Evidence, logChainCloudWatchEvidence{Source: plan.Source, Evidence: evidence})
		}
	}
	return out
}

func (m *Monitor) lookupOneCloudWatchSource(ctx context.Context, parser *cloudWatchEvidenceParser, plan logChainCloudWatchPlan,
	requestID string, from, to time.Time) (logChainCloudWatchSourceStatus, []cloudWatchStructuredEvidence) {
	result, err := m.cloudWatchLogs.filter(ctx, cloudWatchFilterRequest{
		Kind: plan.Kind, From: from, To: to, Value: requestID, Limit: logChainCloudWatchLimit,
	})
	if err != nil {
		return m.cloudWatchSourceStatus(plan, cloudWatchLookupStatus(err), plan.Note), nil
	}
	batch := m.cloudWatchLogs.parseFilterEvidence(parser, result)
	status := m.cloudWatchSourceStatus(plan, "empty", plan.Note)
	status.Events, status.Parsed, status.ParseFailed = len(result.Events), batch.Parsed, batch.ParseFailed
	status.Truncated = result.Truncated
	if len(batch.Evidence) > 0 {
		status.Status = "found"
	}
	if len(result.Events) > 0 && len(batch.Evidence) == 0 {
		// 有事件但一条都没解析成功：这是格式漂移，不是"没有这条请求"。
		status.Status = "unavailable"
		status.Note = plan.Note + " 该窗口内有事件但格式无法识别，已计入解析失败。"
	}
	return status, batch.Evidence
}

// cloudWatchLookupStatus 把阶段 1 的错误分类映射成页面状态。
// 只输出闭集取值，绝不透传 AWS 原始错误消息（可能含查询输入）。
func cloudWatchLookupStatus(err error) string {
	switch cloudWatchLogsErrorKindOf(err) {
	case cwLogsErrDisabled:
		return "disabled"
	case cwLogsErrInvalid:
		return "invalid"
	case cwLogsErrAccessDenied:
		return "access_denied"
	case cwLogsErrThrottled:
		return "throttled"
	case cwLogsErrTimeout:
		return "timeout"
	case cwLogsErrCancelled:
		return "cancelled"
	default:
		return "unavailable"
	}
}

func (m *Monitor) cloudWatchSourceStatus(plan logChainCloudWatchPlan, status, note string) logChainCloudWatchSourceStatus {
	source, err := cloudWatchLogSourceByID(plan.Source)
	out := logChainCloudWatchSourceStatus{Source: plan.Source, Status: status, Linkage: plan.Linkage, Note: note}
	if err == nil {
		out.LogGroup, out.Region, out.Sensitive = source.LogGroup, source.Region, source.Sensitive
	}
	return out
}
