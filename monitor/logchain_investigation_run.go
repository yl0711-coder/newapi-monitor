package monitor

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const logChainInvestigationCandidateLimit = 20

var logChainInvestigationSourceOrder = []cloudWatchLogSourceID{
	cwSourceCloudFrontAccess,
	cwSourceCloudFrontDiagnostic,
	cwSourceWorkerNginx,
	cwSourceWorkerNewAPI,
	cwSourceMaster,
	cwSourceRDSError,
	cwSourceRDSSlowQuery,
}

type logChainInvestigationRunner struct {
	m        *Monitor
	parser   *cloudWatchEvidenceParser
	mu       sync.Mutex
	statuses map[cloudWatchLogSourceID]logChainCloudWatchSourceStatus
	evidence map[cloudWatchLogSourceID][]cloudWatchStructuredEvidence
	queries  int
	bytes    uint64
}

func (m *Monitor) executeLogChainInvestigation(ctx context.Context, id string, in logChainInvestigationInput) logChainInvestigationResult {
	result := logChainInvestigationResult{
		InvestigationID: id, Status: "running", Scope: m.investigationScopeView(in), StartedAt: time.Now().Unix(),
		Requests: []LogChainRow{}, Timeline: []logChainInvestigationTimelineEvent{},
		SourceStatus: []logChainCloudWatchSourceStatus{}, Evidence: []logChainCloudWatchEvidence{},
		BlindSpots: []string{
			"CloudFront Request ID 与 NewAPI Request ID 尚未统一透传；跨入口与应用层的关联最多只能标为高置信相关或候选，不能伪装成精确关联。",
			"CloudFront 标准日志存在投递延迟；暂时查不到只表示当前没有入口证据，不等于请求没有到达。",
			"页面只返回结构化字段和脱敏证据引用，不读取 Authorization、Cookie、API Key、提示词或响应正文。",
		},
	}
	parser, err := m.cloudWatchEvidenceParser(in.IncludeSensitiveDiagnostics)
	if err != nil {
		result.Status = "failed"
		result.Summary = logChainInvestigationSummary{Classification: "telemetry_unavailable", EvidenceLevel: "unavailable", Conclusion: "证据脱敏配置不可用，已拒绝读取 CloudWatch 日志", CustomerImpact: "unknown"}
		result.BlindSpots = append(result.BlindSpots, "CloudWatch 证据脱敏密钥不可用。")
		return result
	}
	runner := &logChainInvestigationRunner{m: m, parser: parser, statuses: make(map[cloudWatchLogSourceID]logChainCloudWatchSourceStatus), evidence: make(map[cloudWatchLogSourceID][]cloudWatchStructuredEvidence)}

	rows, truncated, candidateErrs := m.investigationCandidates(ctx, in)
	result.Requests, result.CandidateTruncated = rows, truncated
	result.BlindSpots = append(result.BlindSpots, candidateErrs...)
	requestIDs := investigationRequestIDs(in.NewAPIRequestID, rows)
	paths := investigationPaths(in.Path, rows)

	cloudFrontIDs := runner.queryCloudFront(ctx, in, paths)

	if len(requestIDs) > 0 {
		for _, requestID := range requestIDs {
			runner.filter(ctx, cwSourceWorkerNginx, cwFilterWorkerNginxID, requestID, in.From, in.To, "exact", "按 NewAPI Request ID 精确匹配 Nginx 日志。", nil)
			runner.filter(ctx, cwSourceWorkerNewAPI, cwFilterWorkerNewAPIID, requestID, in.From, in.To, "exact", "按 NewAPI Request ID 精确匹配应用日志。", nil)
		}
	} else {
		if len(paths) > 0 {
			for _, path := range paths {
				runner.insights(ctx, cwSourceWorkerNginx, cwQueryWorkerPath, path, in.From, in.To, "ambiguous", "没有 Request ID，按路径和窄时间窗寻找 Nginx 候选。", nil)
			}
		} else {
			runner.skip(cwSourceWorkerNginx, "ambiguous", "没有 NewAPI Request ID 或路径，未查询 Nginx。")
		}
		if in.UserID > 0 || in.Model != "" || in.Group != "" {
			match := func(e cloudWatchStructuredEvidence) bool {
				if in.UserID > 0 && (e.UserID == nil || *e.UserID != in.UserID) {
					return false
				}
				if in.Model != "" && e.Model != in.Model {
					return false
				}
				return in.Group == "" || e.Group == in.Group
			}
			runner.filter(ctx, cwSourceWorkerNewAPI, cwFilterWorkerFixedErrors, "", in.From, in.To, "ambiguous", "没有 Request ID，只在固定错误集合中按客户/模型/分组筛选候选。", match)
		} else {
			runner.skip(cwSourceWorkerNewAPI, "ambiguous", "没有 Request ID、客户、模型或分组，未执行宽泛应用日志扫描。")
		}
	}

	runner.queryCloudFrontDiagnostic(ctx, in, cloudFrontIDs)

	allEvidence := runner.allEvidence()
	databaseSignal := investigationHasDatabaseSignal(rows, allEvidence)
	masterSignal := investigationHasMasterSignal(in, allEvidence)
	if databaseSignal {
		runner.filter(ctx, cwSourceRDSError, cwFilterRDSErrors, "", in.From, in.To, "inferred", "前序证据指向数据库，按同一时间窗补查 RDS 错误；它不是单请求精确关联。", nil)
		runner.filter(ctx, cwSourceRDSSlowQuery, cwFilterRDSSlowQueries, "", in.From, in.To, "inferred", "前序证据指向数据库，按同一时间窗补查慢查询；它不是单请求精确关联。", nil)
	} else {
		runner.skip(cwSourceRDSError, "skipped", "前序证据未指向数据库，本次未触发 RDS 错误日志查询。")
		runner.skip(cwSourceRDSSlowQuery, "skipped", "前序证据未指向数据库，本次未触发 RDS 慢查询日志查询。")
	}
	if masterSignal {
		runner.filter(ctx, cwSourceMaster, cwFilterMasterFixedErrors, "", in.From, in.To, "inferred", "排障用途或前序证据指向后台任务，按同一时间窗补查 Master。", nil)
	} else {
		runner.skip(cwSourceMaster, "skipped", "普通客户 API 请求不经过 Master，本次未触发查询。")
	}

	runner.refineCloudFrontLinkage(rows, in)
	result.SourceStatus = runner.orderedStatuses()
	result.Evidence = runner.groupedEvidence()
	result.Timeline = m.investigationTimeline(rows, result.Evidence, result.SourceStatus, in)
	result.Summary = summarizeInvestigation(rows, runner.allEvidence(), result.SourceStatus, in)
	result.Cost = logChainInvestigationCost{Queries: runner.queries, BytesScanned: runner.bytes}
	result.SensitiveDiagnosticsRead = sourceWasRead(result.SourceStatus, cwSourceCloudFrontDiagnostic)
	result.Status = investigationCompletionStatus(ctx, result)
	if result.CandidateTruncated {
		result.BlindSpots = append(result.BlindSpots, "业务候选超过单任务安全上限，只对本次返回的候选补证据；请缩小时间或筛选条件。")
	}
	return result
}

// recheckPendingCloudFront 只复查投递延迟来源，不重复读取 NewAPI、Worker、RDS
// 或 Master。旧证据保持不变，CloudFront 两个来源用本轮结果原子替换。
func (m *Monitor) recheckPendingCloudFront(ctx context.Context, task *logChainInvestigationTask) logChainInvestigationResult {
	m.investigationMu.Lock()
	current := m.investigationTasks[task.ID]
	if current == nil || current.Result == nil {
		m.investigationMu.Unlock()
		return logChainInvestigationResult{InvestigationID: task.ID, Status: "failed"}
	}
	result := *current.Result
	m.investigationMu.Unlock()

	parser, err := m.cloudWatchEvidenceParser(task.Input.IncludeSensitiveDiagnostics)
	if err != nil {
		result.Status = "partial"
		result.BlindSpots = append(append([]string(nil), result.BlindSpots...), "CloudFront 延迟复查因脱敏配置不可用而失败。")
		return result
	}
	runner := &logChainInvestigationRunner{m: m, parser: parser, statuses: make(map[cloudWatchLogSourceID]logChainCloudWatchSourceStatus), evidence: make(map[cloudWatchLogSourceID][]cloudWatchStructuredEvidence)}
	paths := investigationPaths(task.Input.Path, result.Requests)
	cloudFrontIDs := runner.queryCloudFront(ctx, task.Input, paths)
	runner.queryCloudFrontDiagnostic(ctx, task.Input, cloudFrontIDs)
	runner.refineCloudFrontLinkage(result.Requests, task.Input)

	statuses := append([]logChainCloudWatchSourceStatus(nil), result.SourceStatus...)
	for _, source := range []cloudWatchLogSourceID{cwSourceCloudFrontAccess, cwSourceCloudFrontDiagnostic} {
		if next, ok := runner.status(source); ok {
			statuses = replaceInvestigationSourceStatus(statuses, next)
		}
	}
	evidence := append([]logChainCloudWatchEvidence(nil), result.Evidence...)
	for _, source := range []cloudWatchLogSourceID{cwSourceCloudFrontAccess, cwSourceCloudFrontDiagnostic} {
		evidence = replaceInvestigationEvidence(evidence, source, runner.evidenceFor(source))
	}
	result.SourceStatus, result.Evidence = statuses, evidence
	result.Timeline = m.investigationTimeline(result.Requests, evidence, statuses, task.Input)
	result.Summary = summarizeInvestigation(result.Requests, flattenInvestigationEvidence(evidence), statuses, task.Input)
	result.Cost.Queries += runner.queries
	result.Cost.BytesScanned += runner.bytes
	result.Cost.CacheHit = false
	result.SensitiveDiagnosticsRead = sourceWasRead(statuses, cwSourceCloudFrontDiagnostic)
	result.Status = investigationCompletionStatus(ctx, result)
	return result
}

func (r *logChainInvestigationRunner) status(source cloudWatchLogSourceID) (logChainCloudWatchSourceStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, ok := r.statuses[source]
	return status, ok
}

func (r *logChainInvestigationRunner) evidenceFor(source cloudWatchLogSourceID) []cloudWatchStructuredEvidence {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cloudWatchStructuredEvidence(nil), r.evidence[source]...)
}

func replaceInvestigationSourceStatus(statuses []logChainCloudWatchSourceStatus, next logChainCloudWatchSourceStatus) []logChainCloudWatchSourceStatus {
	out := append([]logChainCloudWatchSourceStatus(nil), statuses...)
	for index := range out {
		if out[index].Source == next.Source {
			out[index] = next
			return out
		}
	}
	return append(out, next)
}

func replaceInvestigationEvidence(groups []logChainCloudWatchEvidence, source cloudWatchLogSourceID, items []cloudWatchStructuredEvidence) []logChainCloudWatchEvidence {
	out := make([]logChainCloudWatchEvidence, 0, len(groups)+1)
	replaced := false
	for _, group := range groups {
		if group.Source != source {
			out = append(out, group)
			continue
		}
		replaced = true
		if len(items) > 0 {
			out = append(out, logChainCloudWatchEvidence{Source: source, Evidence: items})
		}
	}
	if !replaced && len(items) > 0 {
		out = append(out, logChainCloudWatchEvidence{Source: source, Evidence: items})
	}
	return out
}

func flattenInvestigationEvidence(groups []logChainCloudWatchEvidence) []cloudWatchStructuredEvidence {
	var out []cloudWatchStructuredEvidence
	for _, group := range groups {
		out = append(out, group.Evidence...)
	}
	return out
}

func (m *Monitor) investigationCandidates(ctx context.Context, in logChainInvestigationInput) ([]LogChainRow, bool, []string) {
	if m.prodDB == nil {
		return nil, false, []string{"NewAPI 生产只读库未连接，无法取得业务候选；CloudWatch 证据仍会继续查询。"}
	}
	if in.NewAPIRequestID == "" && in.UserID == 0 && in.Model == "" && in.Group == "" {
		return nil, false, nil
	}
	scope := logChainScope{
		FromTs: in.From.Unix(), ToTs: in.To.Unix(), UserID: in.UserID,
		Model: in.Model, Group: in.Group, Endpoint: in.Path, RequestID: in.NewAPIRequestID,
		Asc: true, Limit: logChainInvestigationCandidateLimit,
	}
	rows, more, err := m.queryLogChain(ctx, scope, nil)
	if err != nil {
		return nil, false, []string{"NewAPI 业务候选查询失败；CloudWatch 各来源仍按独立状态返回。"}
	}
	blind := make([]string, 0, 3)
	if err := m.attachChannelSnaps(ctx, rows); err != nil {
		blind = append(blind, "渠道快照补全失败，业务候选仍可用。")
	}
	if err := m.attachNginxEvidence(ctx, rows); err != nil {
		blind = append(blind, "旧 Nginx 证据补全失败，不影响 CloudWatch 查询。")
	}
	if m.cfg.UpstreamErrorLogSyncEnabled {
		if matches, err := m.correlateUpstreamErrors(ctx, rows); err == nil {
			for i := range rows {
				if match, ok := matches[rows[i].ID]; ok {
					rows[i].UpstreamMatch = &match
				}
			}
		} else {
			blind = append(blind, "上游错误日志关联失败，业务候选与 CloudWatch 证据仍可用。")
		}
	}
	logChainRefreshFaults(rows)
	return rows, more, blind
}

func investigationRequestIDs(explicit string, rows []LogChainRow) []string {
	out := make([]string, 0, len(rows)+1)
	if explicit != "" {
		out = append(out, explicit)
	}
	for _, row := range rows {
		if row.RequestID != "" {
			out = appendUniqueBounded(out, row.RequestID, logChainInvestigationCandidateLimit)
		}
	}
	return out
}

func investigationPaths(explicit string, rows []LogChainRow) []string {
	out := make([]string, 0, 3)
	if explicit != "" {
		out = append(out, explicit)
	}
	for _, row := range rows {
		if row.RequestPath != "" {
			out = appendUniqueBounded(out, row.RequestPath, 3)
		}
	}
	return out
}

func (r *logChainInvestigationRunner) queryCloudFront(ctx context.Context, in logChainInvestigationInput, paths []string) []string {
	cloudFrontIDs := make([]string, 0, 4)
	if in.CloudFrontRequestID != "" {
		r.filter(ctx, cwSourceCloudFrontAccess, cwFilterCloudFrontRequestID, in.CloudFrontRequestID,
			in.From, in.To, "exact", "按明确的 CloudFront Request ID 精确查询。", nil)
		return append(cloudFrontIDs, in.CloudFrontRequestID)
	}
	if len(paths) == 0 {
		r.skip(cwSourceCloudFrontAccess, "ambiguous", "没有 CloudFront Request ID 或请求路径，无法安全构造入口层候选查询。")
		return cloudFrontIDs
	}
	for _, path := range paths {
		keep := func(e cloudWatchStructuredEvidence) bool {
			return in.Status == 0 || e.Status != nil && *e.Status == in.Status
		}
		rowsResult := r.insights(ctx, cwSourceCloudFrontAccess, cwQueryCloudFrontPath, path,
			in.From, in.To, "ambiguous", "按路径、完成时间、状态码和耗时寻找入口候选；候选不唯一时不自动选一条。", keep)
		for _, row := range rowsResult {
			if in.Status != 0 && strings.TrimSpace(cwField(row, "sc-status")) != strconv.Itoa(in.Status) {
				continue
			}
			if raw := strings.TrimSpace(cwField(row, "x-edge-request-id")); raw != "" {
				cloudFrontIDs = appendUniqueBounded(cloudFrontIDs, raw, 4)
			}
		}
	}
	return cloudFrontIDs
}

func (r *logChainInvestigationRunner) queryCloudFrontDiagnostic(ctx context.Context, in logChainInvestigationInput, cloudFrontIDs []string) {
	if !in.IncludeSensitiveDiagnostics {
		r.skip(cwSourceCloudFrontDiagnostic, "skipped", "本次未请求客户网络诊断日志。")
		return
	}
	if in.ClientIP != "" {
		r.filter(ctx, cwSourceCloudFrontDiagnostic, cwFilterCloudFrontDiagnosticIP, in.ClientIP, in.From, in.To, "exact", "按客户出口 IP 精确筛选诊断日志；结果只返回 IP HMAC 与网络摘要。", nil)
		return
	}
	if len(cloudFrontIDs) == 0 {
		r.skip(cwSourceCloudFrontDiagnostic, "ambiguous", "请求了客户网络诊断，但当前没有可用于精确查询的 CloudFront Request ID 或客户 IP。")
		return
	}
	for _, requestID := range cloudFrontIDs {
		r.filter(ctx, cwSourceCloudFrontDiagnostic, cwFilterCloudFrontDiagnosticRequestID, requestID, in.From, in.To, "exact", "按 CloudFront Request ID 补查客户网络诊断，原始 IP 与 User-Agent 不回传。", nil)
	}
}

func appendUniqueBounded(values []string, value string, maximum int) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	if len(values) < maximum {
		values = append(values, value)
	}
	return values
}

func (r *logChainInvestigationRunner) filter(ctx context.Context, source cloudWatchLogSourceID, kind cloudWatchFixedFilterKind, value string,
	from, to time.Time, linkage, note string, keep func(cloudWatchStructuredEvidence) bool) {
	if ctx.Err() != nil {
		r.mergeStatus(r.m.cloudWatchSourceStatus(logChainCloudWatchPlan{Source: source, Linkage: linkage}, "skipped", "任务总预算已用尽，该来源未查询。"), true)
		return
	}
	r.mu.Lock()
	r.queries++
	r.mu.Unlock()
	result, err := r.m.cloudWatchLogs.filter(ctx, cloudWatchFilterRequest{Kind: kind, From: from, To: to, Value: value, Limit: logChainCloudWatchLimit})
	plan := logChainCloudWatchPlan{Source: source, Linkage: linkage, Note: note}
	if err != nil {
		r.mergeStatus(r.m.cloudWatchSourceStatus(plan, cloudWatchLookupStatus(err), note), false)
		return
	}
	batch := r.m.cloudWatchLogs.parseFilterEvidence(r.parser, result)
	evidence := batch.Evidence
	if keep != nil {
		filtered := evidence[:0]
		for _, item := range evidence {
			if keep(item) {
				filtered = append(filtered, item)
			}
		}
		evidence = filtered
	}
	status := r.m.cloudWatchSourceStatus(plan, "empty", note)
	status.Events, status.Parsed, status.ParseFailed, status.Truncated = len(result.Events), uint64(len(evidence)), batch.ParseFailed, result.Truncated
	if len(evidence) > 0 {
		status.Status = "found"
	}
	if len(result.Events) > 0 && batch.Parsed == 0 {
		status.Status = "unavailable"
		status.Note = note + " 查询命中事件但格式无法识别，已计入解析失败。"
	}
	r.mergeStatus(status, false)
	r.addEvidence(source, evidence)
}

func (r *logChainInvestigationRunner) insights(ctx context.Context, source cloudWatchLogSourceID, kind cloudWatchFixedQueryKind, value string,
	from, to time.Time, linkage, note string, keep func(cloudWatchStructuredEvidence) bool) []map[string]string {
	if ctx.Err() != nil {
		r.mergeStatus(r.m.cloudWatchSourceStatus(logChainCloudWatchPlan{Source: source, Linkage: linkage}, "skipped", "任务总预算已用尽，该来源未查询。"), true)
		return nil
	}
	r.mu.Lock()
	r.queries++
	r.mu.Unlock()
	result, err := r.m.cloudWatchLogs.insights(ctx, cloudWatchInsightsRequest{Kind: kind, From: from, To: to, Value: value, Limit: logChainCloudWatchLimit})
	plan := logChainCloudWatchPlan{Source: source, Linkage: linkage, Note: note}
	if err != nil {
		r.mergeStatus(r.m.cloudWatchSourceStatus(plan, cloudWatchLookupStatus(err), note), false)
		return nil
	}
	r.mu.Lock()
	r.bytes += result.BytesScanned
	r.mu.Unlock()
	batch := r.m.cloudWatchLogs.parseInsightsEvidence(r.parser, result)
	evidence := batch.Evidence
	if keep != nil {
		filtered := evidence[:0]
		for _, item := range evidence {
			if keep(item) {
				filtered = append(filtered, item)
			}
		}
		evidence = filtered
	}
	status := r.m.cloudWatchSourceStatus(plan, "empty", note)
	status.Events, status.Parsed, status.ParseFailed, status.Truncated = len(result.Rows), uint64(len(evidence)), batch.ParseFailed, result.Truncated
	if len(evidence) > 0 {
		status.Status = "found"
		if linkage == "ambiguous" && len(evidence) == 1 {
			status.Linkage = "correlated"
		}
	}
	if len(result.Rows) > 0 && batch.Parsed == 0 {
		status.Status = "unavailable"
		status.Note = note + " 查询命中事件但格式无法识别，已计入解析失败。"
	}
	r.mergeStatus(status, false)
	r.addEvidence(source, evidence)
	return result.Rows
}

func (r *logChainInvestigationRunner) skip(source cloudWatchLogSourceID, linkage, note string) {
	plan := logChainCloudWatchPlan{Source: source, Linkage: linkage, Note: note}
	r.mergeStatus(r.m.cloudWatchSourceStatus(plan, "skipped", note), false)
}

func (r *logChainInvestigationRunner) mergeStatus(next logChainCloudWatchSourceStatus, forcedPartial bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.statuses[next.Source]
	if !exists {
		next.Partial = forcedPartial
		r.statuses[next.Source] = next
		return
	}
	current.Events += next.Events
	current.Parsed += next.Parsed
	current.ParseFailed += next.ParseFailed
	current.Truncated = current.Truncated || next.Truncated
	current.Partial = current.Partial || forcedPartial
	if current.Note != next.Note && next.Note != "" {
		current.Note = strings.TrimSpace(current.Note + " " + next.Note)
	}
	if next.Status == "found" {
		if current.Status != "found" && current.Status != "empty" && current.Status != "skipped" {
			current.Partial = true
		}
		current.Status = "found"
		if current.Linkage == "ambiguous" && next.Linkage == "correlated" {
			current.Linkage = "correlated"
		}
	} else if next.Status == "empty" {
		if current.Status == "skipped" {
			current.Status = "empty"
		} else if current.Status != "empty" && current.Status != "found" {
			current.Partial = true
		}
	} else if next.Status != "skipped" {
		if current.Status == "found" || current.Status == "empty" {
			current.Partial = true
		} else {
			current.Status = next.Status
		}
	}
	r.statuses[next.Source] = current
}

func (r *logChainInvestigationRunner) addEvidence(source cloudWatchLogSourceID, evidence []cloudWatchStructuredEvidence) {
	if len(evidence) == 0 {
		return
	}
	r.mu.Lock()
	r.evidence[source] = append(r.evidence[source], evidence...)
	r.mu.Unlock()
}

func (r *logChainInvestigationRunner) allEvidence() []cloudWatchStructuredEvidence {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []cloudWatchStructuredEvidence
	for _, source := range logChainInvestigationSourceOrder {
		out = append(out, r.evidence[source]...)
	}
	return out
}

func (r *logChainInvestigationRunner) orderedStatuses() []logChainCloudWatchSourceStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]logChainCloudWatchSourceStatus, 0, len(logChainInvestigationSourceOrder))
	for _, source := range logChainInvestigationSourceOrder {
		status, ok := r.statuses[source]
		if !ok {
			status = r.m.cloudWatchSourceStatus(logChainCloudWatchPlan{Source: source, Linkage: "skipped"}, "skipped", "本次任务未触发该来源。")
		}
		out = append(out, status)
	}
	return out
}

func (r *logChainInvestigationRunner) groupedEvidence() []logChainCloudWatchEvidence {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]logChainCloudWatchEvidence, 0, len(r.evidence))
	for _, source := range logChainInvestigationSourceOrder {
		items := r.evidence[source]
		if len(items) > 0 {
			out = append(out, logChainCloudWatchEvidence{Source: source, Evidence: append([]cloudWatchStructuredEvidence(nil), items...)})
		}
	}
	return out
}

func (r *logChainInvestigationRunner) refineCloudFrontLinkage(rows []LogChainRow, in logChainInvestigationInput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, ok := r.statuses[cwSourceCloudFrontAccess]
	if !ok || status.Linkage == "exact" || status.Status != "found" {
		return
	}
	levels := cloudFrontEvidenceLevels(rows, r.evidence[cwSourceCloudFrontAccess], in)
	correlated := 0
	for _, level := range levels {
		if level == "correlated" {
			correlated++
		}
	}
	if correlated == 1 {
		status.Linkage = "correlated"
		status.Note += " 仅一条候选同时满足完成时间、路径、状态码/耗时约束，标为高置信相关；仍非统一 ID 精确关联。"
	}
	r.statuses[cwSourceCloudFrontAccess] = status
}

func investigationHasDatabaseSignal(rows []LogChainRow, evidence []cloudWatchStructuredEvidence) bool {
	for _, item := range evidence {
		if item.Category == "database_error" || item.Kind == cwEvidenceRDSError || item.Kind == cwEvidenceRDSSlowQuery {
			return true
		}
	}
	for _, row := range rows {
		lower := strings.ToLower(row.Content + " " + row.EndError)
		for _, fragment := range []string{"database", "mysql", "sql error", "deadlock", "too many connections", "server has gone away"} {
			if strings.Contains(lower, fragment) {
				return true
			}
		}
	}
	return false
}

func investigationHasMasterSignal(in logChainInvestigationInput, evidence []cloudWatchStructuredEvidence) bool {
	switch investigationPurposeClass(in.Purpose) {
	case "background_task", "channel_test", "subscription", "email":
		return true
	}
	for _, item := range evidence {
		if item.Category == "worker_drain_timeout" {
			return true
		}
	}
	return false
}

func (m *Monitor) investigationTimeline(rows []LogChainRow, grouped []logChainCloudWatchEvidence, statuses []logChainCloudWatchSourceStatus, in logChainInvestigationInput) []logChainInvestigationTimelineEvent {
	linkage := make(map[cloudWatchLogSourceID]string, len(statuses))
	complete := make(map[cloudWatchLogSourceID]bool, len(statuses))
	for _, status := range statuses {
		linkage[status.Source] = status.Linkage
		complete[status.Source] = status.Status == "found" && !status.Partial && status.ParseFailed == 0 && !status.Truncated
	}
	var cloudFrontItems []cloudWatchStructuredEvidence
	for _, group := range grouped {
		if group.Source == cwSourceCloudFrontAccess {
			cloudFrontItems = group.Evidence
			break
		}
	}
	cloudFrontLevels := cloudFrontEvidenceLevels(rows, cloudFrontItems, in)
	out := make([]logChainInvestigationTimelineEvent, 0, len(rows)+16)
	for _, row := range rows {
		fact := "NewAPI 业务日志记录到" + row.TypeName
		if row.Fault != "" {
			fact += "；疑似责任方 " + row.Fault + "（" + row.FaultConfidence + "）"
		}
		out = append(out, logChainInvestigationTimelineEvent{
			EventMS: row.CreatedAt * 1000, Node: "NewAPI 业务记录", Source: cloudWatchLogSourceID("newapi_database"), Fact: fact,
			EvidenceRef: m.investigationDigest("newapi-log-row", strconv.FormatInt(row.ID, 10)),
			RequestRef:  m.investigationDigest("oneapi-request-id", row.RequestID), EvidenceLevel: "exact", Complete: true,
		})
	}
	for _, group := range grouped {
		for _, item := range group.Evidence {
			level := linkage[group.Source]
			if item.Source == cwSourceCloudFrontAccess && level != "exact" {
				level = cloudFrontLevels[item.EventRef]
			}
			if level == "" || level == "skipped" {
				level = "inferred"
			}
			out = append(out, logChainInvestigationTimelineEvent{
				EventMS: item.EventMS, Node: investigationTimelineNode(item.Kind), Source: group.Source, Fact: item.Summary,
				EvidenceRef: item.EventRef, RequestRef: item.OneAPIIDHMAC, EvidenceLevel: level, Complete: complete[group.Source],
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EventMS != out[j].EventMS {
			return out[i].EventMS < out[j].EventMS
		}
		return out[i].Node < out[j].Node
	})
	return out
}

// cloudFrontEvidenceLevels 用完成时间、路径、显式状态码和耗时共同筛候选。
// 只有唯一最高分且至少满足两类约束时才标 correlated；其余一律 ambiguous。
func cloudFrontEvidenceLevels(rows []LogChainRow, evidence []cloudWatchStructuredEvidence, in logChainInvestigationInput) map[string]string {
	out := make(map[string]string, len(evidence))
	if in.CloudFrontRequestID != "" {
		for _, item := range evidence {
			out[item.EventRef] = "exact"
		}
		return out
	}
	if len(evidence) == 1 && len(rows) == 0 {
		out[evidence[0].EventRef] = "correlated"
		return out
	}
	type scored struct {
		ref   string
		score int
	}
	scores := make([]scored, 0, len(evidence))
	best := -1
	for _, item := range evidence {
		baseScore := 0
		if in.Status > 0 && item.Status != nil && *item.Status == in.Status {
			baseScore++
		}
		score := baseScore
		for _, row := range rows {
			candidate := baseScore
			if absInt64(item.EventMS-row.CreatedAt*1000) <= 5000 {
				candidate++
			}
			if row.RequestPath != "" && cwRoute(row.RequestPath) == item.Route {
				candidate++
			}
			if row.UseTime > 0 && item.RequestMS != nil {
				expected := row.UseTime * 1000
				tolerance := expected / 2
				if tolerance < 2000 {
					tolerance = 2000
				}
				if absInt64(*item.RequestMS-expected) <= tolerance {
					candidate++
				}
			}
			if candidate > score {
				score = candidate
			}
		}
		scores = append(scores, scored{ref: item.EventRef, score: score})
		if score > best {
			best = score
		}
	}
	bestCount := 0
	for _, candidate := range scores {
		if candidate.score == best {
			bestCount++
		}
	}
	for _, candidate := range scores {
		out[candidate.ref] = "ambiguous"
		if best >= 2 && bestCount == 1 && candidate.score == best {
			out[candidate.ref] = "correlated"
		}
	}
	return out
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func investigationTimelineNode(kind cloudWatchEvidenceKind) string {
	switch kind {
	case cwEvidenceCloudFrontAccess:
		return "CloudFront 接收与回传"
	case cwEvidenceCloudFrontDiagnostic:
		return "CloudFront 客户网络诊断"
	case cwEvidenceNginxAccess, cwEvidenceNginxError:
		return "Worker Nginx"
	case cwEvidenceNewAPIError:
		return "Worker NewAPI"
	case cwEvidenceMasterError:
		return "Master 后台任务"
	case cwEvidenceRDSError:
		return "RDS 错误"
	case cwEvidenceRDSSlowQuery:
		return "RDS 慢查询"
	default:
		return "未知节点"
	}
}

func summarizeInvestigation(rows []LogChainRow, evidence []cloudWatchStructuredEvidence, statuses []logChainCloudWatchSourceStatus, in logChainInvestigationInput) logChainInvestigationSummary {
	summary := logChainInvestigationSummary{Classification: "inconclusive", EvidenceLevel: "ambiguous", Conclusion: "已有证据不足以确定故障位置，请结合各来源状态与盲区继续排查", CustomerImpact: "unknown"}
	if in.NewAPIRequestID != "" || in.CloudFrontRequestID != "" {
		summary.CustomerImpact = "single_request"
	} else if len(rows) > 1 {
		summary.CustomerImpact = "multiple_candidates"
	}
	for _, item := range evidence {
		switch {
		case item.Kind == cwEvidenceCloudFrontAccess && item.Status != nil && *item.Status == 0:
			return logChainInvestigationSummary{Classification: "client_disconnect_at_edge", EvidenceLevel: "correlated", Conclusion: "CloudFront 记录到响应完成前断开；这只能确认下游连接结束，不能单凭这一条归责客户", CustomerImpact: summary.CustomerImpact}
		case item.Kind == cwEvidenceNginxAccess && item.Status != nil && *item.Status == 499:
			return logChainInvestigationSummary{Classification: "client_or_network_disconnect", EvidenceLevel: "exact", Conclusion: "Nginx 以同一 NewAPI Request ID 记录到 499，连接在向客户回传阶段结束", CustomerImpact: summary.CustomerImpact}
		case item.Category == "database_error":
			return logChainInvestigationSummary{Classification: "database_error", EvidenceLevel: evidenceLevelForSource(statuses, item.Source), Conclusion: "NewAPI 记录到数据库异常，并已按同一时间窗补查 RDS 证据", CustomerImpact: summary.CustomerImpact}
		case item.Category == "route_no_channel":
			return logChainInvestigationSummary{Classification: "platform_routing_rejection", EvidenceLevel: evidenceLevelForSource(statuses, item.Source), Conclusion: "请求到达 NewAPI，但当时没有可用渠道", CustomerImpact: summary.CustomerImpact}
		case item.FaultClass == "upstream_5xx":
			return logChainInvestigationSummary{Classification: "upstream_5xx", EvidenceLevel: evidenceLevelForSource(statuses, item.Source), Conclusion: "请求已到达平台，NewAPI 记录到上游 5xx", CustomerImpact: summary.CustomerImpact}
		case item.Category == "request_completed" && summary.Classification == "inconclusive":
			summary = logChainInvestigationSummary{Classification: "application_reached", EvidenceLevel: evidenceLevelForSource(statuses, item.Source), Conclusion: "请求已到达 Worker 与 NewAPI；是否完整交付仍需结合 Nginx/CloudFront 回传证据", CustomerImpact: summary.CustomerImpact}
		}
	}
	for _, row := range rows {
		switch row.Fault {
		case "upstream":
			return logChainInvestigationSummary{Classification: "upstream_error", EvidenceLevel: "exact", Conclusion: "NewAPI 业务日志记录到上游侧失败；请按页面显示的归因依据复核", CustomerImpact: summary.CustomerImpact}
		case "ours":
			return logChainInvestigationSummary{Classification: "platform_error", EvidenceLevel: "exact", Conclusion: "NewAPI 业务日志指向平台配置、路由或运行时问题", CustomerImpact: summary.CustomerImpact}
		case "downstream":
			return logChainInvestigationSummary{Classification: "downstream_disconnect", EvidenceLevel: "exact", Conclusion: "业务日志记录到下游连接中断；需结合时间与是否已输出判断是否为超时等待", CustomerImpact: summary.CustomerImpact}
		}
		if row.Type == 2 && len(row.AnomalyTags) == 0 {
			summary = logChainInvestigationSummary{Classification: "business_completed", EvidenceLevel: "exact", Conclusion: "NewAPI 业务日志记录到正常消费；完整回传情况仍以入口层证据为准", CustomerImpact: summary.CustomerImpact}
		}
	}
	return summary
}

func evidenceLevelForSource(statuses []logChainCloudWatchSourceStatus, source cloudWatchLogSourceID) string {
	for _, status := range statuses {
		if status.Source == source && status.Linkage != "" {
			return status.Linkage
		}
	}
	return "ambiguous"
}

func sourceWasRead(statuses []logChainCloudWatchSourceStatus, source cloudWatchLogSourceID) bool {
	for _, status := range statuses {
		if status.Source == source {
			return status.Status == "found" || status.Status == "empty"
		}
	}
	return false
}

func investigationCompletionStatus(ctx context.Context, result logChainInvestigationResult) string {
	failures, successful := 0, 0
	cloudFrontEmpty := false
	for _, status := range result.SourceStatus {
		switch status.Status {
		case "found", "empty":
			successful++
			if status.Source == cwSourceCloudFrontAccess && status.Status == "empty" {
				cloudFrontEmpty = true
			}
		case "skipped":
		default:
			failures++
		}
		if status.Partial {
			failures++
		}
	}
	if successful == 0 && failures > 0 && len(result.Requests) == 0 {
		return "failed"
	}
	if failures > 0 || ctx.Err() != nil {
		return "partial"
	}
	if cloudFrontEmpty {
		to, _ := time.Parse(time.RFC3339, result.Scope.ToUTC)
		if time.Since(to) < time.Hour {
			return "pending_delivery"
		}
	}
	return "complete"
}

// finalizePendingDeliveryRecheck 防止 60 分钟最后一轮复查后任务仍永久停在
// pending_delivery。最终仍没有 CloudFront 证据属于明确的数据缺口，因此收口为
// partial，而不是伪装成完整结果；页面会停止轮询并保留下一步说明。
func finalizePendingDeliveryRecheck(result logChainInvestigationResult, final bool) logChainInvestigationResult {
	if !final || result.Status != "pending_delivery" {
		return result
	}
	result.Status = "partial"
	result.BlindSpots = append(result.BlindSpots, "CloudFront 已完成 5、15、60 分钟三轮延迟复查，仍无可用入口证据；任务已停止自动复查，不能据此断定请求未到达。")
	return result
}
