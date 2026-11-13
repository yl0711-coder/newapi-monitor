package monitor

// Incident 只读查询接口（MON-003 B3）。
//
// 全部挂在 reports 组（noStoreSensitive + requireRole(roleAdmin)），全部只读
// 本地 SQLite，绝不触碰 m.prodDB、绝不修改 NewAPI。
//
// 异常监控的事故子页复用这些端点；不改变 B2/B3 判定或开关。

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// errIncidentBadCursor 是分页游标格式错误。
var errIncidentBadCursor = &incidentHTTPError{msg: "cursor 格式应为 <last_seen_ts>:<incident_id>"}

// incidentQueryGateMax 是闸门的硬上限。实际容量由配置决定，见下。
const incidentQueryGateMax = 64

// incidentQueryGate 并发闸门。
//
// 新增端点不得挤占既有报表的共享泳道，所以用自己的信号量而不是复用别人的。
//
// ★ 容量用计数器判定，不靠 channel 的 cap ★
//
// 以前的实现算出了 QueryGateCapacity 却只用 `incidentQueryGate <- struct{}{}`
// 入队，于是真实并发上限永远是 channel 容量 64，配置成 1 也会放进两个查询。
// 配置容量是运维的隔离手段，必须真实执行，所以改为显式计数。
var incidentQueryGate = struct {
	mu     sync.Mutex
	inUse  int
	waiter chan struct{}
}{waiter: make(chan struct{}, incidentQueryGateMax)}

// incidentEffectiveGateCapacity 返回生效容量。
func incidentEffectiveGateCapacity(cfg IncidentConfig) int {
	capacity := cfg.QueryGateCapacity
	if capacity <= 0 || capacity > incidentQueryGateMax {
		capacity = incidentQueryGateMax
	}
	return capacity
}

// acquireIncidentQueryGate 按配置容量与超时取闸门。
//
// 容量变更即时生效（每次获取都重读配置）：缩小容量不会强行中断已在执行的
// 查询，只让新的请求等待或超时——不伪造「已释放」。
func acquireIncidentQueryGate(ctx context.Context, cfg IncidentConfig) (func(), bool) {
	capacity := incidentEffectiveGateCapacity(cfg)
	timeout := time.Duration(cfg.GateTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		incidentQueryGate.mu.Lock()
		if incidentQueryGate.inUse < capacity {
			incidentQueryGate.inUse++
			incidentQueryGate.mu.Unlock()
			return releaseIncidentQueryGate, true
		}
		incidentQueryGate.mu.Unlock()

		// 等一个释放信号、超时或调用方取消。轮询间隔兜底，防止释放信号
		// 恰好在上面的检查与下面的等待之间丢失而永久阻塞。
		tick := time.NewTimer(20 * time.Millisecond)
		select {
		case <-incidentQueryGate.waiter:
			tick.Stop()
		case <-tick.C:
		case <-deadline.C:
			tick.Stop()
			return nil, false
		case <-ctx.Done():
			tick.Stop()
			return nil, false
		}
	}
}

func releaseIncidentQueryGate() {
	incidentQueryGate.mu.Lock()
	if incidentQueryGate.inUse > 0 {
		incidentQueryGate.inUse--
	}
	incidentQueryGate.mu.Unlock()
	// 非阻塞通知：没有等待者时直接丢弃，上面的轮询兜底保证不会漏醒。
	select {
	case incidentQueryGate.waiter <- struct{}{}:
	default:
	}
}

// incidentQueryContext 施加查询超时。
func incidentQueryContext(parent context.Context, cfg IncidentConfig) (context.Context, context.CancelFunc) {
	ms := cfg.QueryTimeoutMS
	if ms <= 0 {
		ms = 3000
	}
	return context.WithTimeout(parent, time.Duration(ms)*time.Millisecond)
}

// incidentAnomalyRateCaveat 是必须随异常率一起输出的口径说明。
const incidentAnomalyRateCaveat = "异常率口径为 (anomaly+failed)/(success+anomaly+failed)，" +
	"来自已有日志口径，**不代表客户端最终失败率**；阈值为待校准的 shadow 初值，需按 02.1 观察至少 7 天后校准。"

// serveIncidentList 列表。
func (m *Monitor) serveIncidentList(c *gin.Context) {
	cfg := LoadIncidentConfig()
	if !cfg.Enabled {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "note": "事故核心开关未开启"})
		return
	}
	release, ok := acquireIncidentQueryGate(c.Request.Context(), cfg)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "事故查询并发已满，请稍后重试"})
		return
	}
	defer release()
	ctx, cancel := incidentQueryContext(c.Request.Context(), cfg)
	defer cancel()

	limit := incidentClampLimit(c.Query("limit"), 50, 200)
	from, to, err := incidentParseRange(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if c.Query("consistent") == "1" {
		if from <= 0 || to <= from {
			c.JSON(http.StatusBadRequest, gin.H{"error": "事故页面查询需要明确起止时间"})
			return
		}
		if _, _, err := incidentParseListCursor(c.Query("cursor")); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		m.serveIncidentPageList(ctx, c, cfg, from, to, limit)
		return
	}

	q := m.storeDB.WithContext(ctx).Model(&Incident{})
	if from > 0 {
		q = q.Where("last_seen_ts >= ?", from)
	}
	if to > 0 {
		q = q.Where("first_seen_ts < ?", to)
	}

	// ★ 真正的游标分页，且相同时间戳也要稳定排序 ★
	//
	// 以前只有 limit/has_more，没有任何取下一页的手段：超过上限时无法可靠
	// 遍历全部记录。纯按 last_seen_ts 排序还不够——同一秒内多条事故的相对
	// 顺序不确定，翻页会重复或漏掉。所以用 (last_seen_ts, id) 复合游标，
	// id 作为唯一的决胜键保证全序。
	cursorTs, cursorID, err := incidentParseListCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if cursorTs > 0 {
		// 下一页 = 严格排在游标之后的记录（降序：时间更早，或同秒内 id 更大）。
		q = q.Where("last_seen_ts < ? OR (last_seen_ts = ? AND id > ?)",
			cursorTs, cursorTs, cursorID)
	}

	var rows []Incident
	if err := q.Order("last_seen_ts DESC, id ASC").Limit(limit + 1).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取事故列表失败"})
		return
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	nextCursor := ""
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		nextCursor = fmt.Sprintf("%d:%s", last.LastSeenTs, last.ID)
	}

	out := make([]gin.H, 0, len(rows))
	for _, r := range rows {
		out = append(out, m.incidentListItem(ctx, r))
	}
	c.JSON(http.StatusOK, gin.H{
		"enabled":   true,
		"incidents": out,
		"has_more":  hasMore,
		// 把 next_cursor 回传给 ?cursor= 即可取下一页；为空表示已到末页。
		"next_cursor":         nextCursor,
		"limit":               limit,
		"anomaly_rate_caveat": incidentAnomalyRateCaveat,
	})
}

// incidentParseListCursor 解析 "<last_seen_ts>:<id>" 复合游标。
//
// 空串表示首页。格式非法时明确报错，不静默当首页——那会让调用方以为
// 自己在翻页，实际一直在读第一页。
func incidentParseListCursor(raw string) (int64, string, error) {
	if raw == "" {
		return 0, "", nil
	}
	tsPart, idPart, found := strings.Cut(raw, ":")
	if !found || idPart == "" {
		return 0, "", errIncidentBadCursor
	}
	ts, err := strconv.ParseInt(tsPart, 10, 64)
	if err != nil || ts <= 0 {
		return 0, "", errIncidentBadCursor
	}
	return ts, idPart, nil
}

// incidentListItem 组装一条列表项。
//
// ★ scenario_pending 只从显式状态读取 ★
//
// 绝不写 severity == "" → scenario_pending = true 这种推导：引擎的更新路径
// 不同步 scenario，空 severity 也可能来自别的原因。唯一数据来源是
// IncidentIngestWindowState.ScenarioPending 与事故级 IncidentPendingSummary。
func (m *Monitor) incidentListItem(ctx context.Context, r Incident) gin.H {
	pending, reason, err := m.incidentScenarioPendingByID(ctx, r.ID)
	item := gin.H{
		"id":                  r.ID,
		"revision":            r.Revision,
		"status":              r.Status,
		"primary_fault_class": r.PrimaryFaultClass,
		"scenario":            r.Scenario,
		"severity":            r.Severity,
		"peak_severity":       r.PeakSeverity,
		"responsibility":      r.Responsibility,
		"evidence_level":      r.EvidenceLevel,
		"channel_scope":       r.ChannelScope,
		"channel_scope_label": IncidentChannelScope(r.ChannelScope).DisplayName(),
		"channel_id":          r.ChannelID,
		"model":               r.Model,
		"user_group":          r.UserGroup,
		"ingress_protocol":    r.IngressProtocol,
		"route":               r.Route,
		"first_seen_ts":       r.FirstSeenTs,
		"last_seen_ts":        r.LastSeenTs,
		"resolved_ts":         r.ResolvedTs,
		"cooldown_until_ts":   r.CooldownUntilTs,
		"scenario_pending":    pending,
		"pending_reason":      reason,
	}
	if err != nil {
		// 读不到不等于不需要确认：显式标为未知，不回落成 false。
		item["scenario_pending"] = nil
		item["pending_reason"] = ""
		item["scenario_pending_status"] = "unavailable"
		item["scenario_pending_error"] = "待确认状态读取失败，未知不等于无需确认"
	}
	if r.Severity == "" {
		// 空严重度不是第五种 SEV，也不是「正常」。显式说明不可据此处置。
		item["severity_reason"] = "该事故尚未定级（场景待确认），不可据此处置"
	}
	return item
}

// incidentScenarioPendingByID 按 IncidentID 读事故级待确认结论。
//
// ★ 必须按 IncidentID 读，且不能只依赖窗口表 ★
//
// 窗口行会被保留期清理删除，活跃事故的 scenario_pending 随之从 true 变成
// false、pending_reason 变空——待确认结论被清理抹掉了。所以结论在产生时
// 就持久化到事故级摘要（IncidentPendingSummary），这里优先读它。
//
// 也不能按 dedup_key 查窗口：同键的**其他**事故的窗口会冒充本事故的结论。
//
// 返回第三个值表示是否读取成功；失败时调用方必须显式报告不可用，
// 不能把读取失败当作「无需确认」。
func (m *Monitor) incidentScenarioPendingByID(ctx context.Context, incidentID string) (bool, string, error) {
	var sum IncidentPendingSummary
	res := m.storeDB.WithContext(ctx).
		Where("incident_id = ?", incidentID).Limit(1).Find(&sum)
	if res.Error != nil {
		return false, "", fmt.Errorf("读事故待确认摘要失败: %w", res.Error)
	}
	if res.RowsAffected > 0 && sum.ScenarioPending {
		return true, sum.PendingReason, nil
	}

	// 回落到窗口表：摘要尚未落盘的旧数据仍能读到结论。
	var row IncidentIngestWindowState
	res = m.storeDB.WithContext(ctx).
		Where("incident_id = ? AND scenario_pending = ?", incidentID, true).
		Limit(1).Find(&row)
	if res.Error != nil {
		return false, "", fmt.Errorf("读窗口待确认状态失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return false, "", nil
	}
	return true, row.PendingReason, nil
}

// serveIncidentDetail 单事故详情。观测与分段有界、可分页。
func (m *Monitor) serveIncidentDetail(c *gin.Context) {
	cfg := LoadIncidentConfig()
	if !cfg.Enabled {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "note": "事故核心开关未开启"})
		return
	}
	id := c.Query("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "缺少 id"})
		return
	}
	release, ok := acquireIncidentQueryGate(c.Request.Context(), cfg)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "事故查询并发已满，请稍后重试"})
		return
	}
	defer release()
	ctx, cancel := incidentQueryContext(c.Request.Context(), cfg)
	defer cancel()

	// A detail response must read its revision, frozen evidence and impact from
	// one snapshot, even when an ingestion transaction commits during this read.
	tx := m.storeDB.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		incidentRespondQueryFailure(c, "detail_snapshot", tx.Error)
		return
	}
	defer tx.Rollback()
	reader := &Monitor{storeDB: tx}
	reader.serveIncidentDetailSnapshot(ctx, c, id)
}

func (m *Monitor) serveIncidentDetailSnapshot(ctx context.Context, c *gin.Context, id string) {

	var inc Incident
	res := m.storeDB.WithContext(ctx).Where("id = ?", id).Limit(1).Find(&inc)
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取事故失败"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "事故不存在"})
		return
	}
	if expected := c.Query("revision"); expected != "" && expected != strconv.FormatInt(inc.Revision, 10) {
		c.JSON(http.StatusConflict, gin.H{"error": "该事故已更新，请重新查询列表后查看对应详情", "data_status": "snapshot_changed"})
		return
	}
	consistent := c.Query("consistent") == "1"
	evidenceSnapshot := ""
	item := m.incidentListItem(ctx, inc)
	if consistent {
		var err error
		item, err = m.incidentPageItem(ctx, inc)
		if err != nil {
			incidentRespondQueryFailure(c, "detail_item", err)
			return
		}
		if expected := c.Query("row_snapshot"); expected != "" && expected != item["row_snapshot"] {
			c.JSON(http.StatusConflict, gin.H{"error": "事故待确认状态已变化，请更新列表后查看详情", "data_status": "snapshot_changed"})
			return
		}
		evidenceSnapshot, err = m.incidentEvidenceFingerprint(ctx, inc)
		if err != nil {
			incidentRespondQueryFailure(c, "evidence_snapshot", err)
			return
		}
		if expected := c.Query("evidence_snapshot"); expected != "" && expected != evidenceSnapshot {
			c.JSON(http.StatusConflict, gin.H{"error": "事故证据已修订，请重新打开详情；当前已读取的证据保留", "data_status": "snapshot_changed"})
			return
		}
	}

	limit := incidentClampLimit(c.Query("limit"), 100, 500)
	cursor, _ := strconv.ParseInt(c.Query("cursor"), 10, 64)

	// ★ 分段要有真正的翻页能力 ★
	//
	// 以前 cursor 只作用于 observations，segments 永远从头取 limit 条：
	// has_more=true 也拿不到后面的分段。这里给它独立的 seq_no 游标。
	// seq_no 在一个事故内唯一且单调，所以排序稳定、翻页不重不漏。
	segCursor, _ := strconv.ParseInt(c.Query("segments_cursor"), 10, 64)
	segQuery := m.storeDB.WithContext(ctx).Where("incident_id = ?", id)
	if segCursor > 0 {
		segQuery = segQuery.Where("seq_no > ?", segCursor)
	}
	var segs []IncidentSegment
	if err := segQuery.Order("seq_no ASC").Limit(limit + 1).Find(&segs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取分段失败"})
		return
	}
	segHasMore := len(segs) > limit
	if segHasMore {
		segs = segs[:limit]
	}
	segNextCursor := int64(0)
	if segHasMore && len(segs) > 0 {
		segNextCursor = segs[len(segs)-1].SeqNo
	}

	obQuery := m.storeDB.WithContext(ctx).Model(&IncidentObservation{}).
		Where("incident_id = ?", id)
	obTs, obID, err := incidentParseListCursor(c.Query("observations_cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if obTs > 0 {
		obQuery = obQuery.Where("window_to_ts < ? OR (window_to_ts = ? AND id > ?)", obTs, obTs, obID)
	} else if cursor > 0 {
		obQuery = obQuery.Where("window_to_ts < ?", cursor)
	}
	var obs []IncidentObservation
	if err := obQuery.Order("window_to_ts DESC, id ASC").Limit(limit + 1).Find(&obs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取观测失败"})
		return
	}
	obHasMore := len(obs) > limit
	if obHasMore {
		obs = obs[:limit]
	}
	nextCursor := int64(0)
	nextCursorKey := ""
	if obHasMore && len(obs) > 0 {
		last := obs[len(obs)-1]
		nextCursor = last.WindowToTs
		nextCursorKey = fmt.Sprintf("%d:%s", last.WindowToTs, last.ID)
	}

	// 影响面 / 归档 / 修订的读取失败同样不得吞掉：它们失败时返回的空数组
	// 会被读成「无影响、无归档、无修订」。
	var impacts []IncidentImpact
	impactQuery := m.storeDB.WithContext(ctx).Where("incident_id = ?", id)
	if consistent && c.Query("impacts_cursor") != "" {
		impactQuery = impactQuery.Where("id > ?", c.Query("impacts_cursor"))
	}
	if err := impactQuery.
		Order("id ASC").Limit(201).Find(&impacts).Error; err != nil {
		incidentRespondQueryFailure(c, "impacts", err)
		return
	}
	impactTruncated := len(impacts) > 200
	if impactTruncated {
		impacts = impacts[:200]
	}
	impactNext := ""
	if impactTruncated {
		impactNext = impacts[len(impacts)-1].ID
	}

	var archived []IncidentImpactWindow
	archiveQuery := m.storeDB.WithContext(ctx).Where("incident_id = ? AND row_kind = ?", id, incidentImpactRowKindArchive)
	if consistent && c.Query("impacts_archived_cursor") != "" {
		archiveQuery = archiveQuery.Where("id > ?", c.Query("impacts_archived_cursor"))
	}
	if err := archiveQuery.
		Order("id ASC").Limit(201).Find(&archived).Error; err != nil {
		incidentRespondQueryFailure(c, "impacts_archived", err)
		return
	}
	archiveTruncated := len(archived) > 200
	if archiveTruncated {
		archived = archived[:200]
	}
	archiveNext := ""
	if archiveTruncated {
		archiveNext = archived[len(archived)-1].ID
	}
	archivedOut := make([]gin.H, 0, len(archived))
	for _, a := range archived {
		// 归档区间显示折叠条目与窗口数，不假装仍有明细。
		archivedOut = append(archivedOut, gin.H{
			"dimension": a.Dimension, "dim_value": a.DimValue,
			"archive_from_ts": a.ArchiveFromTs, "archive_through_ts": a.ArchiveThroughTs,
			"archived_window_count": a.ArchivedWindowCount,
			"detail_available":      false,
		})
	}

	var revisions []IncidentIngestRevision
	revisionQuery := m.storeDB.WithContext(ctx).Where("incident_id = ?", id)
	if consistent {
		if cursor := c.Query("revisions_cursor"); cursor != "" {
			revisionQuery = revisionQuery.Where("id > ?", cursor)
		}
		revisionQuery = revisionQuery.Order("id ASC")
	} else {
		revisionQuery = revisionQuery.Order("detected_at DESC, revision_seq DESC, id ASC")
	}
	if err := revisionQuery.Limit(limit + 1).Find(&revisions).Error; err != nil {
		incidentRespondQueryFailure(c, "revisions", err)
		return
	}
	revisionTruncated := len(revisions) > limit
	if revisionTruncated {
		revisions = revisions[:limit]
	}
	revisionNext := ""
	if revisionTruncated {
		revisionNext = revisions[len(revisions)-1].ID
	}
	revOut := make([]gin.H, 0, len(revisions))
	for _, rv := range revisions {
		revOut = append(revOut, gin.H{
			"kind": rv.Kind, "window_from_ts": rv.WindowFromTs, "window_to_ts": rv.WindowToTs,
			"revision_seq": rv.RevisionSeq,
			// 原判定依据与最新实测值并列输出，不隐藏分歧、不互相冒充。
			"old_sample_total":   rv.OldSampleTotal,
			"new_sample_total":   rv.NewSampleTotal,
			"old_anomaly_total":  rv.OldAnomalyTotal,
			"new_anomaly_total":  rv.NewAnomalyTotal,
			"old_success_total":  rv.OldSuccessTotal,
			"new_success_total":  rv.NewSuccessTotal,
			"old_anomaly_rate":   rv.OldAnomalyRate,
			"new_anomaly_rate":   rv.NewAnomalyRate,
			"old_window_state":   rv.OldWindowState,
			"new_window_state":   rv.NewWindowState,
			"old_coverage_proof": rv.OldCoverageProof,
			"new_coverage_proof": rv.CoverageProof,
			// 来源行已消失：样本/覆盖变了，但事故身份不变。
			"source_rows_missing": rv.SourceRowsMissing,
			// 修订只记录差异与影响面，绝不回改引擎已判定的观测与严重度。
			"engine_judgment_unchanged":   true,
			"revision_detect_lag_seconds": rv.RevisionDetectLagSeconds,
			"detected_at":                 rv.DetectedAt,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"enabled":           true,
		"incident":          item,
		"evidence_snapshot": evidenceSnapshot,
		"segments":          segs, "segments_has_more": segHasMore,
		// 把 segments_next_cursor 回传给 ?segments_cursor= 即可取下一页。
		"segments_next_cursor": segNextCursor,
		"observations":         obs, "observations_has_more": obHasMore,
		"observations_next_cursor":     nextCursor,
		"observations_next_cursor_key": nextCursorKey,
		"impacts":                      impacts,
		"impacts_truncated":            impactTruncated,
		"impacts_has_more":             impactTruncated,
		"impacts_next_cursor":          impactNext,
		"impacts_archived":             archivedOut,
		"impacts_archived_truncated":   archiveTruncated,
		"impacts_archived_has_more":    archiveTruncated,
		"impacts_archived_next_cursor": archiveNext,
		"revisions":                    revOut,
		"revisions_truncated":          revisionTruncated,
		"revisions_has_more":           revisionTruncated,
		"revisions_next_cursor":        revisionNext,
		"limit":                        limit,
		"anomaly_rate_caveat":          incidentAnomalyRateCaveat,
		"impact_dimension_gaps": []string{
			"ingress_protocol（无事实）", "node（无事实）", "user（无事实）",
		},
	})
}

// serveIncidentIngestHealth 采集健康与缺口清单。
func (m *Monitor) serveIncidentIngestHealth(c *gin.Context) {
	core, ingest, active := incidentIngestActive()
	if !core.Enabled {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "note": "事故核心开关未开启"})
		return
	}
	release, ok := acquireIncidentQueryGate(c.Request.Context(), core)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "事故查询并发已满，请稍后重试"})
		return
	}
	defer release()
	ctx, cancel := incidentQueryContext(c.Request.Context(), core)
	defer cancel()

	cursor, err := m.loadIncidentIngestCursor(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取采集游标失败"})
		return
	}
	// ★ 查询失败必须明确报错，不能把未知当零 ★
	//
	// 以前这些计数的错误被 `_` 丢掉：计数查询超时或读失败时，接口仍返回
	// HTTP 200 且 pending_backlog=0，运维看到的是「无积压」。那是把
	// 「不知道」显示成「没有」，比直接报错危险得多。
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		incidentRespondQueryFailure(c, "pending_backlog", err)
		return
	}
	terminal, err := m.countIncidentTerminalGaps(ctx)
	if err != nil {
		incidentRespondQueryFailure(c, "terminal_gap_count", err)
		return
	}

	stateCounts := map[string]int64{}
	type agg struct {
		ProcessState string
		N            int64
	}
	var rows []agg
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Select("process_state, COUNT(*) AS n").Group("process_state").Find(&rows).Error; err != nil {
		incidentRespondQueryFailure(c, "process_state_counts", err)
		return
	}
	for _, r := range rows {
		stateCounts[r.ProcessState] = r.N
	}
	windowCounts := map[string]int64{}
	var wrows []struct {
		WindowState string
		N           int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Select("window_state, COUNT(*) AS n").Group("window_state").Find(&wrows).Error; err != nil {
		incidentRespondQueryFailure(c, "window_state_counts", err)
		return
	}
	for _, r := range wrows {
		windowCounts[r.WindowState] = r.N
	}

	// 能力边界标记独立计数，不混入积压。
	boundary := map[string]int64{}
	for col, name := range map[string]string{
		"scenario_pending":     "scenario_pending",
		"below_min_sample":     "below_min_sample",
		"recheck_closed":       "recheck_closed",
		"gap_recheck_closed":   "gap_recheck_closed",
		"mixed_version_closed": "mixed_version_closed",
		"source_facts_expired": "source_facts_expired",
	} {
		var n int64
		if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
			Where(col+" = ?", true).Count(&n).Error; err != nil {
			incidentRespondQueryFailure(c, "capability_boundaries."+name, err)
			return
		}
		boundary[name] = n
	}

	var lagP50, lagP95, lagMax, lagSamples int64
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestRevision{}).
		Where("revision_detect_lag_seconds > 0").Count(&lagSamples).Error; err != nil {
		incidentRespondQueryFailure(c, "revision_detect_lag.samples", err)
		return
	}
	if lagSamples > 0 {
		if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestRevision{}).
			Select("COALESCE(MAX(revision_detect_lag_seconds),0)").Scan(&lagMax).Error; err != nil {
			incidentRespondQueryFailure(c, "revision_detect_lag.max", err)
			return
		}
		if err := m.storeDB.WithContext(ctx).Raw(`
			SELECT COALESCE(MAX(revision_detect_lag_seconds),0) FROM (
				SELECT revision_detect_lag_seconds FROM incident_ingest_revisions
				WHERE revision_detect_lag_seconds > 0
				ORDER BY revision_detect_lag_seconds LIMIT (SELECT (COUNT(*)+1)/2 FROM incident_ingest_revisions WHERE revision_detect_lag_seconds > 0))`).
			Scan(&lagP50).Error; err != nil {
			incidentRespondQueryFailure(c, "revision_detect_lag.p50", err)
			return
		}
		if err := m.storeDB.WithContext(ctx).Raw(`
			SELECT COALESCE(MAX(revision_detect_lag_seconds),0) FROM (
				SELECT revision_detect_lag_seconds FROM incident_ingest_revisions
				WHERE revision_detect_lag_seconds > 0
				ORDER BY revision_detect_lag_seconds LIMIT (SELECT (COUNT(*)*95+99)/100 FROM incident_ingest_revisions WHERE revision_detect_lag_seconds > 0))`).
			Scan(&lagP95).Error; err != nil {
			incidentRespondQueryFailure(c, "revision_detect_lag.p95", err)
			return
		}
	}

	var summaries []IncidentIngestGapSummary
	if err := m.storeDB.WithContext(ctx).Order("gap_from_ts").Limit(200).Find(&summaries).Error; err != nil {
		incidentRespondQueryFailure(c, "gap_summaries", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"enabled":        true,
		"ingest_running": active,
		"watermarks": gin.H{
			"scan_through_ts":     cursor.ScanThroughTs,
			"complete_through_ts": cursor.CompleteThroughTs,
			"note": "scan 表示已尝试处理到哪里（含永久失败）；complete 表示来源数据完整到哪里。" +
				"两者不同是正常的，差值即积压区间。",
		},
		"backlog": gin.H{
			"mode": cursor.BacklogMode,
			// pending_backlog 只含仍可被推进的行；历史失败不占队列，
			// 否则大量永久失败会把新数据处理永久卡住且无法自动恢复。
			"pending_backlog":    backlog,
			"terminal_gap_count": terminal,
			"warn_rows":          ingest.BacklogWarnRows,
			"max_rows":           ingest.BacklogMaxRows,
		},
		"process_state_counts":  stateCounts,
		"window_state_counts":   windowCounts,
		"capability_boundaries": boundary,
		"gap_summaries":         summaries,
		"revision_detect_lag": gin.H{
			"p50": lagP50, "p95": lagP95, "max": lagMax, "samples": lagSamples,
			// 指标口径限制必须与指标一起输出。
			"caveat": "该值只表示「定稿后多久被复核发现」，不是日志实际迟到时长：" +
				"受复核频率离散化、受复核范围与次数上限截断，且只统计被发现的修订（未被发现的迟到不在样本内，系统性低估）。" +
				"不足以单独判断处理延迟是否够用。",
			"recheck_window_seconds": ingest.RecheckWindowSeconds,
			"recheck_max_attempts":   ingest.RecheckMaxAttempts,
			"recheck_every_ticks":    ingest.RecheckEveryTicks,
		},
		"config": gin.H{
			"anomaly_rate_threshold": ingest.AnomalyRate,
			"finalize_lag_seconds":   ingest.FinalizeLagSeconds,
			"poll_seconds":           ingest.PollSeconds,
			"threshold_note":         "20% 为待校准 shadow 初值，需按 02.1 观察至少 7 天后校准",
		},
		"anomaly_rate_caveat": incidentAnomalyRateCaveat,
		"unconnected_sources": []string{
			"入口协议维度（无事实，填 ingress_unknown）",
			"归一化路由维度（无事实，填 route_unknown）",
			"独立请求数 / 可见尝试数 / 日志行数（分钟聚合桶不可诚实导出，填 nil）",
			"上游侧硬信号与双源确认（SEV0/SEV1 的佐证路径不可达）",
			"备用渠道配置事实（场景无法判定，全部「场景待确认」）",
			"已有事故的 scenario 后续补充（引擎更新路径不同步 scenario，本轮不自动改写）",
			"影响面的 ingress_protocol / node / user 三维（无事实）",
			"独立的数据质量事故族（FaultTelemetryGap 不进 IncidentKey）",
			"MetricSample / StabilityHourSample 在 8204 为 stale",
		},
		"known_limits": []string{
			"无样本窗口不送引擎：异常后彻底无流量的事故会长期 open，且冷却不会结束它" +
				"（冷却只决定已结束事故能否被新异常复用），需人工或后续恢复门禁收尾",
			"超出复核范围或次数的迟到数据不会被发现（recheck_closed 明示）",
			"超出 7 天补采窗的缺口不再复核（gap_recheck_closed 明示）",
			"源事实被 RetentionDays 清理后失败窗口不可重放（source_facts_expired 明示）",
			"日志实际迟到时长无法由本地事实测量",
		},
	})
}

// incidentRespondQueryFailure 以明确错误回应某个子查询的失败。
//
// ★ 不把未知包装成「无积压、无影响、无需确认」★
//
// 返回 503 而不是 200+零值：调用方（人或告警）必须能区分「确实是 0」与
// 「读不到」。field 指明是哪个子查询失败，便于定位；错误文本经
// stabilityProblemText 脱敏并截断，不外泄内部细节。
func incidentRespondQueryFailure(c *gin.Context, field string, cause error) {
	detail, _ := stabilityProblemText(cause.Error())
	if len(detail) > 200 {
		detail = detail[:200]
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{
		"error":        "事故查询失败，结果不可用",
		"failed_field": field,
		"detail":       detail,
		"data_status":  "unavailable",
		"note":         "该字段读取失败，未知不等于零；请重试或检查本地库。",
	})
}

func incidentClampLimit(raw string, def, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func incidentParseRange(c *gin.Context) (int64, int64, error) {
	from, _ := strconv.ParseInt(c.Query("from"), 10, 64)
	to, _ := strconv.ParseInt(c.Query("to"), 10, 64)
	if from > 0 && to > 0 && to <= from {
		return 0, 0, errIncidentBadRange
	}
	// 范围上界：防止一次请求扫全表。
	if from > 0 && to > 0 && to-from > 90*86400 {
		return 0, 0, errIncidentRangeTooWide
	}
	return from, to, nil
}

var (
	errIncidentBadRange     = &incidentHTTPError{"时间范围非法：to 必须大于 from"}
	errIncidentRangeTooWide = &incidentHTTPError{"时间范围过宽：一次最多查询 90 天"}
)

type incidentHTTPError struct{ msg string }

func (e *incidentHTTPError) Error() string { return e.msg }
