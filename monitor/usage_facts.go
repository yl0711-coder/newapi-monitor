package monitor

// usage_facts.go 将用户用量的“读生产 logs 再聚合”拆成两个独立阶段：
//
//   1. 后台以单小时、小窗口、串行的方式从只读 NewAPI 库取得事实；
//   2. 管理端和客户门户在显式切读后只查询 Monitor 本地 SQLite。
//
// 这样页面请求不会随 logs 表增长而放大生产库压力。资料(users/tokens)也有独立
// 快照，因此资料同步失败不会把已有的消费事实一并变成“查询失败”。开关默认关闭，
// 先 shadow/backfill、校验覆盖率后才允许开启 ReadEnabled。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	usageFactHourSeconds    int64 = 3600
	usageFactDaySeconds     int64 = 24 * usageFactHourSeconds
	usageFactMaxRowsPerHour       = 50000
	usageFactProfileBatch         = 200
	// 历史补数恢复或成员变更后，已完成小时可能很多。每轮只在本地台账中跳过
	// 一小批，避免每 2 秒重新扫描整段 366 天台账；不触发来源库查询。
	usageFactBackfillSkipBatch = 168 // 最多一周小时数
	usageFactGapAuditInterval  = time.Hour
	usageFactGapAuditMembers   = 10 // 200 人最长 20 小时轮完；单轮不扫全库
	// 后台事实同步不能排队抢占前台按需查询；占不到生产读取槽位就留待下个
	// 周期，而不是持续阻塞或叠加连接。
	usageFactSourceGateWait      = 250 * time.Millisecond
	usageFactSourceBackoffBase   = 5 * time.Minute
	usageFactSourceBackoffMax    = time.Hour
	usageFactAdaptiveQueryBudget = 8
	// A local SQLite writer (most commonly the paired online backup at
	// startup) may briefly outlive busy_timeout.  That is not a source failure
	// and must not postpone the high-priority live lane for its normal five
	// minute period.  Retry locally after a bounded pause; the shared source
	// gate still enforces all production-query spacing and duty limits.
	usageFactLocalBusyRetry = 30 * time.Second
)

func usageFactLocalStoreBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "sqlite_busy") ||
		strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked")
}

var (
	errUsageFactSourceBusy        = errors.New("生产用量查询槽位繁忙")
	errUsageFactLeaseBusy         = errors.New("用量事实小时已由其他任务领取")
	errUsageFactHourRangeTooLarge = errors.New("单小时聚合维度超过安全上限")
	errUsageFactAdaptiveBudget    = errors.New("单轮自适应来源查询预算已用尽")
	// errUsageFactsNotReady 表示运维已经请求将页面切到本地事实层，但本地
	// 快照尚未通过完整性校验。调用方必须做局部降级，绝不能悄悄回扫主站 logs。
	errUsageFactsNotReady = errors.New("本地用量事实尚未完成完整性校验")
)

// UsageFactsStatus 是用量本地事实层的最小运行状态。它只描述本地台账完整性，
// 不暴露成员 ID、查询条件或底层错误文本；也不把 Redis 命中率混进来，因为 Redis
// 只是缓存、不是事实源。FactCoverageReady 用于指导两阶段切换，绝不自动改变读源。
type UsageFactsStatus struct {
	Enabled                    bool                      `json:"enabled"`
	ReadEnabled                bool                      `json:"read_enabled"` // 配置已请求切读
	ReadActive                 bool                      `json:"read_active"`  // 已发布快照可用，实际正在切读
	SnapshotReadOnly           bool                      `json:"snapshot_read_only"`
	SnapshotUsable             bool                      `json:"snapshot_usable"`
	TrackedUsers               int                       `json:"tracked_users"`
	MembershipSynchronized     bool                      `json:"membership_synchronized"`
	FactCoverageReady          bool                      `json:"fact_coverage_ready"`
	BackfillDays               int                       `json:"backfill_days"`
	RangeStart                 string                    `json:"range_start"`
	RangeEnd                   string                    `json:"range_end"`
	ExpectedHours              int64                     `json:"expected_hours"`
	CompleteHours              int64                     `json:"complete_hours"`
	CoveragePercent            float64                   `json:"coverage_percent"`
	CoverageBasis              string                    `json:"coverage_basis"`
	NextBackfillHour           int64                     `json:"next_backfill_hour"`
	LastFactSyncAt             int64                     `json:"last_fact_sync_at"`
	LastProfileSyncAt          int64                     `json:"last_profile_sync_at"`
	LastFactFailureAt          int64                     `json:"last_fact_failure_at"`
	LastProfileFailureAt       int64                     `json:"last_profile_failure_at"`
	NextReconcileHour          int64                     `json:"next_reconcile_hour"`
	LastReconciledHour         int64                     `json:"last_reconciled_hour"`
	LastReconcileAt            int64                     `json:"last_reconcile_at"`
	LastReconcileFailureAt     int64                     `json:"last_reconcile_failure_at"`
	ReconcileCorrections       int64                     `json:"reconcile_corrections"`
	LoopHeartbeatAt            int64                     `json:"loop_heartbeat_at"`
	LoopRestarts               int64                     `json:"loop_restarts"`
	LatestCompleteHour         int64                     `json:"latest_complete_hour"`
	ServingUsers               int                       `json:"serving_users"`
	PendingUsers               int                       `json:"pending_users"`
	ExpectedMemberHours        int64                     `json:"expected_member_hours"`
	CompleteMemberHours        int64                     `json:"complete_member_hours"`
	PublishedAt                int64                     `json:"published_at"`
	PublishedRangeStart        int64                     `json:"published_range_start"`
	PublishedThrough           int64                     `json:"published_through"`
	SemanticAuditOK            bool                      `json:"semantic_audit_ok"`
	SemanticAuditAt            int64                     `json:"semantic_audit_at"`
	SemanticAuditFailureAt     int64                     `json:"semantic_audit_failure_at"`
	RepairFrom                 int64                     `json:"repair_from"`
	RepairThrough              int64                     `json:"repair_through"`
	RepairMode                 string                    `json:"repair_mode"`
	RepairTargetMembers        int64                     `json:"repair_target_members"`
	RepairRequestedAt          int64                     `json:"repair_requested_at"`
	RepairCompletedAt          int64                     `json:"repair_completed_at"`
	RepairLastFailureAt        int64                     `json:"repair_last_failure_at"`
	RepairLastError            string                    `json:"repair_last_error,omitempty"`
	RepairTotalMemberHours     int64                     `json:"repair_total_member_hours"`
	RepairCompletedMemberHours int64                     `json:"repair_completed_member_hours"`
	RepairActive               bool                      `json:"repair_active"`
	ExpectedMemberDays         int64                     `json:"expected_member_days"`
	CompleteMemberDays         int64                     `json:"complete_member_days"`
	ProofMigrationRequired     bool                      `json:"proof_migration_required"`
	ProofMigrationFrom         int64                     `json:"proof_migration_from"`
	ProofMigrationThrough      int64                     `json:"proof_migration_through"`
	SourceFailureStreak        int64                     `json:"source_failure_streak"`
	SourceBackoffUntil         int64                     `json:"source_backoff_until"`
	SourceBackoffActive        bool                      `json:"source_backoff_active"`
	HistoryWorkerRestarts      int64                     `json:"history_worker_restarts"`
	FullHistory                *UsageFactHistoryProgress `json:"full_history,omitempty"`
}

// usageFactsEnabled 控制后台采集；read 开关不能脱离采集开关单独生效。
// prodDB 必须存在，避免本地预览在没有来源时把空 SQLite 误当为完整事实源。
func (m *Monitor) usageFactsEnabled() bool {
	return m.cfg.UsageFactsEnabled && m.usageFactsStore() != nil && m.prodDB != nil
}

// usageFactsLocalSnapshotReadOnly 是本机验收的硬隔离模式。它只能读取本地挂载的
// SQLite 快照，且必须同时显式开启两个开关，避免生产配置误把空库当作事实源。
func (m *Monitor) usageFactsLocalSnapshotReadOnly() bool {
	return m.cfg.LocalSnapshotOnly && m.cfg.UsageFactsLocalReadOnly && m.usageFactsStore() != nil && m.prodDB == nil
}

// usageFactsReadRequested 表示运维已显式允许切读；它还需要 usageFactsReadReady
// 这一份本地完整性证明才能真正生效。生产环境只允许采集器已启用时请求切读；
// 本机验收则可读取一个已挂载、显式标记的离线快照。
func (m *Monitor) usageFactsReadRequested() bool {
	if m.usageFactsStore() == nil {
		return false
	}
	// An explicit classification maintenance window intentionally has no
	// publishable v5 snapshot yet. Treat it as local-read intent even though the
	// normal READ_ENABLED switch must be false: aggregate endpoints then return
	// facts-not-ready instead of falling back to wide production logs queries
	// for the entire rebuild window.
	if m.cfg.UsageFactsClassificationMigrationEnabled && m.cfg.UsageFactsFullHistoryEnabled && !m.cfg.LocalSnapshotOnly {
		return true
	}
	if !m.cfg.UsageFactsReadEnabled {
		return false
	}
	return m.usageFactsEnabled() || m.usageFactsLocalSnapshotReadOnly()
}

func (m *Monitor) usageFactsReadEnabled() bool {
	// 一旦历史事实已经为当前名单完成核验，页面读取始终固定在 Monitor 本地库。
	// 新闭合小时在下一轮同步前可能尚未入库，但这只能表现为“数据更新水位稍后”，
	// 不能为了追求实时性重新扫描主站 logs。否则高峰时的页面访问会反向放大生产库
	// 压力，也破坏了事实层切读的隔离边界。
	return m.usageFactsReadRequested() && m.usageFactsReadReady.Load()
}

// usageReadServingEnabled 区分“整个 Monitor 是否连着生产库”和“用量页是否有
// 一个经过验证的本地事实源”。本机离线验收允许后者为真而前者为假。
func (m *Monitor) usageReadServingEnabled() bool {
	return m.Enabled() || m.usageFactsReadRequested()
}

// usageFactsRangeStaleness 只报告“本该已完成采集的小时”是否落后于本地事实水位。
// 当前延迟窗口内仍可能有日志写入，这是正常设计，不在这里标记为异常。返回 true
// 时，调用方仍只读取本地事实，只需提示当前查询范围不包含水位之后的已闭合时段。
func (m *Monitor) usageFactsRangeStaleness(fromTs, toTs int64, now time.Time) (bool, string) {
	if !m.usageFactsReadEnabled() || toTs <= fromTs {
		return false, ""
	}
	readyThrough := m.usageFactsReadyThrough.Load()
	finalizedThrough := m.usageFactFinalizedHour(now)
	// 查询范围完全在已验证水位之前，或水位已经追上当前可采集时段，都没有缺口。
	if readyThrough <= 0 || toTs <= readyThrough || finalizedThrough <= readyThrough {
		return false, ""
	}
	// 查询在水位后只包含仍处于延迟窗口中的数据时，不属于事实同步落后。
	if fromTs >= finalizedThrough {
		return false, ""
	}
	coveredUntil := time.Unix(readyThrough, 0).In(usageCST).Format("2006-01-02 15:04")
	return true, "本地用量汇总正在补齐，当前统计已汇总至 " + coveredUntil + "（CST）"
}

// usageDataStaleness 统一组合两类“数据非最新”状态：缓存回退优先于本地事实水位。
// 前者代表本次来源读取失败后使用了最近成功结果，后者代表事实同步尚未追平已闭合
// 小时；两种情况都不改变已返回的结构，也不暴露底层数据库或网络错误。
func (m *Monitor) usageDataStaleness(cacheStale bool, fromTs, toTs int64, now time.Time) (bool, string) {
	if cacheStale {
		return true, "数据源暂时不可用，当前显示最近一次成功统计"
	}
	return m.usageFactsRangeStaleness(fromTs, toTs, now)
}

// usageFactsUnavailableMessage 为事实域不可读时提供面向页面的、稳定的说明。
// 不能把数据库、Redis 或网络报错直接透给客户；更不能提示页面退回扫描主站 logs。
// 本机快照模式额外明确“快照未通过完整性校验”，避免把验收环境的空库误解为真实零用量。
func (m *Monitor) usageFactsUnavailableMessage(err error, feature string) string {
	if feature == "" {
		feature = "用量明细"
	}
	if m.usageFactsLocalSnapshotReadOnly() && errors.Is(err, errUsageFactsNotReady) {
		return feature + "暂不可用：本机验收快照未通过完整性校验"
	}
	return feature + "暂不可用，余额和累计消耗仍可查看"
}

func (m *Monitor) setUsageFactsPublishedReadiness(ready bool, readyFrom, readyThrough int64) {
	if ready && m.usageFactsRepairHoldPending.Load() > 0 {
		ready = false
	}
	if !ready || readyThrough <= 0 {
		m.usageFactsReadReady.Store(false)
		m.usageFactsReadyFrom.Store(0)
		m.usageFactsReadyThrough.Store(0)
		return
	}
	// 先写边界、后开许可；读取侧不会看见“许可已开但范围为空”的中间态。
	m.usageFactsReadyFrom.Store(readyFrom)
	m.usageFactsReadyThrough.Store(readyThrough)
	m.usageFactsReadReady.Store(true)
}

// publishUsageFactGenerations makes the in-memory cache namespace monotonic.
// Several writers commit independent SQLite transactions; a slower writer may
// reach its post-commit Store after a newer transaction. Plain Store would then
// move the atomic generation backwards and keep Portal in a DB/atomic mismatch
// state until another write or restart.
func (m *Monitor) publishUsageFactGenerations(generation, servingGeneration int64) {
	for generation > 0 {
		current := m.usageFactsRevision.Load()
		if current >= generation || m.usageFactsRevision.CompareAndSwap(current, generation) {
			break
		}
	}
	for servingGeneration > 0 {
		current := m.usageFactsServingRevision.Load()
		if current >= servingGeneration || m.usageFactsServingRevision.CompareAndSwap(current, servingGeneration) {
			break
		}
	}
}

func (m *Monitor) publishUsageFactReadBoundsAfterMutation(state UsageFactSyncState) {
	// A delayed writer must not publish its older bounds after another
	// transaction has already advanced the cache/publication generation.
	if m.usageFactsRevision.Load() != state.Generation ||
		m.usageFactsServingRevision.Load() != state.ServingGeneration {
		return
	}
	if state.PublishedAt <= 0 || state.PublishedRangeStart <= 0 || state.PublishedThrough <= state.PublishedRangeStart {
		m.setUsageFactsPublishedReadiness(false, 0, 0)
		return
	}
	// A membership withdrawal may safely shrink the serving range while the
	// source is offline. Keep an already-open gate on the durable new bounds;
	// never reopen a gate closed by an unrelated semantic audit.
	if m.usageFactsReadReady.Load() {
		m.setUsageFactsPublishedReadiness(true, state.PublishedRangeStart, state.PublishedThrough)
	}
}

// setUsageFactsPublishedReadinessIfCurrent closes the final checkpoint->atomic
// race. A readiness audit runs without holding the publication mutex; another
// writer may commit a newer member set/range before that audit stores its old
// bounds. Re-enter the writer mutex, bind the expected range to the current
// durable and in-memory generations, then publish all three readiness atomics
// while no publication writer can interleave.
func (m *Monitor) setUsageFactsPublishedReadinessIfCurrent(
	ctx context.Context,
	ready bool,
	expectedFrom, expectedThrough int64,
) bool {
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	var current UsageFactSyncState
	if err := m.usageFactsStore().WithContext(ctx).First(&current, 1).Error; err != nil {
		m.setUsageFactsPublishedReadiness(false, 0, 0)
		return false
	}
	if current.PublishedRangeStart != expectedFrom || current.PublishedThrough != expectedThrough ||
		current.ServingGeneration != m.usageFactsServingRevision.Load() {
		// The concurrent writer owns the newer atomics. Do not overwrite them
		// with the range audited by this stale refresh.
		return false
	}
	m.setUsageFactsPublishedReadiness(ready, current.PublishedRangeStart, current.PublishedThrough)
	return true
}

// setUsageFactsReadiness 保留给旧读取单元测试使用；readyFrom=0 表示测试
// 直接构造了无限历史范围。生产发布路径必须调用上面的完整范围方法。
func (m *Monitor) setUsageFactsReadiness(ready bool, readyThrough int64) {
	m.setUsageFactsPublishedReadiness(ready, 0, readyThrough)
}

// usageFactsPublishedRangeCovers only answers whether the request starts inside
// the published authority range. Callers must still clamp the right edge to
// usageFactsReadyThrough; otherwise a lagging Tail would turn unknown future
// buckets into zeroes.
func (m *Monitor) usageFactsPublishedRangeCovers(fromTs, toTs int64) bool {
	if !m.usageFactsReadEnabled() || toTs <= fromTs {
		return false
	}
	readyFrom := m.usageFactsReadyFrom.Load()
	return readyFrom <= 0 || fromTs >= readyFrom
}

// usageAggregateReadRange 是页面聚合的实际可读自然日窗口。请求范围早于当前
// 已发布事实时，页面可以先展示与服务版重叠的完整自然日；绝不能把更早的未知日期
// 补成 0，更不能为了补齐页面回退扫描生产 logs。
type usageAggregateReadRange struct {
	RequestedFrom int64
	RequestedTo   int64
	From          int64
	To            int64
	Available     bool
	Partial       bool
	Message       string
}

func (m *Monitor) resolveUsageAggregateReadRange(fromTs, toTs int64) usageAggregateReadRange {
	return m.resolveUsageAggregateReadRangeWithFloor(fromTs, toTs, m.usageFactsReadyFrom.Load())
}

// resolveUsageAggregateReadRangeForMembers verifies that every selected member
// belongs to the current signed publication, then uses the publication's global
// readable range.  A member's SourceFloorHour is an applicability boundary, not
// a boundary for its peers: full-history discovery has already proved that the
// member has no source usage before that hour (account not yet created or a
// verified known-empty prefix). usageFactsCTE applies that boundary per member.
//
// Raising the aggregate left edge to MAX(member.SourceFloorHour) would make one
// newly-created account hide otherwise complete history for every older member
// in the company and in the administrator's all-user matrix.
func (m *Monitor) resolveUsageAggregateReadRangeForMembers(ctx context.Context, fromTs, toTs int64, ids []int64) (usageAggregateReadRange, error) {
	if !m.usageFactsReadRequested() || !m.usageFactsFullHistoryMode() || len(ids) == 0 || m.usageFactsLocalSnapshotReadOnly() {
		return m.resolveUsageAggregateReadRange(fromTs, toTs), nil
	}
	unique := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id > 0 {
			unique[id] = struct{}{}
		}
	}
	if len(unique) == 0 {
		return m.resolveUsageAggregateReadRange(fromTs, toTs), nil
	}
	memberIDs := make([]int64, 0, len(unique))
	for id := range unique {
		memberIDs = append(memberIDs, id)
	}
	var rows []UsageFactPublishedMember
	if err := m.usageFactsStore().WithContext(ctx).Select("user_id", "source_floor_hour").
		Where("user_id IN ?", memberIDs).Find(&rows).Error; err != nil {
		return usageAggregateReadRange{}, err
	}
	if len(rows) != len(memberIDs) {
		r := m.resolveUsageAggregateReadRange(fromTs, toTs)
		r.Available, r.Partial = false, true
		r.Message = "所选成员的近期用量尚未全部签收，当前拒绝生成不完整合计"
		return r, nil
	}
	for _, row := range rows {
		if row.SourceFloorHour <= 0 {
			return usageAggregateReadRange{}, fmt.Errorf("published member floor missing user_id=%d", row.UserID)
		}
	}
	return m.resolveUsageAggregateReadRange(fromTs, toTs), nil
}

func (m *Monitor) resolveUsageAggregateReadRangeWithFloor(fromTs, toTs, readyFrom int64) usageAggregateReadRange {
	r := usageAggregateReadRange{
		RequestedFrom: fromTs,
		RequestedTo:   toTs,
		From:          fromTs,
		To:            toTs,
		Available:     toTs > fromTs,
	}
	// 旧来源读取模式没有“已发布事实左界”，保持原查询范围。
	if !m.usageFactsReadRequested() {
		return r
	}
	if !m.usageFactsReadEnabled() {
		r.Available = false
		r.Message = "历史每日消费正在初始化，目前尚无完整数据可展示"
		return r
	}

	readyThrough := m.usageFactsReadyThrough.Load()
	firstAvailable := fromTs
	if readyFrom > 0 {
		firstFullDay, _ := usageFactSemanticFullDayRange(readyFrom, readyThrough)
		if firstFullDay > firstAvailable {
			firstAvailable = firstFullDay
		}
	}
	lastAvailable := toTs
	if readyThrough <= 0 {
		lastAvailable = 0
	} else if readyThrough < lastAvailable {
		lastAvailable = readyThrough
	}
	if firstAvailable >= lastAvailable {
		r.Available = false
		r.Partial = true
		r.Message = "所选范围的历史消费正在补全，目前尚无已发布时段可展示"
		return r
	}

	r.From = firstAvailable
	r.To = lastAvailable
	r.Partial = firstAvailable > fromTs || lastAvailable < toTs
	if r.Partial {
		requestedDays := (toTs - fromTs) / usageFactDaySeconds
		availableSeconds := lastAvailable - firstAvailable
		availableDays := availableSeconds / usageFactDaySeconds
		fromLabel := time.Unix(firstAvailable, 0).In(usageCST).Format("2006-01-02 15:04")
		toLabel := time.Unix(lastAvailable, 0).In(usageCST).Format("2006-01-02 15:04")
		if requestedDays > 0 && availableSeconds%usageFactDaySeconds == 0 {
			r.Message = fmt.Sprintf("已先展示 %s 至 %s（%d/%d 天），其余时段正在后台补全；补全后刷新即可查看",
				fromLabel, toLabel, availableDays, requestedDays)
		} else {
			r.Message = fmt.Sprintf("已先展示 %s 至 %s，其余时段正在后台补全；补全后刷新即可查看", fromLabel, toLabel)
		}
	}
	return r
}

// usageFactServingReadSnapshot binds an aggregate response to one durable
// publication generation and its exact read bounds.  Writers commit SQLite
// before publishing the lock-free atomics; this snapshot deliberately rejects
// that hand-off interval so cache keys, member authority and range metadata can
// never come from different generations.
type usageFactServingReadSnapshot struct {
	ServingGeneration    int64
	PublishedFingerprint string
	From                 int64
	Through              int64
}

func (s usageFactServingReadSnapshot) equal(other usageFactServingReadSnapshot) bool {
	return s.ServingGeneration > 0 && s.ServingGeneration == other.ServingGeneration &&
		s.PublishedFingerprint != "" && s.PublishedFingerprint == other.PublishedFingerprint &&
		s.From == other.From && s.Through == other.Through
}

func (m *Monitor) loadUsageFactServingReadSnapshot(ctx context.Context) (usageFactServingReadSnapshot, error) {
	if !m.usageFactsReadRequested() {
		return usageFactServingReadSnapshot{}, nil
	}
	if !m.usageFactsReadEnabled() || m.usageFactsRepairHoldPending.Load() > 0 {
		return usageFactServingReadSnapshot{}, errUsageFactsNotReady
	}
	var state UsageFactSyncState
	var ids []int64
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&state, 1).Error; err != nil {
			return err
		}
		if err := tx.Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &ids).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return usageFactServingReadSnapshot{}, err
	}
	if state.TrafficClassVersion != userTrafficClassificationVersion ||
		state.PublishedAt <= 0 || state.PublishedRangeStart <= 0 ||
		state.PublishedThrough <= state.PublishedRangeStart || state.PublishedFingerprint == "" ||
		len(ids) == 0 || state.PublishedFingerprint != portalMemberFingerprintFromIDs(ids) || state.ServingGeneration <= 0 {
		return usageFactServingReadSnapshot{}, errUsageFactsNotReady
	}
	if state.ServingGeneration != m.usageFactsServingRevision.Load() ||
		state.PublishedRangeStart != m.usageFactsReadyFrom.Load() ||
		state.PublishedThrough != m.usageFactsReadyThrough.Load() {
		return usageFactServingReadSnapshot{}, errUsageFactsNotReady
	}
	return usageFactServingReadSnapshot{
		ServingGeneration: state.ServingGeneration, PublishedFingerprint: state.PublishedFingerprint,
		From: state.PublishedRangeStart, Through: state.PublishedThrough,
	}, nil
}

// usageFactPublishedSnapshot 核验服务版元数据与成员表相互一致。
// 只有发布事务完整提交后才会返回 usable；单独存在状态行或成员行
// 都不会被当成可读快照。
func (m *Monitor) usageFactPublishedSnapshot(ctx context.Context) (UsageFactSyncState, bool, error) {
	var state UsageFactSyncState
	if err := m.usageFactsStore().WithContext(ctx).First(&state, 1).Error; err != nil {
		return state, false, err
	}
	if state.TrafficClassVersion != userTrafficClassificationVersion ||
		state.PublishedAt <= 0 || state.PublishedRangeStart <= 0 || state.PublishedThrough <= state.PublishedRangeStart ||
		state.PublishedWindowDays <= 0 || state.PublishedFingerprint == "" {
		return state, false, nil
	}
	var ids []int64
	if err := m.usageFactsStore().WithContext(ctx).Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &ids).Error; err != nil {
		return state, false, err
	}
	return state, len(ids) > 0 && state.PublishedFingerprint == portalMemberFingerprintFromIDs(ids), nil
}

// listTrackedForUsageRead 返回“当前权限名单 ∩ 已发布事实成员”。
// 这个交集同时满足两个边界：删除/移组后的用户不会因旧快照越权出现；
// 新用户在历史回填完成前也不会显示半份数据。还没有发布快照时保留
// 旧模式/测试辅助的当前名单行为。
func (m *Monitor) listTrackedForUsageRead(ctx context.Context) ([]TrackedUser, error) {
	tracked, _, err := m.listTrackedForUsageReadCoverage(ctx)
	return tracked, err
}

type usageFactReadMembershipCoverage struct {
	Active    int
	Published int
	Complete  bool
}

func (m *Monitor) listTrackedForUsageReadCoverage(ctx context.Context) ([]TrackedUser, usageFactReadMembershipCoverage, error) {
	if !m.usageFactsReadRequested() {
		tracked, err := m.listTracked()
		return tracked, usageFactReadMembershipCoverage{Active: len(tracked), Published: len(tracked), Complete: err == nil}, err
	}
	if !m.usageFactsReadEnabled() {
		return nil, usageFactReadMembershipCoverage{}, errUsageFactsNotReady
	}
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if _, usable, err := m.usageFactPublishedSnapshot(qctx); err != nil {
		return nil, usageFactReadMembershipCoverage{}, err
	} else if !usable {
		// 离线快照模式由完整小时台账证明候选库本身可读，不要求快照来自
		// 新版“发布成员表”。这仅用于完全断开生产库的本机验收；线上采集
		// 模式仍必须命中下面的原子发布校验，不能借此放宽。
		if m.usageFactsLocalSnapshotReadOnly() {
			snapshot, snapshotErr := m.loadUsageMemberControlSnapshot(qctx)
			coverage := usageFactReadMembershipCoverage{Active: len(snapshot.Tracked), Published: len(snapshot.Tracked), Complete: snapshotErr == nil}
			return snapshot.Tracked, coverage, snapshotErr
		}
		// 生产发布路径已经记录了明确左边界；此时若服务版元数据/成员表损坏，
		// 绝不能退回当前候选名单，否则会把未补齐的新成员暴露给页面。
		if m.usageFactsReadyFrom.Load() > 0 {
			return nil, usageFactReadMembershipCoverage{}, errUsageFactsNotReady
		}
		snapshot, snapshotErr := m.loadUsageMemberControlSnapshot(qctx)
		coverage := usageFactReadMembershipCoverage{Active: len(snapshot.Tracked), Published: len(snapshot.Tracked), Complete: snapshotErr == nil}
		return snapshot.Tracked, coverage, snapshotErr
	}
	membership, err := m.currentPublishedUsageMembership(qctx, nil)
	if err != nil {
		return nil, usageFactReadMembershipCoverage{}, fmt.Errorf("%w: %w", errUsageFactsNotReady, err)
	}
	return membership.Members, usageFactReadMembershipCoverage{
		Active: membership.Active, Published: membership.Published, Complete: membership.Complete,
	}, nil
}

func (m *Monitor) portalTrackedMembersForUsageRead(ctx context.Context, gid int64) ([]TrackedUser, error) {
	if !m.usageFactsReadRequested() {
		return m.portalTrackedMembers(gid)
	}
	if !m.usageFactsReadEnabled() {
		// Once local facts are the requested authority, an audit/repair/startup
		// fail-close must never fall back to the current TrackedUser projection:
		// that projection includes partial/rejoined revisions which have not been
		// published and would expose their identity in Portal endpoints.
		return nil, errUsageFactsNotReady
	}
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if _, usable, err := m.usageFactPublishedSnapshot(qctx); err != nil {
		return nil, err
	} else if !usable {
		if m.usageFactsLocalSnapshotReadOnly() {
			snapshot, snapshotErr := m.loadUsageMemberControlSnapshot(qctx)
			if snapshotErr != nil {
				return nil, fmt.Errorf("%w: %w", errPortalMemberStore, snapshotErr)
			}
			out := make([]TrackedUser, 0, len(snapshot.Tracked))
			for _, member := range snapshot.Tracked {
				if member.GroupID == gid {
					out = append(out, member)
				}
			}
			return out, nil
		}
		if m.usageFactsReadyFrom.Load() > 0 {
			return nil, errUsageFactsNotReady
		}
		snapshot, snapshotErr := m.loadUsageMemberControlSnapshot(qctx)
		if snapshotErr != nil {
			return nil, fmt.Errorf("%w: %w", errPortalMemberStore, snapshotErr)
		}
		out := make([]TrackedUser, 0, len(snapshot.Tracked))
		for _, member := range snapshot.Tracked {
			if member.GroupID == gid {
				out = append(out, member)
			}
		}
		return out, nil
	}
	membership, err := m.currentPublishedUsageMembership(qctx, &gid)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errPortalMemberStore, err)
	}
	if !membership.Complete {
		// Company totals are only meaningful when every currently-authorized
		// member of that company has a compatible publication. Other companies
		// remain available while this one catches up.
		return nil, errUsageFactsNotReady
	}
	if blocked, err := m.portalGroupHasActiveUsageFactRepair(qctx, gid); err != nil {
		return nil, fmt.Errorf("%w: %w", errPortalMemberStore, err)
	} else if blocked {
		// A newly added member is intentionally admin-only until first publish,
		// but withdrawing an already published member for a correctness repair
		// must not silently undercount that company's total. Scope the fail-close
		// to the affected company; unrelated customer portals keep serving.
		return nil, errUsageFactsNotReady
	}
	return membership.Members, nil
}

func (m *Monitor) portalGroupHasActiveUsageFactRepair(ctx context.Context, gid int64) (bool, error) {
	if !m.usageFactsFullHistoryMode() {
		return false, nil
	}
	active, err := loadUsageFactActiveRepairUsers(m.usageFactsStore().WithContext(ctx))
	if err != nil || len(active) == 0 {
		return false, err
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return false, err
	}
	for id := range active {
		control, ok := snapshot.Controls[id]
		if ok && control.Active && control.CurrentGroupID == gid {
			return true, nil
		}
	}
	return false, nil
}

func clampUsageFactInt(v, fallback, min, max int) int {
	if v <= 0 {
		return fallback
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func (m *Monitor) usageFactBackfillDays() int {
	return clampUsageFactInt(m.cfg.UsageFactsBackfillDays, 366, 1, 366)
}

func (m *Monitor) usageFactRetentionDays() int {
	min := m.usageFactBackfillDays()
	return clampUsageFactInt(m.cfg.UsageFactsRetentionDays, 400, min, 732)
}

func (m *Monitor) usageFactHourRetentionDays() int {
	// 小时事实只服务近期晚到日志复核；完整历史由日事实承载。若把小时
	// 留存强制抬到整个回填窗口（默认 366 天），SQLite 会无谓膨胀，且
	// 单小时低频复核一次全轮转需要数月。小时窗口因此独立配置，但不能
	// 超出实际回填窗口。
	max := m.usageFactBackfillDays()
	fallback := 8
	if fallback > max {
		fallback = max
	}
	return clampUsageFactInt(m.cfg.UsageFactsHourRetentionDays, fallback, 1, max)
}

func (m *Monitor) usageFactSyncInterval() time.Duration {
	return time.Duration(clampUsageFactInt(m.cfg.UsageFactsSyncMinutes, 5, 1, 60)) * time.Minute
}

func (m *Monitor) usageFactProfileSyncInterval() time.Duration {
	return time.Duration(clampUsageFactInt(m.cfg.UsageFactsProfileSyncMinutes, 5, 1, 60)) * time.Minute
}

func (m *Monitor) usageFactReconcileInterval() time.Duration {
	// 历史复核只为发现晚到/事后修订，并非实时链路。最短 15 分钟且每次只查
	// 一个小时，防止误配把来源 logs 变成持续轮询对象。
	return time.Duration(clampUsageFactInt(m.cfg.UsageFactsReconcileMinutes, 30, 15, 1440)) * time.Minute
}

func (m *Monitor) usageFactBackfillDelay() time.Duration {
	// 历史事实回填每次只读取一个小时，但不能以高频循环持续命中来源 logs。
	// 默认 15 秒、最小 5 秒：前台仍优先拿到读取槽位，后台回填则保持低、稳定且可预期的负载。
	return time.Duration(clampUsageFactInt(m.cfg.UsageFactsBackfillDelayMS, 15000, 5000, 60000)) * time.Millisecond
}

func (m *Monitor) usageFactQueryTimeout() time.Duration {
	return time.Duration(clampUsageFactInt(m.cfg.UsageFactsQueryTimeoutSec, 20, 5, 60)) * time.Second
}

func (m *Monitor) usageFactLag() time.Duration {
	return time.Duration(clampUsageFactInt(m.cfg.UsageFactsLagMinutes, 10, 3, 60)) * time.Minute
}

// usageFactJitteredDelay 只向后增加少量抖动，不会把配置的安全间隔缩短。
// 这样既避免多个实例/任务形成可识别的固定节拍，也不会为了随机化提高 QPS。
func (m *Monitor) usageFactJitteredDelay(base time.Duration, percent uint64) time.Duration {
	if base <= 0 || percent == 0 {
		return base
	}
	maxExtra := base * time.Duration(percent) / 100
	if maxExtra <= 0 {
		return base
	}
	seq := m.usageFactsScheduleSeq.Add(1)
	x := uint64(time.Now().UnixNano()) ^ seq*0x9e3779b97f4a7c15
	// SplitMix64 finalizer；只用于调度抖动，不承担安全随机数用途。
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return base + time.Duration(x%uint64(maxExtra+1))
}

func (m *Monitor) usageFactSourceBackoffActive(now time.Time) bool {
	until := m.usageFactsSourceBackoffUntil.Load()
	return until > 0 && now.UnixNano() < until
}

// recordUsageFactSourceResult 只记录实际来源查询结果。前台占槽、任务取消和本地
// 租约冲突不会触发退避；连续 MySQL 错误按 5/10/20/40/60 分钟退避并再加
// 0～20% 单向抖动。任一成功来源查询会立即清零。
func (m *Monitor) recordUsageFactSourceResult(err error) {
	if err == nil {
		m.usageFactsSourceFailureStreak.Store(0)
		m.usageFactsSourceBackoffUntil.Store(0)
		return
	}
	if errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, errSourceNotReady) ||
		errors.Is(err, context.Canceled) || errors.Is(err, errUsageFactLeaseBusy) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errUsageFactHourRangeTooLarge) ||
		errors.Is(err, errUsageFactHistoryRangeTooLarge) || errors.Is(err, errUsageFactHistoryControl) {
		return
	}
	var my *mysqlDriver.MySQLError
	if errors.As(err, &my) && my.Number == 3024 {
		return
	}
	streak := m.usageFactsSourceFailureStreak.Add(1)
	shift := streak - 1
	if shift > 4 {
		shift = 4
	}
	delay := usageFactSourceBackoffBase * time.Duration(int64(1)<<shift)
	if delay > usageFactSourceBackoffMax {
		delay = usageFactSourceBackoffMax
	}
	delay = m.usageFactJitteredDelay(delay, 20)
	m.usageFactsSourceBackoffUntil.Store(time.Now().Add(delay).UnixNano())
}

// usageFactsStatus 只查询本地 SQLite 的小时状态台账。完整小时即使没有任何消费日志
// 也会被写成 complete，因此覆盖率能严格区分“零流量”和“尚未读取”；当前尾部的
// 延迟窗口不计入期望小时，避免刚产生的日志被误判为缺失。
func (m *Monitor) usageFactsStatus(ctx context.Context, now time.Time) (UsageFactsStatus, error) {
	status := UsageFactsStatus{
		Enabled:          m.usageFactsEnabled(),
		ReadEnabled:      m.usageFactsReadRequested(),
		SnapshotReadOnly: m.usageFactsLocalSnapshotReadOnly(),
		BackfillDays:     m.usageFactBackfillDays(),
	}
	if now.IsZero() {
		now = time.Now()
	}
	status.SourceFailureStreak = m.usageFactsSourceFailureStreak.Load()
	if until := m.usageFactsSourceBackoffUntil.Load(); until > 0 {
		status.SourceBackoffUntil = time.Unix(0, until).Unix()
		status.SourceBackoffActive = time.Now().UnixNano() < until
	}
	if m.usageFactsStore() == nil {
		return status, errors.New("用量事实库不可用")
	}
	end := m.usageFactFinalizedHour(now)
	start := end - int64(status.BackfillDays)*usageFactDaySeconds
	status.ExpectedHours = max(end-start, 0) / usageFactHourSeconds
	if status.ExpectedHours > 0 {
		status.RangeStart = time.Unix(start, 0).In(usageCST).Format("2006-01-02 15:04 MST")
		status.RangeEnd = time.Unix(end-usageFactHourSeconds, 0).In(usageCST).Format("2006-01-02 15:04 MST")
	}
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	var latestComplete sql.NullInt64
	row := m.usageFactsStore().WithContext(qctx).Raw(
		"SELECT MAX(hour_ts) FROM usage_fact_member_hour_states WHERE status = ? AND content_hash <> ''", "complete",
	).Row()
	if err := row.Scan(&latestComplete); err != nil {
		return status, fmt.Errorf("读取本地事实最新小时失败: %w", err)
	}
	if !latestComplete.Valid {
		legacyRow := m.usageFactsStore().WithContext(qctx).Raw(
			"SELECT MAX(hour_ts) FROM usage_hour_ingest_states WHERE status = ? AND content_hash <> ''", "complete",
		).Row()
		if err := legacyRow.Scan(&latestComplete); err != nil {
			return status, fmt.Errorf("读取旧版事实最新小时失败: %w", err)
		}
	}
	if latestComplete.Valid {
		status.LatestCompleteHour = latestComplete.Int64
	}

	tracked, err := m.listTracked()
	if err != nil {
		return status, fmt.Errorf("读取本地关注客户失败: %w", err)
	}
	status.TrackedUsers = len(tracked)

	var syncState UsageFactSyncState
	if err := m.usageFactsStore().WithContext(qctx).First(&syncState, 1).Error; err != nil {
		return status, fmt.Errorf("读取本地事实同步状态失败: %w", err)
	}
	status.NextBackfillHour = syncState.NextBackfillHour
	status.LastFactSyncAt = syncState.LastFactSyncAt
	status.LastProfileSyncAt = syncState.LastProfileSyncAt
	status.LastFactFailureAt = syncState.LastFactFailureAt
	status.LastProfileFailureAt = syncState.LastProfileFailureAt
	status.NextReconcileHour = syncState.NextReconcileHour
	status.LastReconciledHour = syncState.LastReconciledHour
	status.LastReconcileAt = syncState.LastReconcileAt
	status.LastReconcileFailureAt = syncState.LastReconcileFailureAt
	status.ReconcileCorrections = syncState.ReconcileCorrections
	status.LoopHeartbeatAt = m.usageFactsLoopHeartbeat.Load()
	status.LoopRestarts = m.usageFactsRestarts.Load()
	status.PublishedAt = syncState.PublishedAt
	status.PublishedRangeStart = syncState.PublishedRangeStart
	status.PublishedThrough = syncState.PublishedThrough
	status.SemanticAuditOK = m.usageFactsSemanticAuditOK.Load()
	status.SemanticAuditAt = m.usageFactsSemanticAuditAt.Load()
	status.SemanticAuditFailureAt = m.usageFactsSemanticAuditFailureAt.Load()
	status.RepairFrom = syncState.RepairFrom
	status.RepairThrough = syncState.RepairThrough
	status.RepairMode = syncState.RepairMode
	status.RepairTargetMembers = syncState.RepairTargetMembers
	status.RepairRequestedAt = syncState.RepairRequestedAt
	status.RepairCompletedAt = syncState.RepairCompletedAt
	status.RepairLastFailureAt = syncState.RepairLastFailureAt
	status.RepairLastError = syncState.RepairLastError
	status.RepairTotalMemberHours = syncState.RepairTotalMemberHours
	status.RepairCompletedMemberHours = syncState.RepairCompletedMemberHours
	status.RepairActive = usageFactRepairActive(syncState)
	if m.usageFactsFullHistoryMode() {
		return m.populateUsageFactsFullHistoryStatus(qctx, status, syncState)
	}
	var memberStates []UsageFactMemberState
	if err := m.usageFactsStore().WithContext(qctx).Where("active = ?", true).Order("user_id").Find(&memberStates).Error; err != nil {
		return status, fmt.Errorf("读取用量事实成员水位失败: %w", err)
	}
	activeIDs := make([]int64, 0, len(memberStates))
	memberWindowOK := len(memberStates) == status.TrackedUsers && status.TrackedUsers > 0
	for _, member := range memberStates {
		activeIDs = append(activeIDs, member.UserID)
		if member.BackfillWindowDays != status.BackfillDays || member.RangeStart != start {
			memberWindowOK = false
		}
		if member.NextBackfillHour < end {
			status.PendingUsers++
		}
	}
	status.ExpectedMemberHours = status.ExpectedHours * int64(status.TrackedUsers)
	if len(activeIDs) > 0 && status.ExpectedHours > 0 {
		// NextBackfillHour 只有在该成员的当前小时已从来源读取并
		// 原子落盘，或本地 proof+事实/日指纹已重新核验后才会前移。
		// 因此它是持久化的“连续完成前缀”，可以 O(成员数)计算
		// 进度。原实现为了同一个百分比每次扫两遍 175 万行 proof，
		// 在 256MiB 冷启动下超过 13 秒并使切读失败。
		status.CoverageBasis = "contiguous_member_cursor"
		status.CompleteHours = status.ExpectedHours
		for _, member := range memberStates {
			cursor := member.NextBackfillHour
			if cursor < start {
				cursor = start
			}
			if cursor > end {
				cursor = end
			}
			completed := max(cursor-start, 0) / usageFactHourSeconds
			status.CompleteMemberHours += completed
			if completed < status.CompleteHours {
				status.CompleteHours = completed
			}
		}
		if status.ExpectedMemberHours > 0 {
			status.CoveragePercent = float64(status.CompleteMemberHours) * 100 / float64(status.ExpectedMemberHours)
		}
	} else if len(memberStates) == 0 && status.ExpectedHours > 0 {
		// 兼容早期单水位快照：只供本地迁移/验收读取；新同步器一旦运行便会
		// 创建成员水位，生产切换不依赖此分支。
		if err := m.usageFactsStore().WithContext(qctx).Model(&UsageHourIngestState{}).
			Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND content_hash <> ''", start, end, "complete").
			Count(&status.CompleteHours).Error; err != nil {
			return status, fmt.Errorf("统计旧版事实覆盖率失败: %w", err)
		}
		status.CompleteMemberHours = status.CompleteHours * int64(status.TrackedUsers)
		status.CoveragePercent = float64(status.CompleteHours) * 100 / float64(status.ExpectedHours)
		status.CoverageBasis = "legacy_global_hour_proof"
		memberWindowOK = syncState.MemberFingerprint == portalMemberFingerprintFromIDs(idsOf(tracked))
	}
	status.MembershipSynchronized = memberWindowOK &&
		portalMemberFingerprintFromIDs(activeIDs) == portalMemberFingerprintFromIDs(idsOf(tracked)) &&
		syncState.MemberFingerprint == portalMemberFingerprintFromIDs(idsOf(tracked))
	if len(memberStates) == 0 {
		status.MembershipSynchronized = status.TrackedUsers > 0 &&
			syncState.MemberFingerprint == portalMemberFingerprintFromIDs(idsOf(tracked))
	}
	status.FactCoverageReady = status.MembershipSynchronized && status.ExpectedMemberHours > 0 &&
		status.CompleteMemberHours == status.ExpectedMemberHours && status.PendingUsers == 0
	// 本机快照必须通过与线上相同的完整性校验，才能被页面当作事实来源。
	// 仅有一个同步水位不能说明中间没有漏小时；若放宽这里，本机验收会把不完整
	// 的数据伪装成正常统计，反而掩盖问题。LocalSnapshotOnly 永远不会回连来源库补洞。
	var publishedIDs []int64
	if syncState.PublishedAt > 0 {
		if err := m.usageFactsStore().WithContext(qctx).Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &publishedIDs).Error; err != nil {
			return status, fmt.Errorf("读取已发布成员失败: %w", err)
		}
	}
	status.ServingUsers = len(publishedIDs)
	publishedUsable := syncState.TrafficClassVersion == userTrafficClassificationVersion &&
		syncState.PublishedAt > 0 && syncState.PublishedRangeStart > 0 &&
		syncState.PublishedThrough > syncState.PublishedRangeStart &&
		syncState.PublishedWindowDays > 0 && syncState.PublishedFingerprint != "" &&
		syncState.PublishedFingerprint == portalMemberFingerprintFromIDs(publishedIDs)
	// 旧版快照没有成员×自然日的内容证明，SQLite quick_check 即使通过，
	// 也无法发现日事实行被逻辑删除。在状态中显式标出可迁移范围，由
	// proof_migration 受控重算补齐；不会通过空证明行“自证正确”。
	if publishedUsable && len(publishedIDs) > 0 {
		firstDay, throughDay := usageFactSemanticFullDayRange(syncState.PublishedRangeStart, syncState.PublishedThrough)
		if firstDay < throughDay {
			status.ProofMigrationFrom = firstDay
			status.ProofMigrationThrough = throughDay
			status.ExpectedMemberDays = (throughDay - firstDay) / usageFactDaySeconds * int64(len(publishedIDs))
			inSQL, inArgs := usageIn("user_id", publishedIDs)
			countArgs := append([]any{firstDay, throughDay}, inArgs...)
			countArgs = append(countArgs, "")
			if err := m.usageFactsStore().WithContext(qctx).Model(&UsageFactMemberDayState{}).
				Where("date_ts >= ? AND date_ts < ? AND "+inSQL+" AND content_hash <> ?", countArgs...).
				Count(&status.CompleteMemberDays).Error; err != nil {
				return status, fmt.Errorf("统计日事实语义证明失败: %w", err)
			}
			status.ProofMigrationRequired = status.CompleteMemberDays != status.ExpectedMemberDays
		}
	}
	// 在新版服务快照尚未创建前，本机离线快照仍使用候选覆盖率做严格校验；
	// 一旦已经有发布快照，后续候选回填不再影响页面可用性。
	status.SnapshotUsable = status.SnapshotReadOnly && (publishedUsable || status.FactCoverageReady)
	status.ReadActive = status.ReadEnabled && status.SemanticAuditOK &&
		(publishedUsable || (status.SnapshotReadOnly && status.FactCoverageReady))
	return status, nil
}

func (m *Monitor) populateUsageFactsFullHistoryStatus(
	ctx context.Context,
	status UsageFactsStatus,
	syncState UsageFactSyncState,
) (UsageFactsStatus, error) {
	progress, err := m.usageFactHistoryProgress(ctx, time.Now())
	if err != nil {
		return status, fmt.Errorf("读取全历史事实进度失败: %w", err)
	}
	status.FullHistory = &progress
	status.HistoryWorkerRestarts = m.usageFactsHistoryRestarts.Load()
	status.TrackedUsers = progress.TotalMembers
	status.ServingUsers = progress.PublishedMembers
	status.PendingUsers = progress.PendingMembers
	status.ExpectedHours = 0 // per-member boundaries differ; global window hours are meaningless
	status.CompleteHours = 0
	status.ExpectedMemberHours = progress.TotalHours
	status.CompleteMemberHours = progress.CompletedHours
	status.CoveragePercent = progress.CoveragePercent
	status.CoverageBasis = "per_member_full_history_job"
	status.FactCoverageReady = progress.TotalMembers > 0 && progress.ReadyMembers == progress.TotalMembers &&
		progress.PausedMembers == 0 && progress.FailedMembers == 0
	status.BackfillDays = 0
	if syncState.PublishedRangeStart > 0 && syncState.PublishedThrough > syncState.PublishedRangeStart {
		status.RangeStart = time.Unix(syncState.PublishedRangeStart, 0).In(usageCST).Format("2006-01-02 15:04 MST")
		status.RangeEnd = time.Unix(syncState.PublishedThrough-usageFactHourSeconds, 0).In(usageCST).Format("2006-01-02 15:04 MST")
	}
	var published []UsageFactPublishedMember
	if err := m.usageFactsStore().WithContext(ctx).Order("user_id").Find(&published).Error; err != nil {
		return status, fmt.Errorf("读取全历史发布成员失败: %w", err)
	}
	ids := make([]int64, 0, len(published))
	for _, row := range published {
		ids = append(ids, row.UserID)
	}
	publishedUsable := len(published) > 0 && syncState.TrafficClassVersion == userTrafficClassificationVersion &&
		syncState.PublishedAt > 0 && syncState.PublishedRangeStart > 0 &&
		syncState.PublishedThrough > syncState.PublishedRangeStart &&
		syncState.PublishedFingerprint == portalMemberFingerprintFromIDs(ids)
	status.MembershipSynchronized = publishedUsable && progress.PublishedMembers == len(published)
	status.SnapshotUsable = publishedUsable
	status.ReadActive = status.ReadEnabled && publishedUsable && m.usageFactsReadReady.Load()
	// Full-history uses durable per-member verification, so the legacy global
	// member×day proof matrix and its migration counters are intentionally not
	// queried here. This keeps the status endpoint O(members+jobs).
	status.ExpectedMemberDays = 0
	status.CompleteMemberDays = 0
	status.ProofMigrationRequired = false
	return status, nil
}

// refreshUsageFactsReadiness 只读取 Monitor SQLite，启动、历史回填完成或名单变化
// 后刷新切读许可。它从不访问主站 logs/users/tokens，因此不会给生产库增加额外负担。
func (m *Monitor) refreshUsageFactsReadiness(ctx context.Context, now time.Time) {
	// Shadow 阶段也允许把完整候选原子发布为“已核验但未服务”的快照。
	// 发布只写 Monitor SQLite；status.ReadActive 仍严格要求 ReadEnabled，
	// 因而不会在运维显式切读前改变页面读源。
	if !m.usageFactsEnabled() && !m.usageFactsLocalSnapshotReadOnly() {
		m.setUsageFactsPublishedReadiness(false, 0, 0)
		return
	}
	if m.usageFactsFullHistoryMode() && !m.usageFactsLocalSnapshotReadOnly() {
		// Authority/signature cleanup is SQLite-only and must run even when the
		// source is offline. Otherwise a removed/rejoined member left in the
		// publication table can make every unrelated customer's restart audit fail.
		if _, _, err := m.reconcileUsageFactPublishedMembersLocal(ctx); err != nil {
			m.setUsageFactsPublishedReadiness(false, 0, 0)
			slog.Warn("清理失效的全历史发布成员失败，暂不切读", "err", err)
			return
		}
	}
	status, err := m.usageFactsStatus(ctx, now)
	if err != nil {
		// 本地状态本身无法读取时不能安全继续服务。源库/Redis 故障不会
		// 走到这里，它们只会让候选同步失败，已发布 SQLite 快照仍可读。
		m.setUsageFactsPublishedReadiness(false, 0, 0)
		slog.Warn("核验本地用量事实完整性失败，暂不切读", "err", err)
		return
	}
	if m.usageFactsFullHistoryMode() {
		m.refreshUsageFactsFullHistoryReadiness(ctx, now, status)
		return
	}
	forceServingAudit := false
	if status.FactCoverageReady && !m.usageFactsLocalSnapshotReadOnly() && !m.usageFactsFullHistoryMode() {
		published, publishErr := m.publishUsageFactsSnapshot(ctx, now)
		if publishErr != nil {
			// 发布候选版失败时继续用上一个服务版，不中断页面。
			slog.Warn("发布用量事实快照失败，继续使用上一版", "err", publishErr)
			var dayIssue *usageFactMemberDayAuditError
			if errors.As(publishErr, &dayIssue) {
				repair, repairErr := m.requestUsageFactsCandidateGapRepair(ctx, dayIssue.DayTs, now)
				if repairErr == nil {
					slog.Warn("候选日事实缺口已转为持久精确修复任务",
						"day", dayIssue.DayTs, "target_members", repair.RepairTargetMembers,
						"total_member_hours", repair.RepairTotalMemberHours)
				} else if !errors.Is(repairErr, errUsageFactRepairConflict) && !errors.Is(repairErr, errUsageFactRepairNotNeeded) {
					slog.Warn("候选日事实缺口自动建单失败", "day", dayIssue.DayTs, "err", repairErr)
				}
			}
			// 候选失败可能来自候选独有成员，也可能正好暴露了旧服务版
			// 共用事实的逻辑损坏。立即强制复核旧版，不能等下一小时轮转。
			forceServingAudit = true
		} else {
			// publish 事务已返回最终服务水位，不再立即重扫一遍完整
			// member-hour 台账。大规模 366 天窗口下这一遍可达数百毫秒。
			status.PublishedAt = published.PublishedAt
			status.PublishedRangeStart = published.PublishedRangeStart
			status.PublishedThrough = published.PublishedThrough
			status.ReadActive = status.ReadEnabled && published.PublishedAt > 0 &&
				published.PublishedRangeStart > 0 && published.PublishedThrough > published.PublishedRangeStart
		}
	}
	// 已发布服务版在进程启动后必须先完成一次业务语义审计，随后至少
	// 每小时复核；不能因为成员表/水位元数据存在就直接信任日事实内容。
	// 候选发布失败时这里审计的是上一服务版，因此不会让一个坏候选拖垮
	// 仍然正确的旧快照。离线快照没有发布表时对候选完整范围执行同一校验。
	semanticReady := false
	if status.PublishedAt > 0 && status.PublishedRangeStart > 0 && status.PublishedThrough > status.PublishedRangeStart {
		var publishedIDs []int64
		qctx, cancel := usageFactQueryContext(ctx)
		err := m.usageFactsStore().WithContext(qctx).Model(&UsageFactPublishedMember{}).
			Order("user_id").Pluck("user_id", &publishedIDs).Error
		cancel()
		if err == nil && len(publishedIDs) > 0 {
			err = m.ensureUsageFactsSemanticAudit(ctx, status.PublishedRangeStart, status.PublishedThrough, publishedIDs, now, forceServingAudit)
		}
		if err != nil {
			slog.Warn("已发布用量事实语义审计失败，已停止事实读取", "err", err)
		} else if len(publishedIDs) > 0 {
			semanticReady = true
		}
	} else if status.SnapshotReadOnly && status.FactCoverageReady {
		ids, idsErr := m.trackedIDsForUsageFacts()
		if idsErr == nil && len(ids) > 0 {
			readyThrough := m.usageFactFinalizedHour(now)
			readyFrom := readyThrough - int64(m.usageFactBackfillDays())*usageFactDaySeconds
			idsErr = m.ensureUsageFactsSemanticAudit(ctx, readyFrom, readyThrough, ids, now, false)
		}
		if idsErr != nil {
			slog.Warn("本机用量事实快照语义审计失败，已停止事实读取", "err", idsErr)
		} else if len(ids) > 0 {
			semanticReady = true
		}
	}
	status.SemanticAuditOK = m.usageFactsSemanticAuditOK.Load()
	status.SemanticAuditAt = m.usageFactsSemanticAuditAt.Load()
	status.SemanticAuditFailureAt = m.usageFactsSemanticAuditFailureAt.Load()
	status.ReadActive = status.ReadEnabled && semanticReady && status.SemanticAuditOK
	readyThrough := status.PublishedThrough
	readyFrom := status.PublishedRangeStart
	if status.SnapshotReadOnly && status.FactCoverageReady && readyThrough <= 0 {
		readyThrough = m.usageFactFinalizedHour(now)
		readyFrom = readyThrough - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	}
	if status.SnapshotReadOnly && status.PublishedAt <= 0 {
		// Legacy offline fixtures may intentionally predate PublishedMember and
		// ServingGeneration. There is no writer in this mode, so their validated
		// candidate bounds can be published directly.
		m.setUsageFactsPublishedReadiness(status.ReadActive, readyFrom, readyThrough)
		return
	}
	_ = m.setUsageFactsPublishedReadinessIfCurrent(ctx, status.ReadActive, readyFrom, readyThrough)
}

func (m *Monitor) refreshUsageFactsFullHistoryReadiness(ctx context.Context, now time.Time, status UsageFactsStatus) {
	if !status.SnapshotUsable || status.PublishedRangeStart <= 0 || status.PublishedThrough <= status.PublishedRangeStart {
		m.recordUsageFactsSemanticAudit(now, errors.New("full-history published snapshot unavailable"))
		m.setUsageFactsPublishedReadiness(false, 0, 0)
		return
	}
	auditErr := m.validateUsageFactFullHistoryCheckpoint(ctx, status.PublishedThrough)
	if errors.Is(auditErr, errUsageFactLegacyPublication) {
		// During the upgrade, the old finite-window snapshot remains the service
		// version. Its bounded legacy audit is still valid and avoids taking the
		// page down merely because a new member is only partially backfilled.
		var ids []int64
		qctx, cancel := usageFactQueryContext(ctx)
		auditErr = m.usageFactsStore().WithContext(qctx).Model(&UsageFactPublishedMember{}).
			Order("user_id").Pluck("user_id", &ids).Error
		cancel()
		if auditErr == nil && len(ids) > 0 {
			auditErr = m.ensureUsageFactsSemanticAudit(ctx, status.PublishedRangeStart, status.PublishedThrough, ids, now, false)
		}
		if auditErr == nil && len(ids) == 0 {
			auditErr = errors.New("full-history serving snapshot has no members")
		}
	}
	if auditErr != nil {
		m.recordUsageFactsSemanticAudit(now, auditErr)
		m.setUsageFactsPublishedReadiness(false, 0, 0)
		slog.Warn("全历史用量事实发布签名核验失败，已停止事实读取", "err", auditErr)
		return
	}
	m.recordUsageFactsSemanticAudit(now, nil)
	_ = m.setUsageFactsPublishedReadinessIfCurrent(ctx, status.ReadEnabled,
		status.PublishedRangeStart, status.PublishedThrough)
}

// publishUsageFactsSnapshot 只在当前成员和时间窗口全部回填完成后调用。
// 成员表与发布水位在同一 SQLite 事务中替换，并用 Generation 切换
// Redis/L1 命名空间，读请求只会看到完整旧版或完整新版。
func (m *Monitor) publishUsageFactsSnapshot(ctx context.Context, now time.Time) (UsageFactSyncState, error) {
	through := m.usageFactFinalizedHour(now)
	start := through - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	publishedAt := time.Now().Unix()
	// 覆盖率只证明每个来源小时执行完成；发布前还要重新读取页面真正使用的
	// 日/小时事实并核对内容证明。持有本地同步锁直到发布事务结束，消除
	// “审计完成后、发布提交前”被 Tail/reconcile 改写的窗口。
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	controlBefore, err := m.loadUsageMemberControlSnapshot(qctx)
	if err != nil {
		return UsageFactSyncState{}, err
	}
	ids := idsOf(controlBefore.Tracked)
	fingerprint := portalMemberFingerprintFromIDs(ids)
	var current UsageFactSyncState
	if err := m.usageFactsStore().WithContext(qctx).First(&current, 1).Error; err != nil {
		return UsageFactSyncState{}, err
	}
	if current.TrafficClassVersion != userTrafficClassificationVersion ||
		current.MemberFingerprint != fingerprint || current.BackfillWindowDays != m.usageFactBackfillDays() {
		return UsageFactSyncState{}, errors.New("候选用量快照在发布前已变化")
	}
	// Tail 后的 readiness 刷新可能每 5 分钟抵达这里。若候选与当前发布版
	// 完全相同，只返回已有元数据；周期分片审计由 refresh 随后统一执行。
	// 不能在发现“无需发布”前先全扫 366 天日事实。
	if current.PublishedFingerprint == fingerprint && current.PublishedWindowDays == m.usageFactBackfillDays() &&
		current.PublishedRangeStart == start && current.PublishedThrough == through {
		var publishedRows []UsageFactPublishedMember
		if err := m.usageFactsStore().WithContext(qctx).Order("user_id").Find(&publishedRows).Error; err != nil {
			return UsageFactSyncState{}, err
		}
		compatible := len(publishedRows) == len(ids)
		for _, row := range publishedRows {
			control, ok := controlBefore.Controls[row.UserID]
			if !ok || !usageFactPublishedMemberCompatible(row, control) {
				compatible = false
				break
			}
		}
		if compatible {
			return current, nil
		}
	}
	// Candidate coverage belongs to a concrete member lifecycle revision. A
	// fully covered old revision must never authorize a remove/rejoin revision.
	var memberStates []UsageFactMemberState
	if err := m.usageFactsStore().WithContext(qctx).Where("active = ?", true).Order("user_id").Find(&memberStates).Error; err != nil {
		return UsageFactSyncState{}, err
	}
	legacyGlobalProof := len(memberStates) == 0
	if legacyGlobalProof {
		for _, control := range controlBefore.Controls {
			if control.TrackedRevision != 1 {
				legacyGlobalProof = false
				break
			}
		}
	}
	if !legacyGlobalProof {
		if len(memberStates) != len(ids) {
			return UsageFactSyncState{}, errors.New("候选用量快照的成员 revision 尚未同步")
		}
		for _, state := range memberStates {
			control, ok := controlBefore.Controls[state.UserID]
			if !ok || !state.Active || state.TrackedRevision != control.TrackedRevision {
				return UsageFactSyncState{}, fmt.Errorf("候选用量快照的成员 revision 过期: user_id=%d", state.UserID)
			}
		}
	}
	if err := m.auditUsageFactSnapshot(qctx, start, through, ids); err != nil {
		m.usageFactsSemanticAuditFailureAt.Store(time.Now().Unix())
		return UsageFactSyncState{}, fmt.Errorf("候选用量事实语义审计失败: %w", err)
	}
	controlAtCommit, err := m.loadUsageMemberControlSnapshot(qctx)
	if err != nil {
		return UsageFactSyncState{}, err
	}
	if !usageMemberControlSnapshotsEqual(controlBefore, controlAtCommit) {
		return UsageFactSyncState{}, errors.New("用量成员在发布审计期间已变化")
	}
	var generation int64
	var servingGeneration int64
	var published UsageFactSyncState
	err = m.usageFactsStore().WithContext(qctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&published, 1).Error; err != nil {
			return err
		}
		// 防止完整性校验与发布之间成员/窗口发生变化。
		if published.MemberFingerprint != fingerprint || published.BackfillWindowDays != m.usageFactBackfillDays() {
			return errors.New("候选用量快照在发布前已变化")
		}
		if err := tx.Where("1 = 1").Delete(&UsageFactPublishedMember{}).Error; err != nil {
			return err
		}
		members := make([]UsageFactPublishedMember, 0, len(ids))
		for _, id := range ids {
			members = append(members, UsageFactPublishedMember{
				UserID: id, TrackedRevision: controlAtCommit.Controls[id].TrackedRevision, PublishedAt: publishedAt,
			})
		}
		if len(members) > 0 {
			if err := tx.CreateInBatches(members, usageFactProfileBatch).Error; err != nil {
				return err
			}
		}
		published.PublishedFingerprint = fingerprint
		published.PublishedWindowDays = m.usageFactBackfillDays()
		published.PublishedRangeStart = start
		published.PublishedThrough = through
		published.PublishedAt = publishedAt
		published.TrafficClassVersion = userTrafficClassificationVersion
		published.Generation++
		published.ServingGeneration++
		generation = published.Generation
		servingGeneration = published.ServingGeneration
		return tx.Save(&published).Error
	})
	if err != nil {
		return UsageFactSyncState{}, err
	}
	m.publishUsageFactGenerations(generation, servingGeneration)
	controlAfter, controlErr := m.loadUsageMemberControlSnapshot(qctx)
	if controlErr != nil || !usageMemberControlSnapshotsEqual(controlAtCommit, controlAfter) {
		if controlErr != nil {
			return UsageFactSyncState{}, controlErr
		}
		return UsageFactSyncState{}, errors.New("用量成员在发布提交期间已变化，已保留 revision 闸门并等待重发")
	}
	m.recordUsageFactsSemanticAudit(now, nil)
	return published, nil
}

// withUsageFactSourceRead 将后台事实采集纳入高优先级来源调度和现有 usageGate。
// 来源调度必须等到下一个可用窗口，不能再用 250ms 超时把 Tail 当成“本轮繁忙”
// 丢弃；Stability migration 在同一调度器中属于 low，必定给已经等待的 Tail 让路。
// 页面旧读路径和后台采集仍不会并发扫描生产库；取得来源槽后，usageGate 只短等
// 250ms，继续保留交互查询优先语义。
// 在取得闸门前后都会检查交互等待标记：只要已有页面请求排队，后台便主动让路，
// 而不依赖 channel waiter 的偶然调度顺序。
func (m *Monitor) withUsageFactSourceRead(ctx context.Context, fn func() error) error {
	if m.usageInteractiveWaiters.Load() > 0 {
		return errUsageFactSourceBusy
	}
	releaseBackground, err := m.acquireBackgroundSource(ctx)
	if err != nil {
		return err
	}
	defer releaseBackground()
	waitCtx, cancel := context.WithTimeout(ctx, usageFactSourceGateWait)
	defer cancel()
	if err := m.acquireUsageGate(waitCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return errUsageFactSourceBusy
		}
		return err
	}
	defer m.releaseUsageGate()
	if m.usageInteractiveWaiters.Load() > 0 {
		return errUsageFactSourceBusy
	}
	return fn()
}

// withUsageFactSourceQuery 为每一条后台来源查询设置独立的执行上限，并继续遵守
// usageGate 的前台优先规则。资料同步可能包含多个 200 人批次；不能让第一批的
// 耗时吞掉后续批次的全部超时时间，也不能因批次变多而放宽单条来源查询的上限。
func (m *Monitor) withUsageFactSourceQuery(ctx context.Context, fn func(context.Context) error) error {
	if m.usageFactSourceBackoffActive(time.Now()) {
		return errUsageFactSourceBusy
	}
	err := m.withUsageFactSourceRead(ctx, func() error {
		// SQL execution timeout starts only after both source and usage gates are
		// owned. A long Stability duty window therefore delays the query without
		// consuming its 20s database execution budget.
		cctx, cancel := context.WithTimeout(ctx, m.usageFactQueryTimeout())
		defer cancel()
		return fn(cctx)
	})
	m.recordUsageFactSourceResult(err)
	return err
}

// usageFactFinalizedHour 返回可安全采集的右开小时上界。延迟窗口让刚写入的日志
// 有时间落库；延迟中的数据不当成漏采，也不会被前端显示为零。
func (m *Monitor) usageFactFinalizedHour(now time.Time) int64 {
	return now.Add(-m.usageFactLag()).Truncate(time.Hour).Unix()
}

func usageFactDayStart(ts int64) int64 {
	t := time.Unix(ts, 0).In(usageCST)
	y, month, day := t.Date()
	return time.Date(y, month, day, 0, 0, 0, 0, usageCST).Unix()
}

// startUsageFactsSync 会先核验本地读取状态；只有明确开启采集时才会启动来源库
// 同步。因此本机快照验收可复用同一读取路径，却不会触发任何来源库查询。
func (m *Monitor) startUsageFactsSync(ctx context.Context) {
	// 采集模式即使处于 shadow/read=false，也要恢复已完成候选的发布状态，
	// 供切读前验收与低频历史复核使用；页面仍由 ReadEnabled 单独控制。
	if m.usageFactsEnabled() || m.usageFactsReadRequested() {
		m.refreshUsageFactsReadiness(ctx, time.Now())
	}
	if !m.usageFactsEnabled() {
		return
	}
	go m.superviseUsageFactsSync(ctx)
}

func (m *Monitor) runUsageFactsSync(ctx context.Context) {
	// 立即补最近闭合小时与资料，随后低频、单小时地补历史。每次源查询都严格串行，
	// 不与页面旧读路径抢并发，也不会连续占用生产库连接。
	started := time.Now()
	var nextFacts time.Time // Tail 启动即跑，优先补齐最新闭合小时。
	// 资料与 Tail 错开半个周期，避免启动及每五分钟形成 3B+2B 条来源 SQL 突发。
	profileInterval := m.usageFactProfileSyncInterval()
	nextProfiles := started.Add(m.usageFactJitteredDelay(profileInterval/2, 10))
	nextReconcile := started.Add(m.usageFactJitteredDelay(m.usageFactReconcileInterval(), 10))
	for {
		m.usageFactsLoopHeartbeat.Store(time.Now().Unix())
		if err := ctx.Err(); err != nil {
			return
		}
		now := time.Now()
		if nextFacts.IsZero() || !now.Before(nextFacts) {
			tailErr := m.syncUsageFactsTail(ctx, now)
			if tailErr != nil {
				if !errors.Is(tailErr, context.Canceled) && !errors.Is(tailErr, errUsageFactSourceBusy) {
					slog.Warn("用量事实近期同步失败", "err", tailErr)
				}
			}
			nextDelay := m.usageFactSyncInterval()
			if usageFactLocalStoreBusy(tailErr) {
				nextDelay = usageFactLocalBusyRetry
			}
			nextFacts = now.Add(m.usageFactJitteredDelay(nextDelay, 10))
		}
		if nextProfiles.IsZero() || !now.Before(nextProfiles) {
			if err := m.syncUsageProfiles(ctx, now); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errUsageFactSourceBusy) {
				slog.Warn("用量资料快照同步失败", "err", err)
			}
			nextProfiles = now.Add(m.usageFactJitteredDelay(profileInterval, 10))
		}

		worked := false
		var err error
		if !m.usageFactsFullHistoryEnabled() {
			worked, err = m.syncNextUsageFactHour(ctx, now)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errUsageFactSourceBusy) {
				slog.Warn("用量事实历史补齐失败", "err", err)
			}
		}
		if worked {
			m.usageFactsLoopHeartbeat.Store(time.Now().Unix())
			if !waitUsageFact(ctx, m.usageFactJitteredDelay(m.usageFactBackfillDelay(), 20)) {
				return
			}
			continue
		}
		if !m.usageFactsFullHistoryEnabled() && !now.Before(nextReconcile) {
			_, err = m.reconcileNextUsageFactHour(ctx, now)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errUsageFactSourceBusy) {
				slog.Warn("用量事实历史复核失败", "err", err)
			}
			nextReconcile = now.Add(m.usageFactJitteredDelay(m.usageFactReconcileInterval(), 10))
		}

		wake := nextFacts
		if nextProfiles.Before(wake) {
			wake = nextProfiles
		}
		if !m.usageFactsFullHistoryEnabled() && nextReconcile.Before(wake) {
			wake = nextReconcile
		}
		wait := time.Until(wake)
		if wait < time.Second {
			wait = time.Second
		}
		if wait > time.Minute {
			wait = time.Minute // 响应退出，不做长时间不可取消休眠
		}
		if !waitUsageFact(ctx, wait) {
			return
		}
	}
}

func waitUsageFact(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ensureUsageFactMembership 维护按成员拆分的历史回填游标。新增成员只创建该成员
// 的候选水位；已有成员的游标、小时证明和事实均不动；移除成员只停用水位，
// 权限读取会立即通过“当前名单 ∩ 已发布成员”排除它。这样成员变化不会再触发
// 整个组织 366 天历史重扫。
func (m *Monitor) ensureUsageFactMembership(ids []int64) error {
	end := m.usageFactFinalizedHour(time.Now())
	start := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	return m.ensureUsageFactMembershipAt(ids, start)
}

func (m *Monitor) ensureUsageFactMembershipAt(ids []int64, start int64) error {
	qctx, cancel := usageFactQueryContext(context.Background())
	defer cancel()
	return m.ensureUsageFactMembershipAtContext(qctx, ids, start)
}

func (m *Monitor) ensureUsageFactMembershipAtContext(ctx context.Context, ids []int64, start int64) error {
	if start <= 0 {
		return errors.New("用量事实成员回填起点无效")
	}
	controlBefore, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	ordered := append([]int64(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	unique := ordered[:0]
	for _, id := range ordered {
		if id <= 0 || (len(unique) > 0 && unique[len(unique)-1] == id) {
			continue
		}
		unique = append(unique, id)
	}
	ordered = unique
	if portalMemberFingerprintFromIDs(ordered) != portalMemberFingerprintFromIDs(idsOf(controlBefore.Tracked)) {
		return fmt.Errorf("%w: facts membership input is not the current active projection", errUsageMemberControlIntegrity)
	}
	fingerprint := portalMemberFingerprintFromIDs(ordered)
	days := m.usageFactBackfillDays()
	nowUnix := time.Now().Unix()
	var generation int64
	changed := false
	var allControls []UsageMemberControl
	if err := m.storeDB.WithContext(ctx).Find(&allControls).Error; err != nil {
		return err
	}
	allControlByID := make(map[int64]UsageMemberControl, len(allControls))
	for _, control := range allControls {
		allControlByID[control.UserID] = control
	}
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var global UsageFactSyncState
		if err := tx.First(&global, 1).Error; err != nil {
			return err
		}
		var existing []UsageFactMemberState
		if err := tx.Find(&existing).Error; err != nil {
			return err
		}
		byID := make(map[int64]UsageFactMemberState, len(existing))
		wanted := make(map[int64]struct{}, len(ordered))
		for _, row := range existing {
			byID[row.UserID] = row
		}
		for _, id := range ordered {
			wanted[id] = struct{}{}
			row, found := byID[id]
			control := controlBefore.Controls[id]
			rowChanged := false
			membershipReset := false
			if !found {
				row = UsageFactMemberState{
					UserID: id, NextBackfillHour: start, TrackedRevision: control.TrackedRevision,
					SourceHistoryStatus: "discovering", CoverageStatus: "discovering",
					ClassificationVersion: userTrafficClassificationVersion,
					QuerySemanticsVersion: usageFactQuerySemanticsVersion,
				}
				rowChanged = true
				membershipReset = true
			}
			legacyRevision := found && row.TrackedRevision == 0 && control.TrackedRevision == 1
			revisionChanged := found && row.TrackedRevision > 0 && row.TrackedRevision != control.TrackedRevision
			if found && (!row.Active || revisionChanged) {
				// Rejoin resumes at the hour in which removal happened. Facts and
				// coverage before that point remain immutable; only this member's
				// inactive gap is replayed. The lifecycle revision keeps it hidden
				// until that replay and later source verification complete.
				gapStart := row.NextBackfillHour
				if row.CoverageThroughHour != nil && *row.CoverageThroughHour > 0 {
					gapStart = *row.CoverageThroughHour
				}
				if control.LastDeactivatedAt > 0 {
					deactivatedHour := control.LastDeactivatedAt / usageFactHourSeconds * usageFactHourSeconds
					if gapStart <= 0 || deactivatedHour < gapStart {
						gapStart = deactivatedHour
					}
				}
				if gapStart <= 0 {
					gapStart = start
				}
				row.NextBackfillHour = gapStart
				row.LastFailureAt = 0
				row.LastError = ""
				row.CoverageStatus = "backfilling"
				rowChanged = true
				membershipReset = true
			}
			if row.TrackedRevision != control.TrackedRevision {
				row.TrackedRevision = control.TrackedRevision
				rowChanged = true
			}
			if row.ClassificationVersion == 0 {
				row.ClassificationVersion = userTrafficClassificationVersion
				rowChanged = true
			}
			if row.QuerySemanticsVersion == 0 {
				row.QuerySemanticsVersion = usageFactQuerySemanticsVersion
				rowChanged = true
			}
			if legacyRevision && row.CoverageStatus == "" {
				// Additive upgrade only: retain current serving/cursors. Legacy day
				// proofs are still not all-history-ready until source hashes exist.
				row.CoverageStatus = "backfilling"
				rowChanged = true
			}
			if row.BackfillWindowDays != days || row.RangeStart != start {
				// 历史窗口每小时自然向前滑动。左边界向后移时只丢弃已经
				// 离开窗口的待办，绝不能把已前进的游标重置回起点；否则 366 天
				// 回填会在每个整点重启，永远无法发布。只有左边界真正向更早
				// 扩展（例如回填天数从 30 改为 90）时，才从新起点补历史。
				if !membershipReset && (row.RangeStart <= 0 || start < row.RangeStart) {
					row.NextBackfillHour = start
				} else if !membershipReset && row.NextBackfillHour < start {
					row.NextBackfillHour = start
				}
				row.BackfillWindowDays = days
				row.RangeStart = start
				rowChanged = true
			}
			if rowChanged {
				row.Active = true
				row.UpdatedAt = nowUnix
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
				changed = true
			}
		}
		for _, row := range existing {
			if _, keep := wanted[row.UserID]; keep || !row.Active {
				continue
			}
			row.Active = false
			if control, ok := allControlByID[row.UserID]; ok && control.TrackedRevision >= row.TrackedRevision {
				row.TrackedRevision = control.TrackedRevision
			}
			row.CoverageStatus = "inactive"
			row.UpdatedAt = nowUnix
			if err := tx.Save(&row).Error; err != nil {
				return err
			}
			changed = true
		}
		if global.MemberFingerprint != fingerprint || global.BackfillWindowDays != days {
			global.MemberFingerprint = fingerprint
			global.BackfillWindowDays = days
			global.NextReconcileHour = start
			global.LastReconciledHour = 0
			global.LastReconcileAt = 0
			global.LastReconcileFailureAt = 0
			changed = true
		}
		var next sql.NullInt64
		if err := tx.Model(&UsageFactMemberState{}).Select("MIN(next_backfill_hour)").
			Where("active = ?", true).Scan(&next).Error; err != nil {
			return err
		}
		nextValue := int64(0)
		if next.Valid {
			nextValue = next.Int64
		}
		globalStateChanged := global.NextBackfillHour != nextValue
		if globalStateChanged {
			global.NextBackfillHour = nextValue
		}
		if changed {
			global.Generation++
		}
		generation = global.Generation
		if !changed && !globalStateChanged {
			return nil
		}
		return tx.Save(&global).Error
	})
	if err == nil && changed {
		m.publishUsageFactGenerations(generation, 0)
	}
	if err != nil {
		return err
	}
	controlAfter, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	if !usageMemberControlSnapshotsEqual(controlBefore, controlAfter) {
		return fmt.Errorf("%w: member manifest changed while mirroring facts state", errUsageMemberControlIntegrity)
	}
	return nil
}

func (m *Monitor) trackedIDsForUsageFacts() ([]int64, error) {
	qctx, cancel := usageFactQueryContext(context.Background())
	defer cancel()
	return m.trackedIDsForUsageFactsContext(qctx)
}

func (m *Monitor) trackedIDsForUsageFactsContext(ctx context.Context) ([]int64, error) {
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return idsOf(snapshot.Tracked), nil
}

// syncUsageFactsTail 回读最近三个已闭合小时。NewAPI 日志可能稍晚落库；有限回读
// 既修正迟到记录，也避免把全量历史反复扫一遍。
func (m *Monitor) syncUsageFactsTail(ctx context.Context, now time.Time) error {
	if err := m.ensureUsageFactDerivedWritesCapacity(); err != nil {
		return err
	}
	if m.usageFactsFullHistoryEnabled() && m.usageFactsRawPageImportEnabled() {
		// Raw mode has its own durable per-member live cursor. Do not also run the
		// legacy source-side GROUP BY Tail: two owners would duplicate source work
		// and let a slow batch hide which member is actually stale.
		if err := m.reconcileUsageFactHistoryJobs(ctx, now); err != nil {
			return err
		}
		var tailErr error
		if err := m.syncUsageFactsRawLiveTail(ctx, now); err != nil {
			tailErr = errors.Join(tailErr, err)
		}
		if _, err := m.publishUsageFactFullHistorySnapshot(ctx, now); err != nil {
			tailErr = errors.Join(tailErr, err)
		}
		if err := m.pruneUsageFacts(now); err != nil {
			tailErr = errors.Join(tailErr, err)
		}
		return tailErr
	}
	ids, err := m.trackedIDsForUsageFactsContext(ctx)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	end := m.usageFactFinalizedHour(now)
	if m.usageFactsFullHistoryEnabled() {
		// The full-history reconciler is the sole owner of revision transitions.
		// Letting the legacy fixed-window mirror advance a rejoin revision first
		// would hide the revision change and skip the mandatory source re-audit.
		if err := m.reconcileUsageFactHistoryJobs(ctx, now); err != nil {
			return err
		}
	} else {
		windowStart := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
		if err := m.ensureUsageFactMembershipAtContext(ctx, ids, windowStart); err != nil {
			return err
		}
	}
	start := end - 3*usageFactHourSeconds
	var tailErr error
	for hour := start; hour < end; hour += usageFactHourSeconds {
		if hour <= 0 {
			continue
		}
		result, syncErr := m.syncUsageFactHourBatchedWithOptions(ctx, hour, ids, usageFactHourSyncOptions{
			recordFailure:       true,
			updateLastFactSync:  true,
			invalidateNoHistory: true,
		})
		if syncErr != nil {
			tailErr = errors.Join(tailErr, syncErr)
		}
		invalidated := make(map[int64]bool, len(result.InvalidatedNoHistoryUserIDs))
		for _, id := range result.InvalidatedNoHistoryUserIDs {
			invalidated[id] = true
		}
		finalizeIDs := make([]int64, 0, len(result.SucceededUserIDs))
		for _, id := range result.SucceededUserIDs {
			if !invalidated[id] {
				finalizeIDs = append(finalizeIDs, id)
			}
		}
		if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, hour, finalizeIDs, false); err != nil {
			tailErr = errors.Join(tailErr, err)
		}
	}
	// 历史窗口扩容期间，页面仍在读取上一个已发布服务版。
	// Tail 已经把新闭合小时串行采集、本地核验并标记为 complete；
	// 当候选名单与服务版名单一致、且中间没有缺小时时，可以只向前
	// 推进服务版的右侧水位，并按该服务版自己的窗口天数同步滑动左界；
	// 90 天候选数据仍要全部通过完整性校验后才能原子发布。
	if m.usageFactsFullHistoryEnabled() {
		if _, err := m.publishUsageFactFullHistorySnapshot(ctx, now); err != nil {
			tailErr = errors.Join(tailErr, err)
		}
	} else if err := m.advanceUsageFactsPublishedTail(ctx, end); err != nil {
		tailErr = errors.Join(tailErr, err)
	}
	if err := m.pruneUsageFacts(now); err != nil {
		tailErr = errors.Join(tailErr, err)
	}
	return tailErr
}

// advanceUsageFactsPublishedTail 只在 Monitor SQLite 内推进已发布服务版的
// 右侧水位。它不读取生产库，也不会把候选版更长的历史范围提前暴露；
// 左边界按当前服务版自己的 PublishedWindowDays 等长向前滑动。
func (m *Monitor) advanceUsageFactsPublishedTail(ctx context.Context, through int64) error {
	if through <= 0 {
		return nil
	}
	if m.usageFactsFullHistoryEnabled() {
		// Full-history has a permanent source floor. The legacy helper derives a
		// moving left edge from PublishedWindowDays and must never run in this
		// mode; syncUsageFactsTail invokes the full-history publisher instead.
		return nil
	}
	controlBefore, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	var generation int64
	var servingGeneration int64
	var published UsageFactSyncState
	var advanced bool
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&published, 1).Error; err != nil {
			return err
		}
		if published.PublishedAt <= 0 || published.PublishedRangeStart <= 0 ||
			published.PublishedThrough <= published.PublishedRangeStart || through <= published.PublishedThrough ||
			published.PublishedFingerprint == "" {
			return nil
		}

		// 发布成员表必须仍与发布指纹一致；否则宁可保留旧水位。
		var publishedRows []UsageFactPublishedMember
		if err := tx.Order("user_id").Find(&publishedRows).Error; err != nil {
			return err
		}
		publishedIDs := make([]int64, 0, len(publishedRows))
		for _, row := range publishedRows {
			control, ok := controlBefore.Controls[row.UserID]
			if !ok || !usageFactPublishedMemberCompatible(row, control) {
				return nil
			}
			publishedIDs = append(publishedIDs, row.UserID)
		}
		if portalMemberFingerprintFromIDs(publishedIDs) != published.PublishedFingerprint {
			return nil
		}

		expectedHours := (through - published.PublishedThrough) / usageFactHourSeconds
		if expectedHours <= 0 || published.PublishedThrough+expectedHours*usageFactHourSeconds != through || len(publishedIDs) == 0 {
			return nil
		}
		inSQL, inArgs := usageIn("user_id", publishedIDs)
		countArgs := append([]any{published.PublishedThrough, through}, inArgs...)
		countArgs = append(countArgs, "complete")
		var complete int64
		if err := tx.Model(&UsageFactMemberHourState{}).
			Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> ''", countArgs...).
			Count(&complete).Error; err != nil {
			return err
		}
		if complete != expectedHours*int64(len(publishedIDs)) {
			return nil
		}

		published.PublishedThrough = through
		published.PublishedRangeStart = through - int64(published.PublishedWindowDays)*usageFactDaySeconds
		published.PublishedAt = time.Now().Unix()
		published.Generation++
		published.ServingGeneration++
		generation = published.Generation
		servingGeneration = published.ServingGeneration
		advanced = true
		return tx.Save(&published).Error
	})
	if err != nil {
		return err
	}
	if advanced {
		m.publishUsageFactGenerations(generation, servingGeneration)
		m.publishUsageFactReadBoundsAfterMutation(published)
		controlAfter, controlErr := m.loadUsageMemberControlSnapshot(ctx)
		if controlErr != nil {
			return controlErr
		}
		if !usageMemberControlSnapshotsEqual(controlBefore, controlAfter) {
			return fmt.Errorf("%w: member manifest changed while advancing facts tail", errUsageMemberControlIntegrity)
		}
	}
	return nil
}

// syncNextUsageFactHour 每轮只补一个小时，但把处于同一游标的新增成员合成
// 一条 IN 查询。已有成员的游标不会因名单变化回退，因此添加一个用户只会
// 扫描这个用户的历史，不会重扫整个组织。
func (m *Monitor) syncNextUsageFactHour(ctx context.Context, now time.Time) (bool, error) {
	ids, err := m.trackedIDsForUsageFactsContext(ctx)
	if err != nil {
		return false, err
	}
	if len(ids) == 0 {
		return false, nil
	}
	end := m.usageFactFinalizedHour(now)
	backfillDays := m.usageFactBackfillDays()
	start := end - int64(backfillDays)*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAtContext(ctx, ids, start); err != nil {
		return false, err
	}
	var first UsageFactMemberState
	err = m.usageFactsStore().Where("active = ? AND next_backfill_hour < ?", true, end).
		Order("next_backfill_hour, user_id").First(&first).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 游标已全部到达尾部后，稳态缺口审计最多每小时一次。原先
		// 每分钟按成员扫描整个 366 天台账，会与页面事实查询争抢
		// SQLite CPU/IO。发现缺口仍立即回退单成员游标，不影响自愈。
		lastAudit := m.usageFactsGapAuditAt.Load()
		if lastAudit > 0 && now.Unix()-lastAudit < int64(usageFactGapAuditInterval/time.Second) {
			return false, nil
		}
		m.usageFactsGapAuditAt.Store(now.Unix())
		gapUser, gapHour, found, gapErr := m.findEarliestUsageFactMemberHourGap(start, end)
		if gapErr != nil {
			return false, gapErr
		}
		if found {
			if err := m.rewindUsageFactMemberBackfillCursor(gapUser, gapHour); err != nil {
				return false, err
			}
			return true, nil
		}
		m.refreshUsageFactsReadiness(ctx, now)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	hour := first.NextBackfillHour
	if hour < start {
		hour = start
	}
	if first.LastFailureAt > now.Add(-5*time.Minute).Unix() {
		return false, nil
	}
	var batch []UsageFactMemberState
	if err := m.usageFactsStore().Where("active = ? AND next_backfill_hour = ?", true, first.NextBackfillHour).
		Order("user_id").Limit(usageFactProfileBatch).Find(&batch).Error; err != nil {
		return false, err
	}
	batchIDs := make([]int64, 0, len(batch))
	for _, member := range batch {
		batchIDs = append(batchIDs, member.UserID)
	}
	if len(batchIDs) == 0 {
		return false, nil
	}
	if err := m.usageFactBatchRevisionCurrent(ctx, batch); err != nil {
		return true, err
	}
	// Tail、重启恢复或扩窗可能已经为游标前方留下完整的成员小时证明。先在本地
	// 一次取最多一周的连续证明并核对事实指纹，完整部分只推进成员游标，不再
	// 重复查询来源 logs。遇到第一个缺失/逻辑不一致小时便停止，交给正常同步修复。
	through, skipped, err := m.verifiedUsageFactMemberBackfillThrough(hour, end, batchIDs, usageFactBackfillSkipBatch)
	if err != nil {
		return false, err
	}
	if skipped {
		if err := m.usageFactBatchRevisionCurrent(ctx, batch); err != nil {
			return true, err
		}
		if err := m.setUsageFactMemberBackfillCursor(batchIDs, through); err != nil {
			return false, err
		}
		return true, nil
	}
	err = m.syncUsageFactHour(ctx, hour, batchIDs)
	if errors.Is(err, errUsageFactSourceBusy) {
		// 前台仍在读取生产库时，历史回填不应每个 backfillDelay 重试。
		// 等下一个周期再尝试，避免形成后台忙循环。
		return false, nil
	}
	if err != nil {
		m.recordUsageFactRepairFailure(hour, err)
		return true, err
	}
	if err := m.usageFactBatchRevisionCurrent(ctx, batch); err != nil {
		return true, err
	}
	if err := m.setUsageFactMemberBackfillCursor(batchIDs, hour+usageFactHourSeconds); err != nil {
		return false, err
	}
	return true, nil
}

// verifiedUsageFactMemberBackfillThrough 只读取本地事实专库。返回值 through 是
// [start,through) 内每个成员都已由完整 proof+内容 hash 证明的第一个未跳过小时。
func (m *Monitor) verifiedUsageFactMemberBackfillThrough(start, end int64, ids []int64, maxHours int) (through int64, skipped bool, err error) {
	if start <= 0 || end <= start || len(ids) == 0 || maxHours <= 0 {
		return start, false, nil
	}
	limit := end
	if candidate := start + int64(maxHours)*usageFactHourSeconds; candidate < limit {
		limit = candidate
	}
	inSQL, inArgs := usageIn("user_id", ids)
	stateArgs := append([]any{start, limit}, inArgs...)
	var states []UsageFactMemberHourState
	if err := m.usageFactsStore().Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> ''",
		append(stateArgs, "complete")...).Order("hour_ts, user_id").Find(&states).Error; err != nil {
		return start, false, err
	}
	if len(states) == 0 {
		return start, false, nil
	}
	stateByHour := make(map[int64]map[int64]UsageFactMemberHourState)
	for _, state := range states {
		if stateByHour[state.HourTs] == nil {
			stateByHour[state.HourTs] = make(map[int64]UsageFactMemberHourState)
		}
		stateByHour[state.HourTs][state.UserID] = state
	}
	factArgs := append([]any{start, limit}, inArgs...)
	var factRows []UsageHourFact
	if err := m.usageFactsStore().Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL, factArgs...).
		Order("hour_ts, user_id, channel_id, grp, model_name, token_id").Find(&factRows).Error; err != nil {
		return start, false, err
	}
	// 小时明细只保留近期窗口；90→366 天扩窗时，原有 90 天的
	// member-hour proof 仍在，但早期小时事实已合法清理。若只核对
	// 小时行，会把整段已发布历史重读一遍来源库。完整自然日可用
	// 24 小时覆盖台账 + 成员日语义 proof + 当前日事实内容做等价核验。
	// 这不会“自证”旧数据：缺少 proof、24 小时台账或日事实被改坏
	// 都会使 fallback 失效，立即回到受限的来源修复路径。
	dailyFallbacks, err := m.verifiedUsageFactDailyFallbacks(start, limit, ids)
	if err != nil {
		return start, false, err
	}
	type memberHourKey struct{ userID, hourTs int64 }
	rowsByMemberHour := make(map[memberHourKey][]UsageHourFact, len(states))
	for _, row := range factRows {
		key := memberHourKey{userID: row.UserID, hourTs: row.HourTs}
		rowsByMemberHour[key] = append(rowsByMemberHour[key], row)
	}
	for hour := start; hour < limit; hour += usageFactHourSeconds {
		byUser := stateByHour[hour]
		if len(byUser) != len(ids) {
			break
		}
		valid := true
		for _, id := range ids {
			state, ok := byUser[id]
			memberRows := rowsByMemberHour[memberHourKey{userID: id, hourTs: hour}]
			hourlyValid := ok && usageFactMemberMetricsMatchState(factsMetrics(memberRows), state) &&
				usageFactContentHash(memberRows) == state.ContentHash
			if !hourlyValid && !dailyFallbacks[usageFactMemberDayKey{userID: id, dayTs: usageFactDayStart(hour)}] {
				valid = false
				break
			}
		}
		if !valid {
			break
		}
		through = hour + usageFactHourSeconds
		skipped = true
	}
	if !skipped {
		through = start
	}
	return through, skipped, nil
}

// verifiedUsageFactDailyFallbacks 返回可以替代已清理小时明细的
// 成员×自然日。只读 Monitor SQLite，单次最多覆盖 backfill skip 的一周。
func (m *Monitor) verifiedUsageFactDailyFallbacks(start, end int64, ids []int64) (map[usageFactMemberDayKey]bool, error) {
	valid := make(map[usageFactMemberDayKey]bool)
	if start <= 0 || end <= start || len(ids) == 0 {
		return valid, nil
	}
	dayStart := usageFactDayStart(start)
	dayEnd := usageFactDayStart(end-1) + usageFactDaySeconds
	inSQL, inArgs := usageIn("user_id", ids)

	stateArgs := append([]any{dayStart, dayEnd}, inArgs...)
	stateArgs = append(stateArgs, "complete", "")
	var hourStates []UsageFactMemberHourState
	if err := m.usageFactsStore().Where(
		"hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> ?", stateArgs...).
		Order("hour_ts, user_id").Find(&hourStates).Error; err != nil {
		return nil, err
	}
	type dayCoverage struct {
		hours    int
		requests int64
		tokens   int64
	}
	coverage := make(map[usageFactMemberDayKey]dayCoverage)
	for _, state := range hourStates {
		key := usageFactMemberDayKey{userID: state.UserID, dayTs: usageFactDayStart(state.HourTs)}
		item := coverage[key]
		item.hours++
		item.requests += state.Requests
		item.tokens += state.Tokens
		coverage[key] = item
	}

	proofArgs := append([]any{dayStart, dayEnd}, inArgs...)
	proofArgs = append(proofArgs, "")
	var proofs []UsageFactMemberDayState
	if err := m.usageFactsStore().Where(
		"date_ts >= ? AND date_ts < ? AND "+inSQL+" AND content_hash <> ?", proofArgs...).
		Order("date_ts, user_id").Find(&proofs).Error; err != nil {
		return nil, err
	}
	dailyRows, err := loadUsageDailyFacts(m.usageFactsStore(), dayStart, dayEnd, ids)
	if err != nil {
		return nil, err
	}
	rowsByDay := usageDailyFactsByMemberDay(dailyRows)
	for _, proof := range proofs {
		key := usageFactMemberDayKey{userID: proof.UserID, dayTs: proof.DateTs}
		covered := coverage[key]
		rows := rowsByDay[key]
		metrics := dailyFactsMetrics(rows)
		if covered.hours == 24 && covered.requests == proof.Requests && covered.tokens == proof.Tokens &&
			usageFactMemberDayMetricsMatchState(metrics, proof) && usageDailyFactContentHash(rows) == proof.ContentHash {
			valid[key] = true
		}
	}
	return valid, nil
}

func (m *Monitor) setUsageFactMemberBackfillCursor(ids []int64, hourTs int64) error {
	if len(ids) == 0 {
		return nil
	}
	inSQL, args := usageIn("user_id", ids)
	updateArgs := append([]any{hourTs, time.Now().Unix()}, args...)
	var generation int64
	var servingGeneration int64
	err := m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("UPDATE usage_fact_member_states SET next_backfill_hour = ?, updated_at = ? WHERE "+inSQL+" AND next_backfill_hour < ?",
			append(updateArgs, hourTs)...).Error; err != nil {
			return err
		}
		if err := refreshUsageFactRepairProgressTx(tx); err != nil {
			return err
		}
		var next sql.NullInt64
		if err := tx.Model(&UsageFactMemberState{}).Select("MIN(next_backfill_hour)").Where("active = ?", true).Scan(&next).Error; err != nil {
			return err
		}
		value := int64(0)
		if next.Valid {
			value = next.Int64
		}
		if err := tx.Model(&UsageFactSyncState{}).Where("id = 1").Update("next_backfill_hour", value).Error; err != nil {
			return err
		}
		var state UsageFactSyncState
		if err := tx.First(&state, 1).Error; err != nil {
			return err
		}
		generation = state.Generation
		servingGeneration = state.ServingGeneration
		return nil
	})
	if err == nil {
		m.publishUsageFactGenerations(generation, servingGeneration)
	}
	return err
}

// rewindUsageFactMemberBackfillCursor 只在本地完整性检查发现真实缺口时回退单个
// 成员。普通回填仍使用只前移的 setUsageFactMemberBackfillCursor，避免并发完成的
// 较新游标被旧任务覆盖。
func (m *Monitor) rewindUsageFactMemberBackfillCursor(userID, hourTs int64) error {
	if userID <= 0 || hourTs <= 0 {
		return nil
	}
	return m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND next_backfill_hour > ?", userID, true, hourTs).
			Updates(map[string]any{"next_backfill_hour": hourTs, "updated_at": time.Now().Unix()}).Error; err != nil {
			return err
		}
		var next sql.NullInt64
		if err := tx.Model(&UsageFactMemberState{}).Select("MIN(next_backfill_hour)").Where("active = ?", true).Scan(&next).Error; err != nil {
			return err
		}
		value := int64(0)
		if next.Valid {
			value = next.Int64
		}
		return tx.Model(&UsageFactSyncState{}).Where("id = 1").Update("next_backfill_hour", value).Error
	})
}

// findEarliestUsageFactMemberHourGap 是游标之外的本地自愈检查。它只读事实专库；
// 每小时最多检查 usageFactGapAuditMembers 个成员，通过进程内游标轮转。
// 200 人最长 20 小时覆盖一轮；日事实内容正确性仍由独立语义指纹审计，
// 因此这个降频只延长“已发布后 proof 行被单独删除”的自愈时间，
// 不会让错误的日事实继续服务。
func (m *Monitor) findEarliestUsageFactMemberHourGap(start, end int64) (int64, int64, bool, error) {
	if start <= 0 || end <= start {
		return 0, 0, false, nil
	}
	var members []UsageFactMemberState
	cursor := m.usageFactsGapAuditNextUser.Load()
	query := m.usageFactsStore().Where("active = ? AND next_backfill_hour >= ?", true, end)
	if cursor > 0 {
		query = query.Where("user_id > ?", cursor)
	}
	if err := query.Order("user_id").Limit(usageFactGapAuditMembers).Find(&members).Error; err != nil {
		return 0, 0, false, err
	}
	if len(members) == 0 && cursor > 0 {
		m.usageFactsGapAuditNextUser.Store(0)
		if err := m.usageFactsStore().Where("active = ? AND next_backfill_hour >= ?", true, end).
			Order("user_id").Limit(usageFactGapAuditMembers).Find(&members).Error; err != nil {
			return 0, 0, false, err
		}
	}
	if len(members) == 0 {
		return 0, 0, false, nil
	}
	m.usageFactsGapAuditNextUser.Store(members[len(members)-1].UserID)
	expected := (end - start) / usageFactHourSeconds
	memberIDs := make([]int64, 0, len(members))
	for _, member := range members {
		memberIDs = append(memberIDs, member.UserID)
	}
	inSQL, inArgs := usageIn("user_id", memberIDs)
	type memberHourCount struct {
		UserID int64
		Count  int64
	}
	var grouped []memberHourCount
	countArgs := append([]any{start, end}, inArgs...)
	countArgs = append(countArgs, "complete")
	if err := m.usageFactsStore().Model(&UsageFactMemberHourState{}).
		Select("user_id, COUNT(*) AS count").
		Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND content_hash <> ''", countArgs...).
		Group("user_id").Scan(&grouped).Error; err != nil {
		return 0, 0, false, err
	}
	counts := make(map[int64]int64, len(grouped))
	for _, row := range grouped {
		counts[row.UserID] = row.Count
	}
	for _, member := range members {
		if counts[member.UserID] == expected {
			continue
		}
		var completed []int64
		if err := m.usageFactsStore().Model(&UsageFactMemberHourState{}).
			Where("user_id = ? AND hour_ts >= ? AND hour_ts < ? AND status = ? AND content_hash <> ''",
				member.UserID, start, end, "complete").Order("hour_ts").Pluck("hour_ts", &completed).Error; err != nil {
			return 0, 0, false, err
		}
		hour := start
		for _, completedHour := range completed {
			if completedHour < hour {
				continue
			}
			if completedHour > hour {
				return member.UserID, hour, true, nil
			}
			hour += usageFactHourSeconds
		}
		if hour < end {
			return member.UserID, hour, true, nil
		}
	}
	return 0, 0, false, nil
}

// findEarliestUsageFactHourGap 只检查本地完成台账，不读取生产 logs。
// complete + 非空内容指纹是一个小时已被来源查询、落盘并核验过的证明；空流量
// 小时同样有空集合指纹，因此不会被误认为缺口。返回最早缺失/失败小时，让
// 后台从该处恢复，同时避免无缺口时反复扫描来源库。
func (m *Monitor) findEarliestUsageFactHourGap(start, end int64) (int64, bool, error) {
	if start <= 0 || end <= start {
		return 0, false, nil
	}
	var completed []int64
	if err := m.usageFactsStore().Model(&UsageHourIngestState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND content_hash <> ''", start, end, "complete").
		Order("hour_ts").Pluck("hour_ts", &completed).Error; err != nil {
		return 0, false, err
	}
	expected := start
	for _, hour := range completed {
		if hour < expected {
			continue
		}
		if hour > expected {
			return expected, true, nil
		}
		expected += usageFactHourSeconds
	}
	if expected < end {
		return expected, true, nil
	}
	return 0, false, nil
}

type usageFactMetrics struct {
	Rows             int64
	Requests         int64
	RefundRecords    int64
	PromptTokens     int64
	CompletionTokens int64
	ConsumeQuota     int64
	RefundQuota      int64
}

func (x usageFactMetrics) tokens() int64 { return x.PromptTokens + x.CompletionTokens }

func usageFactMetricsEqual(a, b usageFactMetrics) bool {
	return a.Requests == b.Requests && a.RefundRecords == b.RefundRecords &&
		a.PromptTokens == b.PromptTokens && a.CompletionTokens == b.CompletionTokens &&
		a.ConsumeQuota == b.ConsumeQuota && a.RefundQuota == b.RefundQuota
}

func factsMetrics(rows []UsageHourFact) usageFactMetrics {
	var out usageFactMetrics
	out.Rows = int64(len(rows))
	for _, row := range rows {
		out.Requests += row.Requests
		out.RefundRecords += row.RefundRecords
		out.PromptTokens += row.PromptTokens
		out.CompletionTokens += row.CompletionTokens
		out.ConsumeQuota += row.ConsumeQuota
		out.RefundQuota += row.RefundQuota
	}
	return out
}

func (m *Monitor) fetchUsageFactHour(ctx context.Context, hourTs int64, ids []int64) ([]UsageHourFact, error) {
	return m.fetchUsageFactHourWithPriority(ctx, hourTs, ids, false)
}

func (m *Monitor) fetchUsageFactHourWithPriority(ctx context.Context, hourTs int64, ids []int64, lowPriority bool) ([]UsageHourFact, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	inSQL, inArgs := usageIn("user_id", ids)
	hint := "/*+ MAX_EXECUTION_TIME(8000) */ "
	if m.usageDayExpr != "" { // local fake SQLite
		hint = ""
	}
	q := `SELECT ` + hint + `
  COALESCE(user_id,0), COALESCE(channel_id,0), COALESCE(` + "`group`" + `,''),
  COALESCE(model_name,''), COALESCE(token_id,0), COALESCE(MAX(token_name),''),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN 1 ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 6 THEN 1 ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(prompt_tokens,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)
 FROM logs
 WHERE type IN (2,6) AND created_at >= ? AND created_at < ? AND ` + inSQL + `
   AND NOT (` + m.channelTestSourcePredicateSQL() + `)
 GROUP BY user_id, channel_id, ` + "`group`" + `, model_name, token_id
 LIMIT ?`
	args := append([]any{hourTs, hourTs + usageFactHourSeconds}, inArgs...)
	args = append(args, usageFactMaxRowsPerHour+1)
	dayTs := usageFactDayStart(hourTs)
	out := make([]UsageHourFact, 0)
	run := m.withUsageFactSourceQuery
	if lowPriority {
		run = m.withUsageFactHistorySourceQuery
	}
	err := run(ctx, func(cctx context.Context) error {
		rows, err := m.prodDB.QueryContext(cctx, q, args...)
		if err != nil {
			return fmt.Errorf("读取生产 logs 单小时聚合失败: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row UsageHourFact
			if err := rows.Scan(&row.UserID, &row.ChannelID, &row.Grp, &row.ModelName, &row.TokenID, &row.TokenName,
				&row.Requests, &row.RefundRecords, &row.PromptTokens, &row.CompletionTokens, &row.ConsumeQuota, &row.RefundQuota); err != nil {
				return err
			}
			row.HourTs, row.DayTs = hourTs, dayTs
			out = append(out, row)
			if len(out) > usageFactMaxRowsPerHour {
				return fmt.Errorf("%w(%d)", errUsageFactHourRangeTooLarge, usageFactMaxRowsPerHour)
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// syncUsageFactHour 的完整实现在 usage_facts_hour_sync.go。这里只保留
// Tail/回填调用的稳定入口，避免历史补漏与普通同步各写一套替换逻辑。
func (m *Monitor) syncUsageFactHour(ctx context.Context, hourTs int64, ids []int64) error {
	_, err := m.syncUsageFactHourBatchedWithOptions(ctx, hourTs, ids, usageFactHourSyncOptions{
		recordFailure:       true,
		updateLastFactSync:  true,
		invalidateNoHistory: true,
	})
	return err
}

// syncUsageFactHourBatchedWithOptions 对所有 Tail/复核调用强制 <=200 人的来源
// 查询上限。大量关注成员不会生成无限增长的 IN 列表；一个批次失败时，已完成
// 批次保留原子结果，下一轮可幂等续跑。
func (m *Monitor) syncUsageFactHourBatchedWithOptions(ctx context.Context, hourTs int64, ids []int64, options usageFactHourSyncOptions) (usageFactHourSyncResult, error) {
	var combined usageFactHourSyncResult
	var combinedErr error
	ordered := append([]int64(nil), ids...)
	if len(ordered) > 1 {
		// Adaptive splitting is intentionally bounded. Rotate the first member on
		// every turn so an all-pathological batch cannot spend all eight queries
		// repeatedly isolating the same left-most IDs while the right side never
		// receives a source attempt. Budget sentinels are no-penalty to durable
		// jobs, so subsequent turns continue this fair walk.
		offset := int((m.usageFactsAdaptiveTurn.Add(1) - 1) % uint64(len(ordered)))
		ordered = append(append(make([]int64, 0, len(ordered)), ordered[offset:]...), ordered[:offset]...)
	}
	budget := usageFactAdaptiveQueryBudget
	for start := 0; start < len(ordered); start += usageFactProfileBatch {
		end := start + usageFactProfileBatch
		if end > len(ordered) {
			end = len(ordered)
		}
		if budget <= 0 {
			if combined.FailedByUser == nil {
				combined.FailedByUser = make(map[int64]error)
			}
			for _, id := range ordered[start:] {
				combined.FailedByUser[id] = errUsageFactAdaptiveBudget
			}
			combinedErr = errors.Join(combinedErr, errUsageFactAdaptiveBudget)
			break
		}
		result, err := m.syncUsageFactHourBatchAdaptiveBudgeted(ctx, hourTs, ordered[start:end], options, &budget)
		mergeUsageFactHourSyncResult(&combined, result)
		if err != nil {
			combinedErr = errors.Join(combinedErr, err)
		}
	}
	return combined, combinedErr
}

func (m *Monitor) syncUsageFactHourBatchAdaptiveBudgeted(ctx context.Context, hourTs int64, ids []int64, options usageFactHourSyncOptions, budget *int) (usageFactHourSyncResult, error) {
	if budget == nil || *budget <= 0 {
		failed := make(map[int64]error, len(ids))
		for _, id := range ids {
			failed[id] = errUsageFactAdaptiveBudget
		}
		return usageFactHourSyncResult{FailedByUser: failed}, errUsageFactAdaptiveBudget
	}
	*budget--
	result, err := m.syncUsageFactHourWithOptions(ctx, hourTs, ids, options)
	if err == nil {
		return result, err
	}
	if len(ids) <= 1 || (!errors.Is(err, errUsageFactHourRangeTooLarge) && !usageFactHistoryRangeShouldFallback(err)) {
		if result.FailedByUser == nil {
			result.FailedByUser = make(map[int64]error, len(ids))
		}
		for _, id := range ids {
			result.FailedByUser[id] = err
		}
		return result, err
	}
	mid := len(ids) / 2
	left, leftErr := m.syncUsageFactHourBatchAdaptiveBudgeted(ctx, hourTs, ids[:mid], options, budget)
	right, rightErr := m.syncUsageFactHourBatchAdaptiveBudgeted(ctx, hourTs, ids[mid:], options, budget)
	var combined usageFactHourSyncResult
	mergeUsageFactHourSyncResult(&combined, left)
	mergeUsageFactHourSyncResult(&combined, right)
	return combined, errors.Join(leftErr, rightErr)
}

func mergeUsageFactHourSyncResult(dst *usageFactHourSyncResult, src usageFactHourSyncResult) {
	if dst == nil {
		return
	}
	dst.Changed = dst.Changed || src.Changed
	dst.HadPriorFingerprint = dst.HadPriorFingerprint || src.HadPriorFingerprint
	dst.SucceededUserIDs = append(dst.SucceededUserIDs, src.SucceededUserIDs...)
	dst.ChangedUserIDs = append(dst.ChangedUserIDs, src.ChangedUserIDs...)
	dst.InvalidatedNoHistoryUserIDs = append(dst.InvalidatedNoHistoryUserIDs, src.InvalidatedNoHistoryUserIDs...)
	if len(src.FailedByUser) > 0 {
		if dst.FailedByUser == nil {
			dst.FailedByUser = make(map[int64]error, len(src.FailedByUser))
		}
		for id, err := range src.FailedByUser {
			dst.FailedByUser[id] = err
		}
	}
}

func truncateUsageFactError(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 480 {
		return s
	}
	return s[:480] + "…"
}

// rebuildUsageDailyFact 只在自然日 24 个小时均为 complete 时重建日事实。小时缺口
// 期间保留上一版日事实：读取侧会为新出现的细分事实回退到小时表，既不双计，也不
// 因一次成员变更或短暂采集失败把长期历史清空。
func (m *Monitor) rebuildUsageDailyFact(tx *gorm.DB, dayTs int64, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	verified, err := verifyUsageFactDayHours(tx, dayTs, ids)
	if err != nil {
		return err
	}
	if !verified {
		return nil
	}
	// 与小时事实相同，日汇总也只替换候选成员；上一服务版独有成员的
	// 日事实必须保留到新候选完整发布之后，避免回填时页面金额骤降。
	memberSQL, memberArgs := usageIn("user_id", ids)
	deleteArgs := append([]any{dayTs}, memberArgs...)
	if err := tx.Where("date_ts = ? AND "+memberSQL, deleteArgs...).Delete(&UsageDailyFact{}).Error; err != nil {
		return err
	}
	insertArgs := append([]any{dayTs}, memberArgs...)
	if err := tx.Exec(`INSERT INTO usage_daily_facts
  (date_ts,user_id,channel_id,grp,model_name,token_id,token_name,requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota)
 SELECT day_ts,user_id,channel_id,grp,model_name,token_id,MAX(token_name),
  SUM(requests),SUM(refund_records),SUM(prompt_tokens),SUM(completion_tokens),SUM(consume_quota),SUM(refund_quota)
 FROM usage_hour_facts WHERE day_ts = ? AND `+memberSQL+`
 GROUP BY day_ts,user_id,channel_id,grp,model_name,token_id`, insertArgs...).Error; err != nil {
		return err
	}
	var hourly, daily usageFactMetrics
	hourArgs := append([]any{dayTs}, memberArgs...)
	if err := tx.Raw(`SELECT COUNT(*) AS rows,
  COALESCE(SUM(requests),0) AS requests, COALESCE(SUM(refund_records),0) AS refund_records,
  COALESCE(SUM(prompt_tokens),0) AS prompt_tokens, COALESCE(SUM(completion_tokens),0) AS completion_tokens,
  COALESCE(SUM(consume_quota),0) AS consume_quota, COALESCE(SUM(refund_quota),0) AS refund_quota
 FROM usage_hour_facts WHERE day_ts = ? AND `+memberSQL, hourArgs...).Scan(&hourly).Error; err != nil {
		return err
	}
	dailyArgs := append([]any{dayTs}, memberArgs...)
	if err := tx.Raw(`SELECT COUNT(*) AS rows,
  COALESCE(SUM(requests),0) AS requests, COALESCE(SUM(refund_records),0) AS refund_records,
  COALESCE(SUM(prompt_tokens),0) AS prompt_tokens, COALESCE(SUM(completion_tokens),0) AS completion_tokens,
  COALESCE(SUM(consume_quota),0) AS consume_quota, COALESCE(SUM(refund_quota),0) AS refund_quota
 FROM usage_daily_facts WHERE date_ts = ? AND `+memberSQL, dailyArgs...).Scan(&daily).Error; err != nil {
		return err
	}
	if !usageFactMetricsEqual(hourly, daily) {
		return fmt.Errorf("本地日事实核验不一致 day=%d", dayTs)
	}
	// 日事实和其成员级内容证明必须在同一事务提交。小时明细超过近期
	// 留存后会被清理，发布前/周期审计仍可用这个指纹识别日事实被
	// 合法 SQL 误删、改写或迁移残缺，而不只依赖 SQLite quick_check。
	dailyRows, err := loadUsageDailyFacts(tx, dayTs, dayTs+usageFactDaySeconds, ids)
	if err != nil {
		return err
	}
	rowsByMemberDay := usageDailyFactsByMemberDay(dailyRows)
	nowUnix := time.Now().Unix()
	for _, id := range ids {
		memberRows := rowsByMemberDay[usageFactMemberDayKey{userID: id, dayTs: dayTs}]
		metrics := dailyFactsMetrics(memberRows)
		state := UsageFactMemberDayState{
			UserID: id, DateTs: dayTs, Rows: int(metrics.Rows), Requests: metrics.Requests,
			Tokens: metrics.tokens(), ContentHash: usageDailyFactContentHash(memberRows), UpdatedAt: nowUnix,
		}
		if m.usageFactsFullHistoryEnabled() {
			// Hour replacement invalidates the previous independent day control.
			// Keep the locally verified fact hash, but require the day-finalizer to
			// re-read the source control totals before this day can be considered
			// full-history signed again.
			state.Status = "pending"
			state.LastError = "hour facts changed; source day control pending"
		}
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) pruneUsageFacts(now time.Time) error {
	var state UsageFactSyncState
	if err := m.usageFactsStore().First(&state, 1).Error; err != nil {
		return err
	}
	if state.LastPrunedAt > now.Add(-24*time.Hour).Unix() {
		return nil
	}
	// 按自然日边界清理，不能把某一天只删除前半天。日事实重建以完整
	// 24 小时为原子单位，保留半天会制造“看似完整、实际残缺”的输入。
	hourCutoff := usageFactDayStart(now.AddDate(0, 0, -m.usageFactHourRetentionDays()).Unix())
	dailyCutoff := usageFactDayStart(now.AddDate(0, 0, -m.usageFactRetentionDays()).Unix())
	proofCutoff := dailyCutoff
	if m.usageFactsFullHistoryEnabled() {
		// Full-history daily facts and their source-controlled day proofs are
		// permanent derived state. Only recent hourly correction material is
		// pruned; deleting old daily rows while leaving coverage=ready would turn
		// a signed member silently incomplete.
		proofCutoff = hourCutoff
	}
	return m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		if m.usageFactsFullHistoryEnabled() {
			// Hour rows are staging for a strict independently-controlled day.
			// Never age out an old pathological/repair day merely because eight
			// calendar days elapsed: a crash at hour 23 would otherwise leave its
			// durable cursor pointing at missing 0..22 input forever. Only a current
			// epoch/version complete day proof authorizes removal of that member-day's
			// hour staging. A pre-existing strict proof is not sufficient while an
			// exact repair or non-revoking hourly audit is actively rebuilding that
			// same day; keep its 0..23 staging across prune/restart. The main Tail is
			// also protected for the rolling 24-hour window used by a pathological
			// historical-day fallback.
			activeHourlyKinds := []string{usageFactHistoryKindRepairHour, usageFactHistoryKindAuditHour, usageFactHistoryKindTail}
			terminalStatuses := []string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}
			strictArgs := []any{hourCutoff, "complete", strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch),
				userTrafficClassificationVersion, usageFactQuerySemanticsVersion,
				activeHourlyKinds, terminalStatuses,
				usageFactHistoryKindRepairHour, usageFactHistoryKindAuditHour, usageFactDaySeconds,
				usageFactHistoryKindTail, 23 * usageFactHourSeconds}
			if err := tx.Exec(`DELETE FROM usage_hour_facts
 WHERE hour_ts < ? AND EXISTS (
  SELECT 1 FROM usage_fact_member_day_states p
  WHERE p.user_id = usage_hour_facts.user_id
    AND p.date_ts = usage_hour_facts.day_ts
    AND p.status = ? AND p.source_epoch = ?
    AND p.classification_version = ? AND p.query_semantics_version = ?
    AND p.source_result_hash <> '' AND p.fact_content_hash <> ''
 ) AND NOT EXISTS (
  SELECT 1 FROM usage_fact_jobs j
  WHERE j.user_id = usage_hour_facts.user_id
    AND j.kind IN ? AND j.status NOT IN ?
    AND (
      (j.kind = ? AND usage_hour_facts.hour_ts >= j.from_ts AND usage_hour_facts.hour_ts < j.through_ts)
      OR (j.kind = ? AND j.verify_next_hour > 0
          AND usage_hour_facts.hour_ts >= j.verify_next_hour - ? AND usage_hour_facts.hour_ts < j.verify_next_hour)
      OR (j.kind = ? AND usage_hour_facts.hour_ts >= j.next_hour - ? AND usage_hour_facts.hour_ts <= j.next_hour)
    )
 )`, strictArgs...).Error; err != nil {
				return err
			}
			if err := tx.Exec(`DELETE FROM usage_fact_member_hour_states
 WHERE hour_ts < ? AND EXISTS (
  SELECT 1 FROM usage_fact_member_day_states p
  WHERE p.user_id = usage_fact_member_hour_states.user_id
    AND p.date_ts <= usage_fact_member_hour_states.hour_ts
    AND usage_fact_member_hour_states.hour_ts < p.date_ts + 86400
    AND p.status = ? AND p.source_epoch = ?
    AND p.classification_version = ? AND p.query_semantics_version = ?
    AND p.source_result_hash <> '' AND p.fact_content_hash <> ''
 ) AND NOT EXISTS (
  SELECT 1 FROM usage_fact_jobs j
  WHERE j.user_id = usage_fact_member_hour_states.user_id
    AND j.kind IN ? AND j.status NOT IN ?
    AND (
      (j.kind = ? AND usage_fact_member_hour_states.hour_ts >= j.from_ts AND usage_fact_member_hour_states.hour_ts < j.through_ts)
      OR (j.kind = ? AND j.verify_next_hour > 0
          AND usage_fact_member_hour_states.hour_ts >= j.verify_next_hour - ? AND usage_fact_member_hour_states.hour_ts < j.verify_next_hour)
      OR (j.kind = ? AND usage_fact_member_hour_states.hour_ts >= j.next_hour - ? AND usage_fact_member_hour_states.hour_ts <= j.next_hour)
    )
 )`, strictArgs...).Error; err != nil {
				return err
			}
		} else if err := tx.Where("hour_ts < ?", hourCutoff).Delete(&UsageHourFact{}).Error; err != nil {
			return err
		}
		// 完成台账的留存必须覆盖日事实留存；若只保留 8 天，后台会误以为
		// 第 9 天以前从未回填，永久从第一小时开始重复扫描生产 logs。
		if err := tx.Where("hour_ts < ?", proofCutoff).Delete(&UsageHourIngestState{}).Error; err != nil {
			return err
		}
		if !m.usageFactsFullHistoryEnabled() {
			if err := tx.Where("hour_ts < ?", proofCutoff).Delete(&UsageFactMemberHourState{}).Error; err != nil {
				return err
			}
		}
		if !m.usageFactsFullHistoryEnabled() {
			if err := tx.Where("date_ts < ?", dailyCutoff).Delete(&UsageDailyFact{}).Error; err != nil {
				return err
			}
			if err := tx.Where("date_ts < ?", dailyCutoff).Delete(&UsageFactMemberDayState{}).Error; err != nil {
				return err
			}
		}
		state.LastPrunedAt = now.Unix()
		return tx.Save(&state).Error
	})
}

// syncUsageProfiles 单独采 users/tokens 的轻量快照。它严格只查询被纳入监测名单的
// 用户，批量 <=200；任何失败均保留旧快照，不调用方不会因资料失败而清空消费事实。
func (m *Monitor) syncUsageProfiles(ctx context.Context, now time.Time) error {
	if err := m.ensureUsageFactDerivedWritesCapacity(); err != nil {
		return err
	}
	ids, err := m.trackedIDsForUsageFacts()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	users := make([]UsageUserSnapshot, 0, len(ids))
	tokens := make([]UsageTokenSnapshot, 0)
	for start := 0; start < len(ids); start += usageFactProfileBatch {
		end := start + usageFactProfileBatch
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]
		inSQL, args := usageIn("id", chunk)
		err = m.withUsageFactSourceQuery(ctx, func(cctx context.Context) error {
			rows, err := m.prodDB.QueryContext(cctx,
				"SELECT id, COALESCE(username,''), COALESCE(email,''), COALESCE(quota,0), COALESCE(used_quota,0) FROM users WHERE "+inSQL,
				args...)
			if err != nil {
				return fmt.Errorf("读取用户资料失败: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var row UsageUserSnapshot
				if err := rows.Scan(&row.UserID, &row.Username, &row.Email, &row.BalanceQuota, &row.UsedQuota); err != nil {
					return err
				}
				row.Exists, row.CapturedAt = true, now.Unix()
				users = append(users, row)
			}
			return rows.Err()
		})
		if err != nil {
			m.noteUsageProfileFailure(now, err)
			return err
		}

		inSQL, args = usageIn("user_id", chunk)
		err = m.withUsageFactSourceQuery(ctx, func(cctx context.Context) error {
			rows, err := m.prodDB.QueryContext(cctx, "SELECT id, user_id, COALESCE(name,''), COALESCE(`key`,''), COALESCE(`group`,''), CAST(COALESCE(used_quota,0) AS SIGNED), (deleted_at IS NOT NULL) FROM tokens WHERE "+inSQL, args...)
			if err != nil {
				return fmt.Errorf("读取令牌资料失败: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var row UsageTokenSnapshot
				var key string
				if err := rows.Scan(&row.TokenID, &row.UserID, &row.Name, &key, &row.Grp, &row.UsedQuota, &row.Deleted); err != nil {
					return err
				}
				row.MaskedKey, row.CapturedAt = maskTokenKey(key), now.Unix()
				tokens = append(tokens, row)
			}
			return rows.Err()
		})
		if err != nil {
			m.noteUsageProfileFailure(now, err)
			return err
		}
	}

	// 远程 users/tokens 查询全部结束后才短暂持有本地写锁。慢 SQL 或来源超时
	// 不再阻塞小时租约、本地状态读取和事实库维护。
	m.usageFactsSyncMu.Lock()
	err = m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		watermarks := make([]UsageUserQuotaWatermark, 0, len(users))
		for _, user := range users {
			watermarks = append(watermarks, UsageUserQuotaWatermark{
				UserID: user.UserID, CapturedAt: user.CapturedAt, UsedQuota: user.UsedQuota, Exists: user.Exists,
			})
		}
		if len(watermarks) > 0 {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(watermarks, usageFactProfileBatch).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("captured_at < ?", now.Add(-usageLiveProjectionWatermarkRetention).Unix()).
			Delete(&UsageUserQuotaWatermark{}).Error; err != nil {
			return err
		}
		inSQL, args := usageIn("user_id", ids)
		// 先标为已删除，再 UPSERT 本轮完整读取到的令牌；硬删后旧令牌仍可为
		// 历史消费补齐名称，但不再被当作现存令牌展示。
		if err := tx.Exec("UPDATE usage_token_snapshots SET deleted = ?, captured_at = ? WHERE "+inSQL,
			append([]any{true, now.Unix()}, args...)...).Error; err != nil {
			return err
		}
		inUsers, userArgs := usageIn("user_id", ids)
		if err := tx.Exec("UPDATE usage_user_snapshots SET `exists` = ?, captured_at = ? WHERE "+inUsers,
			append([]any{false, now.Unix()}, userArgs...)...).Error; err != nil {
			return err
		}
		if len(users) > 0 {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}}, UpdateAll: true}).CreateInBatches(users, usageFactProfileBatch).Error; err != nil {
				return err
			}
		}
		if len(tokens) > 0 {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "token_id"}}, UpdateAll: true}).CreateInBatches(tokens, usageFactProfileBatch).Error; err != nil {
				return err
			}
		}
		return tx.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
			"last_profile_sync_at": now.Unix(), "last_profile_failure_at": int64(0),
		}).Error
	})
	m.usageFactsSyncMu.Unlock()
	if err != nil {
		m.noteUsageProfileFailure(now, err)
	}
	return err
}

func (m *Monitor) noteUsageProfileFailure(now time.Time, err error) {
	if errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, context.Canceled) {
		return
	}
	m.markUsageProfileFailure(now)
}

func (m *Monitor) markUsageProfileFailure(now time.Time) {
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Update("last_profile_failure_at", now.Unix()).Error; err != nil {
		slog.Warn("记录用量资料快照失败状态失败", "err", err)
	}
}

// usageFactsCTE 把已签收的日事实和尚未形成日版本的小时事实组合成
// 一份本地只读逻辑表。可见性的最小原子单位是“成员×自然日”，不是维度行：
//
//   - 只要该成员日已有内容证明，就只读日事实；
//   - 修复缺 proof 但仍保留上一份日事实时，也继续服务这份最后正确版本；
//   - 只有既无日事实也无日 proof 的真正 partial 日才读小时行。
//
// 这样 repair 逐小时回填时，新模型/渠道/token 维度不会提前与旧日版本
// 混合。24 小时齐备后，rebuildUsageDailyFact 在同一事务内替换日事实和 proof，
// 页面才一次性看到新版本。
func usageFactsCTE(ids []int64, fromTs, toTs, tokenID int64) (string, []any, error) {
	if len(ids) == 0 {
		return "", nil, nil
	}
	if toTs <= fromTs {
		return "WITH facts AS (SELECT 0 AS bucket_ts,0 AS user_id,0 AS channel_id,'' AS grp,'' AS model_name,0 AS token_id,'' AS token_name,0 AS requests,0 AS refund_records,0 AS prompt_tokens,0 AS completion_tokens,0 AS consume_quota,0 AS refund_quota WHERE 1=0) ", nil, nil
	}
	// Daily rows are whole-day versions. A published watermark inside the current
	// day must therefore use hours for the tail; selecting that day's candidate
	// daily row would expose future/unverified hours in one step.
	fullDayThrough := usageFactDayStart(toTs)
	dailyIn, dailyArgs := usageIn("d.user_id", ids)
	hourIn, hourArgs := usageIn("h.user_id", ids)
	dailyWhere := "d.date_ts >= ? AND d.date_ts < ? AND " + dailyIn
	hourWhere := "h.hour_ts >= ? AND h.hour_ts < ? AND " + hourIn
	dailyParams := append([]any{fromTs, fullDayThrough}, dailyArgs...)
	hourParams := append([]any{fromTs, toTs}, hourArgs...)
	if tokenID > 0 {
		dailyWhere += " AND d.token_id = ?"
		dailyParams = append(dailyParams, tokenID)
		hourWhere += " AND h.token_id = ?"
		hourParams = append(hourParams, tokenID)
	}
	q := `WITH facts AS (
 SELECT d.date_ts AS bucket_ts,d.user_id,d.channel_id,d.grp,d.model_name,d.token_id,d.token_name,
  d.requests,d.refund_records,d.prompt_tokens,d.completion_tokens,d.consume_quota,d.refund_quota
 FROM usage_daily_facts d
 LEFT JOIN usage_fact_published_members pm ON pm.user_id=d.user_id
 WHERE ` + dailyWhere + `
  AND ((pm.user_id IS NOT NULL AND (pm.source_floor_hour<=0 OR d.date_ts>=pm.source_floor_hour))
       OR (pm.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM usage_fact_published_members LIMIT 1)))
 UNION ALL
 SELECT h.day_ts AS bucket_ts,h.user_id,h.channel_id,h.grp,h.model_name,h.token_id,h.token_name,
  h.requests,h.refund_records,h.prompt_tokens,h.completion_tokens,h.consume_quota,h.refund_quota
	 FROM usage_hour_facts h
	 LEFT JOIN usage_fact_published_members pm ON pm.user_id=h.user_id
	 WHERE ` + hourWhere + `
	  AND ((pm.user_id IS NOT NULL AND (pm.source_floor_hour<=0 OR h.hour_ts>=pm.source_floor_hour))
	       OR (pm.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM usage_fact_published_members LIMIT 1)))
	  AND NOT EXISTS (
	   SELECT 1 FROM usage_fact_member_day_states p
	   WHERE p.date_ts = h.day_ts AND p.user_id = h.user_id AND p.date_ts < ` + strconv.FormatInt(fullDayThrough, 10) + ` AND p.content_hash <> ''
	  )
	  AND NOT EXISTS (
	   SELECT 1 FROM usage_daily_facts d
	   WHERE d.date_ts = h.day_ts AND d.user_id = h.user_id AND d.date_ts < ` + strconv.FormatInt(fullDayThrough, 10) + `
	  )
) `
	return q, append(dailyParams, hourParams...), nil
}

func usageFactQueryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}

const usageFactsReadCapacity = 2

type usageFactsReadBudgetStats struct {
	Capacity    int    `json:"capacity"`
	InUse       int64  `json:"in_use"`
	Waiters     int64  `json:"waiters"`
	Acquired    uint64 `json:"acquired"`
	Failed      uint64 `json:"failed"`
	Completed   uint64 `json:"completed"`
	TotalWaitMS int64  `json:"total_wait_ms"`
	MaxWaitMS   int64  `json:"max_wait_ms"`
	TotalRunMS  int64  `json:"total_run_ms"`
	MaxRunMS    int64  `json:"max_run_ms"`
}

func (m *Monitor) usageFactsReadBudgetStats() usageFactsReadBudgetStats {
	metrics := &m.usageFactsReadMetrics
	return usageFactsReadBudgetStats{
		Capacity: usageFactsReadCapacity, InUse: metrics.active.Load(), Waiters: metrics.waiters.Load(),
		Acquired: metrics.acquired.Load(), Failed: metrics.failed.Load(), Completed: metrics.completed.Load(),
		TotalWaitMS: time.Duration(metrics.waitNanos.Load()).Milliseconds(),
		MaxWaitMS:   time.Duration(metrics.maxWaitNanos.Load()).Milliseconds(),
		TotalRunMS:  time.Duration(metrics.runNanos.Load()).Milliseconds(),
		MaxRunMS:    time.Duration(metrics.maxRunNanos.Load()).Milliseconds(),
	}
}

// acquireUsageFactsReadBudget 把一次完整本地聚合（stats 包含其顺序子查询）
// 视为一个占用单元。同键并发先由缓存 singleflight 合并，这里主要限制
// 不同组织/日期键同时冷查造成的内存和 CPU 峰值。
func (m *Monitor) acquireUsageFactsReadBudget(ctx context.Context) (func(), error) {
	metrics := &m.usageFactsReadMetrics
	startedWait := time.Now()
	metrics.waiters.Add(1)
	err := acquireUsageSemaphore(ctx, &m.usageFactsReadGateOnce, &m.usageFactsReadGate, usageFactsReadCapacity)
	metrics.waiters.Add(-1)
	waited := time.Since(startedWait).Nanoseconds()
	metrics.waitNanos.Add(waited)
	updateUsageMetricMax(&metrics.maxWaitNanos, waited)
	if err != nil {
		metrics.failed.Add(1)
		return nil, err
	}
	metrics.acquired.Add(1)
	metrics.active.Add(1)
	startedRun := time.Now()
	return func() {
		run := time.Since(startedRun).Nanoseconds()
		metrics.runNanos.Add(run)
		updateUsageMetricMax(&metrics.maxRunNanos, run)
		metrics.completed.Add(1)
		metrics.active.Add(-1)
		<-m.usageFactsReadGate
	}, nil
}

// computeUsageStatsFromFacts 保持 API JSON 口径不变，只把数据源换为本地事实表。
func (m *Monitor) computeUsageStatsFromFacts(ctx context.Context, ids []int64, fromTs, toTs, tokenID int64) (*UsageStats, error) {
	st := &UsageStats{From: time.Unix(fromTs, 0).In(usageCST).Format("2006-01-02"), To: time.Unix(toTs-1, 0).In(usageCST).Format("2006-01-02")}
	if len(ids) == 0 {
		return st, nil
	}
	release, err := m.acquireUsageFactsReadBudget(ctx)
	if err != nil {
		return nil, fmt.Errorf("等待本地事实查询槽位失败: %w", err)
	}
	defer release()
	cte, args, err := usageFactsCTE(ids, fromTs, toTs, tokenID)
	if err != nil {
		return nil, err
	}
	// 每个查询各自拥有本地 SQLite 的短超时。趋势页需依次读取每日、维度、模型
	// 三类事实，不能让前一次慢查询耗尽后续正常查询的完整预算。
	readRows := func(q string, queryArgs ...any) (*sql.Rows, context.CancelFunc, error) {
		qctx, cancel := usageFactQueryContext(ctx)
		rows, err := m.usageFactsStore().WithContext(qctx).Raw(q, queryArgs...).Rows()
		if err != nil {
			cancel()
			return nil, nil, err
		}
		return rows, cancel, nil
	}
	closeRows := func(rows *sql.Rows, cancel context.CancelFunc) error {
		rowErr := rows.Err()
		closeErr := rows.Close()
		cancel()
		if rowErr != nil {
			return rowErr
		}
		return closeErr
	}

	dailyRows, cancelDaily, err := readRows(cte+`SELECT bucket_ts,
 COALESCE(SUM(requests),0),COALESCE(SUM(refund_records),0),
 COALESCE(SUM(prompt_tokens),0),COALESCE(SUM(completion_tokens),0),
 COALESCE(SUM(consume_quota),0),COALESCE(SUM(refund_quota),0)
 FROM facts GROUP BY bucket_ts ORDER BY bucket_ts`, args...)
	if err != nil {
		return nil, fmt.Errorf("读取本地每日事实失败: %w", err)
	}
	for dailyRows.Next() {
		var dayTs int64
		var row UsageDaily
		if err := dailyRows.Scan(&dayTs, &row.Requests, &row.RefundRecords, &row.PromptTokens, &row.CompletionTokens, &row.ConsumeQuota, &row.RefundQuota); err != nil {
			_ = dailyRows.Close()
			cancelDaily()
			return nil, err
		}
		row.Date = time.Unix(dayTs, 0).In(usageCST).Format("2006-01-02")
		row.Tokens = row.PromptTokens + row.CompletionTokens
		row.finalize()
		st.Daily = append(st.Daily, row)
	}
	if err := closeRows(dailyRows, cancelDaily); err != nil {
		return nil, err
	}
	for _, day := range st.Daily {
		st.Summary.Requests += day.Requests
		st.Summary.RefundRecords += day.RefundRecords
		st.Summary.Tokens += day.Tokens
		st.Summary.ConsumeQuota += day.ConsumeQuota
		st.Summary.RefundQuota += day.RefundQuota
	}
	st.Summary.finalize()

	dims := []struct {
		col       string
		dst       *[]UsageDim
		truncated *bool
		desc      string
	}{
		{"grp", &st.ByGroup, &st.ByGroupTruncated, "分组"},
		{"model_name", &st.ByModel, &st.ByModelTruncated, "模型"},
	}
	for _, dim := range dims {
		rows, cancelRows, err := readRows(cte+`SELECT COALESCE(`+dim.col+`,''),
 COALESCE(SUM(requests),0),COALESCE(SUM(refund_records),0),
 COALESCE(SUM(prompt_tokens),0)+COALESCE(SUM(completion_tokens),0),
 COALESCE(SUM(consume_quota),0),COALESCE(SUM(refund_quota),0)
 FROM facts GROUP BY `+dim.col+` ORDER BY COALESCE(SUM(consume_quota),0) DESC, `+dim.col+` LIMIT `+strconv.Itoa(maxUsageDimRows+1), args...)
		if err != nil {
			return nil, fmt.Errorf("读取本地%s事实失败: %w", dim.desc, err)
		}
		for rows.Next() {
			var row UsageDim
			if err := rows.Scan(&row.Key, &row.Requests, &row.RefundRecords, &row.Tokens, &row.ConsumeQuota, &row.RefundQuota); err != nil {
				_ = rows.Close()
				cancelRows()
				return nil, err
			}
			row.finalize()
			*dim.dst = append(*dim.dst, row)
		}
		if err := closeRows(rows, cancelRows); err != nil {
			return nil, err
		}
		if len(*dim.dst) > maxUsageDimRows {
			*dim.truncated = true
			*dim.dst = (*dim.dst)[:maxUsageDimRows]
		}
	}

	const topDailyModels = 6
	const otherModelSentinel = "__newapi_monitor_other_models__"
	topModels := make([]string, 0, topDailyModels)
	for _, row := range st.ByModel {
		if len(topModels) == topDailyModels {
			break
		}
		topModels = append(topModels, row.Key)
	}
	if len(topModels) == 0 {
		return st, nil
	}
	ph := make([]string, len(topModels))
	modelArgs := append([]any(nil), args...)
	for i, model := range topModels {
		ph[i] = "?"
		modelArgs = append(modelArgs, model)
	}
	modelExpr := "CASE WHEN model_name IN (" + strings.Join(ph, ",") + ") THEN model_name ELSE '" + otherModelSentinel + "' END"
	// 先在子查询中计算模型桶，外层只按别名分组/排序。这样 CASE 的占位符只出现
	// 一次，避免 SELECT、GROUP BY、ORDER BY 重复表达式造成参数错位。
	rows, cancelModels, err := readRows(cte+`SELECT bucket_ts, model_bucket,
 COALESCE(SUM(requests),0),COALESCE(SUM(refund_records),0),
 COALESCE(SUM(prompt_tokens),0)+COALESCE(SUM(completion_tokens),0),
 COALESCE(SUM(consume_quota),0),COALESCE(SUM(refund_quota),0)
 FROM (
  SELECT bucket_ts, `+modelExpr+` AS model_bucket,
   requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota
  FROM facts
 ) model_facts
 GROUP BY bucket_ts, model_bucket ORDER BY bucket_ts, model_bucket`, modelArgs...)
	if err != nil {
		return nil, fmt.Errorf("读取本地每日模型事实失败: %w", err)
	}
	for rows.Next() {
		var dayTs int64
		var row UsageDailyModel
		if err := rows.Scan(&dayTs, &row.Model, &row.Requests, &row.RefundRecords, &row.Tokens, &row.ConsumeQuota, &row.RefundQuota); err != nil {
			_ = rows.Close()
			cancelModels()
			return nil, err
		}
		row.Date = time.Unix(dayTs, 0).In(usageCST).Format("2006-01-02")
		if row.Model == otherModelSentinel {
			row.Model, row.Other = "其他", true
		}
		row.finalize()
		st.DailyByModel = append(st.DailyByModel, row)
	}
	if err := closeRows(rows, cancelModels); err != nil {
		return nil, err
	}
	return st, nil
}

func (m *Monitor) computeUsageMatrixFromFacts(ctx context.Context, ids []int64, fromTs, toTs int64) (*UsageMatrix, error) {
	mx := newUsageMatrixRange(fromTs, toTs)
	if len(ids) == 0 {
		return mx, nil
	}
	release, err := m.acquireUsageFactsReadBudget(ctx)
	if err != nil {
		return nil, fmt.Errorf("等待本地事实查询槽位失败: %w", err)
	}
	defer release()
	cte, args, err := usageFactsCTE(ids, fromTs, toTs, 0)
	if err != nil {
		return nil, err
	}
	cctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	rows, err := m.usageFactsStore().WithContext(cctx).Raw(cte+`SELECT user_id,bucket_ts,
 COALESCE(SUM(requests),0),COALESCE(SUM(refund_records),0),
 COALESCE(SUM(consume_quota),0),COALESCE(SUM(refund_quota),0)
 FROM facts GROUP BY user_id,bucket_ts`, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("读取本地用量矩阵事实失败: %w", err)
	}
	for rows.Next() {
		var dayTs int64
		var cell UsageMatrixCell
		if err := rows.Scan(&cell.UserID, &dayTs, &cell.Requests, &cell.RefundRecords, &cell.ConsumeQuota, &cell.RefundQuota); err != nil {
			rows.Close()
			return nil, err
		}
		cell.Date = time.Unix(dayTs, 0).In(usageCST).Format("2006-01-02")
		cell.finalize()
		mx.Cells = append(mx.Cells, cell)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return mx, nil
}

func (m *Monitor) computeUserTokenAggregatesFromFacts(ctx context.Context, uid, fromTs, toTs int64) ([]tokenUsageAggregate, error) {
	release, err := m.acquireUsageFactsReadBudget(ctx)
	if err != nil {
		return nil, fmt.Errorf("等待本地事实查询槽位失败: %w", err)
	}
	defer release()
	cte, args, err := usageFactsCTE([]int64{uid}, fromTs, toTs, 0)
	if err != nil {
		return nil, err
	}
	cctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	rows, err := m.usageFactsStore().WithContext(cctx).Raw(cte+`SELECT token_id,COALESCE(MAX(token_name),''),
 COALESCE(SUM(requests),0),COALESCE(SUM(refund_records),0),
 COALESCE(SUM(prompt_tokens),0)+COALESCE(SUM(completion_tokens),0),
 COALESCE(SUM(consume_quota),0),COALESCE(SUM(refund_quota),0)
 FROM facts GROUP BY token_id`, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("读取本地令牌事实失败: %w", err)
	}
	out := make([]tokenUsageAggregate, 0)
	for rows.Next() {
		var row tokenUsageAggregate
		if err := rows.Scan(&row.TokenID, &row.LogName, &row.Requests, &row.RefundRecords, &row.Tokens, &row.ConsumeQuota, &row.RefundQuota); err != nil {
			rows.Close()
			return nil, err
		}
		row.finalize()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

// 读取包装器保证切读开关关闭时保持旧行为；一旦运维显式请求本地事实读取，
// 即使完整性尚未通过也绝不能再回扫生产 logs。调用方会收到可识别的“暂不可用”
// 错误并做局部降级，避免一个页面请求在高峰期变成三条宽扫描。
func (m *Monitor) computeUsageStatsForRead(ctx context.Context, ids []int64, fromTs, toTs, tokenID int64) (*UsageStats, error) {
	return m.computeUsageStatsForReadRange(ctx, ids, fromTs, toTs, toTs, tokenID)
}

// computeUsageStatsForReadRange keeps the authoritative fact window separate
// from the originally requested right edge. The distinction matters around
// midnight: at 00:10 the last finalized hour ends exactly at today's 00:00,
// while the requested range already includes today and is eligible for the
// cumulative-watermark projection.
func (m *Monitor) computeUsageStatsForReadRange(ctx context.Context, ids []int64, fromTs, factToTs, requestedToTs, tokenID int64) (*UsageStats, error) {
	if m.usageFactsReadEnabled() {
		if factToTs > fromTs && !m.usageFactsPublishedRangeCovers(fromTs, factToTs) {
			return nil, errUsageFactsNotReady
		}
		if readyThrough := m.usageFactsReadyThrough.Load(); readyThrough > 0 && factToTs > readyThrough {
			factToTs = readyThrough
		}
		var st *UsageStats
		var err error
		if factToTs <= fromTs {
			displayTo := requestedToTs
			if displayTo <= fromTs {
				displayTo = fromTs
			}
			st = newUsageStatsRange(fromTs, displayTo)
		} else {
			st, err = m.computeUsageStatsFromFacts(ctx, ids, fromTs, factToTs, tokenID)
		}
		if err == nil {
			m.projectUsageStatsIfSafe(ctx, st, ids, fromTs, requestedToTs, tokenID, time.Now())
		}
		return st, err
	}
	if m.usageFactsReadRequested() {
		return nil, errUsageFactsNotReady
	}
	return m.computeUsageStats(ctx, ids, fromTs, requestedToTs, tokenID)
}

func (m *Monitor) computeUsageMatrixForRead(ctx context.Context, ids []int64, fromTs, toTs int64) (*UsageMatrix, error) {
	return m.computeUsageMatrixForReadRange(ctx, ids, fromTs, toTs, toTs)
}

func (m *Monitor) computeUsageMatrixForReadRange(ctx context.Context, ids []int64, fromTs, factToTs, requestedToTs int64) (*UsageMatrix, error) {
	if m.usageFactsReadEnabled() {
		if factToTs > fromTs && !m.usageFactsPublishedRangeCovers(fromTs, factToTs) {
			return nil, errUsageFactsNotReady
		}
		if readyThrough := m.usageFactsReadyThrough.Load(); readyThrough > 0 && factToTs > readyThrough {
			factToTs = readyThrough
		}
		var mx *UsageMatrix
		var err error
		if factToTs <= fromTs {
			displayTo := requestedToTs
			if displayTo <= fromTs {
				displayTo = fromTs
			}
			mx = newUsageMatrixRange(fromTs, displayTo)
		} else {
			mx, err = m.computeUsageMatrixFromFacts(ctx, ids, fromTs, factToTs)
		}
		if err == nil {
			m.projectUsageMatrixIfSafe(ctx, mx, ids, fromTs, requestedToTs, time.Now())
		}
		return mx, err
	}
	if m.usageFactsReadRequested() {
		return nil, errUsageFactsNotReady
	}
	return m.computeUsageMatrix(ctx, ids, fromTs, requestedToTs)
}

func (m *Monitor) computeUserTokenAggregatesForRead(ctx context.Context, uid, fromTs, toTs int64) ([]tokenUsageAggregate, error) {
	if m.usageFactsReadEnabled() {
		if !m.usageFactsPublishedRangeCovers(fromTs, toTs) {
			return nil, errUsageFactsNotReady
		}
		if readyThrough := m.usageFactsReadyThrough.Load(); readyThrough > 0 && toTs > readyThrough {
			toTs = readyThrough
		}
		if toTs <= fromTs {
			return []tokenUsageAggregate{}, nil
		}
		return m.computeUserTokenAggregatesFromFacts(ctx, uid, fromTs, toTs)
	}
	if m.usageFactsReadRequested() {
		return nil, errUsageFactsNotReady
	}
	return m.computeUserTokenAggregates(ctx, uid, fromTs, toTs)
}

func (m *Monitor) usageFactCacheKey(key string) string {
	if !m.usageFactsReadRequested() {
		return key
	}
	mode := "facts-pending"
	revision := m.usageFactsRevision.Load()
	if m.usageFactsReadEnabled() {
		mode = "facts"
		revision = m.usageFactsServingRevision.Load()
	}
	// requested 与 active 使用不同命名空间，避免切换期间误命中旧的来源库缓存。
	return key + ":" + mode + ":" + strconv.FormatInt(revision, 10)
}

// refreshTrackedLabelsForRead 在本地事实模式下只读本地资料快照。快照失效时返回
// 名单本身和 nil 金额，而非退回主站 users，从而保证资料故障不影响日志事实读取。
func (m *Monitor) refreshTrackedLabelsForRead(ctx context.Context, tracked []TrackedUser) ([]TrackedUser, map[int64]int64, map[int64]int64) {
	if !m.usageFactsReadRequested() {
		return m.refreshTrackedLabels(ctx, tracked)
	}
	balances, used := map[int64]int64{}, map[int64]int64{}
	if len(tracked) == 0 {
		return tracked, balances, used
	}
	var snapshots []UsageUserSnapshot
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if err := m.usageFactsStore().WithContext(qctx).Where("user_id IN ?", idsOf(tracked)).Find(&snapshots).Error; err != nil {
		slog.Warn("读取本地用户资料快照失败，沿用名单", "err", err)
		return tracked, balances, used
	}
	byID := make(map[int64]UsageUserSnapshot, len(snapshots))
	for _, snap := range snapshots {
		byID[snap.UserID] = snap
	}
	for i := range tracked {
		snap, ok := byID[tracked[i].UserID]
		if !ok || !snap.Exists {
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
	return tracked, balances, used
}

func (m *Monitor) userLiveUsageForRead(ctx context.Context, uid int64) (string, *int64, *int64) {
	if !m.usageFactsReadRequested() {
		return m.userLiveUsage(ctx, uid)
	}
	var owner string
	var snap UsageUserSnapshot
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if err := m.usageFactsStore().WithContext(qctx).First(&snap, "user_id = ?", uid).Error; err != nil || !snap.Exists {
		return owner, nil, nil
	}
	if snap.Username != "" {
		owner = snap.Username
	} else if snap.Email != "" {
		owner = snap.Email
	}
	balance, used := snap.BalanceQuota, snap.UsedQuota
	return owner, &balance, &used
}

func (m *Monitor) tokenMetaOfForRead(ctx context.Context, uid, tokenID int64) *TokenUsage {
	if !m.usageFactsReadRequested() {
		return m.tokenMetaOf(ctx, uid, tokenID)
	}
	var snap UsageTokenSnapshot
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if err := m.usageFactsStore().WithContext(qctx).Where("token_id = ? AND user_id = ?", tokenID, uid).First(&snap).Error; err != nil {
		return nil
	}
	total := float64(snap.UsedQuota) / quotaPerUSD
	return &TokenUsage{TokenID: tokenID, Name: snap.Name, MaskedKey: snap.MaskedKey, Group: snap.Grp, TotalCostQuota: &snap.UsedQuota, TotalCostUSD: &total, Deleted: snap.Deleted}
}

func (m *Monitor) usageOwnerLabelForRead(ctx context.Context, uid int64) string {
	owner, _, _ := m.userLiveUsageForRead(ctx, uid)
	return owner
}

func (m *Monitor) hydrateUserTokenUsageForRead(ctx context.Context, uid int64, aggregates []tokenUsageAggregate, ownerOverride *string) ([]TokenUsage, error) {
	if !m.usageFactsReadRequested() {
		return m.hydrateUserTokenUsage(ctx, uid, aggregates, ownerOverride)
	}
	var owner string
	if ownerOverride != nil && *ownerOverride != "" {
		owner = *ownerOverride
	} else {
		owner = m.usageOwnerLabelForRead(ctx, uid)
	}
	if owner == "" {
		owner = fmt.Sprintf("#%d", uid)
	}
	byToken := make(map[int64]tokenUsageAggregate, len(aggregates))
	for _, aggregate := range aggregates {
		byToken[aggregate.TokenID] = aggregate
	}
	var snapshots []UsageTokenSnapshot
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if err := m.usageFactsStore().WithContext(qctx).Where("user_id = ?", uid).Find(&snapshots).Error; err != nil {
		// 资料表异常时仍返回可用的日志聚合，不能将整个页面变成 500。
		slog.Warn("读取本地令牌资料快照失败，仅返回聚合事实", "err", err, "user_id", uid)
		snapshots = nil
	}
	out := make([]TokenUsage, 0, len(snapshots)+len(byToken))
	for _, snap := range snapshots {
		agg, usedInRange := byToken[snap.TokenID]
		if snap.Deleted && !usedInRange {
			continue
		}
		delete(byToken, snap.TokenID)
		name := snap.Name
		if name == "" {
			name = agg.LogName
		}
		if name == "" {
			name = "(未命名)"
		}
		total := float64(snap.UsedQuota) / quotaPerUSD
		out = append(out, TokenUsage{UsageBilling: agg.UsageBilling, TokenID: snap.TokenID, Owner: owner, Name: name, MaskedKey: snap.MaskedKey, Group: snap.Grp, Tokens: agg.Tokens, TotalCostQuota: &snap.UsedQuota, TotalCostUSD: &total, Deleted: snap.Deleted})
	}
	for tokenID, agg := range byToken {
		name := agg.LogName
		if name == "" {
			name = "(未命名)"
		}
		out = append(out, TokenUsage{UsageBilling: agg.UsageBilling, TokenID: tokenID, Owner: owner, Name: name, Tokens: agg.Tokens, Deleted: true})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Deleted != out[j].Deleted {
			return !out[i].Deleted
		}
		if out[i].ConsumeQuota != out[j].ConsumeQuota {
			return out[i].ConsumeQuota > out[j].ConsumeQuota
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (m *Monitor) sumUsageLiveQuotasForRead(ctx context.Context, ids []int64) (balance, used *int64) {
	if !m.usageFactsReadRequested() {
		return m.sumUsageLiveQuotas(ctx, ids)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var rows []UsageUserSnapshot
	qctx, cancel := usageFactQueryContext(ctx)
	defer cancel()
	if err := m.usageFactsStore().WithContext(qctx).Where("user_id IN ? AND `exists` = ?", ids, true).Find(&rows).Error; err != nil {
		slog.Warn("读取本地分组资料快照失败", "err", err)
		return nil, nil
	}
	if len(rows) == 0 {
		return nil, nil
	}
	var b, u int64
	for _, row := range rows {
		b += row.BalanceQuota
		u += row.UsedQuota
	}
	return &b, &u
}
