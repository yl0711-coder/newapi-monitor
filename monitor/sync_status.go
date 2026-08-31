package monitor

// sync_status.go exposes a bounded, local-only operational projection. The
// domain tables remain authoritative: this layer never advances cursors,
// renews leases, changes retry deadlines, or wakes a worker.

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	syncStatusCacheTTL        = 10 * time.Second
	syncStatusDefaultPageSize = 50
	syncStatusMaxPageSize     = 100
)

type syncStatusSection[T any] struct {
	Available bool   `json:"available"`
	CheckedAt int64  `json:"checked_at"`
	Error     string `json:"error,omitempty"`
	Data      T      `json:"data"`
}

type syncUsageStatus struct {
	Facts                    UsageFactsStatus         `json:"facts"`
	History                  UsageFactHistoryProgress `json:"history"`
	Store                    StoreReliabilityStatus   `json:"store"`
	FactsStore               StoreReliabilityStatus   `json:"facts_store"`
	AttentionMembers         int                      `json:"attention_members"`
	ReturnedAttentionMembers int                      `json:"returned_attention_members"`
	AttentionTruncated       bool                     `json:"attention_truncated"`
	MaintenanceTotal         int                      `json:"maintenance_total"`
	MaintenanceAttention     int                      `json:"maintenance_attention"`
}

type syncUpstreamDomainStatus struct {
	Domain   string                     `json:"domain"`
	Upstream ChannelUpstreamAccountView `json:"upstream"`
}

type syncUpstreamStatus struct {
	Enabled  bool                       `json:"enabled"`
	Accounts int                        `json:"accounts"`
	Domains  []syncUpstreamDomainStatus `json:"domains"`
}

type syncOverviewResponse struct {
	Version     int                                        `json:"version"`
	SnapshotAt  int64                                      `json:"snapshot_at"`
	Runtime     syncStatusSection[readyStatusResponse]     `json:"runtime"`
	Usage       syncStatusSection[syncUsageStatus]         `json:"usage"`
	Stability   syncStatusSection[stabilityHealthResponse] `json:"stability"`
	Upstream    syncStatusSection[syncUpstreamStatus]      `json:"upstream"`
	CostClosure syncStatusSection[syncChannelCostStatus]   `json:"cost_closure"`
}

type syncStatusSnapshot struct {
	Overview    syncOverviewResponse
	FullHistory UsageFactHistoryProgress
}

type syncStatusUsageRead struct {
	Facts      UsageFactsStatus
	Store      StoreReliabilityStatus
	FactsStore StoreReliabilityStatus
}

type syncStatusReaders struct {
	Usage       func(context.Context) (syncStatusUsageRead, error)
	Stability   func(context.Context) (stabilityHealthResponse, error)
	Upstream    func(context.Context) (map[string]ChannelUpstreamAccountView, error)
	CostClosure func(context.Context) (syncChannelCostStatus, error)
}

type syncStatusReadResult[T any] struct {
	Data T
	Err  error
}

func launchSyncStatusRead[T any](ctx context.Context, label string, read func(context.Context) (T, error)) <-chan syncStatusReadResult[T] {
	result := make(chan syncStatusReadResult[T], 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- syncStatusReadResult[T]{Err: fmt.Errorf("%s状态读取异常", label)}
			}
		}()
		data, err := read(ctx)
		result <- syncStatusReadResult[T]{Data: data, Err: err}
	}()
	return result
}

func awaitSyncStatusRead[T any](ctx context.Context, result <-chan syncStatusReadResult[T]) syncStatusReadResult[T] {
	// 总预算刚到期时，已完成的分区仍应返回，不能因 select
	// 在 ready channel 和 ctx.Done 之间随机选择而误报不可用。
	select {
	case value := <-result:
		return value
	default:
	}
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		return syncStatusReadResult[T]{Err: ctx.Err()}
	}
}

func usageHistoryMemberNeedsAttention(member UsageFactHistoryMemberProgress) bool {
	return !member.ServiceReady || !member.ArchiveReady || member.JobStatus == usageFactHistoryJobPaused ||
		member.LastError != "" || member.LiveLastError != "" || member.RecentLastError != "" ||
		member.RawPageLastError != "" || (member.LiveStatus != "" && member.LiveStatus != "ready") ||
		(member.RecentStatus != "" && member.RecentStatus != "ready")
}

func usageMaintenanceNeedsAttention(job UsageFactMaintenanceProgress) bool {
	return !usageFactHistoryTerminal(job.Status) || job.LastError != ""
}

func boundedUsageHistoryProjection(full UsageFactHistoryProgress) (UsageFactHistoryProgress, int, int) {
	projection := full
	attention := make([]UsageFactHistoryMemberProgress, 0, min(len(full.Members), syncStatusDefaultPageSize))
	total := 0
	for _, member := range full.Members {
		if !usageHistoryMemberNeedsAttention(member) {
			continue
		}
		total++
		if len(attention) < syncStatusDefaultPageSize {
			attention = append(attention, member)
		}
	}
	projection.Members = attention
	maintenance := make([]UsageFactMaintenanceProgress, 0, min(len(full.Maintenance), syncStatusDefaultPageSize))
	maintenanceAttention := 0
	for _, job := range full.Maintenance {
		if !usageMaintenanceNeedsAttention(job) {
			continue
		}
		maintenanceAttention++
		if len(maintenance) < syncStatusDefaultPageSize {
			maintenance = append(maintenance, job)
		}
	}
	projection.Maintenance = maintenance
	return projection, total, maintenanceAttention
}

func (m *Monitor) defaultSyncStatusReaders(now time.Time) syncStatusReaders {
	return syncStatusReaders{
		Usage: func(ctx context.Context) (syncStatusUsageRead, error) {
			facts, err := m.usageFactsStatus(ctx, now)
			return syncStatusUsageRead{Facts: facts, Store: m.storeReliabilityStatus(), FactsStore: m.usageFactsStoreReliabilityStatus()}, err
		},
		Stability: func(ctx context.Context) (stabilityHealthResponse, error) {
			return m.stabilityHealth(ctx, now), nil
		},
		Upstream:    m.loadChannelUpstreamViews,
		CostClosure: m.channelCostSyncStatus,
	}
}

func (m *Monitor) buildSyncStatusSnapshot(ctx context.Context, now time.Time) *syncStatusSnapshot {
	return m.buildSyncStatusSnapshotWith(ctx, now, m.defaultSyncStatusReaders(now))
}

func (m *Monitor) buildSyncStatusSnapshotWith(ctx context.Context, now time.Time, readers syncStatusReaders) *syncStatusSnapshot {
	nowUnix := now.Unix()
	ready, _ := m.readyStatus(now)
	snapshot := &syncStatusSnapshot{
		Overview: syncOverviewResponse{
			Version: 1, SnapshotAt: nowUnix,
			Runtime: syncStatusSection[readyStatusResponse]{Available: true, CheckedAt: nowUnix, Data: ready},
		},
	}

	// 四个业务分区并行读本地状态；某一分区超时不阻断其他分区返回。
	overallCtx, overallCancel := context.WithTimeout(ctx, 5*time.Second)
	defer overallCancel()
	usageCtx, usageCancel := context.WithTimeout(overallCtx, 4*time.Second)
	defer usageCancel()
	stabilityCtx, stabilityCancel := context.WithTimeout(overallCtx, 2*time.Second)
	defer stabilityCancel()
	upstreamCtx, upstreamCancel := context.WithTimeout(overallCtx, 2*time.Second)
	defer upstreamCancel()
	costCtx, costCancel := context.WithTimeout(overallCtx, 2*time.Second)
	defer costCancel()
	usageCh := launchSyncStatusRead(usageCtx, "用量事实", readers.Usage)
	stabilityCh := launchSyncStatusRead(stabilityCtx, "稳定性", readers.Stability)
	upstreamCh := launchSyncStatusRead(upstreamCtx, "上游账户", readers.Upstream)
	costRead := readers.CostClosure
	if costRead == nil {
		costRead = func(context.Context) (syncChannelCostStatus, error) { return syncChannelCostStatus{}, nil }
	}
	costCh := launchSyncStatusRead(costCtx, "渠道成本闭环", costRead)

	usageResult := awaitSyncStatusRead(overallCtx, usageCh)
	stabilityResult := awaitSyncStatusRead(overallCtx, stabilityCh)
	upstreamResult := awaitSyncStatusRead(overallCtx, upstreamCh)
	costResult := awaitSyncStatusRead(overallCtx, costCh)

	usage := syncUsageStatus{Store: usageResult.Data.Store, FactsStore: usageResult.Data.FactsStore}
	if usageResult.Err != nil {
		snapshot.Overview.Usage = syncStatusSection[syncUsageStatus]{
			Available: false, CheckedAt: nowUnix, Error: "读取本地用量事实状态失败", Data: usage,
		}
	} else {
		facts := usageResult.Data.Facts
		full := UsageFactHistoryProgress{AsOf: nowUnix}
		if facts.FullHistory != nil {
			full = *facts.FullHistory
		}
		snapshot.FullHistory = full
		facts.FullHistory = nil // 避免摘要中再次携带完整成员任务。
		projected, attentionTotal, maintenanceAttention := boundedUsageHistoryProjection(full)
		usage.Facts = facts
		usage.History = projected
		usage.AttentionMembers = attentionTotal
		usage.ReturnedAttentionMembers = len(projected.Members)
		usage.AttentionTruncated = attentionTotal > len(projected.Members)
		usage.MaintenanceTotal = len(full.Maintenance)
		usage.MaintenanceAttention = maintenanceAttention
		snapshot.Overview.Usage = syncStatusSection[syncUsageStatus]{
			Available: true, CheckedAt: max(full.AsOf, nowUnix), Data: usage,
		}
	}

	stability := stabilityResult.Data
	if stabilityResult.Err != nil {
		snapshot.Overview.Stability = syncStatusSection[stabilityHealthResponse]{Available: false, CheckedAt: nowUnix, Error: "读取本地稳定性状态失败"}
	} else {
		snapshot.Overview.Stability = syncStatusSection[stabilityHealthResponse]{Available: true, CheckedAt: stability.CheckedAt, Data: stability}
	}

	views, upstreamErr := upstreamResult.Data, upstreamResult.Err
	upstream := syncUpstreamStatus{Enabled: m.cfg.UpstreamSyncEnabled || m.cfg.UpstreamUsageSyncEnabled || m.cfg.UpstreamPricingLedgerEnabled || m.cfg.UpstreamErrorLogSyncEnabled}
	if upstreamErr != nil {
		snapshot.Overview.Upstream = syncStatusSection[syncUpstreamStatus]{
			Available: false, CheckedAt: nowUnix, Error: "读取本地上游同步状态失败", Data: upstream,
		}
	} else {
		domains := make([]string, 0, len(views))
		for domain := range views {
			domains = append(domains, domain)
		}
		sort.Strings(domains)
		upstream.Accounts = len(domains)
		upstream.Domains = make([]syncUpstreamDomainStatus, 0, len(domains))
		for _, domain := range domains {
			upstream.Domains = append(upstream.Domains, syncUpstreamDomainStatus{Domain: domain, Upstream: views[domain]})
		}
		snapshot.Overview.Upstream = syncStatusSection[syncUpstreamStatus]{
			Available: true, CheckedAt: nowUnix, Data: upstream,
		}
	}
	if costResult.Err != nil {
		snapshot.Overview.CostClosure = syncStatusSection[syncChannelCostStatus]{Available: false, CheckedAt: nowUnix, Error: "读取本地渠道成本闭环状态失败"}
	} else {
		snapshot.Overview.CostClosure = syncStatusSection[syncChannelCostStatus]{Available: true, CheckedAt: nowUnix, Data: costResult.Data}
	}
	return snapshot
}

func (m *Monitor) lockSyncStatusCache(ctx context.Context) error {
	if m.syncStatusMu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if m.syncStatusMu.TryLock() {
				return nil
			}
		}
	}
}

func (m *Monitor) currentSyncStatusSnapshot(ctx context.Context, force bool) (*syncStatusSnapshot, error) {
	requestedAt := time.Now()
	if err := m.lockSyncStatusCache(ctx); err != nil {
		return nil, err
	}
	defer m.syncStatusMu.Unlock()
	// 并发的强制刷新也只构建一次：等锁期间若已有更新的快照，直接复用。
	if m.syncStatusCached != nil && time.Since(m.syncStatusCachedAt) < syncStatusCacheTTL && (!force || m.syncStatusCachedAt.After(requestedAt)) {
		return m.syncStatusCached, nil
	}
	snapshot := m.buildSyncStatusSnapshot(ctx, time.Now())
	// 页面切换会主动取消旧请求；这种取消不能把一份已取消或
	// 部分超时的快照发布进共享短缓存。
	if ctx.Err() != nil {
		return snapshot, ctx.Err()
	}
	m.syncStatusCached = snapshot
	m.syncStatusCachedAt = time.Now()
	return snapshot, nil
}

func (m *Monitor) invalidateSyncStatusCache() {
	m.syncStatusMu.Lock()
	m.syncStatusCached = nil
	m.syncStatusCachedAt = time.Time{}
	m.syncStatusMu.Unlock()
}

func (m *Monitor) serveSyncOverview(c *gin.Context) {
	force := strings.TrimSpace(c.Query("refresh")) == "1"
	snapshot, err := m.currentSyncStatusSnapshot(c.Request.Context(), force)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "同步状态快照暂不可用"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, snapshot.Overview)
}

type syncUsageWorkloadsResponse struct {
	AsOf     int64                            `json:"as_of"`
	Kind     string                           `json:"kind"`
	State    string                           `json:"state"`
	Total    int                              `json:"total"`
	AllTotal int                              `json:"all_total"`
	Offset   int                              `json:"offset"`
	Limit    int                              `json:"limit"`
	Items    []UsageFactHistoryMemberProgress `json:"items,omitempty"`
	Jobs     []UsageFactMaintenanceProgress   `json:"jobs,omitempty"`
}

func syncPositiveInt(raw string, fallback, maximum int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

func (m *Monitor) serveSyncWorkloads(c *gin.Context) {
	if domain := strings.TrimSpace(c.Query("domain")); domain != "" && domain != "usage" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "当前只支持 usage 工作负载明细"})
		return
	}
	state := strings.TrimSpace(c.Query("state"))
	if state == "" {
		state = "attention"
	}
	if state != "attention" && state != "all" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "state 仅支持 attention 或 all"})
		return
	}
	kind := strings.TrimSpace(c.Query("kind"))
	if kind == "" {
		kind = "members"
	}
	if kind != "members" && kind != "maintenance" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "kind 仅支持 members 或 maintenance"})
		return
	}
	offset := syncPositiveInt(c.Query("offset"), 0, 1_000_000)
	limit := syncPositiveInt(c.Query("limit"), syncStatusDefaultPageSize, syncStatusMaxPageSize)
	if limit < 1 {
		limit = syncStatusDefaultPageSize
	}
	userID := int64(0)
	if raw := strings.TrimSpace(c.Query("user_id")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 无效"})
			return
		}
		userID = parsed
	}

	snapshot, err := m.currentSyncStatusSnapshot(c.Request.Context(), false)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "用量同步任务暂不可用"})
		return
	}
	if !snapshot.Overview.Usage.Available {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": snapshot.Overview.Usage.Error})
		return
	}
	if kind == "maintenance" {
		filtered := make([]UsageFactMaintenanceProgress, 0, len(snapshot.FullHistory.Maintenance))
		for _, job := range snapshot.FullHistory.Maintenance {
			if userID > 0 && job.UserID != userID {
				continue
			}
			if state == "attention" && !usageMaintenanceNeedsAttention(job) {
				continue
			}
			filtered = append(filtered, job)
		}
		end := min(offset+limit, len(filtered))
		jobs := []UsageFactMaintenanceProgress{}
		if offset < len(filtered) {
			jobs = filtered[offset:end]
		}
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, syncUsageWorkloadsResponse{
			AsOf: snapshot.FullHistory.AsOf, Kind: kind, State: state, Total: len(filtered), AllTotal: len(snapshot.FullHistory.Maintenance),
			Offset: offset, Limit: limit, Jobs: jobs,
		})
		return
	}
	filtered := make([]UsageFactHistoryMemberProgress, 0, len(snapshot.FullHistory.Members))
	for _, member := range snapshot.FullHistory.Members {
		if userID > 0 && member.UserID != userID {
			continue
		}
		if state == "attention" && !usageHistoryMemberNeedsAttention(member) {
			continue
		}
		filtered = append(filtered, member)
	}
	end := min(offset+limit, len(filtered))
	items := []UsageFactHistoryMemberProgress{}
	if offset < len(filtered) {
		items = filtered[offset:end]
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, syncUsageWorkloadsResponse{
		AsOf: snapshot.FullHistory.AsOf, Kind: kind, State: state, Total: len(filtered), AllTotal: len(snapshot.FullHistory.Members),
		Offset: offset, Limit: limit, Items: items,
	})
}
