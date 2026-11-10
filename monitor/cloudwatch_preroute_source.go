package monitor

// CloudWatch 前置拒绝连续采集。
//
// 这条 lane 与按需排障、Shadow 对账互相独立：它只执行仓库内固定的
// CloudWatch Logs Insights 查询，把已完整覆盖窗口中的前置拒绝按分钟聚合后
// 原子替换到 rejection_samples。页面只读本地事实，刷新不会访问 AWS。

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	cloudWatchPreRouteCursorID      = uint(1)
	cloudWatchPreRouteVersion       = 1
	cloudWatchPreRouteNode          = "cloudwatch-direct"
	cloudWatchPreRouteFinalizeDelay = 2 * time.Minute
	cloudWatchPreRouteReplay        = 20 * time.Minute
	cloudWatchPreRouteWindow        = time.Hour
	cloudWatchPreRouteCatchupDelay  = 2 * time.Second
	cloudWatchPreRouteQueryBudget   = 2 * time.Minute
	cloudWatchPreRouteInitRetry     = 15 * time.Second
)

// CloudWatchPreRouteCursor 是连续覆盖水位。ThroughTs 只在一个完整窗口已经
// 查询、解析并原子发布后向右推进；失败或截断时原地重试，不把缺口当成零。
type CloudWatchPreRouteCursor struct {
	ID               uint   `gorm:"primaryKey;autoIncrement:false"`
	CoverageFromTs   int64  `gorm:"column:coverage_from_ts"`
	NextTs           int64  `gorm:"column:next_ts;index"`
	ThroughTs        int64  `gorm:"column:through_ts"`
	TargetThroughTs  int64  `gorm:"column:target_through_ts"`
	SemanticsVersion int    `gorm:"column:semantics_version;index"`
	Status           string `gorm:"size:24;index"`
	LastSuccessAt    int64  `gorm:"column:last_success_at"`
	LastFailureAt    int64  `gorm:"column:last_failure_at"`
	LastError        string `gorm:"size:64;column:last_error"`
	UpdatedAt        int64  `gorm:"column:updated_at"`
}

func cloudWatchPreRouteRange(now time.Time, lookbackHours int) (int64, int64) {
	if now.IsZero() {
		now = time.Now()
	}
	through := now.Add(-cloudWatchPreRouteFinalizeDelay).Unix() / 60 * 60
	from := through - int64(lookbackHours)*3600
	if from < 0 {
		from = 0
	}
	return from / 60 * 60, through
}

func (m *Monitor) loadCloudWatchPreRouteCursor(now time.Time) (CloudWatchPreRouteCursor, error) {
	from, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	var state CloudWatchPreRouteCursor
	err := m.storeDB.First(&state, "id = ?", cloudWatchPreRouteCursorID).Error
	if err == nil && state.SemanticsVersion == cloudWatchPreRouteVersion {
		state.TargetThroughTs = target
		if state.CoverageFromTs <= 0 {
			state.CoverageFromTs = from
		}
		if state.NextTs < state.CoverageFromTs {
			state.NextTs = state.CoverageFromTs
		}
		if state.ThroughTs < state.CoverageFromTs {
			state.ThroughTs = state.CoverageFromTs
		}
		state.UpdatedAt = now.Unix()
		if saveErr := m.storeDB.Save(&state).Error; saveErr != nil {
			return CloudWatchPreRouteCursor{}, saveErr
		}
		return state, nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return CloudWatchPreRouteCursor{}, err
	}
	state = CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from,
		ThroughTs: from, TargetThroughTs: target, SemanticsVersion: cloudWatchPreRouteVersion,
		Status: "running", UpdatedAt: now.Unix(),
	}
	// 语义升级只清理这条 CloudWatch lane 自己写入的行；历史旁路采集器数据
	// 属于另一来源，不能顺手删除。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("node = ?", cloudWatchPreRouteNode).Delete(&RejectionSample{}).Error; err != nil {
			return err
		}
		return tx.Save(&state).Error
	}); err != nil {
		return CloudWatchPreRouteCursor{}, err
	}
	return state, nil
}

func cloudWatchPreRouteErrorCode(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "truncated"):
		return "truncated"
	case strings.Contains(message, "parse"):
		return "parse_failed"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return string(cloudWatchLogsErrorKindOf(err))
	}
}

func (m *Monitor) recordCloudWatchPreRouteFailure(state *CloudWatchPreRouteCursor, cause error, now int64) {
	m.cloudWatchPreRouteLastFailure.Store(now)
	if state == nil || m.storeDB == nil {
		return
	}
	state.Status = "degraded"
	state.LastFailureAt = now
	state.LastError = cloudWatchPreRouteErrorCode(cause)
	state.UpdatedAt = now
	_ = m.storeDB.Save(state).Error
}

func (m *Monitor) queryCloudWatchPreRouteRange(ctx context.Context, parser *cloudWatchEvidenceParser,
	from, to time.Time, queries *int, depth int) ([]cloudWatchStructuredEvidence, error) {
	return m.queryCloudWatchCompleteEvidenceRange(ctx, parser, cwQueryWorkerPreRouteReject,
		cloudWatchLogsPreRouteLimit, from, to, queries, depth)
}

func cloudWatchPreRouteSamples(evidence []cloudWatchStructuredEvidence, from, to int64) []RejectionSample {
	type key struct {
		bucket       int64
		reason       string
		model, group string
		userID       int64
	}
	counts := make(map[key]int64)
	seen := make(map[string]struct{}, len(evidence))
	for _, item := range evidence {
		if item.Kind != cwEvidenceNewAPIError || !isShadowRejectionCategory(item.Category) {
			continue
		}
		second := item.EventMS / 1000
		if second < from || second >= to {
			continue
		}
		if item.EventRef != "" {
			if _, exists := seen[item.EventRef]; exists {
				continue
			}
			seen[item.EventRef] = struct{}{}
		}
		userID := int64(0)
		if item.UserID != nil {
			userID = *item.UserID
		}
		k := key{bucket: second / 60 * 60, reason: canonicalShadowRejectionReason(item.Category),
			model: strings.TrimSpace(item.Model), group: strings.TrimSpace(item.Group), userID: userID}
		counts[k]++
	}
	rows := make([]RejectionSample, 0, len(counts))
	for k, count := range counts {
		rows = append(rows, RejectionSample{BucketTs: k.bucket, Node: cloudWatchPreRouteNode,
			Reason: k.reason, Model: k.model, Grp: k.group, UserID: k.userID, Count: count})
	}
	return rows
}

func (m *Monitor) publishCloudWatchPreRouteWindow(ctx context.Context, state *CloudWatchPreRouteCursor,
	from, to int64, rows []RejectionSample, target int64) error {
	finishedAt := time.Now().Unix()
	next := *state
	if to > next.ThroughTs {
		next.ThroughTs = to
	}
	next.NextTs = to
	next.TargetThroughTs = target
	next.Status = "running"
	if next.ThroughTs >= target {
		next.Status = "caught_up"
	}
	next.LastSuccessAt = finishedAt
	next.LastError = ""
	next.UpdatedAt = finishedAt
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("node = ? AND bucket_ts >= ? AND bucket_ts < ?", cloudWatchPreRouteNode, from, to).
			Delete(&RejectionSample{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := tx.Create(&rows).Error; err != nil {
				return err
			}
		}
		return tx.Save(&next).Error
	})
	if err != nil {
		return err
	}
	*state = next
	return nil
}

func (m *Monitor) runCloudWatchPreRouteWindow(ctx context.Context, state *CloudWatchPreRouteCursor,
	from, to, target int64) (int, error) {
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		return 0, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, cloudWatchPreRouteQueryBudget)
	defer cancel()
	queries := 0
	evidence, err := m.queryCloudWatchPreRouteRange(queryCtx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
	if err != nil {
		return queries, err
	}
	rows := cloudWatchPreRouteSamples(evidence, from, to)
	if err := m.publishCloudWatchPreRouteWindow(ctx, state, from, to, rows, target); err != nil {
		return queries, err
	}
	m.cloudWatchPreRouteFrom.Store(state.CoverageFromTs)
	m.cloudWatchPreRouteThrough.Store(state.ThroughTs)
	m.cloudWatchPreRouteLastSuccess.Store(state.LastSuccessAt)
	return queries, nil
}

func (m *Monitor) startCloudWatchPreRoute(ctx context.Context) {
	if m == nil || !m.cfg.CloudWatchPreRouteEnabled || m.storeDB == nil || m.cloudWatchLogs == nil || !m.cloudWatchLogs.enabled {
		return
	}
	go func() {
		m.cloudWatchPreRouteRunning.Store(true)
		defer m.cloudWatchPreRouteRunning.Store(false)
		poll := time.Duration(m.cfg.CloudWatchPreRoutePollSeconds) * time.Second
		if poll <= 0 {
			poll = 5 * time.Minute
		}
		livePoll := time.Duration(m.cfg.CloudWatchPreRouteLivePollSeconds) * time.Second
		if livePoll < time.Minute {
			livePoll = time.Minute
		}
		if livePoll > poll {
			livePoll = poll
		}
		var nextReplay time.Time
		initRetry := cloudWatchPreRouteInitRetry
		if poll < initRetry {
			initRetry = poll
		}
		var state CloudWatchPreRouteCursor
		for {
			var err error
			state, err = m.loadCloudWatchPreRouteCursor(time.Now())
			if err == nil {
				break
			}
			m.recordCloudWatchPreRouteFailure(nil, err, time.Now().Unix())
			slog.Error("初始化 CloudWatch 前置拒绝采集水位失败，将重试", "err", cloudWatchPreRouteErrorCode(err))
			if !waitSourceLifecycle(ctx, initRetry) {
				return
			}
		}
		m.cloudWatchPreRouteFrom.Store(state.CoverageFromTs)
		m.cloudWatchPreRouteThrough.Store(state.ThroughTs)
		// Restore the durable health timestamps before the first poll.  Without
		// this, a restart temporarily reports a previously degraded lane as
		// healthy (and the alerts page loses its historical failure hint) until
		// the next CloudWatch request finishes.
		m.cloudWatchPreRouteLastSuccess.Store(state.LastSuccessAt)
		m.cloudWatchPreRouteLastFailure.Store(state.LastFailureAt)
		for {
			if ctx.Err() != nil {
				return
			}
			started := time.Now()
			_, target := cloudWatchPreRouteRange(started, m.cfg.CloudWatchPreRouteLookbackHours)
			from, to, replay := cloudWatchPreRouteNextWindow(state, target, started, nextReplay)
			state.TargetThroughTs = target
			if to <= from {
				if !waitSourceLifecycle(ctx, livePoll) {
					return
				}
				continue
			}
			queries, runErr := m.runCloudWatchPreRouteWindow(ctx, &state, from, to, target)
			if runErr != nil {
				m.recordCloudWatchPreRouteFailure(&state, runErr, time.Now().Unix())
				slog.Warn("CloudWatch 前置拒绝持续采集失败(保留水位重试)",
					"from", from, "to", to, "queries", queries, "class", cloudWatchPreRouteErrorCode(runErr))
				if !waitSourceLifecycle(ctx, poll) {
					return
				}
				continue
			}
			slog.Info("CloudWatch 前置拒绝持续采集完成窗口", "from", from, "to", to,
				"through", state.ThroughTs, "target", target, "queries", queries, "replay", replay,
				"duration_ms", time.Since(started).Milliseconds())
			if replay {
				nextReplay = started.Add(poll)
			}
			delay := cloudWatchPreRouteCatchupDelay
			if state.ThroughTs >= target {
				delay = sourceCadenceDelay(started, time.Now(), livePoll)
			}
			if !waitSourceLifecycle(ctx, delay) {
				return
			}
		}
	}()
}

// A minute increment no longer waits for the five-minute replay cadence. The
// replay span and cadence stay bounded, rather than scanning twenty minutes on
// every live poll. A failed replay remains due; publication still requires the
// whole source query and the local fact transaction to succeed.
func cloudWatchPreRouteNextWindow(state CloudWatchPreRouteCursor, target int64, now, nextReplay time.Time) (from, to int64, replay bool) {
	from = state.NextTs
	replay = !now.Before(nextReplay) && (state.Status == "caught_up" || state.Status == "degraded")
	if replay {
		from = max(state.CoverageFromTs, state.ThroughTs-int64(cloudWatchPreRouteReplay/time.Second))
	}
	to = min(target, from+int64(cloudWatchPreRouteWindow/time.Second))
	return
}

// Keep the configured start-to-start cadence. Query time must not be added to
// a full sleep on every successful cycle; retain a small yield after slow work.
func sourceCadenceDelay(started, now time.Time, period time.Duration) time.Duration {
	return max(2*time.Second, period-now.Sub(started))
}
