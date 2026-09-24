package monitor

// 固定后台采集共用的“完整窗口”查询器。它与人工排障、Shadow 分离：
// 调用方只能选择仓库内的固定查询，撞到结果上限时递归拆分，任何解析失败
// 都让整段窗口失败，从而保证持久水位绝不越过已知缺口。

import (
	"context"
	"fmt"
	"time"
)

const (
	cloudWatchContinuousMaxQueriesTurn = 64
	cloudWatchContinuousMaxSplitDepth  = 12
	// A complete CloudWatch result can be below AWS's 10,000-row limit and
	// still be too large to hold comfortably for one evidence replacement.
	// Keep each query leaf bounded; persistence has a separate transaction
	// batch bound below, so high-volume windows are split before the database.
	cloudWatchNginxPersistMaxEvents = 4000
	// Evidence rows are committed in independent transactions below this
	// bound.  A complete CloudWatch window may contain many query leaves (and
	// therefore many thousands of rows); keeping the delete/rebuild work in
	// one SQLite transaction can exhaust the container's transient storage
	// even when the evidence database itself still has room.
	cloudWatchNginxEvidencePersistBatchSize = 500
)

func (m *Monitor) queryCloudWatchCompleteEvidenceRange(ctx context.Context, parser *cloudWatchEvidenceParser,
	kind cloudWatchFixedQueryKind, limit int, from, to time.Time, queries *int, depth int) ([]cloudWatchStructuredEvidence, error) {
	if !from.Before(to) {
		return nil, nil
	}
	if depth > cloudWatchContinuousMaxSplitDepth || *queries >= cloudWatchContinuousMaxQueriesTurn {
		return nil, fmt.Errorf("cloudwatch continuous truncated query budget")
	}
	*queries++
	result, err := m.cloudWatchLogs.insights(ctx, cloudWatchInsightsRequest{
		Kind: kind, From: from, To: to, Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	if result.Truncated {
		if to.Sub(from) <= time.Second {
			return nil, fmt.Errorf("cloudwatch continuous truncated second")
		}
		mid := from.Add(to.Sub(from) / 2).Truncate(time.Second)
		if !mid.After(from) || !mid.Before(to) {
			return nil, fmt.Errorf("cloudwatch continuous truncated unsplittable window")
		}
		left, err := m.queryCloudWatchCompleteEvidenceRange(ctx, parser, kind, limit, from, mid, queries, depth+1)
		if err != nil {
			return nil, err
		}
		right, err := m.queryCloudWatchCompleteEvidenceRange(ctx, parser, kind, limit, mid, to, queries, depth+1)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil
	}
	batch := m.cloudWatchLogs.parseInsightsEvidence(parser, result)
	if batch.ParseFailed > 0 {
		return nil, fmt.Errorf("cloudwatch continuous parse failed")
	}
	if kind == cwQueryWorkerNginxContinuous && len(batch.Evidence) > cloudWatchNginxPersistMaxEvents {
		if to.Sub(from) <= time.Second {
			return nil, fmt.Errorf("cloudwatch continuous evidence batch exceeds bounded persistence size")
		}
		mid := from.Add(to.Sub(from) / 2).Truncate(time.Second)
		if !mid.After(from) || !mid.Before(to) {
			return nil, fmt.Errorf("cloudwatch continuous evidence batch cannot be split")
		}
		left, err := m.queryCloudWatchCompleteEvidenceRange(ctx, parser, kind, limit, from, mid, queries, depth+1)
		if err != nil {
			return nil, err
		}
		right, err := m.queryCloudWatchCompleteEvidenceRange(ctx, parser, kind, limit, mid, to, queries, depth+1)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil
	}
	return batch.Evidence, nil
}
