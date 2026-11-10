package monitor

// FRT 总和一次性受控回算。
//
// 为什么需要它：frt_sum_ms / frt_sum_observed / frt_sum_version 是后加的列，
// 既有分钟行写入时还没有这几列，但请求覆盖水位早已推过那些时间，所以
//   - 普通 /admin/backfill 走 sampleMetricRangeLow（includeCapacity=false），
//     只写 metric_samples，而首字耗时页读的是 capacity_user_minute_samples；
//   - 客户维护 worker 的历史补采以"请求覆盖水位"判断某天是否已采，
//     水位已满就不会回头重采。
// 两条既有链路都补不回这三列，因此单独提供本任务。
//
// 边界（逐条对应用户要求，不得放宽）：
//   - 口径与 sampleWindowUserSQL 的 FRT 投影完全一致：只累加
//     type=2 AND is_stream=1 AND FRT>0 的真实 FRT 毫秒值，样本数同条件计数。
//     绝不使用 use_time（请求总耗时）、P95/P99、直方图桶中点，
//     也绝不用部分总和除全量样本。
//   - 只 UPDATE 已存在行的这三列，不插入新行、不碰请求数/Token/金额/
//     客户维护统计/直方图/语义版本。无法回算的行保持 version=0，
//     页面继续显示"不可计算"，不补零。
//   - 不推进任何请求覆盖水位。本任务只写自己的独立状态表。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 按分钟切片读取生产日志；与客户维护采集同量级，避免长事务与大结果集。
const frtRecomputeSliceSeconds = 600

// 单个切片的有界重试次数。来源 lease 被常态采集抢占是瞬时的，退避后通常即可成功；
// 给上界是为了让真正的来源故障仍然如实失败，而不是无限重试把问题藏起来。
const frtRecomputeSliceRetries = 5

// Kept as a small indirection so retry tests can exercise the real source
// failure path without waiting through the production backoff schedule. The
// production default remains the normal bounded sleep.
var frtRecomputeRetryWait = waitSourceLifecycle

// A database lease is the cross-process guard. The in-process flag below is
// still useful for returning a quick 409 from the common single-process HTTP
// path, but it is not a correctness mechanism on its own.
const frtRecomputeLeaseSeconds int64 = 120

var frtRecomputeRunning atomic.Bool
var frtRecomputeOwnerSeq atomic.Uint64

var errFRTRecomputeLeaseHeld = errors.New("已有 FRT 总和回算持有数据库租约")

// FRTSumRecomputeState 是本任务独立的持久状态，和请求覆盖、FRT 样本覆盖
// 三者分开记录（用户要求第 11 条）。重启后从 DoneThroughTs 续跑。
type FRTSumRecomputeState struct {
	ID uint `gorm:"primaryKey;autoIncrement:false" json:"-"`
	// TaskID and RequestedHours make an HTTP retry refer to the same durable
	// window. The old implementation recomputed from wall-clock now on every
	// POST, so a retry a few minutes later silently became a new task.
	TaskID             string `gorm:"column:task_id;size:96" json:"task_id"`
	RequestedHours     int    `gorm:"column:requested_hours" json:"requested_hours"`
	FromTs             int64  `gorm:"column:from_ts" json:"from_ts"`
	ToTs               int64  `gorm:"column:to_ts" json:"to_ts"`
	DoneThroughTs      int64  `gorm:"column:done_through_ts" json:"done_through_ts"`
	Version            int    `gorm:"column:version" json:"version"`
	StartedAt          int64  `gorm:"column:started_at" json:"started_at"`
	FinishedAt         int64  `gorm:"column:finished_at" json:"finished_at"`
	Status             string `gorm:"column:status" json:"status"`
	LastReason         string `gorm:"column:last_reason" json:"last_reason"`
	RowsUpdated        int64  `gorm:"column:rows_updated" json:"rows_updated"`
	RowsMissing        int64  `gorm:"column:rows_missing" json:"rows_missing"`
	RowsSampleMismatch int64  `gorm:"column:rows_sample_mismatch" json:"rows_sample_mismatch"`
	Slices             int    `gorm:"column:slices" json:"slices"`
	FailedSlices       int    `gorm:"column:failed_slices" json:"failed_slices"`
	LeaseOwner         string `gorm:"column:lease_owner;size:128" json:"-"`
	LeaseUntilTs       int64  `gorm:"column:lease_until_ts" json:"lease_until_ts"`
}

// FRTSumWindowStats 是单个窗口回算前后的实测计数，用于逐窗口对账留证。
type FRTSumWindowStats struct {
	Window            string `json:"window"`
	FromTs            int64  `json:"from_ts"`
	ToTs              int64  `json:"to_ts"`
	Rows              int64  `json:"rows"`
	RowsWithObserved  int64  `json:"rows_with_ttft_observed"`
	TtftObserved      int64  `json:"ttft_observed"`
	FRTSumMs          int64  `json:"frt_sum_ms"`
	FRTSumObserved    int64  `json:"frt_sum_observed"`
	RowsCurrentVer    int64  `json:"rows_frt_sum_version_current"`
	RowsZeroVer       int64  `json:"rows_frt_sum_version_zero"`
	ComputableChannel int64  `json:"computable_channels"`
	TotalChannel      int64  `json:"total_channels"`
}

// frtRecomputeWindowSQL 按与 sampleWindowUserSQL 相同的 FRT 判据重读日志。
// 维度与 capacity_user_minute_samples 主键一致：分钟、用户、渠道、分组、模型。
func frtRecomputeWindowSQL() string {
	q := `
SELECT /*+ MAX_EXECUTION_TIME(8000) */
  (created_at DIV 60)*60 AS bucket,
  user_id, channel_id, model_name, ` + "`group`" + ` AS grp,
  CAST(COALESCE(SUM(CASE WHEN type=2 AND COALESCE(is_stream,0)=1 AND FRT>0 THEN FRT END),0) AS SIGNED) AS frt_sum_ms,
  CAST(COALESCE(SUM(type=2 AND COALESCE(is_stream,0)=1 AND FRT>0),0) AS SIGNED) AS frt_sum_observed
FROM logs
WHERE created_at >= ? AND created_at < ? AND type = 2
  AND NOT (` + channelTestLogPredicateSQL() + `)
GROUP BY bucket, user_id, channel_id, model_name, grp
HAVING frt_sum_observed > 0`
	return strings.ReplaceAll(q, "FRT", frtSourceExpr)
}

type frtRecomputeRow struct {
	BucketTs       int64
	UserID         int64
	ChannelID      int
	ModelName      string
	Grp            string
	FRTSumMs       int64
	FRTSumObserved int64
}

// recomputeFRTSumSlice 回算一个时间切片。只更新已存在行的三列；
// 幂等靠主键定位 + 固定口径：同一窗口重复执行得到同一结果。
func (m *Monitor) recomputeFRTSumSlice(ctx context.Context, from, to int64, owner string) (updated, missing, sampleMismatch int64, err error) {
	// Bound source acquisition, query and local writes together. Renewing before
	// every attempt keeps the 20s slice + at most 25s backoff below the 120s lease.
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	release, err := m.acquireBackgroundSourceLow(qctx)
	if err != nil {
		return 0, 0, 0, err
	}
	defer release()

	rows, err := m.prodDB.QueryContext(qctx, frtRecomputeWindowSQL(), from, to)
	if err != nil {
		m.reportSourceQueryError(err)
		return 0, 0, 0, err
	}
	defer rows.Close()
	var batch []frtRecomputeRow
	for rows.Next() {
		var r frtRecomputeRow
		var grp sql.NullString
		if err := rows.Scan(&r.BucketTs, &r.UserID, &r.ChannelID, &r.ModelName, &grp, &r.FRTSumMs, &r.FRTSumObserved); err != nil {
			return 0, 0, 0, err
		}
		r.Grp = grp.String
		// 守住口径：总和必须为正、样本数必须为正，否则该行不可回算，
		// 保持 version=0 让页面显示"不可计算"，绝不写 0 冒充已知。
		if r.FRTSumMs <= 0 || r.FRTSumObserved <= 0 {
			continue
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		m.reportSourceQueryError(err)
		return 0, 0, 0, err
	}

	// Fence the writes as well as the checkpoint. The first UPDATE obtains
	// SQLite's write lock: a takeover or sampler write cannot interleave with
	// classification and the three-column update. A failed slice rolls back all
	// local changes, so retries cannot double count a partially written batch.
	err = m.storeDB.WithContext(qctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now().Unix()
		lease := tx.Model(&FRTSumRecomputeState{}).
			Where("id = ? AND lease_owner = ? AND lease_until_ts >= ?", 1, owner, now).
			Update("lease_until_ts", now+frtRecomputeLeaseSeconds)
		if lease.Error != nil {
			return lease.Error
		}
		if lease.RowsAffected != 1 {
			return errFRTRecomputeLeaseHeld
		}
		for _, r := range batch {
			result := tx.Model(&CapacityUserMinuteSample{}).
				Where("bucket_ts = ? AND user_id = ? AND channel_id = ? AND model_name = ? AND grp = ? AND ttft_observed = ?",
					r.BucketTs, r.UserID, r.ChannelID, r.ModelName, r.Grp, r.FRTSumObserved).
				UpdateColumns(map[string]any{
					"frt_sum_ms":       r.FRTSumMs,
					"frt_sum_observed": r.FRTSumObserved,
					"frt_sum_version":  frtSumSemanticsVersion,
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				updated += result.RowsAffected
				continue
			}
			// Zero affected rows is not necessarily a missing fact: distinguish
			// an absent key from a denominator mismatch without inserting rows.
			var target CapacityUserMinuteSample
			lookup := tx.Select("ttft_observed").Where("bucket_ts = ? AND user_id = ? AND channel_id = ? AND model_name = ? AND grp = ?",
				r.BucketTs, r.UserID, r.ChannelID, r.ModelName, r.Grp).Take(&target)
			if errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
				missing++
			} else if lookup.Error != nil {
				return lookup.Error
			} else if target.TtftObserved != r.FRTSumObserved {
				sampleMismatch++
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	return updated, missing, sampleMismatch, nil
}

func newFRTRecomputeOwner() string {
	return fmt.Sprintf("frt-%d-%d", time.Now().UnixNano(), frtRecomputeOwnerSeq.Add(1))
}

func newFRTRecomputeTaskID() string {
	return fmt.Sprintf("frt-task-%d-%d", time.Now().UnixNano(), frtRecomputeOwnerSeq.Add(1))
}

// claimFRTRecomputeLease is an atomic database compare-and-set. It is safe
// across Monitor processes sharing the same SQLite store (and remains valid
// when a process restarts); an expired owner can be taken over.
func (m *Monitor) claimFRTRecomputeLease(ctx context.Context, owner string, now int64) error {
	if owner == "" {
		return fmt.Errorf("FRT 回算租约 owner 为空")
	}
	var state FRTSumRecomputeState
	err := m.storeDB.WithContext(ctx).Where("id = ?", 1).First(&state).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if err := m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&FRTSumRecomputeState{ID: 1, Status: "idle"}).Error; err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	result := m.storeDB.WithContext(ctx).Model(&FRTSumRecomputeState{}).
		Where("id = ? AND (COALESCE(lease_until_ts, 0) < ? OR lease_owner = ?)", 1, now, owner).
		Updates(map[string]any{"lease_owner": owner, "lease_until_ts": now + frtRecomputeLeaseSeconds})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errFRTRecomputeLeaseHeld
	}
	return nil
}

func (m *Monitor) renewFRTRecomputeLease(ctx context.Context, owner string, now int64) error {
	result := m.storeDB.WithContext(ctx).Model(&FRTSumRecomputeState{}).
		Where("id = ? AND lease_owner = ? AND lease_until_ts >= ?", 1, owner, now).
		Update("lease_until_ts", now+frtRecomputeLeaseSeconds)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errFRTRecomputeLeaseHeld
	}
	return nil
}

func (m *Monitor) releaseFRTRecomputeLease(ctx context.Context, owner string) {
	if owner == "" || m == nil || m.storeDB == nil {
		return
	}
	_ = m.storeDB.WithContext(ctx).Model(&FRTSumRecomputeState{}).
		Where("id = ? AND lease_owner = ?", 1, owner).
		Updates(map[string]any{"lease_owner": "", "lease_until_ts": 0})
}

// RecomputeFRTSum 在 [from,to) 上按分钟切片回算，支持重复执行与重启续跑。
func (m *Monitor) RecomputeFRTSum(ctx context.Context, from, to int64) (*FRTSumRecomputeState, error) {
	return m.recomputeFRTSumWithOwner(ctx, from, to, newFRTRecomputeOwner())
}

func (m *Monitor) recomputeFRTSumWithOwner(ctx context.Context, from, to int64, owner string) (result *FRTSumRecomputeState, resultErr error) {
	if m == nil || m.storeDB == nil {
		return nil, fmt.Errorf("本地事实库不可用")
	}
	if m.prodDB == nil {
		return nil, fmt.Errorf("未配置生产库(只读)，无法回算 FRT 总和")
	}
	if to <= from {
		return nil, fmt.Errorf("回算窗口无效")
	}
	if err := m.claimFRTRecomputeLease(ctx, owner, time.Now().Unix()); err != nil {
		return nil, err
	}
	defer func() {
		// Cancellation ends source work, but must not cancel its durable status
		// update. Keep the owner/expiry predicate so an old worker cannot mark a
		// replacement worker failed, and bound cleanup even during store failure.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if result != nil && resultErr != nil {
			result.Status, result.LastReason = "failed", clip(resultErr.Error(), 512)
			result.FinishedAt = time.Now().Unix()
			if err := m.saveFRTSumRecomputeStateOwned(cleanupCtx, *result, owner); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("保存 FRT 回算终止状态: %w", err))
			}
		}
		m.releaseFRTRecomputeLease(cleanupCtx, owner)
	}()
	state := FRTSumRecomputeState{ID: 1}
	if err := m.storeDB.WithContext(ctx).Where("id = ?", 1).Limit(1).Find(&state).Error; err != nil {
		return nil, err
	}
	// 续跑判定只看左边界与版本，不要求右边界完全相同：
	// 右边界取"当前已定稿水位"，每次重试都会自然前移几分钟。若要求完全相同，
	// 一次长任务的每次重试都会被判成"新窗口"而从头重扫，永远跑不完
	// （2026-10-21 在 8204 真实数据上踩到：7 天任务四次重试，两次从 1% 重来）。
	// 右边界只允许前移，不允许回退，否则会漏掉已跳过的尾部区间。
	resume := state.FromTs == from && state.Version == frtSumSemanticsVersion &&
		state.ToTs <= to && state.DoneThroughTs >= from && state.DoneThroughTs <= to && state.Status != "complete"
	if resume {
		// 右边界前移后要覆盖记录，否则进度分母仍是旧值。
		state.ToTs = to
	} else {
		state = FRTSumRecomputeState{ID: 1, TaskID: newFRTRecomputeTaskID(), RequestedHours: int((to - from) / 3600), FromTs: from, ToTs: to, DoneThroughTs: from,
			Version: frtSumSemanticsVersion, RowsUpdated: 0, RowsMissing: 0, RowsSampleMismatch: 0, Slices: 0, FailedSlices: 0}
	}
	if state.TaskID == "" {
		state.TaskID = newFRTRecomputeTaskID()
	}
	if state.RequestedHours <= 0 {
		state.RequestedHours = int((state.ToTs - state.FromTs) / 3600)
	}
	state.LeaseOwner = owner
	state.LeaseUntilTs = time.Now().Unix() + frtRecomputeLeaseSeconds
	state.StartedAt, state.FinishedAt, state.Status, state.LastReason = time.Now().Unix(), 0, "running", ""
	if err := m.saveFRTSumRecomputeStateOwned(ctx, state, owner); err != nil {
		return &state, err
	}

	for cursor := state.DoneThroughTs; cursor < to; {
		if err := ctx.Err(); err != nil {
			return &state, err
		}
		next := min(cursor+frtRecomputeSliceSeconds, to)
		// 来源 lease 丢失是瞬时的（回算与常态采集共用同一条低优先来源泳道，
		// 采集抢到 lease 时回算会被拒）。有界退避重试，不让整个长任务停在这里；
		// 退避期间仍受 ctx 取消控制。耗尽次数才算真失败。
		var updated, missing, sampleMismatch int64
		var err error
		for attempt := 0; ; attempt++ {
			leaseNow := time.Now().Unix()
			if err = m.renewFRTRecomputeLease(ctx, owner, leaseNow); err != nil {
				break
			}
			state.LeaseUntilTs = leaseNow + frtRecomputeLeaseSeconds
			updated, missing, sampleMismatch, err = m.recomputeFRTSumSlice(ctx, cursor, next, owner)
			if err == nil || errors.Is(err, errFRTRecomputeLeaseHeld) || attempt >= frtRecomputeSliceRetries {
				break
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = ctxErr
				break
			}
			state.LastReason = clip(fmt.Sprintf("切片 %d 第 %d 次重试：%v", cursor, attempt+1, err), 512)
			if saveErr := m.saveFRTSumRecomputeStateOwned(ctx, state, owner); saveErr != nil {
				err = saveErr
				break
			}
			if !frtRecomputeRetryWait(ctx, time.Duration(attempt+1)*5*time.Second) {
				err = ctx.Err()
				if err == nil {
					err = context.Canceled
				}
				break
			}
		}
		state.Slices++
		if err != nil {
			// 失败切片不推进水位，重试从同一位置继续。
			state.FailedSlices++
			return &state, err
		}
		state.LastReason = ""
		state.RowsUpdated += updated
		state.RowsMissing += missing
		state.RowsSampleMismatch += sampleMismatch
		state.DoneThroughTs = next
		if err := m.saveFRTSumRecomputeStateOwned(ctx, state, owner); err != nil {
			return &state, err
		}
		cursor = next
	}
	state.Status, state.FinishedAt = "complete", time.Now().Unix()
	if err := m.saveFRTSumRecomputeStateOwned(ctx, state, owner); err != nil {
		return &state, err
	}
	return &state, nil
}

func (m *Monitor) saveFRTSumRecomputeState(ctx context.Context, state FRTSumRecomputeState) error {
	state.ID = 1
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}}, UpdateAll: true,
	}).Create(&state).Error
}

// saveFRTSumRecomputeStateOwned is the checkpoint path used by the active
// worker. The lease owner is part of the UPDATE predicate so a process whose
// lease expired cannot overwrite the checkpoint after another process takes
// over the task.
func (m *Monitor) saveFRTSumRecomputeStateOwned(ctx context.Context, state FRTSumRecomputeState, owner string) error {
	if owner == "" {
		return errFRTRecomputeLeaseHeld
	}
	state.ID = 1
	result := m.storeDB.WithContext(ctx).Model(&FRTSumRecomputeState{}).
		Where("id = ? AND lease_owner = ? AND lease_until_ts >= ?", 1, owner, time.Now().Unix()).
		Updates(map[string]any{
			"task_id":              state.TaskID,
			"requested_hours":      state.RequestedHours,
			"from_ts":              state.FromTs,
			"to_ts":                state.ToTs,
			"done_through_ts":      state.DoneThroughTs,
			"version":              state.Version,
			"started_at":           state.StartedAt,
			"finished_at":          state.FinishedAt,
			"status":               state.Status,
			"last_reason":          state.LastReason,
			"rows_updated":         state.RowsUpdated,
			"rows_missing":         state.RowsMissing,
			"rows_sample_mismatch": state.RowsSampleMismatch,
			"slices":               state.Slices,
			"failed_slices":        state.FailedSlices,
			"lease_owner":          owner,
			"lease_until_ts":       state.LeaseUntilTs,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errFRTRecomputeLeaseHeld
	}
	return nil
}

// FRTSumWindowReport 统计某窗口回算前后的实测计数（用户要求第 9 条）。
func (m *Monitor) FRTSumWindowReport(ctx context.Context, label string, from, to int64) (FRTSumWindowStats, error) {
	s := FRTSumWindowStats{Window: label, FromTs: from, ToTs: to}
	row := m.storeDB.WithContext(ctx).Model(&CapacityUserMinuteSample{}).
		Where("bucket_ts >= ? AND bucket_ts < ?", from, to).
		Select(`COUNT(*) AS rows,
			SUM(CASE WHEN ttft_observed > 0 THEN 1 ELSE 0 END) AS rows_with_observed,
			COALESCE(SUM(ttft_observed),0) AS ttft_observed,
			COALESCE(SUM(COALESCE(frt_sum_ms,0)),0) AS frt_sum_ms,
			COALESCE(SUM(frt_sum_observed),0) AS frt_sum_observed,
			SUM(CASE WHEN frt_sum_version = ` + strconv.Itoa(frtSumSemanticsVersion) + ` THEN 1 ELSE 0 END) AS rows_current_ver,
			SUM(CASE WHEN COALESCE(frt_sum_version,0) = 0 THEN 1 ELSE 0 END) AS rows_zero_ver`)
	var scanned struct {
		Rows             int64
		RowsWithObserved int64
		TtftObserved     int64
		FRTSumMs         int64
		FRTSumObserved   int64
		RowsCurrentVer   int64
		RowsZeroVer      int64
	}
	if err := row.Scan(&scanned).Error; err != nil {
		return s, err
	}
	s.Rows, s.RowsWithObserved, s.TtftObserved = scanned.Rows, scanned.RowsWithObserved, scanned.TtftObserved
	// Do not treat every non-null frt_sum_ms as a valid page value.  The FRT
	// page rejects old semantic versions, invalid totals and denominator
	// mismatches at the per-user leaf before it merges channels.  Reusing that
	// same tree here keeps the status/reconciliation numbers from claiming a
	// computable channel which the page renders as unavailable.
	s.FRTSumMs, s.FRTSumObserved = 0, 0
	s.RowsCurrentVer, s.RowsZeroVer = scanned.RowsCurrentVer, scanned.RowsZeroVer

	// 可计算渠道数必须与页面同源：走 modelFRTRows + modelFRTTree 的同一条路径，
	// 按 MeanMs 是否为 nil 计数。另写一条 SQL 判据会在口径变更时和页面显示脱节，
	// 那样这份对账数字就失去了证明力。
	frtRows, err := m.modelFRTRows(ctx, from, to, ModelFRTFilter{})
	if err != nil {
		return s, err
	}
	channels := modelFRTTree(frtRows, map[int]string{}, true)
	computable := make(map[int]bool, len(channels))
	for _, channel := range channels {
		s.TotalChannel++
		if channel.FRT.MeanMs != nil {
			s.ComputableChannel++
			computable[channel.ChannelID] = true
		}
	}
	for _, row := range frtRows {
		if !computable[row.Row.ChannelID] {
			continue
		}
		// A computable channel can only contain rows accepted by the same
		// aggregate validity checks used by modelFRTAggregate.view.
		if row.HistoricalRows > 0 || row.InvalidRows > 0 || row.MeanUnsupportedRows > 0 || row.MeanInvalidRows > 0 ||
			row.FRTSumObserved <= 0 || row.FRTSumMs <= 0 || row.FRTSumObserved != row.Row.TtftObserved {
			continue
		}
		if s.FRTSumMs > math.MaxInt64-row.FRTSumMs || s.FRTSumObserved > math.MaxInt64-row.FRTSumObserved {
			return s, fmt.Errorf("FRT 对账总和溢出")
		}
		s.FRTSumMs += row.FRTSumMs
		s.FRTSumObserved += row.FRTSumObserved
	}
	return s, nil
}

func (m *Monitor) serveFRTSumRecompute(c *gin.Context) {
	if m.storeDB == nil || m.prodDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "FRT 回算来源或本地事实库不可用"})
		return
	}
	hours, _ := strconv.Atoi(c.Query("hours"))
	if hours <= 0 || hours > 24*31 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "hours 需在 1 到 744 之间"})
		return
	}
	if !frtRecomputeRunning.CompareAndSwap(false, true) {
		c.JSON(http.StatusConflict, gin.H{"error": "已有 FRT 总和回算在进行"})
		return
	}
	// Strictly use the closed-minute target. A retry reuses the persisted task's
	// left edge; only the right edge may move forward.
	now := time.Now()
	end := metricFinalizeTarget(now.Unix())
	if m.cfg.CustomerHealthSourceEnabled {
		end = customerHealthSourceFinalizedThrough(now)
	}
	owner := newFRTRecomputeOwner()
	if err := m.claimFRTRecomputeLease(c.Request.Context(), owner, now.Unix()); err != nil {
		frtRecomputeRunning.Store(false)
		if errors.Is(err, errFRTRecomputeLeaseHeld) {
			c.JSON(http.StatusConflict, gin.H{"error": "已有 FRT 总和回算持有数据库租约"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "FRT 回算租约不可用"})
		}
		return
	}
	// Read the checkpoint only after acquiring the lease. Another instance may
	// have advanced it while this request was waiting to claim ownership.
	var previous FRTSumRecomputeState
	if err := m.storeDB.WithContext(c.Request.Context()).Where("id = ?", 1).Take(&previous).Error; err != nil {
		m.releaseFRTRecomputeLease(context.Background(), owner)
		frtRecomputeRunning.Store(false)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "FRT 回算状态不可读"})
		return
	}
	reuse := previous.TaskID != "" && previous.RequestedHours == hours && previous.Version == frtSumSemanticsVersion &&
		previous.Status != "complete" && previous.FromTs > 0 && previous.ToTs > previous.FromTs &&
		previous.DoneThroughTs >= previous.FromTs && previous.DoneThroughTs <= previous.ToTs
	from := end - int64(hours)*3600
	taskID := newFRTRecomputeTaskID()
	if reuse {
		from = previous.FromTs
		end = max(end, previous.ToTs)
		taskID = previous.TaskID
	}
	seed := previous
	if !reuse {
		seed = FRTSumRecomputeState{ID: 1, TaskID: taskID, RequestedHours: hours, FromTs: from, ToTs: end, DoneThroughTs: from, Version: frtSumSemanticsVersion, Status: "queued"}
	} else {
		seed.TaskID, seed.RequestedHours, seed.ToTs, seed.LeaseOwner, seed.LeaseUntilTs = taskID, hours, end, owner, now.Unix()+frtRecomputeLeaseSeconds
	}
	seed.LeaseOwner, seed.LeaseUntilTs = owner, now.Unix()+frtRecomputeLeaseSeconds
	seed.Status, seed.LastReason, seed.FinishedAt = "queued", "", 0
	if err := m.saveFRTSumRecomputeStateOwned(c.Request.Context(), seed, owner); err != nil {
		m.releaseFRTRecomputeLease(context.Background(), owner)
		frtRecomputeRunning.Store(false)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "FRT 回算状态不可写"})
		return
	}
	go func() {
		defer frtRecomputeRunning.Store(false)
		defer m.releaseFRTRecomputeLease(context.Background(), owner)
		ctx, cancel := context.WithTimeout(m.taskContext(), 6*time.Hour)
		defer cancel()
		if _, err := m.recomputeFRTSumWithOwner(ctx, from, end, owner); err != nil {
			return
		}
	}()
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "task_id": taskID, "reused": reuse, "from_ts": from, "to_ts": end, "hours": hours})
}

func (m *Monitor) serveFRTSumRecomputeStatus(c *gin.Context) {
	var state FRTSumRecomputeState
	if err := m.storeDB.WithContext(c.Request.Context()).Where("id = ?", 1).Limit(1).Find(&state).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "回算状态不可读"})
		return
	}
	if state.Status == "" {
		state.Status = "idle"
	}
	stats := []FRTSumWindowStats{}
	now := time.Now()
	end := metricFinalizeTarget(now.Unix())
	if m.cfg.CustomerHealthSourceEnabled {
		end = customerHealthSourceFinalizedThrough(now)
	}
	// 对账需要把本汇总与页面钉在同一个闭合窗口上：默认右边界取"当前已定稿水位"，
	// 它每分钟前移，两次请求之间就会错开几分钟，样本数对不上并非数据不一致。
	// 允许显式传 to（与页面同一个参数名和格式），但不得超过已定稿水位——
	// 否则会把未闭合的当前分钟算进来，那是虚假的"不一致"。
	if raw := strings.TrimSpace(c.Query("to")); raw != "" {
		at, err := modelReportUTCMinute(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if at > end {
			c.JSON(http.StatusBadRequest, gin.H{"error": "to 不能超过已定稿水位"})
			return
		}
		end = at
	}
	for _, w := range []struct {
		label string
		hours int64
	}{{"24h", 24}, {"3d", 72}, {"7d", 168}} {
		s, err := m.FRTSumWindowReport(c.Request.Context(), w.label, end-w.hours*3600, end)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "窗口统计不可读"})
			return
		}
		stats = append(stats, s)
	}
	c.JSON(http.StatusOK, gin.H{"job": state, "windows": stats})
}

// 表名显式固定，避免复数化规则变化影响既有状态行。
func (FRTSumRecomputeState) TableName() string { return "frt_sum_recompute_states" }
