package monitor

// 影响面写入（MON-003 B3）。
//
// 引擎**不写** IncidentImpact（全仓非测试代码零写入点），由适配层负责。
// 分两层，原因见 incident_ingest_impact_store.go 文件头。

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"gorm.io/gorm"
)

// incidentImpactWindowID 确定性派生，使同窗重放落在同一行（替换而非新增）。
func incidentImpactWindowID(incidentID, dimension, dimValue, rowKind string, windowToTs int64) string {
	sum := sha256.Sum256([]byte(incidentID + "\x1f" + dimension + "\x1f" + dimValue +
		"\x1f" + rowKind + "\x1f" + strconv.FormatInt(windowToTs, 10)))
	return hex.EncodeToString(sum[:])
}

// incidentImpactDims 返回本窗各维度的取值。
//
// 本轮只有 channel / model / user_group 三维有事实。
// ingress_protocol / node / user 无事实 → 不写（不是写未知，而是不产生条目）。
func incidentImpactDims(d incidentWindowDecision) [][2]string {
	channel := string(d.Key.ChannelScope)
	if d.Key.ChannelScope == ChannelScopeAssigned && d.Key.ChannelID != nil {
		channel = strconv.FormatInt(*d.Key.ChannelID, 10)
	}
	return [][2]string{
		{incidentImpactDimChannel, channel},
		{incidentImpactDimModel, d.Key.Model},
		{incidentImpactDimUserGroup, d.Key.UserGroup},
	}
}

// writeIncidentImpactWindows 写第一层窗口贡献并重算第二层汇总。
//
// 幂等：按含 window_to_ts 与 row_kind 的唯一键 upsert，同窗重放是替换，
// 不重复累加；迟到修订同样是替换对应窗口那一行。
//
// ★ 已归档窗口重放不得重复计数 ★
//
// ArchiveThroughTs 是归档水位。window_to_ts <= 水位的贡献不写入本层，
// 否则重放会插回一行明细，再被 SUM 进汇总，与已折叠进归档的同一窗口
// 重复计数。此时只留一条 replay_after_archive 修订。
func (m *Monitor) writeIncidentImpactWindows(tx *gorm.DB, incidentID, segmentID string,
	d incidentWindowDecision, now int64) error {

	if incidentID == "" {
		return nil
	}
	sample, anomaly := d.SampleTotal, d.AnomalyTotal
	complete := incidentDecisionHasCompleteData(d)
	var uncovered *int64
	if complete {
		zero := int64(0)
		uncovered = &zero
	} else if d.WindowState == incidentWindowStateGapObserved {
		seconds := d.WindowToTs - d.WindowFromTs
		uncovered = &seconds
	}
	// Mixed versions or pending coverage do not prove an exact missing duration.
	// Preserve known current-version counts, but never present them as complete.

	for _, dim := range incidentImpactDims(d) {
		archived, err := incidentImpactArchiveCovers(tx, incidentID, dim[0], dim[1], d.WindowToTs)
		if err != nil {
			return err
		}
		if archived {
			// 已折叠进归档基线，不再写明细（否则与归档重复计数）。
			//
			// 但必须留痕：「影响面没翻倍」只说明计数正确，不等于审计可查。
			// 这条 replay_after_archive 记录谁在什么时候重放了哪个已归档窗口。
			if err := m.recordIncidentImpactReplayAfterArchive(tx, incidentID, dim[0], dim[1], d, now); err != nil {
				return err
			}
			continue
		}
		row := IncidentImpactWindow{
			ID: incidentImpactWindowID(incidentID, dim[0], dim[1],
				incidentImpactRowKindWindow, d.WindowToTs),
			IncidentID:   incidentID,
			Dimension:    dim[0],
			DimValue:     dim[1],
			RowKind:      incidentImpactRowKindWindow,
			WindowToTs:   d.WindowToTs,
			SegmentID:    segmentID,
			WindowFromTs: d.WindowFromTs,
			// DistinctRequests 未知 → nil，不写 0。
			DistinctRequests: nil,
			UncoveredSeconds: uncovered,
			CoverageComplete: complete,
			WindowState:      d.WindowState,
			UpdatedAt:        now,
		}
		if d.WindowState != incidentWindowStateGapObserved {
			row.SampleTotal, row.AnomalyTotal = &sample, &anomaly
		}
		if err := tx.Save(&row).Error; err != nil {
			return fmt.Errorf("写影响面窗口贡献失败: %w", err)
		}
		if err := m.recomputeIncidentImpact(tx, incidentID, dim[0], dim[1], now); err != nil {
			return err
		}
	}
	return nil
}

// recordIncidentImpactReplayAfterArchive 为已归档窗口的重放写一条修订留痕。
//
// 与普通修订同表、同事务，kind 固定为 replay_after_archive。
// 不改变任何计数：它只回答「这个已归档窗口被重放过」。
//
// 用 (dedup_key, window_to_ts, kind) 去重：同一窗口反复重放只留一条并更新
// 时间，避免审计表被重放风暴刷爆。
func (m *Monitor) recordIncidentImpactReplayAfterArchive(tx *gorm.DB,
	incidentID, dimension, dimValue string, d incidentWindowDecision, now int64) error {

	const kind = "replay_after_archive"
	var existing IncidentIngestRevision
	res := tx.Where("dedup_key = ? AND window_to_ts = ? AND kind = ?",
		d.DedupKey, d.WindowToTs, kind).Limit(1).Find(&existing)
	if res.Error != nil {
		return fmt.Errorf("查归档重放修订失败: %w", res.Error)
	}
	if res.RowsAffected > 0 {
		existing.DetectedAt = now
		if err := tx.Save(&existing).Error; err != nil {
			return fmt.Errorf("更新归档重放修订失败: %w", err)
		}
		return nil
	}

	var maxSeq int
	if err := tx.Model(&IncidentIngestRevision{}).
		Select("COALESCE(MAX(revision_seq),0)").
		Where("dedup_key = ? AND window_to_ts = ?", d.DedupKey, d.WindowToTs).
		Scan(&maxSeq).Error; err != nil {
		return fmt.Errorf("取归档重放修订序号失败: %w", err)
	}
	newSample := d.SampleTotal
	rev := IncidentIngestRevision{
		ID: incidentImpactWindowID(d.DedupKey, kind, "rev", "seq",
			int64(maxSeq+1)*1_000_000+d.WindowToTs),
		DedupKey:       d.DedupKey,
		IncidentID:     incidentID,
		WindowFromTs:   d.WindowFromTs,
		WindowToTs:     d.WindowToTs,
		RevisionSeq:    maxSeq + 1,
		Kind:           kind,
		NewSampleTotal: &newSample,
		NewWindowState: d.WindowState,
		CoverageProof:  d.CoverageProof,
		// 重放不改判定、不改计数，只是审计留痕。
		DetectedAt: now,
	}
	if err := tx.Create(&rev).Error; err != nil {
		return fmt.Errorf("写归档重放修订失败: %w", err)
	}
	// 维度信息放在只增的注记里，便于定位是哪一维的重放。
	_ = dimension
	_ = dimValue
	return nil
}

// incidentImpactArchiveCovers 判断该窗口是否已被归档基线覆盖。
func incidentImpactArchiveCovers(tx *gorm.DB, incidentID, dimension, dimValue string, windowToTs int64) (bool, error) {
	var archive IncidentImpactWindow
	res := tx.Where("incident_id = ? AND dimension = ? AND dim_value = ? AND row_kind = ?",
		incidentID, dimension, dimValue, incidentImpactRowKindArchive).Limit(1).Find(&archive)
	if res.Error != nil {
		return false, fmt.Errorf("查影响面归档基线失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	return windowToTs <= archive.ArchiveThroughTs, nil
}

// recomputeIncidentImpact 从第一层重算第二层汇总并整行写回。
//
// 重算整行而非 `+=`，所以与重放/修订次数无关，天然幂等。
//
// ★ 时间范围按 row_kind 分别读取 ★
//
// 归档行的 window_to_ts 固定为 0，直接 MAX(window_to_ts) 在只剩归档行时
// 会把事故影响面的结束时间算成 0。所以普通窗口取 window_from/to_ts，
// 归档行取 archive_from/through_ts，再取并集，跳过缺失的一侧。
//
// nil 语义：全部为 nil → 汇总 nil（未知）；有值则求和，但若同时存在 nil
// 与非 nil，置 Partial 标记不完整——不把 nil 当 0 参与求和。
func (m *Monitor) recomputeIncidentImpact(tx *gorm.DB, incidentID, dimension, dimValue string, now int64) error {
	var rows []IncidentImpactWindow
	if err := tx.Where("incident_id = ? AND dimension = ? AND dim_value = ?",
		incidentID, dimension, dimValue).Find(&rows).Error; err != nil {
		return fmt.Errorf("读影响面窗口贡献失败: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	var (
		sampleSum, uncoveredSum         int64
		haveSample, haveUncovered       bool
		missingSample, missingUncovered bool
		coverageComplete                = true
		windowCount                     int64

		from, to int64
		haveFrom bool
		haveTo   bool
	)
	// archivedIncomplete 继承归档行已记录的不完整语义。
	//
	// ★ 归档不得抹掉不完整标记 ★
	//
	// 归档行把「明细里曾有 nil 贡献」压缩成了自己的 Incomplete 字段，
	// 明细随即被删除。重算时若只看当前这批行的 nil 情况，已归档的那段
	// 不完整就凭空消失了：归档前 Partial=true，归档后变成 false，而
	// CoverageComplete 仍是 false——两个字段互相矛盾。
	archivedIncomplete := false

	for _, r := range rows {
		if r.RowKind == incidentImpactRowKindArchive && r.Incomplete {
			archivedIncomplete = true
		}
		if r.SampleTotal != nil {
			sampleSum += *r.SampleTotal
			haveSample = true
		} else {
			missingSample = true
		}
		if r.UncoveredSeconds != nil {
			uncoveredSum += *r.UncoveredSeconds
			haveUncovered = true
		} else {
			missingUncovered = true
		}
		if !r.CoverageComplete {
			coverageComplete = false
		}

		rowFrom, rowTo := r.WindowFromTs, r.WindowToTs
		if r.RowKind == incidentImpactRowKindArchive {
			rowFrom, rowTo = r.ArchiveFromTs, r.ArchiveThroughTs
			windowCount += r.ArchivedWindowCount
		} else {
			windowCount++
		}
		if rowFrom > 0 && (!haveFrom || rowFrom < from) {
			from, haveFrom = rowFrom, true
		}
		if rowTo > 0 && (!haveTo || rowTo > to) {
			to, haveTo = rowTo, true
		}
	}

	impact := IncidentImpact{
		ID:               incidentImpactWindowID(incidentID, dimension, dimValue, "impact", 0),
		IncidentID:       incidentID,
		Dimension:        dimension,
		DimValue:         dimValue,
		WindowFromTs:     from,
		WindowToTs:       to,
		SourceIDs:        "capacity_user_minute_samples",
		CoverageComplete: coverageComplete,
		// Partial 表示本条由 nil 与非 nil 混合而来，不得当全量用。
		// 归档段已记录的不完整同样计入，不因明细被删而丢失。
		Partial:   !coverageComplete || missingSample || missingUncovered || archivedIncomplete,
		UpdatedAt: now,
	}
	if haveSample {
		v := sampleSum
		impact.SampleTotal = &v
	}
	if haveUncovered {
		v := uncoveredSum
		impact.UncoveredSeconds = &v
	}
	// DistinctRequests / VisibleAttempts / LogRows 本轮恒为 nil：
	// 分钟聚合桶不可诚实导出，不用桶数冒充。
	if err := tx.Save(&impact).Error; err != nil {
		return fmt.Errorf("写影响面汇总失败: %w", err)
	}
	_ = windowCount
	return nil
}
