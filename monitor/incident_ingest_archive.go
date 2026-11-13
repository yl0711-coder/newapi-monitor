package monitor

// 影响面归档与缺口摘要（MON-003 B3）。
//
// 两种归档，规则相似但目的不同：
//
//	archiveIncidentImpactWindows  折叠影响面窗口贡献 → 保住事故累计不缩水
//	archiveIncidentGapWindows     折叠已终结的不完整窗口明细 → 保住完整水位正确
//
// 两者都必须「先形成可参与后续计算的基线，再删明细」，且合并与删除同事务。

import (
	"fmt"
	"sort"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// archiveIncidentImpactWindows 把过期窗口贡献折叠成归档基线。
//
// ★ 为什么不能直接按时间删 ★
//
// 第二层汇总是「SUM 第一层」。原来 20+30=50，清掉前面的 20 再新增 10，
// 就会变成 40 而不是 60。所以必须先把待删贡献折叠成一行仍参与 SUM 的
// 归档基线，再删明细。
//
// ★ 存续事故的贡献不参与归档 ★
//
// 只有事故已终结（resolved）且冷却已过，其贡献才可折叠——活跃事故的
// 明细还要支撑详情展示与迟到修订。
func (m *Monitor) archiveIncidentImpactWindows(tx *gorm.DB, incidentID string,
	beforeTs int64, now int64) error {

	type group struct {
		Dimension string
		DimValue  string
	}
	var groups []group
	if err := tx.Model(&IncidentImpactWindow{}).
		Select("dimension, dim_value").
		Where("incident_id = ? AND row_kind = ? AND window_to_ts <= ?",
			incidentID, incidentImpactRowKindWindow, beforeTs).
		Group("dimension, dim_value").Find(&groups).Error; err != nil {
		return fmt.Errorf("查待归档影响面维度失败: %w", err)
	}

	for _, g := range groups {
		var rows []IncidentImpactWindow
		if err := tx.Where("incident_id = ? AND dimension = ? AND dim_value = ? AND row_kind = ? AND window_to_ts <= ?",
			incidentID, g.Dimension, g.DimValue, incidentImpactRowKindWindow, beforeTs).
			Find(&rows).Error; err != nil {
			return fmt.Errorf("读待归档影响面贡献失败: %w", err)
		}
		if len(rows) == 0 {
			continue
		}

		archiveID := incidentImpactWindowID(incidentID, g.Dimension, g.DimValue,
			incidentImpactRowKindArchive, 0)
		archive := IncidentImpactWindow{
			ID:         archiveID,
			IncidentID: incidentID,
			Dimension:  g.Dimension,
			DimValue:   g.DimValue,
			RowKind:    incidentImpactRowKindArchive,
			// 归档行的 window_to_ts 固定 0：它不代表某个窗口，
			// 因此与任何真实窗口都不会撞键，重复归档也不会产生第二行。
			WindowToTs: 0,
		}
		res := tx.Where("id = ?", archiveID).Limit(1).Find(&archive)
		if res.Error != nil {
			return fmt.Errorf("查影响面归档基线失败: %w", res.Error)
		}
		existing := res.RowsAffected > 0

		var (
			sampleSum, uncoveredSum   int64
			haveSample, haveUncovered bool
			missing                   bool
			count                     int64
			from, to                  int64
			haveFrom, haveTo          bool
			coverageComplete          = true
		)
		if existing {
			if archive.SampleTotal != nil {
				sampleSum, haveSample = *archive.SampleTotal, true
			}
			if archive.UncoveredSeconds != nil {
				uncoveredSum, haveUncovered = *archive.UncoveredSeconds, true
			}
			count = archive.ArchivedWindowCount
			if archive.ArchiveFromTs > 0 {
				from, haveFrom = archive.ArchiveFromTs, true
			}
			if archive.ArchiveThroughTs > 0 {
				to, haveTo = archive.ArchiveThroughTs, true
			}
			missing = archive.Incomplete
			coverageComplete = archive.CoverageComplete
		}

		for _, r := range rows {
			// 幂等判重：该窗口若已被现有归档区间**整体覆盖**，只删明细不再累加。
			// 判据必须是整窗覆盖，不能用 window_to_ts 是否落在半开区间内——
			// 窗口 [0,60) 与归档 [0,60) 完全相同时，右界 60 不在 [0,60) 内，
			// 会被误判成新窗口而重复计数。
			if existing && archive.ArchiveFromTs <= r.WindowFromTs &&
				r.WindowToTs <= archive.ArchiveThroughTs {
				continue
			}
			if r.SampleTotal != nil {
				sampleSum += *r.SampleTotal
				haveSample = true
			} else {
				missing = true
			}
			if r.UncoveredSeconds != nil {
				uncoveredSum += *r.UncoveredSeconds
				haveUncovered = true
			} else {
				missing = true
			}
			if !r.CoverageComplete {
				coverageComplete = false
			}
			count++
			if r.WindowFromTs > 0 && (!haveFrom || r.WindowFromTs < from) {
				from, haveFrom = r.WindowFromTs, true
			}
			if r.WindowToTs > 0 && (!haveTo || r.WindowToTs > to) {
				to, haveTo = r.WindowToTs, true
			}
		}

		archive.ID = archiveID
		archive.IncidentID = incidentID
		archive.Dimension = g.Dimension
		archive.DimValue = g.DimValue
		archive.RowKind = incidentImpactRowKindArchive
		archive.WindowToTs = 0
		archive.ArchiveFromTs = from
		archive.ArchiveThroughTs = to
		archive.ArchivedWindowCount = count
		archive.CoverageComplete = coverageComplete
		archive.Incomplete = missing
		archive.UpdatedAt = now
		archive.SampleTotal, archive.UncoveredSeconds = nil, nil
		if haveSample {
			v := sampleSum
			archive.SampleTotal = &v
		}
		if haveUncovered {
			v := uncoveredSum
			archive.UncoveredSeconds = &v
		}
		if err := tx.Save(&archive).Error; err != nil {
			return fmt.Errorf("写影响面归档基线失败: %w", err)
		}
		// 同事务删除明细：不存在「已合并未删除」或「已删除未合并」。
		if err := tx.Where("incident_id = ? AND dimension = ? AND dim_value = ? AND row_kind = ? AND window_to_ts <= ?",
			incidentID, g.Dimension, g.DimValue, incidentImpactRowKindWindow, beforeTs).
			Delete(&IncidentImpactWindow{}).Error; err != nil {
			return fmt.Errorf("删除已归档影响面贡献失败: %w", err)
		}
		if err := m.recomputeIncidentImpact(tx, incidentID, g.Dimension, g.DimValue, now); err != nil {
			return err
		}
	}
	return nil
}

// incidentGapArchivable 是归档前置条件。
//
// ★ 单凭 data_complete=false + 超保留期就删会误删仍需处理的任务 ★
//
// 等待来源、待重试、崩溃恢复中、以及仍在 7 天补齐期内的窗口都可能
// data_complete=false，但它们还要继续处理。必须全部满足：
//
//	流程已终结（done / failed_permanent / unattributable）
//	且 复核已关闭（done 的缺口窗必须已标 gap/mixed closed）
//	且 已过 7 天补齐期
//	且 已过保留期
func incidentGapArchivable(row IncidentIngestWindowState, retentionDays int, now int64) bool {
	switch row.ProcessState {
	case incidentProcessDone, incidentProcessFailedPermanent, incidentProcessUnattributable:
	default:
		return false
	}
	if row.DataComplete {
		return false
	}
	if row.ProcessState == incidentProcessDone &&
		!row.GapRecheckClosed && !row.MixedVersionClosed {
		// 缺口仍在复核期内，可能被补采消退，不能归档。
		return false
	}
	if incidentGapRecheckDeadline(row.WindowToTs) >= now {
		return false
	}
	if retentionDays > 0 && row.WindowToTs >= now-int64(retentionDays)*86400 {
		return false
	}
	return true
}

// archiveIncidentGapWindows 把满足条件的不完整窗口折叠进缺口摘要并删明细。
//
// ★ 只合并连续区间 ★
//
// 绝不跨过尚未归档的窗口：若外层区间把那个窗口也囊括进来，后来它满足
// 归档条件时会被判为「已覆盖」而跳过，造成漏计。所以先按起点排序切成
// 连续段（窗口已对齐到 IncidentWindowSeconds 格栅，连续性可精确判定），
// 每段写一条独立摘要行。
func (m *Monitor) archiveIncidentGapWindows(tx *gorm.DB, retentionDays int, now int64) (int, error) {
	var rows []IncidentIngestWindowState
	if err := tx.Where("data_complete = ?", false).
		Order("dedup_key, window_to_ts").Find(&rows).Error; err != nil {
		return 0, fmt.Errorf("读待归档缺口窗口失败: %w", err)
	}

	byKey := map[string][]IncidentIngestWindowState{}
	for _, r := range rows {
		if !incidentGapArchivable(r, retentionDays, now) {
			continue
		}
		byKey[r.DedupKey] = append(byKey[r.DedupKey], r)
	}

	archived := 0
	for dedupKey, group := range byKey {
		sort.Slice(group, func(i, j int) bool {
			return group[i].WindowFromTs < group[j].WindowFromTs
		})
		// 切分连续段。
		segments := [][]IncidentIngestWindowState{}
		cur := []IncidentIngestWindowState{group[0]}
		for _, r := range group[1:] {
			prev := cur[len(cur)-1]
			if r.WindowFromTs == prev.WindowToTs {
				cur = append(cur, r)
				continue
			}
			segments = append(segments, cur)
			cur = []IncidentIngestWindowState{r}
		}
		segments = append(segments, cur)

		for _, seg := range segments {
			n, err := m.mergeIncidentGapSummary(tx, dedupKey, seg, now)
			if err != nil {
				return archived, err
			}
			archived += n
		}
	}
	return archived, nil
}

// mergeIncidentGapSummary 把一个连续段合并进摘要并删除其明细（同事务）。
func (m *Monitor) mergeIncidentGapSummary(tx *gorm.DB, dedupKey string,
	seg []IncidentIngestWindowState, now int64) (int, error) {

	if len(seg) == 0 {
		return 0, nil
	}
	segFrom, segTo := seg[0].WindowFromTs, seg[len(seg)-1].WindowToTs

	// 找区间紧邻或重叠的已有摘要行；否则新建。只有紧邻/重叠才合并，
	// 不跨空洞。
	var existing []IncidentIngestGapSummary
	if err := tx.Where("dedup_key = ? AND gap_from_ts <= ? AND gap_through_ts >= ?",
		dedupKey, segTo, segFrom).Order("gap_from_ts").Find(&existing).Error; err != nil {
		return 0, fmt.Errorf("查缺口摘要失败: %w", err)
	}

	summary := IncidentIngestGapSummary{
		DedupKey: dedupKey, GapFromTs: segFrom, GapThroughTs: segTo,
		FirstDetectedAt: seg[0].CreatedAt, ArchivedAt: now,
	}
	covered := func(r IncidentIngestWindowState) bool { return false }
	if len(existing) > 0 {
		base := existing[0]
		summary = base
		for _, e := range existing[1:] {
			// 多条紧邻摘要一并并成一条，旧行随后删除。
			summary.WindowCount += e.WindowCount
			summary.GapObservedCount += e.GapObservedCount
			summary.FailedPermanentCount += e.FailedPermanentCount
			summary.UnattributableCount += e.UnattributableCount
			if e.GapFromTs < summary.GapFromTs {
				summary.GapFromTs = e.GapFromTs
			}
			if e.GapThroughTs > summary.GapThroughTs {
				summary.GapThroughTs = e.GapThroughTs
			}
			if e.FirstDetectedAt > 0 && (summary.FirstDetectedAt == 0 || e.FirstDetectedAt < summary.FirstDetectedAt) {
				summary.FirstDetectedAt = e.FirstDetectedAt
			}
		}
		// 判重按**整窗是否已被某一条原始摘要覆盖**。
		//
		// 两点都关键：
		//  1. 必须是整窗覆盖（gap_from <= window_from && window_to <= gap_through），
		//     不能用「右界落在半开区间内」——窗口 [0,60) 与摘要 [0,60) 完全相同时
		//     右界 60 不在 [0,60) 内，会被误判成新窗口而重复计数。
		//  2. 必须逐条比对**合并前**的原始摘要，不能用合并后的外层区间。
		//     合并 [0,300) 与 [600,900) 得到的外层 [0,900) 会把尚未归档的
		//     [300,600) 也囊括进来，于是它真的被归档时反而被跳过（漏计）。
		originals := make([][2]int64, 0, len(existing))
		for _, e := range existing {
			originals = append(originals, [2]int64{e.GapFromTs, e.GapThroughTs})
		}
		covered = func(r IncidentIngestWindowState) bool {
			for _, o := range originals {
				if o[0] <= r.WindowFromTs && r.WindowToTs <= o[1] {
					return true
				}
			}
			return false
		}
		if segFrom < summary.GapFromTs {
			summary.GapFromTs = segFrom
		}
		if segTo > summary.GapThroughTs {
			summary.GapThroughTs = segTo
		}
		summary.ArchivedAt = now
	}

	deleted := 0
	for _, r := range seg {
		if !covered(r) {
			summary.WindowCount++
			switch r.ProcessState {
			case incidentProcessFailedPermanent:
				summary.FailedPermanentCount++
			case incidentProcessUnattributable:
				summary.UnattributableCount++
			default:
				summary.GapObservedCount++
			}
			if r.CreatedAt > 0 && (summary.FirstDetectedAt == 0 || r.CreatedAt < summary.FirstDetectedAt) {
				summary.FirstDetectedAt = r.CreatedAt
			}
		}
		if err := tx.Where("dedup_key = ? AND window_to_ts = ?", r.DedupKey, r.WindowToTs).
			Delete(&IncidentIngestWindowState{}).Error; err != nil {
			return deleted, fmt.Errorf("删除已归档缺口窗口失败: %w", err)
		}
		deleted++
	}

	// 删掉被并掉的旧摘要行（主键可能变了），再写入合并结果。
	for _, e := range existing {
		if e.GapFromTs == summary.GapFromTs {
			continue
		}
		if err := tx.Where("dedup_key = ? AND gap_from_ts = ?", e.DedupKey, e.GapFromTs).
			Delete(&IncidentIngestGapSummary{}).Error; err != nil {
			return deleted, fmt.Errorf("合并缺口摘要失败: %w", err)
		}
	}
	// 用显式 upsert 而不是 Save：复合主键里 gap_from_ts 可以是 0
	// （窗口起点为 0 的情形），GORM 会把零值主键当成新记录而走 INSERT，
	// 于是重复归档会撞唯一约束而不是更新。
	if err := tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "dedup_key"}, {Name: "gap_from_ts"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"gap_through_ts", "window_count", "gap_observed_count",
			"failed_permanent_count", "unattributable_count",
			"first_detected_at", "archived_at",
		}),
	}).Create(&summary).Error; err != nil {
		return deleted, fmt.Errorf("写缺口摘要失败: %w", err)
	}
	return deleted, nil
}
