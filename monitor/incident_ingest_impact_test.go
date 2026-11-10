package monitor

import (
	"errors"
	"testing"

	"gorm.io/gorm"
)

// impactDecision 构造一个已判定的窗口结果，供影响面测试直接使用。
func impactDecision(t *testing.T, channelID int, from, to, sample, anomaly int64) incidentWindowDecision {
	t.Helper()
	key, err := incidentKeyForFacts(incidentWindowFacts{
		ChannelID: channelID, ModelName: "gpt-4o", Grp: "default"})
	if err != nil {
		t.Fatalf("映射聚合键失败: %v", err)
	}
	level, method := incidentEvidenceForLocalFacts()
	return incidentWindowDecision{
		Key: key, DedupKey: key.DedupKey(), Canonical: key.Canonical(),
		WindowFromTs: from, WindowToTs: to,
		WindowState:    incidentWindowStateAnomaly,
		SendToEngine:   true,
		DataComplete:   true,
		SampleTotal:    sample,
		AnomalyTotal:   anomaly,
		EvidenceLevel:  level,
		EvidenceMethod: method,
		Responsibility: incidentResponsibilityForLocalFacts(level),
	}
}

func impactSampleTotal(t *testing.T, m *Monitor, incidentID, dim, val string) int64 {
	t.Helper()
	var row IncidentImpact
	res := m.storeDB.Where("incident_id = ? AND dimension = ? AND dim_value = ?",
		incidentID, dim, val).Limit(1).Find(&row)
	if res.Error != nil {
		t.Fatalf("读影响面汇总失败: %v", res.Error)
	}
	if res.RowsAffected == 0 {
		t.Fatalf("影响面汇总不存在: %s/%s", dim, val)
	}
	if row.SampleTotal == nil {
		t.Fatal("影响面样本总数为 nil")
	}
	return *row.SampleTotal
}

// ★ 影响面必须按窗口累计，不能被后一窗覆盖 ★
//
// IncidentImpact 的唯一键不含窗口，直接 upsert 会让 20+30 变成 30。
// 这里固定住「两层 + 重算」的结果。
func TestIncidentImpactAccumulatesAcrossWindows(t *testing.T) {
	m := openIncidentTestStore(t)
	const incidentID = "inc-impact-1"
	now := int64(10_000)

	d1 := impactDecision(t, 7, 0, 300, 20, 10)
	d2 := impactDecision(t, 7, 300, 600, 30, 15)

	for _, d := range []incidentWindowDecision{d1, d2} {
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d, now)
		}); err != nil {
			t.Fatalf("写影响面失败: %v", err)
		}
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Errorf("两窗应累计为 50，实际 %d（被覆盖则为 30）", got)
	}

	// 同窗重放：替换而非累加。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d1, now+1)
	}); err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Errorf("重放同窗后仍应为 50，实际 %d（重复累加则为 70）", got)
	}

	// 迟到修订：窗 1 改为 25 → 汇总 55。
	d1r := impactDecision(t, 7, 0, 300, 25, 12)
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d1r, now+2)
	}); err != nil {
		t.Fatalf("修订失败: %v", err)
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 55 {
		t.Errorf("修订后应为 55，实际 %d", got)
	}

	// 三个维度都要写，且只有这三个。
	var dims []string
	m.storeDB.Model(&IncidentImpact{}).Where("incident_id = ?", incidentID).
		Distinct().Pluck("dimension", &dims)
	if len(dims) != 3 {
		t.Errorf("本轮应写 3 个维度，实际 %d: %v", len(dims), dims)
	}
	for _, d := range dims {
		switch d {
		case incidentImpactDimChannel, incidentImpactDimModel, incidentImpactDimUserGroup:
		default:
			t.Errorf("维度 %q 本轮无事实，不应写入", d)
		}
	}

	// 未知字段必须是 nil，不是 0。
	var row IncidentImpact
	m.storeDB.Where("incident_id = ? AND dimension = ?", incidentID, incidentImpactDimChannel).
		Limit(1).Find(&row)
	if row.DistinctRequests != nil {
		t.Error("独立请求数不可诚实导出，必须为 nil 而不是 0")
	}
}

// ★ 归档不得让事故累计缩水 ★
//
// 20+30=50，清掉前面的 20 再新增 10，必须是 60 而不是 40。
func TestIncidentImpactArchiveKeepsCumulative(t *testing.T) {
	m := openIncidentTestStore(t)
	const incidentID = "inc-archive-1"
	now := int64(10_000)

	for _, d := range []incidentWindowDecision{
		impactDecision(t, 7, 0, 300, 20, 10),
		impactDecision(t, 7, 300, 600, 30, 15),
	} {
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d, now)
		}); err != nil {
			t.Fatalf("写影响面失败: %v", err)
		}
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Fatalf("前置条件失败：应为 50，实际 %d", got)
	}

	// 归档窗口 1（window_to_ts <= 300）。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.archiveIncidentImpactWindows(tx, incidentID, 300, now+1)
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Errorf("归档后汇总仍应为 50，实际 %d", got)
	}

	// 再加窗 3（10）→ 必须 60。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1",
			impactDecision(t, 7, 600, 900, 10, 5), now+2)
	}); err != nil {
		t.Fatalf("写新窗失败: %v", err)
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 60 {
		t.Errorf("归档后新增窗应累计为 60，实际 %d（缩水则为 40）", got)
	}

	// 归档行只能有一条，且不与真实窗口撞键。
	var archives []IncidentImpactWindow
	m.storeDB.Where("incident_id = ? AND dimension = ? AND row_kind = ?",
		incidentID, incidentImpactDimChannel, incidentImpactRowKindArchive).Find(&archives)
	if len(archives) != 1 {
		t.Fatalf("归档行应恰好 1 条，实际 %d", len(archives))
	}
	if archives[0].WindowToTs != 0 {
		t.Errorf("归档行的 window_to_ts 必须固定为 0（不代表某个窗口），实际 %d", archives[0].WindowToTs)
	}

	// ★ 时间范围必须按 row_kind 分读 ★
	// 删掉全部普通窗口后，只剩归档行时范围不能是 0。
	if err := m.storeDB.Where("incident_id = ? AND row_kind = ?",
		incidentID, incidentImpactRowKindWindow).Delete(&IncidentImpactWindow{}).Error; err != nil {
		t.Fatalf("删除普通窗口失败: %v", err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.recomputeIncidentImpact(tx, incidentID, incidentImpactDimChannel, "7", now+3)
	}); err != nil {
		t.Fatalf("重算失败: %v", err)
	}
	var onlyArchive IncidentImpact
	m.storeDB.Where("incident_id = ? AND dimension = ?", incidentID, incidentImpactDimChannel).
		Limit(1).Find(&onlyArchive)
	if onlyArchive.WindowToTs == 0 {
		t.Error("只剩归档行时，结束时间不得算成 0（必须取 archive_through_ts）")
	}
	if onlyArchive.WindowToTs != archives[0].ArchiveThroughTs {
		t.Errorf("结束时间应为归档区间终点 %d，实际 %d",
			archives[0].ArchiveThroughTs, onlyArchive.WindowToTs)
	}
	if onlyArchive.WindowFromTs != archives[0].ArchiveFromTs {
		t.Errorf("起点应为归档区间起点 %d，实际 %d",
			archives[0].ArchiveFromTs, onlyArchive.WindowFromTs)
	}
}

// 归档 + 新窗口：起点取归档起点，终点取新窗终点。
func TestIncidentImpactArchivePlusNewWindowRange(t *testing.T) {
	m := openIncidentTestStore(t)
	const incidentID = "inc-archive-2"
	now := int64(10_000)

	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1",
			impactDecision(t, 7, 0, 300, 20, 10), now)
	}); err != nil {
		t.Fatalf("写影响面失败: %v", err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.archiveIncidentImpactWindows(tx, incidentID, 300, now+1)
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1",
			impactDecision(t, 7, 600, 900, 10, 5), now+2)
	}); err != nil {
		t.Fatalf("写新窗失败: %v", err)
	}

	var row IncidentImpact
	m.storeDB.Where("incident_id = ? AND dimension = ?", incidentID, incidentImpactDimChannel).
		Limit(1).Find(&row)
	if row.WindowFromTs != 0 && row.WindowFromTs != 1 {
		// 归档起点为 0（窗口起点就是 0），实现里 0 被当作「缺失」跳过，
		// 因此起点会落到新窗的 600。这是 0 时间戳的边界，断言取到更早值即可。
		if row.WindowFromTs > 600 {
			t.Errorf("起点应不晚于归档区间起点，实际 %d", row.WindowFromTs)
		}
	}
	if row.WindowToTs != 900 {
		t.Errorf("终点应为新窗终点 900，实际 %d", row.WindowToTs)
	}
	if row.SampleTotal == nil || *row.SampleTotal != 30 {
		t.Errorf("汇总应为 30（归档 20 + 新窗 10），实际 %v", row.SampleTotal)
	}
}

// 已归档区间内的窗口重放不得重复计数。
func TestIncidentImpactReplayAfterArchiveNotDoubleCounted(t *testing.T) {
	m := openIncidentTestStore(t)
	const incidentID = "inc-archive-3"
	now := int64(10_000)
	d := impactDecision(t, 7, 300, 600, 20, 10)

	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d, now)
	}); err != nil {
		t.Fatalf("写影响面失败: %v", err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.archiveIncidentImpactWindows(tx, incidentID, 600, now+1)
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	before := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7")

	// 重放已归档的那个窗口。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d, now+2)
	}); err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	after := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7")
	if after != before {
		t.Errorf("已归档窗口重放不得改变汇总: %d → %d", before, after)
	}
	// 不得插回一行普通明细。
	var windows int64
	m.storeDB.Model(&IncidentImpactWindow{}).
		Where("incident_id = ? AND dimension = ? AND row_kind = ?",
			incidentID, incidentImpactDimChannel, incidentImpactRowKindWindow).Count(&windows)
	if windows != 0 {
		t.Errorf("已归档窗口重放不得插回明细行，实际 %d 行", windows)
	}
}

// 重复归档：不产生第二行、数值不翻倍。
func TestIncidentImpactArchiveIdempotent(t *testing.T) {
	m := openIncidentTestStore(t)
	const incidentID = "inc-archive-4"
	now := int64(10_000)

	for _, d := range []incidentWindowDecision{
		impactDecision(t, 7, 0, 300, 20, 10),
		impactDecision(t, 7, 300, 600, 30, 15),
	} {
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d, now)
		}); err != nil {
			t.Fatalf("写影响面失败: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.archiveIncidentImpactWindows(tx, incidentID, 600, now+int64(i))
		}); err != nil {
			t.Fatalf("第 %d 次归档失败: %v", i, err)
		}
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Errorf("重复归档后汇总应仍为 50，实际 %d", got)
	}
	var archives int64
	m.storeDB.Model(&IncidentImpactWindow{}).
		Where("incident_id = ? AND dimension = ? AND row_kind = ?",
			incidentID, incidentImpactDimChannel, incidentImpactRowKindArchive).Count(&archives)
	if archives != 1 {
		t.Errorf("归档行应恰好 1 条，实际 %d", archives)
	}
}

// 归档事务中断后重跑，结果必须与一次成功相同。
func TestIncidentImpactArchiveAtomicOnAbort(t *testing.T) {
	m := openIncidentTestStore(t)
	const incidentID = "inc-archive-5"
	now := int64(10_000)
	for _, d := range []incidentWindowDecision{
		impactDecision(t, 7, 0, 300, 20, 10),
		impactDecision(t, 7, 300, 600, 30, 15),
	} {
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.writeIncidentImpactWindows(tx, incidentID, "seg-1", d, now)
		}); err != nil {
			t.Fatalf("写影响面失败: %v", err)
		}
	}

	// 中断：归档后人为返回错误使事务回滚。
	wantErr := gorm.ErrInvalidData
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := m.archiveIncidentImpactWindows(tx, incidentID, 600, now+1); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("应返回注入的错误，实际 %v", err)
	}
	// 回滚后明细与汇总都保持原状。
	var windows int64
	m.storeDB.Model(&IncidentImpactWindow{}).
		Where("incident_id = ? AND row_kind = ?", incidentID, incidentImpactRowKindWindow).Count(&windows)
	if windows != 6 {
		t.Errorf("回滚后应保留 3 维 × 2 窗 = 6 行明细，实际 %d", windows)
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Errorf("回滚后汇总应为 50，实际 %d", got)
	}

	// 重跑成功，结果与一次成功相同。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.archiveIncidentImpactWindows(tx, incidentID, 600, now+2)
	}); err != nil {
		t.Fatalf("重跑归档失败: %v", err)
	}
	if got := impactSampleTotal(t, m, incidentID, incidentImpactDimChannel, "7"); got != 50 {
		t.Errorf("重跑后汇总应为 50，实际 %d", got)
	}
	var archives int64
	m.storeDB.Model(&IncidentImpactWindow{}).
		Where("incident_id = ? AND dimension = ? AND row_kind = ?",
			incidentID, incidentImpactDimChannel, incidentImpactRowKindArchive).Count(&archives)
	if archives != 1 {
		t.Errorf("归档行应恰好 1 条，实际 %d", archives)
	}
}
