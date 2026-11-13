package monitor

// B1 持久化测试：真实 SQLite（t.TempDir 隔离库），不连生产库。
//
// 覆盖：建表与索引、复发三态、并发去重、重启续接、plan ID bump、开关关闭时的迁移行为。

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// readSourceForPlanCheck 读源码文本。plan ID 与模型集都是字面量，
// 只能按源码串校验，反射拿不到。
func readSourceForPlanCheck(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("读 %s: %v", name, err)
	}
	return string(b)
}

func countOccurrences(s, sub string) int { return strings.Count(s, sub) }

// openIncidentTestStore 开一个隔离的真实 SQLite 库，走完整 openStore（含 AutoMigrate）。
func openIncidentTestStore(t *testing.T) *Monitor {
	t.Helper()
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	cleanupLocalTestStore(t, m)
	return m
}

// 七张 incident 表必须真的建出来，且带上关键唯一索引。
// 这是「新表未进 AutoMigrate，生产上报表不存在」那类事故的防线。
func TestIncidentTablesCreated(t *testing.T) {
	m := openIncidentTestStore(t)
	for _, tbl := range []string{
		"incidents", "incident_segments", "incident_continuity",
		"incident_observations", "incident_impacts", "incident_states", "incident_audits",
	} {
		if !m.storeDB.Migrator().HasTable(tbl) {
			t.Errorf("表 %s 未建出来：检查 store.go 的 AutoMigrate 模型集", tbl)
		}
	}
	// 活跃去重索引必须存在，否则并发建单无法靠数据库拦截。
	for _, idx := range []struct{ table, name string }{
		{"incidents", "idx_incident_active_dedup"},
		{"incident_segments", "idx_incident_segment_seq"},
		{"incident_impacts", "idx_incident_impact_dim"},
		{"incident_audits", "idx_incident_audit_idem"},
	} {
		if !m.storeDB.Migrator().HasIndex(idx.table, idx.name) {
			t.Errorf("索引 %s.%s 缺失", idx.table, idx.name)
		}
	}
}

// 同一聚合键下只能有一个活跃事故：第二条 INSERT 必须被数据库拒绝。
// 这条是「重复扫描/并发不重复建单」的底层保证。
func TestIncidentActiveUniquePerDedupKey(t *testing.T) {
	m := openIncidentTestStore(t)
	mk := func(id string) *Incident {
		return &Incident{ID: id, DedupKey: "k1", ActiveMarker: incidentActiveMarker,
			Status: "open", FirstSeenTs: 100, LastSeenTs: 100, Revision: 1}
	}
	if err := m.storeDB.Create(mk("inc-1")).Error; err != nil {
		t.Fatalf("第一条活跃事故应能插入: %v", err)
	}
	if err := m.storeDB.Create(mk("inc-2")).Error; err == nil {
		t.Fatal("同一聚合键的第二条活跃事故必须被唯一索引拒绝，否则会重复建单")
	}
}

// 存储层对复发场景提供的约束——**不是**复发逻辑的验收。
//
// 只有「持续未结束」这一态能由索引单独保证（活跃槽位被占）。
// 另两态必须读 status 与 cooldown_until_ts 才能分辨，是 B2 的职责；
// 这里只验证「存储结构允许正确实现」，并显式钉住索引管不到的边界。
func TestIncidentStorageConstraintsForRecurrence(t *testing.T) {
	m := openIncidentTestStore(t)

	// 态一：持续未结束。冷却到期也不新建——活跃槽位被占着。
	// 这一条是真正由索引保证的。
	t.Run("持续未结束_冷却到期也不新建", func(t *testing.T) {
		live := &Incident{ID: "live-1", DedupKey: "kA", ActiveMarker: incidentActiveMarker,
			Status: "open", FirstSeenTs: 100, LastSeenTs: 200,
			CooldownUntilTs: 150, Revision: 1} // 冷却早已到期
		if err := m.storeDB.Create(live).Error; err != nil {
			t.Fatalf("建活跃事故: %v", err)
		}
		dup := &Incident{ID: "live-2", DedupKey: "kA", ActiveMarker: incidentActiveMarker,
			Status: "open", FirstSeenTs: 300, LastSeenTs: 300, Revision: 1}
		if err := m.storeDB.Create(dup).Error; err == nil {
			t.Error("事故仍未结束时，冷却到期不得成为新建事故的理由")
		}
	})

	// 态三：已结束且冷却到期 → 可以新建，且原事故历史必须保留。
	t.Run("已结束且冷却到期_新建且保留历史", func(t *testing.T) {
		old := &Incident{ID: "old-1", DedupKey: "kB", ActiveMarker: incidentActiveMarker,
			Status: "open", FirstSeenTs: 100, LastSeenTs: 200, Revision: 1}
		if err := m.storeDB.Create(old).Error; err != nil {
			t.Fatalf("建事故: %v", err)
		}
		// 结束：让出活跃槽位（marker 改成自身派生值）。
		if err := m.storeDB.Model(&Incident{}).Where("id = ?", "old-1").
			Updates(map[string]any{"status": "resolved", "resolved_ts": 250,
				"cooldown_until_ts": 300, "active_marker": int64(9001)}).Error; err != nil {
			t.Fatalf("结束事故: %v", err)
		}
		// 冷却到期后复发 → 新建成功。
		fresh := &Incident{ID: "new-1", DedupKey: "kB", ActiveMarker: incidentActiveMarker,
			Status: "open", FirstSeenTs: 400, LastSeenTs: 400, Revision: 1}
		if err := m.storeDB.Create(fresh).Error; err != nil {
			t.Fatalf("已结束且冷却到期后复发应能新建事故: %v", err)
		}
		// 原事故必须还在，没被覆盖。
		var kept Incident
		if err := m.storeDB.Where("id = ?", "old-1").First(&kept).Error; err != nil {
			t.Fatalf("原事故历史必须保留: %v", err)
		}
		if kept.Status != "resolved" || kept.ResolvedTs != 250 {
			t.Errorf("原事故被改写了: status=%s resolved_ts=%d", kept.Status, kept.ResolvedTs)
		}
		var total int64
		m.storeDB.Model(&Incident{}).Where("dedup_key = ?", "kB").Count(&total)
		if total != 2 {
			t.Errorf("同一聚合键应能并存 1 活跃 + 1 历史，实际 %d 条", total)
		}
	})

	// 态二的**存储能力**：同一事故能挂多个段，供 B2 复用原事故时追加。
	// 注意这里没有调用任何冷却判断——"不新建"是 B2 的决定，不是索引的效果。
	t.Run("存储支持同一事故挂多段", func(t *testing.T) {
		inc := &Incident{ID: "cool-1", DedupKey: "kC", ActiveMarker: int64(9002),
			Status: "resolved", FirstSeenTs: 100, LastSeenTs: 200, ResolvedTs: 200,
			CooldownUntilTs: 9999999999, Revision: 1} // 冷却远未到期
		if err := m.storeDB.Create(inc).Error; err != nil {
			t.Fatalf("建已结束事故: %v", err)
		}
		// 复发：追加第二段，挂在原事故上。
		for i, seg := range []*IncidentSegment{
			{ID: "seg-1", IncidentID: "cool-1", SeqNo: 1, StartTs: 100, EndTs: 200},
			{ID: "seg-2", IncidentID: "cool-1", SeqNo: 2, StartTs: 300, EndTs: 0},
		} {
			if err := m.storeDB.Create(seg).Error; err != nil {
				t.Fatalf("追加第 %d 段: %v", i+1, err)
			}
		}
		var segs int64
		m.storeDB.Model(&IncidentSegment{}).Where("incident_id = ?", "cool-1").Count(&segs)
		if segs != 2 {
			t.Errorf("同一事故应能挂多段，期望 2 段实际 %d", segs)
		}
	})

	// 索引管不到的边界，必须显式钉住，避免把存储约束误当成业务保证：
	// 事故已结束（让出活跃槽位）后，即使冷却远未到期，
	// 数据库**仍然允许**插入同键新事故。
	// 所以"冷却期内复发不新建事故"只能由 B2 的业务判断保证。
	t.Run("索引不阻止冷却期内新建_须由B2把关", func(t *testing.T) {
		resolved := &Incident{ID: "gap-1", DedupKey: "kG", ActiveMarker: int64(9100),
			Status: "resolved", FirstSeenTs: 100, LastSeenTs: 200, ResolvedTs: 200,
			CooldownUntilTs: 9999999999, Revision: 1} // 冷却远未到期
		if err := m.storeDB.Create(resolved).Error; err != nil {
			t.Fatalf("建已结束事故: %v", err)
		}
		inCooldown := &Incident{ID: "gap-2", DedupKey: "kG", ActiveMarker: incidentActiveMarker,
			Status: "open", FirstSeenTs: 300, LastSeenTs: 300, Revision: 1}
		if err := m.storeDB.Create(inCooldown).Error; err != nil {
			t.Fatalf("预期数据库允许插入（证明索引不负责冷却判断），却报错: %v", err)
		}
		// 这不是缺陷，而是分工：记录下来，B2 必须在业务层拦住。
		t.Log("已确认：冷却期内新建事故不会被索引拒绝，B2 需按 cooldown_until_ts 自行判断")
	})
}

// 并发建单：N 个 goroutine 抢同一聚合键，只能成一条活跃事故。
// 用 ON CONFLICT DO NOTHING 而非先查后写——并发下先查后写必然重复。
func TestIncidentConcurrentUpsertSingleActive(t *testing.T) {
	m := openIncidentTestStore(t)
	const n = 12
	var wg sync.WaitGroup
	// 收集每个 goroutine 的错误：DoNothing 下唯一冲突应被静默吸收，
	// 所以**任何**错误都意味着真实故障（锁超时、驱动问题），不能丢。
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			row := &Incident{ID: fmt.Sprintf("c-%d", i), DedupKey: "race",
				ActiveMarker: incidentActiveMarker, Status: "open",
				FirstSeenTs: int64(100 + i), LastSeenTs: int64(100 + i), Revision: 1}
			errs[i] = m.storeDB.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d 写入出错（DoNothing 本应吸收冲突，报错说明是真实故障）: %v", i, err)
		}
	}
	var active int64
	m.storeDB.Model(&Incident{}).
		Where("dedup_key = ? AND active_marker = ?", "race", incidentActiveMarker).Count(&active)
	if active != 1 {
		t.Errorf("并发下活跃事故必须恰好 1 条，实际 %d", active)
	}
}

// 重启续接：重新 openStore 同一文件后，事故与连续窗口状态必须还在。
func TestIncidentSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/t.db"

	m1 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m1.openStore(path); err != nil {
		t.Fatalf("首次 openStore: %v", err)
	}
	if err := m1.storeDB.Create(&Incident{ID: "r-1", DedupKey: "kR",
		ActiveMarker: incidentActiveMarker, Status: "open",
		FirstSeenTs: 100, LastSeenTs: 500, Severity: "SEV2", Revision: 3}).Error; err != nil {
		t.Fatalf("写事故: %v", err)
	}
	if err := m1.storeDB.Create(&IncidentContinuity{IncidentID: "r-1",
		LastCountedWindowToTs: 500, GapWindows: 1,
		EverHardSignal: true, WindowSeconds: 300}).Error; err != nil {
		t.Fatalf("写连续状态: %v", err)
	}
	if db, err := m1.storeDB.DB(); err == nil {
		_ = db.Close()
	}

	// 重启。
	m2 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m2.openStore(path); err != nil {
		t.Fatalf("重启 openStore: %v", err)
	}
	var inc Incident
	if err := m2.storeDB.Where("id = ?", "r-1").First(&inc).Error; err != nil {
		t.Fatalf("重启后事故丢失: %v", err)
	}
	if inc.Revision != 3 || inc.Severity != "SEV2" || inc.LastSeenTs != 500 {
		t.Errorf("重启后状态不一致: revision=%d severity=%s last_seen=%d",
			inc.Revision, inc.Severity, inc.LastSeenTs)
	}
	var cont IncidentContinuity
	if err := m2.storeDB.Where("incident_id = ?", "r-1").First(&cont).Error; err != nil {
		t.Fatalf("重启后连续窗口状态丢失: %v", err)
	}
	if !cont.EverHardSignal || cont.LastCountedWindowToTs != 500 || cont.GapWindows != 1 {
		t.Errorf("连续窗口状态不一致: %+v", cont)
	}
	// 重启后同一聚合键仍不得重复建活跃单。
	if err := m2.storeDB.Create(&Incident{ID: "r-2", DedupKey: "kR",
		ActiveMarker: incidentActiveMarker, Status: "open", Revision: 1}).Error; err == nil {
		t.Error("重启后同一聚合键重复建活跃事故未被拒绝")
	}
}

// 覆盖缺口与计数的三态：未知(nil) / 真实零值(0) / 已知正值。
//
// 这是 review 复现的那个缺陷的回归测试：用普通 int64 时，
// 数据库 NULL 读进来变成 0，再保存就把「未知」变成了「已知无缺口」。
func TestIncidentImpactUnknownZeroPositiveRoundTrip(t *testing.T) {
	m := openIncidentTestStore(t)
	i64 := func(v int64) *int64 { return &v }

	rows := []*IncidentImpact{
		// 未知：算不出来。
		{ID: "u", IncidentID: "i", Dimension: "channel", DimValue: "unknown-case",
			CoverageComplete: false, UncoveredSeconds: nil, PendingSeconds: nil,
			DistinctRequests: nil, VisibleAttempts: nil, LogRows: nil, SampleTotal: nil},
		// 真实零值：确实没有缺口、确实零请求。
		{ID: "z", IncidentID: "i", Dimension: "channel", DimValue: "zero-case",
			CoverageComplete: true, UncoveredSeconds: i64(0), PendingSeconds: i64(0),
			DistinctRequests: i64(0), VisibleAttempts: i64(0), LogRows: i64(0), SampleTotal: i64(0)},
		// 已知正值。
		{ID: "p", IncidentID: "i", Dimension: "channel", DimValue: "positive-case",
			CoverageComplete: false, UncoveredSeconds: i64(120), PendingSeconds: i64(60),
			DistinctRequests: i64(7), VisibleAttempts: i64(9), LogRows: i64(31), SampleTotal: i64(40)},
	}
	for _, r := range rows {
		if err := m.storeDB.Create(r).Error; err != nil {
			t.Fatalf("写 %s: %v", r.ID, err)
		}
	}

	// 未知必须读回 nil，不能变成 0。
	var unk IncidentImpact
	if err := m.storeDB.Where("id = ?", "u").First(&unk).Error; err != nil {
		t.Fatalf("读未知行: %v", err)
	}
	for name, p := range map[string]*int64{
		"uncovered_seconds": unk.UncoveredSeconds, "pending_seconds": unk.PendingSeconds,
		"distinct_requests": unk.DistinctRequests, "visible_attempts": unk.VisibleAttempts,
		"log_rows": unk.LogRows, "sample_total": unk.SampleTotal,
	} {
		if p != nil {
			t.Errorf("%s 未知应读回 nil，实际读到 %d —— 未知被当成了真实值", name, *p)
		}
	}

	// 真实零值必须读回非 nil 的 0，与未知区分。
	var zero IncidentImpact
	if err := m.storeDB.Where("id = ?", "z").First(&zero).Error; err != nil {
		t.Fatalf("读零值行: %v", err)
	}
	for name, p := range map[string]*int64{
		"uncovered_seconds": zero.UncoveredSeconds, "distinct_requests": zero.DistinctRequests,
		"log_rows": zero.LogRows, "sample_total": zero.SampleTotal,
	} {
		if p == nil {
			t.Errorf("%s 的真实零值被存成了未知", name)
		} else if *p != 0 {
			t.Errorf("%s 真实零值读回 %d", name, *p)
		}
	}

	// 已知正值原样往返。
	var pos IncidentImpact
	if err := m.storeDB.Where("id = ?", "p").First(&pos).Error; err != nil {
		t.Fatalf("读正值行: %v", err)
	}
	for name, want := range map[string]int64{
		"uncovered_seconds": 120, "pending_seconds": 60, "distinct_requests": 7,
		"visible_attempts": 9, "log_rows": 31, "sample_total": 40,
	} {
		got := map[string]*int64{
			"uncovered_seconds": pos.UncoveredSeconds, "pending_seconds": pos.PendingSeconds,
			"distinct_requests": pos.DistinctRequests, "visible_attempts": pos.VisibleAttempts,
			"log_rows": pos.LogRows, "sample_total": pos.SampleTotal,
		}[name]
		if got == nil || *got != want {
			t.Errorf("%s 期望 %d，实际 %v", name, want, got)
		}
	}

	// 读出来再保存一遍，未知不得退化成 0（这是原缺陷的触发路径）。
	if err := m.storeDB.Save(&unk).Error; err != nil {
		t.Fatalf("回存未知行: %v", err)
	}
	var reread IncidentImpact
	if err := m.storeDB.Where("id = ?", "u").First(&reread).Error; err != nil {
		t.Fatalf("重读: %v", err)
	}
	if reread.UncoveredSeconds != nil || reread.DistinctRequests != nil {
		t.Error("读出再保存后未知退化成了具体值")
	}
}

// 观测表的计数同样要能表达未知。
func TestIncidentObservationCountsUnknown(t *testing.T) {
	m := openIncidentTestStore(t)
	if err := m.storeDB.Create(&IncidentObservation{ID: "o-1", IncidentID: "i",
		WindowFromTs: 100, WindowToTs: 400,
		DataPresent:      false, // 采集缺口：这一窗没有数据
		DistinctRequests: nil, VisibleAttempts: nil, LogRows: nil, SampleTotal: nil,
	}).Error; err != nil {
		t.Fatalf("写缺口观测: %v", err)
	}
	var ob IncidentObservation
	if err := m.storeDB.Where("id = ?", "o-1").First(&ob).Error; err != nil {
		t.Fatalf("读: %v", err)
	}
	if ob.DistinctRequests != nil || ob.VisibleAttempts != nil ||
		ob.LogRows != nil || ob.SampleTotal != nil {
		t.Error("采集缺口窗口的计数必须是 nil（未知），不能是 0（已知无请求）")
	}
	if ob.DataPresent {
		t.Error("data_present 应为 false")
	}
}

// 审计幂等：同一 IdempotencyKey 第二次写入不得产生第二条记录。
func TestIncidentAuditIdempotent(t *testing.T) {
	m := openIncidentTestStore(t)
	row := func() *IncidentAudit {
		return &IncidentAudit{ID: "a-1", IncidentID: "i-1", Layer: "approved",
			Action: "approve", ToState: governanceStateQuarantined, Actor: "op-a",
			ActorKind: "human", IdempotencyKey: "idem-1", CreatedAt: 100}
	}
	if err := m.storeDB.Clauses(clause.OnConflict{DoNothing: true}).Create(row()).Error; err != nil {
		t.Fatalf("首次写审计: %v", err)
	}
	second := row()
	second.ID = "a-2"
	if err := m.storeDB.Clauses(clause.OnConflict{DoNothing: true}).Create(second).Error; err != nil {
		t.Fatalf("重复提交不应报错，应静默忽略: %v", err)
	}
	var n int64
	m.storeDB.Model(&IncidentAudit{}).Where("idempotency_key = ?", "idem-1").Count(&n)
	if n != 1 {
		t.Errorf("同一幂等键应只留 1 条审计，实际 %d", n)
	}
}

// 四层状态各自独立：写 approved 不得连带改 applied，反之亦然。
// 「已审批」≠「已执行」必须在存储层就立得住。
func TestIncidentStateLayersIndependent(t *testing.T) {
	m := openIncidentTestStore(t)
	// 前三层用 01.3 的治理状态枚举；第四层是 NewAPI 实际配置快照，不是治理状态。
	enabled := true
	group := "default"
	var priority int64 = 10
	var weight int64 = 100
	if err := m.storeDB.Create(&IncidentState{IncidentID: "i-1",
		ObservedState:         governanceStateDegraded,
		RecommendedState:      governanceStateQuarantined,
		ApprovedState:         "", // 尚无人审批
		AppliedNewapiEnabled:  &enabled,
		AppliedNewapiGroup:    &group,
		AppliedNewapiPriority: &priority,
		AppliedNewapiWeight:   &weight,
		AppliedSyncOK:         true,
		AppliedSyncedAt:       150,
		Revision:              1}).Error; err != nil {
		t.Fatalf("建状态行: %v", err)
	}
	// 人工审批只写 approved 那几列。
	if err := m.storeDB.Model(&IncidentState{}).Where("incident_id = ?", "i-1").
		Updates(map[string]any{"approved_state": governanceStateQuarantined,
			"approved_by": "op-a", "approved_at": 200, "revision": 2}).Error; err != nil {
		t.Fatalf("审批: %v", err)
	}
	var st IncidentState
	if err := m.storeDB.Where("incident_id = ?", "i-1").First(&st).Error; err != nil {
		t.Fatalf("读状态: %v", err)
	}
	// 审批不得触碰第四层的任何配置字段——这才是「已审批≠已执行」的存储保证。
	if st.AppliedNewapiEnabled == nil || !*st.AppliedNewapiEnabled {
		t.Error("审批不得改动 NewAPI 实际启停")
	}
	if st.AppliedNewapiPriority == nil || *st.AppliedNewapiPriority != 10 {
		t.Error("审批不得改动 NewAPI 实际优先级")
	}
	if st.AppliedNewapiWeight == nil || *st.AppliedNewapiWeight != 100 {
		t.Error("审批不得改动 NewAPI 实际权重")
	}
	if st.AppliedSyncedAt != 150 {
		t.Error("审批不得改动实际配置的同步时刻")
	}
	if st.ApprovedState != governanceStateQuarantined {
		t.Errorf("审批层未写入: %q", st.ApprovedState)
	}
	// 本轮不断言漂移：治理状态与启停/分组/优先级/权重不同类，
	// 不能用字符串比较证明漂移。判定需要「该治理决定期望什么实际配置」的映射，
	// 与只读同步同属后续阶段。这里只确认两层能独立并存且都被完整保存。
	if st.DriftDetected {
		t.Error("B1 不写入漂移判定结果，drift_detected 应保持 false")
	}
}

// 前三层只接受 01.3 的五种治理状态；第四层不受这套枚举约束。
func TestIncidentGovernanceStateEnum(t *testing.T) {
	for _, s := range []string{"HEALTHY", "SUSPECT", "DEGRADED", "QUARANTINED", "RECOVERY_PROBATION"} {
		if !validGovernanceState(s) {
			t.Errorf("%q 属 01.3 治理状态枚举，应被接受", s)
		}
	}
	// 这三个是此前测试里自造的值，不在共享枚举内。
	for _, s := range []string{"degraded", "quarantine_recommended", "quarantine_approved",
		"enabled", "disabled", ""} {
		if validGovernanceState(s) {
			t.Errorf("%q 不属 01.3 治理状态枚举，不得被接受", s)
		}
	}
}

// 第四层四个配置字段必须能表达「未知」，且未知与真实值可区分。
// 同步失败时不能把 false/0 当成真实配置写进去。
func TestIncidentAppliedConfigUnknownDistinctFromReal(t *testing.T) {
	m := openIncidentTestStore(t)
	// 同步失败：四个字段全 nil，AppliedSyncOK=false。
	if err := m.storeDB.Create(&IncidentState{IncidentID: "unk-1",
		ObservedState: governanceStateSuspect,
		AppliedSyncOK: false, AppliedSyncError: "只读同步超时", Revision: 1}).Error; err != nil {
		t.Fatalf("建未知行: %v", err)
	}
	var unk IncidentState
	if err := m.storeDB.Where("incident_id = ?", "unk-1").First(&unk).Error; err != nil {
		t.Fatalf("读: %v", err)
	}
	for name, got := range map[string]bool{
		"enabled":  unk.AppliedNewapiEnabled != nil,
		"group":    unk.AppliedNewapiGroup != nil,
		"priority": unk.AppliedNewapiPriority != nil,
		"weight":   unk.AppliedNewapiWeight != nil,
	} {
		if got {
			t.Errorf("同步失败时 %s 必须为 nil（未知），不能填默认值", name)
		}
	}

	// 真实的「停用 + 权重 0」：字段非 nil 且值为 false/0，必须与未知区分开。
	disabled := false
	var zeroWeight int64 = 0
	if err := m.storeDB.Create(&IncidentState{IncidentID: "real-0",
		ObservedState:        governanceStateQuarantined,
		AppliedNewapiEnabled: &disabled, AppliedNewapiWeight: &zeroWeight,
		AppliedSyncOK: true, Revision: 1}).Error; err != nil {
		t.Fatalf("建真实零值行: %v", err)
	}
	var real IncidentState
	if err := m.storeDB.Where("incident_id = ?", "real-0").First(&real).Error; err != nil {
		t.Fatalf("读: %v", err)
	}
	if real.AppliedNewapiEnabled == nil {
		t.Fatal("真实的「已停用」被存成了未知")
	}
	if *real.AppliedNewapiEnabled {
		t.Error("enabled=false 未正确保存")
	}
	if real.AppliedNewapiWeight == nil {
		t.Fatal("真实的「权重 0」被存成了未知")
	}
	if *real.AppliedNewapiWeight != 0 {
		t.Errorf("weight=0 未正确保存，得到 %d", *real.AppliedNewapiWeight)
	}
}

// 乐观并发：revision 不匹配的更新必须影响 0 行。
func TestIncidentStateOptimisticConcurrency(t *testing.T) {
	m := openIncidentTestStore(t)
	if err := m.storeDB.Create(&IncidentState{IncidentID: "i-2", Revision: 5}).Error; err != nil {
		t.Fatalf("建状态行: %v", err)
	}
	stale := m.storeDB.Model(&IncidentState{}).
		Where("incident_id = ? AND revision = ?", "i-2", 4).
		Updates(map[string]any{"approved_state": governanceStateQuarantined, "revision": 5})
	if stale.Error != nil {
		t.Fatalf("CAS 更新: %v", stale.Error)
	}
	if stale.RowsAffected != 0 {
		t.Errorf("过期 revision 的更新必须被拒，实际影响 %d 行", stale.RowsAffected)
	}
	fresh := m.storeDB.Model(&IncidentState{}).
		Where("incident_id = ? AND revision = ?", "i-2", 5).
		Updates(map[string]any{"approved_state": governanceStateRecoveryProbation, "revision": 6})
	if fresh.RowsAffected != 1 {
		t.Errorf("当前 revision 的更新应成功，实际影响 %d 行", fresh.RowsAffected)
	}
}

// 事务内失败必须整体回滚，不留半条事故。
func TestIncidentWriteRollsBackOnError(t *testing.T) {
	m := openIncidentTestStore(t)
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Incident{ID: "tx-1", DedupKey: "kT",
			ActiveMarker: incidentActiveMarker, Status: "open", Revision: 1}).Error; err != nil {
			return err
		}
		return fmt.Errorf("模拟后续步骤失败")
	})
	if err == nil {
		t.Fatal("事务应返回错误")
	}
	var n int64
	m.storeDB.Model(&Incident{}).Where("id = ?", "tx-1").Count(&n)
	if n != 0 {
		t.Errorf("事务失败后不得留下部分写入，实际残留 %d 条", n)
	}
}

// plan ID 必须随 incident 建表一起 bump，且两个常量同步。
// 只改一个会让 source-v2 库复用错误的迁移前快照。
func TestIncidentPlanIDBumped(t *testing.T) {
	src := readSourceForPlanCheck(t, "store_migration_backup.go")
	for _, want := range []string{"v59", "incident-core-v1"} {
		if n := countOccurrences(src, want); n < 2 {
			t.Errorf("plan ID 标记 %q 应在两个常量里都出现，实际出现 %d 次", want, n)
		}
	}
	// 旧段保留是为了 plan 串自带可读的版本回溯链（既有惯例），
	// 不代表旧 v58 快照仍被钉住——保留策略可以淘汰不再被引用的旧计划快照。
	if countOccurrences(src, "from-v58") < 2 {
		t.Error("plan 串应保留 from-v58 回溯段（既有惯例）")
	}
	store := readSourceForPlanCheck(t, "store.go")
	for _, model := range []string{"&Incident{}", "&IncidentSegment{}", "&IncidentContinuity{}",
		"&IncidentObservation{}", "&IncidentImpact{}", "&IncidentState{}", "&IncidentAudit{}"} {
		if countOccurrences(store, model) < 1 {
			t.Errorf("模型 %s 未注册进 AutoMigrate，生产上会报「表不存在」", model)
		}
	}
}

// 旧库（没有 incident 表）升级后：新表建出来、旧数据一条不丢、迁移前快照已生成。
// 这是「迁移成功」的实际验证，不是只看 AutoMigrate 没报错。
func TestIncidentMigrationOnLegacyStoreKeepsOldData(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/t.db"
	t.Setenv("MONITOR_STORE_BACKUP_DIR", dir+"/backups")

	// 造一个「旧库」：先正常建库，写入既有业务数据，然后把 incident 表删掉，
	// 模拟升级前的真实形态。
	m1 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m1.openStore(path); err != nil {
		t.Fatalf("建旧库: %v", err)
	}
	if err := m1.storeDB.Create(&AlertConfig{ID: 1}).Error; err != nil {
		t.Fatalf("写既有业务数据: %v", err)
	}
	for _, tbl := range []string{"incidents", "incident_segments", "incident_continuity",
		"incident_observations", "incident_impacts", "incident_states", "incident_audits"} {
		if err := m1.storeDB.Migrator().DropTable(tbl); err != nil {
			t.Fatalf("模拟旧库删表 %s: %v", tbl, err)
		}
	}
	if db, err := m1.storeDB.DB(); err == nil {
		_ = db.Close()
	}

	// 升级：重新 openStore 应补齐 incident 表。
	m2 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m2.openStore(path); err != nil {
		t.Fatalf("升级旧库失败: %v", err)
	}
	for _, tbl := range []string{"incidents", "incident_states", "incident_audits"} {
		if !m2.storeDB.Migrator().HasTable(tbl) {
			t.Errorf("升级后 %s 仍不存在", tbl)
		}
	}
	// 旧数据不能丢。
	var alerts int64
	m2.storeDB.Model(&AlertConfig{}).Count(&alerts)
	if alerts != 1 {
		t.Errorf("迁移丢了既有数据：AlertConfig 期望 1 条实际 %d", alerts)
	}
	// 升级后新表可正常写入（证明索引与约束真的建好了）。
	if err := m2.storeDB.Create(&Incident{ID: "mig-1", DedupKey: "kM",
		ActiveMarker: incidentActiveMarker, Status: "open", Revision: 1}).Error; err != nil {
		t.Errorf("升级后写入新表失败: %v", err)
	}
	// 迁移前快照必须已生成，这是回滚点。
	entries, err := os.ReadDir(dir + "/backups")
	if err != nil {
		t.Fatalf("读备份目录: %v", err)
	}
	var found bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), preMigrationSnapshotPrefix) {
			found = true
			break
		}
	}
	if !found {
		// 这是本次 v59 迁移前生成的快照，保存迁移前（v58 结构）的数据，
		// 也正是正确的回滚点；不是"v58 当年的那个快照"。
		t.Error("本次迁移前快照未生成：回滚点缺失，不得降低既有备份保护")
	}
}

// 功能开关默认关闭时，建表照样发生（AutoMigrate 不受 flag 控制），
// 且不得因为开关关闭就删历史数据。
func TestIncidentTablesExistWhenFeatureDisabled(t *testing.T) {
	cfg := DefaultIncidentConfig()
	if cfg.Enabled {
		t.Fatal("incident 功能默认必须关闭")
	}
	m := openIncidentTestStore(t)
	if !m.storeDB.Migrator().HasTable("incidents") {
		t.Error("开关关闭时表仍须建出来，否则开启时首次写入会失败")
	}
	// 关闭状态下写入历史数据，再确认没有任何清理逻辑删它。
	if err := m.storeDB.Create(&Incident{ID: "keep-1", DedupKey: "kK",
		ActiveMarker: incidentActiveMarker, Status: "resolved", Revision: 1}).Error; err != nil {
		t.Fatalf("写历史: %v", err)
	}
	var n int64
	m.storeDB.Model(&Incident{}).Where("id = ?", "keep-1").Count(&n)
	if n != 1 {
		t.Error("开关关闭不得删除历史事故数据")
	}
}
