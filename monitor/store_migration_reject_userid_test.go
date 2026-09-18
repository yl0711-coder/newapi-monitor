package monitor

import (
	"strings"
	"testing"

	"gorm.io/gorm"
)

// 建旧主键(无 user_id)的表，模拟 v41 之前的库。
func createLegacyRejectTables(t *testing.T, db *gorm.DB, withUserIDColumn bool) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE rejection_samples (
			bucket_ts integer, node text, reason text, model text, grp text, count integer,
			PRIMARY KEY (bucket_ts, node, reason, model, grp))`,
		`CREATE TABLE stability_reject_hours (
			hour_ts integer, node text, reason text, model text, grp text, count integer,
			PRIMARY KEY (hour_ts, node, reason, model, grp))`,
	}
	if withUserIDColumn {
		// v41 的 AutoMigrate 加过列但没能改主键，旧行是 NULL。
		stmts = append(stmts,
			`ALTER TABLE rejection_samples ADD COLUMN user_id integer`,
			`ALTER TABLE stability_reject_hours ADD COLUMN user_id integer`)
	}
	for _, s := range stmts {
		if err := db.Exec(s).Error; err != nil {
			t.Fatalf("建旧表失败: %v", err)
		}
	}
}

func rejectPK(t *testing.T, db *gorm.DB, table string) []string {
	t.Helper()
	pk, _, err := sqlitePrimaryKeyColumns(db, table)
	if err != nil {
		t.Fatalf("读主键失败: %v", err)
	}
	return pk
}

// 旧库迁移后：主键含 user_id，历史行保留且 user_id 落到 0。
func TestRejectUserIDMigrationRebuildsPK(t *testing.T) {
	m := newTestMonitor(t)
	db := m.storeDB
	if err := db.Migrator().DropTable("rejection_samples", "stability_reject_hours"); err != nil {
		t.Fatal(err)
	}
	createLegacyRejectTables(t, db, false)
	if err := db.Exec(`INSERT INTO rejection_samples VALUES (60,'master','invalid_token','m1','g1',7)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO stability_reject_hours VALUES (3600,'master','invalid_token','m1','g1',7)`).Error; err != nil {
		t.Fatal(err)
	}

	if err := migrateRejectionUserIDPrimaryKey(db); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	for _, table := range []string{"rejection_samples", "stability_reject_hours"} {
		pk := rejectPK(t, db, table)
		if len(pk) != 6 || pk[5] != "user_id" {
			t.Errorf("%s 主键未含 user_id: %v", table, pk)
		}
		var row struct {
			UserID int64 `gorm:"column:user_id"`
			Count  int64 `gorm:"column:count"`
		}
		if err := db.Raw("SELECT user_id, count FROM " + table).Scan(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.Count != 7 {
			t.Errorf("%s 历史 count 丢了: %d", table, row.Count)
		}
		if row.UserID != 0 {
			t.Errorf("%s 历史 user_id 应为 0，实为 %d", table, row.UserID)
		}
	}
}

// v41 加过列、旧行为 NULL 的库：NULL 必须归 0。
// 留着 NULL 会绕过唯一约束，同一客户重复累积成多行。
func TestRejectUserIDMigrationCoalescesNull(t *testing.T) {
	m := newTestMonitor(t)
	db := m.storeDB
	if err := db.Migrator().DropTable("rejection_samples", "stability_reject_hours"); err != nil {
		t.Fatal(err)
	}
	createLegacyRejectTables(t, db, true)
	if err := db.Exec(`INSERT INTO rejection_samples
		(bucket_ts,node,reason,model,grp,count,user_id) VALUES (60,'master','r','m','g',3,NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrateRejectionUserIDPrimaryKey(db); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	var nulls int64
	if err := db.Raw("SELECT COUNT(*) FROM rejection_samples WHERE user_id IS NULL").Scan(&nulls).Error; err != nil {
		t.Fatal(err)
	}
	if nulls != 0 {
		t.Errorf("仍有 %d 行 user_id 为 NULL", nulls)
	}
}

// 迁移必须幂等：已是新主键时直接跳过，不重建、不丢数据。
func TestRejectUserIDMigrationIdempotent(t *testing.T) {
	m := newTestMonitor(t)
	db := m.storeDB
	if err := m.upsertRejections([]RejectionSample{
		{BucketTs: 60, Node: "master", Reason: "r", Model: "m", Grp: "g", UserID: 42, Count: 5},
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := migrateRejectionUserIDPrimaryKey(db); err != nil {
			t.Fatalf("重复迁移失败: %v", err)
		}
	}
	var got RejectionSample
	if err := db.First(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got.UserID != 42 || got.Count != 5 {
		t.Errorf("幂等迁移改了数据: user=%d count=%d", got.UserID, got.Count)
	}
}

// 主键结构不认识时必须报错，不能猜着改用户的表。
func TestRejectUserIDMigrationRefusesUnknownPK(t *testing.T) {
	m := newTestMonitor(t)
	db := m.storeDB
	if err := db.Migrator().DropTable("rejection_samples"); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TABLE rejection_samples (
		bucket_ts integer, node text, count integer, PRIMARY KEY (bucket_ts, node))`).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrateRejectionUserIDPrimaryKey(db); err == nil {
		t.Error("未知主键结构应报错")
	}
}

// 本轮的根因回归：迁移后 UPSERT 必须能跑通。
// 迁移前这里就是生产上那条 "ON CONFLICT clause does not match any
// PRIMARY KEY or UNIQUE constraint" 的 500。
func TestRejectUpsertWorksAfterMigration(t *testing.T) {
	m := newTestMonitor(t)
	db := m.storeDB
	if err := db.Migrator().DropTable("rejection_samples", "stability_reject_hours"); err != nil {
		t.Fatal(err)
	}
	createLegacyRejectTables(t, db, false)
	if err := migrateRejectionUserIDPrimaryKey(db); err != nil {
		t.Fatal(err)
	}

	// 同分钟同错误的两个客户：必须是两行，不能被并成一行。
	rows := []RejectionSample{
		{BucketTs: 60, Node: "master", Reason: "no_available_channel", Model: "m", Grp: "g", UserID: 102, Count: 2},
		{BucketTs: 60, Node: "master", Reason: "no_available_channel", Model: "m", Grp: "g", UserID: 143, Count: 3},
	}
	if err := m.upsertRejections(rows); err != nil {
		t.Fatalf("迁移后 UPSERT 仍失败: %v", err)
	}
	var n int64
	if err := db.Model(&RejectionSample{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("两个客户被并成 %d 行，错误会对应到错的客户", n)
	}
	// 重推同批：按主键累加，客户各自累加互不干扰。
	if err := m.upsertRejections(rows); err != nil {
		t.Fatal(err)
	}
	var got RejectionSample
	if err := db.Where("user_id = ?", 143).First(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got.Count != 6 {
		t.Errorf("user 143 累加后应为 6，实为 %d", got.Count)
	}
}

// 小时表 rollup 也要能跑通，且按客户分行。
func TestRejectRollupPerUserAfterMigration(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.upsertRejections([]RejectionSample{
		{BucketTs: 3660, Node: "master", Reason: "r", Model: "m", Grp: "g", UserID: 1, Count: 2},
		{BucketTs: 3720, Node: "master", Reason: "r", Model: "m", Grp: "g", UserID: 1, Count: 3},
		{BucketTs: 3660, Node: "master", Reason: "r", Model: "m", Grp: "g", UserID: 2, Count: 4},
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.rollupStabilityRejections(0); err != nil {
		t.Fatalf("rollup 失败: %v", err)
	}
	var hours []StabilityRejectHour
	if err := m.storeDB.Order("user_id").Find(&hours).Error; err != nil {
		t.Fatal(err)
	}
	if len(hours) != 2 {
		t.Fatalf("应按客户出 2 行，实为 %d", len(hours))
	}
	if hours[0].UserID != 1 || hours[0].Count != 5 {
		t.Errorf("user 1 应累加为 5，实为 user=%d count=%d", hours[0].UserID, hours[0].Count)
	}
	if hours[1].UserID != 2 || hours[1].Count != 4 {
		t.Errorf("user 2 应为 4，实为 user=%d count=%d", hours[1].UserID, hours[1].Count)
	}
}

func TestRejectUserIDMigrationBumpsBaseAndCombinedPlans(t *testing.T) {
	for name, plan := range map[string]string{
		"base":     preMigrationPlanID,
		"combined": preMigrationCombinedPlanID,
	} {
		if !strings.Contains(plan, "rejection-user-id-pk-user-directory") {
			t.Errorf("%s migration plan 未包含 rejection user_id schema：%q", name, plan)
		}
	}
	if preMigrationPlanID == preMigrationCombinedPlanID {
		t.Fatal("source-v2 combined plan 必须与普通 plan 分开")
	}
}
