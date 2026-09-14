package monitor

import (
	"fmt"
	"log/slog"
	"slices"

	"gorm.io/gorm"
)

// 前置拒绝两张表把 user_id 加进主键。GORM 的 SQLite AutoMigrate 只加列、
// 不重建已有表的主键约束，因此旧库上 ON CONFLICT(..., user_id) 找不到匹配的
// 唯一约束，整条 UPSERT 在部署后才会以
// "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint" 失败。
// 这里在强制快照之后、AutoMigrate 之前显式重建。
//
// 与上游错误日志那次主键迁移(migrateChannelUpstreamErrorLogEventKey)的关键区别：
// 本次是给既有主键**追加**一列，旧键已唯一 ⇒ 新键必然唯一。不存在行合并或
// 重复，无需去重、无需回退标识，一条 INSERT..SELECT 即可整表搬迁。
//
// 历史行的 user_id 填 0 = "采集时未上报客户"，与运行期 user_id=0
// (未鉴权/无效令牌)语义一致，页面按 0 显示"未鉴权"，不会把历史数据
// 错认成某个具体客户。
type rejectUserIDMigration struct {
	table     string
	legacy    string
	legacyPK  []string // 迁移前应有的主键列，按 PRAGMA 顺序
	createSQL string
}

// rejectUserIDMigrations 的建表语句只写列与主键，索引仍交给随后的 AutoMigrate
// 按模型标签建，避免索引定义在两处漂移。列类型按 GORM 的 SQLite 映射写死
// (int64→integer、string→text)，否则 AutoMigrate 会认为类型不符再次改表。
var rejectUserIDMigrations = []rejectUserIDMigration{
	{
		table:    "rejection_samples",
		legacy:   "rejection_samples_v29_userid_migration",
		legacyPK: []string{"bucket_ts", "node", "reason", "model", "grp"},
		createSQL: `CREATE TABLE rejection_samples (
			bucket_ts integer, node text, reason text, model text, grp text,
			user_id integer, count integer,
			PRIMARY KEY (bucket_ts, node, reason, model, grp, user_id)
		)`,
	},
	{
		table:    "stability_reject_hours",
		legacy:   "stability_reject_hours_v29_userid_migration",
		legacyPK: []string{"hour_ts", "node", "reason", "model", "grp"},
		createSQL: `CREATE TABLE stability_reject_hours (
			hour_ts integer, node text, reason text, model text, grp text,
			user_id integer, count integer,
			PRIMARY KEY (hour_ts, node, reason, model, grp, user_id)
		)`,
	},
}

func migrateRejectionUserIDPrimaryKey(db *gorm.DB) error {
	for _, spec := range rejectUserIDMigrations {
		if err := migrateOneRejectUserIDTable(db, spec); err != nil {
			return fmt.Errorf("%s 主键迁移失败: %w", spec.table, err)
		}
	}
	return nil
}

func sqliteTableExists(db *gorm.DB, table string) (bool, error) {
	var n int64
	err := db.Raw(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n).Error
	return n > 0, err
}

// sqlitePrimaryKeyColumns 返回按 PRAGMA pk 序号排列的主键列，以及全部列名集合。
func sqlitePrimaryKeyColumns(db *gorm.DB, table string) ([]string, map[string]bool, error) {
	var rows []struct {
		Name string `gorm:"column:name"`
		PK   int    `gorm:"column:pk"`
	}
	if err := db.Raw("PRAGMA table_info(" + quoteSQLiteIdentifier(table) + ")").Scan(&rows).Error; err != nil {
		return nil, nil, err
	}
	byOrder := map[int]string{}
	columns := make(map[string]bool, len(rows))
	for _, r := range rows {
		columns[r.Name] = true
		if r.PK > 0 {
			byOrder[r.PK] = r.Name
		}
	}
	pk := make([]string, 0, len(byOrder))
	for i := 1; i <= len(byOrder); i++ {
		name, ok := byOrder[i]
		if !ok {
			return nil, nil, fmt.Errorf("%s 主键序号不连续: %v", table, byOrder)
		}
		pk = append(pk, name)
	}
	return pk, columns, nil
}

func migrateOneRejectUserIDTable(db *gorm.DB, spec rejectUserIDMigration) error {
	has, err := sqliteTableExists(db, spec.table)
	if err != nil {
		return err
	}
	if !has {
		// 全新库：AutoMigrate 会直接按模型建出含 user_id 的主键。
		return nil
	}
	pk, columns, err := sqlitePrimaryKeyColumns(db, spec.table)
	if err != nil {
		return err
	}
	if len(pk) == len(spec.legacyPK)+1 && pk[len(pk)-1] == "user_id" {
		return nil // 已迁移
	}
	if !slices.Equal(pk, spec.legacyPK) {
		return fmt.Errorf("主键结构未知，拒绝自动迁移: %v", pk)
	}
	if has, err := sqliteTableExists(db, spec.legacy); err != nil {
		return err
	} else if has {
		return fmt.Errorf("发现未完成的迁移临时表 %s", spec.legacy)
	}
	// v41 镜像的 AutoMigrate 可能已加过 user_id 列（旧行为 NULL）。列在就沿用它的值，
	// 不在就填 0。COALESCE 不能省：SQLite 允许主键列为 NULL 且 NULL 之间互不相等，
	// 留着 NULL 会让同一客户的多行绕过唯一约束重复累积。
	userIDExpr := "0"
	if columns["user_id"] {
		userIDExpr = "COALESCE(user_id, 0)"
	}
	var migrated int64
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("ALTER TABLE " + spec.table + " RENAME TO " + spec.legacy).Error; err != nil {
			return err
		}
		if err := tx.Exec(spec.createSQL).Error; err != nil {
			return err
		}
		// 旧主键已唯一 ⇒ 追加一列后仍唯一，整表一次搬迁即可，不会有冲突或行合并。
		keyCols := ""
		for _, c := range spec.legacyPK {
			keyCols += c + ", "
		}
		result := tx.Exec("INSERT INTO " + spec.table +
			" (" + keyCols + "user_id, count) SELECT " + keyCols +
			userIDExpr + ", count FROM " + spec.legacy)
		if result.Error != nil {
			return result.Error
		}
		migrated = result.RowsAffected
		return tx.Exec("DROP TABLE " + spec.legacy).Error
	})
	if err != nil {
		return err
	}
	slog.Info("前置拒绝主键迁移完成", "table", spec.table, "rows", migrated, "user_id", userIDExpr)
	return nil
}
