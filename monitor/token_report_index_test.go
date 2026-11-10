package monitor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestTokenReportIndexMigrationAndTransactionalMaintenance(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		if err := db.Exec("DROP TRIGGER IF EXISTS token_report_entry_" + event + "_v2").Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("DROP TRIGGER IF EXISTS token_report_entry_" + event + "_v1").Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Migrator().DropTable(tokenReportIndexTable); err != nil {
		t.Fatal(err)
	}
	for i, row := range []TokenDetectionCurrent{
		{Candidate: true, SourceComplete: true, ResultJSON: `{"rules":[]}`},
		{SourceComplete: false, ResultJSON: `{"rules":[{"eligible":true}]}`},
		{SourceComplete: true, ResultJSON: `{"rules":[{"eligible":false}]}`},
		{SourceComplete: true, ResultJSON: `{"rules":[{"eligible":true}]}`},
		{SourceComplete: true, ResultJSON: `{"rules":[]}`},
	} {
		row.SourceEpoch, row.LogID, row.CreatedAt = m.cfg.TokenFactsSourceEpoch, int64(i+1), now-100+int64(i)
		row.UserID, row.ChannelID, row.UserGroup, row.Model = 2, 3, "group", "model"
		row.DetectionState = "evaluated"
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 200, To: now, Status: "insufficient"}
	want, err := tokenReportInsufficientCount(db, q)
	if err != nil || want != 2 {
		t.Fatal("legacy status predicate", want, err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	verify := func(query tokenDetectionQuery) {
		t.Helper()
		var oldCount int64
		legacy := tokenReportIdentityRange(db.Model(&TokenDetectionCurrent{}), query).Where(`candidate=0 AND (source_complete=0 OR EXISTS (SELECT 1 FROM json_each(result_json,'$.rules') WHERE json_extract(value,'$.eligible')=0))`)
		if err := legacy.Count(&oldCount).Error; err != nil {
			t.Fatal(err)
		}
		got, err := tokenReportInsufficientCount(db, query)
		if err != nil || got != oldCount {
			t.Fatalf("derived count %d != legacy %d: %v", got, oldCount, err)
		}
		for _, status := range []string{"candidate", "insufficient", "not_hit"} {
			query.Status = status
			predicate := map[string]string{
				"candidate":    "candidate=1",
				"insufficient": `candidate=0 AND (source_complete=0 OR EXISTS (SELECT 1 FROM json_each(result_json,'$.rules') WHERE json_extract(value,'$.eligible')=0))`,
				"not_hit":      `candidate=0 AND source_complete=1 AND NOT EXISTS (SELECT 1 FROM json_each(result_json,'$.rules') WHERE json_extract(value,'$.eligible')=0)`,
			}[status]
			var expected, actual []int64
			if err := tokenReportIdentityRange(db.Model(&TokenDetectionCurrent{}), query).Where(predicate).Order("log_id").Pluck("log_id", &expected).Error; err != nil {
				t.Fatal(err)
			}
			if err := tokenDetectionRange(db, query).Order("log_id").Pluck("log_id", &actual).Error; err != nil || !slices.Equal(actual, expected) {
				t.Fatalf("status %s changed identities: got=%v want=%v err=%v", status, actual, expected, err)
			}
			var rows []TokenDetectionCurrent
			if err := tokenDetectionRange(db, query).Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				var stored string
				if err := db.Table(tokenReportIndexTable).Select("status").Where("source_epoch=? AND log_id=?", row.SourceEpoch, row.LogID).Scan(&stored).Error; err != nil || stored != status {
					t.Fatalf("filtered result disagrees with indexed outcome: %s %s %v", stored, status, err)
				}
			}
		}
	}
	verify(q)
	rollback := errors.New("rollback test")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=2", q.SourceEpoch).Updates(map[string]any{"source_complete": true}).Error; err != nil {
			return err
		}
		got, err := tokenReportInsufficientCount(tx, q)
		if err != nil || got != 1 {
			t.Fatal("uncommitted update not reflected atomically", got, err)
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	verify(q)
	if err := db.Exec(`UPDATE token_detection_currents SET user_id=0,channel_id=0,user_group='',model='' WHERE source_epoch=? AND log_id=3`, q.SourceEpoch).Error; err != nil {
		t.Fatal(err)
	}
	filtered := q
	filtered.UserSet, filtered.ChannelSet, filtered.GroupSet, filtered.ModelSet = true, true, true, true
	verify(filtered)
	if err := db.Where("source_epoch=? AND log_id=2", q.SourceEpoch).Delete(&TokenDetectionCurrent{}).Error; err != nil {
		t.Fatal(err)
	}
	verify(q)
	if err := db.Exec(`UPDATE token_detection_currents SET source_complete=NULL WHERE source_epoch=? AND log_id IN (3,4)`, q.SourceEpoch).Error; err != nil {
		t.Fatal(err)
	}
	verify(q)
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal("restart initialization", err)
	}
	verify(q)
	invalid := TokenDetectionCurrent{SourceEpoch: q.SourceEpoch, LogID: 9, CreatedAt: now - 1, SourceComplete: true, DetectionState: "evaluated", ResultJSON: "not-json"}
	if err := db.Create(&invalid).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := tokenReportInsufficientCount(db, q); err == nil {
		t.Fatal("invalid stored JSON became an all-clear")
	}
}

func TestTokenReportIndexesKeepEmptyDetectionStatePending(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	head := TokenDetectionCurrent{SourceEpoch: epoch, LogID: 901, CreatedAt: now - 10, Candidate: true, SourceComplete: true, DetectionState: "", ResultJSON: `{"candidate":true,"source_complete":true,"rules":[]}`}
	if err := db.Create(&head).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.Table(tokenReportIndexTable).Select("status").Where("source_epoch=? AND log_id=?", epoch, head.LogID).Scan(&status).Error; err != nil || status != "pending" {
		t.Fatalf("empty detection state indexed as %q: %v", status, err)
	}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: now - 60, To: now + 60, Status: "candidate"}
	var candidates int64
	if err := tokenDetectionRange(db, q).Count(&candidates).Error; err != nil || candidates != 0 {
		t.Fatalf("pending head entered candidate query: count=%d err=%v", candidates, err)
	}
	if err := db.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=?", epoch, head.LogID).Update("detection_state", "candidate").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Table(tokenReportIndexTable).Select("status").Where("source_epoch=? AND log_id=?", epoch, head.LogID).Scan(&status).Error; err != nil || status != "candidate" {
		t.Fatalf("committed detection state not indexed as candidate: %q %v", status, err)
	}
	if err := tokenDetectionRange(db, q).Count(&candidates).Error; err != nil || candidates != 1 {
		t.Fatalf("evaluated candidate missing from query: count=%d err=%v", candidates, err)
	}
	var triggerNames []string
	if err := db.Raw("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'token_report_entry_%' ORDER BY name").Pluck("name", &triggerNames).Error; err != nil {
		t.Fatal(err)
	}
	for _, name := range triggerNames {
		if strings.HasSuffix(name, "_v1") {
			t.Fatalf("stale report trigger survived upgrade: %s", name)
		}
	}
}

func TestTokenReportCountCoveringPlans(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	db := m.tokenReadStore()
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 2400, To: now, Status: "insufficient"}
	var statements []string
	var args [][]any
	capture := func(tx *gorm.DB) {
		sql := tx.Statement.SQL.String()
		if strings.HasPrefix(sql, "SELECT count(*) FROM `"+tokenReportIndexTable+"`") || strings.HasPrefix(sql, "SELECT (SELECT COALESCE(SUM(") {
			statements = append(statements, sql)
			args = append(args, append([]any(nil), tx.Statement.Vars...))
		}
	}
	if err := db.Callback().Row().After("gorm:row").Register("coverage_count_plans", capture); err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Query().After("gorm:query").Register("coverage_count_plans", capture); err != nil {
		t.Fatal(err)
	}
	_, countErr := tokenReportInsufficientCount(db, q)
	_, pendingErr := tokenReportPendingCount(db, q, tokenDetectionPolicyHash(detectionPolicy()))
	_ = db.Callback().Row().Remove("coverage_count_plans")
	_ = db.Callback().Query().Remove("coverage_count_plans")
	if countErr != nil || pendingErr != nil || len(statements) != 3 {
		t.Fatal("coverage queries", len(statements), countErr, pendingErr)
	}
	for i, statement := range statements {
		var plan []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN "+statement, args[i]...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		index := "idx_token_report_entry_count_v1"
		if strings.Contains(statement, tokenReportStatusCounts) {
			index = "PRIMARY KEY"
		}
		if strings.Contains(statement, "SUM(g.requests)") {
			index = "idx_token_report_pending_count_v1"
		}
		covered := false
		for _, row := range plan {
			t.Log(row.Detail)
			covered = covered || strings.Contains(row.Detail, "COVERING INDEX "+index) || index == "PRIMARY KEY" && strings.Contains(row.Detail, tokenReportStatusCounts+" USING PRIMARY KEY")
		}
		if !covered {
			t.Fatal("full-range count fetches wide rows", index, plan)
		}
	}
}

func TestTokenReportPendingHealthPlan(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	var statement string
	var args []any
	if err := m.tokenStore().Callback().Row().After("gorm:row").Register("pending_health_plan", func(tx *gorm.DB) {
		if strings.HasPrefix(tx.Statement.SQL.String(), "WITH prefix") {
			statement, args = tx.Statement.SQL.String(), append([]any(nil), tx.Statement.Vars...)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := tokenDetectionPendingSample(m.tokenReadStore(), m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now, 50)
	_ = m.tokenStore().Callback().Row().Remove("pending_health_plan")
	if err != nil {
		t.Fatal(err)
	}
	var plan []struct{ Detail string }
	if statement == "" {
		t.Fatal("health statement was not captured")
	}
	if err := m.tokenStore().Raw("EXPLAIN QUERY PLAN "+statement, args...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	covered := false
	for _, row := range plan {
		t.Log(row.Detail)
		covered = covered || strings.Contains(row.Detail, "COVERING INDEX idx_token_detection_discovery_v1")
	}
	if !covered || !strings.Contains(statement, "LIMIT 512") {
		t.Fatal("health discovery must use a bounded covering prefix", plan)
	}
}

func TestTokenReportPendingIndexTransactionsBoundariesAndRebuild(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	for id := int64(1); id <= 20; id++ {
		detectionFact(t, m, id, now-1801+id*73, 3, 20000, 0, 0)
	}
	verify := func(db *gorm.DB) {
		t.Helper()
		for _, q := range []tokenDetectionQuery{
			{SourceEpoch: epoch, From: now - 2400, To: now},
			{SourceEpoch: epoch, From: now - 1201, To: now - 421},
			{SourceEpoch: epoch, From: now - 1109, To: now - 1049},
			{SourceEpoch: epoch, From: now - 2400, To: now, UserSet: true, ChannelSet: true, ModelSet: true, GroupSet: true},
			{SourceEpoch: "other-epoch", From: now - 2400, To: now},
		} {
			want, err := tokenReportLegacyPendingCount(db, q, hash)
			if err != nil {
				t.Fatal(err)
			}
			got, err := tokenReportPendingCount(db, q, hash)
			if err != nil || got != want {
				t.Fatalf("pending=%d want=%d q=%+v err=%v", got, want, q, err)
			}
		}
		var facts, members, groups int64
		if err := db.Model(&TokenMeteringFact{}).Count(&facts).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table(tokenReportPendingMembers).Count(&members).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table(tokenReportPendingGroups).Select("COALESCE(SUM(requests),0)").Scan(&groups).Error; err != nil || facts != members || members != groups {
			t.Fatal("pending membership/counters drifted", facts, members, groups, err)
		}
	}
	verify(db)
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	verify(db)
	rollback := errors.New("rollback pending index")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`UPDATE token_metering_facts SET revision=revision+1 WHERE source_epoch=?`, epoch).Error; err != nil {
			return err
		}
		verify(tx)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	verify(db)
	for _, sql := range []string{
		`UPDATE token_metering_facts SET created_at=created_at+601,user_id=0,channel_id=0,model='',user_group='' WHERE log_id=3`,
		`UPDATE token_metering_facts SET source_epoch='other-epoch',log_id=99 WHERE log_id=4`,
		`UPDATE token_detection_currents SET log_id=199,source_epoch='other-epoch' WHERE log_id=5`,
		`UPDATE token_detection_currents SET reference_generation=NULL,policy_hash=NULL WHERE log_id=6`,
		`UPDATE token_detection_currents SET reference_bucket='changed',reference_generation=0 WHERE log_id=7`,
		`INSERT INTO token_detection_buckets(id,generation) VALUES ('changed',1)`,
		`DELETE FROM token_metering_facts WHERE log_id=1`,
		`DELETE FROM token_detection_currents WHERE log_id=2`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
		verify(db)
	}
	for _, source := range []string{"fact", "head", "retry"} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			if err := db.Exec(fmt.Sprintf("DROP TRIGGER token_report_pending_%s_%s_v1", source, event)).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Migrator().DropTable(tokenReportPendingMembers, tokenReportPendingGroups); err != nil {
		t.Fatal(err)
	}
	if _, err := tokenReportPendingCount(db, tokenDetectionQuery{SourceEpoch: epoch, From: now - 2400, To: now}, hash); err != nil {
		t.Fatal("legacy fallback", err)
	}
	if err := db.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal("populated-store rebuild", err)
	}
	verify(db)
	if err := db.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal("idempotent startup", err)
	}
	verify(db)
}

func TestTokenReportPendingMatchesLegacyForChangesAndNulls(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 8; id++ {
		detectionFact(t, m, id, now-3000+id*300, 3, 20000, 0, 0)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now); err != nil || n != 8 {
		t.Fatal(n, err)
	}
	epoch, hash := m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy())
	mutations := []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM token_detection_currents WHERE source_epoch=? AND log_id=1`, []any{epoch}},
		{`UPDATE token_metering_facts SET revision=revision+1 WHERE source_epoch=? AND log_id=2`, []any{epoch}},
		{`UPDATE token_metering_facts SET link_revision=link_revision+1,user_id=0,channel_id=0,model='',user_group='' WHERE source_epoch=? AND log_id=3`, []any{epoch}},
		{`UPDATE token_detection_currents SET policy_hash='different' WHERE source_epoch=? AND log_id=4`, []any{epoch}},
		{`UPDATE token_detection_currents SET reference_bucket='changed-bucket',reference_generation=0 WHERE source_epoch=? AND log_id=5`, []any{epoch}},
		{`UPDATE token_detection_currents SET source_complete=NULL WHERE source_epoch=? AND log_id=7`, []any{epoch}},
		{`UPDATE token_detection_currents SET policy_hash=NULL WHERE source_epoch=? AND log_id=8`, []any{epoch}},
	}
	for _, mutation := range mutations {
		if err := db.Exec(mutation.sql, mutation.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&TokenDetectionBucket{ID: "changed-bucket", Generation: 1}).Error; err != nil {
		t.Fatal(err)
	}
	at := (now - 3000 + 6*300) / 300 * 300
	if err := db.Save(&TokenMeteringWindow{SourceEpoch: epoch, StartAt: at, EndAt: at + 300, Done: true, SourceComplete: true}).Error; err != nil {
		t.Fatal(err)
	}
	base := tokenDetectionQuery{SourceEpoch: epoch, From: now - 3600, To: now}
	for _, q := range []tokenDetectionQuery{base, {SourceEpoch: epoch, From: base.From, To: base.To, UserSet: true, ChannelSet: true, ModelSet: true, GroupSet: true}, {SourceEpoch: "another-epoch", From: base.From, To: base.To}} {
		legacy := db.Table("token_metering_facts f").Joins(tokenDetectionHeadJoin(db)).Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").Where("f.source_epoch=? AND f.created_at>=? AND f.created_at<?", q.SourceEpoch, q.From, q.To)
		if q.UserSet {
			legacy = legacy.Where("f.user_id=0 AND f.channel_id=0 AND f.model='' AND f.user_group=''")
		}
		var want int64
		if err := legacy.Where(`c.log_id IS NULL OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=? OR c.reference_generation!=COALESCE(b.generation,0) OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END`, hash).Count(&want).Error; err != nil {
			t.Fatal(err)
		}
		got, err := tokenReportPendingCount(db, q, hash)
		if err != nil || got != want {
			t.Fatalf("pending=%d want=%d query=%+v err=%v", got, want, q, err)
		}
	}
}
