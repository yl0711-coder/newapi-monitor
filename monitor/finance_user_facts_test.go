package monitor

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestFetchFinanceUserHourFactsUsesAllUsersAndExcludesInternalTests(t *testing.T) {
	source, err := sql.Open("sqlite", t.TempDir()+"/finance-source.db")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Exec(`CREATE TABLE logs(
		id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,quota INTEGER,
		token_id INTEGER,token_name TEXT,request_id TEXT,content TEXT)`); err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		id, user, at, kind, quota, token int64
		tokenName, requestID, content    string
	}{
		{1, 7, 100, 2, 500000, 1, "customer", "req-1", "ok"},
		{2, 7, 200, 6, 100000, 1, "customer", "req-1", "refund"},
		{3, 8, 300, 2, 250000, 2, "customer-2", "req-2", "ok"},
		{4, 1, 400, 2, 900000, 0, "模型测试", "", "模型测试"},
		{5, 9, 3600, 2, 999999, 3, "next-hour", "req-3", "ok"},
	}
	for _, row := range rows {
		if _, err := source.Exec(`INSERT INTO logs VALUES(?,?,?,?,?,?,?,?,?)`, row.id, row.user, row.at, row.kind, row.quota, row.token, row.tokenName, row.requestID, row.content); err != nil {
			t.Fatal(err)
		}
	}
	facts, sourceRows, err := fetchFinanceUserHourFacts(context.Background(), source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sourceRows != 3 || len(facts) != 2 {
		t.Fatalf("sourceRows=%d facts=%+v", sourceRows, facts)
	}
	if facts[0].UserID != 7 || facts[0].Requests != 1 || facts[0].RefundRecords != 1 || facts[0].ConsumeQuota != 500000 || facts[0].RefundQuota != 100000 {
		t.Fatalf("user 7 fact=%+v", facts[0])
	}
	if facts[1].UserID != 8 || facts[1].ConsumeQuota != 250000 {
		t.Fatalf("user 8 fact=%+v", facts[1])
	}
	query := financeUserHourSQL()
	selectList := strings.Split(query, "FROM logs")[0]
	for _, forbidden := range []string{"prompt", "api_key", "request_id"} {
		if strings.Contains(strings.ToLower(selectList), forbidden) {
			t.Fatalf("finance query retains forbidden request data: %s", forbidden)
		}
	}
}

func TestFinanceFactsSchemaHasDedicatedMigrationPlan(t *testing.T) {
	for name, plan := range map[string]string{
		"base":     preMigrationPlanID,
		"combined": preMigrationCombinedPlanID,
	} {
		if !strings.Contains(plan, "v52") || !strings.Contains(plan, "finance-user-credit-gift-facts-v1") {
			t.Fatalf("%s migration plan does not protect the finance facts schema: %s", name, plan)
		}
	}
}

func TestReplaceFinanceUserHourFactsIsAtomicAndIdempotent(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:finance-user-facts?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&FinanceUserHourFact{}, &FinanceUserHourState{}); err != nil {
		t.Fatal(err)
	}
	hour := int64(3600)
	first := []FinanceUserHourFact{
		{HourTs: hour, UserID: 7, Requests: 2, ConsumeQuota: 500000},
		{HourTs: hour, UserID: 8, Requests: 1, RefundRecords: 1, ConsumeQuota: 250000, RefundQuota: 100000},
	}
	state, err := replaceFinanceUserHourFacts(context.Background(), db, hour, "source-v1", first, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if state.Rows != 2 || state.SourceRows != 4 || state.ConsumeQuota != 750000 || state.RefundQuota != 100000 || state.ContentHash == "" {
		t.Fatalf("state=%+v", state)
	}
	// A source replay with changed content replaces the full hour. The removed
	// user must not survive and totals must not be added twice.
	second := []FinanceUserHourFact{{HourTs: hour, UserID: 7, Requests: 1, ConsumeQuota: 300000}}
	replaced, err := replaceFinanceUserHourFacts(context.Background(), db, hour, "source-v1", second, 10_100)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Rows != 1 || replaced.SourceRows != 1 || replaced.ConsumeQuota != 300000 || replaced.ContentHash == state.ContentHash {
		t.Fatalf("replacement=%+v", replaced)
	}
	var facts []FinanceUserHourFact
	if err := db.Order("user_id").Find(&facts, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].UserID != 7 || facts[0].ConsumeQuota != 300000 {
		t.Fatalf("stored facts=%+v", facts)
	}
	var saved FinanceUserHourState
	if err := db.First(&saved, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if saved.ContentHash != replaced.ContentHash || saved.Status != "complete" {
		t.Fatalf("saved state=%+v", saved)
	}
}

func TestReplaceFinanceUserHourFactsFailsClosed(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:finance-user-invalid?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&FinanceUserHourFact{}, &FinanceUserHourState{}); err != nil {
		t.Fatal(err)
	}
	hour := int64(7200)
	valid := []FinanceUserHourFact{{HourTs: hour, UserID: 1, Requests: 1, ConsumeQuota: 10}}
	if _, err := replaceFinanceUserHourFacts(context.Background(), db, hour, "source-v1", valid, 20_000); err != nil {
		t.Fatal(err)
	}
	invalid := []FinanceUserHourFact{
		{HourTs: hour, UserID: 2, Requests: 1, ConsumeQuota: 20},
		{HourTs: hour, UserID: 2, Requests: 1, ConsumeQuota: 20},
	}
	if _, err := replaceFinanceUserHourFacts(context.Background(), db, hour, "source-v1", invalid, 20_100); err == nil {
		t.Fatal("duplicate user rows must fail closed")
	}
	var facts []FinanceUserHourFact
	if err := db.Find(&facts, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].UserID != 1 {
		t.Fatalf("invalid replacement changed published hour: %+v", facts)
	}
}
