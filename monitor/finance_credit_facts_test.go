package monitor

import (
	"context"
	"database/sql"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func financeCreditTestSource(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/credit-source.db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE logs(id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,content TEXT,other TEXT);
		CREATE TABLE users(id INTEGER PRIMARY KEY,created_at INTEGER);`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestFetchFinanceCreditHourClassifiesGiftEvidence(t *testing.T) {
	source := financeCreditTestSource(t)
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,100),(8,100),(9,100);
		INSERT INTO logs(id,user_id,created_at,type,content,other) VALUES
		(1,7,200,3,'管理员增加用户额度 ＄100.000000 额度',''),
		(2,1,300,3,'',?),
		(3,9,400,3,'管理员增加用户额度 ＄300.000000 额度',''),
		(4,7,500,3,'管理员强制禁用了用户的两步验证',''),
		(5,7,3600,3,'管理员增加用户额度 ＄100.000000 额度','')`,
		`{"op":{"action":"user.quota_override","params":{"from":"＄1.000000 额度","to":"＄151.000000 额度","target_user_id":8}}}`); err != nil {
		t.Fatal(err)
	}
	fetched, err := fetchFinanceCreditHour(context.Background(), source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.SourceRows != 4 || fetched.IgnoredRows != 1 || fetched.UnknownRegistrationRows != 0 || len(fetched.Events) != 3 {
		t.Fatalf("fetch=%+v", fetched)
	}
	if !fetched.Events[0].EligibleTrial || fetched.Events[0].TargetUserID != 7 || fetched.Events[0].NetChangeMicro != 100_000_000 {
		t.Fatalf("legacy gift=%+v", fetched.Events[0])
	}
	if !fetched.Events[1].EligibleTrial || fetched.Events[1].TargetUserID != 8 || fetched.Events[1].NetChangeMicro != 150_000_000 {
		t.Fatalf("structured gift=%+v", fetched.Events[1])
	}
	if fetched.Events[2].EligibleTrial || fetched.Events[2].TargetUserID != 9 {
		t.Fatalf("non-gift adjustment=%+v", fetched.Events[2])
	}
	for _, event := range fetched.Events {
		if event.EvidenceHash == "" || event.SourceEpoch != "" {
			t.Fatalf("normalized event=%+v", event)
		}
	}
}

func TestFetchFinanceCreditHourAcceptsRC26NegativeBalanceOverride(t *testing.T) {
	source := financeCreditTestSource(t)
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,100);
		INSERT INTO logs(id,user_id,created_at,type,content,other) VALUES(3337476,1,200,3,
		'Overrode user quota from ＄-0.955357 额度 to ＄9.044643 额度',
		'{"admin_info":{"admin_id":1,"admin_role":100,"admin_username":"operator","auth_method":"session"},"op":{"action":"user.quota_override","params":{"from":"＄-0.955357 额度","target_user_id":7,"to":"＄9.044643 额度"}}}')`); err != nil {
		t.Fatal(err)
	}
	fetched, err := fetchFinanceCreditHour(context.Background(), source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.SourceRows != 1 || fetched.IgnoredRows != 0 || len(fetched.Events) != 1 {
		t.Fatalf("fetch=%+v", fetched)
	}
	event := fetched.Events[0]
	if event.SourceLogID != 3337476 || event.TargetUserID != 7 || event.BeforeMicro != -955_357 || event.AfterMicro != 9_044_643 || event.NetChangeMicro != 10_000_000 || event.EligibleTrial {
		t.Fatalf("RC26 negative-balance override=%+v", event)
	}
}

func TestFetchFinanceCreditHourFailsOnChangedQuotaFormat(t *testing.T) {
	source := financeCreditTestSource(t)
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,100);
		INSERT INTO logs(id,user_id,created_at,type,content,other) VALUES(1,7,200,3,'管理员增加用户额度 新格式','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := fetchFinanceCreditHour(context.Background(), source, 0); err == nil {
		t.Fatal("changed quota producer format must fail closed")
	}
}

func TestFetchFinanceCreditHourDoesNotFallbackFromMalformedStructuredEvent(t *testing.T) {
	source := financeCreditTestSource(t)
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,100);
		INSERT INTO logs(id,user_id,created_at,type,content,other) VALUES(1,7,200,3,
		'管理员增加用户额度 ＄100.000000 额度',
		'{"op":{"action":"user.quota_add","params":{"quota":"changed-format","target_user_id":8}}}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := fetchFinanceCreditHour(context.Background(), source, 0); err == nil {
		t.Fatal("malformed structured quota event must not fall back to operator-owned legacy content")
	}
}

func TestFetchFinanceCreditHourMarksUnknownRegistration(t *testing.T) {
	source := financeCreditTestSource(t)
	if _, err := source.Exec(`INSERT INTO logs(id,user_id,created_at,type,content,other)
		VALUES(1,404,200,3,'管理员增加用户额度 ＄100.000000 额度','')`); err != nil {
		t.Fatal(err)
	}
	fetched, err := fetchFinanceCreditHour(context.Background(), source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.UnknownRegistrationRows != 1 || len(fetched.Events) != 1 || fetched.Events[0].EligibleTrial || fetched.Events[0].Cohort != "registration_time_unknown" {
		t.Fatalf("fetch=%+v", fetched)
	}
}

func TestReplaceFinanceCreditHourPublishesProofAndPreservesPriorOnFailure(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:finance-credit-facts?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&FinanceCreditEvent{}, &FinanceCreditHourState{}); err != nil {
		t.Fatal(err)
	}
	hour := int64(3600)
	event := FinanceCreditEvent{
		SourceLogID: 10, EventAt: hour + 10, TargetUserID: 7, UserCreatedAt: 100,
		Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000,
		Cohort: "trial_candidate_within_24h", EligibleTrial: true,
	}
	event.EvidenceHash = financeCreditEventHash(event)
	fetched := financeCreditHourFetch{Events: []FinanceCreditEvent{event}, SourceRows: 2, IgnoredRows: 1}
	state, err := replaceFinanceCreditHour(context.Background(), db, hour, 10_000, "source-v1", fetched)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.EventRows != 1 || state.EligibleTrialRows != 1 || state.ContentHash == "" {
		t.Fatalf("state=%+v", state)
	}
	var stored FinanceCreditEvent
	if err := db.First(&stored, "source_epoch=? AND source_log_id=?", "source-v1", 10).Error; err != nil {
		t.Fatal(err)
	}
	if stored.EvidenceHash != event.EvidenceHash || stored.SourceEpoch != "source-v1" {
		t.Fatalf("stored=%+v", stored)
	}
	bad := event
	bad.EvidenceHash = "tampered"
	if _, err := replaceFinanceCreditHour(context.Background(), db, hour, 10_100, "source-v1", financeCreditHourFetch{Events: []FinanceCreditEvent{bad}, SourceRows: 1}); err == nil {
		t.Fatal("tampered evidence must fail closed")
	}
	var count int64
	if err := db.Model(&FinanceCreditEvent{}).Where("source_epoch=? AND source_log_id=?", "source-v1", 10).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("prior publication changed count=%d err=%v", count, err)
	}
}

func TestReplaceFinanceCreditHourDoesNotPublishUnknownRegistration(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:finance-credit-unknown?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&FinanceCreditEvent{}, &FinanceCreditHourState{}); err != nil {
		t.Fatal(err)
	}
	event := FinanceCreditEvent{SourceLogID: 1, EventAt: 1, TargetUserID: 7, Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000, Cohort: "registration_time_unknown"}
	event.EvidenceHash = financeCreditEventHash(event)
	state, err := replaceFinanceCreditHour(context.Background(), db, 0, 10, "source-v1", financeCreditHourFetch{Events: []FinanceCreditEvent{event}, SourceRows: 1, UnknownRegistrationRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "incomplete" || state.UnknownRegistrationRows != 1 {
		t.Fatalf("state=%+v", state)
	}
}
