package monthlysource

import (
	"bytes"
	"strings"
	"testing"
)

func TestTokenCaptureAccountIndexPreservesOriginalsAndUsesBothKeys(t *testing.T) {
	source, driver, request := tokenUsageSource(t)
	db, _, key, scope, plan := tokenCaptureFixture(t, request.SourceIdentity)
	tokenCaptureRowsForDay(driver, plan.Scans[0].From, false)
	if _, err := CaptureNextTokenMonthSlot(t.Context(), source, db, key, scope); err != nil {
		t.Fatal(err)
	}
	var before []byte
	if err := db.QueryRow("SELECT payload FROM portal_monthly_token_capture_slots WHERE job_key=? AND ordinal=0", key).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Model an existing private capture store lacking the additive index.
	if _, err := db.Exec("DROP INDEX monthly_token_capture_job_user"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := InitTokenMonthCaptureStore(t.Context(), db); err != nil {
			t.Fatal(err)
		}
	}
	var after []byte
	if err := db.QueryRow("SELECT payload FROM portal_monthly_token_capture_slots WHERE job_key=? AND ordinal=0", key).Scan(&after); err != nil || !bytes.Equal(before, after) {
		t.Fatal("index initialization changed the original capture", err)
	}
	rows, err := db.Query("EXPLAIN QUERY PLAN "+accountTokenCaptureSlotsQuery, key, scope.Funding.UserIDs[0], len(plan.Scans)+1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	indexed := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		indexed = indexed || strings.Contains(detail, "monthly_token_capture_job_user") && strings.Contains(detail, "job_key=?") && strings.Contains(detail, "user_id=?")
	}
	if err := rows.Err(); err != nil || !indexed {
		t.Fatal("per-account audit does not use both index keys", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE portal_monthly_token_capture_slots SET payload=payload WHERE job_key=?", key); err == nil {
		t.Fatal("index initialization removed append-only protection")
	}
}

func TestTokenCaptureReaderNeverCreatesMissingAccountIndex(t *testing.T) {
	db, _, key, scope, _ := tokenCaptureFixture(t, strings.Repeat("a", 64))
	if _, err := db.Exec("DROP INDEX monthly_token_capture_job_user"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadCapturedTokenMonth(t.Context(), db, key, scope, scope.Funding.UserIDs[0]); err == nil {
		t.Fatal("incomplete original captures were accepted")
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='monthly_token_capture_job_user'").Scan(&count); err != nil || count != 0 {
		t.Fatal("read path initialized a missing index", count, err)
	}
}
