package monthlysource

import (
	"context"
	"reflect"
	"testing"
)

func TestBatchedTokenMonthMatchesEverySingleOriginalAndOldStore(t *testing.T) {
	db, _, key, scope, plan, expected := retainedTokenMonthCapture(t)
	// Reads of an older private store must remain supported without implicit
	// DDL. The new index is an optimization, not a new evidence requirement.
	if _, err := db.Exec("DROP INDEX monthly_token_capture_job_user"); err != nil {
		t.Fatal(err)
	}
	pairs, report, err := LoadCapturedTokenMonth(t.Context(), db, key, scope, 201)
	if err != nil || !reflect.DeepEqual(report, expected) {
		t.Fatal("batched old-store money differs", err)
	}
	for day, pair := range pairs {
		for phase, got := range []TokenUsageWindowObservation{pair.Older, pair.Newer} {
			original, err := loadTokenCaptureSlot(t.Context(), db, plan, day+phase*len(pairs))
			if err != nil || !reflect.DeepEqual(got, original) {
				t.Fatal("batched read changed a retained original", day, phase, err)
			}
		}
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='monthly_token_capture_job_user'").Scan(&count); err != nil || count != 0 {
		t.Fatal("old-store read created an index", count, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if pairs, _, err := LoadCapturedTokenMonth(ctx, db, key, scope, 201); err == nil || pairs != nil {
		t.Fatal("cancelled read returned a partial month", err)
	}
}

func TestBatchedTokenMonthRejectsProjectionAndOrdinalCorruption(t *testing.T) {
	for _, scenario := range []string{"projection_source", "projection_user", "projection_window", "projection_time", "projection_payload", "token_noncanonical", "usage_digest", "ordinal_hole", "foreign_user"} {
		t.Run(scenario, func(t *testing.T) {
			db, _, key, scope, _, _ := retainedTokenMonthCapture(t)
			for _, trigger := range []string{"monthly_token_capture_no_update", "monthly_usage_no_update"} {
				if _, err := db.Exec("DROP TRIGGER " + trigger); err != nil {
					t.Fatal(err)
				}
			}
			var query string
			switch scenario {
			case "projection_source":
				query = "UPDATE portal_monthly_usage_observations SET source_identity='foreign'"
			case "projection_user":
				query = "UPDATE portal_monthly_usage_observations SET user_id=999"
			case "projection_window":
				query = "UPDATE portal_monthly_usage_observations SET window_to=window_to+1"
			case "projection_time":
				query = "UPDATE portal_monthly_usage_observations SET completed_at_ms=completed_at_ms+1"
			case "projection_payload":
				query = "UPDATE portal_monthly_usage_observations SET payload=zeroblob(32768)"
			case "usage_digest":
				query = "UPDATE portal_monthly_token_capture_slots SET usage_id=sha256 WHERE ordinal=0"
			case "ordinal_hole":
				query = "UPDATE portal_monthly_token_capture_slots SET ordinal=100 WHERE ordinal=0"
			case "foreign_user":
				query = "UPDATE portal_monthly_token_capture_slots SET user_id=999 WHERE ordinal=0"
			case "token_noncanonical":
				var raw []byte
				if err := db.QueryRow("SELECT payload FROM portal_monthly_token_capture_slots WHERE ordinal=0").Scan(&raw); err != nil {
					t.Fatal(err)
				}
				raw = append(raw, ' ')
				if _, err := db.Exec("UPDATE portal_monthly_token_capture_slots SET payload=?,sha256=? WHERE ordinal=0", raw, digest(raw)); err != nil {
					t.Fatal(err)
				}
			}
			if query != "" {
				if _, err := db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			pairs, _, err := LoadCapturedTokenMonth(t.Context(), db, key, scope, 201)
			if err == nil || pairs != nil {
				t.Fatal("corrupt month was accepted or returned partial money", scenario, err)
			}
		})
	}
}
