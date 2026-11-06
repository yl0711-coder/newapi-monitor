package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func tokenCaptureFixture(t *testing.T, pin string) (*sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan) {
	t.Helper()
	db, path, _, scope, _ := fundingMonthCaptureFixture(t, pin)
	if err := InitTokenMonthCaptureStore(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	scope.Funding.UserIDs = []int64{201}
	scope.MemberFingerprint = digest([]byte("201,"))[:32]
	key := digest([]byte("synthetic token preparation"))
	plan, err := EnsureFundingMonthCapture(context.Background(), db, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	return db, path, key, scope, plan
}

func tokenCaptureRowsForDay(d *tokenUsageTestDriver, from int64, empty bool) {
	if empty {
		d.usage.data = [][]driver.Value{}
		d.data = [][]driver.Value{}
		return
	}
	row := usageTestRow(2, "wallet", "1500000", 1500000)
	row[10], row[11] = from+10, from+10
	d.usage.data = [][]driver.Value{row}
	d.data = [][]driver.Value{append(append([]driver.Value{}, row...), int64(11), "synthetic token")}
}

func fillTokenOlderSweep(t *testing.T, source, receiver *sql.DB, d *tokenUsageTestDriver, plan FundingMonthCapturePlan, at time.Time, empty bool) {
	t.Helper()
	for i := 0; i < 30; i++ {
		req, err := tokenCaptureRequest(plan, i)
		if err != nil {
			t.Fatal(err)
		}
		tokenCaptureRowsForDay(d, req.From, empty)
		o, err := captureTokenUsageWindow(context.Background(), source, req, func() time.Time { return at.Add(time.Duration(i) * time.Millisecond) })
		if err != nil {
			t.Fatal(err)
		}
		if err := saveTokenCaptureSlot(context.Background(), receiver, plan, i, o); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTokenMonthCaptureAtomicProjectionRestartAndOneSlot(t *testing.T) {
	source, d, req := tokenUsageSource(t)
	receiver, path, key, scope, plan := tokenCaptureFixture(t, req.SourceIdentity)
	tokenCaptureRowsForDay(d, plan.Scans[0].From, false)
	progress, err := CaptureNextTokenMonthSlot(context.Background(), source, receiver, key, scope)
	if err != nil || progress.Completed != 1 || progress.Total != 60 || !progress.Captured || progress.TraversalsFinished {
		t.Fatal(progress, err)
	}
	if source.Stats().InUse != 0 || receiver.Stats().InUse != 0 || !d.usage.s.options.ReadOnly || len(d.usage.s.queries) != 4 {
		t.Fatal("source transaction retained or widened")
	}
	o, err := loadTokenCaptureSlot(context.Background(), receiver, plan, 0)
	if err != nil || o.Rows[0].Quota != 1500000 || o.Totals[0].Quota != 1500000 {
		t.Fatal(o, err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	loaded, err := loadTokenCaptureSlot(context.Background(), receiver, plan, 0)
	if err != nil || !reflect.DeepEqual(loaded, o) {
		t.Fatal("restart lost original observation", err)
	}
	// A new day has no records, not an inferred zero from a missing day.
	tokenCaptureRowsForDay(d, plan.Scans[1].From, true)
	progress, err = CaptureNextTokenMonthSlot(context.Background(), source, receiver, key, scope)
	if err != nil || progress.Completed != 2 {
		t.Fatal(progress, err)
	}
	empty, err := loadTokenCaptureSlot(context.Background(), receiver, plan, 1)
	if err != nil || len(empty.Rows) != 0 || empty.Rows == nil {
		t.Fatal("empty observation missing", empty, err)
	}
}

func TestTokenMonthCaptureIndependentWaitAndMismatchRetained(t *testing.T) {
	for _, scenario := range []string{"wait", "match", "changed"} {
		t.Run(scenario, func(t *testing.T) {
			source, d, req := tokenUsageSource(t)
			receiver, path, key, scope, plan := tokenCaptureFixture(t, req.SourceIdentity)
			at := time.Now().Add(-10 * time.Minute)
			if scenario == "wait" {
				at = time.Now().Add(-time.Minute)
			}
			fillTokenOlderSweep(t, source, receiver, d, plan, at, true)
			tokenCaptureRowsForDay(d, plan.Scans[0].From, scenario != "changed")
			queries := len(d.usage.s.queries)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			progress, err := CaptureNextTokenMonthSlot(context.Background(), source, receiver, key, scope)
			switch scenario {
			case "wait":
				if err != nil || progress.NextEligibleMS <= time.Now().UnixMilli() || progress.Captured || progress.Completed != 30 || len(d.usage.s.queries) != queries {
					t.Fatal("independent wait queried source", progress, err)
				}
				after, err := os.ReadFile(path)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("wait wrote database", err)
				}
			case "match":
				if err != nil || progress.Completed != 31 || !progress.Captured {
					t.Fatal(progress, err)
				}
			case "changed":
				if !errors.Is(err, ErrTokenMonthCaptureIntegrity) {
					t.Fatal("changed consumption accepted", progress, err)
				}
				// Keep the conflicting original and projection for internal analysis;
				// never replace it with another reread that happens to look better.
				if _, err := loadTokenCaptureSlot(context.Background(), receiver, plan, 30); err != nil {
					t.Fatal("changed original discarded", err)
				}
				queries = len(d.usage.s.queries)
				if _, err := CaptureNextTokenMonthSlot(context.Background(), source, receiver, key, scope); !errors.Is(err, ErrTokenMonthCaptureIntegrity) || len(d.usage.s.queries) != queries {
					t.Fatal("mismatch recollected", err)
				}
			}
		})
	}
}

func TestTokenMonthCaptureAtomicWriteFailureAndSourceFailure(t *testing.T) {
	for _, scenario := range []string{"projection", "slot", "source", "wrong_source", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			source, d, req := tokenUsageSource(t)
			receiver, _, key, scope, plan := tokenCaptureFixture(t, req.SourceIdentity)
			tokenCaptureRowsForDay(d, plan.Scans[0].From, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "projection":
				if _, err := receiver.Exec(`CREATE TRIGGER fail_projection BEFORE INSERT ON portal_monthly_usage_observations BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "slot":
				if _, err := receiver.Exec(`CREATE TRIGGER fail_slot BEFORE INSERT ON portal_monthly_token_capture_slots BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "source":
				d.queryErr = errors.New("synthetic token query error")
			case "wrong_source":
				d.usage.s.uuid = "other-source"
			case "cancel":
				cancel()
			}
			if _, err := CaptureNextTokenMonthSlot(ctx, source, receiver, key, scope); err == nil {
				t.Fatal("bad capture accepted")
			}
			for _, q := range []string{`SELECT count(*) FROM portal_monthly_token_capture_slots`, `SELECT count(*) FROM portal_monthly_usage_observations`} {
				var n int
				if err := receiver.QueryRow(q).Scan(&n); err != nil || n != 0 {
					t.Fatal("partial original/projection committed", n, err)
				}
			}
			if len(d.usage.s.queries) > 4 {
				t.Fatal("source failure retried")
			}
		})
	}
}

func retainedTokenMonthCapture(t *testing.T) (*sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan, TokenUsageMonthReport) {
	t.Helper()
	req, pairs, now := tokenMonthFixture()
	// The original pure-archive fixture needs only per-pair separation. The
	// automatic collector additionally waits after the WHOLE older sweep.
	// Adjust synthetic test clocks only; keep all amounts and original rules.
	for i := range pairs {
		pairs[i].Newer.StartedAtMS += 30000
		pairs[i].Newer.CompletedAtMS += 30000
		// Automatic source-assembly fixtures represent a present source user.
		// The pure token archive fixture deliberately leaves wallet existence
		// unknown, which the separate user-total audit correctly rejects.
		for _, o := range []*TokenUsageWindowObservation{&pairs[i].Older, &pairs[i].Newer} {
			balance, used := int64(0), int64(0)
			o.Wallet = WalletUser{UserID: o.Request.UserID, Exists: true, BalanceQuota: &balance, UsedQuota: &used}
		}
	}
	receiver, path, key, scope, plan := tokenCaptureFixture(t, req.SourceIdentity)
	for phase := 0; phase < 2; phase++ {
		for i, pair := range pairs {
			o := pair.Older
			if phase == 1 {
				o = pair.Newer
			}
			if err := saveTokenCaptureSlot(context.Background(), receiver, plan, phase*30+i, o); err != nil {
				t.Fatal(err)
			}
		}
	}
	report, err := SummarizeTokenUsageMonth(context.Background(), req, pairs, now)
	if err != nil {
		t.Fatal(err)
	}
	return receiver, path, key, scope, plan, report
}

func TestTokenMonthCaptureMonthMoneyIDsRefundsNamesAndReadOnly(t *testing.T) {
	receiver, path, key, scope, _, expected := retainedTokenMonthCapture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		pairs, report, err := LoadCapturedTokenMonth(context.Background(), receiver, key, scope, 201)
		if err != nil || len(pairs) != 30 || !reflect.DeepEqual(report, expected) {
			t.Fatal("original month money changed", report, expected, err)
		}
		if report.ConsumeQuota != 1000006 || report.RefundQuota != 2 || len(report.Rows) != 3 || report.Rows[0].TokenID != 0 || report.Rows[1].Name != "renamed" {
			t.Fatal("lost refund/unknown ID/renamed token", report)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("read modified retained source", err)
	}
	if _, _, err := LoadCapturedTokenMonth(context.Background(), receiver, key, scope, 999); !errors.Is(err, ErrTokenMonthCaptureIntegrity) {
		t.Fatal("foreign account selected", err)
	}
}

func TestTokenMonthCaptureRejectsMissingOriginalProjectionAndMetadataHoles(t *testing.T) {
	for _, scenario := range []string{"empty_day", "projection", "wrong_metadata"} {
		t.Run(scenario, func(t *testing.T) {
			receiver, _, key, scope, _, _ := retainedTokenMonthCapture(t)
			switch scenario {
			case "empty_day":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_token_capture_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(`DELETE FROM portal_monthly_token_capture_slots WHERE ordinal=10`); err != nil {
					t.Fatal(err)
				}
			case "projection":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_usage_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(`DELETE FROM portal_monthly_usage_observations WHERE id=(SELECT usage_id FROM portal_monthly_token_capture_slots WHERE ordinal=0)`); err != nil {
					t.Fatal(err)
				}
			case "wrong_metadata":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_token_capture_no_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(`UPDATE portal_monthly_token_capture_slots SET sha256=? WHERE ordinal=0`, strings.Repeat("0", 64)); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := LoadCapturedTokenMonth(context.Background(), receiver, key, scope, 201); err == nil {
				t.Fatal("damaged month read as zero/complete")
			}
		})
	}
}

func TestTokenMonthCaptureByteBudgetStopsBeforeSourceAndRejectsAppend(t *testing.T) {
	source, d, req := tokenUsageSource(t)
	receiver, _, key, scope, plan := fundingMonthCaptureFixture(t, req.SourceIdentity)
	if err := InitTokenMonthCaptureStore(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	// Synthetic damaged originals are not accepted as evidence: only LENGTH
	// metadata is used to prove that the guard stops BEFORE loading/capturing
	// another payload. SQLite creates these blobs without a 64-MiB Go object.
	_, err := receiver.Exec(`WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<31)
INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload)
SELECT ?,i,201,?,?,1,2,zeroblob(2097152) FROM n`, key, strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureNextTokenMonthSlot(context.Background(), source, receiver, key, scope); !errors.Is(err, ErrTokenMonthCaptureBudget) {
		t.Fatal("full capture budget ignored", err)
	}
	if len(d.usage.s.queries) != 0 {
		t.Fatal("full capture queried source")
	}
	want, err := tokenCaptureRequest(plan, 32)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Second).UnixMilli()
	o := TokenUsageWindowObservation{Version: 1, Basis: tokenUsageBasis, Request: want, StartedAtMS: now, CompletedAtMS: now,
		Wallet: WalletUser{UserID: want.UserID}, Totals: []UsageWindowRow{}, Rows: []TokenUsageWindowRow{}}
	if err := saveTokenCaptureSlot(context.Background(), receiver, plan, 32, o); !errors.Is(err, ErrTokenMonthCaptureBudget) {
		t.Fatal("append exceeded atomic byte guard", err)
	}
	var count int
	if err := receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_capture_slots`).Scan(&count); err != nil || count != 32 {
		t.Fatal("budget failure advanced slot", count, err)
	}
	if err := receiver.QueryRow(`SELECT count(*) FROM portal_monthly_usage_observations`).Scan(&count); err != nil || count != 0 {
		t.Fatal("budget failure saved partial projection", count, err)
	}
}
