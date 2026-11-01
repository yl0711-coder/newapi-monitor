package monthlysource

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func fundingMonthCaptureFixture(t *testing.T, source string) (*sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan) {
	t.Helper()
	receiver, path := fundingTestStore(t)
	if err := InitFundingMonthCaptureStore(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("201,202,"))
	scope := OrganizationMonthScope{Version: 1, Mode: OrganizationMonthScopeMode, GroupID: 1, AuthVersion: 1,
		MemberFingerprint: hex.EncodeToString(sum[:16]), MemberManifest: strings.Repeat("b", 64),
		Funding: FundingMonthRequest{Month: "2026-09", SourceIdentity: source, Rules: fundingTestRules(), UserIDs: []int64{201, 202}}}
	key := digest([]byte("synthetic authority-bound job"))
	plan, err := EnsureFundingMonthCapture(context.Background(), receiver, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	return receiver, path, key, scope, plan
}

func TestFundingMonthCapturePlanDurableIdentityAndConcurrentReuse(t *testing.T) {
	receiver, path, key, scope, plan := fundingMonthCaptureFixture(t, strings.Repeat("a", 64))
	if len(plan.Scans) != 60 || plan.Scans[0].From != 1788192000 || plan.Scans[29].To != 1790784000 {
		t.Fatal("wrong Beijing month", plan.Scans[0])
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := EnsureFundingMonthCapture(context.Background(), reopened, key, scope)
			if err == nil && !reflect.DeepEqual(p, plan) {
				err = errors.New("scan IDs changed after restart")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []string{`DELETE FROM portal_monthly_funding_capture_plans`, `UPDATE portal_monthly_funding_capture_plans SET sha256=''`} {
		if _, err := reopened.Exec(query); err == nil {
			t.Fatal("plan mutable")
		}
	}
	for _, kind := range []string{"auth", "member_manifest", "source", "month", "group"} {
		s := scope
		switch kind {
		case "auth":
			s.AuthVersion++
		case "member_manifest":
			s.MemberManifest = strings.Repeat("c", 64)
		case "source":
			s.Funding.SourceIdentity = strings.Repeat("d", 64)
		case "month":
			s.Funding.Month = "2026-08"
		case "group":
			s.GroupID++
		}
		if _, err := EnsureFundingMonthCapture(context.Background(), reopened, key, s); !errors.Is(err, ErrFundingMonthCaptureIntegrity) {
			t.Fatal("old plan reused across changed scope", kind, err)
		}
	}
}

func TestFundingMonthCaptureOnePageRealCollectorAndRestart(t *testing.T) {
	source, d, scan := fundingTestSource(t, 201)
	receiver, path, key, scope, plan := fundingMonthCaptureFixture(t, scan.SourceIdentity)
	for i := range d.s.data {
		d.s.data[i][2] = plan.Scans[0].From + 10
	}
	ctx := context.Background()
	first, err := CaptureNextFundingMonthPage(ctx, source, receiver, key, scope)
	if err != nil || first.FinishedScans != 0 || first.NextScanID != plan.Scans[0].ID || first.TraversalsFinished {
		t.Fatal(first, err)
	}
	head, err := LoadLatestFundingPage(ctx, receiver, plan.Scans[0].ID)
	if err != nil || head.Next.ID != 200 || head.EndReached {
		t.Fatal(head, err)
	}
	if !d.s.options.ReadOnly || d.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || source.Stats().InUse != 0 {
		t.Fatal("not a released read-only source transaction")
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
	d.s.mu.Lock()
	d.s.data = d.s.data[200:]
	d.s.queries = nil
	d.s.args = nil
	d.fence = 9999
	d.s.mu.Unlock()
	second, err := CaptureNextFundingMonthPage(ctx, source, receiver, key, scope)
	if err != nil || second.FinishedScans != 1 || second.NextScanID != plan.Scans[1].ID {
		t.Fatal(second, err)
	}
	head, err = LoadLatestFundingPage(ctx, receiver, plan.Scans[0].ID)
	if err != nil || !head.EndReached || head.Next.ID != 201 || head.FenceID != 1000 {
		t.Fatal("cursor/fence changed", head, err)
	}
	d.s.mu.Lock()
	defer d.s.mu.Unlock()
	if len(d.s.queries) != 2 || d.s.args[1][3].Value != plan.Scans[0].From+10 || d.s.args[1][5].Value != int64(200) {
		t.Fatal("did not resume exact cursor", d.s.args)
	}
}

func TestFundingMonthCaptureFailuresDoNotAdvanceOrRetry(t *testing.T) {
	for _, scenario := range []string{"source", "local_write", "wrong_source", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			source, d, scan := fundingTestSource(t, 1)
			receiver, _, key, scope, plan := fundingMonthCaptureFixture(t, scan.SourceIdentity)
			d.s.data[0][2] = plan.Scans[0].From + 10
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "source":
				d.s.queryErr = errors.New("synthetic source error")
			case "local_write":
				if _, err := receiver.Exec(`CREATE TRIGGER reject_page BEFORE INSERT ON portal_monthly_funding_pages BEGIN SELECT RAISE(ABORT,'synthetic disk error'); END`); err != nil {
					t.Fatal(err)
				}
			case "wrong_source":
				d.s.uuid = "other-server"
			case "cancel":
				cancel()
			}
			if _, err := CaptureNextFundingMonthPage(ctx, source, receiver, key, scope); err == nil {
				t.Fatal("bad capture accepted")
			}
			if _, err := LoadLatestFundingPage(context.Background(), receiver, plan.Scans[0].ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("failure advanced local cursor", err)
			}
			if len(d.s.queries) > 2 {
				t.Fatal("source failure retried", len(d.s.queries))
			}
			if scenario == "wrong_source" && len(d.s.queries) != 1 {
				t.Fatal("body read before identity checked")
			}
			p, err := EnsureFundingMonthCapture(context.Background(), receiver, key, scope)
			if err != nil || !reflect.DeepEqual(p, plan) {
				t.Fatal("failure regenerated plan", err)
			}
		})
	}
}

func saveEmptyFundingCapture(t *testing.T, receiver, source *sql.DB, scan FundingScan, at time.Time) {
	t.Helper()
	calls := 0
	page, err := captureFundingPage(context.Background(), source, scan, nil, func() time.Time { calls++; return at.Add(time.Duration(calls) * time.Millisecond) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SaveFundingPage(context.Background(), receiver, page); err != nil {
		t.Fatal(err)
	}
}

func TestFundingMonthCaptureFullOrderedSweepsRequireAuditAndAvoidRecollection(t *testing.T) {
	source, d, scan := fundingTestSource(t, 0)
	receiver, path, key, scope, plan := fundingMonthCaptureFixture(t, scan.SourceIdentity)
	base := time.Now().Add(-time.Hour)
	for i, s := range plan.Scans {
		saveEmptyFundingCapture(t, receiver, source, s, base.Add(time.Duration(i)*time.Second))
	}
	progress, err := InspectFundingMonthCapture(context.Background(), receiver, key, scope)
	if err != nil || !progress.TraversalsFinished || progress.FinishedScans != 60 || progress.Mode != "funding_traversals_not_bill_approval" {
		t.Fatal(progress, err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	d.s.mu.Lock()
	queries := len(d.s.queries)
	d.s.queryErr = errors.New("must not recollect")
	d.s.mu.Unlock()
	if _, err := CaptureNextFundingMonthPage(context.Background(), source, receiver, key, scope); err != nil {
		t.Fatal(err)
	}
	d.s.mu.Lock()
	afterQueries := len(d.s.queries)
	d.s.mu.Unlock()
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) || afterQueries != queries {
		t.Fatal("finished collection wrote or reread source", err)
	}
	// Real original chains, not completion flags, must independently pass the
	// existing comparison and full-month coverage rules.
	var windows []FundingWindowObservation
	for i := 0; i < 30; i++ {
		w, err := ReviewFundingWindow(context.Background(), receiver, plan.Scans[i].ID, plan.Scans[i+30].ID)
		if err != nil {
			t.Fatal(err)
		}
		windows = append(windows, w)
	}
	review, err := ReviewFundingMonth(context.Background(), scope.Funding, windows)
	if err != nil || len(review.MissingWindows) != 0 || len(review.Windows) != 30 || len(review.Accounts) != 2 {
		t.Fatal(review, err)
	}
	for _, account := range review.Accounts {
		if account.MatchingSeconds != 30*86400 {
			t.Fatal("missing coverage", account)
		}
	}
}

func TestFundingMonthCaptureRejectsHolesMixedSweepsAndAlteredPlans(t *testing.T) {
	for _, scenario := range []string{"hole", "early_newer", "changed_scan", "duplicate_id", "unknown_field"} {
		t.Run(scenario, func(t *testing.T) {
			source, _, scan := fundingTestSource(t, 0)
			receiver, _, key, scope, plan := fundingMonthCaptureFixture(t, scan.SourceIdentity)
			switch scenario {
			case "hole":
				saveEmptyFundingCapture(t, receiver, source, plan.Scans[1], time.Now().Add(-time.Hour))
			case "early_newer":
				saveEmptyFundingCapture(t, receiver, source, plan.Scans[30], time.Now().Add(-time.Hour))
			default:
				if scenario == "changed_scan" {
					plan.Scans[1].From++
				} else if scenario == "duplicate_id" {
					plan.Scans[1].ID = plan.Scans[0].ID
				}
				raw, _ := json.Marshal(plan)
				if scenario == "unknown_field" {
					raw = append(raw[:len(raw)-1], []byte(`,"complete":true}`)...)
				}
				if _, err := decodeFundingMonthCapturePlan(raw, key, scope, time.Now()); !errors.Is(err, ErrFundingMonthCaptureIntegrity) {
					t.Fatal("altered plan accepted", err)
				}
				return
			}
			if _, err := InspectFundingMonthCapture(context.Background(), receiver, key, scope); !errors.Is(err, ErrFundingMonthCaptureIntegrity) {
				t.Fatal("invalid traversal order accepted", err)
			}
		})
	}
}

func TestFundingMonthCaptureCapacityDoesNotBreakExistingJob(t *testing.T) {
	receiver, _, key, scope, plan := fundingMonthCaptureFixture(t, strings.Repeat("a", 64))
	tx, err := receiver.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 255; i++ {
		k := sha256.Sum256([]byte{byte(i)})
		if _, err := tx.Exec(`INSERT INTO portal_monthly_funding_capture_plans(key,sha256,payload) VALUES(?,?,?)`, hex.EncodeToString(k[:]), strings.Repeat("a", 64), []byte(`{}`)); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureFundingMonthCapture(context.Background(), receiver, digest([]byte("new job")), scope); !errors.Is(err, ErrFundingMonthCaptureCapacity) {
		t.Fatal("capacity bypassed", err)
	}
	p, err := EnsureFundingMonthCapture(context.Background(), receiver, key, scope)
	if err != nil || !reflect.DeepEqual(p, plan) {
		t.Fatal("existing plan broken at capacity", err)
	}
}

func TestFundingMonthCaptureConcurrentCreationAndCalendar(t *testing.T) {
	receiver, _, _, scope, _ := fundingMonthCaptureFixture(t, strings.Repeat("a", 64))
	for _, tc := range []struct {
		month string
		days  int
	}{{"2026-02", 28}, {"2024-02", 29}, {"2025-12", 31}} {
		s := scope
		s.Funding.Month = tc.month
		key := digest([]byte(tc.month))
		var wg sync.WaitGroup
		plans := make(chan FundingMonthCapturePlan, 4)
		errs := make(chan error, 4)
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				plan, err := EnsureFundingMonthCapture(context.Background(), receiver, key, s)
				plans <- plan
				errs <- err
			}()
		}
		wg.Wait()
		close(plans)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		var expected FundingMonthCapturePlan
		for plan := range plans {
			if len(plan.Scans) != tc.days*2 {
				t.Fatal("wrong calendar coverage", tc.month, len(plan.Scans))
			}
			if expected.Key == "" {
				expected = plan
			} else if !reflect.DeepEqual(plan, expected) {
				t.Fatal("competing creators produced different durable plans")
			}
		}
	}
}
