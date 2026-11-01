package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Test-only multi-account view of the same SQL protocol. The funding fixture
// has an actor-owned type-3 row; it remains visible for every requested user.
type tailRosterDriver struct{ base *walletTailTestDriver }
type tailRosterConn struct{ *walletTailTestConn }
type tailRosterRows struct {
	driver.Rows
	user int64
}

func (d *tailRosterDriver) Open(string) (driver.Conn, error) {
	return &tailRosterConn{&walletTailTestConn{walletConn: &walletConn{d.base.s}, d: d.base}}, nil
}

func (c *tailRosterConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	usage, funding := strings.Contains(q, "WITH usage_keys AS"), strings.Contains(q, "AS funding_candidates")
	if usage || funding {
		// Expand only exact singleton source fixtures into raw rows, then let
		// the bounded-query fixture interpret the actual SQL and cursor args.
		// Never label a full-month aggregate as a completed bounded page.
		base := c.d
		oldQuery := walletTailUsageQuery
		if funding {
			oldQuery = walletTailFundingQuery
		}
		if base.failQuery == oldQuery {
			return nil, errors.New("synthetic source failure")
		}
		d := &pageTailDriver{s: base.s}
		user := args[0].Value.(int64)
		if funding {
			if len(args) == 5 {
				user, d.r.Wallet.Boundary = args[1].Value.(int64), args[0].Value.(int64)
			} else {
				user = args[3].Value.(int64)
			}
		} else if len(args) == 6 {
			d.r.Wallet.Boundary = args[1].Value.(int64)
		}
		for _, row := range base.usage {
			if row[3] != int64(1) || row[4] != int64(1) || row[5] != fmt.Sprint(row[6]) || row[6] != row[7] || row[8] != row[9] || row[10] != row[11] {
				return nil, errors.New("roster fixture requires exact singleton usage rows")
			}
			d.items = append(d.items, pageTailItem{id: row[8].(int64), at: row[10].(int64), typ: row[1].(int64), user: user, quota: row[6].(int64), billing: row[2].(string)})
		}
		for _, row := range base.funding {
			d.items = append(d.items, pageTailItem{id: row[0].(int64), user: row[1].(int64), at: row[2].(int64), typ: row[3].(int64), content: row[4].(string), other: row[5].(string)})
		}
		rows, err := (&pageTailConn{walletConn: c.walletConn, d: d}).QueryContext(ctx, q, args)
		if err == nil && base.failRows == oldQuery {
			rows.(*walletRows).err = errors.New("synthetic row transfer failure")
		}
		return rows, err
	}
	rows, err := c.walletTailTestConn.QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	if q == walletTailUsageQuery || strings.Contains(q, "id,created_at,quota,used_quota FROM users") {
		return &tailRosterRows{Rows: rows, user: args[0].Value.(int64)}, nil
	}
	return rows, nil
}

func (r *tailRosterRows) Next(dst []driver.Value) error {
	err := r.Rows.Next(dst)
	if err == nil {
		dst[0] = r.user
	}
	return err
}

func tailCaptureFixture(t *testing.T) (*sql.DB, *walletTailTestDriver, *sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan, time.Time) {
	t.Helper()
	_, d, req, now := walletTailTestSource(t)
	name := fmt.Sprintf("tail-roster-%d", walletDriverSequence.Add(1))
	sql.Register(name, &tailRosterDriver{base: d})
	source, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	source.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = source.Close() })
	receiver, file, key, scope, plan := fundingMonthCaptureFixture(t, req.SourceIdentity)
	if err := InitWalletTailCaptureStore(t.Context(), receiver); err != nil {
		t.Fatal(err)
	}
	return source, d, receiver, file, key, scope, plan, now
}

func tailCaptureStep(t *testing.T, source, receiver *sql.DB, key string, scope OrganizationMonthScope, warm bool, now time.Time) WalletTailCaptureProgress {
	t.Helper()
	out, err := captureNextWalletTailMonthView(t.Context(), source, receiver, key, scope, warm, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWalletTailCaptureDurablePairWaitRestartAndOfflineReuse(t *testing.T) {
	source, d, receiver, file, key, scope, _, now := tailCaptureFixture(t)
	for i := 1; i <= 2; i++ {
		p := tailCaptureStep(t, source, receiver, key, scope, true, now)
		if !p.Captured || p.Completed != i || p.FirstViews != i || p.TotalViews != 4 || source.Stats().InUse != 0 || receiver.Stats().InUse != 0 {
			t.Fatal("one step must commit one complete view and release both handles", p)
		}
	}
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	queries := len(d.s.queries)
	p := tailCaptureStep(t, source, receiver, key, scope, false, now)
	if p.Captured || p.Completed != 2 || p.NextEligibleMS != now.UnixMilli()+300000 || len(d.s.queries) != queries {
		t.Fatal("independent wait recaptured source", p)
	}
	after, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("waiting mutated persisted originals", err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", file+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	now = now.Add(5 * time.Minute)
	for i := 3; i <= 4; i++ {
		p = tailCaptureStep(t, source, reopened, key, scope, false, now)
		if p.Completed != i || !p.Captured {
			t.Fatal("restart lost first originals or omitted a paired view", p)
		}
	}
	queries = len(d.s.queries)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	p = tailCaptureStep(t, source, reopened, key, scope, false, now.AddDate(0, 1, 0))
	if p.Captured || p.Completed != 4 || p.Boundary != walletTailMonthStart(now.UnixMilli()) || len(d.s.queries) != queries {
		t.Fatal("completed originals changed across restart/month or offline reuse", p)
	}
	for _, statement := range []string{
		"UPDATE portal_monthly_wallet_tail_captures SET id='changed' WHERE job_key=?",
		"DELETE FROM portal_monthly_wallet_tail_captures WHERE job_key=?",
	} {
		if _, err := reopened.Exec(statement, key); err == nil {
			t.Fatal("originals are mutable")
		}
	}
}

func TestWalletTailCapturePinsScopeAndSchemaBeforeSource(t *testing.T) {
	for _, scenario := range []string{"key", "auth", "group", "source", "roster", "month", "schema", "guard", "nil_context", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			source, d, receiver, _, key, scope, _, now := tailCaptureFixture(t)
			ctx := t.Context()
			switch scenario {
			case "key":
				key = strings.Repeat("a", 64)
			case "auth":
				scope.AuthVersion++
			case "group":
				scope.GroupID++
			case "source":
				scope.Funding.SourceIdentity = strings.Repeat("a", 64)
			case "roster":
				scope.Funding.UserIDs = []int64{201}
			case "month":
				scope.Funding.Month = "2026-08"
			case "schema":
				if _, err := receiver.Exec("DROP TABLE portal_monthly_wallet_tail_captures"); err != nil {
					t.Fatal(err)
				}
			case "guard":
				if _, err := receiver.Exec("DROP TRIGGER monthly_wallet_tail_no_replace"); err != nil {
					t.Fatal(err)
				}
			case "nil_context":
				ctx = nil
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if out, err := captureNextWalletTailMonthView(ctx, source, receiver, key, scope, false, func() time.Time { return now }); err == nil || out != (WalletTailCaptureProgress{}) {
				t.Fatal("wrong authority/scope/schema yielded source progress", out, err)
			}
			if len(d.s.queries) != 0 {
				t.Fatal("invalid scope opened financial source")
			}
		})
	}
}

func TestWalletTailCaptureAtomicFailureDoesNotCreateCheckpoint(t *testing.T) {
	for _, scenario := range []string{"source", "missing_user", "row_transfer", "write"} {
		t.Run(scenario, func(t *testing.T) {
			source, d, receiver, _, key, scope, _, now := tailCaptureFixture(t)
			switch scenario {
			case "source":
				d.failQuery = walletTailUsageQuery
			case "missing_user":
				d.s.data = nil
			case "row_transfer":
				d.failRows = walletTailFundingQuery
			case "write":
				if _, err := receiver.Exec(`CREATE TRIGGER synthetic_tail_write_failure BEFORE INSERT ON portal_monthly_wallet_tail_captures BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			if p, err := captureNextWalletTailMonthView(t.Context(), source, receiver, key, scope, true, func() time.Time { return now }); err == nil || p.Captured {
				t.Fatal("failed capture became a checkpoint", err)
			}
			var count int
			if err := receiver.QueryRow("SELECT count(*) FROM portal_monthly_wallet_tail_captures").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial original/reference commit", count, err)
			}
		})
	}
}

func TestWalletTailCaptureChangedPairIsRetainedAndNeverSilentlyRecaptured(t *testing.T) {
	source, d, receiver, _, key, scope, _, now := tailCaptureFixture(t)
	for range 2 {
		tailCaptureStep(t, source, receiver, key, scope, true, now)
	}
	d.s.data[0][2] = int64(1721) // unlogged movement: must not adjust consumption
	now = now.Add(5 * time.Minute)
	if out, err := captureNextWalletTailMonthView(t.Context(), source, receiver, key, scope, false, func() time.Time { return now }); !errors.Is(err, ErrWalletMonthCaptureIntegrity) || out.Captured {
		t.Fatal("changed boundary admitted", out, err)
	}
	var count int
	if err := receiver.QueryRow("SELECT count(*) FROM portal_monthly_wallet_tail_captures").Scan(&count); err != nil || count != 3 {
		t.Fatal("original failed pair was lost", count, err)
	}
	queries := len(d.s.queries)
	if _, err := captureNextWalletTailMonthView(t.Context(), source, receiver, key, scope, false, func() time.Time { return now }); !errors.Is(err, ErrWalletMonthCaptureIntegrity) || len(d.s.queries) != queries {
		t.Fatal("integrity failure was treated as uncaptured", err)
	}
}

func TestWalletTailCaptureCrossMonthKeepsIncompleteOriginals(t *testing.T) {
	source, d, receiver, _, key, scope, _, now := tailCaptureFixture(t)
	old := tailCaptureStep(t, source, receiver, key, scope, true, now)
	nextMonth := time.Unix(walletTailMonthStart(now.AddDate(0, 1, 0).UnixMilli()), 0)
	// First minute yields a queue hint without reading the source.
	p := tailCaptureStep(t, source, receiver, key, scope, true, nextMonth.Add(30*time.Second))
	if p.Captured || p.Completed != 0 || p.NextEligibleMS != nextMonth.Add(time.Minute).UnixMilli() {
		t.Fatal("month rollover reused/retimed an incomplete original", p)
	}
	d.usage, d.funding = nil, nil // new-month source has no old-month tail rows
	p = tailCaptureStep(t, source, receiver, key, scope, true, nextMonth.Add(time.Minute))
	if !p.Captured || p.Completed != 1 || p.Boundary == old.Boundary {
		t.Fatal("new month did not get an independent original", p)
	}
	var rows, rounds int
	if err := receiver.QueryRow("SELECT count(*),count(DISTINCT boundary) FROM portal_monthly_wallet_tail_captures").Scan(&rows, &rounds); err != nil || rows != 2 || rounds != 2 {
		t.Fatal("old incomplete original was overwritten", rows, rounds, err)
	}
}

func TestWalletTailCaptureLegacyFirstViewsResumeWithBoundedReader(t *testing.T) {
	source, d, receiver, _, key, scope, plan, now := tailCaptureFixture(t)
	sha := registeredCapturePlanSHA(plan)
	for _, user := range scope.Funding.UserIDs {
		r := WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: user, Boundary: walletTailMonthStart(now.UnixMilli())}
		old, err := captureWalletTail(t.Context(), source, r, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if err := saveWalletTailCapture(t.Context(), receiver, key, sha, scope, 0, old, now); err != nil {
			t.Fatal(err)
		}
	}
	_, before, err := inspectWalletTailSet(t.Context(), receiver, key, scope, now)
	if err != nil {
		t.Fatal(err)
	}
	queries := len(d.s.queries)
	now = now.Add(5 * time.Minute)
	for range scope.Funding.UserIDs {
		tailCaptureStep(t, source, receiver, key, scope, false, now)
	}
	_, after, err := inspectWalletTailSet(t.Context(), receiver, key, scope, now)
	if err != nil {
		t.Fatal("bounded reader cannot resume legacy originals", err)
	}
	for i, pair := range after.pairs {
		if !reflect.DeepEqual(pair.Older, before.pairs[i].Older) {
			t.Fatal("reader switch rewrote a legacy original")
		}
		if _, err := DeriveWalletTailBoundary(pair.Older, pair.Newer); err != nil {
			t.Fatal("mixed-reader views changed money", err)
		}
	}
	usage, funds := 0, 0
	for _, query := range d.s.queries[queries:] {
		if query == walletTailUsageQuery || query == walletTailFundingQuery {
			t.Fatal("runtime retried the legacy unbounded read")
		}
		if strings.Contains(query, "WITH usage_keys AS") {
			usage++
		}
		if strings.Contains(query, "AS funding_candidates") {
			funds++
		}
	}
	if usage != len(scope.Funding.UserIDs) || funds != len(scope.Funding.UserIDs) {
		t.Fatal("bounded runtime read omitted an account or stream", usage, funds)
	}
}

func TestWalletTailCaptureExactReplayAndTamperReject(t *testing.T) {
	_, _, receiver, _, key, scope, plan, now := tailCaptureFixture(t)
	a, b := walletTailFixture(t)
	sha := registeredCapturePlanSHA(plan)
	if err := saveWalletTailCapture(t.Context(), receiver, key, sha, scope, 1, b, now.Add(5*time.Minute)); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("second view without original accepted", err)
	}
	if err := saveWalletTailCapture(t.Context(), receiver, key, sha, scope, 0, a, now); err != nil {
		t.Fatal(err)
	}
	if err := saveWalletTailCapture(t.Context(), receiver, key, sha, scope, 0, a, now); err != nil {
		t.Fatal("exact replay rejected", err)
	}
	if err := saveWalletTailCapture(t.Context(), receiver, key, sha, scope, 0, b, now.Add(5*time.Minute)); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("original replaced", err)
	}
	if _, err := receiver.Exec("PRAGMA recursive_triggers=OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Exec(`INSERT OR REPLACE INTO portal_monthly_wallet_tail_captures SELECT job_key,plan_sha256,scope_sha256,boundary,user_id,ordinal,?,started_at_ms,completed_at_ms,payload FROM portal_monthly_wallet_tail_captures`, strings.Repeat("a", 64)); err == nil {
		t.Fatal("REPLACE bypassed immutability")
	}
	// Corruption fault injection is confined to this disposable test DB.
	if _, err := receiver.Exec("DROP TRIGGER monthly_wallet_tail_no_update"); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Exec("UPDATE portal_monthly_wallet_tail_captures SET completed_at_ms=completed_at_ms-1"); err != nil {
		t.Fatal(err)
	}
	if err := InitWalletTailCaptureStore(t.Context(), receiver); err != nil {
		t.Fatal(err)
	}
	if _, _, err := inspectWalletTailSet(t.Context(), receiver, key, scope, now); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("metadata no longer matches original but accepted", err)
	}
}

func tailCopyFixture(t *testing.T) (*sql.DB, string, []byte, SelectedWalletTails) {
	t.Helper()
	source, file, selection, _ := registeredCopyFixture(t)
	var s CapturedMonthSelection
	if err := json.Unmarshal(selection, &s); err != nil {
		t.Fatal(err)
	}
	if err := InitWalletTailCaptureStore(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-6 * time.Minute)
	boundary := walletTailMonthStart(now.UnixMilli())
	for _, id := range s.Scope.Funding.UserIDs {
		for i, at := range []time.Time{now, now.Add(5 * time.Minute)} {
			o := WalletTailObservation{Version: 1, Basis: walletTailBasis, Request: WalletTailRequest{SourceIdentity: s.Scope.Funding.SourceIdentity, Rules: s.Scope.Funding.Rules, UserID: id, Boundary: boundary}, StartedAtMS: at.UnixMilli(), CompletedAtMS: at.UnixMilli(), User: RegisteredWalletUser{UserID: id, RegisteredAt: boundary - 86400, BalanceQuota: walletPointer(9007199254740993), UsedQuota: walletPointer(123)}, Usage: []UsageWindowRow{}, Funding: []FundingLog{}}
			if err := saveWalletTailCapture(t.Context(), source, s.Key, s.PlanSHA256, s.Scope, i, o, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	want, err := ReadSelectedWalletTails(t.Context(), source, selection)
	if err != nil {
		t.Fatal(err)
	}
	return source, file, selection, want
}

func TestWalletTailCaptureCopyExactSelectedAndReopenReadOnly(t *testing.T) {
	source, _, selection, want := tailCopyFixture(t)
	var s CapturedMonthSelection
	if err := json.Unmarshal(selection, &s); err != nil {
		t.Fatal(err)
	}
	// A second authority-bound job with the SAME roster must not be copied.
	otherKey := digest([]byte("unrelated wallet tail job"))
	other, err := EnsureFundingMonthCapture(t.Context(), source, otherKey, s.Scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range want.Pairs {
		for i, o := range []WalletTailObservation{pair.Older, pair.Newer} {
			if err := saveWalletTailCapture(t.Context(), source, otherKey, registeredCapturePlanSHA(other), s.Scope, i, o, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	destination, file := emptyCapturedCopy(t)
	if _, err := CopyCapturedMonthObservations(t.Context(), source, destination, s.Key, s.Scope); err != nil {
		t.Fatal(err)
	}
	if err := CopySelectedRegisteredWallet(t.Context(), source, destination, selection); err != nil {
		t.Fatal(err)
	}
	var maxPagesBefore int64
	if err := destination.QueryRow("PRAGMA max_page_count").Scan(&maxPagesBefore); err != nil {
		t.Fatal(err)
	}
	if err := CopySelectedWalletTails(t.Context(), source, destination, selection); err != nil {
		t.Fatal(err)
	}
	var maxPagesAfter int64
	if err := destination.QueryRow("PRAGMA max_page_count").Scan(&maxPagesAfter); err != nil || maxPagesAfter != maxPagesBefore {
		t.Fatal("tail copy relaxed physical disk budget", err)
	}
	var count, others int
	if err := destination.QueryRow("SELECT count(*),coalesce(sum(CASE WHEN job_key<>? THEN 1 ELSE 0 END),0) FROM portal_monthly_wallet_tail_captures", s.Key).Scan(&count, &others); err != nil || count != len(want.Pairs)*2 || others != 0 {
		t.Fatal("foreign job escaped exact selection", count, others, err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+file+"?mode=ro&immutable=1&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	got, err := ReadSelectedWalletTails(t.Context(), ro, selection)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("source originals changed across seal/reopen", err)
	}
	if err := VerifyCapturedObservationSelection(t.Context(), ro, selection); err != nil {
		t.Fatal("legacy core no longer readable", err)
	}
}

func TestWalletTailCaptureCopyRejectsForeignAliasesAndUnprepared(t *testing.T) {
	for _, scenario := range []string{"empty", "core_only", "foreign", "alias", "already_copied", "wrong_selection", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			source, file, selection, _ := tailCopyFixture(t)
			var s CapturedMonthSelection
			if err := json.Unmarshal(selection, &s); err != nil {
				t.Fatal(err)
			}
			dest, _ := emptyCapturedCopy(t)
			ctx := t.Context()
			if scenario != "empty" && scenario != "alias" {
				if _, err := CopyCapturedMonthObservations(ctx, source, dest, s.Key, s.Scope); err != nil {
					t.Fatal(err)
				}
				if scenario != "core_only" {
					if err := CopySelectedRegisteredWallet(ctx, source, dest, selection); err != nil {
						t.Fatal(err)
					}
				}
			}
			switch scenario {
			case "foreign":
				if _, err := dest.Exec("CREATE TABLE unrelated_customer(x TEXT)"); err != nil {
					t.Fatal(err)
				}
			case "alias":
				var err error
				dest, err = sql.Open("sqlite", file)
				if err != nil {
					t.Fatal(err)
				}
				defer dest.Close()
			case "already_copied":
				if err := CopySelectedWalletTails(ctx, source, dest, selection); err != nil {
					t.Fatal(err)
				}
			case "wrong_selection":
				s.Key = digest([]byte("other-job"))
				selection, _ = json.Marshal(s)
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := CopySelectedWalletTails(ctx, source, dest, selection); err == nil {
				t.Fatal("unprepared/foreign/alias destination accepted")
			}
		})
	}
}

func TestWalletTailCaptureMaximumRosterReauditsExactAmounts(t *testing.T) {
	_, _, receiver, _, _, scope, _, now := tailCaptureFixture(t)
	key := digest([]byte("maximum tail roster"))
	scope.Funding.UserIDs = nil
	var ids strings.Builder
	for id := int64(1); id <= WalletBatchLimit; id++ {
		scope.Funding.UserIDs = append(scope.Funding.UserIDs, id)
		_, _ = fmt.Fprintf(&ids, "%d,", id)
	}
	scope.MemberFingerprint = digest([]byte(ids.String()))[:32]
	plan, err := EnsureFundingMonthCapture(t.Context(), receiver, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range scope.Funding.UserIDs {
		for i, at := range []time.Time{now, now.Add(5 * time.Minute)} {
			o := WalletTailObservation{Version: 1, Basis: walletTailBasis, Request: WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: id, Boundary: walletTailMonthStart(at.UnixMilli())}, StartedAtMS: at.UnixMilli(), CompletedAtMS: at.UnixMilli(), User: RegisteredWalletUser{UserID: id, RegisteredAt: walletTailMonthStart(at.UnixMilli()) - 86400, BalanceQuota: walletPointer(9007199254740993 + id), UsedQuota: walletPointer(id)}, Usage: []UsageWindowRow{}, Funding: []FundingLog{}}
			if err := saveWalletTailCapture(t.Context(), receiver, key, registeredCapturePlanSHA(plan), scope, i, o, now.Add(5*time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
	}
	started := time.Now()
	_, set, err := inspectWalletTailSet(t.Context(), receiver, key, scope, now.Add(5*time.Minute))
	if err != nil || len(set.pairs) != WalletBatchLimit {
		t.Fatal("maximum canonical roster not fully re-audited", err)
	}
	p := walletTailProgress(set, now.Add(5*time.Minute))
	if p.Completed != 400 || p.TotalViews != 400 || p.FirstViews != 200 {
		t.Fatal("maximum-roster completeness count wrong", p)
	}
	for i, pair := range set.pairs {
		b, err := DeriveWalletTailBoundary(pair.Older, pair.Newer)
		id := int64(i + 1)
		if err != nil || b.UserID() != id || b.BalanceQuota() != 9007199254740993+id || b.UsedQuota() != id {
			t.Fatal("large exact money crossed account", i, b, err)
		}
	}
	t.Logf("400 original views, 200 exact account calculations re-audited in %s", time.Since(started))
	dest, _ := emptyCapturedCopy(t)
	if err := InitWalletTailCaptureStore(t.Context(), dest); err != nil {
		t.Fatal(err)
	}
	copyCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	selected := CapturedMonthSelection{Key: key, PlanSHA256: registeredCapturePlanSHA(plan), Scope: scope}
	if err := copyWalletTailRows(copyCtx, dest, selected, SelectedWalletTails{Boundary: set.boundary, Pairs: set.pairs}); err != nil {
		t.Fatal("maximum-roster batch copy did not meet its local budget", err)
	}
	restored, err := loadWalletTailRound(t.Context(), dest, key, selected.PlanSHA256, scope, set.boundary, now.Add(5*time.Minute))
	if err != nil || !reflect.DeepEqual(restored, set) {
		t.Fatal("batched copy changed original amounts", err)
	}
}

func TestWalletTailCaptureBatchCopyFailureRollsBackWholeSet(t *testing.T) {
	source, _, selection, originals := tailCopyFixture(t)
	var s CapturedMonthSelection
	if err := json.Unmarshal(selection, &s); err != nil {
		t.Fatal(err)
	}
	destination, _ := emptyCapturedCopy(t)
	if err := InitWalletTailCaptureStore(t.Context(), destination); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.Exec(`CREATE TRIGGER synthetic_tail_copy_failure BEFORE INSERT ON portal_monthly_wallet_tail_captures WHEN NEW.ordinal=1 BEGIN SELECT RAISE(ABORT,'synthetic second-row failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := copyWalletTailRows(t.Context(), destination, s, originals); err == nil {
		t.Fatal("injected partial-copy failure was ignored")
	}
	var count int
	if err := destination.QueryRow("SELECT count(*) FROM portal_monthly_wallet_tail_captures").Scan(&count); err != nil || count != 0 {
		t.Fatal("batch copy retained partial originals", count, err)
	}
	if _, err := ReadSelectedWalletTails(t.Context(), source, selection); err != nil {
		t.Fatal("failed destination copy damaged original sources", err)
	}
}

func TestWalletTailCapturePayloadAndPhysicalCapacityFailureIsAtomic(t *testing.T) {
	for _, kind := range []string{"encoded_payload", "sqlite_full"} {
		t.Run(kind, func(t *testing.T) {
			_, _, receiver, _, key, scope, plan, now := tailCaptureFixture(t)
			a, _ := walletTailFixture(t)
			if kind == "encoded_payload" {
				a.Funding = nil
				for i := 0; i < 200; i++ {
					a.Funding = append(a.Funding, FundingLog{ID: int64(i + 1), UserID: 100, CreatedAt: a.Request.Boundary + 1, Type: 3, Content: strings.Repeat("\x00", 4096), Other: ""})
				}
				if err := ValidateWalletTailObservation(a); err != nil {
					t.Fatal("expected bounded valid source bytes before encoding", err)
				}
				if err := saveWalletTailCapture(t.Context(), receiver, key, registeredCapturePlanSHA(plan), scope, 0, a, now); !errors.Is(err, ErrWalletTailCaptureCapacity) {
					t.Fatal("encoded payload escaped budget", err)
				}
			} else {
				var pages int64
				if err := receiver.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages)); err != nil {
					t.Fatal(err)
				}
				a.Funding = nil
				for i := 0; i < 100; i++ {
					a.Funding = append(a.Funding, FundingLog{ID: int64(i + 1), UserID: 100, CreatedAt: a.Request.Boundary + 1, Type: 3, Content: strings.Repeat("x", 4096), Other: ""})
				}
				if err := saveWalletTailCapture(t.Context(), receiver, key, registeredCapturePlanSHA(plan), scope, 0, a, now); err == nil {
					t.Fatal("physical capacity did not stop atomic write")
				}
			}
			var count int
			if err := receiver.QueryRow("SELECT count(*) FROM portal_monthly_wallet_tail_captures").Scan(&count); err != nil || count != 0 {
				t.Fatal("capacity failure created partial checkpoint", count, err)
			}
		})
	}
}
