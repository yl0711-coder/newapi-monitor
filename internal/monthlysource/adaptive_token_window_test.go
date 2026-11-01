package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type adaptiveTokenFixture struct {
	source, store *sql.DB
	driver        *tokenUsageTestDriver
	path, job     string
	root          UsageWindowRequest
	now           time.Time
	ops           adaptiveTokenCaptureOps
	fail          func(UsageWindowRequest) error
}

func newAdaptiveTokenFixture(t *testing.T) *adaptiveTokenFixture {
	t.Helper()
	source, d, req := tokenUsageSource(t)
	store, path := walletTestStore(t)
	if err := InitAdaptiveTokenCaptureStore(t.Context(), store); err != nil {
		t.Fatal(err)
	}
	req.From, req.To = 1788192000, 1788278400 // closed CST 2026-09-01
	f := &adaptiveTokenFixture{source: source, store: store, driver: d, path: path, root: req,
		job: digest([]byte("synthetic authority/source preparation job")), now: time.Now().Add(-2 * time.Hour)}
	f.ops.now = func() time.Time { return f.now }
	f.ops.pace = func(ctx context.Context) error {
		if f.store.Stats().InUse != 0 {
			return errors.New("capture SQLite retained during source I/O")
		}
		return ctx.Err()
	}
	f.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		if db != f.source || r.UserID != f.root.UserID || r.SourceIdentity != f.root.SourceIdentity || r.Rules != f.root.Rules || r.From < f.root.From || r.To > f.root.To {
			return TokenUsageWindowObservation{}, errors.New("source scope broadened")
		}
		f.driver.queryErr = nil
		if f.fail != nil {
			f.driver.queryErr = f.fail(r)
		}
		// A fixed independent synthetic log set: sixteen actual events, with
		// a large unequal last event. Child charges cannot be prorated by time.
		var count, quota, first, last, low, high int64
		for k := int64(0); k < 16; k++ {
			at := f.root.From + 10 + k*5400
			if at < r.From || at >= r.To {
				continue
			}
			q := 100 + k
			if k == 15 {
				q = 9999
			}
			if count == 0 {
				first, low = k, q
			}
			count++
			last, high, quota = k, q, quota+q
		}
		f.driver.usage.data, f.driver.data = [][]driver.Value{}, [][]driver.Value{}
		if count > 0 {
			row := []driver.Value{f.root.UserID, int64(2), "wallet", count, count, strconv.FormatInt(quota, 10), low, high, 1000 + first, 1000 + last,
				f.root.From + 10 + first*5400, f.root.From + 10 + last*5400}
			f.driver.usage.data = [][]driver.Value{row}
			f.driver.data = [][]driver.Value{append(append([]driver.Value{}, row...), int64(11), "stable original token")}
		}
		return captureTokenUsageWindow(ctx, db, r, func() time.Time {
			at := f.now
			f.now = f.now.Add(time.Millisecond)
			return at
		})
	}
	return f
}

func (f *adaptiveTokenFixture) advance(t *testing.T) (AdaptiveTokenWindowProgress, error) {
	t.Helper()
	return advanceAdaptiveTokenWindow(t.Context(), f.source, f.store, f.job, f.root, f.ops)
}

func (f *adaptiveTokenFixture) queries() int {
	f.driver.usage.s.mu.Lock()
	defer f.driver.usage.s.mu.Unlock()
	return len(f.driver.usage.s.queries)
}

func adaptiveTokenCount(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func finishAdaptiveFixture(t *testing.T, f *adaptiveTokenFixture) AdaptiveTokenWindowProgress {
	t.Helper()
	for step := 0; step < 80; step++ {
		p, err := f.advance(t)
		if err != nil {
			t.Fatal("adaptive advance failed", err)
		}
		if p.Complete {
			return p
		}
		if p.NextEligibleMS > f.now.UnixMilli() {
			f.now = time.UnixMilli(p.NextEligibleMS)
		}
	}
	t.Fatal("adaptive capture did not terminate within its finite tree")
	return AdaptiveTokenWindowProgress{}
}

func TestAdaptiveTokenSplitKeepsIndependentChildrenExactMoneyAndRestarts(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	f.fail = func(r UsageWindowRequest) error {
		if r.To-r.From > 43200 {
			return &mysql.MySQLError{Number: 3024, Message: "synthetic execution deadline"}
		}
		return nil
	}
	p, err := f.advance(t)
	if err != nil || !p.Split || p.Captured || p.Leaves != 2 || p.Observations != 0 || p.Complete {
		t.Fatal("failed parent counted as a source observation", p, err)
	}
	if count := adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_observations`); count != 0 {
		t.Fatal("fabricated parent", count)
	}
	for i := 0; i < 2; i++ {
		p, err = f.advance(t)
		if err != nil || !p.Captured || p.Complete {
			t.Fatal(p, err)
		}
	}
	queries := f.queries()
	changes := adaptiveTokenCount(t, f.store, `SELECT total_changes()`)
	for i := 0; i < 3; i++ {
		p, err = f.advance(t)
		if err != nil || p.Captured || p.NextEligibleMS <= f.now.UnixMilli() || f.queries() != queries {
			t.Fatal("waiting queried or captured source", p, err)
		}
	}
	if adaptiveTokenCount(t, f.store, `SELECT total_changes()`) != changes || f.source.Stats().InUse != 0 || f.store.Stats().InUse != 0 {
		t.Fatal("wait wrote or retained a connection")
	}
	if _, err := LoadAdaptiveTokenWindowPairs(t.Context(), f.store, f.job, f.root); err == nil {
		t.Fatal("one sweep counted as complete")
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = sql.Open("sqlite", f.path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	f.store.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = f.store.Close() })
	p = finishAdaptiveFixture(t, f)
	if p.Leaves != 2 || p.Observations != 4 || p.MatchingPairs != 2 {
		t.Fatal(p)
	}
	pairs, err := LoadAdaptiveTokenWindowPairs(t.Context(), f.store, f.job, f.root)
	if err != nil || len(pairs) != 2 || pairs[0].Older.Request.From != f.root.From || pairs[0].Older.Request.To != pairs[1].Older.Request.From || pairs[1].Older.Request.To != f.root.To {
		t.Fatal("child gaps or overlap", err)
	}
	var total int64
	for _, pair := range pairs {
		if !TokenUsageWindowsMatch(pair.Older, pair.Newer) || pair.Older.Request == f.root {
			t.Fatal("not two original child views")
		}
		total += pair.Newer.Rows[0].Quota
	}
	if total != 11604 || pairs[0].Newer.Rows[0].Quota != 828 || pairs[1].Newer.Rows[0].Quota != 10776 {
		t.Fatal("source money prorated or counted twice", total)
	}
	if !f.driver.usage.s.options.ReadOnly || f.driver.usage.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) {
		t.Fatal("source transaction widened")
	}
	queries = f.queries()
	if p, err := f.advance(t); err != nil || !p.Complete || p.Captured || f.queries() != queries {
		t.Fatal("complete root recaptured", p, err)
	}
	_, err = SummarizeTokenUsageMonth(t.Context(), FundingMonthRequest{Month: "2026-09", SourceIdentity: f.root.SourceIdentity, Rules: f.root.Rules, UserIDs: []int64{f.root.UserID}}, pairs, time.Now())
	if err == nil {
		t.Fatal("one day of child observations became a complete month")
	}
}

func TestAdaptiveTokenSecondSweepTimeoutPreservesParentAndRecapturesBothChildren(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	p, err := f.advance(t)
	if err != nil || !p.Captured || p.Observations != 1 {
		t.Fatal(p, err)
	}
	_, _, id, _ := adaptiveTokenIdentity(f.job, f.root)
	s, err := loadAdaptiveTokenState(t.Context(), f.store, f.job, f.root, f.now)
	if err != nil {
		t.Fatal(err)
	}
	original, err := loadAdaptiveTokenRecord(t.Context(), f.store, s, f.root, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.now = time.UnixMilli(p.NextEligibleMS)
	f.fail = func(r UsageWindowRequest) error {
		if r == f.root {
			return &mysql.MySQLError{Number: 3024}
		}
		return nil
	}
	p, err = f.advance(t)
	if err != nil || !p.Split || p.Observations != 0 {
		t.Fatal("parent old view was divided into child data", p, err)
	}
	finishAdaptiveFixture(t, f)
	after, err := loadAdaptiveTokenState(t.Context(), f.store, f.job, f.root, f.now)
	if err != nil || after.id != id {
		t.Fatal(err)
	}
	retained, err := loadAdaptiveTokenRecord(t.Context(), f.store, after, f.root, 0)
	if err != nil || !reflect.DeepEqual(original, retained) || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_observations`) != 5 {
		t.Fatal("original parent overwritten or child originals missing", err)
	}
	queries := f.queries()
	f.now = f.now.Add(25 * time.Hour)
	if p, err := f.advance(t); err != nil || !p.Complete || f.queries() != queries {
		t.Fatal("completed original evidence expired with source-work budget", err)
	}
}

func TestAdaptiveTokenFailureBudgetCooldownAndMinimumSurviveRestart(t *testing.T) {
	for _, scenario := range []string{"auth", "minimum"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAdaptiveTokenFixture(t)
			code := uint16(1045)
			if scenario == "minimum" {
				code = 3024
			}
			f.fail = func(UsageWindowRequest) error { return &mysql.MySQLError{Number: code} }
			for attempt := 0; attempt < 6; attempt++ {
				_, err := f.advance(t)
				if scenario == "minimum" && err == nil {
					continue
				}
				if err == nil {
					t.Fatal("source fault accepted")
				}
				queries := f.queries()
				p, nextErr := f.advance(t)
				if nextErr != nil {
					if !errors.Is(nextErr, ErrAdaptiveTokenStopped) || f.queries() != queries {
						t.Fatal(nextErr)
					}
					break
				}
				if p.NextEligibleMS <= f.now.UnixMilli() || f.queries() != queries {
					t.Fatal("no cooldown after source failure")
				}
				f.now = time.UnixMilli(p.NextEligibleMS)
			}
			queries := f.queries()
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			f.store, err = sql.Open("sqlite", f.path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.store.Close() })
			if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) || f.queries() != queries {
				t.Fatal("restart reset durable source failure budget", err)
			}
			if scenario == "auth" && adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_splits`) != 0 {
				t.Fatal("auth failure activated query splitting")
			}
		})
	}
}

func TestAdaptiveTokenOldProjectionLostStopsBeforeMoreSourceQueries(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	p, err := f.advance(t)
	if err != nil {
		t.Fatal(err)
	}
	queries := f.queries()
	if _, err := f.store.Exec(`DROP TRIGGER monthly_usage_no_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Exec(`DELETE FROM portal_monthly_usage_observations`); err != nil {
		t.Fatal(err)
	}
	f.now = time.UnixMilli(p.NextEligibleMS)
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenIntegrity) || f.queries() != queries {
		t.Fatal("lost old projection caused more source queries", err)
	}
}

func TestAdaptiveTokenFullSixteenLeafTreeHasExactOriginalCoverage(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	f.fail = func(r UsageWindowRequest) error {
		if r.To-r.From > 5400 {
			return &mysql.MySQLError{Number: 3024}
		}
		return nil
	}
	p := finishAdaptiveFixture(t, f)
	if p.Leaves != 16 || p.Observations != 32 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_splits`) != 15 {
		t.Fatal("maximum finite tree not completed", p)
	}
	pairs, err := LoadAdaptiveTokenWindowPairs(t.Context(), f.store, f.job, f.root)
	if err != nil || len(pairs) != 16 {
		t.Fatal(err)
	}
	cursor, quota := f.root.From, int64(0)
	for _, pair := range pairs {
		if pair.Older.Request.From != cursor || pair.Older.Request.To-pair.Older.Request.From != 5400 || !TokenUsageWindowsMatch(pair.Older, pair.Newer) {
			t.Fatal("child scope changed, missing or overlapping")
		}
		cursor = pair.Older.Request.To
		quota += pair.Newer.Rows[0].Quota
	}
	if cursor != f.root.To || quota != 11604 {
		t.Fatal("full tree lost or duplicated money", quota)
	}
}

func TestAdaptiveTokenOrphanSplitCannotCauseSourceQueries(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	f.fail = func(UsageWindowRequest) error { return &mysql.MySQLError{Number: 3024} }
	if _, err := f.advance(t); err != nil {
		t.Fatal(err)
	}
	queries := f.queries()
	_, _, id, _ := adaptiveTokenIdentity(f.job, f.root)
	if _, err := f.store.Exec(`INSERT INTO monthly_adaptive_token_splits(root_id,window_from,window_to,phase) VALUES(?,?,?,0)`, id, f.root.From+17, f.root.To-10); err != nil {
		t.Fatal(err)
	}
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenIntegrity) || f.queries() != queries {
		t.Fatal("unrelated split activated a source read", err)
	}
}
