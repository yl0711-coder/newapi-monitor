package monthlysource

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// Independent pre-optimization oracle: keep both the old scalar SQL and the
// canonical/digest checks, rather than comparing two uses of the new decoder.
func legacyFundingLatest(ctx context.Context, db *sql.DB, scan string) (FundingPage, error) {
	var p FundingPage
	var id, storedScan string
	var sequence int64
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT id,scan_id,sequence,CASE WHEN length(payload)<=2097152 THEN payload ELSE NULL END FROM portal_monthly_funding_pages WHERE scan_id=? ORDER BY sequence DESC LIMIT 1`, scan).Scan(&id, &storedScan, &sequence, &raw)
	if err != nil {
		return p, err
	}
	if digest(raw) != id || json.Unmarshal(raw, &p) != nil || p.Scan.ID != storedScan || sequence <= 0 || (sequence == 1) != (p.PreviousID == "") {
		return FundingPage{}, errors.New("funding stored page integrity failure")
	}
	canonical, err := fundingPageBytes(p)
	if err != nil || !bytes.Equal(canonical, raw) {
		return FundingPage{}, errors.New("funding stored page is not a complete canonical record")
	}
	return p, nil
}

func legacyFundingMonthInspect(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope, now time.Time) (FundingMonthCaptureProgress, error) {
	var out FundingMonthCaptureProgress
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	plan, err := loadFundingMonthCapturePlan(ctx, db, key, scope, now)
	if err != nil {
		return out, err
	}
	out.Mode, out.TotalScans = "funding_traversals_not_bill_approval", len(plan.Scans)
	var oldEnd int64
	missing := false
	for i, scan := range plan.Scans {
		page, err := legacyFundingLatest(ctx, db, scan.ID)
		if errors.Is(err, sql.ErrNoRows) {
			missing = true
			if out.NextScanID == "" {
				out.NextScanID = scan.ID
			}
			continue
		}
		if err != nil {
			return FundingMonthCaptureProgress{}, err
		}
		if !sameFundingScan(page.Scan, scan) || page.CompletedAtMS > now.UnixMilli() || missing {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if i < len(plan.Scans)/2 {
			if page.CompletedAtMS > oldEnd {
				oldEnd = page.CompletedAtMS
			}
		} else if page.StartedAtMS <= oldEnd {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if !page.EndReached {
			missing, out.NextScanID = true, scan.ID
		} else {
			out.FinishedScans++
		}
	}
	if out.FinishedScans >= len(plan.Scans)/2 && now.UnixMilli() <= oldEnd {
		out.NextEligibleMS = oldEnd + 1
	}
	out.TraversalsFinished = out.FinishedScans == len(plan.Scans)
	return out, nil
}

func batchFundingMonthInspect(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope, now time.Time) (FundingMonthCaptureProgress, error) {
	return inspectFundingMonthCapture(parent, db, key, scope, now)
}

type fundingHeadsFixture struct {
	db    *sql.DB
	count *sqlAuditReadCounter
	key   string
	scope OrganizationMonthScope
	plan  FundingMonthCapturePlan
	now   time.Time
}

func newFundingHeadsFixture(t *testing.T, month string) fundingHeadsFixture {
	return newFundingHeadsMemberFixture(t, month, 2)
}

func newFundingHeadsMemberFixture(t *testing.T, month string, members int) fundingHeadsFixture {
	t.Helper()
	base, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	count := &sqlAuditReadCounter{}
	db := sql.OpenDB(sqlAuditCounterConnector{base: base.Driver(), count: count})
	_ = base.Close()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := InitFundingMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, members)
	var membership strings.Builder
	for i := range ids {
		ids[i] = int64(201 + i)
		fmt.Fprintf(&membership, "%d,", ids[i])
	}
	sum := sha256.Sum256([]byte(membership.String()))
	scope := OrganizationMonthScope{Version: 1, Mode: OrganizationMonthScopeMode, GroupID: 1, AuthVersion: 1,
		MemberFingerprint: hex.EncodeToString(sum[:16]), MemberManifest: strings.Repeat("c", 64),
		Funding: FundingMonthRequest{Month: month, SourceIdentity: strings.Repeat("a", 64), Rules: fundingTestRules(), UserIDs: ids}}
	key := digest([]byte("funding-heads-" + month))
	plan, err := EnsureFundingMonthCapture(t.Context(), db, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	return fundingHeadsFixture{db, count, key, scope, plan, time.Now().Truncate(time.Millisecond)}
}

func (f fundingHeadsFixture) page(ordinal, n, contentBytes int) FundingPage {
	p := FundingPage{Version: 1, Scan: f.plan.Scans[ordinal], FenceID: 1000000, EndReached: true, Rows: []FundingLog{},
		StartedAtMS: f.now.Add(-time.Hour).UnixMilli() + int64(ordinal*1000), CompletedAtMS: f.now.Add(-time.Hour).UnixMilli() + int64(ordinal*1000) + 1}
	for i := 0; i < n; i++ {
		r := FundingLog{ID: int64(ordinal*1000 + i + 1), UserID: 9, CreatedAt: p.Scan.From + int64(i+1), Type: 3, Content: strings.Repeat("a", contentBytes)}
		p.Rows = append(p.Rows, r)
		p.Next = FundingCursor{At: r.CreatedAt, ID: r.ID}
	}
	return p
}

func (f fundingHeadsFixture) save(t *testing.T, p FundingPage) string {
	t.Helper()
	id, err := SaveFundingPage(t.Context(), f.db, p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (f fundingHeadsFixture) compare(t *testing.T, wantError bool) {
	t.Helper()
	want, oldErr := legacyFundingMonthInspect(t.Context(), f.db, f.key, f.scope, f.now)
	got, newErr := batchFundingMonthInspect(t.Context(), f.db, f.key, f.scope, f.now)
	if !reflect.DeepEqual(want, got) || (oldErr == nil) != (newErr == nil) || (newErr != nil) != wantError {
		t.Fatalf("head batching changed progress/error: old=%+v/%v new=%+v/%v", want, oldErr, got, newErr)
	}
	if oldErr != nil && oldErr.Error() != newErr.Error() {
		t.Fatalf("head batching changed error classification: old=%v new=%v", oldErr, newErr)
	}
	if f.db.Stats().InUse != 0 {
		t.Fatal("inspection leaked single-connection reader")
	}
}

func TestFundingMonthHeadsBatchPreservesCalendarsAndProgress(t *testing.T) {
	for _, month := range []string{"2025-02", "2024-02", "2026-09", "2026-10"} {
		t.Run(month, func(t *testing.T) {
			f := newFundingHeadsFixture(t, month)
			for i := 0; i <= len(f.plan.Scans); i++ {
				f.compare(t, false)
				if i < len(f.plan.Scans) {
					f.save(t, f.page(i, i%4, 20))
				}
			}
			*f.count = sqlAuditReadCounter{}
			if _, err := batchFundingMonthInspect(t.Context(), f.db, f.key, f.scope, f.now); err != nil || f.count.queries != 2 {
				t.Fatalf("batch must use one plan read and one indexed head read: count=%d err=%v", f.count.queries, err)
			}
		})
	}
}

func TestFundingMonthHeadsBatchRejectsHolesAndCorruption(t *testing.T) {
	for _, name := range []string{"late-page", "unfinished-then-late", "overlapping-sweep", "future-page", "wrong-scope", "noncanonical", "wrong-digest", "wrong-scan", "wrong-sequence", "oversize"} {
		t.Run(name, func(t *testing.T) {
			f := newFundingHeadsFixture(t, "2026-10")
			p := f.page(0, 0, 0)
			switch name {
			case "late-page":
				p = f.page(1, 0, 0)
			case "unfinished-then-late":
				first := f.page(0, 200, 1)
				first.EndReached = false
				f.save(t, first)
				p = f.page(1, 0, 0)
			case "overlapping-sweep":
				for i := 0; i < 31; i++ {
					f.save(t, f.page(i, 0, 0))
				}
				p = f.page(31, 0, 0)
				p.StartedAtMS, p.CompletedAtMS = f.page(30, 0, 0).CompletedAtMS, f.page(30, 0, 0).CompletedAtMS+1
			case "future-page":
				p.StartedAtMS, p.CompletedAtMS = f.now.UnixMilli()+1000, f.now.UnixMilli()+1001
			}
			if name == "wrong-scope" {
				// A different otherwise valid source scope with the same scan ID.
				p.Scan.Rules = fundingTestRules()
				p.Scan.SourceIdentity = strings.Repeat("d", 64)
			}
			id := f.save(t, p)
			if strings.Contains(name, "wrong-") && name != "wrong-scope" || name == "noncanonical" || name == "oversize" {
				if _, err := f.db.Exec("DROP TRIGGER monthly_funding_pages_no_update"); err != nil {
					t.Fatal(err)
				}
				var err error
				switch name {
				case "wrong-digest":
					_, err = f.db.Exec("UPDATE portal_monthly_funding_pages SET id=? WHERE id=?", strings.Repeat("e", 64), id)
				case "wrong-scan":
					// Keep the selected scan ID but change the original's scan ID.
					p.Scan.ID = strings.Repeat("f", 32)
					raw, _ := json.Marshal(p)
					_, err = f.db.Exec("UPDATE portal_monthly_funding_pages SET id=?,payload=? WHERE id=?", digest(raw), raw, id)
				case "wrong-sequence":
					_, err = f.db.Exec("UPDATE portal_monthly_funding_pages SET sequence=2 WHERE id=?", id)
				case "noncanonical":
					raw, _ := json.Marshal(p)
					raw = append(raw, ' ')
					_, err = f.db.Exec("UPDATE portal_monthly_funding_pages SET id=?,payload=? WHERE id=?", digest(raw), raw, id)
				case "oversize":
					if _, err := f.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
						t.Fatal(err)
					}
					_, err = f.db.Exec("UPDATE portal_monthly_funding_pages SET payload=? WHERE id=?", bytes.Repeat([]byte{'a'}, (2<<20)+1), id)
				}
				if err != nil {
					t.Fatal("failed to install local fault", err)
				}
			}
			f.compare(t, true)
		})
	}
}

func TestFundingMonthHeadsBatchFullMemberScope(t *testing.T) {
	f := newFundingHeadsMemberFixture(t, "2026-10", WalletBatchLimit)
	for i := range f.plan.Scans {
		f.save(t, f.page(i, 1, 128))
	}
	f.compare(t, false)
	// A valid page from a smaller roster must not masquerade as the pinned
	// 200-member plan, even if the page's scan ID and source still match.
	p := f.page(61, 1, 128)
	p.Scan.UserIDs = append([]int64(nil), p.Scan.UserIDs[:len(p.Scan.UserIDs)-1]...)
	raw, err := fundingPageBytes(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.Exec("DROP TRIGGER monthly_funding_pages_no_update"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.Exec("UPDATE portal_monthly_funding_pages SET id=?,payload=? WHERE scan_id=?", digest(raw), raw, p.Scan.ID); err != nil {
		t.Fatal(err)
	}
	f.compare(t, true)
}

func TestFundingMonthHeadsBatchLatestSequenceAndPlan(t *testing.T) {
	f := newFundingHeadsFixture(t, "2026-10")
	p := f.page(0, 200, 1)
	p.EndReached = false
	id := f.save(t, p)
	f.compare(t, false)
	next := f.page(0, 0, 0)
	next.PreviousID, next.After, next.Next = id, p.Next, p.Next
	next.StartedAtMS, next.CompletedAtMS = p.CompletedAtMS+1, p.CompletedAtMS+2
	f.save(t, next)
	f.compare(t, false)
	q, args, err := fundingMonthHeadsQuery(f.plan)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := f.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+q, args...)
	if err != nil {
		t.Fatal(err)
	}
	var searches int
	var planDetails []string
	for rows.Next() {
		var a, b, c int
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		planDetails = append(planDetails, detail)
		if strings.Contains(detail, "SEARCH p USING INDEX") || strings.Contains(detail, "SEARCH h USING COVERING INDEX") {
			searches++
		}
		if strings.Contains(detail, "TEMP B-TREE") || strings.HasPrefix(detail, "SCAN p") || strings.HasPrefix(detail, "SCAN h") {
			t.Fatal("batch regressed to historical scan or payload sort", detail)
		}
		t.Log(detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if searches != 2 {
		t.Fatal("missing existing latest-head range and exact-page seeks", searches)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := batchFundingMonthInspect(ctx, f.db, f.key, f.scope, f.now); !errors.Is(err, context.Canceled) || got != (FundingMonthCaptureProgress{}) {
		t.Fatal("cancelled inspection returned progress", got, err)
	}
	if os.Getenv("SQL_AUDIT_HEAD_PLAN") == "1" {
		source, err := os.ReadFile("funding_month_heads.go")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.MarshalIndent(struct {
			SQL          string
			Parameters   int
			Plan         []string
			SourceSHA256 string
		}{q, len(args), planDetails, fmt.Sprintf("%x", sha256.Sum256(source))}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile("../../docs/sql-audit-20261126/funding-heads-current-plan-20261127.json", append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

type fundingHeadRows struct {
	columns     []string
	values      [][]driver.Value
	position    int
	beforeNext  func()
	nextErrorAt int
	closeError  error
}

func (r *fundingHeadRows) Columns() []string { return r.columns }
func (r *fundingHeadRows) Close() error      { return r.closeError }
func (r *fundingHeadRows) Next(out []driver.Value) error {
	if r.beforeNext != nil {
		r.beforeNext()
	}
	if r.nextErrorAt > 0 && r.position == r.nextErrorAt {
		return errors.New("synthetic head read failure")
	}
	if r.position >= len(r.values) {
		return io.EOF
	}
	copy(out, r.values[r.position])
	r.position++
	return nil
}

func TestFundingMonthHeadsBatchOrderShapeAndCancellation(t *testing.T) {
	for _, mode := range []string{"reverse", "missing", "duplicate", "foreign-ordinal", "null-metadata", "cancel-during-read", "read-error", "close-error", "zero-heads"} {
		t.Run(mode, func(t *testing.T) {
			f := newFundingHeadsFixture(t, "2026-10")
			for i := 0; i < len(f.plan.Scans); i++ {
				f.save(t, f.page(i, 0, 0))
			}
			want, err := legacyFundingMonthInspect(t.Context(), f.db, f.key, f.scope, f.now)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			f.count.rewriteRows = func(q string, in driver.Rows) (driver.Rows, error) {
				if !strings.Contains(q, "WITH requested(ordinal,scan_id)") {
					return in, nil
				}
				out := &fundingHeadRows{columns: in.Columns()}
				for {
					v := make([]driver.Value, len(out.columns))
					err := in.Next(v)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						_ = in.Close()
						return nil, err
					}
					for i, value := range v {
						if b, ok := value.([]byte); ok {
							v[i] = append([]byte(nil), b...)
						}
					}
					out.values = append(out.values, v)
				}
				if err := in.Close(); err != nil {
					return nil, err
				}
				switch mode {
				case "reverse":
					for i, j := 0, len(out.values)-1; i < j; i, j = i+1, j-1 {
						out.values[i], out.values[j] = out.values[j], out.values[i]
					}
				case "missing":
					out.values = out.values[:len(out.values)-1]
				case "duplicate":
					out.values = append(out.values, out.values[0])
				case "foreign-ordinal":
					out.values[0][0] = int64(99)
				case "null-metadata":
					out.values[0][1] = nil
				case "cancel-during-read":
					out.beforeNext = func() {
						if out.position == 4 {
							cancel()
						}
					}
				case "read-error":
					out.nextErrorAt = 3
				case "close-error":
					out.closeError = errors.New("synthetic head close failure")
				case "zero-heads":
					out.values = nil
				}
				return out, nil
			}
			got, err := batchFundingMonthInspect(ctx, f.db, f.key, f.scope, f.now)
			if mode == "reverse" {
				if err != nil || got != want {
					t.Fatalf("physical result order changed progress: %+v %v", got, err)
				}
			} else if err == nil || got != (FundingMonthCaptureProgress{}) {
				t.Fatalf("partial/malformed stream returned progress: %+v %v", got, err)
			}
			if mode == "cancel-during-read" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation classification lost", err)
			}
			if f.db.Stats().InUse != 0 {
				t.Fatal("error stream leaked reader")
			}
			f.count.rewriteRows = nil
			if err := f.db.PingContext(t.Context()); err != nil {
				t.Fatal("connection unusable after stream", err)
			}
		})
	}
	// Pool wait must honor the caller, not start a fresh two-second budget.
	f := newFundingHeadsFixture(t, "2026-10")
	held, err := f.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	got, err := batchFundingMonthInspect(ctx, f.db, f.key, f.scope, f.now)
	_ = held.Close()
	if !errors.Is(err, context.DeadlineExceeded) || got != (FundingMonthCaptureProgress{}) {
		t.Fatal("pool wait ignored deadline", got, err)
	}
}

func TestFundingMonthHeadsBatchExactEligibilityBoundary(t *testing.T) {
	f := newFundingHeadsFixture(t, "2026-10")
	for i := 0; i < 31; i++ {
		p := f.page(i, 0, 0)
		if i == 30 {
			p.StartedAtMS = f.now.UnixMilli() - 1
			p.CompletedAtMS = f.now.UnixMilli()
		}
		f.save(t, p)
	}
	f.compare(t, false)
	got, err := batchFundingMonthInspect(t.Context(), f.db, f.key, f.scope, f.now)
	if err != nil || got.NextEligibleMS != f.now.UnixMilli()+1 || got.FinishedScans != 31 || got.TraversalsFinished {
		t.Fatalf("independent sweep boundary changed: %+v %v", got, err)
	}
}

func TestFundingMonthHeadsBatchExperiment(t *testing.T) {
	if os.Getenv("MONTHLY_FUNDING_HEAD_BENCH") != "1" {
		t.Skip("explicit synthetic local timing experiment")
	}
	type sample struct {
		Name                   string `json:"name"`
		Old, Batch             []int64
		OldMedian, BatchMedian int64
	}
	var samples []sample
	for _, spec := range []struct {
		name                   string
		completed, rows, bytes int
	}{{"empty", 0, 0, 0}, {"half", 31, 0, 0}, {"all-empty-pages", 62, 0, 0}, {"all-200-record-pages", 62, 200, 256}} {
		f := newFundingHeadsFixture(t, "2026-10")
		for i := 0; i < spec.completed; i++ {
			f.save(t, f.page(i, spec.rows, spec.bytes))
		}
		s := sample{Name: spec.name}
		var result FundingMonthCaptureProgress
		haveResult := false
		for i := 0; i < 9; i++ {
			for _, batch := range []bool{i%2 != 0, i%2 == 0} {
				fn := legacyFundingMonthInspect
				if batch {
					fn = batchFundingMonthInspect
				}
				start := time.Now()
				got, err := fn(t.Context(), f.db, f.key, f.scope, f.now)
				us := time.Since(start).Microseconds()
				if err != nil || haveResult && !reflect.DeepEqual(result, got) {
					t.Fatalf("timing path changed result or exceeded original budget: %s batch=%t err=%v", spec.name, batch, err)
				}
				result = got
				haveResult = true
				if batch {
					s.Batch = append(s.Batch, us)
				} else {
					s.Old = append(s.Old, us)
				}
			}
		}
		median := func(v []int64) int64 {
			a := append([]int64(nil), v...)
			sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
			return a[len(a)/2]
		}
		s.OldMedian, s.BatchMedian = median(s.Old), median(s.Batch)
		t.Logf("%s old median=%dus batch median=%dus", s.Name, s.OldMedian, s.BatchMedian)
		samples = append(samples, s)
		_ = f.db.Close() // Release each synthetic memory database before the next.
	}
	raw, err := json.MarshalIndent(struct {
		Warning string
		Samples []sample
	}{"Nine alternating local in-memory samples; not production, disk I/O, network, p95 or statistical significance.", samples}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("../../docs/sql-audit-20261126", "funding-heads-integrated-20261127.json"), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
