package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fundingTestDriver struct {
	s        *walletDriverState
	fence    int64
	fenceErr error
}
type fundingTestConn struct {
	*walletConn
	d *fundingTestDriver
}

func (d *fundingTestDriver) Open(string) (driver.Conn, error) {
	return &fundingTestConn{&walletConn{d.s}, d}, nil
}
func (c *fundingTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == "SELECT @@server_uuid, DATABASE()" {
		return c.walletConn.QueryContext(ctx, query, args)
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded funding query")
	}
	c.s.mu.Lock()
	c.s.queries = append(c.s.queries, query)
	c.s.args = append(c.s.args, append([]driver.NamedValue(nil), args...))
	if strings.Contains(query, "MAX(id)") {
		fence, err := c.d.fence, c.d.fenceErr
		uuid, database := c.s.uuid, c.s.database
		c.s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if strings.Contains(query, "@@server_uuid") {
			return &walletRows{columns: []string{"uuid", "database", "fence"}, data: [][]driver.Value{{uuid, database, fence}}}, nil
		}
		return &walletRows{columns: []string{"fence"}, data: [][]driver.Value{{fence}}}, nil
	}
	block, queryErr, rowErr, data := c.s.block, c.s.queryErr, c.s.rowErr, c.s.data
	c.s.mu.Unlock()
	if block != nil {
		close(block)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if queryErr != nil {
		return nil, queryErr
	}
	return &walletRows{columns: []string{"id", "user_id", "created_at", "type", "content", "other"}, data: data, err: rowErr}, nil
}

func fundingTestSource(t *testing.T, n int) (*sql.DB, *fundingTestDriver, FundingScan) {
	t.Helper()
	s := &walletDriverState{uuid: "funding-synthetic-server", database: "funding-synthetic-db"}
	d := &fundingTestDriver{s: s, fence: 1000}
	name := fmt.Sprintf("funding-driver-%d", walletDriverSequence.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	pin, err := WalletSourceIdentity(s.uuid, s.database)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := NewFundingScan(pin, fundingTestRules(), []int64{202, 201}, 1780000000, 1780003600)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		s.data = append(s.data, []driver.Value{int64(i + 1), int64(9), scan.From + 10, int64(3), "", fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_user_id":201}`)})
	}
	return db, d, scan
}

func TestFundingCaptureSameSecondLookaheadAndBeneficiaryScope(t *testing.T) {
	db, d, scan := fundingTestSource(t, 201)
	p, err := CaptureFundingPage(context.Background(), db, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Rows) != 200 || p.EndReached || p.Next.ID != 200 || p.Next.At != scan.From+10 || p.FenceID != 1000 {
		t.Fatal("lookahead skipped or falsely ended")
	}
	projection, err := ProjectFundingPage(p)
	if err != nil || len(projection.Movements) != 200 || len(projection.Review) != 0 || projection.Movements[0].TargetUserID != 201 {
		t.Fatal("operator incorrectly used as beneficiary", err)
	}
	d.s.mu.Lock()
	if !d.s.options.ReadOnly || d.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !d.s.committed || len(d.s.queries) != 3 || d.s.queries[2] != fundingPageQuery(scan.UserIDs) {
		t.Fatal("source transaction/query contract changed")
	}
	for i, want := range []int64{scan.From, scan.To, 1000, 0, 0, 0, 201, 202} {
		if d.s.args[2][i].Value != want {
			t.Fatal("query arguments not bound")
		}
	}
	d.s.data = d.s.data[200:]
	d.s.queries = nil
	d.s.args = nil
	d.s.mu.Unlock()
	second, err := CaptureFundingPage(context.Background(), db, scan, &p)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Rows) != 1 || second.Rows[0].ID != 201 || !second.EndReached || second.After != p.Next || second.FenceID != p.FenceID {
		t.Fatal("same-second continuation lost record")
	}
	d.s.mu.Lock()
	if len(d.s.queries) != 2 || d.s.args[1][3].Value != scan.From+10 || d.s.args[1][5].Value != int64(200) {
		t.Fatal("continuation widened fence or lost cursor")
	}
	d.s.mu.Unlock()
	if db.Stats().InUse != 0 {
		t.Fatal("source transaction held")
	}
	if _, err := CaptureFundingPage(context.Background(), db, scan, &second); err == nil {
		t.Fatal("terminal page resumed")
	}
	changed := scan
	changed.UserIDs = []int64{201}
	if _, err := CaptureFundingPage(context.Background(), db, changed, &p); err == nil {
		t.Fatal("scope changed mid traversal")
	}
}

func TestFundingCaptureFailuresHaveNoPartialPage(t *testing.T) {
	for name, change := range map[string]func(*fundingTestDriver, *FundingScan){
		"begin":           func(d *fundingTestDriver, _ *FundingScan) { d.s.beginErr = errors.New("begin") },
		"fence":           func(d *fundingTestDriver, _ *FundingScan) { d.fenceErr = errors.New("fence") },
		"query":           func(d *fundingTestDriver, _ *FundingScan) { d.s.queryErr = errors.New("query") },
		"partial rows":    func(d *fundingTestDriver, _ *FundingScan) { d.s.rowErr = errors.New("rows") },
		"commit":          func(d *fundingTestDriver, _ *FundingScan) { d.s.commitErr = errors.New("commit") },
		"source changed":  func(d *fundingTestDriver, _ *FundingScan) { d.s.uuid = "other" },
		"negative fence":  func(d *fundingTestDriver, _ *FundingScan) { d.fence = -1 },
		"outside fence":   func(d *fundingTestDriver, _ *FundingScan) { d.fence = 0 },
		"null content":    func(d *fundingTestDriver, _ *FundingScan) { d.s.data[0][4] = nil },
		"null metadata":   func(d *fundingTestDriver, _ *FundingScan) { d.s.data[0][5] = nil },
		"oversized field": func(d *fundingTestDriver, _ *FundingScan) { d.s.data[0][4] = strings.Repeat("a", 4097) },
		"page byte cap": func(d *fundingTestDriver, _ *FundingScan) {
			for i := range d.s.data {
				d.s.data[i][5] = strings.Repeat("a", 65536)
			}
		},
		"duplicate id":   func(d *fundingTestDriver, _ *FundingScan) { d.s.data[1][0] = d.s.data[0][0] },
		"bad order":      func(d *fundingTestDriver, _ *FundingScan) { d.s.data[0], d.s.data[1] = d.s.data[1], d.s.data[0] },
		"upper boundary": func(d *fundingTestDriver, s *FundingScan) { d.s.data[0][2] = s.To },
		"before start":   func(d *fundingTestDriver, s *FundingScan) { d.s.data[0][2] = s.From - 1 },
		"excluded type":  func(d *fundingTestDriver, _ *FundingScan) { d.s.data[0][3] = int64(2) },
		"foreign topup":  func(d *fundingTestDriver, _ *FundingScan) { d.s.data[0][3] = int64(1) },
		"scan too wide":  func(_ *fundingTestDriver, s *FundingScan) { s.To++ },
		"open window":    func(_ *fundingTestDriver, s *FundingScan) { s.From = time.Now().Unix() - 60; s.To = time.Now().Unix() },
		"unknown rule":   func(_ *fundingTestDriver, s *FundingScan) { s.Rules.Producer = "future" },
	} {
		t.Run(name, func(t *testing.T) {
			db, d, scan := fundingTestSource(t, 20)
			change(d, &scan)
			page, err := CaptureFundingPage(context.Background(), db, scan, nil)
			if err == nil || page.Version != 0 || page.Rows != nil {
				t.Fatal("failure returned a usable partial page")
			}
			d.s.mu.Lock()
			defer d.s.mu.Unlock()
			if len(d.s.queries) > 3 {
				t.Fatal("automatic retry amplified failure")
			}
		})
	}
}

func TestFundingCaptureBusyCancelTimeoutAndRecovery(t *testing.T) {
	db, d, scan := fundingTestSource(t, 0)
	d.s.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := CaptureFundingPage(ctx, db, scan, nil); done <- err }()
	select {
	case <-d.s.block:
	case <-time.After(time.Second):
		t.Fatal("capture did not begin")
	}
	if _, err := CaptureFundingPage(context.Background(), db, scan, nil); !errors.Is(err, ErrFundingCaptureBusy) {
		t.Fatal("parallel collector was not bounded")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation held slot")
	}
	d.s.mu.Lock()
	d.s.block = nil
	d.s.mu.Unlock()
	page, err := CaptureFundingPage(context.Background(), db, scan, nil)
	if err != nil || !page.EndReached || len(page.Rows) != 0 || page.Next != (FundingCursor{}) {
		t.Fatal("empty traversal or recovery failed", err)
	}
	d.s.mu.Lock()
	d.s.block = make(chan struct{})
	d.s.mu.Unlock()
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := CaptureFundingPage(short, db, scan, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline not enforced", err)
	}
}

func fundingPageFixture(t *testing.T, n int) FundingPage {
	t.Helper()
	db, _, s := fundingTestSource(t, n)
	p, err := CaptureFundingPage(context.Background(), db, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func fundingNextFixture(t *testing.T, p FundingPage) FundingPage {
	t.Helper()
	raw, err := fundingPageBytes(p)
	if err != nil {
		t.Fatal(err)
	}
	next := p
	next.PreviousID = digest(raw)
	next.After = p.Next
	next.StartedAtMS = p.CompletedAtMS + 1
	next.CompletedAtMS = next.StartedAtMS
	next.EndReached = true
	row := p.Rows[len(p.Rows)-1]
	row.ID++
	next.Rows = []FundingLog{row}
	next.Next = FundingCursor{At: row.CreatedAt, ID: row.ID}
	return next
}

func TestFundingProjectionDoesNotMixOtherAccountsOrInventCompleteness(t *testing.T) {
	p := fundingPageFixture(t, 4)
	p.Rows[1].Other = fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_user_id":999}`)
	p.Rows[2].Other = fundingTestAudit("user.quota_add", `{"quota":"1 点额度","target_user_id":null}`)
	p.Rows[3].Type = 4
	p.Rows[3].UserID = 201
	p.Rows[3].Other = ""
	p.Rows[3].Content = "邀请用户赠送 $1.000000 额度"
	before := append([]FundingLog(nil), p.Rows...)
	projection, err := ProjectFundingPage(p)
	if err != nil || len(projection.Movements) != 1 || len(projection.Review) != 1 || len(projection.Separate) != 1 {
		t.Fatal("unrelated beneficiary counted or unknown evidence lost", err)
	}
	if !reflect.DeepEqual(p.Rows, before) {
		t.Fatal("source observation mutated")
	}
	if projection.Review[0].DeltaQuota != nil || projection.Review[0].TargetUserID != 0 {
		t.Fatal("unknown target or amount fabricated")
	}
}

func TestFundingPageRejectsInvalidBoundsBeforeStorageOrProjection(t *testing.T) {
	base := fundingPageFixture(t, 201)
	for name, change := range map[string]func(*FundingPage){
		"version":          func(p *FundingPage) { p.Version++ },
		"unordered scope":  func(p *FundingPage) { p.Scan.UserIDs = []int64{202, 201} },
		"duplicate scope":  func(p *FundingPage) { p.Scan.UserIDs = []int64{201, 201} },
		"unverified rules": func(p *FundingPage) { p.Scan.Rules.QuotaPerUnit = 1 },
		"invalid ID":       func(p *FundingPage) { p.Scan.ID = "bad" },
		"nil rows":         func(p *FundingPage) { p.Rows = nil; p.EndReached = true },
		"backward clock":   func(p *FundingPage) { p.CompletedAtMS = p.StartedAtMS - 1 },
		"over time limit":  func(p *FundingPage) { p.CompletedAtMS = p.StartedAtMS + fundingCaptureTimeout.Milliseconds() + 1 },
		"short nonterminal page": func(p *FundingPage) {
			p.Rows = p.Rows[:1]
			p.Next = FundingCursor{At: p.Rows[0].CreatedAt, ID: p.Rows[0].ID}
		},
		"too many rows":              func(p *FundingPage) { p.Rows = append(p.Rows, p.Rows[0]) },
		"initial skips prefix":       func(p *FundingPage) { p.After = FundingCursor{At: p.Scan.From, ID: 1} },
		"false predecessor":          func(p *FundingPage) { p.PreviousID = "invalid" },
		"predecessor missing cursor": func(p *FundingPage) { p.PreviousID = strings.Repeat("f", 64) },
		"duplicate event":            func(p *FundingPage) { p.Rows[1] = p.Rows[0] },
		"next points beyond data":    func(p *FundingPage) { p.Next.ID++ },
		"aggregate byte budget": func(p *FundingPage) {
			for i := range p.Rows {
				p.Rows[i].Other = strings.Repeat("a", 65536)
			}
		},
		"escaped serialized byte budget": func(p *FundingPage) {
			for i := range p.Rows {
				p.Rows[i].Content = strings.Repeat("\x00", 4000)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := base
			p.Rows = append([]FundingLog(nil), base.Rows...)
			p.Scan.UserIDs = append([]int64(nil), base.Scan.UserIDs...)
			change(&p)
			if _, err := fundingPageBytes(p); err == nil {
				t.Fatal("invalid page passed storage validation")
			}
			if projection, err := ProjectFundingPage(p); err == nil || len(projection.Movements) != 0 {
				t.Fatal("invalid page reached account projection")
			}
		})
	}
	for _, ids := range [][]int64{nil, {0}, {201, 201}, make([]int64, 201)} {
		if _, err := NewFundingScan(base.Scan.SourceIdentity, base.Scan.Rules, ids, base.Scan.From, base.Scan.To); err == nil {
			t.Fatal("invalid account scope accepted")
		}
	}
	if _, err := NewFundingScan(base.Scan.SourceIdentity, base.Scan.Rules, []int64{201}, base.Scan.From, base.Scan.To+1); err == nil {
		t.Fatal("oversize window accepted")
	}
}

func TestFundingCaptureEmptyFencePartialCancellationAndClockGuard(t *testing.T) {
	db, d, scan := fundingTestSource(t, 0)
	d.fence = 0
	p, err := CaptureFundingPage(context.Background(), db, scan, nil)
	if err != nil || !p.EndReached || p.FenceID != 0 || p.Rows == nil || len(p.Rows) != 0 {
		t.Fatal("empty source wrongly represented", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CaptureFundingPage(ctx, db, scan, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled request queried source")
	}
	calls := 0
	clock := func() time.Time {
		calls++
		return time.Unix(scan.To+120, 0).Add(time.Duration(calls-1) * (fundingCaptureTimeout + time.Second))
	}
	if p, err := captureFundingPage(context.Background(), db, scan, nil, clock); err == nil || p.Version != 0 {
		t.Fatal("capture beyond time budget returned page")
	}
}
