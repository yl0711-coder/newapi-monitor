package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type walletTailTestDriver struct {
	s                   *walletDriverState
	usage, funding      [][]driver.Value
	failQuery, failRows string
	failure             error
	blockQuery          string
	block               chan struct{}
}

type walletTailTestConn struct {
	*walletConn
	d *walletTailTestDriver
}

func (d *walletTailTestDriver) Open(string) (driver.Conn, error) {
	return &walletTailTestConn{&walletConn{d.s}, d}, nil
}

func (c *walletTailTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == c.d.blockQuery {
		close(c.d.block)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if query == c.d.failQuery {
		if c.d.failure != nil {
			return nil, c.d.failure
		}
		return nil, errors.New("synthetic source failure")
	}
	if query != walletTailUsageQuery && query != walletTailFundingQuery {
		return c.walletConn.QueryContext(ctx, query, args)
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded tail query")
	}
	c.s.mu.Lock()
	c.s.queries = append(c.s.queries, query)
	c.s.args = append(c.s.args, append([]driver.NamedValue(nil), args...))
	c.s.mu.Unlock()
	var rowErr error
	if query == c.d.failRows {
		rowErr = errors.New("synthetic row transfer failure")
	}
	if query == walletTailUsageQuery {
		return &walletRows{columns: []string{"user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}, data: c.d.usage, err: rowErr}, nil
	}
	data := [][]driver.Value{}
	after := FundingCursor{At: args[1].Value.(int64), ID: args[3].Value.(int64)}
	for _, row := range c.d.funding {
		if fundingCursorBefore(after, FundingCursor{At: row[2].(int64), ID: row[0].(int64)}) {
			data = append(data, row)
		}
		if len(data) == FundingPageLimit+1 {
			break
		}
	}
	return &walletRows{columns: []string{"id", "user_id", "created_at", "type", "content", "other"}, data: data, err: rowErr}, nil
}

func TestWalletTailFailureStagesDoNotLeakDriverDataOrLoseTimeout(t *testing.T) {
	for _, stage := range []string{"tail_usage", "tail_funding", "commit"} {
		t.Run(stage, func(t *testing.T) {
			db, d, req, now := walletTailTestSource(t)
			cause := &mysql.MySQLError{Number: 3024, Message: "synthetic-private-SQL-and-password"}
			if stage == "commit" {
				d.s.commitErr = cause
			} else {
				d.failQuery, d.failure = walletTailUsageQuery, cause
				if stage == "tail_funding" {
					d.failQuery = walletTailFundingQuery
				}
			}
			out, err := captureWalletTail(t.Context(), db, req, func() time.Time { return now })
			var classified *mysql.MySQLError
			if WalletTailFailureStage(err) != stage || !errors.Is(err, cause) || !errors.As(err, &classified) || classified.Number != 3024 ||
				strings.Contains(err.Error(), cause.Message) || !reflect.DeepEqual(out, WalletTailObservation{}) || db.Stats().InUse != 0 {
				t.Fatal("stage leaked details, lost classification, returned partial data or retained the source connection", stage)
			}
		})
	}
	db, d, req, now := walletTailTestSource(t)
	d.failQuery, d.failure = walletTailUsageQuery, context.DeadlineExceeded
	_, err := captureWalletTail(t.Context(), db, req, func() time.Time { return now })
	if !errors.Is(err, context.DeadlineExceeded) || WalletTailFailureStage(err) != "tail_usage" || WalletTailFailureStage(errors.New("unrelated")) != "" || WalletTailFailureStage(&walletTailCaptureFailure{stage: "unsupported", cause: err}) != "" {
		t.Fatal("timeout became an integrity error or arbitrary stages were exposed")
	}
}

func walletTailTestSource(t *testing.T) (*sql.DB, *walletTailTestDriver, WalletTailRequest, time.Time) {
	t.Helper()
	boundary := time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)).Unix()
	now := time.Unix(boundary+14*86400, 123000000)
	s := &walletDriverState{uuid: "synthetic-tail", database: "synthetic-tail-db",
		data: [][]driver.Value{{int64(201), boundary - 86400, int64(1720), int64(480)}}}
	d := &walletTailTestDriver{s: s,
		usage:   [][]driver.Value{walletTailUsageRow(2, 300, boundary+1), walletTailUsageRow(6, 20, now.Unix())},
		funding: [][]driver.Value{{int64(31), int64(100), boundary + 2, int64(3), "audited target, not actor", fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"1000 点额度"}`)}}}
	name := fmt.Sprintf("monthly-tail-%d", walletDriverSequence.Add(1))
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
	return db, d, WalletTailRequest{SourceIdentity: pin, Rules: fundingTestRules(), UserID: 201, Boundary: boundary}, now
}

func walletTailUsageRow(kind, quota, at int64) []driver.Value {
	return []driver.Value{int64(201), kind, "wallet", int64(1), int64(1), fmt.Sprint(quota), quota, quota, kind + 10, kind + 10, at, at}
}

func walletTailFixture(t *testing.T) (WalletTailObservation, WalletTailObservation) {
	t.Helper()
	db, d, req, now := walletTailTestSource(t)
	a, err := captureWalletTail(context.Background(), db, req, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	d.s.data[0][2], d.s.data[0][3] = int64(1880), int64(520)
	d.usage = [][]driver.Value{walletTailUsageRow(2, 350, req.Boundary+1), walletTailUsageRow(6, 30, now.Unix())}
	d.funding = append(d.funding, []driver.Value{int64(32), int64(201), now.Unix(), int64(1), "使用Creem充值成功，充值额度: 200，支付金额：0.01", ""})
	now = now.Add(5 * time.Minute)
	b, err := captureWalletTail(context.Background(), db, req, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestWalletTailSameViewIncludesCurrentSecondAndExactTarget(t *testing.T) {
	db, d, req, now := walletTailTestSource(t)
	d.funding = append(d.funding, []driver.Value{int64(32), int64(100), now.Unix(), int64(3), "other beneficiary", fundingTestAudit("user.quota_add", `{"target_user_id":999,"quota":"9007199254740993 点额度"}`)})
	o, err := captureWalletTail(context.Background(), db, req, func() time.Time { return now })
	if err != nil || len(o.Funding) != 2 || o.Usage[1].LastAt != now.Unix() {
		t.Fatal("same-second data or actor/beneficiary rows omitted", err)
	}
	if balance, used, err := deriveWalletTail(o); err != nil || balance != 1000 || used != 200 {
		t.Fatal("boundary uses wrong target/refund/sign", balance, used, err)
	}
	if !d.s.options.ReadOnly || d.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !d.s.committed || len(d.s.queries) != 4 ||
		d.s.queries[2] != walletTailUsageQuery || d.s.queries[3] != walletTailFundingQuery || len(d.s.args[2]) != 2 || len(d.s.args[3]) != 9 || db.Stats().InUse != 0 {
		t.Fatal("snapshot/budget/query scope or connection release changed", d.s.queries)
	}
	for _, query := range []string{walletTailUsageQuery, walletTailFundingQuery} {
		if strings.Contains(query, "created_at<?") || strings.Contains(query, "id<=?") || !strings.Contains(query, "MAX_EXECUTION_TIME(2000)") || !strings.Contains(query, "FORCE INDEX(idx_user_created_type)") {
			t.Fatal("wall clock/fence cutoff or index/time budget changed")
		}
	}
	if o.StartedAtMS != now.UnixMilli() || o.CompletedAtMS != now.UnixMilli() || *o.User.BalanceQuota != 1720 {
		t.Fatal("physical original retimed to a month boundary")
	}
}

func TestWalletTailIndependentBoundaryAndOriginalsUnchanged(t *testing.T) {
	a, b := walletTailFixture(t)
	beforeA, _ := json.Marshal(a)
	beforeB, _ := json.Marshal(b)
	result, err := DeriveWalletTailBoundary(a, b)
	x, y := result.OriginalHashes()
	if err != nil || result.UserID() != 201 || result.Boundary() != a.Request.Boundary || result.RegisteredAt() != a.User.RegisteredAt ||
		result.SourceIdentity() != a.Request.SourceIdentity || result.BalanceQuota() != 1000 || result.UsedQuota() != 200 || x != digest(beforeA) || y != digest(beforeB) || x == y {
		t.Fatal("independent moving wallets did not derive identical exact boundary", result, err)
	}
	afterA, _ := json.Marshal(a)
	afterB, _ := json.Marshal(b)
	if !reflect.DeepEqual(beforeA, afterA) || !reflect.DeepEqual(beforeB, afterB) {
		t.Fatal("projection mutated physical originals")
	}
	resultType := reflect.TypeOf(result)
	for i := 0; i < resultType.NumField(); i++ {
		if resultType.Field(i).IsExported() {
			t.Fatal("opaque calculation became a decodable financial assertion")
		}
	}
}

func TestWalletTailSourceScopeFailureReturnsNoPartial(t *testing.T) {
	for _, scenario := range []string{"nil_context", "nil_db", "nil_clock", "bad_source", "wrong_server", "bad_unit", "bad_producer", "bad_user", "older_month", "not_month_start", "just_started_month", "cancelled", "begin", "wallet_query", "missing_user", "null_wallet", "float_wallet", "negative_used", "usage_query", "usage_rows", "funding_query", "funding_rows", "null_funding", "future_funding", "future_usage", "pre_registration_usage", "commit", "cross_month", "too_long"} {
		t.Run(scenario, func(t *testing.T) {
			db, d, r, now := walletTailTestSource(t)
			ctx := context.Background()
			clock := func() time.Time { return now }
			switch scenario {
			case "nil_context":
				ctx = nil
			case "nil_db":
				db = nil
			case "nil_clock":
				clock = nil
			case "bad_source":
				r.SourceIdentity = ""
			case "wrong_server":
				d.s.uuid = "changed"
			case "bad_unit":
				r.Rules.QuotaPerUnit = 1
			case "bad_producer":
				r.Rules.Producer = "unknown"
			case "bad_user":
				r.UserID = 0
			case "older_month":
				r.Boundary -= 30 * 86400
			case "not_month_start":
				r.Boundary++
			case "just_started_month":
				now = time.Unix(r.Boundary+59, 0)
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "begin":
				d.s.beginErr = errors.New("begin failed")
			case "wallet_query":
				d.s.queryErr = errors.New("wallet failed")
			case "missing_user":
				d.s.data = nil
			case "null_wallet":
				d.s.data[0][2] = nil
			case "float_wallet":
				d.s.data[0][2] = float64(1720)
			case "negative_used":
				d.s.data[0][3] = int64(-1)
			case "usage_query":
				d.failQuery = walletTailUsageQuery
			case "usage_rows":
				d.failRows = walletTailUsageQuery
			case "funding_query":
				d.failQuery = walletTailFundingQuery
			case "funding_rows":
				d.failRows = walletTailFundingQuery
			case "null_funding":
				d.funding[0][4] = nil
			case "future_funding":
				d.funding[0][2] = now.Unix() + 1
			case "future_usage":
				d.usage[1] = walletTailUsageRow(6, 20, now.Unix()+1)
			case "pre_registration_usage":
				d.s.data[0][1] = r.Boundary + 2
			case "commit":
				d.s.commitErr = errors.New("commit failed")
			case "cross_month":
				count := 0
				clock = func() time.Time {
					count++
					if count == 1 {
						return time.Unix(walletTailMonthStart(now.AddDate(0, 1, 0).UnixMilli())-1, 0)
					}
					return time.Unix(walletTailMonthStart(now.AddDate(0, 1, 0).UnixMilli()), 0)
				}
			case "too_long":
				count := 0
				clock = func() time.Time { count++; return now.Add(time.Duration(count-1) * 6 * time.Second) }
			}
			o, err := captureWalletTail(ctx, db, r, clock)
			if err == nil || !reflect.DeepEqual(o, WalletTailObservation{}) || d.s.committed {
				t.Fatal("invalid source committed or returned partial observation", err)
			}
			if db != nil && db.Stats().InUse != 0 {
				t.Fatal("connection leaked on failure")
			}
		})
	}
}

func TestWalletTailFundingPaginationAndLimits(t *testing.T) {
	for _, count := range []int{0, 200, 201, 400, 2000, 2001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db, d, r, now := walletTailTestSource(t)
			d.funding = nil
			for i := 0; i < count; i++ {
				d.funding = append(d.funding, []driver.Value{int64(i + 1), int64(201), r.Boundary + 1, int64(3), "管理员增加用户额度 1 点额度", ""})
			}
			o, err := captureWalletTail(context.Background(), db, r, func() time.Time { return now })
			if count > walletTailRowsLimit {
				if err == nil || o.Version != 0 || d.s.committed {
					t.Fatal("oversized tail silently truncated")
				}
				return
			}
			if err != nil || len(o.Funding) != count {
				t.Fatal("page dropped or duplicated a row", len(o.Funding), err)
			}
			wantPages := (count + FundingPageLimit - 1) / FundingPageLimit
			if wantPages == 0 {
				wantPages = 1
			}
			if len(d.s.queries) != 3+wantPages {
				t.Fatal("unexpected page count", len(d.s.queries), wantPages)
			}
		})
	}
	for _, scenario := range []string{"page_bytes", "total_bytes", "duplicate", "bad_amount", "null_usage"} {
		t.Run(scenario, func(t *testing.T) {
			db, d, r, now := walletTailTestSource(t)
			switch scenario {
			case "page_bytes", "total_bytes":
				d.funding = nil
				count, bytes := 17, 65536
				if scenario == "total_bytes" {
					count, bytes = 801, 3000
				}
				for i := 0; i < count; i++ {
					d.funding = append(d.funding, []driver.Value{int64(i + 1), int64(201), r.Boundary + 1, int64(3), "x", strings.Repeat(" ", bytes)})
				}
			case "duplicate":
				d.funding = append(d.funding, d.funding[0])
			case "bad_amount":
				d.usage[0][5] = "9223372036854775808"
			case "null_usage":
				d.usage[0][4] = int64(0)
			}
			if o, err := captureWalletTail(context.Background(), db, r, func() time.Time { return now }); err == nil || o.Version != 0 || d.s.committed {
				t.Fatal("invalid/oversized source admitted", err)
			}
		})
	}
}

func TestWalletTailPairRejectsUncertainOrChangedMoney(t *testing.T) {
	for _, scenario := range []string{"same_view", "overlap", "less_than_five_minutes", "different_source", "different_user", "different_registration", "changed_balance", "changed_used", "unknown_source", "subscription", "invalid_json", "unknown_funding", "unknown_admin", "created_target", "pre_registration_funding", "registration_nonzero", "negative_boundary_used", "overflow_balance", "malformed_original"} {
		t.Run(scenario, func(t *testing.T) {
			a, b := walletTailFixture(t)
			switch scenario {
			case "same_view":
				b = a
			case "overlap":
				b.StartedAtMS = a.StartedAtMS - 1
			case "less_than_five_minutes":
				b.StartedAtMS--
			case "different_source":
				b.Request.SourceIdentity = strings.Repeat("a", 64)
			case "different_user":
				b.Request.UserID++
			case "different_registration":
				b.User.RegisteredAt--
			case "changed_balance":
				*b.User.BalanceQuota++
			case "changed_used":
				*b.User.UsedQuota++
			case "unknown_source":
				b.Usage[0].BillingSource = "unspecified"
			case "subscription":
				b.Usage[0].BillingSource = "subscription"
			case "invalid_json":
				b.Usage[0].BillingSource = "invalid_json"
			case "unknown_funding":
				b.Funding[1].Content = "unknown money"
			case "unknown_admin":
				b.Funding[0].Other = `{"op":{"action":"future.quota","params":{}}}`
			case "created_target":
				b.Funding[0].Other = fundingTestAudit("user.create", `{"id":201,"username":"new"}`)
			case "pre_registration_funding":
				a.User.RegisteredAt = a.Request.Boundary + 2
				b.User.RegisteredAt = a.User.RegisteredAt
				a.Usage = []UsageWindowRow{}
				b.Usage = []UsageWindowRow{}
				b.Funding[0].CreatedAt = b.Request.Boundary + 1
			case "registration_nonzero":
				a.User.RegisteredAt = a.Request.Boundary
				b.User.RegisteredAt = a.User.RegisteredAt
			case "negative_boundary_used":
				*b.User.UsedQuota = 0
			case "overflow_balance":
				*b.User.BalanceQuota = math.MaxInt64
				b.Funding = []FundingLog{}
			case "malformed_original":
				b.Basis = "month_end_wallet"
			}
			if got, err := DeriveWalletTailBoundary(a, b); err == nil || got != (WalletTailBoundary{}) {
				t.Fatal("uncertain original became a boundary", scenario, err)
			}
		})
	}
}

func TestWalletTailExactIntegersRegistrationAndRefunds(t *testing.T) {
	a, b := walletTailFixture(t)
	for _, o := range []*WalletTailObservation{&a, &b} {
		o.Funding = []FundingLog{}
		o.Usage = []UsageWindowRow{}
		*o.User.BalanceQuota = 9007199254740993
		*o.User.UsedQuota = math.MaxInt64
	}
	got, err := DeriveWalletTailBoundary(a, b)
	if err != nil || got.BalanceQuota() != 9007199254740993 || got.UsedQuota() != math.MaxInt64 {
		t.Fatal("int64 passed through float", got, err)
	}
	for _, o := range []*WalletTailObservation{&a, &b} {
		o.User.RegisteredAt = o.Request.Boundary + 1
		*o.User.BalanceQuota = 1000
		*o.User.UsedQuota = 0
		o.Funding = []FundingLog{{ID: 1, UserID: 201, CreatedAt: o.User.RegisteredAt, Type: 3, Content: "管理员增加用户额度 1000 点额度", Other: ""}}
	}
	got, err = DeriveWalletTailBoundary(a, b)
	if err != nil || got.BalanceQuota() != 0 || got.UsedQuota() != 0 {
		t.Fatal("registered-inside-month zero must be derived, not assumed", got, err)
	}
	for _, o := range []*WalletTailObservation{&a, &b} {
		o.User.RegisteredAt = o.Request.Boundary - 1
		*o.User.BalanceQuota = -1
		*o.User.UsedQuota = 0
		o.Funding = []FundingLog{}
		o.Usage = []UsageWindowRow{{UserID: 201, Type: 6, BillingSource: "wallet", Records: 1, Quota: 1, MinQuota: 1, MaxQuota: 1, MinID: 10, MaxID: 10, FirstAt: o.Request.Boundary + 1, LastAt: o.Request.Boundary + 1}}
	}
	got, err = DeriveWalletTailBoundary(a, b)
	if err != nil || got.BalanceQuota() != -2 || got.UsedQuota() != 1 {
		t.Fatal("negative balance or refund direction changed", got, err)
	}
}

func TestWalletTailSimulatedMovingWalletsPreserveBoundary(t *testing.T) {
	a, b := walletTailFixture(t)
	rng := rand.New(rand.NewSource(20261030))
	for i := 0; i < 2000; i++ {
		balance, used := rng.Int63n(1<<52)-100000, rng.Int63n(1<<52)
		for _, o := range []*WalletTailObservation{&a, &b} {
			gross, refund, funds := rng.Int63n(1000000), rng.Int63n(1000000), rng.Int63n(1000000)
			// Positive current counters, independent gross/refunds and wallets.
			if used+gross-refund < 0 {
				refund = 0
			}
			*o.User.BalanceQuota = balance + funds - gross + refund
			*o.User.UsedQuota = used + gross - refund
			o.Usage = []UsageWindowRow{}
			for _, x := range []struct{ kind, amount int64 }{{2, gross}, {6, refund}} {
				o.Usage = append(o.Usage, UsageWindowRow{UserID: 201, Type: int(x.kind), BillingSource: "wallet", Records: 1, Quota: x.amount, MinQuota: x.amount, MaxQuota: x.amount, MinID: x.kind, MaxID: x.kind, FirstAt: o.Request.Boundary + 1, LastAt: o.Request.Boundary + 1})
			}
			o.Funding = []FundingLog{}
			if funds > 0 {
				o.Funding = append(o.Funding, FundingLog{ID: 1, UserID: 201, CreatedAt: o.Request.Boundary + 1, Type: 3, Content: fmt.Sprintf("管理员增加用户额度 %d 点额度", funds), Other: ""})
			}
		}
		got, err := DeriveWalletTailBoundary(a, b)
		if err != nil || got.BalanceQuota() != balance || got.UsedQuota() != used {
			t.Fatal("simulated moving wallets changed history", i, got, err)
		}
	}
}

func TestWalletTailIntermediateSumsDoNotOverflow(t *testing.T) {
	a, b := walletTailFixture(t)
	for _, o := range []*WalletTailObservation{&a, &b} {
		*o.User.BalanceQuota, *o.User.UsedQuota = math.MaxInt64, 0
		o.Usage, o.Funding = []UsageWindowRow{}, []FundingLog{}
		for i, direction := range []string{"增加", "增加", "减少", "减少"} {
			o.Funding = append(o.Funding, FundingLog{ID: int64(i + 1), UserID: 201, CreatedAt: o.Request.Boundary + 1, Type: 3,
				Content: fmt.Sprintf("管理员%s用户额度 %d 点额度", direction, int64(math.MaxInt64)), Other: ""})
		}
	}
	got, err := DeriveWalletTailBoundary(a, b)
	if err != nil || got.BalanceQuota() != math.MaxInt64 || got.UsedQuota() != 0 {
		t.Fatal("safe final amount lost to an overflowing intermediate sum", got, err)
	}
}

func TestWalletTailCancellationAndSharedLane(t *testing.T) {
	db, d, r, now := walletTailTestSource(t)
	d.blockQuery, d.block = walletTailUsageQuery, make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := captureWalletTail(ctx, db, r, func() time.Time { return now }); done <- err }()
	select {
	case <-d.block:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("source did not block")
	}
	if _, err := captureWalletTail(context.Background(), db, r, func() time.Time { return now }); !errors.Is(err, ErrWalletCaptureBusy) {
		cancel()
		t.Fatal("another view bypassed shared lane", err)
	}
	if _, err := CaptureRegisteredWallet(context.Background(), db, r.SourceIdentity, []int64{201}); !errors.Is(err, ErrWalletCaptureBusy) {
		cancel()
		t.Fatal("registered collector bypassed shared lane", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop view")
	}
	// database/sql may finish its cancellation-triggered rollback on a helper
	// goroutine after returning the context error. Require bounded release, not
	// an invalid assumption that this asynchronous cleanup is synchronous.
	deadline := time.Now().Add(time.Second)
	for db.Stats().InUse != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if db.Stats().InUse != 0 || len(walletCaptureSlots) != 0 || d.s.committed {
		t.Fatal("cancelled capture retained resource or committed")
	}
}
