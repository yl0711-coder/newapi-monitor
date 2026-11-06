package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

type usageTestDriver struct {
	s        *walletDriverState
	data     [][]driver.Value
	queryErr error
	rowErr   error
	block    chan struct{}
}

type usageTestConn struct {
	*walletConn
	d *usageTestDriver
}

func (d *usageTestDriver) Open(string) (driver.Conn, error) {
	return &usageTestConn{&walletConn{d.s}, d}, nil
}

func (c *usageTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != usageWindowQuery {
		return c.walletConn.QueryContext(ctx, query, args)
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded usage query")
	}
	c.s.mu.Lock()
	c.s.queries = append(c.s.queries, query)
	c.s.args = append(c.s.args, append([]driver.NamedValue(nil), args...))
	block, queryErr, rowErr, data := c.d.block, c.d.queryErr, c.d.rowErr, c.d.data
	c.s.mu.Unlock()
	if block != nil {
		close(block)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if queryErr != nil {
		return nil, queryErr
	}
	return &walletRows{columns: []string{"user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}, data: data, err: rowErr}, nil
}

func usageTestSource(t *testing.T) (*sql.DB, *usageTestDriver, UsageWindowRequest) {
	t.Helper()
	s := &walletDriverState{uuid: "synthetic-usage-server", database: "synthetic-usage-db", data: [][]driver.Value{{int64(201), int64(-1), int64(200000000)}}}
	d := &usageTestDriver{s: s, data: [][]driver.Value{usageTestRow(2, "wallet", "1500000", 1500000)}}
	name := fmt.Sprintf("monthly-usage-%d", walletDriverSequence.Add(1))
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
	return db, d, UsageWindowRequest{SourceIdentity: pin, Rules: fundingTestRules(), UserID: 201, From: 1780000000, To: 1780003600}
}

func usageTestRow(kind int64, source, total string, quota int64) []driver.Value {
	return []driver.Value{int64(201), kind, source, int64(1), int64(1), total, quota, quota, int64(11), int64(11), int64(1780000010), int64(1780000010)}
}

func usageFixture(t *testing.T) UsageWindowObservation {
	t.Helper()
	db, _, req := usageTestSource(t)
	o, err := CaptureUsageWindow(context.Background(), db, req)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestUsageWindowSameReadOnlyViewExactMoneyAndSources(t *testing.T) {
	db, d, req := usageTestSource(t)
	d.data = [][]driver.Value{
		usageTestRow(2, "subscription", "0", 0),
		usageTestRow(2, "unspecified", "5", 5),
		usageTestRow(2, "wallet", "9007199254740993", 9007199254740993),
		usageTestRow(6, "wallet", "500000", 500000),
	}
	o, err := CaptureUsageWindow(context.Background(), db, req)
	if err != nil || len(o.Rows) != 4 || o.Rows[2].Quota != 9007199254740993 || o.Rows[3].Quota != 500000 || *o.Wallet.BalanceQuota != -1 {
		t.Fatal("integer precision, refund, source or signed balance changed", o, err)
	}
	if o.Rows[1].BillingSource != "unspecified" || o.Rows[0].BillingSource != "subscription" {
		t.Fatal("non-wallet sources treated as wallet")
	}
	d.s.mu.Lock()
	if !d.s.options.ReadOnly || d.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !d.s.committed || len(d.s.queries) != 3 ||
		d.s.queries[2] != usageWindowQuery || !reflect.DeepEqual(d.s.args[2], []driver.NamedValue{{Ordinal: 1, Value: req.UserID}, {Ordinal: 2, Value: req.From}, {Ordinal: 3, Value: req.To}}) {
		t.Fatal("same transaction or bounded query contract changed", d.s.queries)
	}
	d.s.mu.Unlock()
	if db.Stats().InUse != 0 {
		t.Fatal("source connection retained")
	}
	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"complete"`, `"wallet_only"`, `"month"`, `"reviewed"`, `"admin_info"`, `"content"`, `"username"`, `"email"`, `"prompt"`} {
		if strings.Contains(string(raw), field) {
			t.Fatal("unearned financial assertion or private raw fields", field)
		}
	}
}

func TestUsageWindowScopeAndIdentityRejectBeforeDataRead(t *testing.T) {
	for _, scenario := range []string{"nil_context", "nil_db", "bad_source", "wrong_server", "wrong_database", "bad_unit", "bad_producer", "bad_user", "zero_from", "reversed", "too_long", "not_closed"} {
		t.Run(scenario, func(t *testing.T) {
			db, d, req := usageTestSource(t)
			ctx := context.Background()
			switch scenario {
			case "nil_context":
				ctx = nil
			case "nil_db":
				db = nil
			case "bad_source":
				req.SourceIdentity = ""
			case "wrong_server":
				d.s.uuid = "different-server"
			case "wrong_database":
				d.s.database = "different-database"
			case "bad_unit":
				req.Rules.QuotaPerUnit = 1
			case "bad_producer":
				req.Rules.Producer = "unknown"
			case "bad_user":
				req.UserID = 0
			case "zero_from":
				req.From = 0
			case "reversed":
				req.To = req.From
			case "too_long":
				req.To = req.From + 86401
			case "not_closed":
				req.To, req.From = time.Now().Unix(), time.Now().Unix()-3600
			}
			if o, err := CaptureUsageWindow(ctx, db, req); err == nil || o.Version != 0 || o.Rows != nil {
				t.Fatal("invalid scope produced observation", err)
			}
			if len(d.s.queries) > 1 {
				t.Fatal("source data queried before scope and identity validation")
			}
		})
	}
}

func TestUsageWindowFailuresNoPartialDataOrRetry(t *testing.T) {
	for _, scenario := range []string{"begin", "wallet_query", "wallet_null", "wallet_foreign", "usage_query", "rows", "commit", "null_count", "null_sum", "overflow", "fraction", "negative", "bad_count", "sum_mismatch", "bad_id", "too_many_ids", "wrong_user", "wrong_type", "wrong_source", "wrong_time", "duplicate", "unordered"} {
		t.Run(scenario, func(t *testing.T) {
			db, d, req := usageTestSource(t)
			fail := errors.New("synthetic transport failure")
			r := d.data[0]
			switch scenario {
			case "begin":
				d.s.beginErr = fail
			case "wallet_query":
				d.s.queryErr = fail
			case "wallet_null":
				d.s.data[0][1] = nil
			case "wallet_foreign":
				d.s.data[0][0] = int64(999)
			case "usage_query":
				d.queryErr = fail
			case "rows":
				d.rowErr = fail
			case "commit":
				d.s.commitErr = fail
			case "null_count":
				r[4] = int64(0)
			case "null_sum":
				r[5] = nil
			case "overflow":
				r[5] = "9223372036854775808"
			case "fraction":
				r[5] = "1500000.0"
			case "negative":
				r[5], r[6] = "-1", int64(-1)
			case "bad_count":
				r[3], r[4] = int64(0), int64(0)
			case "sum_mismatch":
				r[5] = "1499999"
			case "bad_id":
				r[8] = int64(0)
			case "too_many_ids":
				r[3], r[4] = int64(2), int64(2)
			case "wrong_user":
				r[0] = int64(999)
			case "wrong_type":
				r[1] = int64(3)
			case "wrong_source":
				r[2] = "Wallet"
			case "wrong_time":
				r[11] = req.To
			case "duplicate":
				d.data = append(d.data, append([]driver.Value(nil), r...))
			case "unordered":
				d.data = append(d.data, usageTestRow(2, "subscription", "0", 0))
			}
			if o, err := CaptureUsageWindow(context.Background(), db, req); err == nil || o.Version != 0 || o.Rows != nil {
				t.Fatal("failure returned partial or fabricated observation", err)
			}
			if d.s.committed || len(d.s.queries) > 3 || db.Stats().InUse != 0 {
				t.Fatal("failed transaction committed, retried or leaked connection")
			}
			if scenario != "begin" && scenario != "commit" && !d.s.rolledBack {
				t.Fatal("source transaction not rolled back")
			}
		})
	}
}

func TestUsageWindowEmptyAndMissingWalletAreExplicit(t *testing.T) {
	db, d, req := usageTestSource(t)
	d.data, d.s.data = nil, nil
	o, err := CaptureUsageWindow(context.Background(), db, req)
	if err != nil || o.Rows == nil || len(o.Rows) != 0 || o.Wallet.Exists || o.Wallet.BalanceQuota != nil || o.Wallet.UsedQuota != nil {
		t.Fatal("empty query or missing account became unknown/zero balance", o, err)
	}
	// Historic logs may remain after a user is deleted. Keep their existence;
	// the later producer must not interpret a missing wallet as a zero balance.
	d.data = [][]driver.Value{usageTestRow(2, "unspecified", "0", 0)}
	if o, err := CaptureUsageWindow(context.Background(), db, req); err != nil || len(o.Rows) != 1 || o.Wallet.Exists {
		t.Fatal("retained history lost when user is absent", err)
	}
}

func TestUsageWindowSharedConcurrencyCancellationAndRecovery(t *testing.T) {
	db, d, req := usageTestSource(t)
	d.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := CaptureUsageWindow(ctx, db, req); done <- err }()
	select {
	case <-d.block:
	case <-time.After(time.Second):
		t.Fatal("usage read did not start")
	}
	other, _, pin := walletTestSource(t, nil)
	if _, err := CaptureWallet(context.Background(), other, pin, []int64{201}); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("usage and wallet capture budgets not shared", err)
	}
	if _, err := CaptureUsageWindow(context.Background(), db, req); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("parallel usage reads not bounded", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not release source")
	}
	if _, err := CaptureWallet(context.Background(), other, pin, []int64{201}); err != nil {
		t.Fatal("shared slot not recovered", err)
	}
	d.block = nil
	if _, err := CaptureUsageWindow(context.Background(), db, req); err != nil {
		t.Fatal("usage read did not recover", err)
	}
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	d.block = make(chan struct{})
	if o, err := CaptureUsageWindow(deadline, db, req); !errors.Is(err, context.DeadlineExceeded) || o.Version != 0 {
		t.Fatal("deadline returned partial capture", err)
	}
}

func TestUsageWindowStructuralValidationAndClock(t *testing.T) {
	o := usageFixture(t)
	for _, scenario := range []string{"basis", "unit", "null_rows", "wallet_target", "missing_balance", "missing_used", "absent_with_money", "negative_used", "backward", "too_slow", "row_amount", "row_source"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(o)
			var x UsageWindowObservation
			if err := json.Unmarshal(raw, &x); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "basis":
				x.Basis = "exact_month_boundary"
			case "unit":
				x.Request.Rules.QuotaPerUnit = 1
			case "null_rows":
				x.Rows = nil
			case "wallet_target":
				x.Wallet.UserID++
			case "missing_balance":
				x.Wallet.BalanceQuota = nil
			case "missing_used":
				x.Wallet.UsedQuota = nil
			case "absent_with_money":
				x.Wallet.Exists = false
			case "negative_used":
				*x.Wallet.UsedQuota = -1
			case "backward":
				x.CompletedAtMS = x.StartedAtMS - 1
			case "too_slow":
				x.CompletedAtMS = x.StartedAtMS + 5001
			case "row_amount":
				x.Rows[0].Quota++
			case "row_source":
				x.Rows[0].BillingSource = ""
			}
			if err := ValidateUsageWindowObservation(x); err == nil {
				t.Fatal("invalid capture accepted", scenario)
			}
		})
	}
	db, _, req := usageTestSource(t)
	times := []time.Time{time.Now(), time.Now().Add(-time.Second)}
	i := 0
	if o, err := captureUsageWindow(context.Background(), db, req, func() time.Time { v := times[i]; i++; return v }); err == nil || o.Version != 0 {
		t.Fatal("clock regression accepted")
	}
	// Integer bounds must not overflow during aggregate self-consistency checks.
	r := UsageWindowRow{Records: math.MaxInt64, Quota: math.MaxInt64, MinQuota: math.MaxInt64, MaxQuota: math.MaxInt64}
	if validUsageWindowAmount(r) {
		t.Fatal("multiplication overflow accepted")
	}
}
