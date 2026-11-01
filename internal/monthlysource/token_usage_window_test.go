package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type tokenUsageTestDriver struct {
	usage            *usageTestDriver
	data             [][]driver.Value
	queryErr, rowErr error
}
type tokenUsageTestConn struct {
	*usageTestConn
	d *tokenUsageTestDriver
}

func (d *tokenUsageTestDriver) Open(string) (driver.Conn, error) {
	return &tokenUsageTestConn{&usageTestConn{&walletConn{d.usage.s}, d.usage}, d}, nil
}
func (c *tokenUsageTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != tokenUsageWindowQuery {
		return c.usageTestConn.QueryContext(ctx, query, args)
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded token query")
	}
	c.d.usage.s.mu.Lock()
	c.d.usage.s.queries = append(c.d.usage.s.queries, query)
	c.d.usage.s.args = append(c.d.usage.s.args, append([]driver.NamedValue(nil), args...))
	c.d.usage.s.mu.Unlock()
	if c.d.queryErr != nil {
		return nil, c.d.queryErr
	}
	return &walletRows{columns: []string{"user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at", "token_id", "name"}, data: c.d.data, err: c.d.rowErr}, nil
}
func tokenUsageSource(t *testing.T) (*sql.DB, *tokenUsageTestDriver, UsageWindowRequest) {
	t.Helper()
	_, u, req := usageTestSource(t)
	d := &tokenUsageTestDriver{usage: u, data: [][]driver.Value{append(usageTestRow(2, "wallet", "1500000", 1500000), int64(11), "synthetic token")}}
	name := fmt.Sprintf("monthly-token-usage-%d", walletDriverSequence.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db, d, req
}

func TestTokenUsageWindowSameSnapshotExactMoneyAndRepeat(t *testing.T) {
	db, d, req := tokenUsageSource(t)
	o, err := CaptureTokenUsageWindow(context.Background(), db, req)
	if err != nil || len(o.Rows) != 1 || o.Rows[0].TokenID != 11 || o.Rows[0].Quota != 1500000 || !reflect.DeepEqual(o.Totals[0], o.Rows[0].UsageWindowRow) {
		t.Fatal(o, err)
	}
	s := d.usage.s
	if !s.options.ReadOnly || s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !s.committed || len(s.queries) != 4 || s.queries[2] != usageWindowQuery || s.queries[3] != tokenUsageWindowQuery {
		t.Fatal("not one bounded read-only transaction", s.queries)
	}
	if !reflect.DeepEqual(s.args[3], []driver.NamedValue{{Ordinal: 1, Value: req.UserID}, {Ordinal: 2, Value: req.From}, {Ordinal: 3, Value: req.To}}) || db.Stats().InUse != 0 {
		t.Fatal("scope widened or connection retained")
	}
	raw, _ := json.Marshal(o)
	for _, private := range []string{`"complete"`, `"reviewed"`, `"key"`, `"content"`, `"prompt"`, `"wallet_only"`} {
		if strings.Contains(string(raw), private) {
			t.Fatal("private or unearned field", private)
		}
	}
	copy := o
	copy.StartedAtMS = o.CompletedAtMS + 300000
	copy.CompletedAtMS = copy.StartedAtMS + 1
	quota := int64(9)
	copy.Wallet.BalanceQuota = &quota
	if !TokenUsageWindowsMatch(o, copy) {
		t.Fatal("moving live wallet treated as changed historical logs")
	}
	if TokenUsageWindowsMatch(o, o) {
		t.Fatal("one capture counted twice")
	}
	copy.Rows = append([]TokenUsageWindowRow{}, o.Rows...)
	copy.Rows[0].Name = "changed"
	if TokenUsageWindowsMatch(o, copy) {
		t.Fatal("changed retained observation accepted")
	}
}

func TestTokenUsageWindowFailuresNeverReturnPartialOrRetry(t *testing.T) {
	for _, scenario := range []string{"nil_context", "nil_db", "bad_user", "wrong_server", "wrong_unit", "window_too_long", "token_query", "rows", "commit", "null_sum", "null_count", "overflow", "negative", "negative_id", "unsafe_id", "long_name", "foreign_user", "type", "source", "outside", "duplicate", "account_mismatch", "bad_order", "too_many"} {
		t.Run(scenario, func(t *testing.T) {
			db, d, req := tokenUsageSource(t)
			ctx := context.Background()
			fail := errors.New("synthetic token failure")
			row := d.data[0]
			switch scenario {
			case "nil_context":
				ctx = nil
			case "nil_db":
				db = nil
			case "bad_user":
				req.UserID = 0
			case "wrong_server":
				d.usage.s.uuid = "other"
			case "wrong_unit":
				req.Rules.QuotaPerUnit = 1
			case "window_too_long":
				req.To = req.From + 86401
			case "token_query":
				d.queryErr = fail
			case "rows":
				d.rowErr = fail
			case "commit":
				d.usage.s.commitErr = fail
			case "null_sum":
				row[5] = nil
			case "null_count":
				row[4] = int64(0)
			case "overflow":
				row[5] = "9223372036854775808"
			case "negative":
				row[5] = "-1"
			case "negative_id":
				row[12] = int64(-1)
			case "unsafe_id":
				row[12] = int64(9007199254740992)
			case "long_name":
				row[13] = strings.Repeat("x", 513)
			case "foreign_user":
				row[0] = int64(202)
			case "type":
				row[1] = int64(1)
			case "source":
				row[2] = "Wallet"
			case "outside":
				row[11] = req.To
			case "duplicate":
				d.data = append(d.data, append([]driver.Value{}, row...))
			case "account_mismatch":
				d.usage.data[0] = usageTestRow(2, "wallet", "1500001", 1500001)
			case "bad_order":
				other := append([]driver.Value{}, row...)
				other[12] = int64(10)
				d.data = append(d.data, other)
			case "too_many":
				d.data = nil
				for i := 0; i <= tokenUsageRowsLimit; i++ {
					r := append([]driver.Value{}, row...)
					r[12] = int64(i)
					d.data = append(d.data, r)
				}
			}
			o, err := CaptureTokenUsageWindow(ctx, db, req)
			if err == nil || o.Version != 0 || o.Rows != nil {
				t.Fatal("failure returned partial token money", o, err)
			}
			stage := TokenUsageFailureStage(err)
			if stage == "" || strings.Contains(err.Error(), fail.Error()) {
				t.Fatal("missing safe stage or private cause leaked", stage)
			}
			if (scenario == "token_query" || scenario == "rows" || scenario == "commit") && !errors.Is(err, fail) {
				t.Fatal("wrapped diagnostic lost original error classification")
			}
			if scenario == "token_query" && stage != "token_groups" || scenario == "commit" && stage != "commit" || scenario == "wrong_server" && stage != "identity" || scenario == "account_mismatch" && stage != "validation" {
				t.Fatal("wrong safe diagnostic stage", scenario, stage)
			}
			if d.usage.s.committed || len(d.usage.s.queries) > 4 {
				t.Fatal("failure committed or retried")
			}
			if db != nil && db.Stats().InUse != 0 {
				t.Fatal("source connection retained")
			}
		})
	}
}

func TestTokenUsageWindowEmptyMissingWalletAndExactInteger(t *testing.T) {
	db, d, req := tokenUsageSource(t)
	d.usage.data = nil
	d.usage.s.data = nil
	d.data = nil
	o, err := CaptureTokenUsageWindow(context.Background(), db, req)
	if err != nil || o.Rows == nil || o.Totals == nil || len(o.Rows) != 0 || o.Wallet.Exists {
		t.Fatal("empty source confused with failed capture", o, err)
	}
	d.usage.data = [][]driver.Value{usageTestRow(2, "wallet", "9007199254740993", 9007199254740993)}
	d.data = [][]driver.Value{append(usageTestRow(2, "wallet", "9007199254740993", 9007199254740993), int64(0), "")}
	o, err = CaptureTokenUsageWindow(context.Background(), db, req)
	if err != nil || o.Rows[0].Quota != 9007199254740993 || o.Rows[0].TokenID != 0 {
		t.Fatal("exact integer or unknown legacy token lost", o, err)
	}
}

func TestTokenUsageWindowByteBudgetAndSharedSlot(t *testing.T) {
	db, d, req := tokenUsageSource(t)
	walletCaptureSlots <- struct{}{}
	_, err := CaptureTokenUsageWindow(context.Background(), db, req)
	<-walletCaptureSlots
	if !errors.Is(err, ErrWalletCaptureBusy) || len(d.usage.s.queries) != 0 {
		t.Fatal("busy capture opened another source view", err)
	}
	total := usageTestRow(2, "wallet", "1000", 1)
	total[3], total[4], total[8], total[9] = int64(1000), int64(1000), int64(11), int64(1010)
	d.usage.data = [][]driver.Value{total}
	d.data = nil
	for i := 0; i < 1000; i++ {
		r := usageTestRow(2, "wallet", "1", 1)
		r[8], r[9] = int64(11+i), int64(11+i)
		d.data = append(d.data, append(r, int64(i), strings.Repeat("\x00", 512)))
	}
	// Each source name is within its byte bound; JSON escaping makes the
	// combined payload exceed 2 MiB. No completed observation may escape.
	if o, err := CaptureTokenUsageWindow(context.Background(), db, req); err == nil || o.Version != 0 || d.usage.s.committed {
		t.Fatal("encoded observation byte limit bypassed", err)
	}
}
