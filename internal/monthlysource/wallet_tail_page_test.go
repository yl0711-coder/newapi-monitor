package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type pageTailItem struct {
	id, at, typ, user, quota int64
	billing, content, other  string
}

type pageTailDriver struct {
	s              *walletDriverState
	r              WalletTailPageRequest
	items          []pageTailItem
	indexOverride  [][]driver.Value
	bodyOverride   [][]driver.Value
	serverOverride [][]driver.Value
	fail           string
	onQuery        func(string)
}

type pageTailConn struct {
	*walletConn
	d *pageTailDriver
}

func (d *pageTailDriver) Open(string) (driver.Conn, error) {
	return &pageTailConn{&walletConn{d.s}, d}, nil
}

func (c *pageTailConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	index := strings.Contains(q, "AS index_page")
	body := strings.Contains(q, "FROM JSON_TABLE")
	selective := strings.Contains(q, "AS funding_candidates")
	serverUsage := strings.Contains(q, "WITH usage_keys AS")
	if !index && !body && !selective && !serverUsage {
		return c.walletConn.QueryContext(ctx, q, args)
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded source query")
	}
	if c.d.onQuery != nil {
		c.d.onQuery(q)
	}
	c.s.mu.Lock()
	c.s.queries = append(c.s.queries, q)
	c.s.args = append(c.s.args, args)
	c.s.mu.Unlock()
	if (index || selective || serverUsage) && c.d.fail == "index" || (body || selective || serverUsage) && c.d.fail == "body" {
		return nil, context.DeadlineExceeded
	}
	if serverUsage {
		return serverTailFixtureRows(c.d, q, args)
	}
	if selective {
		return selectiveTailFixtureRows(c.d, args)
	}
	// Interpret the emitted SQL/arguments, not a manually advanced fixture
	// cursor. This also supports both streams inside a single transaction.
	r := c.d.r
	r.Kind = "user_usage"
	if strings.Contains(q, "idx_created_at_type") || strings.Contains(q, "WHERE type IN (1,3,4)") {
		r.Kind = "global_funding"
	}
	if index {
		offset := 0
		if r.Kind == "user_usage" {
			r.Wallet.UserID = args[0].Value.(int64)
			offset = 1
		}
		r.After = WalletTailPageCursor{}
		if strings.Contains(q, "UNION ALL") {
			r.After = WalletTailPageCursor{At: args[offset].Value.(int64), Type: args[offset+1].Value.(int64), ID: args[offset+2].Value.(int64)}
		} else {
			r.Wallet.Boundary = args[offset].Value.(int64)
		}
		data := c.d.indexOverride
		if data == nil {
			for _, item := range c.d.items {
				cursor := WalletTailPageCursor{At: item.at, Type: item.typ, ID: item.id}
				if item.at >= r.Wallet.Boundary && tailPageCursorBefore(r.After, cursor) && (r.Kind == "global_funding" || item.user == r.Wallet.UserID) {
					data = append(data, []driver.Value{item.id, item.at, item.typ})
				}
			}
			sort.Slice(data, func(i, j int) bool {
				return tailPageCursorBefore(WalletTailPageCursor{ID: data[i][0].(int64), At: data[i][1].(int64), Type: data[i][2].(int64)}, WalletTailPageCursor{ID: data[j][0].(int64), At: data[j][1].(int64), Type: data[j][2].(int64)})
			})
			if len(data) > walletTailPageRows+1 {
				data = data[:walletTailPageRows+1]
			}
		}
		return &walletRows{columns: []string{"id", "created_at", "type"}, data: data}, nil
	}
	keysAt := 0
	if r.Kind == "global_funding" {
		keysAt = 3
		r.Wallet.UserID = args[0].Value.(int64)
	} else {
		r.Wallet.UserID = args[1].Value.(int64)
	}
	var ids []int64
	if len(args) <= keysAt || json.Unmarshal([]byte(args[keysAt].Value.(string)), &ids) != nil {
		return nil, errors.New("invalid key table")
	}
	selected := map[int64]bool{}
	for _, id := range ids {
		selected[id] = true
	}
	data := c.d.bodyOverride
	if data == nil {
		if r.Kind == "user_usage" {
			data = pageTailUsageRows(c.d.items, selected, r.Wallet.UserID)
		} else {
			for _, item := range c.d.items {
				if selected[item.id] {
					user, content, other := item.user, item.content, item.other
					if item.typ != 3 && user != r.Wallet.UserID {
						user, content, other = 0, "", ""
					}
					data = append(data, []driver.Value{item.id, user, item.at, item.typ, content, other})
				}
			}
			sort.Slice(data, func(i, j int) bool {
				return fundingCursorBefore(FundingCursor{At: data[i][2].(int64), ID: data[i][0].(int64)}, FundingCursor{At: data[j][2].(int64), ID: data[j][0].(int64)})
			})
		}
	}
	columns := []string{"user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}
	if r.Kind == "global_funding" {
		columns = []string{"id", "user_id", "created_at", "type", "content", "other"}
	}
	return &walletRows{columns: columns, data: data}, nil
}

func serverTailFixtureRows(d *pageTailDriver, q string, args []driver.NamedValue) (driver.Rows, error) {
	user, after := args[0].Value.(int64), WalletTailPageCursor{}
	boundary := d.r.Wallet.Boundary
	if strings.Contains(q, "AS usage_candidates") {
		after = WalletTailPageCursor{At: args[1].Value.(int64), Type: args[2].Value.(int64), ID: args[3].Value.(int64)}
	} else {
		boundary = args[1].Value.(int64)
	}
	if args[len(args)-4].Value.(int64) != user || args[len(args)-3].Value.(int64) != after.At || args[len(args)-2].Value.(int64) != after.Type || args[len(args)-1].Value.(int64) != after.ID {
		return nil, errors.New("server usage scope/metadata bindings differ")
	}
	items := []pageTailItem{}
	for _, item := range d.items {
		if item.user == user && (item.typ == 2 || item.typ == 6) && item.at >= boundary && tailPageCursorBefore(after, WalletTailPageCursor{At: item.at, Type: item.typ, ID: item.id}) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return tailPageCursorBefore(WalletTailPageCursor{At: items[i].at, Type: items[i].typ, ID: items[i].id}, WalletTailPageCursor{At: items[j].at, Type: items[j].typ, ID: items[j].id})
	})
	if len(items) > walletTailPageRows {
		items = items[:walletTailPageRows]
	}
	next := after
	ids := map[int64]bool{}
	for _, item := range items {
		ids[item.id] = true
		next = WalletTailPageCursor{At: item.at, Type: item.typ, ID: item.id}
	}
	data := d.serverOverride
	if data == nil {
		data = [][]driver.Value{}
		meta := []driver.Value{int64(len(items)), next.At, next.Type, next.ID}
		if len(items) == 0 {
			data = append(data, append(meta, int64(0), int64(0), "", int64(0), int64(0), "0", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)))
		} else {
			for _, row := range pageTailUsageRows(d.items, ids, user) {
				data = append(data, append(append([]driver.Value{}, meta...), row...))
			}
		}
	}
	return &walletRows{columns: []string{"page_records", "next_at", "next_type", "next_id", "user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}, data: data}, nil
}

func selectiveTailFixtureRows(d *pageTailDriver, args []driver.NamedValue) (driver.Rows, error) {
	after, user := FundingCursor{}, int64(0)
	switch len(args) {
	case 5:
		user = args[1].Value.(int64)
	case 13:
		after = FundingCursor{At: args[0].Value.(int64), ID: args[1].Value.(int64)}
		user = args[3].Value.(int64)
	default:
		return nil, errors.New("unexpected selective funding arguments")
	}
	data := [][]driver.Value{}
	for _, item := range d.items {
		if item.at >= d.r.Wallet.Boundary && fundingCursorBefore(after, FundingCursor{At: item.at, ID: item.id}) && (item.typ == 3 || (item.typ == 1 || item.typ == 4) && item.user == user) {
			data = append(data, []driver.Value{item.id, item.user, item.at, item.typ, item.content, item.other})
		}
	}
	sort.Slice(data, func(i, j int) bool {
		return fundingCursorBefore(FundingCursor{At: data[i][2].(int64), ID: data[i][0].(int64)}, FundingCursor{At: data[j][2].(int64), ID: data[j][0].(int64)})
	})
	if len(data) > FundingPageLimit+1 {
		data = data[:FundingPageLimit+1]
	}
	return &walletRows{columns: []string{"id", "user_id", "created_at", "type", "content", "other"}, data: data}, nil
}

func pageTailUsageRows(items []pageTailItem, selected map[int64]bool, user int64) [][]driver.Value {
	groups := map[string][]pageTailItem{}
	for _, item := range items {
		if selected[item.id] && item.user == user {
			key := fmt.Sprintf("%d:%s", item.typ, item.billing)
			groups[key] = append(groups[key], item)
		}
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := [][]driver.Value{}
	for _, key := range keys {
		g := groups[key]
		first := g[0]
		minQ, maxQ, minID, maxID, lo, hi := first.quota, first.quota, first.id, first.id, first.at, first.at
		total := new(big.Int)
		for _, item := range g {
			total.Add(total, big.NewInt(item.quota))
			minQ = min(minQ, item.quota)
			maxQ = max(maxQ, item.quota)
			minID = min(minID, item.id)
			maxID = max(maxID, item.id)
			lo = min(lo, item.at)
			hi = max(hi, item.at)
		}
		out = append(out, []driver.Value{user, first.typ, first.billing, int64(len(g)), int64(len(g)), total.String(), minQ, maxQ, minID, maxID, lo, hi})
	}
	return out
}

func pageTailSource(t *testing.T, kind string, items []pageTailItem) (*sql.DB, *pageTailDriver, time.Time) {
	t.Helper()
	boundary := time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("CST", 28800)).Unix()
	s := &walletDriverState{uuid: "bounded-page-fixture", database: "synthetic", data: [][]driver.Value{{int64(201), boundary - 1, int64(12345), int64(9007199254740993)}}}
	pin, err := WalletSourceIdentity(s.uuid, s.database)
	if err != nil {
		t.Fatal(err)
	}
	d := &pageTailDriver{s: s, r: WalletTailPageRequest{Wallet: WalletTailRequest{SourceIdentity: pin, Rules: fundingTestRules(), UserID: 201, Boundary: boundary}, Kind: kind}, items: items}
	name := fmt.Sprintf("bounded-tail-page-%d", walletDriverSequence.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db, d, time.Unix(boundary+14*86400, 37000000)
}

func TestWalletTailPageUsageCursorPreservesAllAmountsAcrossDenseTies(t *testing.T) {
	db, d, now := pageTailSource(t, "user_usage", nil)
	var wantC, wantR, wantN int64
	for i := int64(1); i <= 2*walletTailPageRows+19; i++ {
		item := pageTailItem{id: i, at: d.r.Wallet.Boundary + 10 + i/10000, typ: 2, user: 201, quota: i % 7, billing: "wallet"}
		if i%11 == 0 {
			item.typ = 6
		}
		if i%23 == 0 {
			item.typ = 0
		}
		if i%29 == 0 {
			item.user = 202
		}
		d.items = append(d.items, item)
		if item.user == 201 {
			wantN++
			if item.typ == 2 {
				wantC += item.quota
			}
			if item.typ == 6 {
				wantR += item.quota
			}
		}
	}
	var gotC, gotR, gotN int64
	pages := 0
	for {
		o, err := captureWalletTailPage(t.Context(), db, d.r, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if o.Index.Records > walletTailPageRows || !validDigest(o.Index.SHA256) || o.OtherUserFundingRecords != 0 {
			t.Fatal("unbounded or mixed page")
		}
		gotN += int64(o.Index.Records)
		for _, r := range o.Usage {
			if r.Type == 2 {
				gotC += r.Quota
			} else {
				gotR += r.Quota
			}
		}
		if o.ViewEndReached {
			break
		}
		if !tailPageCursorBefore(d.r.After, o.Index.Next) {
			t.Fatal("cursor did not advance")
		}
		d.r.After = o.Index.Next
		if pages > 4 {
			t.Fatal("cursor did not terminate on fixed fixture")
		}
	}
	if pages < 2 || gotN != wantN || gotC != wantC || gotR != wantR || !d.s.options.ReadOnly || d.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || db.Stats().InUse != 0 {
		t.Fatal("lost/doubled records, changed money or source view", pages, gotN, wantN, gotC, wantC, gotR, wantR)
	}
	for _, q := range d.s.queries {
		if strings.Contains(q, "AS index_page") && (strings.Contains(q, "SUM(") || !strings.Contains(q, "LIMIT 8193")) {
			t.Fatal("whole-month work before limit")
		}
		if strings.Contains(q, "SUM(") && (!strings.Contains(q, "JSON_TABLE") || !strings.Contains(q, "STRAIGHT_JOIN logs FORCE INDEX(PRIMARY)")) {
			t.Fatal("unbounded aggregate")
		}
	}
}

func TestWalletTailPageFundingCursorsKeepOtherActorsWithoutLeakingOtherTopups(t *testing.T) {
	db, d, now := pageTailSource(t, "global_funding", nil)
	for i := int64(1); i <= 401; i++ {
		user := int64(201)
		typ := int64(1)
		content := "own-topup"
		if i%3 == 0 {
			typ, user, content = 3, 999, "other-actor-management"
		} else if i%5 == 0 {
			user, content = 202, "foreign-private-topup"
		}
		d.items = append(d.items, pageTailItem{id: i, at: d.r.Wallet.Boundary + 1, typ: typ, user: user, content: content})
	}
	seen, skipped, pages := map[int64]bool{}, 0, 0
	for {
		o, err := captureWalletTailPage(t.Context(), db, d.r, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if len(o.Funding) > FundingPageLimit {
			t.Fatal("body budget changed")
		}
		skipped += o.OtherUserFundingRecords
		for _, r := range o.Funding {
			if seen[r.ID] {
				t.Fatal("double counted funding")
			}
			seen[r.ID] = true
			if r.Type == 3 && r.UserID != 999 {
				t.Fatal("actor incorrectly reassigned")
			}
		}
		raw, _ := json.Marshal(o)
		if strings.Contains(string(raw), "foreign-private-topup") || strings.Contains(string(raw), `"user_id":202`) {
			t.Fatal("another account's topup leaked")
		}
		if o.ViewEndReached {
			break
		}
		if !tailPageCursorBefore(d.r.After, o.Index.Next) {
			t.Fatal("funding cursor stuck")
		}
		d.r.After = o.Index.Next
		if pages > 4 {
			t.Fatal("unbounded pages")
		}
	}
	if pages != 3 || len(seen)+skipped != 401 || skipped == 0 {
		t.Fatal("funding missing or not budgeted", pages, len(seen), skipped)
	}
}

func TestWalletTailPagePreservesLargeExactAndUnknownSourceWithoutGrantingCompleteness(t *testing.T) {
	db, d, now := pageTailSource(t, "user_usage", nil)
	d.items = []pageTailItem{{id: 9007199254740993, at: d.r.Wallet.Boundary + 1, typ: 2, user: 201, quota: 9007199254740993, billing: "unspecified"}}
	o, err := captureWalletTailPage(t.Context(), db, d.r, func() time.Time { return now })
	if err != nil || len(o.Usage) != 1 || o.Usage[0].Quota != 9007199254740993 || o.Usage[0].BillingSource != "unspecified" {
		t.Fatal("rounded money or assumed billing source", err)
	}
	raw, _ := json.Marshal(o)
	var full WalletTailObservation
	if json.Unmarshal(raw, &full) != nil {
		t.Fatal("fixture serialization failed")
	}
	if ValidateWalletTailObservation(full) == nil {
		t.Fatal("partial page became a full tail")
	}
	if _, err := DeriveWalletTailBoundary(full, full); err == nil {
		t.Fatal("page claimed historical balance")
	}
	typ := reflect.TypeOf(o)
	for _, field := range []string{"Complete", "WalletOnly", "Approved", "Ready"} {
		if _, ok := typ.FieldByName(field); ok {
			t.Fatal("financial approval flag in source page")
		}
	}
}

func TestWalletTailPageRejectsInvalidRequestRowsAndIncompleteBodies(t *testing.T) {
	for _, scenario := range []string{"bad_kind", "partial_cursor", "before_month", "cancelled", "index_error", "body_error", "duplicate_id", "float_id", "unordered", "future", "missing_usage", "missing_funding", "foreign_header_body", "commit"} {
		t.Run(scenario, func(t *testing.T) {
			kind := "user_usage"
			if scenario == "missing_funding" || scenario == "foreign_header_body" {
				kind = "global_funding"
			}
			db, d, now := pageTailSource(t, kind, nil)
			b := d.r.Wallet.Boundary
			d.items = []pageTailItem{{id: 1, at: b + 1, typ: 2, user: 201, quota: 1, billing: "wallet"}}
			ctx := t.Context()
			switch scenario {
			case "bad_kind":
				d.r.Kind = "other"
			case "partial_cursor":
				d.r.After = WalletTailPageCursor{At: b + 1}
			case "before_month":
				d.r.After = WalletTailPageCursor{At: b - 1, ID: 1}
			case "cancelled":
				c, stop := context.WithCancel(ctx)
				stop()
				ctx = c
			case "index_error":
				d.fail = "index"
			case "body_error":
				d.fail = "body"
			case "duplicate_id":
				d.indexOverride = [][]driver.Value{{int64(1), b + 1, int64(2)}, {int64(1), b + 2, int64(2)}}
			case "float_id":
				d.indexOverride = [][]driver.Value{{float64(1), b + 1, int64(2)}}
			case "unordered":
				d.indexOverride = [][]driver.Value{{int64(2), b + 2, int64(2)}, {int64(1), b + 1, int64(2)}}
			case "future":
				d.items[0].at = now.Unix() + 1
			case "missing_usage":
				d.bodyOverride = [][]driver.Value{}
			case "missing_funding":
				d.items[0].typ = 3
				d.items[0].content = "admin"
				d.bodyOverride = [][]driver.Value{}
			case "foreign_header_body":
				d.items[0].typ = 1
				d.bodyOverride = [][]driver.Value{{int64(1), int64(0), b + 1, int64(1), "private", ""}}
			case "commit":
				d.s.commitErr = errors.New("synthetic-private-driver-message")
			}
			o, err := captureWalletTailPage(ctx, db, d.r, func() time.Time { return now })
			if err == nil || !reflect.DeepEqual(o, WalletTailPageObservation{}) || d.s.committed || db.Stats().InUse != 0 {
				t.Fatal("failure returned partial data, committed or leaked connection", err)
			}
			if scenario == "index_error" && (!errors.Is(err, context.DeadlineExceeded) || WalletTailFailureStage(err) != "tail_index") {
				t.Fatal("lost timeout classification")
			}
			if strings.Contains(err.Error(), "synthetic-private-driver-message") {
				t.Fatal("driver details leaked")
			}
		})
	}
}

func TestWalletTailPageEmptyViewAndQueryShapeDoNotScanOrAuthorizeWholeMonth(t *testing.T) {
	db, d, now := pageTailSource(t, "user_usage", nil)
	o, err := captureWalletTailPage(t.Context(), db, d.r, func() time.Time { return now })
	if err != nil || !o.ViewEndReached || o.Index.Records != 0 || o.Index.Next != d.r.After || len(d.s.queries) != 3 {
		t.Fatal("empty page invented body data or consumed extra reads", err)
	}
	for _, kind := range []string{"user_usage", "global_funding"} {
		r := d.r
		r.Kind = kind
		r.After = WalletTailPageCursor{At: r.Wallet.Boundary + 1, Type: 2, ID: 9007199254740993}
		q, args := tailPageIndexQuery(r)
		if strings.Count(q, "LIMIT 8193") != 4 || strings.Count(q, "UNION ALL") != 2 || strings.Contains(q, "OFFSET") || strings.Contains(q, "SUM(") || len(args) == 0 {
			t.Fatal("unbounded index union or offset")
		}
		q, args, err := tailPageBodyQuery(r, []any{int64(9007199254740993)})
		if err != nil || !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") || !strings.Contains(q, "STRAIGHT_JOIN") || len(args) == 0 {
			t.Fatal("unbounded body query", err)
		}
	}
	for _, ids := range [][]any{nil, {float64(1)}, {int64(0)}, {int64(1), int64(1)}} {
		if _, _, err := tailPageBodyQuery(d.r, ids); err == nil {
			t.Fatal("unsafe key table accepted")
		}
	}
}
