package monitor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
)

func TestUsageLogsCountSpanKeepsSelectiveAndCoveringPlans(t *testing.T) {
	base := usageLogFilter{Start: 100, End: 200}
	for name, tc := range map[string]struct {
		f    usageLogFilter
		want bool
	}{
		"channel":           {usageLogFilter{Start: 100, End: 200, Channel: 7}, true},
		"model":             {usageLogFilter{Start: 100, End: 200, ModelName: "gpt-6"}, true},
		"group":             {usageLogFilter{Start: 100, End: 200, Group: "codex"}, true},
		"time only":         {base, false},
		"user id":           {usageLogFilter{Start: 100, End: 200, Channel: 7, UserID: 3}, false},
		"username":          {usageLogFilter{Start: 100, End: 200, Channel: 7, Username: "u"}, true},
		"username only":     {usageLogFilter{Start: 100, End: 200, Username: "u"}, false},
		"wildcard":          {usageLogFilter{Start: 100, End: 200, Channel: 7, Username: "ab%"}, false},
		"token":             {usageLogFilter{Start: 100, End: 200, Channel: 7, TokenName: "t"}, false},
		"request id":        {usageLogFilter{Start: 100, End: 200, Channel: 7, RequestID: "r"}, false},
		"upstream request":  {usageLogFilter{Start: 100, End: 200, Channel: 7, UpstreamRequestID: "r"}, false},
		"open-ended window": {usageLogFilter{Start: 100, Channel: 7}, false},
		"inverted window":   {usageLogFilter{Start: 300, End: 200, Channel: 7}, false},
		"no window at all":  {usageLogFilter{Channel: 7}, false},
	} {
		if got := usageLogsUseCountSpan(tc.f); got != tc.want {
			t.Errorf("%s: got %v want %v", name, got, tc.want)
		}
	}
}

// created_at is not strictly monotonic in id; the span must still include every
// row of the window and nothing outside it.
func TestUsageLogsIDSpanCountsAndSumsMatchOriginalPredicates(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	rows := []usageLogRow{
		{id: 1, createdAt: 90, typ: 2, channel: 7, quota: 1},
		{id: 2, createdAt: 101, typ: 2, channel: 7, quota: 10},
		{id: 3, createdAt: 99, typ: 2, channel: 7, quota: 100}, // inside the span, outside the window
		{id: 4, createdAt: 150, typ: 5, channel: 7, quota: 0},
		{id: 5, createdAt: 150, typ: 2, channel: 9, quota: 1000},
		{id: 6, createdAt: 210, typ: 2, channel: 7, quota: 10000}, // inside the span, outside the window
		{id: 7, createdAt: 199, typ: 2, channel: 7, quota: 100000},
		{id: 8, createdAt: 200, typ: 2, channel: 7, quota: 1000000},
		{id: 9, createdAt: 201, typ: 2, channel: 7, quota: 7},
	}
	for i := range rows {
		rows[i].username, rows[i].model, rows[i].group = "u", "m", "g"
	}
	insertUsageLogs(t, db, rows...)
	for _, f := range []usageLogFilter{
		{Start: 100, End: 200, Channel: 7},
		{Start: 100, End: 200, Channel: 9},
		{Start: 100, End: 200, Channel: 7, Type: 5},
		{Start: 100, End: 200, ModelName: "m"},
		{Start: 100, End: 200, Group: "g"},
	} {
		for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
			where, err := buildUsageLogWhere(f, scope)
			if err != nil {
				t.Fatal(err)
			}
			for _, agg := range []string{"COUNT(*)", "COALESCE(SUM(quota),0)"} {
				var want int64
				var got sql.NullInt64
				if err := db.QueryRow("SELECT "+agg+" FROM logs"+where.sql(), where.args...).Scan(&want); err != nil {
					t.Fatal(err)
				}
				query, args := usageLogsAggregateSpanSQL(f, where, agg)
				if err := db.QueryRow(sqliteQuotaSpanSQL(query), args...).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if !got.Valid || got.Int64 != want {
					t.Fatalf("%+v scope=%d %s: span=%v original=%d", f, scope, agg, got, want)
				}
			}
		}
	}
	// A window without rows is answered without the main scan.
	f := usageLogFilter{Start: 500, End: 600, Channel: 7}
	where, _ := buildUsageLogWhere(f, usageLogWhereList)
	query, args := usageLogsAggregateSpanSQL(f, where, "COUNT(*)")
	var empty int64
	if err := db.QueryRow(sqliteQuotaSpanSQL(query), args...).Scan(&empty); err != nil || empty != 0 {
		t.Fatalf("empty window: %d %v", empty, err)
	}
}

// Page rows and aggregates preserve the original historical username filter.
func TestUsageLogsListRowsKeepHistoricalNamesAndCombinedFilters(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	insertUsageLogs(t, db,
		usageLogRow{id: 1, userID: 31, createdAt: 110, typ: 2, username: "kjl_Dev", channel: 7, quota: 1},
		usageLogRow{id: 2, userID: 31, createdAt: 120, typ: 5, username: "kjl_Dev", channel: 7},
		usageLogRow{id: 3, userID: 31, createdAt: 130, typ: 2, username: "kjl_Dev", channel: 9, quota: 2},
		usageLogRow{id: 4, userID: 44, createdAt: 140, typ: 2, username: "other", channel: 7, quota: 3},
	)
	m := &Monitor{prodDB: db}
	list := func(q string) []int64 {
		t.Helper()
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+q, nil)
		var resp struct {
			Success bool `json:"success"`
			Data    struct {
				Items []struct {
					ID int64 `json:"id"`
				} `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || !resp.Success {
			t.Fatalf("%s: %v %s", q, err, w.Body.String())
		}
		var ids []int64
		for _, it := range resp.Data.Items {
			ids = append(ids, it.ID)
		}
		return ids
	}
	for q, want := range map[string]string{
		"username=kjl_Dev&start_timestamp=100&end_timestamp=200":                         "[3 2 1]",
		"username=kjl_Dev&channel=7&start_timestamp=100&end_timestamp=200":               "[2 1]",
		"username=kjl_Dev&channel=7&type=5&start_timestamp=100&end_timestamp=200":        "[2]",
		"username=kjl_Dev&channel=7&start_timestamp=100&end_timestamp=200&defer_total=1": "[2 1]",
		"channel=7&start_timestamp=100&end_timestamp=200":                                "[4 2 1]",
	} {
		if got := fmt.Sprint(list(q)); got != want {
			t.Errorf("%s: got %s want %s", q, got, want)
		}
	}
	// Renamed user: the earliest log carries the old name, so user_id would add
	// rows the username filter excludes. The username predicate must be kept.
	if _, err := db.Exec("UPDATE users SET username = 'renamed' WHERE id = 31"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, username) VALUES (32, 'kjl_Dev')"); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(list("username=kjl_Dev&channel=7&start_timestamp=100&end_timestamp=200")); got != "[2 1]" {
		t.Errorf("name reused by another user must keep username rows, got %s", got)
	}
}
