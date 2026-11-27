package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"testing"
	"time"
)

func addUsageLogsUser(t *testing.T, db *sql.DB, id int64, username string) {
	t.Helper()
	if _, err := db.Exec("INSERT INTO users (id, username) VALUES (?, ?)", id, username); err != nil {
		t.Fatal(err)
	}
}

func TestUsageLogsHistoricalUsernameNeverBecomesCurrentUserID(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		users     map[int64]string
		logs      []usageLogRow
		username  string
		wantCount int64
	}{
		{name: "stable user", users: map[int64]string{31: "kjl_Dev"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}, {id: 5, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "kjl_Dev", wantCount: 2},
		{name: "wildcard keeps username", users: map[int64]string{31: "kjl_Dev"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "kjl%", wantCount: 1},
		{name: "unknown username", users: map[int64]string{31: "kjl_Dev"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "nobody", wantCount: 0},
		{name: "renamed user: earliest row has the old name", users: map[int64]string{40: "newname"},
			logs:     []usageLogRow{{id: 1, userID: 40, typ: 2, username: "oldname"}, {id: 2, userID: 40, typ: 2, username: "newname"}},
			username: "newname", wantCount: 1},
		{name: "name used earlier by another user", users: map[int64]string{41: "reused", 42: "moved"},
			logs:     []usageLogRow{{id: 1, userID: 42, typ: 2, username: "reused"}, {id: 2, userID: 41, typ: 2, username: "reused"}},
			username: "reused", wantCount: 2},
		{name: "no rows at all", users: map[int64]string{43: "quiet"},
			logs:     []usageLogRow{{id: 1, userID: 31, typ: 2, username: "kjl_Dev"}},
			username: "quiet", wantCount: 0},
		{name: "no own rows but another user's rows carry the name", users: map[int64]string{44: "taken"},
			logs:     []usageLogRow{{id: 1, userID: 45, typ: 2, username: "taken"}},
			username: "taken", wantCount: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newUsageLogsFakeProd(t)
			if _, err := db.Exec("DELETE FROM users"); err != nil {
				t.Fatal(err)
			}
			for id, name := range tc.users {
				addUsageLogsUser(t, db, id, name)
			}
			insertUsageLogs(t, db, tc.logs...)
			where, err := buildUsageLogWhere(usageLogFilter{Username: tc.username}, usageLogWhereList)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(where.sql(), "user_id") || !strings.Contains(where.sql(), "username") {
				t.Fatal("historical name predicate was replaced")
			}
			var count int64
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != tc.wantCount {
				t.Fatalf("historical count=%d want=%d", count, tc.wantCount)
			}
		})
	}
}

// List/count/stat keep the exact historical-name predicate for every filter
// combination, including renamed names and names reused by different users.
func TestUsageLogsHistoricalNamesKeepCountAndStatIdentical(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	addUsageLogsUser(t, db, 40, "newname")
	addUsageLogsUser(t, db, 41, "reused")
	addUsageLogsUser(t, db, 42, "moved")
	now := time.Now().Unix()
	rng := rand.New(rand.NewSource(7))
	type owner struct {
		id   int64
		name string
	}
	var rows []usageLogRow
	id := int64(1)
	add := func(o owner, at int64) {
		rows = append(rows, usageLogRow{id: id, userID: o.id, createdAt: at, typ: []int{2, 2, 2, 5, 6}[rng.Intn(5)],
			username: o.name, token: []string{"a", "b"}[rng.Intn(2)], model: []string{"m1", "m2"}[rng.Intn(2)],
			quota: int64(rng.Intn(1000)), prompt: int64(rng.Intn(50)), complete: int64(rng.Intn(50)),
			channel: []int64{7, 9}[rng.Intn(2)], group: []string{"codex", "default"}[rng.Intn(2)], requestID: fmt.Sprintf("r%d", id)})
		id++
	}
	// Old era: user 40 was "oldname"; user 42 was "reused" before renaming to "moved".
	for i := 0; i < 60; i++ {
		at := now - 7200 + int64(i)
		add(owner{40, "oldname"}, at)
		add(owner{42, "reused"}, at)
	}
	for i := 0; i < 200; i++ {
		at := now - 3600 + int64(i*15)
		add(owner{31, "kjl_Dev"}, at)
		add(owner{40, "newname"}, at)
		add(owner{41, "reused"}, at)
		add(owner{42, "moved"}, at)
		if i%5 == 0 {
			add(owner{31, "kjl_Dev"}, now-30+int64(i%20))
		}
	}
	insertUsageLogs(t, db, rows...)

	m := newChannelUpstreamTestMonitor(t)
	m.prodDB = db
	direct := func(q string, args ...any) int64 {
		var v int64
		if err := db.QueryRow(q, args...).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	filters := []url.Values{
		{},
		{"type": {"2"}},
		{"type": {"5"}},
		{"model_name": {"m1"}},
		{"token_name": {"b"}},
		{"channel": {"9"}},
		{"group": {"codex"}},
		{"start_timestamp": {fmt.Sprint(now - 1800)}},
		{"start_timestamp": {fmt.Sprint(now - 5000)}, "end_timestamp": {fmt.Sprint(now - 600)}, "model_name": {"m2"}},
	}
	for _, username := range []string{"kjl_Dev", "newname", "oldname", "reused", "moved"} {
		for _, extra := range filters {
			q := url.Values{"username": {username}}
			for k, v := range extra {
				q[k] = v
			}
			where := "username = ?"
			args := []any{username}
			listWhere, statWhere := where, where
			listArgs, statArgs := append([]any{}, args...), append([]any{}, args...)
			if v := extra.Get("type"); v != "" {
				listWhere += " AND type = ?"
				listArgs = append(listArgs, v)
			}
			for col, key := range map[string]string{"model_name": "model_name", "token_name": "token_name", "channel_id": "channel", "`group`": "group"} {
				if v := extra.Get(key); v != "" {
					listWhere += " AND " + col + " = ?"
					listArgs = append(listArgs, v)
					statWhere += " AND " + col + " = ?"
					statArgs = append(statArgs, v)
				}
			}
			if v := extra.Get("start_timestamp"); v != "" {
				listWhere += " AND created_at >= ?"
				listArgs = append(listArgs, v)
				statWhere += " AND created_at >= ?"
				statArgs = append(statArgs, v)
			}
			if v := extra.Get("end_timestamp"); v != "" {
				listWhere += " AND created_at <= ?"
				listArgs = append(listArgs, v)
				statWhere += " AND created_at <= ?"
				statArgs = append(statArgs, v)
			}
			wantCount := direct("SELECT COUNT(*) FROM logs WHERE "+listWhere, listArgs...)
			wantQuota := direct("SELECT COALESCE(SUM(quota),0) FROM logs WHERE type = 2 AND "+statWhere, statArgs...)

			var countResp struct {
				Success bool
				Message string
				Data    struct{ Total int64 }
			}
			w := usageLogsGET(t, m, m.serveUsageLogsCount, "/usage-logs/count?"+q.Encode(), nil)
			if json.Unmarshal(w.Body.Bytes(), &countResp) != nil || !countResp.Success {
				t.Fatalf("%s %v count: %s", username, extra, w.Body.String())
			}
			if countResp.Data.Total != wantCount {
				t.Fatalf("%s %v: /count=%d, username predicate=%d", username, extra, countResp.Data.Total, wantCount)
			}
			var list usageLogsListResp
			w = usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?page_size=10&"+q.Encode(), nil)
			if json.Unmarshal(w.Body.Bytes(), &list) != nil || !list.Success {
				t.Fatalf("%s %v list: %s", username, extra, w.Body.String())
			}
			if list.Data.Total != wantCount {
				t.Fatalf("%s %v: list total=%d, username predicate=%d", username, extra, list.Data.Total, wantCount)
			}
			for _, it := range list.Data.Items {
				if it.Username != username {
					t.Fatalf("%s %v: page row %d has username %q", username, extra, it.ID, it.Username)
				}
			}
			var stat struct {
				Success bool
				Data    struct{ Quota, RPM, TPM int64 }
			}
			w = usageLogsGET(t, m, m.serveUsageLogsStat, "/usage-logs/stat?"+q.Encode(), nil)
			if json.Unmarshal(w.Body.Bytes(), &stat) != nil || !stat.Success {
				t.Fatalf("%s %v stat: %s", username, extra, w.Body.String())
			}
			if stat.Data.Quota != wantQuota {
				t.Fatalf("%s %v: stat quota=%d, username predicate=%d", username, extra, stat.Data.Quota, wantQuota)
			}
		}
	}
	// Every endpoint above is checked against the original historical name,
	// including renamed/reused names; no current-user equivalence assumption.
}
