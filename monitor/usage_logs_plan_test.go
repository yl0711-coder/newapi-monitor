package monitor

import (
	"encoding/json"
	"fmt"
	"testing"
)

// The page rows take the user_id switch only when it selects exactly the
// username's rows; a renamed user keeps the username predicate.
func TestUsageLogsListRowsUseUserIndexWithoutChangingResults(t *testing.T) {
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
