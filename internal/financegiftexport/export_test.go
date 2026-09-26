package financegiftexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func giftExportData(count int) [][]driver.Value {
	rows := make([][]driver.Value, count)
	for i := range rows {
		rows[i] = []driver.Value{int64(i + 1), int64(7), int64(3600 + i/3), int64(2), int64(i), "business"}
	}
	return rows
}

func TestGiftScopeExplicitExportBoundsAndPrivacy(t *testing.T) {
	for _, expected := range []int{0, 1, 100, 101, 2498, 3000} {
		t.Run(fmt.Sprint(expected), func(t *testing.T) {
			count := expected
			if count == 0 {
				count = 100 // Existing invocation without an explicit bound.
			}
			db, state := giftExportTestDB(t, giftExportData(count))
			path := filepath.Join(t.TempDir(), "evidence.json")
			if err := Export(context.Background(), db, 7, 3600, path, expected); err != nil {
				t.Fatal(err)
			}
			if !state.options.ReadOnly || state.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !state.committed || len(state.queries) != 2 {
				t.Fatal("readonly, isolation, commit or query count changed")
			}
			if state.queries[0] != "EXPLAIN "+state.queries[1] || !strings.HasSuffix(state.queries[1], fmt.Sprintf("LIMIT %d", count+1)) {
				t.Fatal("plan must validate the exact bounded query")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var evidence struct {
				UserID int64            `json:"user_id"`
				Hour   int64            `json:"hour_ts"`
				Rows   []map[string]any `json:"rows"`
			}
			if err = json.Unmarshal(data, &evidence); err != nil || evidence.UserID != 7 || evidence.Hour != 3600 || len(evidence.Rows) != count {
				t.Fatal("incomplete exported evidence", err)
			}
			for _, r := range evidence.Rows {
				if len(r) != 6 {
					t.Fatal("unexpected fields exported")
				}
				for _, key := range []string{"id", "user_id", "created_at", "type", "quota", "group"} {
					if _, ok := r[key]; !ok {
						t.Fatal("missing evidence field", key)
					}
				}
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("evidence is not private", err)
			}
			files, _ := os.ReadDir(filepath.Dir(path))
			if len(files) != 1 {
				t.Fatal("temporary export file leaked")
			}
		})
	}
}

func TestGiftScopeExportFailsClosed(t *testing.T) {
	for _, scenario := range []string{"default_overflow", "empty", "missing", "extra", "duplicate", "unordered", "other_user", "outside_hour", "negative_quota", "invalid_type", "null_group", "scan_error", "commit_error", "unsafe_plan", "wide_plan", "bad_bound", "negative_bound", "cancel", "oversize"} {
		t.Run(scenario, func(t *testing.T) {
			data, expected := giftExportData(101), 101
			switch scenario {
			case "empty":
				data, expected = nil, 0
			case "default_overflow":
				expected = 0
			case "missing":
				expected++
			case "extra":
				expected--
			case "duplicate":
				data[100][0] = data[99][0]
			case "unordered":
				data[100][2] = int64(3600)
			case "other_user":
				data[100][1] = int64(8)
			case "outside_hour":
				data[100][2] = int64(7200)
			case "negative_quota":
				data[100][4] = int64(-1)
			case "invalid_type":
				data[100][3] = int64(1)
			case "null_group":
				data[100][5] = nil
			case "bad_bound":
				expected = 3001
			case "negative_bound":
				expected = -1
			case "oversize":
				data[100][5] = strings.Repeat("x", giftScopeExportBytes)
			}
			db, state := giftExportTestDB(t, data)
			switch scenario {
			case "scan_error":
				state.readErr = errors.New("interrupted stream")
			case "commit_error":
				state.commitErr = errors.New("commit failed")
			case "unsafe_plan":
				state.planType = "ALL"
			case "wide_plan":
				state.planRows = "10001"
			case "cancel":
				state.waitForCancel = true
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			path := filepath.Join(t.TempDir(), "evidence.json")
			if err := Export(ctx, db, 7, 3600, path, expected); err == nil {
				t.Fatal("unsafe export accepted")
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("failed query published evidence")
			}
			if len(state.queries) > 2 {
				t.Fatal("automatic retry widened production work")
			}
			if (scenario == "unsafe_plan" || scenario == "wide_plan") && len(state.queries) != 1 {
				t.Fatal("data read despite failed EXPLAIN")
			}
			if (scenario == "bad_bound" || scenario == "negative_bound") && len(state.queries) != 0 {
				t.Fatal("invalid bound reached source")
			}
		})
	}
}

func TestGiftScopeEvidencePublicationDoesNotOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.json")
	if err := os.WriteFile(path, []byte("previous"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publishGiftScopeEvidence(path, []byte("replacement")); err == nil {
		t.Fatal("overwrote existing output")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "previous" {
		t.Fatal("old evidence changed")
	}
	files, _ := os.ReadDir(filepath.Dir(path))
	if len(files) != 1 {
		t.Fatal("failed publication left temporary file")
	}
}
