package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Deliberately bypass append-only protection ONLY in synthetic disposable DBs
// to model corrupt copies. Production audit must detect, never repair, damage.
func fundingAuditTamper(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	for _, name := range []string{"monthly_funding_pages_no_update", "monthly_funding_pages_no_delete", "monthly_funding_records_no_update", "monthly_funding_records_no_delete"} {
		if _, err := db.Exec("DROP TRIGGER IF EXISTS " + name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestFundingAuditRejectsDamagedFullChains(t *testing.T) {
	for _, tc := range []struct {
		name   string
		query  string
		params []any
	}{
		{"missing_first", "DELETE FROM portal_monthly_funding_pages WHERE scan_id=? AND sequence=1", nil},
		{"missing_middle", "DELETE FROM portal_monthly_funding_pages WHERE scan_id=? AND sequence=2", nil},
		{"unfinished", "DELETE FROM portal_monthly_funding_pages WHERE scan_id=? AND sequence=3", nil},
		{"page_hash", "UPDATE portal_monthly_funding_pages SET id='wrong' WHERE scan_id=? AND sequence=2", nil},
		{"payload", "UPDATE portal_monthly_funding_pages SET payload='{}' WHERE scan_id=? AND sequence=2", nil},
		{"missing_index", "DELETE FROM portal_monthly_funding_records WHERE scan_id=? AND log_id=201", nil},
		{"index_wrong_digest", "UPDATE portal_monthly_funding_records SET row_digest='wrong' WHERE scan_id=? AND log_id=201", nil},
		{"index_wrong_page", "UPDATE portal_monthly_funding_records SET page_id='wrong' WHERE scan_id=? AND log_id=201", nil},
		{"extra_index", "INSERT INTO portal_monthly_funding_records(scan_id,log_id,page_id,row_digest) VALUES(?,9999,'wrong','wrong')", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			rows := make([]FundingLog, 401)
			for i := range rows {
				rows[i] = fundingAuditRow(int64(i+1), 201, "1 点额度")
			}
			old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
			newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
			fundingAuditTamper(t, db, tc.query, append([]any{newer}, tc.params...)...)
			got, err := CompareFundingScans(context.Background(), db, old, newer)
			if err == nil || !reflect.DeepEqual(got, FundingScanComparison{}) {
				t.Fatal("corrupt scan returned partial success", err, got)
			}
		})
	}
}

func TestFundingAuditRejectsValidPagesInInvalidChains(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]FundingPage)
	}{
		{"skipped_cursor", func(p []FundingPage) { p[1].After.ID-- }},
		{"wrong_predecessor", func(p []FundingPage) { p[1].PreviousID = strings.Repeat("a", 64) }},
		{"after_terminal", func(p []FundingPage) { p[0].EndReached = true }},
		{"changed_fence", func(p []FundingPage) { p[1].FenceID-- }},
		{"changed_scope", func(p []FundingPage) { p[1].Scan.UserIDs = []int64{201} }},
		{"time_regression", func(p []FundingPage) { p[1].StartedAtMS = p[0].StartedAtMS; p[1].CompletedAtMS = p[0].CompletedAtMS }},
		{"duplicate_cross_page", func(p []FundingPage) {
			p[1].Rows[0].ID = 1
			p[1].Rows[0].CreatedAt++
			p[1].Next = FundingCursor{At: p[1].Rows[0].CreatedAt, ID: 1}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			rows := make([]FundingLog, 201)
			for i := range rows {
				rows[i] = fundingAuditRow(int64(i+1), 201, "1 点额度")
			}
			pages := fundingAuditFixture(t, rows, 1780100000000)
			// Copy scope slices so changing page two doesn't edit page one.
			pages[1].Scan.UserIDs = append([]int64(nil), pages[1].Scan.UserIDs...)
			tc.edit(pages)
			for i, p := range pages {
				raw, err := fundingPageBytes(p)
				if err != nil {
					t.Fatal("test must contain individually valid pages", err)
				}
				if _, err := db.Exec("INSERT INTO portal_monthly_funding_pages(id,scan_id,sequence,payload) VALUES(?,?,?,?)", digest(raw), p.Scan.ID, i+1, raw); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := auditFundingScan(context.Background(), db, pages[0].Scan.ID); err == nil || strings.Contains(err.Error(), "index") {
				t.Fatal("chain failed to reject before index check", err)
			}
		})
	}
}

func TestFundingAuditRejectsNoncanonicalAndOversizedPayloads(t *testing.T) {
	for _, tc := range []string{"unknown_field", "duplicate_field", "oversized"} {
		t.Run(tc, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			p := fundingAuditFixture(t, nil, 1780100000000)[0]
			raw, err := fundingPageBytes(p)
			if err != nil {
				t.Fatal(err)
			}
			switch tc {
			case "unknown_field":
				raw = append([]byte(`{"unexpected":true,`), raw[1:]...)
			case "duplicate_field":
				raw = append([]byte(`{"version":1,`), raw[1:]...)
			case "oversized":
				if _, err := db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
					t.Fatal(err)
				}
				raw = []byte(strings.Repeat("x", 2097153))
			}
			if _, err := db.Exec("INSERT INTO portal_monthly_funding_pages(id,scan_id,sequence,payload) VALUES(?,?,1,?)", digest(raw), p.Scan.ID, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := auditFundingScan(context.Background(), db, p.Scan.ID); err == nil {
				t.Fatal("noncanonical payload accepted")
			}
		})
	}
}

func TestFundingAuditResourceBounds(t *testing.T) {
	for _, tc := range []struct {
		name, content, wantError string
		count                    int
	}{
		{"rows", "", "row budget", fundingAuditRowsLimit + 1},
		{"bytes", strings.Repeat("x", 4096), "byte budget", fundingAuditBytesLimit/4096 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			rows := make([]FundingLog, tc.count)
			for i := range rows {
				rows[i] = fundingAuditRow(int64(i+1), 201, "1 点额度")
				rows[i].Content = tc.content
			}
			pages := fundingAuditFixture(t, rows, 1780100000000)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			for i, p := range pages {
				raw, err := fundingPageBytes(p)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec("INSERT INTO portal_monthly_funding_pages(id,scan_id,sequence,payload) VALUES(?,?,?,?)", digest(raw), p.Scan.ID, i+1, raw); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			_, err = auditFundingScan(context.Background(), db, pages[0].Scan.ID)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatal("resource limit did not fail explicitly", err)
			}
		})
	}
}

func TestFundingComparisonMovedAcrossWindowAndStableUnknown(t *testing.T) {
	db, _ := fundingTestStore(t)
	rows := []FundingLog{fundingAuditRow(1, 201, "invalid amount")}
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
	got, err := CompareFundingScans(context.Background(), db, old, newer)
	if err != nil || !got.ObservationsMatch || !reflect.DeepEqual(got.Accounts[0].ReviewLogIDs, []int64{1}) {
		t.Fatal("matching unknown amount lost review requirement", err, got)
	}
	// The second window no longer contains the record. Do not equate absence
	// to a refund or cancellation; the timestamp could simply have changed.
	last := saveFundingAuditFixture(t, db, fundingAuditFixture(t, nil, 1780100010000))
	got, err = CompareFundingScans(context.Background(), db, newer, last)
	if err != nil || len(got.Changes) != 1 || got.Changes[0].Kind != "absent_in_new_scan" || got.Accounts[0].ObservationsMatch || !got.Accounts[1].ObservationsMatch {
		t.Fatal("missing source record not localized", err, got)
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "refund") || strings.Contains(string(raw), "delta_quota") {
		t.Fatal("source disappearance synthesized a money movement")
	}
}
