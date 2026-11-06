package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func tailPageJournalFixture(t *testing.T, kind string, count int) (*sql.DB, *pageTailDriver, *sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan, time.Time) {
	t.Helper()
	source, d, now := pageTailSource(t, kind, nil)
	for i := int64(1); i <= int64(count); i++ {
		typ := int64(2)
		if kind == "global_funding" {
			typ = 1
		}
		d.items = append(d.items, pageTailItem{id: i, at: d.r.Wallet.Boundary + 10, typ: typ, user: 201, quota: i % 7, billing: "wallet", content: "own-topup"})
	}
	receiver, file, key, scope, plan := fundingMonthCaptureFixture(t, d.r.Wallet.SourceIdentity)
	if err := InitWalletTailPageJournal(t.Context(), receiver); err != nil {
		t.Fatal(err)
	}
	return source, d, receiver, file, key, scope, plan, now
}

func tailPageJournalRowCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM portal_monthly_tail_page_journal`).Scan(&n); err != nil || n != want {
		t.Fatal("unexpected durable page count", n, want, err)
	}
}

func TestWalletTailPageJournalRestartResumeAndOfflineReuse(t *testing.T) {
	source, d, receiver, file, key, scope, _, now := tailPageJournalFixture(t, "user_usage", walletTailPageRows+37)
	step := func(db *sql.DB) (WalletTailPageJournalProgress, error) {
		return captureNextWalletTailPage(t.Context(), source, db, key, scope, 201, "user_usage", 0, func() time.Time { return now })
	}
	first, err := step(receiver)
	if err != nil || first.Pages != 1 || first.Traversed || !first.Captured || first.After.ID != walletTailPageRows {
		t.Fatal("first bounded page not committed", first, err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", file+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	d.r.After, now = first.After, now.Add(time.Second)
	d.fail = "body"
	if p, err := step(reopened); err == nil || p != (WalletTailPageJournalProgress{}) {
		t.Fatal("failed resumed page advanced the durable cursor", p, err)
	}
	tailPageJournalRowCount(t, reopened, 1)
	d.fail = ""
	registeredAt := d.s.data[0][1]
	d.s.data[0][1] = d.r.Wallet.Boundary - 2
	if p, err := step(reopened); err == nil || p != (WalletTailPageJournalProgress{}) {
		t.Fatal("changed account registration was mixed into a traversal", p, err)
	}
	tailPageJournalRowCount(t, reopened, 1)
	d.s.data[0][1] = registeredAt
	second, err := step(reopened)
	if err != nil || second.Pages != 2 || !second.Traversed || !second.Captured || second.After.ID != walletTailPageRows+37 || second.LastSHA == first.LastSHA {
		t.Fatal("restart lost/replaced a page", second, err)
	}
	tailPageJournalRowCount(t, reopened, 2)
	var got int64
	rows, err := reopened.Query(`SELECT payload FROM portal_monthly_tail_page_journal ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var raw []byte
		var p WalletTailPageObservation
		if rows.Scan(&raw) != nil || json.Unmarshal(raw, &p) != nil {
			t.Fatal("invalid retained original")
		}
		for _, v := range p.Usage {
			got += v.Quota
		}
		var full WalletTailObservation
		if json.Unmarshal(raw, &full) != nil || ValidateWalletTailObservation(full) == nil {
			t.Fatal("checkpoint became full financial proof")
		}
	}
	if rows.Err() != nil || rows.Close() != nil {
		t.Fatal("page audit interrupted")
	}
	var want int64
	for _, item := range d.items {
		want += item.quota
	}
	if got != want {
		t.Fatal("resumption changed money", got, want)
	}
	queries := len(d.s.queries)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := step(reopened)
	second.Captured = false
	if err != nil || again != second || len(d.s.queries) != queries {
		t.Fatal("finished traversal re-read source", again, err)
	}
	for _, q := range []string{
		`UPDATE portal_monthly_tail_page_journal SET id='changed'`,
		`DELETE FROM portal_monthly_tail_page_journal`,
		`INSERT OR REPLACE INTO portal_monthly_tail_page_journal SELECT * FROM portal_monthly_tail_page_journal`,
	} {
		if _, err := reopened.Exec(q); err == nil {
			t.Fatal("committed original was mutable", q)
		}
	}
}

func TestWalletTailPageJournalFailureDoesNotAdvanceAndStaleWriterCannotAppend(t *testing.T) {
	for _, fault := range []string{"source", "write", "cancel", "schema", "plan", "scope", "member", "kind", "pass", "second_before_first", "nil_context"} {
		t.Run(fault, func(t *testing.T) {
			source, d, receiver, _, key, scope, _, now := tailPageJournalFixture(t, "user_usage", 1)
			ctx := t.Context()
			user, kind, pass := int64(201), "user_usage", 0
			switch fault {
			case "source":
				d.fail = "index"
			case "write":
				if _, err := receiver.Exec(`CREATE TRIGGER synthetic_tail_page_write BEFORE INSERT ON portal_monthly_tail_page_journal BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "schema":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_tail_page_append`); err != nil {
					t.Fatal(err)
				}
			case "plan":
				key = strings.Repeat("a", 64)
			case "scope":
				scope.AuthVersion++
			case "member":
				user = 999
			case "kind":
				kind = "other"
			case "pass":
				pass = 2
			case "second_before_first":
				pass = 1
			case "nil_context":
				ctx = nil
			}
			p, err := captureNextWalletTailPage(ctx, source, receiver, key, scope, user, kind, pass, func() time.Time { return now })
			if err == nil || p != (WalletTailPageJournalProgress{}) {
				t.Fatal("failed step returned progress", p, err)
			}
			tailPageJournalRowCount(t, receiver, 0)
			if fault != "source" && fault != "write" && len(d.s.queries) != 0 {
				t.Fatal("invalid state accessed financial source")
			}
		})
	}
	source, d, receiver, _, key, scope, plan, now := tailPageJournalFixture(t, "user_usage", 1)
	o, err := captureWalletTailPage(t.Context(), source, d.r, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	s := walletTailPageSeries{JobKey: key, PlanSHA: registeredCapturePlanSHA(plan), ScopeSHA: registeredCaptureScopeSHA(scope), Request: d.r}
	if err := saveTailPageJournal(t.Context(), receiver, s, WalletTailPageJournalProgress{}, o, now); err != nil {
		t.Fatal(err)
	}
	if err := saveTailPageJournal(t.Context(), receiver, s, WalletTailPageJournalProgress{}, o, now); err == nil {
		t.Fatal("stale writer replaced/duplicated committed page")
	}
	tailPageJournalRowCount(t, receiver, 1)
}

func TestWalletTailPageJournalBackfillRequiresSeparateTraversalNotAppendOrApproval(t *testing.T) {
	source, d, receiver, _, key, scope, _, now := tailPageJournalFixture(t, "user_usage", walletTailPageRows+1)
	step := func(pass int) (WalletTailPageJournalProgress, error) {
		return captureNextWalletTailPage(t.Context(), source, receiver, key, scope, 201, "user_usage", pass, func() time.Time { return now })
	}
	first, err := step(0)
	if err != nil {
		t.Fatal(err)
	}
	// A later insert has a larger ID but an older event time, behind the cursor.
	d.items = append(d.items, pageTailItem{id: 90001, at: d.r.Wallet.Boundary + 1, typ: 2, user: 201, quota: 99, billing: "wallet"})
	d.r.After, now = first.After, now.Add(time.Second)
	last, err := step(0)
	if err != nil || !last.Traversed {
		t.Fatal(last, err)
	}
	queries := len(d.s.queries)
	if p, err := step(1); !errors.Is(err, ErrWalletCaptureBusy) || p != (WalletTailPageJournalProgress{}) || len(d.s.queries) != queries {
		t.Fatal("independent traversal wait was bypassed", p, err)
	}
	now, d.r.After = now.Add(5*time.Minute), WalletTailPageCursor{}
	second, err := step(1)
	if err != nil || second.Pages != 1 || second.After == first.After || second.LastSHA == first.LastSHA || !second.Captured {
		t.Fatal("backfill was not independently rediscovered", second, err)
	}
	tailPageJournalRowCount(t, receiver, 3)
	for _, field := range []string{"Complete", "Approved", "WalletOnly", "BalanceQuota", "UsedQuota"} {
		if _, ok := reflect.TypeOf(second).FieldByName(field); ok {
			t.Fatal("checkpoint acquired a financial approval or inferred wallet")
		}
	}
}

func TestWalletTailPageJournalFundingIsolationAndExactCandidateAccounting(t *testing.T) {
	source, d, receiver, _, key, scope, _, now := tailPageJournalFixture(t, "global_funding", 401)
	for i := range d.items {
		if i%3 == 0 {
			d.items[i].typ, d.items[i].user, d.items[i].content = 3, 999, "admin-actor"
		} else if i%5 == 0 {
			d.items[i].user, d.items[i].content = 202, "foreign-private-topup"
		}
	}
	for i := 0; i < 4; i++ {
		p, err := captureNextWalletTailPage(t.Context(), source, receiver, key, scope, 201, "global_funding", 0, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if p.Traversed {
			if p.Pages != 3 {
				t.Fatal("unexpected funding page budget")
			}
			break
		}
		d.r.After, now = p.After, now.Add(time.Second)
	}
	rows, err := receiver.Query(`SELECT payload FROM portal_monthly_tail_page_journal ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	var candidates, seenActor int
	for rows.Next() {
		var raw []byte
		var p WalletTailPageObservation
		if rows.Scan(&raw) != nil || json.Unmarshal(raw, &p) != nil || strings.Contains(string(raw), "foreign-private-topup") {
			t.Fatal("retained funding contains another account's private data")
		}
		candidates += len(p.FundingKeys)
		for _, row := range p.Funding {
			if row.Type == 3 && row.UserID == 999 {
				seenActor++
			} else if row.UserID != 201 {
				t.Fatal("foreign funding assigned to requested account")
			}
		}
	}
	if rows.Err() != nil || rows.Close() != nil || candidates != 401 || seenActor == 0 {
		t.Fatal("funding candidates omitted/doubled or actor replaced", candidates, seenActor)
	}
}

func TestWalletTailPageJournalRolloverCorruptionAndCapacityStopBeforeSource(t *testing.T) {
	for _, fault := range []string{"rollover", "expiry", "corrupt", "gap", "rows_capacity", "bytes_capacity"} {
		t.Run(fault, func(t *testing.T) {
			source, d, receiver, _, key, scope, _, now := tailPageJournalFixture(t, "user_usage", walletTailPageRows+1)
			if fault == "rollover" {
				now = time.Unix(d.r.Wallet.Boundary, 0).In(fundingMonthZone).AddDate(0, 1, 0).Add(-2 * time.Minute)
			}
			if fault == "rows_capacity" || fault == "bytes_capacity" {
				q := `WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<4096)
INSERT INTO portal_monthly_tail_page_journal SELECT printf('%064d',v),0,printf('%064d',v),'',1,1,X'00' FROM n`
				if fault == "bytes_capacity" {
					q = `WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<32)
INSERT INTO portal_monthly_tail_page_journal SELECT printf('%064d',v),0,printf('%064d',v),'',1,1,zeroblob(2097152) FROM n`
				}
				if _, err := receiver.Exec(q); err != nil {
					t.Fatal(err)
				}
			} else {
				p, err := captureNextWalletTailPage(t.Context(), source, receiver, key, scope, 201, "user_usage", 0, func() time.Time { return now })
				if err != nil {
					t.Fatal(err)
				}
				d.r.After = p.After
				switch fault {
				case "rollover":
					now = now.Add(4 * time.Minute)
				case "expiry":
					now = now.Add(24*time.Hour + time.Millisecond)
				case "corrupt":
					if _, err := receiver.Exec(`DROP TRIGGER monthly_tail_page_no_update`); err != nil {
						t.Fatal(err)
					}
					if _, err := receiver.Exec(`UPDATE portal_monthly_tail_page_journal SET payload=X'00'`); err != nil {
						t.Fatal(err)
					}
					if err := InitWalletTailPageJournal(t.Context(), receiver); err != nil {
						t.Fatal(err)
					}
				case "gap":
					if _, err := receiver.Exec(`DROP TRIGGER monthly_tail_page_no_update`); err != nil {
						t.Fatal(err)
					}
					if _, err := receiver.Exec(`UPDATE portal_monthly_tail_page_journal SET seq=1`); err != nil {
						t.Fatal(err)
					}
					if err := InitWalletTailPageJournal(t.Context(), receiver); err != nil {
						t.Fatal(err)
					}
				}
			}
			queries := len(d.s.queries)
			p, err := captureNextWalletTailPage(t.Context(), source, receiver, key, scope, 201, "user_usage", 0, func() time.Time { return now })
			if err == nil || p != (WalletTailPageJournalProgress{}) || len(d.s.queries) != queries {
				t.Fatal("rollover/corruption/capacity initiated a fresh source traversal", p, err)
			}
		})
	}
}
