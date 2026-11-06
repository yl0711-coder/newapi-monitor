package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fundingAuditFixture(t *testing.T, records []FundingLog, startMS int64) []FundingPage {
	t.Helper()
	pin, err := WalletSourceIdentity("funding-audit-test", "funding-audit-test")
	if err != nil {
		t.Fatal(err)
	}
	scan, err := NewFundingScan(pin, fundingTestRules(), []int64{201, 202}, 1780000000, 1780003600)
	if err != nil {
		t.Fatal(err)
	}
	pages := []FundingPage{}
	for offset := 0; ; {
		end := min(offset+FundingPageLimit, len(records))
		p := FundingPage{Version: 1, Scan: scan, FenceID: 1000000, EndReached: end == len(records), StartedAtMS: startMS + int64(len(pages))*100, CompletedAtMS: startMS + int64(len(pages))*100 + 10, Rows: append([]FundingLog{}, records[offset:end]...)}
		if len(pages) > 0 {
			previous := pages[len(pages)-1]
			raw, err := fundingPageBytes(previous)
			if err != nil {
				t.Fatal(err)
			}
			p.PreviousID, p.After = digest(raw), previous.Next
		}
		p.Next = p.After
		if len(p.Rows) > 0 {
			row := p.Rows[len(p.Rows)-1]
			p.Next = FundingCursor{At: row.CreatedAt, ID: row.ID}
		}
		pages = append(pages, p)
		if p.EndReached {
			return pages
		}
		offset = end
	}
}

func fundingAuditRow(id, target int64, amount string) FundingLog {
	return FundingLog{ID: id, UserID: 9, CreatedAt: 1780000010, Type: 3, Other: fundingTestAudit("user.quota_add", fmt.Sprintf(`{"target_user_id":%d,"quota":%q}`, target, amount))}
}

func saveFundingAuditFixture(t *testing.T, db *sql.DB, pages []FundingPage) string {
	t.Helper()
	for _, p := range pages {
		if _, err := SaveFundingPage(context.Background(), db, p); err != nil {
			t.Fatal(err)
		}
	}
	return pages[0].Scan.ID
}

func TestFundingComparisonFullChainAndStableIndependentAccounts(t *testing.T) {
	db, path := fundingTestStore(t)
	rows := make([]FundingLog, 407)
	for i := range rows {
		rows[i] = fundingAuditRow(int64(i+1), 201+int64(i%2), "1 点额度")
	}
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
	// Audit must also work after reopening strictly read-only, without schema
	// setup, source credentials, or a writable receiver connection.
	ro, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	ro.SetMaxOpenConns(1)
	got, err := CompareFundingScans(context.Background(), ro, old, newer)
	if err != nil || !got.ObservationsMatch || got.OlderRows != 407 || got.NewerRows != 407 || len(got.Changes) != 0 || len(got.Accounts) != 2 || !got.Accounts[0].ObservationsMatch || !got.Accounts[1].ObservationsMatch {
		t.Fatal("stable three-page observations not recognized", err, got)
	}
	if got.OlderHeadID == got.NewerHeadID || !validDigest(got.OlderHeadID) || !validDigest(got.NewerHeadID) {
		t.Fatal("independent versions lost")
	}
	var count int
	if err := ro.QueryRow("SELECT COUNT(*) FROM portal_monthly_funding_pages").Scan(&count); err != nil || count != 6 {
		t.Fatal("audit changed storage", count, err)
	}
}

func TestFundingComparisonRepresentativeCapacity(t *testing.T) {
	db, _ := fundingTestStore(t)
	rows := make([]FundingLog, 5000)
	for i := range rows {
		rows[i] = fundingAuditRow(int64(i+1), 201+int64(i%2), "100 点额度")
	}
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
	started := time.Now()
	got, err := CompareFundingScans(context.Background(), db, old, newer)
	if err != nil || !got.ObservationsMatch || got.NewerRows != len(rows) || got.OlderRows != len(rows) {
		t.Fatal("representative scan unexpectedly truncated or timed out", err)
	}
	t.Logf("two independent 5,000-record/25-page scans, including dedup indexes, audited in %s; local synthetic sample only", time.Since(started))
}

func TestFundingComparisonDetectsChangesEvenWhenTotalsMatch(t *testing.T) {
	db, _ := fundingTestStore(t)
	before := []FundingLog{fundingAuditRow(1, 201, "10 点额度"), fundingAuditRow(2, 202, "20 点额度"), fundingAuditRow(3, 201, "1 点额度"), fundingAuditRow(4, 201, "2 点额度"), fundingAuditRow(5, 999, "1 点额度")}
	after := []FundingLog{fundingAuditRow(1, 201, "11 点额度"), fundingAuditRow(2, 202, "19 点额度"), fundingAuditRow(3, 202, "1 点额度"), fundingAuditRow(5, 999, "2 点额度"), fundingAuditRow(6, 202, "2 点额度")}
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, before, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, after, 1780100005000))
	got, err := CompareFundingScans(context.Background(), db, old, newer)
	if err != nil || got.ObservationsMatch || len(got.Changes) != 6 {
		t.Fatal("same net or same row count hid changes", err, got)
	}
	want := []FundingObservedChange{
		{LogID: 1, Kind: "changed", AffectedUserIDs: []int64{201}},
		{LogID: 2, Kind: "changed", AffectedUserIDs: []int64{202}},
		{LogID: 3, Kind: "changed", AffectedUserIDs: []int64{201, 202}},
		{LogID: 4, Kind: "absent_in_new_scan", AffectedUserIDs: []int64{201}},
		{LogID: 5, Kind: "changed"},
		{LogID: 6, Kind: "inserted", AffectedUserIDs: []int64{202}},
	}
	if !reflect.DeepEqual(got.Changes, want) || !reflect.DeepEqual(got.Accounts[0].ChangedLogIDs, []int64{1, 3, 4}) || !reflect.DeepEqual(got.Accounts[1].ChangedLogIDs, []int64{2, 3, 6}) {
		t.Fatal("cross-account attribution incorrect", got)
	}
}

func TestFundingComparisonScopesReviewWithoutGuessingAmounts(t *testing.T) {
	db, _ := fundingTestStore(t)
	rows := []FundingLog{fundingAuditRow(1, 202, "unknown amount"), fundingAuditRow(2, 999, "unknown amount"), {ID: 3, UserID: 201, CreatedAt: 1780000010, Type: 4, Content: "邀请用户赠送 10 点额度"}, fundingAuditRow(4, 201, "5 点额度")}
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
	got, err := CompareFundingScans(context.Background(), db, old, newer)
	if err != nil || !got.ObservationsMatch || len(got.UnscopedReviewLogIDs) != 0 || len(got.Accounts[0].ReviewLogIDs) != 0 || !reflect.DeepEqual(got.Accounts[1].ReviewLogIDs, []int64{1}) || !reflect.DeepEqual(got.Accounts[0].SeparateLedgerLogIDs, []int64{3}) {
		t.Fatal("known unrelated account blocked or invented amount", err, got)
	}
	rows[0].Other = `{"op":{"action":"future.multi_account","params":{"target_user_id":202}},"secret":"synthetic-secret-never-output"}`
	last := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100010000))
	got, err = CompareFundingScans(context.Background(), db, newer, last)
	if err != nil || !reflect.DeepEqual(got.UnscopedReviewLogIDs, []int64{1}) || !reflect.DeepEqual(got.UnscopedChangedLogIDs, []int64{1}) || got.Accounts[0].ObservationsMatch || got.Accounts[1].ObservationsMatch {
		t.Fatal("ambiguous action claimed independent stability", err, got)
	}
	raw, err := json.Marshal(got)
	if err != nil || strings.Contains(string(raw), "synthetic-secret") || strings.Contains(string(raw), "future.multi_account") || strings.Contains(string(raw), "unknown amount") {
		t.Fatal("raw audit leaked to result")
	}
}

func TestFundingComparisonAccountLocalAndMetadataChanges(t *testing.T) {
	db, _ := fundingTestStore(t)
	rows := []FundingLog{fundingAuditRow(1, 202, "1 点额度")}
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
	rows[0].Content = "same money; changed source metadata"
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
	got, err := CompareFundingScans(context.Background(), db, old, newer)
	if err != nil || got.ObservationsMatch || !got.Accounts[0].ObservationsMatch || got.Accounts[1].ObservationsMatch || !reflect.DeepEqual(got.Accounts[1].ChangedLogIDs, []int64{1}) {
		t.Fatal("metadata change ignored or unrelated account affected", err, got)
	}
}

func TestFundingComparisonRejectsIncomparableObservations(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*FundingPage)
	}{
		{"overlapping_capture", func(p *FundingPage) { p.StartedAtMS, p.CompletedAtMS = 1780100000005, 1780100000015 }},
		{"same_capture_boundary", func(p *FundingPage) { p.StartedAtMS, p.CompletedAtMS = 1780100000010, 1780100000020 }},
		{"fence_regression", func(p *FundingPage) { p.FenceID-- }},
		{"source_changed", func(p *FundingPage) { p.Scan.SourceIdentity = strings.Repeat("b", 64) }},
		{"scope_changed", func(p *FundingPage) { p.Scan.UserIDs = []int64{201} }},
		{"window_changed", func(p *FundingPage) { p.Scan.To-- }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, nil, 1780100000000))
			pages := fundingAuditFixture(t, nil, 1780100005000)
			tc.edit(&pages[0])
			newer := saveFundingAuditFixture(t, db, pages)
			got, err := CompareFundingScans(context.Background(), db, old, newer)
			if err == nil || !reflect.DeepEqual(got, FundingScanComparison{}) {
				t.Fatal("incomparable scans accepted", err, got)
			}
		})
	}
}

func TestFundingComparisonEmptyIsObservationNotFinancialProof(t *testing.T) {
	db, _ := fundingTestStore(t)
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, nil, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, nil, 1780100005000))
	got, err := CompareFundingScans(context.Background(), db, old, newer)
	if err != nil || !got.ObservationsMatch || got.NewerRows != 0 {
		t.Fatal(err, got)
	}
	raw, _ := json.Marshal(got)
	for _, forbidden := range []string{"complete", "publish", "eligible", "delta_quota", "balance", "amount"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("observation acquired financial authority", forbidden)
		}
	}
}

func TestFundingAffectedTargetSafety(t *testing.T) {
	for _, tc := range []struct {
		name, other string
		want        int64
		known       bool
	}{
		{"bad_amount_known_target", fundingTestAudit("user.quota_add", `{"target_user_id":202,"quota":null}`), 202, true},
		{"unknown_action", fundingTestAudit("user.quota_future", `{"target_user_id":202}`), 0, false},
		{"duplicate_target", fundingTestAudit("user.quota_add", `{"target_user_id":202,"target_user_id":201,"quota":"wrong"}`), 0, false},
		{"null_target", fundingTestAudit("user.quota_add", `{"target_user_id":null,"quota":"wrong"}`), 0, false},
		{"missing_target", `{"op":{"action":"user.quota_add","params":{"quota":"wrong"}}}`, 0, false},
		{"self_confirmed", `{"admin_info":{"admin_id":9},"op":{"action":"user.quota_add","params":{"quota":"wrong"}}}`, 9, true},
		{"case_alias", `{"Op":{},"op":{"action":"user.quota_add","params":{"target_user_id":202,"quota":"wrong"}}}`, 0, false},
		{"extra_targets", fundingTestAudit("user.quota_add", `{"target_user_id":202,"second_user_id":201,"quota":"wrong"}`), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := fundingAuditRow(1, 201, "1 点额度")
			row.Other = tc.other
			result := NormalizeFundingLog(row, fundingTestRules())
			if result.Status != FundingReview || result.DeltaQuota != nil {
				t.Fatal("invalid amount accepted", result)
			}
			target, known := fundingAffectedTarget(row, result)
			if known != tc.known || (known && target != tc.want) {
				t.Fatal("unsafe attribution", target, known)
			}
		})
	}
}

func TestFundingComparisonCancelledBusyAndMissing(t *testing.T) {
	db, _ := fundingTestStore(t)
	old := saveFundingAuditFixture(t, db, fundingAuditFixture(t, nil, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, nil, 1780100005000))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CompareFundingScans(ctx, db, old, newer); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
	fundingAuditSlots <- struct{}{}
	_, err := CompareFundingScans(context.Background(), db, old, newer)
	<-fundingAuditSlots
	if !errors.Is(err, ErrFundingAuditBusy) {
		t.Fatal("unbounded concurrent audit", err)
	}
	for _, ids := range [][2]string{{old, old}, {"bad", newer}, {old, strings.Repeat("a", 32)}} {
		if got, err := CompareFundingScans(context.Background(), db, ids[0], ids[1]); err == nil || !reflect.DeepEqual(got, FundingScanComparison{}) {
			t.Fatal("same/missing/invalid scan accepted", got, err)
		}
	}
	// Holding the only local connection must honor the caller's shorter bound.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = CompareFundingScans(short, db, old, newer)
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("connection queue not bounded", err)
	}
	if _, err := CompareFundingScans(context.Background(), db, old, newer); err != nil {
		t.Fatal("cancel/busy leaked audit slot", err)
	}
}
