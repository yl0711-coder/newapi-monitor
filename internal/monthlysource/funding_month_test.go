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

var fundingMonthTestNow = time.Date(2026, 10, 21, 12, 0, 0, 0, fundingMonthZone)

func fundingMonthFixture(t *testing.T, month string, ids []int64) (FundingMonthRequest, int64, int64) {
	t.Helper()
	start, err := time.ParseInLocation("2006-01", month, fundingMonthZone)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := WalletSourceIdentity("funding-month-fixture", "funding-month-fixture")
	if err != nil {
		t.Fatal(err)
	}
	return FundingMonthRequest{Month: month, SourceIdentity: pin, Rules: fundingTestRules(), UserIDs: append([]int64(nil), ids...)}, start.Unix(), start.AddDate(0, 1, 0).Unix()
}

func fundingMonthObservation(t *testing.T, db *sql.DB, request FundingMonthRequest, from, to int64, before, after []FundingLog, olderMS, newerMS, olderFence, newerFence int64) FundingWindowObservation {
	t.Helper()
	save := func(rows []FundingLog, at, fence int64) string {
		t.Helper()
		scan, err := NewFundingScan(request.SourceIdentity, request.Rules, request.UserIDs, from, to)
		if err != nil {
			t.Fatal(err)
		}
		p := FundingPage{Version: 1, Scan: scan, FenceID: fence, EndReached: true, StartedAtMS: at, CompletedAtMS: at + 10, Rows: append([]FundingLog{}, rows...)}
		for i := range p.Rows {
			if p.Rows[i].CreatedAt == 0 {
				p.Rows[i].CreatedAt = from + 1
			}
		}
		if len(p.Rows) > 0 {
			last := p.Rows[len(p.Rows)-1]
			p.Next = FundingCursor{At: last.CreatedAt, ID: last.ID}
		}
		if _, err := SaveFundingPage(context.Background(), db, p); err != nil {
			t.Fatal(err)
		}
		return scan.ID
	}
	oldID := save(before, olderMS, olderFence)
	newID := save(after, newerMS, newerFence)
	observation, err := ReviewFundingWindow(context.Background(), db, oldID, newID)
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func fundingMonthRow(id, target int64, amount string) FundingLog {
	r := fundingAuditRow(id, target, amount)
	r.CreatedAt = 0 // filled with this window's actual timestamp, not a stale fixture date
	return r
}

func TestFundingMonthCalendarAndClosedMonthContract(t *testing.T) {
	for _, tc := range []struct {
		month string
		days  int64
	}{{"2024-02", 29}, {"2026-02", 28}, {"2026-09", 30}, {"2026-08", 31}, {"2025-12", 31}} {
		t.Run(tc.month, func(t *testing.T) {
			request, from, to := fundingMonthFixture(t, tc.month, []int64{202, 201})
			got, err := reviewFundingMonth(context.Background(), request, nil, fundingMonthTestNow)
			if err != nil || got.From != from || got.To != to || got.To-got.From != tc.days*86400 || got.Timezone != "UTC+08:00" || !reflect.DeepEqual(got.MissingWindows, []FundingInterval{{From: from, To: to}}) || len(got.Accounts) != 2 || got.Accounts[0].UserID != 201 || got.Accounts[0].SelectedSeconds != 0 {
				t.Fatal("month/calendar/empty coverage incorrect", err, got)
			}
			if time.Unix(got.From, 0).UTC().Hour() != 16 || !reflect.DeepEqual(request.UserIDs, []int64{202, 201}) {
				t.Fatal("wrong timezone or mutated caller scope")
			}
		})
	}
	request, _, _ := fundingMonthFixture(t, "2026-09", []int64{201})
	for _, month := range []string{"", "2026-9", "2026-09 ", "2026-13", "2019-12", "2026-10", "2026-11"} {
		request.Month = month
		if result, err := reviewFundingMonth(context.Background(), request, nil, fundingMonthTestNow); err == nil || !reflect.DeepEqual(result, FundingMonthReview{}) {
			t.Fatal("non-closed or ambiguous month accepted", month, err)
		}
	}
	// Shanghai has already entered October even though UTC is still September.
	request.Month = "2026-09"
	now := time.Date(2026, 9, 30, 16, 0, 0, 0, time.UTC)
	if _, err := reviewFundingMonth(context.Background(), request, nil, now); err != nil {
		t.Fatal("closed month evaluated using UTC instead of Usage timezone", err)
	}
	if _, err := reviewFundingMonth(context.Background(), request, nil, now.Add(-time.Nanosecond)); err == nil {
		t.Fatal("current month accepted before local midnight")
	}
}

func TestFundingMonthExactGapsAndHalfOpenSelection(t *testing.T) {
	db, _ := fundingTestStore(t)
	request, from, to := fundingMonthFixture(t, "2026-09", []int64{201, 202})
	base := (to + 86400) * 1000
	intervals := []FundingInterval{{from + 3600, from + 7200}, {from + 7200, from + 7500}, {from + 10800, from + 14400}}
	var windows []FundingWindowObservation
	for i, interval := range intervals {
		rows := []FundingLog{fundingMonthRow(int64(i+1), 201, "1 点额度")}
		rows[0].CreatedAt = interval.From
		windows = append(windows, fundingMonthObservation(t, db, request, interval.From, interval.To, rows, rows, base+int64(i)*20, base+300000+int64(i)*20, 100, 100))
	}
	windows[0], windows[2] = windows[2], windows[0]
	firstInput := windows[0]
	got, err := ReviewFundingMonth(context.Background(), request, windows)
	wantMissing := []FundingInterval{{from, from + 3600}, {from + 7500, from + 10800}, {from + 14400, to}}
	if err != nil || !reflect.DeepEqual(got.MissingWindows, wantMissing) || len(got.Conflicts) != 0 || windows[0] != firstInput {
		t.Fatal("gap handling, ordering or caller immutability failed", err, got)
	}
	for _, account := range got.Accounts {
		if account.ObservedSeconds != 7500 || account.MatchingSeconds != 7500 || account.SelectedSeconds != 7500 || len(account.Windows) != 3 {
			t.Fatal("partial window duration wrong", account)
		}
		for i, window := range account.Windows {
			version := got.Windows[window.WindowIndex]
			if window.WindowIndex != i || version.From != intervals[i].From || version.To != intervals[i].To || !window.Selected || version.NewerScanID == version.OlderScanID || !validDigest(version.NewerHeadID) || !validDigest(version.OlderHeadID) || len(window.Issues) != 0 {
				t.Fatal("ambiguous version selection", window)
			}
		}
	}
}

func TestFundingMonthRejectsOverlapScopeAndMixedSweeps(t *testing.T) {
	for _, name := range []string{"duplicate_object", "duplicate_range", "overlap", "before_month", "after_month", "different_source", "different_scope", "interleaved_sweeps", "sweep_fence_regression", "zero_observation", "serialized_report"} {
		t.Run(name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			request, from, to := fundingMonthFixture(t, "2026-09", []int64{201, 202})
			base := (to + 86400) * 1000
			first := fundingMonthObservation(t, db, request, from, from+3600, nil, nil, base, base+300000, 100, 100)
			secondRequest := request
			left, right := from+3600, from+7200
			oldMS, newMS, oldFence, newFence := base+20, base+300020, int64(100), int64(100)
			switch name {
			case "duplicate_range":
				left, right = from, from+3600
			case "overlap":
				left, right = from+1800, from+5400
			case "before_month":
				left, right = from-1, from+3599
			case "after_month":
				left, right = to-3599, to+1
			case "different_source":
				secondRequest.SourceIdentity = strings.Repeat("c", 64)
			case "different_scope":
				secondRequest.UserIDs = []int64{201}
			case "interleaved_sweeps":
				oldMS, newMS = base+400000, base+500000
			case "sweep_fence_regression":
				oldFence, newFence = 101, 101
			}
			second := fundingMonthObservation(t, db, secondRequest, left, right, nil, nil, oldMS, newMS, oldFence, newFence)
			switch name {
			case "duplicate_object":
				second = first
			case "zero_observation":
				second = FundingWindowObservation{}
			case "serialized_report":
				raw, err := json.Marshal(second.Comparison())
				if err != nil {
					t.Fatal(err)
				}
				second = FundingWindowObservation{}
				if err := json.Unmarshal(raw, &second); err == nil {
					t.Fatal("serialized report recreated a trusted observation")
				}
			}
			if got, err := reviewFundingMonth(context.Background(), request, []FundingWindowObservation{first, second}, fundingMonthTestNow); err == nil || !reflect.DeepEqual(got, FundingMonthReview{}) {
				t.Fatal("invalid month composition returned partial success", err, got)
			}
		})
	}
}

func TestFundingMonthIndependentAccountsAndStableVersionRecovery(t *testing.T) {
	db, _ := fundingTestStore(t)
	request, from, to := fundingMonthFixture(t, "2026-09", []int64{201, 202})
	base := (to + 86400) * 1000
	before := []FundingLog{fundingMonthRow(1, 201, "10 点额度")}
	after := []FundingLog{fundingMonthRow(1, 201, "11 点额度")}
	changed := fundingMonthObservation(t, db, request, from, from+3600, before, after, base, base+300000, 100, 100)
	unknownRows := []FundingLog{fundingMonthRow(2, 202, "unknown")}
	unknown := fundingMonthObservation(t, db, request, from+3600, from+7200, unknownRows, unknownRows, base+20, base+300020, 100, 100)
	separateRows := []FundingLog{{ID: 3, UserID: 201, Type: 4, Content: "邀请用户赠送 1 点额度"}}
	separate := fundingMonthObservation(t, db, request, from+7200, from+10800, separateRows, separateRows, base+40, base+300040, 100, 100)
	got, err := reviewFundingMonth(context.Background(), request, []FundingWindowObservation{changed, unknown, separate}, fundingMonthTestNow)
	if err != nil || got.Accounts[0].SelectedSeconds != 3600 || got.Accounts[1].SelectedSeconds != 7200 || !reflect.DeepEqual(got.Accounts[0].Windows[0].ChangedLogIDs, []int64{1}) || !reflect.DeepEqual(got.Accounts[1].Windows[1].ReviewLogIDs, []int64{2}) || !reflect.DeepEqual(got.Accounts[0].Windows[2].SeparateLogIDs, []int64{3}) {
		t.Fatal("independent accounts contaminated or unknown money accepted", err, got)
	}
	// A new stable pair may replace, never be added to, the changed pair.
	stable := fundingMonthObservation(t, db, request, from, from+3600, after, after, base+600000, base+900000, 100, 100)
	got, err = reviewFundingMonth(context.Background(), request, []FundingWindowObservation{stable}, fundingMonthTestNow)
	if err != nil || got.Accounts[0].SelectedSeconds != 3600 || !got.Accounts[0].Windows[0].Selected || got.Windows[got.Accounts[0].Windows[0].WindowIndex].NewerScanID != stable.Comparison().NewerScanID {
		t.Fatal("corrected stable version cannot be selected", err, got)
	}
	if changed.Comparison().ObservationsMatch {
		t.Fatal("new observation overwrote earlier mismatch")
	}
}

func TestFundingMonthCrossWindowDuplicateIdentityAndTargetChanges(t *testing.T) {
	for _, name := range []string{"same_account", "target_changed", "unrelated_account", "unknown_scope"} {
		t.Run(name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			request, from, to := fundingMonthFixture(t, "2026-09", []int64{201, 202})
			base := (to + 86400) * 1000
			firstRow, secondRow := fundingMonthRow(7, 201, "1 点额度"), fundingMonthRow(7, 201, "1 点额度")
			switch name {
			case "target_changed":
				secondRow = fundingMonthRow(7, 202, "1 点额度")
			case "unrelated_account":
				firstRow, secondRow = fundingMonthRow(7, 999, "1 点额度"), fundingMonthRow(7, 999, "1 点额度")
			case "unknown_scope":
				secondRow.Other = fundingTestAudit("future.multi_account", `{"target_user_id":202}`)
			}
			first := fundingMonthObservation(t, db, request, from, from+3600, []FundingLog{firstRow}, []FundingLog{firstRow}, base, base+300000, 100, 100)
			second := fundingMonthObservation(t, db, request, from+3600, from+7200, []FundingLog{secondRow}, []FundingLog{secondRow}, base+20, base+300020, 100, 100)
			got, err := reviewFundingMonth(context.Background(), request, []FundingWindowObservation{first, second}, fundingMonthTestNow)
			if err != nil || len(got.Conflicts) != 1 || got.Conflicts[0].LogID != 7 || len(got.Conflicts[0].Windows) != 2 {
				t.Fatal("cross-window duplicate source identity missed", err, got)
			}
			wantA, wantB := int64(0), int64(7200)
			if name == "target_changed" || name == "unknown_scope" {
				wantB = 0
			} else if name == "unrelated_account" {
				wantA = 7200
			}
			if got.Accounts[0].SelectedSeconds != wantA || got.Accounts[1].SelectedSeconds != wantB || got.Conflicts[0].ScopeUncertain != (name == "unknown_scope") {
				t.Fatal("cross-window target isolation incorrect", got)
			}
			if name == "unknown_scope" && (!reflect.DeepEqual(got.Accounts[1].Windows[0].ConflictingLogIDs, []int64{7}) || len(got.Accounts[1].Windows[0].ChangedLogIDs) != 0) {
				t.Fatal("unknown identity must propagate uncertainty without inventing a changed target")
			}
		})
	}
}

func TestFundingWindowReportCannotMutateMonthEvidence(t *testing.T) {
	db, _ := fundingTestStore(t)
	request, from, to := fundingMonthFixture(t, "2026-09", []int64{201, 202})
	base := (to + 86400) * 1000
	before := []FundingLog{fundingMonthRow(1, 201, "1 点额度"), fundingMonthRow(2, 202, "bad amount"), {ID: 3, UserID: 201, Type: 4, Content: "邀请用户赠送 1 点额度"}, fundingMonthRow(4, 201, "1 点额度")}
	after := append([]FundingLog(nil), before...)
	after[0] = fundingMonthRow(1, 202, "1 点额度")
	after[3].Other = "malformed-json"
	observation := fundingMonthObservation(t, db, request, from, from+3600, before, after, base, base+300000, 100, 100)
	original, err := reviewFundingMonth(context.Background(), request, []FundingWindowObservation{observation}, fundingMonthTestNow)
	if err != nil {
		t.Fatal(err)
	}
	report := observation.Comparison()
	report.ObservationsMatch = true
	report.Changes[0].AffectedUserIDs[0] = 999
	report.Accounts[0].UserID = 999
	report.Accounts[0].ChangedLogIDs[0] = 999
	report.Accounts[0].SeparateLedgerLogIDs[0] = 999
	report.Accounts[1].ReviewLogIDs[0] = 999
	report.UnscopedChangedLogIDs[0] = 999
	report.UnscopedReviewLogIDs[0] = 999
	got, err := reviewFundingMonth(context.Background(), request, []FundingWindowObservation{observation}, fundingMonthTestNow)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatal("editing report altered private observation", err)
	}
	got.Accounts[0].Windows[0].ChangedLogIDs[0] = 999
	got.Accounts[0].Windows[0].UnscopedLogIDs[0] = 999
	got.Accounts[1].Windows[0].ReviewLogIDs[0] = 999
	gotAgain, err := reviewFundingMonth(context.Background(), request, []FundingWindowObservation{observation}, fundingMonthTestNow)
	if err != nil || !reflect.DeepEqual(gotAgain, original) {
		t.Fatal("editing month result changed next assembly", err)
	}
	if !reflect.DeepEqual((FundingWindowObservation{}).Comparison(), FundingScanComparison{}) {
		t.Fatal("zero observation fabricated a report")
	}
	if _, err := json.Marshal(observation); err == nil {
		t.Fatal("private observation serialized as a reusable authorization")
	}
	copyObservation := observation
	if err := json.Unmarshal([]byte(`{}`), &copyObservation); err == nil || copyObservation.data != nil || observation.data == nil {
		t.Fatal("failed restore did not invalidate only the attempted destination")
	}
}

func TestFundingMonthCancelledBusyAndLimits(t *testing.T) {
	request, _, _ := fundingMonthFixture(t, "2026-09", []int64{201})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reviewFundingMonth(ctx, request, nil, fundingMonthTestNow); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
	fundingMonthSlots <- struct{}{}
	_, err := reviewFundingMonth(context.Background(), request, nil, fundingMonthTestNow)
	<-fundingMonthSlots
	if !errors.Is(err, ErrFundingMonthBusy) {
		t.Fatal("busy assembly queued unbounded work", err)
	}
	if got, err := reviewFundingMonth(context.Background(), request, make([]FundingWindowObservation, fundingMonthWindowLimit+1), fundingMonthTestNow); err == nil || !reflect.DeepEqual(got, FundingMonthReview{}) {
		t.Fatal("window limit not enforced", err)
	}
	request.UserIDs = make([]int64, WalletBatchLimit)
	for i := range request.UserIDs {
		request.UserIDs[i] = int64(i + 1)
	}
	if got, err := reviewFundingMonth(context.Background(), request, make([]FundingWindowObservation, fundingMonthAccountWindowsLimit/WalletBatchLimit+1), fundingMonthTestNow); err == nil || !reflect.DeepEqual(got, FundingMonthReview{}) {
		t.Fatal("account/window output budget not enforced", err)
	}
	if _, err := reviewFundingMonth(context.Background(), request, nil, fundingMonthTestNow); err != nil {
		t.Fatal("failure leaked assembly slot", err)
	}
}
