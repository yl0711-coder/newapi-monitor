//go:build unix

package monitor

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestFinanceGiftScopeMonthlyInventoryReconcilesWithBackup(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 3, 4, 3001)
	before := giftLocalFileHash(t, backup)
	got, err := InspectFinanceGiftScopeBackup(context.Background(), backup, "v1")
	if err != nil {
		t.Fatal(err)
	}
	var unknown, monetary, hours, oversize, missing, declared int64
	for _, month := range got.Months {
		unknown += month.UnknownRows
		monetary += month.MonetaryRows
		hours += month.UserHours
		oversize += month.OversizeHours
		missing += month.MissingStates
		declared += month.DeclaredWholeHourRows
	}
	if unknown != got.UnknownRows || monetary != got.MonetaryRows || hours != got.UserHours || oversize != got.OversizeHours || missing != got.MissingStates || declared != 3008 {
		t.Fatalf("monthly inventory must reconcile with raw totals: %+v", got)
	}
	if got.Candidates.Rows != 7 || len(got.Candidates.Entries) != 2 || got.Candidates.StopReason != "whole_hour_exceeds_row_limit" {
		t.Fatalf("inventory expanded executable scope: %+v", got.Candidates)
	}
	if giftLocalFileHash(t, backup) != before {
		t.Fatal("inspection changed original backup")
	}
}

func TestFinanceGiftScopeMonthlyInventoryRawScopeAndMetadata(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 1)
	db, closeDB, err := giftLocalDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	// Inventory does not claim content-hash verification. Deliberately use
	// metadata-only fixtures to check unknown vs known rows, gaps and sizing.
	boundary := time.Date(2026, 8, 31, 16, 0, 0, 0, time.UTC).Unix()
	events := []FinanceGiftBoundaryEvent{
		{SourceEpoch: "inventory", SourceLogID: 1, UserID: 1, HourTs: boundary - 3600, Quota: 10},
		{SourceEpoch: "inventory", SourceLogID: 2, UserID: 1, HourTs: boundary - 3600, Quota: 0},
		{SourceEpoch: "inventory", SourceLogID: 3, UserID: 1, HourTs: boundary - 3600, Quota: 20, GroupKnown: true},
		{SourceEpoch: "inventory", SourceLogID: 4, UserID: 1, HourTs: boundary, Quota: 10},
		{SourceEpoch: "inventory", SourceLogID: 5, UserID: 2, HourTs: boundary, Quota: 10},
		{SourceEpoch: "inventory", SourceLogID: 6, UserID: 3, HourTs: boundary, Quota: 10},
	}
	states := []FinanceGiftBoundaryState{
		{SourceEpoch: "inventory", UserID: 1, HourTs: boundary - 3600, Rows: 3, Status: "complete"},
		{SourceEpoch: "inventory", UserID: 1, HourTs: boundary, Rows: 3001, Status: "complete"},
		{SourceEpoch: "inventory", UserID: 2, HourTs: boundary, Rows: 0, Status: "pending"},
		// User 3 deliberately has no state.
	}
	if err := db.Create(&events).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	want := []FinanceGiftScopeMonthGap{
		{Month: "2026-08", UnknownRows: 2, MonetaryRows: 1, UserHours: 1, FirstHourTs: boundary - 3600, LastHourTs: boundary - 3600, DeclaredWholeHourRows: 3},
		{Month: "2026-09", UnknownRows: 3, MonetaryRows: 3, UserHours: 3, FirstHourTs: boundary, LastHourTs: boundary, DeclaredWholeHourRows: 3001, MissingStates: 1, InvalidStates: 1, OversizeHours: 1},
	}
	got, err := inspectFinanceGiftScopeMonths(context.Background(), db, "inventory")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v: %v", got, want, err)
	}
	empty, err := inspectFinanceGiftScopeMonths(context.Background(), db, "absent")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("other epoch leaked into inventory: %+v %v", empty, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	partial, err := inspectFinanceGiftScopeMonths(ctx, db, "inventory")
	if err == nil || partial != nil {
		t.Fatalf("cancelled inventory exposed partial results: %+v %v", partial, err)
	}
}
