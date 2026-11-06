package monitor

// Opt-in real-data regression. Local reviewed files only; skipped by CI.
// No source queries and no customer files are checked into this repository.
import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMonthlyReviewedLocalEvidence(t *testing.T) {
	input, expected := os.Getenv("MONTHLY_LOCAL_EVIDENCE"), os.Getenv("MONTHLY_LOCAL_EXPECTED")
	if input == "" || expected == "" {
		t.Skip("explicit local reviewed evidence paths required")
	}
	raw, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	e, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(expected)
	if err != nil {
		t.Fatal(err)
	}
	var dataset struct {
		Account string                      `json:"account"`
		Months  map[string]monthlyStatement `json:"months"`
	}
	if err = json.Unmarshal(data, &dataset); err != nil {
		t.Fatal(err)
	}
	want := dataset.Months[e.Month]
	for i := range want.Credits.Rows {
		want.Credits.Rows[i].UserID = e.Users[0].UserID
	}
	got, _, err := buildPublishedMonthlyBill(e, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Credits.Rows, want.Credits.Rows) || !reflect.DeepEqual(got.Consumption, want.Consumption) || !reflect.DeepEqual(got.MonthEndBalance, want.MonthEndBalance) || got.Credits.KnownTotal != want.Credits.KnownTotal || *got.Credits.RechargeCount != *want.Credits.RechargeCount || *got.Credits.RechargeTotal != *want.Credits.RechargeTotal || *got.Credits.GiftTotal != *want.Credits.GiftTotal {
		t.Fatal("production builder differs from independently reviewed bill")
	}
	m, _, _ := newPortalTestMonitor(t)
	g := CustomerGroup{ID: e.GroupID, Name: dataset.Account}
	if err = m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, u := range e.Users {
		if err = m.storeDB.Create(&TrackedUser{UserID: u.UserID, GroupID: g.ID, Username: dataset.Account}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err = ImportMonthlyBill(context.Background(), m.storeDB, raw, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	var rec PortalMonthlyBill
	if err = m.storeDB.First(&rec, "group_id = ? AND month = ?", e.GroupID, e.Month).Error; err != nil {
		t.Fatal(err)
	}
	got, err = readPublishedMonthlyBill(rec)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := monthlyStatementXLSX(got)
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("MONTHLY_LOCAL_XLSX"); path != "" {
		if err = os.WriteFile(path, payload, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("reviewed month matched: credits=%d, recharge=%s, consumption=%s, closing=%s", len(got.Credits.Rows), *got.Credits.RechargeTotal, got.Consumption.Amount, *got.MonthEndBalance)
}
