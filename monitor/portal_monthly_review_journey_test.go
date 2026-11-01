package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// This journey starts at the NEW review converter, not a hand-written import
// payload. All writes use a new temporary SQLite and synthetic source amounts.
func TestMonthlyReviewWithdrawCorrectReissueJourney(t *testing.T) {
	f := monthlyReviewFixture(t)
	convert := func(f monthlyReviewInputs) []byte {
		t.Helper()
		raw, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, f.review, f.now)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled, m.cfg.LocalSnapshotOnly, m.cfg.AlertsDisabled = true, true, true
	r := gin.New()
	m.RegisterPortalRoutes(r)
	g := CustomerGroup{ID: 5, Name: "synthetic", PortalEmail: "review@example.invalid"}
	for _, group := range []CustomerGroup{g, {ID: 9, Name: "other synthetic", PortalEmail: "other@example.invalid"}} {
		if err := m.storeDB.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, user := range []TrackedUser{{UserID: 102, GroupID: 5, Username: "account-102"}, {UserID: 999, GroupID: 9, Username: "other-account"}} {
		if err := m.storeDB.Create(&user).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	importBill := func(raw []byte, apply bool) {
		t.Helper()
		if _, err := ImportMonthlyBill(ctx, m.storeDB, raw, apply, f.now); err != nil {
			t.Fatal(err)
		}
	}
	get := func(gid int64, month string) PortalMonthlyBill {
		t.Helper()
		var record PortalMonthlyBill
		if err := m.storeDB.First(&record, "group_id = ? AND month = ?", gid, month).Error; err != nil {
			t.Fatal(err)
		}
		return record
	}
	raw := convert(f)
	importBill(raw, false)
	monthlyRecoveryCounts(t, m, 0, 0)
	importBill(raw, true)
	old := get(5, "2026-06")
	importBill(raw, true)
	if !reflect.DeepEqual(old, get(5, "2026-06")) {
		t.Fatal("duplicate import modified publication")
	}
	var bundle MonthlyPreparationBundle
	if err := json.Unmarshal(f.bundle, &bundle); err != nil {
		t.Fatal(err)
	}
	july := f
	july.review = monthlyReviewJSON(t, monthlyReviewApproval(f.bundle, bundle.Entries[1], f.now))
	importBill(convert(july), true)
	other := monthlyFlatEvidence(9, "2026-06", 500000, f.now)
	other.Users[0].UserID = 999
	importBill(monthlyReviewJSON(t, other), true)
	untouchedJuly, untouchedOther := get(5, "2026-07"), get(9, "2026-06")
	monthlyRecoveryCounts(t, m, 3, 0)
	version, err := MonthlyBillPublicationVersion(ctx, m.storeDB, 5, "2026-06", f.now)
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	download := "/api/monthly-statement/download?month=2026-06&publication="
	if w := portalDo(r, "GET", download+version, "", cookie); w.Code != 200 {
		t.Fatal("original bill unavailable", w.Code)
	}

	// Synthetic correction: both gross consumption and refund increase by 0.20,
	// keeping net debit and adjacent month balances fixed. No real file is edited.
	input := preparationFixture()
	input.Accounts[0].Spans[0].ConsumeQuota += 100000
	input.Accounts[0].Spans[0].RefundQuota += 100000
	corrected := f
	corrected.input, corrected.context = monthlyBundleRaw(t, input, monthlyBundleFixture(input))
	b, err := PrepareMonthlyCandidateBundle(corrected.input, corrected.context, f.now)
	if err != nil {
		t.Fatal(err)
	}
	b.SourceIntegrity, b.Reconciliation.SourceIntegrity = bundle.SourceIntegrity, bundle.SourceIntegrity
	corrected.bundle = monthlyReviewJSON(t, b)
	if _, err := ReviewMonthlyCandidate(corrected.input, corrected.context, corrected.bundle, f.review, f.now); err == nil {
		t.Fatal("old approval authorized corrected sources")
	}
	corrected.review = monthlyReviewJSON(t, monthlyReviewApproval(corrected.bundle, b.Entries[0], f.now))
	newRaw := convert(corrected)
	for _, apply := range []bool{false, true} {
		if _, err := ImportMonthlyBill(ctx, m.storeDB, newRaw, apply, f.now); err == nil {
			t.Fatal("correction silently overwrote issued bill")
		}
	}
	if !reflect.DeepEqual(old, get(5, "2026-06")) {
		t.Fatal("rejected correction modified original")
	}
	withdraw := func(v string, apply bool) error {
		return WithdrawMonthlyBill(ctx, m.storeDB, 5, "2026-06", v, "synthetic-local-review-only", "synthetic gross/refund correction", apply, f.now)
	}
	if err := withdraw(strings.Repeat("0", 64), true); err == nil {
		t.Fatal("wrong publication withdrawn")
	}
	if err := withdraw(version, false); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, m, 3, 0)
	if err := withdraw(version, true); err != nil {
		t.Fatal(err)
	}
	monthlyRecoveryCounts(t, m, 2, 1)
	var audit PortalMonthlyBillWithdrawal
	if err := m.storeDB.First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	var archived PortalMonthlyBill
	if err := json.Unmarshal([]byte(audit.Record), &archived); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, archived) || audit.RecordHash != monthlyDigest([]byte(audit.Record)) || audit.Publication != version {
		t.Fatal("withdrawal lost original record or version")
	}
	if w := portalDo(r, "GET", download+version, "", cookie); w.Code != 404 {
		t.Fatal("withdrawn workbook is still available", w.Code)
	}
	importBill(newRaw, false)
	monthlyRecoveryCounts(t, m, 2, 1)
	importBill(newRaw, true)
	importBill(newRaw, true)
	monthlyRecoveryCounts(t, m, 3, 1)
	if err := withdraw(version, true); !errors.Is(err, errMonthlyPublicationChanged) {
		t.Fatal("old command can withdraw reissued bill", err)
	}
	if !reflect.DeepEqual(untouchedJuly, get(5, "2026-07")) || !reflect.DeepEqual(untouchedOther, get(9, "2026-06")) {
		t.Fatal("another month or customer changed")
	}
	w := portalDo(r, "GET", "/api/monthly-statement?month=2026-06", "", cookie)
	var reply struct {
		Data monthlyStatement `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	p := reply.Data
	if w.Code != 200 || p.Publication == version || p.Consumption.Amount != "6.200000" || p.Consumption.Refund != "0.200000" || p.Consumption.Net != "6.000000" {
		t.Fatal("reissued amounts or version wrong", w.Code)
	}
	for _, private := range []string{"synthetic-local-review-only", "source_reference", "bundle:", "review:", "synthetic gross/refund correction"} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatal("private review metadata leaked")
		}
	}
	if stale := portalDo(r, "GET", download+version, "", cookie); stale.Code != 409 {
		t.Fatal("stale viewed version downloaded corrected money", stale.Code)
	}
	xlsx := portalDo(r, "GET", download+p.Publication, "", cookie)
	expected, err := monthlyStatementXLSX(p)
	if err != nil || xlsx.Code != 200 || !bytes.Equal(xlsx.Body.Bytes(), expected) {
		t.Fatal("page and workbook disagree", err, xlsx.Code)
	}
	monthlyBatchCheckMemberSheet(t, xlsx.Body.Bytes(), []monthlyMemberStatement{{UserID: 102, Username: "account-102", Opening: "1.000000", CreditTotal: "10.000000", Adjustment: "0.000000", Closing: "5.000000", Consumption: monthlyConsumption{Requests: 20, Tokens: 200, Amount: "6.200000", Refund: "0.200000", Net: "6.000000"}}})
}
