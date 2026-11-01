package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A checksum proves that a stored payload is unchanged, not that an older
// producer supplied a complete statement. Exercise the customer read path too.
func TestMonthlyOutputRejectsIncompleteBalancesAndTotals(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	for name, change := range map[string]func(*monthlyStatement){
		"opening absent":      func(p *monthlyStatement) { p.MonthOpeningBalance = nil },
		"opening empty":       func(p *monthlyStatement) { *p.MonthOpeningBalance = "" },
		"opening unconfirmed": func(p *monthlyStatement) { *p.MonthOpeningBalance = "未确认" },
		"opening whitespace":  func(p *monthlyStatement) { *p.MonthOpeningBalance = " 0.000000" },
		"opening fractional quota": func(p *monthlyStatement) {
			*p.MonthOpeningBalance = "0.000001"
		},
		"closing absent":     func(p *monthlyStatement) { p.MonthEndBalance = nil },
		"closing invalid":    func(p *monthlyStatement) { *p.MonthEndBalance = "NaN" },
		"one quota mismatch": func(p *monthlyStatement) { *p.MonthOpeningBalance = "0.000002" },
		"net mismatch":       func(p *monthlyStatement) { p.Consumption.Net = "499.000000" },
		"negative debit":     func(p *monthlyStatement) { p.Consumption.Amount = "-500.000000" },
		"unknown credit":     func(p *monthlyStatement) { p.Credits.Unknown = 1 },
		"credit unavailable": func(p *monthlyStatement) { p.Credits.Available = false },
		"credit total mismatch": func(p *monthlyStatement) {
			p.Credits.KnownTotal = "2200.000002"
		},
		"credit row missing":      func(p *monthlyStatement) { p.Credits.Rows = p.Credits.Rows[1:] },
		"credit count mismatch":   func(p *monthlyStatement) { *p.Credits.RechargeCount++ },
		"credit row unknown":      func(p *monthlyStatement) { p.Credits.Rows[0].Amount = "" },
		"credit row malformed":    func(p *monthlyStatement) { p.Credits.Rows[0].Kind = "unknown" },
		"credit row unattributed": func(p *monthlyStatement) { p.Credits.Rows[0].UserID = 0 },
		"request count invalid":   func(p *monthlyStatement) { p.Consumption.Requests = -1 },
		"balance calculation overflow": func(p *monthlyStatement) {
			*p.MonthOpeningBalance = monthlyQuotaMoney(9223372036854775807)
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, _, err := buildPublishedMonthlyBill(monthlyEvidenceFixture(1, start.Format("2006-01"), now), now)
			if err != nil {
				t.Fatal(err)
			}
			change(&p)
			if monthlySummary(p).Downloadable {
				t.Error("incomplete statement advertised as downloadable")
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			record := PortalMonthlyBill{Month: p.Month, Payload: string(raw), PayloadHash: monthlyDigest(raw)}
			if _, err := readPublishedMonthlyBill(record); err == nil {
				t.Error("incomplete stored statement accepted by customer reader")
			}
			if _, err := monthlyStatementXLSX(p); err == nil {
				t.Error("incomplete statement exported")
			}
		})
	}
}

func TestMonthlyOutputAcceptsExplicitZeroAndSmallNegativeBalances(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	for _, balance := range []int64{0, -1, -416, 1, 500000} {
		p, _, err := buildPublishedMonthlyBill(monthlyFlatEvidence(1, start.Format("2006-01"), balance, now), now)
		if err != nil {
			t.Fatal(err)
		}
		if !monthlySummary(p).Downloadable || *p.MonthOpeningBalance != monthlyQuotaMoney(balance) {
			t.Fatalf("valid explicit balance rejected or rounded: %d", balance)
		}
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readPublishedMonthlyBill(PortalMonthlyBill{Month: p.Month, Payload: string(raw), PayloadHash: monthlyDigest(raw)}); err != nil {
			t.Fatal(err)
		}
		if _, err := monthlyStatementXLSX(p); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMonthlyCustomerRoutesRejectMissingOpeningBalance(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	t.Cleanup(f.m.Close)
	start, _, _ := monthlyWindow(f.e.Month, f.now)
	older := monthlyFlatEvidence(f.g.ID, start.AddDate(0, -1, 0).Format("2006-01"), 0, f.now)
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, monthlyTestRaw(t, older), true, f.now); err != nil {
		t.Fatal(err)
	}
	var p monthlyStatement
	if err := json.Unmarshal([]byte(f.old.Payload), &p); err != nil {
		t.Fatal(err)
	}
	p.MonthOpeningBalance = nil
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic local DB only. Keep a valid hash to test semantic validation,
	// rather than merely re-testing checksum damage detection.
	if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.g.ID, f.e.Month).Updates(map[string]any{"payload": string(raw), "payload_hash": monthlyDigest(raw)}).Error; err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/api/monthly-statement?month=" + f.e.Month,
		"/api/monthly-statement/download?month=" + f.e.Month + "&publication=" + f.version,
	} {
		w := portalDo(f.r, http.MethodGet, path, "", f.cookie)
		if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "1700.000000") {
			t.Fatalf("incomplete customer bill leaked: %d %s", w.Code, w.Body.String())
		}
	}
	w := portalDo(f.r, http.MethodGet, "/api/monthly-statements?month="+f.e.Month, "", f.cookie)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"rows":[]`) || !strings.Contains(w.Body.String(), `"unavailable_months":["`+f.e.Month+`"]`) {
		t.Fatalf("incomplete month advertised as complete: %d %s", w.Code, w.Body.String())
	}
	if w := portalDo(f.r, http.MethodGet, "/api/monthly-statement?month="+older.Month, "", f.cookie); w.Code != http.StatusOK {
		t.Fatal("valid neighboring month blocked", w.Code)
	}
	// An incomplete legacy record must remain explicitly withdrawable. No
	// automatic edits or replacement: retain the exact old payload in the audit.
	version, err := MonthlyBillPublicationVersion(context.Background(), f.m.storeDB, f.g.ID, f.e.Month, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if err := WithdrawMonthlyBill(context.Background(), f.m.storeDB, f.g.ID, f.e.Month, version, "local-reviewer", "replace incomplete synthetic statement", true, f.now); err != nil {
		t.Fatal(err)
	}
	var audit PortalMonthlyBillWithdrawal
	if err := f.m.storeDB.First(&audit, "group_id = ? AND month = ?", f.g.ID, f.e.Month).Error; err != nil {
		t.Fatal(err)
	}
	var archived PortalMonthlyBill
	if err := json.Unmarshal([]byte(audit.Record), &archived); err != nil || archived.Payload != string(raw) {
		t.Fatal("withdrawal did not retain the incomplete original", err)
	}
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, f.raw, true, f.now); err != nil {
		t.Fatal(err)
	}
	if w := portalDo(f.r, http.MethodGet, "/api/monthly-statement?month="+f.e.Month, "", f.cookie); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"month_opening_balance":"0.000000"`) {
		t.Fatalf("verified replacement unreadable: %d %s", w.Code, w.Body.String())
	}
}

func TestMonthlyOutputAcceptsRefundsFromEarlierConsumption(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyFlatEvidence(1, start.Format("2006-01"), 0, now)
	e.Users[0].RefundQuota = 500000
	e.Users[0].ClosingQuota = 500000
	e.Users[0].ReverseClosingQuota = 500000
	e.Users[0].Accounting = monthlyAccountingFixture(0, 500000)
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil || !monthlySummary(p).Downloadable || p.Consumption.Net != "-1.000000" {
		t.Fatal("legitimate negative net consumption rejected", err)
	}
}
