package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func saveRetirementForTest(t *testing.T, m *Monitor, domain string, retiring, expected bool) int {
	t.Helper()
	var reasons []string
	if retiring {
		reasons = []string{"unstable"}
	}
	code, _ := saveRetirementWithReasonsForTest(t, m, channelUpstreamRetirementInput{Domain: domain, Retiring: &retiring, ExpectedRetiring: &expected, Reasons: reasons})
	return code
}

func saveRetirementWithReasonsForTest(t *testing.T, m *Monitor, input channelUpstreamRetirementInput) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/channels/upstream/retirement", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("uname", "operator")
	m.saveChannelUpstreamRetirementHandler(c)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func TestChannelUpstreamRetirementToggleIsIndependentOfRoutingAndBilling(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 9, 18, 10, 0, 0, 0, cstLocation).Unix()
	channel := ChannelSnap{ID: 61, Name: "draining supplier", BaseDomain: "drain.example", Status: 1, Groups: "codex-0.7x", UpdatedAt: hour}
	if err := m.storeDB.Create(&channel).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: channel.ID, ModelName: "gpt-6", Grp: "codex-0.7x", Success: 2, Quota: 1000}).Error; err != nil {
		t.Fatal(err)
	}
	if got := saveRetirementForTest(t, m, " DRAIN.EXAMPLE ", true, false); got != http.StatusOK {
		t.Fatalf("mark retiring status=%d", got)
	}
	retirements, err := m.loadChannelUpstreamRetirements(context.Background())
	if err != nil || !retirements[channel.BaseDomain].Retiring || retirements[channel.BaseDomain].UpdatedBy != "operator" {
		t.Fatalf("retirement not stored: rows=%+v err=%v", retirements, err)
	}
	var unchanged ChannelSnap
	if err := m.storeDB.First(&unchanged, "id = ?", channel.ID).Error; err != nil || unchanged.Status != 1 || unchanged.Groups != channel.Groups {
		t.Fatalf("retirement unexpectedly changed routing: channel=%+v err=%v", unchanged, err)
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+7200)
	if err != nil || len(report.Domains) != 1 || !report.Domains[0].Retiring || report.Domains[0].Usage.Requests != 2 {
		t.Fatalf("retirement must be a visible label without changing usage: report=%+v err=%v", report, err)
	}
	if got := saveRetirementForTest(t, m, channel.BaseDomain, false, false); got != http.StatusConflict {
		t.Fatalf("stale toggle must be rejected, got=%d", got)
	}
	if got := saveRetirementForTest(t, m, channel.BaseDomain, false, true); got != http.StatusOK {
		t.Fatalf("cancel retiring status=%d", got)
	}
	retirements, err = m.loadChannelUpstreamRetirements(context.Background())
	if err != nil || retirements[channel.BaseDomain].Retiring {
		t.Fatalf("cancellation did not restore replenishment evaluation: rows=%+v err=%v", retirements, err)
	}
	if got := saveRetirementForTest(t, m, "unknown.example", true, false); got != http.StatusNotFound {
		t.Fatalf("unknown supplier must not receive a label, got=%d", got)
	}
}

func TestRetiringUpstreamSkipsOnlyReplenishmentAlert(t *testing.T) {
	runway := 0.5
	required := 10.0
	assessment := ChannelUpstreamBalanceAssessment{Status: "warning", EstimatedRunwayDays: &runway, RequiredBalanceUSD: &required}
	if !upstreamBalanceAlertEligible("drain.example", assessment, nil, nil) {
		t.Fatal("ordinary low-balance supplier should still alert")
	}
	if upstreamBalanceAlertEligible("drain.example", assessment, nil, map[string]ChannelUpstreamRetirement{"drain.example": {Retiring: true}}) {
		t.Fatal("retiring supplier should not trigger a recharge reminder")
	}
	if !upstreamBalanceAlertEligible("drain.example", assessment, nil, map[string]ChannelUpstreamRetirement{}) {
		t.Fatal("cancelling the label should restore the reminder")
	}
}

func TestRetiringUpstreamSuppressesLiveBalanceAlertUntilCancelled(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "retiring.example"
	account := seededUpstreamBalanceAccount(now, 10)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: account.Provider, Enabled: true, Status: upstreamStatusOK,
		BalanceUSD: *account.BalanceUSD, BalanceKnown: true, LastSuccessAt: now, UsageSyncEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 81, BaseDomain: domain, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if got := saveRetirementForTest(t, m, domain, true, false); got != http.StatusOK {
		t.Fatalf("mark retiring status=%d", got)
	}
	cfg := defaultAlertConfig()
	cfg.UpstreamBalanceAlertsEnabled = true
	m.evaluateUpstreamBalanceAlerts(cfg, now)
	var count int64
	query := m.storeDB.Model(&AlertLog{}).Where("kind LIKE ? AND target=?", "upstream_balance_low%", domain)
	if err := query.Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("retiring supplier emitted %d low-balance alerts: %v", count, err)
	}
	if got := saveRetirementForTest(t, m, domain, false, true); got != http.StatusOK {
		t.Fatalf("cancel retiring status=%d", got)
	}
	m.evaluateUpstreamBalanceAlerts(cfg, now+300)
	if err := query.Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("cancelling retirement should restore alert, count=%d err=%v", count, err)
	}
}

func TestChannelUpstreamRetirementReasonsAreValidatedStoredAndShown(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 9, 18, 10, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&ChannelSnap{ID: 71, Name: "supplier", BaseDomain: "reason.example", Status: 1, Groups: "g", UpdatedAt: hour}).Error; err != nil {
		t.Fatal(err)
	}
	post := func(retiring, expected bool, reasons []string, note string) (int, map[string]any) {
		r, e := retiring, expected
		return saveRetirementWithReasonsForTest(t, m, channelUpstreamRetirementInput{Domain: "reason.example", Retiring: &r, ExpectedRetiring: &e, Reasons: reasons, ReasonNote: note})
	}
	for name, tc := range map[string]struct {
		reasons []string
		note    string
	}{
		"no reason":           {nil, ""},
		"unknown reason":      {[]string{"degraded", "cheap"}, ""},
		"other without note":  {[]string{"other"}, "   "},
		"other note too long": {[]string{"other"}, strings.Repeat("长", retirementReasonNoteMaxRunes+1)},
	} {
		if code, _ := post(true, false, tc.reasons, tc.note); code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d, want 400", name, code)
		}
	}
	var count int64
	m.storeDB.Model(&ChannelUpstreamRetirement{}).Count(&count)
	if count != 0 {
		t.Fatalf("rejected requests stored %d rows", count)
	}

	code, out := post(true, false, []string{"other", "no_recharge", "token_inflation", "degraded", "degraded"}, "  对账 \n 不一致  ")
	if code != http.StatusOK {
		t.Fatalf("mark with reasons status=%d body=%v", code, out)
	}
	if got := fmt.Sprint(out["reasons"]); got != "[no_recharge degraded token_inflation other]" || out["reason_note"] != "对账 不一致" {
		t.Fatalf("response reasons=%v note=%v", out["reasons"], out["reason_note"])
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+7200)
	if err != nil || len(report.Domains) != 1 {
		t.Fatalf("report err=%v", err)
	}
	d := report.Domains[0]
	if !d.Retiring || strings.Join(d.RetiringReasons, ",") != "no_recharge,degraded,token_inflation,other" || d.RetiringReasonNote != "对账 不一致" {
		t.Fatalf("report reasons=%v note=%q", d.RetiringReasons, d.RetiringReasonNote)
	}

	// Editing the reasons of a marked upstream; the note is dropped without "other".
	if code, out := post(true, true, []string{"slow"}, "ignored"); code != http.StatusOK || out["reason_note"] != "" {
		t.Fatalf("edit reasons status=%d out=%v", code, out)
	}
	var row ChannelUpstreamRetirement
	if err := m.storeDB.First(&row, "domain = ?", "reason.example").Error; err != nil || row.Reasons != `["slow"]` || row.ReasonNote != "" || !row.Retiring {
		t.Fatalf("edited row=%+v err=%v", row, err)
	}

	// Unmarking clears the reasons; a stale expectation is still rejected.
	if code, _ := post(false, false, nil, ""); code != http.StatusConflict {
		t.Fatalf("stale unmark status=%d", code)
	}
	if code, _ := post(false, true, []string{"slow"}, "x"); code != http.StatusOK {
		t.Fatalf("unmark status=%d", code)
	}
	if err := m.storeDB.First(&row, "domain = ?", "reason.example").Error; err != nil || row.Retiring || row.Reasons != "" || row.ReasonNote != "" {
		t.Fatalf("unmarked row=%+v err=%v", row, err)
	}
}

func TestRetirementReasonListToleratesLegacyAndBadRows(t *testing.T) {
	for stored, want := range map[string]string{
		"":                                "",
		"not json":                        "",
		`["other","bogus","no_recharge"]`: "no_recharge,other",
		`["slow"]`:                        "slow",
	} {
		if got := strings.Join(retirementReasonList(stored), ","); got != want {
			t.Fatalf("retirementReasonList(%q)=%q want %q", stored, got, want)
		}
	}
}
