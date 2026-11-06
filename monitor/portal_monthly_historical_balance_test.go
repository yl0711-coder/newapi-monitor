package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func historicalBalanceFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	i, c := monthlyReconstructionFixture()
	i.Sources = append(i.Sources, MonthlyPreparationSource{ID: "decision", Path: "/retained/decision.json", SHA256: strings.Repeat("d", 64)})
	a := &i.Accounts[0]
	a.Months = []string{"2026-06", "2026-07"}
	c.Groups[0].Months = append([]string(nil), a.Months...)
	a.Snapshots[1].BalanceQuota++
	a.Snapshots[1].UsedQuota--
	r := &c.Accounts[0].Reconstructions[0]
	r.SnapshotSource = a.Snapshots[1].Source
	r.BalanceQuota = preparationInt(a.Snapshots[1].BalanceQuota)
	r.UsedQuota = preparationInt(a.Snapshots[1].UsedQuota)
	r.HistoricalBalance = &MonthlyHistoricalBalanceRecognition{Version: 1, UserID: a.UserID, GroupID: a.GroupID, RecognitionMonth: "2026-06", Quota: 1,
		DecisionAt: preparationAt("2026-10-19"), DecisionSource: "decision#owner-accepted-unrecoverable-small-historical-difference", DecisionSHA256: strings.Repeat("d", 64)}
	july := *r
	july.Month = "2026-07"
	july.MonthConsumeQuota, july.MonthRefundQuota = preparationInt(4000000), preparationInt(250000)
	july.BridgeConsumeQuota, july.BridgeRefundQuota = preparationInt(0), preparationInt(0)
	c.Accounts[0].Reconstructions = append(c.Accounts[0].Reconstructions, july)
	return i, c
}

func TestMonthlyHistoricalBalanceRecognitionPreservesLogsAndCarriesOnce(t *testing.T) {
	i, c := historicalBalanceFixture()
	before := monthlyReviewJSON(t, i)
	b := monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 2 || b.Blocked != 0 {
		t.Fatal(b.Entries)
	}
	first, next := b.Entries[0].Candidate.Users[0], b.Entries[1].Candidate.Users[0]
	if first.ConsumeQuota != 3000000 || first.RefundQuota != 0 || first.ClosingQuota != 2500001 || first.BalanceCorrection == nil || first.BalanceCorrection.Quota != 1 ||
		next.ConsumeQuota != 4000000 || next.RefundQuota != 250000 || next.BalanceCorrection != nil || next.OpeningQuota != first.ClosingQuota || next.ClosingQuota != -1749999 {
		t.Fatal("changed logs, repeated correction or wrong balance", first, next)
	}
	if !bytes.Equal(before, monthlyReviewJSON(t, i)) || !reflect.DeepEqual(b, monthlyBundleRun(t, i, c)) {
		t.Fatal("modified retained input or nondeterministic reconstruction")
	}
	now := time.Unix(preparationAt("2026-10-20"), 0)
	for _, entry := range b.Entries {
		raw, err := PrepareAutomaticMonthlyBill(*entry.Candidate, now.Unix(), now)
		if err != nil {
			t.Fatal(err)
		}
		e, err := decodeMonthlyBillEvidence(raw)
		if err != nil || e.Version != 4 || e.ReviewedBy != "policy:"+monthlyHistoricalBalancePolicy {
			t.Fatal("lost historical recognition semantics", e, err)
		}
		p, _, err := buildPublishedMonthlyBill(e, now)
		if err != nil || !monthlyStatementAmountsComplete(p) || p.Consumption.Amount != monthlyQuotaMoney(entry.Candidate.Users[0].ConsumeQuota) {
			t.Fatal("customer money disagrees with original logs", p, err)
		}
		customer := string(monthlyReviewJSON(t, p))
		if strings.Contains(customer, "decision#") || strings.Contains(customer, "recognition_month") || strings.Contains(customer, "decision_sha256") {
			t.Fatal("private decision leaked to customer")
		}
		book, err := monthlyStatementXLSX(p)
		if err != nil {
			t.Fatal(err)
		}
		z, err := zip.NewReader(bytes.NewReader(book), int64(len(book)))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range z.File {
			if file.Name != "xl/worksheets/sheet1.xml" && file.Name != "xl/worksheets/sheet2.xml" {
				continue
			}
			in, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(in)
			in.Close()
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("decision#")) || !bytes.Contains(data, []byte("<v>"+monthlyAuditCents(p.Consumption.Amount)+"</v>")) {
				t.Fatal("workbook consumption differs from customer page or exposes internal evidence")
			}
			if entry.Month == "2026-06" && (!bytes.Contains(data, []byte("余额校正（元）")) || !bytes.Contains(data, []byte("<v>0.00</v>"))) {
				t.Fatal("historical correction missing from workbook")
			}
		}
	}
}

func TestMonthlyHistoricalBalanceRecognitionFailsClosed(t *testing.T) {
	for name, change := range map[string]func(*MonthlyPreparationInput, *MonthlyBundleContext){
		"no decision": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance = nil
		},
		"wrong user": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.UserID++
		},
		"wrong group": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.GroupID++
		},
		"wrong amount": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.Quota++
		},
		"negative": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.Quota = -1
		},
		"absolute cap": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.Quota = monthlyCorrectionQuotaLimit + 1
		},
		"ratio cap": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.Quota = 301
		},
		"unbound decision": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.DecisionSource = "absent#decision"
		},
		"changed decision file": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.DecisionSHA256 = strings.Repeat("f", 64)
		},
		"future decision": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.DecisionAt = preparationAt("2026-10-21")
		},
		"before closed month": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.DecisionAt = preparationAt("2026-06-02")
		},
		"recognition after bill": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].HistoricalBalance.RecognitionMonth = "2026-07"
		},
		"missing funding": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].MovementCoverage = nil },
		"missing logs": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].Spans = i.Accounts[0].Spans[1:]
		},
		"counter conflict": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].UsedQuota = preparationInt(6750000)
		},
		"invented snapshot": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Snapshots[1].BalanceQuota++ },
		"contrary reviewed cutoff": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].Snapshots[0].TimeBasis, i.Accounts[0].Snapshots[0].CutoffSource = monthlySourceLogCutoff, "logs#reviewed"
		},
	} {
		t.Run(name, func(t *testing.T) {
			i, c := historicalBalanceFixture()
			c.Groups[0].Months, i.Accounts[0].Months = []string{"2026-06"}, []string{"2026-06"}
			c.Accounts[0].Reconstructions = c.Accounts[0].Reconstructions[:1]
			change(&i, &c)
			raw, meta := monthlyBundleRaw(t, i, c)
			b, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && (b.ReadyForReview != 0 || b.Entries[0].Candidate != nil) {
				t.Fatal("historical decision hid incomplete/conflicting data", b)
			}
		})
	}
}

func TestMonthlyHistoricalBalancePublicationIsImmutableAndDoesNotRebook(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	i, c := historicalBalanceFixture()
	g := CustomerGroup{ID: i.Accounts[0].GroupID, Name: "synthetic-history", PortalEmail: "history@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: i.Accounts[0].UserID, GroupID: g.ID, Username: i.Accounts[0].Username}).Error; err != nil {
		t.Fatal(err)
	}
	b, now := monthlyBundleRun(t, i, c), time.Unix(preparationAt("2026-10-20"), 0)
	ctx := context.Background()
	for _, index := range []int{1, 0, 1, 0} { // reverse-order generation and retries
		if _, err := IssueMonthlyBillAutomatically(ctx, m.storeDB, *b.Entries[index].Candidate, now.Unix(), now); err != nil {
			t.Fatal("repeated/reverse-order bill generation failed", err)
		}
	}
	var before []PortalMonthlyBill
	if err := m.storeDB.Order("month").Find(&before).Error; err != nil || len(before) != 2 {
		t.Fatal(err)
	}
	// A hole in generated history must not permit a second recognition month.
	bad := *b.Entries[1].Candidate
	bad.Month = "2026-09"
	bad.Users = append([]MonthlyBillUserEvidence(nil), bad.Users...)
	u := &bad.Users[0]
	u.HistoricalBalance = &MonthlyBillHistoricalBalance{Recognition: u.HistoricalBalance.Recognition, LogNetQuota: u.HistoricalBalance.LogNetQuota}
	u.HistoricalBalance.Recognition.RecognitionMonth = bad.Month
	u.Through = preparationAt("2026-10-01")
	u.Events = []MonthlyBillCreditEvidence{}
	u.ExpectedCreditEvents = 0
	u.ClosingQuota = u.OpeningQuota - u.ConsumeQuota + u.RefundQuota + 1
	u.ReverseClosingQuota = u.ClosingQuota
	u.HistoricalBalance.LogNetQuota = u.ConsumeQuota - u.RefundQuota
	u.HistoricalBalance.ScopeSHA256 = MonthlyCorrectionScopeSHA256(*u, bad.Month)
	u.BalanceCorrection = &MonthlyBillBalanceCorrection{Version: 1, Quota: 1, Observations: []MonthlyBillCorrectionObservation{}}
	if _, err := IssueMonthlyBillAutomatically(ctx, m.storeDB, bad, now.Unix(), now); err == nil {
		t.Fatal("same historical difference was booked again with an intervening month absent")
	}
	var after []PortalMonthlyBill
	_ = m.storeDB.Order("month").Find(&after).Error
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed or repeated generation changed saved bills")
	}
	// Changing organization must not give the same wallet a fresh correction.
	other := CustomerGroup{Name: "synthetic-moved-owner", PortalEmail: "moved@example.invalid"}
	if err := m.storeDB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", bad.Users[0].UserID).Update("group_id", other.ID).Error; err != nil {
		t.Fatal(err)
	}
	bad.GroupID = other.ID
	bad.Users[0].HistoricalBalance.Recognition.GroupID = other.ID
	if _, err := IssueMonthlyBillAutomatically(ctx, m.storeDB, bad, now.Unix(), now); err == nil {
		t.Fatal("moving organization allowed the same wallet difference to be rebooked")
	}
}

func TestMonthlyHistoricalBalanceCannotChangeConsumptionOrUseAmbiguousJSON(t *testing.T) {
	i, c := historicalBalanceFixture()
	b, now := monthlyBundleRun(t, i, c), time.Unix(preparationAt("2026-10-20"), 0)
	raw, err := PrepareAutomaticMonthlyBill(*b.Entries[0].Candidate, now.Unix(), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"recognition", "log_net_quota", "scope_sha256", "version", "user_id", "group_id", "recognition_month", "quota", "decision_at", "decision_source", "decision_sha256"} {
		for _, mode := range []string{"missing", "null", "case-alias", "second-case-alias"} {
			var root map[string]any
			_ = json.Unmarshal(raw, &root)
			obj := root["users"].([]any)[0].(map[string]any)["historical_balance"].(map[string]any)
			if field != "recognition" && field != "log_net_quota" && field != "scope_sha256" {
				obj = obj["recognition"].(map[string]any)
			}
			v := obj[field]
			if mode != "second-case-alias" {
				delete(obj, field)
			}
			if mode == "null" {
				obj[field] = nil
			} else if mode != "missing" {
				obj[strings.ToUpper(field)] = v
			}
			bad, _ := json.Marshal(root)
			if _, err := decodeMonthlyBillEvidence(bad); err == nil {
				t.Fatal("ambiguous historical decision accepted", field, mode)
			}
		}
	}
	for _, change := range []func(*MonthlyBillCandidate){
		func(c *MonthlyBillCandidate) { c.Users[0].ConsumeQuota-- },
		func(c *MonthlyBillCandidate) { c.Users[0].OpeningQuota++ },
		func(c *MonthlyBillCandidate) { c.Users[0].HistoricalBalance.ScopeSHA256 = strings.Repeat("f", 64) },
		func(c *MonthlyBillCandidate) { c.GroupID++ },
		func(c *MonthlyBillCandidate) { c.Users[0].BalanceCorrection = nil },
	} {
		copy := MonthlyBillCandidate{}
		_ = json.Unmarshal(monthlyReviewJSON(t, b.Entries[0].Candidate), &copy)
		change(&copy)
		if _, err := PrepareAutomaticMonthlyBill(copy, now.Unix(), now); err == nil {
			t.Fatal("historical treatment changed original charges or lost scope")
		}
	}
}
