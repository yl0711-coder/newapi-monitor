package monitor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func monthlyBundleFixture(input MonthlyPreparationInput) MonthlyBundleContext {
	c := MonthlyBundleContext{Version: 1, Groups: []MonthlyBundleGroup{}, Accounts: []MonthlyBundleAccountContext{}}
	groups := map[int64]int{}
	for _, a := range input.Accounts {
		idx, ok := groups[a.GroupID]
		if !ok {
			idx = len(c.Groups)
			groups[a.GroupID] = idx
			c.Groups = append(c.Groups, MonthlyBundleGroup{GroupID: a.GroupID, Name: "synthetic", Source: "logs#scope", Months: append([]string{}, a.Months...), Members: []MonthlyBundleMember{}})
		}
		c.Groups[idx].Members = append(c.Groups[idx].Members, MonthlyBundleMember{UserID: a.UserID, Username: a.Username})
		meta := MonthlyBundleAccountContext{UserID: a.UserID, WalletOnly: true, WalletSource: "logs#wallet", Events: []MonthlyBundleEvent{}}
		for _, m := range a.Movements {
			kind, method := "recharge", "后台充值"
			if m.DeltaQuota < 0 {
				kind, method = "adjustment", "余额扣减"
			}
			meta.Events = append(meta.Events, MonthlyBundleEvent{ID: m.ID, At: m.At, DeltaQuota: m.DeltaQuota, Kind: kind, Method: method, Source: m.Source})
		}
		c.Accounts = append(c.Accounts, meta)
	}
	return c
}

func monthlyBundleRaw(t *testing.T, input MonthlyPreparationInput, c MonthlyBundleContext) ([]byte, []byte) {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	c.InputSHA256 = monthlyDigest(raw)
	meta, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return raw, meta
}

func monthlyBundleRun(t *testing.T, input MonthlyPreparationInput, c MonthlyBundleContext) MonthlyPreparationBundle {
	t.Helper()
	raw, meta := monthlyBundleRaw(t, input, c)
	b, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMonthlyBundleExactMoneyAndReviewBoundary(t *testing.T) {
	input := preparationFixture()
	c := monthlyBundleFixture(input)
	b := monthlyBundleRun(t, input, c)
	if b.ReadyForReview != 2 || b.Blocked != 1 || b.Mode != "local_candidate_bundle_not_publication" || b.SourceIntegrity != "not_checked_by_pure_calculator" {
		t.Fatalf("unexpected candidates: %+v", b)
	}
	for i, want := range []struct{ opening, closing, consume, refund int64 }{{500000, 2500000, 3000000, 0}, {2500000, -1750000, 4000000, 250000}} {
		e := b.Entries[i]
		u := e.Candidate.Users[0]
		if e.Status != "needs_review" || len(e.Issues) != 0 || u.OpeningQuota != want.opening || u.ClosingQuota != want.closing || u.ReverseClosingQuota != want.closing || u.ConsumeQuota != want.consume || u.RefundQuota != want.refund || len(u.Reversals) != 0 {
			t.Fatalf("accounting changed: %+v", e)
		}
		raw, err := json.Marshal(e.Candidate)
		if err != nil || monthlyDigest(raw) != e.CandidateSHA256 {
			t.Fatal("candidate fingerprint", err)
		}
		if bytes.Contains(raw, []byte("reviewed_by")) || bytes.Contains(raw, []byte("reviewed_at")) {
			t.Fatal("candidate invented an actual review")
		}
		if _, err := decodeMonthlyBillEvidence(raw); err == nil {
			t.Fatal("candidate can be accidentally imported")
		}
	}
	if b.Entries[2].Candidate != nil || b.Entries[2].CandidateSHA256 != "" || len(b.Entries[2].Issues) == 0 {
		t.Fatal("unresolved discrepancy converted into a candidate")
	}
	if !reflect.DeepEqual(b, monthlyBundleRun(t, input, c)) {
		t.Fatal("same evidence generated different results")
	}
	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeMonthlyBillEvidence(raw); err == nil {
		t.Fatal("bundle can be imported as a bill")
	}
}

func TestMonthlyBundleRejectsScopeAndInputMixing(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyBundleContext){
		"wrong name":   func(c *MonthlyBundleContext) { c.Groups[0].Members[0].Username = "other" },
		"wrong group":  func(c *MonthlyBundleContext) { c.Groups[0].GroupID++ },
		"empty member": func(c *MonthlyBundleContext) { c.Groups[0].Members = nil },
		"duplicate member": func(c *MonthlyBundleContext) {
			c.Groups[0].Members = append(c.Groups[0].Members, c.Groups[0].Members[0])
		},
		"duplicate group":   func(c *MonthlyBundleContext) { c.Groups = append(c.Groups, c.Groups[0]) },
		"duplicate context": func(c *MonthlyBundleContext) { c.Accounts = append(c.Accounts, c.Accounts[0]) },
		"foreign context":   func(c *MonthlyBundleContext) { c.Accounts[0].UserID++ },
		"bad source":        func(c *MonthlyBundleContext) { c.Groups[0].Source = "absent#members" },
		"duplicate month":   func(c *MonthlyBundleContext) { c.Groups[0].Months = append(c.Groups[0].Months, "2026-06") },
		"current month":     func(c *MonthlyBundleContext) { c.Groups[0].Months = []string{"2026-10"} },
		"implicit month":    func(c *MonthlyBundleContext) { c.Groups[0].Months = []string{""} },
	} {
		t.Run(name, func(t *testing.T) {
			input := preparationFixture()
			c := monthlyBundleFixture(input)
			mutate(&c)
			raw, meta := monthlyBundleRaw(t, input, c)
			if _, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0)); err == nil {
				t.Fatal("invalid scope accepted")
			}
		})
	}
	input := preparationFixture()
	raw, meta := monthlyBundleRaw(t, input, monthlyBundleFixture(input))
	for name, bad := range map[string][]byte{
		"wrong digest":  bytes.Replace(meta, []byte(monthlyDigest(raw)), []byte(strings.Repeat("a", 64)), 1),
		"duplicate key": append([]byte(`{"version":1,`), meta[1:]...),
		"unknown field": append([]byte(`{"apply":true,`), meta[1:]...),
		"case alias":    bytes.Replace(meta, []byte(`"version"`), []byte(`"Version"`), 1),
		"trailing":      append(append([]byte{}, meta...), []byte("{}")...),
		"oversized":     bytes.Repeat([]byte(" "), MonthlyPreparationInputLimit+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := PrepareMonthlyCandidateBundle(raw, bad, time.Unix(preparationAt("2026-10-20"), 0)); err == nil {
				t.Fatal("invalid context accepted")
			}
		})
	}
}

func TestMonthlyBundleMissingEvidenceDoesNotInventZeros(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyPreparationInput, *MonthlyBundleContext){
		"missing member": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Groups[0].Members = append(c.Groups[0].Members, MonthlyBundleMember{UserID: 103, Username: "missing"})
		},
		"missing wallet review": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) { c.Accounts[0].WalletOnly = false },
		"missing wallet source": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].WalletSource = "unknown#source"
		},
		"null events":             func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) { c.Accounts[0].Events = nil },
		"missing account context": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) { c.Accounts = nil },
	} {
		t.Run(name, func(t *testing.T) {
			input := preparationFixture()
			c := monthlyBundleFixture(input)
			mutate(&input, &c)
			b := monthlyBundleRun(t, input, c)
			if b.ReadyForReview != 0 || b.Blocked != 3 {
				t.Fatalf("missing evidence became a bill: %+v", b)
			}
		})
	}
}

func TestMonthlyBundleEventsMustMatchAndRemainSeparate(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyBundleContext){
		"missing classification": func(c *MonthlyBundleContext) { c.Accounts[0].Events = c.Accounts[0].Events[1:] },
		"changed amount":         func(c *MonthlyBundleContext) { c.Accounts[0].Events[0].DeltaQuota++ },
		"changed time":           func(c *MonthlyBundleContext) { c.Accounts[0].Events[0].At++ },
		"changed source":         func(c *MonthlyBundleContext) { c.Accounts[0].Events[0].Source = "logs#other-record" },
		"unsupported method":     func(c *MonthlyBundleContext) { c.Accounts[0].Events[0].Method = "guessed" },
		"wrong sign kind":        func(c *MonthlyBundleContext) { c.Accounts[0].Events[0].Kind = "adjustment" },
		"extra record": func(c *MonthlyBundleContext) {
			e := c.Accounts[0].Events[0]
			e.ID = 999
			c.Accounts[0].Events = append(c.Accounts[0].Events, e)
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := preparationFixture()
			c := monthlyBundleFixture(input)
			mutate(&c)
			b := monthlyBundleRun(t, input, c)
			if b.Entries[0].Candidate != nil || b.Entries[1].Candidate == nil {
				t.Fatal("bad June accepted or unrelated July blocked")
			}
		})
	}
	input := preparationFixture()
	a := &input.Accounts[0]
	a.Movements = append(a.Movements,
		MonthlyPreparationMovement{UserID: a.UserID, ID: 3, At: a.Baseline.At + 20, DeltaQuota: 400000000, Source: "logs#transfer-in"},
		MonthlyPreparationMovement{UserID: a.UserID, ID: 4, At: a.Baseline.At + 21, DeltaQuota: -400000000, Source: "logs#transfer-out"})
	b := monthlyBundleRun(t, input, monthlyBundleFixture(input))
	u := b.Entries[0].Candidate.Users[0]
	if len(u.Events) != 3 || len(u.Reversals) != 0 || u.Events[1].DeltaQuota != 400000000 || u.Events[2].DeltaQuota != -400000000 || u.ClosingQuota != 2500000 {
		t.Fatal("ordinary movements offset or dropped", u)
	}
}

func monthlyBundleRegistrationFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	input := preparationFixture()
	a := &input.Accounts[0]
	aug, sep := preparationAt("2026-08-01"), preparationAt("2026-09-01")
	a.Months = []string{"2026-08"}
	a.Baseline.At, a.Baseline.BalanceQuota = aug, 0
	a.Movements = []MonthlyPreparationMovement{{UserID: a.UserID, ID: 9, At: aug + 10*86400, DeltaQuota: 500000, Source: "logs#registration"}}
	a.MovementCoverage = []MonthlyPreparationCoverage{{UserID: a.UserID, From: aug, To: sep, Source: "logs#funds"}}
	a.Spans = []MonthlyPreparationSpan{{UserID: a.UserID, From: aug, To: sep, ConsumeQuota: 250000, Requests: preparationInt(1), Tokens: preparationInt(5), Source: "logs#usage"}}
	a.Snapshots = []MonthlyPreparationBalance{{UserID: a.UserID, At: sep, BalanceQuota: 250000, UsedQuota: 250000, Source: "logs#balance"}}
	c := monthlyBundleFixture(input)
	c.Groups[0].Months = []string{"2026-06", "2026-07", "2026-08"}
	c.Accounts[0].Events[0].Kind, c.Accounts[0].Events[0].Method = "gift", "注册赠送"
	c.Accounts[0].Registration = &MonthlyBundleRegistration{At: a.Movements[0].At, EventID: 9, Source: "logs#first-record", NoEarlierActivitySource: "logs#no-earlier-records"}
	return input, c
}

func TestMonthlyBundlePreRegistrationRequiresExplicitEvidence(t *testing.T) {
	input, c := monthlyBundleRegistrationFixture()
	b := monthlyBundleRun(t, input, c)
	if b.ReadyForReview != 3 || b.Blocked != 0 {
		t.Fatal(b)
	}
	for _, e := range b.Entries[:2] {
		u := e.Candidate.Users[0]
		if u.OpeningQuota != 0 || u.ClosingQuota != 0 || u.ConsumeQuota != 0 || u.RefundQuota != 0 || len(u.Events) != 0 || u.Requests != 0 || u.Tokens != 0 {
			t.Fatal("registration money pulled into an earlier month", u)
		}
	}
	for name, mutate := range map[string]func(*MonthlyPreparationInput, *MonthlyBundleContext){
		"no registration review": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) { c.Accounts[0].Registration = nil },
		"no earlier coverage proof": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Registration.NoEarlierActivitySource = ""
		},
		"wrong registration event": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) { c.Accounts[0].Registration.EventID++ },
		"unknown registration source": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Registration.Source = "absent#first"
		},
		"wrong registration method": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Events[0].Kind = "recharge"
			c.Accounts[0].Events[0].Method = "后台充值"
		},
		"nonzero origin": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Baseline.BalanceQuota = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			input, c := monthlyBundleRegistrationFixture()
			mutate(&input, &c)
			b := monthlyBundleRun(t, input, c)
			if b.Entries[0].Candidate != nil || b.Entries[1].Candidate != nil {
				t.Fatal("unproven zero month accepted")
			}
		})
	}
}

func TestMonthlyBundleCrossAccountEvidenceIsolation(t *testing.T) {
	input := preparationFixture()
	second := preparationFixture().Accounts[0]
	second.UserID, second.GroupID, second.Username = 125, 4, "other"
	second.Baseline.UserID = second.UserID
	for i := range second.Spans {
		second.Spans[i].UserID = second.UserID
	}
	for i := range second.Movements {
		second.Movements[i].UserID = second.UserID
		second.Movements[i].ID += 10
	}
	for i := range second.MovementCoverage {
		second.MovementCoverage[i].UserID = second.UserID
	}
	for i := range second.Snapshots {
		second.Snapshots[i].UserID = second.UserID
	}
	input.Accounts = append(input.Accounts, second)
	c := monthlyBundleFixture(input)
	c.Accounts[1].WalletOnly = false
	b := monthlyBundleRun(t, input, c)
	if b.ReadyForReview != 2 || b.Blocked != 4 || b.Entries[3].Candidate == nil {
		t.Fatal("one organization's problem affected another", b)
	}
	// Reuse of one source movement under two users cannot silently double count.
	input.Accounts[1].Movements[0].ID = input.Accounts[0].Movements[0].ID
	b = monthlyBundleRun(t, input, monthlyBundleFixture(input))
	if b.ReadyForReview != 0 {
		t.Fatal("cross-account event reuse accepted")
	}
}

func TestMonthlyBundleCannotOmitKnownOrganizationMember(t *testing.T) {
	input := preparationFixture()
	second := input.Accounts[0]
	second.UserID, second.Username = 103, "known-other-member"
	input.Accounts = append(input.Accounts, second)
	c := monthlyBundleFixture(input)
	c.Groups[0].Members = c.Groups[0].Members[:1]
	c.Accounts = c.Accounts[:1]
	raw, meta := monthlyBundleRaw(t, input, c)
	if _, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0)); err == nil {
		t.Fatal("partial organization bill accepted")
	}
}
