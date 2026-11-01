package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Only new synthetic files and accounts. No original private captures, online
// connections, customer authority or production bill publication are used.
func addTokenArchives(t *testing.T, f *generationFixture) {
	t.Helper()
	start := f.plan.Windows[0].From
	for _, user := range f.plan.Funding.UserIDs {
		archive := monthlysource.TokenUsageMonthArchive{Version: 1, Basis: monthlysource.TokenUsageArchiveBasis, SelectionSHA256: strings.Repeat("c", 64), SourceIdentity: f.plan.Funding.SourceIdentity, Rules: f.plan.Funding.Rules, UserID: user, Month: f.plan.Funding.Month, Pairs: []monthlysource.TokenUsageWindowPair{}}
		for day := 0; day < 30; day++ {
			from := start + int64(day)*86400
			started := time.Now().Add(-24*time.Hour).UnixMilli() + int64(day)*1000
			balance, used := int64(104000000), int64(1001000000)
			o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary", Request: monthlysource.UsageWindowRequest{SourceIdentity: archive.SourceIdentity, Rules: archive.Rules, UserID: user, From: from, To: from + 86400}, StartedAtMS: started, CompletedAtMS: started + 1, Wallet: monthlysource.WalletUser{UserID: user, Exists: true, BalanceQuota: &balance, UsedQuota: &used}, Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
			makeRow := func(token, quota, id int64, kind int, name string) monthlysource.TokenUsageWindowRow {
				return monthlysource.TokenUsageWindowRow{UsageWindowRow: monthlysource.UsageWindowRow{UserID: user, Type: kind, BillingSource: "wallet", Records: 1, Quota: quota, MinQuota: quota, MaxQuota: quota, MinID: id, MaxID: id, FirstAt: from + 10 + id%10, LastAt: from + 10 + id%10}, TokenID: token, Name: name}
			}
			base := user * 100000
			if day == 0 {
				o.Rows = []monthlysource.TokenUsageWindowRow{makeRow(0, 1, base+3, 2, "legacy"), makeRow(user*100+1, 1000000, base+1, 2, "same name"), makeRow(user*100+2, 499999, base+2, 2, "same name"), makeRow(user*100+2, 500000, base+4, 6, "same name")}
				o.Totals = []monthlysource.UsageWindowRow{{UserID: user, Type: 2, BillingSource: "wallet", Records: 3, Quota: 1500000, MinQuota: 1, MaxQuota: 1000000, MinID: base + 1, MaxID: base + 3, FirstAt: from + 11, LastAt: from + 13}, o.Rows[3].UsageWindowRow}
			} else if day == 1 {
				o.Rows = []monthlysource.TokenUsageWindowRow{makeRow(user*100+1, 0, base+11, 2, "renamed")}
				o.Totals = []monthlysource.UsageWindowRow{o.Rows[0].UsageWindowRow}
			}
			newer := o
			newer.StartedAtMS = o.CompletedAtMS + 300000
			newer.CompletedAtMS = newer.StartedAtMS + 1
			newer.Rows = append([]monthlysource.TokenUsageWindowRow{}, o.Rows...)
			newer.Totals = append([]monthlysource.UsageWindowRow{}, o.Totals...)
			archive.Pairs = append(archive.Pairs, monthlysource.TokenUsageWindowPair{Older: o, Newer: newer})
		}
		body, err := json.Marshal(archive)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), body, time.Now()); err != nil {
			t.Fatal("invalid token archive fixture", err)
		}
		id := fmt.Sprintf("tokens-%d", user)
		path := filepath.Join(f.root, id+".json")
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: id, Path: path, SHA256: generationHash(body)})
		f.plan.TokenUsageSources = append(f.plan.TokenUsageSources, TokenUsageSourceSelection{UserID: user, SourceID: id})
	}
	usageInputUpdate(t, f)
}

func applyTokenInput(t *testing.T, f *generationFixture) TokenUsagePreparation {
	t.Helper()
	result, err := f.g.PrepareTokenUsageInput(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(result.Plan, &f.plan) != nil || json.Unmarshal(result.Preparation, &f.input) != nil {
		t.Fatal("invalid normalized token input")
	}
	f.raw = bytes.Clone(result.Preparation)
	return result
}

func TestTokenGenerationPrepareRunReuseAndHandoffMoneyUnchanged(t *testing.T) {
	f := generationFixtureNew(t)
	addTokenArchives(t, &f)
	ctx := context.Background()
	before := bytes.Clone(f.raw)
	original := draftCopy(t, f.input)
	if _, err := f.g.Submit(ctx, f.plan, f.raw); err == nil {
		t.Fatal("unprepared token template queued")
	}
	result, err := f.g.PrepareTokenUsageInput(ctx, f.plan, f.raw)
	if err != nil || !generationHex(result.TokenSHA256, 32) || !bytes.Equal(before, f.raw) {
		t.Fatal("token preparation failed or modified caller", err)
	}
	var normalized monitor.MonthlyPreparationInput
	if json.Unmarshal(result.Preparation, &normalized) != nil {
		t.Fatal("invalid prepared input")
	}
	for i := range normalized.Accounts {
		if len(normalized.Accounts[i].TokenMonths) != 1 || len(normalized.Accounts[i].TokenMonths[0].Rows) != 3 {
			t.Fatal("member token breakdown missing")
		}
		normalized.Accounts[i].TokenMonths = nil
	}
	if !reflect.DeepEqual(normalized, original) {
		t.Fatal("token preparation changed original user money, funding, balances or statistics")
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
	if json.Unmarshal(result.Preparation, &f.input) != nil || json.Unmarshal(result.Plan, &f.plan) != nil {
		t.Fatal("invalid prepared documents")
	}
	f.raw = result.Preparation
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || artifact.TokenSHA256 != result.TokenSHA256 {
		t.Fatal("token generation cannot complete", err)
	}
	assertGenerationAmounts(t, artifact)
	f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		t.Fatal("cached token bill recalculated user wallet")
		return Review{}, nil
	}
	for range 2 {
		again, err := f.g.Run(ctx, job.Request.Owner, job.ID)
		if err != nil || !reflect.DeepEqual(again, artifact) {
			t.Fatal("token draft not stable", err)
		}
	}
	reopened, err := NewGeneration(f.receiver, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || !reflect.DeepEqual(again, artifact) {
		t.Fatal("token draft changed on restart", err)
	}
	handoff, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, handoffContext(t, f))
	if err != nil {
		t.Fatal(err)
	}
	var bundle monitor.MonthlyPreparationBundle
	if json.Unmarshal(handoff.Bundle, &bundle) != nil || bundle.Entries[0].Candidate == nil {
		t.Fatal("token candidate missing")
	}
	candidate := bundle.Entries[0].Candidate
	for _, u := range candidate.Users {
		if u.TokenUsage == nil || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage.Rows[1].Name != "renamed" {
			t.Fatal("handoff changed amounts or attribution", u)
		}
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(*candidate, time.Now().Unix(), time.Now()); err != nil {
		t.Fatal("existing policy rejects source-bound tokens", err)
	}
	if _, err := f.g.PrepareTokenUsageInput(ctx, f.plan, f.raw); err == nil {
		t.Fatal("normalized token rows silently overwritten")
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
	if _, err := f.g.Run(ctx, job.Request.Owner, job.ID); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("revoked authority reused token artifact", err)
	}
}

func TestTokenGenerationRejectsInputsThatBalanceButMisattribute(t *testing.T) {
	for _, scenario := range []string{"redistributed", "wrong_name", "missing_user", "no_selection", "foreign_selection", "incomplete_selection", "duplicate_source", "unknown_source", "changed_file", "hardlink", "old_rules"} {
		t.Run(scenario, func(t *testing.T) {
			f := generationFixtureNew(t)
			addTokenArchives(t, &f)
			applyTokenInput(t, &f)
			switch scenario {
			case "redistributed":
				f.input.Accounts[0].TokenMonths[0].Rows[0].ConsumeQuota--
				f.input.Accounts[0].TokenMonths[0].Rows[1].ConsumeQuota++
			case "wrong_name":
				f.input.Accounts[0].TokenMonths[0].Rows[1].Name = "not the retained name"
			case "missing_user":
				f.input.Accounts[1].TokenMonths = nil
			case "no_selection":
				f.plan.TokenUsageSources = nil
			case "foreign_selection":
				f.plan.TokenUsageSources[0].SourceID, f.plan.TokenUsageSources[1].SourceID = f.plan.TokenUsageSources[1].SourceID, f.plan.TokenUsageSources[0].SourceID
			case "incomplete_selection":
				f.plan.TokenUsageSources = f.plan.TokenUsageSources[:1]
			case "duplicate_source":
				f.plan.TokenUsageSources[1].SourceID = f.plan.TokenUsageSources[0].SourceID
			case "unknown_source":
				f.plan.TokenUsageSources[0].SourceID = "absent"
			case "changed_file":
				s, _ := tokenSource(f.input, f.plan.TokenUsageSources[0].SourceID)
				if err := os.WriteFile(s.Path, []byte("changed synthetic archive"), 0600); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				s, _ := tokenSource(f.input, f.plan.TokenUsageSources[0].SourceID)
				if err := os.Link(s.Path, s.Path+".alias"); err != nil {
					t.Fatal(err)
				}
			case "old_rules":
				f.plan.CalculationVersion = "local-monthly-draft-v6-selected-reconstruction"
			}
			usageInputUpdate(t, &f)
			job, err := f.g.Submit(context.Background(), f.plan, f.raw)
			if err != nil {
				return
			}
			artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
			if err == nil || len(artifact.Reconciliation.Accounts) != 0 || artifact.TokenSHA256 != "" {
				t.Fatal("incorrect token data produced a draft", err)
			}
			var count int
			if err := f.receiver.QueryRow("SELECT count(*) FROM portal_monthly_generation_inputs WHERE artifact IS NOT NULL").Scan(&count); err != nil || count != 0 {
				t.Fatal("bad token artifact saved", err)
			}
		})
	}
}

func TestTokenGenerationRejectsForgedCachedTokenDigest(t *testing.T) {
	f := generationFixtureNew(t)
	addTokenArchives(t, &f)
	applyTokenInput(t, &f)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	artifact.TokenSHA256 = strings.Repeat("a", 64)
	raw, _ := json.Marshal(artifact)
	// Only deliberately corrupt this test's newly created private receiver.
	if _, err := f.receiver.Exec("DROP TRIGGER monthly_generation_no_replace"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec("UPDATE portal_monthly_generation_inputs SET artifact=?,artifact_hash=? WHERE id=?", raw, generationHash(raw), job.Request.InputSHA256); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec("UPDATE portal_monthly_draft_tasks SET result_sha256=? WHERE id=?", generationHash(raw), job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("forged token digest reused", err)
	}
}

func TestTokenGenerationRejectsDifferentWindowsEvenWhenMonthlyAmountsMatch(t *testing.T) {
	f := generationFixtureNew(t)
	addTokenArchives(t, &f)
	a := &f.input.Accounts[0]
	span := a.Spans[0]
	a.Spans = nil
	for day := 0; day < 30; day++ {
		s := span
		s.From = span.From + int64(day)*86400
		s.To = s.From + 86400
		if day != 0 {
			s.ConsumeQuota, s.RefundQuota = 0, 0
		}
		a.Spans = append(a.Spans, s)
	}
	for i, s := range f.input.Sources {
		if s.ID != f.plan.TokenUsageSources[0].SourceID {
			continue
		}
		raw, err := os.ReadFile(s.Path)
		if err != nil {
			t.Fatal(err)
		}
		var archive monthlysource.TokenUsageMonthArchive
		if json.Unmarshal(raw, &archive) != nil {
			t.Fatal("bad archive fixture")
		}
		for _, o := range []*monthlysource.TokenUsageWindowObservation{&archive.Pairs[0].Older, &archive.Pairs[0].Newer} {
			o.Rows[1].Quota--
			o.Rows[1].MinQuota--
			o.Rows[1].MaxQuota--
			o.Totals[0].Quota--
			o.Totals[0].MaxQuota--
		}
		for _, o := range []*monthlysource.TokenUsageWindowObservation{&archive.Pairs[1].Older, &archive.Pairs[1].Newer} {
			o.Rows[0].Quota++
			o.Rows[0].MinQuota++
			o.Rows[0].MaxQuota++
			o.Totals[0] = o.Rows[0].UsageWindowRow
		}
		raw, _ = json.Marshal(archive)
		_, report, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), raw, time.Now())
		if err != nil || report.ConsumeQuota != 1500000 || report.RefundQuota != 500000 {
			t.Fatal("counterexample did not preserve valid monthly source amount", err)
		}
		if err := os.WriteFile(s.Path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		f.input.Sources[i].SHA256 = generationHash(raw)
	}
	usageInputUpdate(t, &f)
	if _, err := f.g.PrepareTokenUsageInput(context.Background(), f.plan, f.raw); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("different daily source windows accepted on monthly amount alone", err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestTokenGenerationFormalPreparationChecksSourceWithoutWrites(t *testing.T) {
	f, cfg := preparationFixture(t)
	addTokenArchives(t, &f)
	applyTokenInput(t, &f)
	result, err := PrepareGenerationInput(context.Background(), cfg, mustGenerationPlan(t, f), f.raw, "", false)
	if err != nil || result.Applied || result.Changed {
		t.Fatal("formal token inspection failed", err)
	}
	preparationCounts(t, f, 0, 0)
	f.input.Accounts[0].TokenMonths[0].Rows[0].ConsumeQuota--
	f.input.Accounts[0].TokenMonths[0].Rows[1].ConsumeQuota++
	usageInputUpdate(t, &f)
	if _, err := PrepareGenerationInput(context.Background(), cfg, mustGenerationPlan(t, f), f.raw, "", false); err == nil {
		t.Fatal("formal inspection ignored bad token attribution")
	}
	preparationCounts(t, f, 0, 0)
}
