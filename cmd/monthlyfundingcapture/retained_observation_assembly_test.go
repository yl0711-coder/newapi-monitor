package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestObservationAssemblyRetainedRealMonthAndNormalization(t *testing.T) {
	usageRoot := os.Getenv("MONTHLY_CAPTURE_RETAINED_MONTH")
	fundingRoot := os.Getenv("MONTHLY_FUNDING_RETAINED_MONTH")
	walletRoot := os.Getenv("MONTHLY_ASSEMBLY_RETAINED_WALLET")
	inputPath := os.Getenv("MONTHLY_BALANCE_RETAINED_INPUT")
	if usageRoot == "" || fundingRoot == "" || walletRoot == "" || inputPath == "" {
		t.Skip("explicit private real usage/funding/wallet originals and independent preparation required")
	}
	// Full original independent evidence checks precede composition. No report
	// JSON or successful normalization is substituted for source verification.
	TestObservationRetainedWholeMonthUsageReadOnly(t)
	TestDailyFundingRetainedRealCompleteMonthReadOnly(t)
	ctx := context.Background()
	root, err := filepath.EvalSymlinks(auditPrivateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(fundingRoot, "september")
	plan := observationAssemblyPlan{Version: 1,
		FundingPlan: assemblyFile{filepath.Join(dir, "month-plan.json"), os.Getenv("MONTHLY_FUNDING_RETAINED_PLAN_SHA")},
		FundingDB:   assemblyFile{filepath.Join(dir, "observation-assembly", "funding.sqlite"), "e1dbbe5d89385965000c4fa3b55a859cf0238bd56fd183f0338acdf1ca862671"}}
	ref := func(path string) assemblyCapture {
		t.Helper()
		raw, err := readAssemblyManifest(path)
		if err != nil {
			t.Fatal(err)
		}
		return assemblyCapture{path, hash(raw)}
	}
	for day := 1; day <= 30; day++ {
		name := fmt.Sprintf("sep-d%02d", day)
		plan.Usage = append(plan.Usage, assemblyUsagePair{ref(filepath.Join(usageRoot, name+"-a")), ref(filepath.Join(usageRoot, name+"-b"))})
	}
	plan.Wallet = []assemblyCapture{ref(walletRoot)}
	raw, _ := json.Marshal(plan)
	before, err := assembleObservations(ctx, root, "combined", plan, raw, false)
	if err != nil {
		t.Fatal("real original composition preflight", err)
	}
	receipt, err := assembleObservations(ctx, root, "combined", plan, raw, true)
	if err != nil || receipt.ReviewSHA256 != before.ReviewSHA256 || receipt.SelectionSHA256 != before.SelectionSHA256 || receipt.FundingPages != 62 || receipt.FundingRows != 1732 || receipt.UsageCaptures != 60 || receipt.WalletCaptures != 1 {
		t.Fatal("real sealed composition", err, receipt)
	}
	selectionRaw, err := privateRead(filepath.Join(root, "combined", "selection.json"), 2<<20)
	var selection observationAssemblySelection
	if err != nil || json.Unmarshal(selectionRaw, &selection) != nil || hash(selectionRaw) != receipt.SelectionSHA256 {
		t.Fatal("real selection changed", err)
	}
	inputRaw, err := privateRead(inputPath, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(inputRaw)
	if err != nil {
		t.Fatal(err)
	}
	var account monitor.MonthlyPreparationAccount
	for _, a := range input.Accounts {
		if a.UserID == 148 && a.GroupID == 27 {
			account = a
		}
	}
	if account.UserID != 148 {
		t.Fatal("retained account missing")
	}
	account.Months = []string{"2026-09"}
	account.Spans = slices.Clone(account.Spans)
	account.Movements = slices.Clone(account.Movements)
	account.Snapshots = slices.Clone(account.Snapshots)
	// Explicit new template selection: preserve all outside-month evidence;
	// the unmodified original input remains retained and is never overwritten.
	account.Spans = slices.DeleteFunc(account.Spans, func(s monitor.MonthlyPreparationSpan) bool {
		if s.From < 1790784000 && s.To > 1788192000 {
			if s.From != 1788192000 || s.To != 1790784000 {
				t.Fatal("cannot discard a partially overlapping historical span")
			}
			return true
		}
		return false
	})
	account.Movements = slices.DeleteFunc(account.Movements, func(m monitor.MonthlyPreparationMovement) bool {
		return m.At >= 1788192000 && m.At < 1790784000
	})
	for i := range account.Snapshots {
		if account.Snapshots[i].TimeBasis != "" && account.Snapshots[i].TimeBasis != "collection_observation" {
			t.Fatal("independent reviewed cutoff needs separate preparation")
		}
		account.Snapshots[i].TimeBasis = "collection_observation"
	}
	refs := []string{account.BasisSource, account.Baseline.Source, account.Baseline.CutoffSource}
	for _, s := range account.Spans {
		refs = append(refs, s.Source)
	}
	for _, m := range account.Movements {
		refs = append(refs, m.Source)
	}
	for _, c := range account.MovementCoverage {
		refs = append(refs, c.Source)
	}
	for _, s := range account.Snapshots {
		refs = append(refs, s.Source, s.CutoffSource)
	}
	wanted := map[string]bool{}
	for _, ref := range refs {
		id, _, _ := strings.Cut(ref, "#")
		if id != "" {
			wanted[id] = true
		}
	}
	sources := []monitor.MonthlyPreparationSource{}
	originals := map[string]string{inputPath: hash(inputRaw), plan.FundingDB.Path: plan.FundingDB.SHA256}
	for _, source := range input.Sources {
		if !wanted[source.ID] {
			continue
		}
		b, err := privateRead(source.Path, monitor.MonthlyPreparationInputLimit)
		if err != nil || hash(b) != source.SHA256 {
			t.Fatal("independent historical source differs", source.ID, err)
		}
		originals[source.Path] = source.SHA256
		source.Path = filepath.Join(root, "original-"+source.ID+".json")
		if err := exclusiveFile(source.Path, b); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, source)
		delete(wanted, source.ID)
	}
	if len(wanted) != 0 {
		t.Fatal("missing historical reference")
	}
	database := filepath.Join(root, "combined", "observations.sqlite")
	sources = append(sources, monitor.MonthlyPreparationSource{ID: "real_observations", Path: database, SHA256: receipt.DatabaseSHA256})
	template := monitor.MonthlyPreparationInput{Version: 1, Sources: sources, Accounts: []monitor.MonthlyPreparationAccount{account}}
	templateRaw, _ := json.Marshal(template)
	receiverPath := filepath.Join(root, "local-receiver.sqlite")
	if err := exclusiveFile(receiverPath, nil); err != nil {
		t.Fatal(err)
	}
	receiver, err := openReceiver(ctx, receiverPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := monthlydraft.InitGenerationReceiver(ctx, receiver); err != nil {
		t.Fatal(err)
	}
	observations, err := openReceiver(ctx, database, false)
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	// Local authorization fixture only. No production directory/group approval
	// is inferred from these original account records or matching balances.
	owner := monthlytasks.Owner{GroupID: 27, AuthVersion: 1, MemberFingerprint: hash([]byte("148,"))[:32], MemberManifest: hash([]byte("explicit-local-acceptance-only"))}
	g, err := monthlydraft.NewGeneration(receiver, observations, root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	generationPlan := monthlydraft.GenerationPlan{Version: 2, Owner: owner, PreparationSHA256: hash(templateRaw), Funding: selection.Funding,
		Windows: selection.FundingWindows, UsageWindows: selection.UsageWindows,
		WalletObservations: &monthlydraft.WalletObservationSelection{SourceID: "real_observations", IDs: selection.WalletIDs}}
	prepared, err := g.PrepareObservedInput(ctx, generationPlan, templateRaw, "real_observations")
	if err != nil {
		t.Fatal("real formal input normalization", err)
	}
	again, err := g.PrepareObservedInput(ctx, generationPlan, templateRaw, "real_observations")
	if err != nil || !reflectPrepared(prepared, again) {
		t.Fatal("original template normalization is not reproducible", err)
	}
	var normalized monitor.MonthlyPreparationInput
	if json.Unmarshal(prepared.Preparation, &normalized) != nil || len(normalized.Accounts) != 1 {
		t.Fatal("normalized account missing")
	}
	a := normalized.Accounts[0]
	var consume, refund, requests, funding int64
	days, events := 0, 0
	for _, span := range a.Spans {
		if span.From >= 1788192000 && span.To <= 1790784000 {
			if span.Tokens != nil || span.Requests == nil || !strings.HasPrefix(span.Source, "real_observations#usage:") {
				t.Fatal("usage provenance or unknown token coverage changed")
			}
			days++
			consume, refund, requests = consume+span.ConsumeQuota, refund+span.RefundQuota, requests+*span.Requests
		}
	}
	for _, movement := range a.Movements {
		if movement.At >= 1788192000 && movement.At < 1790784000 {
			if movement.ID != 3715551 || !strings.HasPrefix(movement.Source, "real_observations#funding:") {
				t.Fatal("credited original funding identity changed")
			}
			events++
			funding += movement.DeltaQuota
		}
	}
	if days != 30 || consume != 7480870 || refund != 0 || requests != 221 || events != 1 || funding != 500000000 ||
		len(a.Movements) != len(account.Movements)+1 || len(a.Snapshots) != len(account.Snapshots)+1 {
		t.Fatal("formal source assembly changed actual monthly money")
	}
	for _, snapshot := range a.Snapshots {
		if strings.HasPrefix(snapshot.Source, "real_observations#wallet:") && (snapshot.TimeBasis != "collection_observation" || snapshot.CutoffSource != "") {
			t.Fatal("current wallet observation became a historical boundary")
		}
	}
	var nextPlan monthlydraft.GenerationPlan
	if json.Unmarshal(prepared.Plan, &nextPlan) != nil {
		t.Fatal("prepared plan missing")
	}
	if _, err := g.PrepareObservedInput(ctx, nextPlan, prepared.Preparation, "real_observations"); err == nil {
		t.Fatal("normalization silently replaced existing monthly amounts")
	}
	for path, want := range originals {
		if fileHash(t, path) != want {
			t.Fatal("original changed", filepath.Base(path))
		}
	}
	if fileHash(t, database) != receipt.DatabaseSHA256 {
		t.Fatal("sealed observation source was modified by input preparation")
	}
	var count int
	if err := receiver.QueryRow("SELECT count(*) FROM portal_monthly_generation_inputs").Scan(&count); err != nil || count != 0 {
		t.Fatal("input assembly silently retained a customer task", err)
	}
	t.Log("real originals assembled and re-opened: 62 funding pages/1732 observation rows, 60 daily usage captures, one current wallet capture; formal PrepareObservedInput preserves 30 days, 221 consumes/7480870 quota, one credit/500000000 quota and unknown tokens; original sources unchanged, no customer task, historical approval or bill publication")
	if sqlPath := os.Getenv("MONTHLY_BALANCE_RETAINED_SCOPE_SQL"); sqlPath != "" {
		t.Run("explicit_local_reconstruction_mechanics_not_authorization", func(t *testing.T) {
			testRealGenerationReconstructionMechanics(t, g, root, normalized, nextPlan, sqlPath)
		})
		if fileHash(t, database) != receipt.DatabaseSHA256 {
			t.Fatal("local generation changed the sealed observation source")
		}
	}
}

func reflectPrepared(a, b monthlydraft.ObservedPreparation) bool {
	return bytes.Equal(a.Plan, b.Plan) && bytes.Equal(a.Preparation, b.Preparation) && a.FundingSHA256 == b.FundingSHA256 && a.UsageSHA256 == b.UsageSHA256 && a.WalletSHA256 == b.WalletSHA256
}

// Uses REAL amounts with an explicitly SYNTHETIC local historical-scope and
// authority selection to exercise task mechanics. This is not an attestation
// of the customer's historical directory/wallet semantics or authority to
// publish a bill. Nothing writes to a real authority/publication database.
func testRealGenerationReconstructionMechanics(t *testing.T, g *monthlydraft.Generation, root string, input monitor.MonthlyPreparationInput, plan monthlydraft.GenerationPlan, sqlPath string) {
	t.Helper()
	sqlRaw, err := privateRead(sqlPath, 1<<20)
	var queries map[string]string
	if err != nil || json.Unmarshal(sqlRaw, &queries) != nil || queries["agg_148_1790784000_1792484737"] != retainedAggregateSQL(148, 1790784000, 1792484737) {
		t.Fatal("real retained bridge query differs", err)
	}
	copyPath := filepath.Join(root, "independent-snapshot-query.json")
	if err := exclusiveFile(copyPath, sqlRaw); err != nil {
		t.Fatal(err)
	}
	input.Sources = append(slices.Clone(input.Sources), monitor.MonthlyPreparationSource{ID: "balance_query", Path: copyPath, SHA256: hash(sqlRaw)})
	account := input.Accounts[0]
	var monthConsume, monthRefund, bridgeConsume, bridgeRefund int64
	for _, span := range account.Spans {
		switch {
		case span.From >= 1788192000 && span.To <= 1790784000:
			monthConsume += span.ConsumeQuota
			monthRefund += span.RefundQuota
		case span.From == 1790784000 && span.To == 1792484691:
			bridgeConsume, bridgeRefund = span.ConsumeQuota, span.RefundQuota
		}
	}
	if len(account.Snapshots) < 2 || account.Snapshots[0].At != 1792484691 || monthConsume != 7480870 || monthRefund != 0 || bridgeConsume != 57005264 || bridgeRefund != 0 {
		t.Fatal("real original reconstruction observations missing")
	}
	snapshot := account.Snapshots[0]
	ids := []int64{}
	for _, movement := range account.Movements {
		if movement.At < snapshot.At {
			ids = append(ids, movement.ID)
		}
	}
	if len(ids) != 3 {
		t.Fatal("real funding set changed")
	}
	r := monitor.MonthlySnapshotReconstruction{UserID: account.UserID, Month: plan.Funding.Month, ObservedAt: snapshot.At,
		SnapshotSource: snapshot.Source, ScopeSource: "balance_query#test-only-view-selection-not-historical-attestation",
		MonthSource: "real_observations#independently-audited-month", BridgeSource: "s002#agg_148_1790784000_1792484737",
		FundingSource: "real_observations#original-selected-funding-plus-retained-history", OpeningBasisSource: account.Baseline.Source,
		BalanceQuota: &snapshot.BalanceQuota, UsedQuota: &snapshot.UsedQuota, MonthConsumeQuota: &monthConsume, MonthRefundQuota: &monthRefund,
		BridgeConsumeQuota: &bridgeConsume, BridgeRefundQuota: &bridgeRefund, FundingEventIDs: ids}
	scope := monthlydraft.AutomaticScope{Group: monitor.MonthlyBundleGroup{GroupID: account.GroupID,
		Name: "local-mechanics-only-not-customer-authorization", Source: "s002#explicit-test-directory-not-historical-attestation", Months: account.Months,
		Members: []monitor.MonthlyBundleMember{{UserID: account.UserID, Username: account.Username}}},
		Accounts: []monthlydraft.AutomaticWalletScope{{UserID: account.UserID, WalletOnly: true,
			WalletSource: "s041#explicit-test-wallet-selection-not-historical-attestation", Reconstructions: []monitor.MonthlySnapshotReconstruction{r}}}}
	raw, _ := json.Marshal(input)
	plan.PreparationSHA256 = hash(raw)
	// This uses the normal retained-scope packager rather than setting private
	// task success flags or turning observations into exact month-end snapshots.
	scopePath := filepath.Join(root, "local-test-history-selection.json")
	selected, err := g.PrepareRetainedScopeInput(context.Background(), plan, raw, "local_test_history", scopePath, scope)
	if err != nil {
		t.Fatal("real arithmetic with explicit synthetic test scope cannot be prepared", err)
	}
	if err := exclusiveFile(scopePath, selected.Scope); err != nil {
		t.Fatal(err)
	}
	var selectedPlan monthlydraft.GenerationPlan
	if json.Unmarshal(selected.Plan, &selectedPlan) != nil {
		t.Fatal("selected test plan missing")
	}
	job, err := g.Submit(context.Background(), selectedPlan, selected.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil || artifact.ScopeReview == nil || artifact.Reconciliation.Accounts[0].Months[0].Status == "reconciled_draft" {
		t.Fatal("real-money local draft failed or erased missing original cutoff", err)
	}
	load, err := monthlydraft.NewRetainedAutomaticScopeLoader(g, "local_test_history")
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := monthlydraft.NewObservedAutomaticContext(g, load)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata(context.Background(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	handoff, err := g.PrepareReviewHandoff(context.Background(), job.Request.Owner, job.ID, meta.BundleContext)
	if err != nil {
		t.Fatal(err)
	}
	var bundle monitor.MonthlyPreparationBundle
	if json.Unmarshal(handoff.Bundle, &bundle) != nil || bundle.ReadyForReview != 1 || len(bundle.Entries) != 1 || bundle.Entries[0].Candidate == nil {
		t.Fatal("real arithmetic candidate missing")
	}
	u := bundle.Entries[0].Candidate.Users[0]
	if u.OpeningQuota != 96011849 || u.ClosingQuota != 588530979 || u.ReverseClosingQuota != 588530979 || u.ConsumeQuota != 7480870 || u.RefundQuota != 0 ||
		len(u.Events) != 1 || u.Events[0].DeltaQuota != 500000000 || u.Requests != 221 {
		t.Fatal("real generation/handoff changed actual money")
	}
	before, _ := json.Marshal(artifact)
	again, err := g.Run(context.Background(), job.Request.Owner, job.ID)
	after, _ := json.Marshal(again)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("real local artifact not reused unchanged", err)
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(*bundle.Entries[0].Candidate, time.Now().Unix(), time.Now()); err != nil {
		t.Fatal("real arithmetic cannot pass unchanged statement builder", err)
	}
	t.Log("REAL numbers, SYNTHETIC local authority/history selection: generation -> saved private draft -> re-audit/reuse -> handoff preserves 192.023698 + 1000 - 14.961740 = 1177.061958 CNY; no original midnight snapshot invented, no approval of real history semantics, no customer bill publication")
}
