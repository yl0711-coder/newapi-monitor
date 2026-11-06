package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestPreRegistrationOriginalRowSelection(t *testing.T) {
	log := monthlysource.FundingLog{ID: 700, UserID: 202, CreatedAt: 1791648000, Type: 4, Content: "新用户注册赠送 $1.000000 额度", Other: ""}
	body := materialPreparationJSON(t, map[string]any{"accounts": []any{map[string]any{"first_funding": []monthlysource.FundingLog{log}}}})
	r := monitor.MonthlyBundleRegistration{At: log.CreatedAt, EventID: log.ID, Source: "original#/accounts/0/first_funding/0", NoEarlierActivitySource: "original#reviewed-no-earlier-activity"}
	rules := monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}
	e, err := registrationFromOriginal(body, "/accounts/0/first_funding/0", 202, r, rules)
	if err != nil || e.ID != 700 || e.DeltaQuota != 500000 || e.Kind != "gift" || e.Method != "注册赠送" {
		t.Fatal("original registration did not use the existing source normalizer", err)
	}
	for _, pointer := range []string{"", "accounts/0/first_funding/0", "/accounts/01/first_funding/0", "/accounts/-1/first_funding/0", "/accounts/0/missing/0", "/accounts/0/first_funding/2", "/accounts/0/first_funding/~2"} {
		if _, err := registrationFromOriginal(body, pointer, 202, r, rules); err == nil {
			t.Fatal("invalid/ambiguous source selection accepted", pointer)
		}
	}
	for name, change := range map[string]func(monthlysource.FundingLog) monthlysource.FundingLog{
		"another user":    func(l monthlysource.FundingLog) monthlysource.FundingLog { l.UserID++; return l },
		"wrong id":        func(l monthlysource.FundingLog) monthlysource.FundingLog { l.ID++; return l },
		"wrong timestamp": func(l monthlysource.FundingLog) monthlysource.FundingLog { l.CreatedAt++; return l },
		"topup not gift": func(l monthlysource.FundingLog) monthlysource.FundingLog {
			l.Type = 1
			l.Content = "使用在线充值成功，充值金额: $1.000000，支付金额：1.00"
			return l
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw := materialPreparationJSON(t, map[string]any{"row": change(log)})
			if _, err := registrationFromOriginal(raw, "/row", 202, r, rules); err == nil {
				t.Fatal("unproven registration accepted")
			}
		})
	}
	duplicate := bytes.Replace(body, []byte(`"accounts":`), []byte(`"accounts":[],"accounts":`), 1)
	if _, err := registrationJSONPointer(duplicate, "/accounts/0/first_funding/0"); err == nil {
		t.Fatal("duplicate source keys accepted")
	}
}

// Fully synthetic closed-month captures. One member is active; the other did
// not register until the following month. No real data or source is rewritten.
func preRegistrationGenerationFixture(t *testing.T) generationFixture {
	t.Helper()
	ctx := t.Context()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, zone)
	from, to := start.Unix(), start.AddDate(0, 1, 0).Unix()
	registered := to + 10*86400
	collected := to + 20*86400
	rules := monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}
	req := monthlysource.FundingMonthRequest{Month: "2026-09", SourceIdentity: strings.Repeat("a", 64), Rules: rules, UserIDs: []int64{201, 202}}
	dbPath := filepath.Join(root, "observations.sqlite")
	db := generationDB(t, dbPath, false)
	if err := monthlysource.InitFundingPageStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitUsageWindowStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	p := GenerationPlan{Version: 2, Funding: req, Owner: monthlytasks.Owner{GroupID: 1, AuthVersion: 1, MemberFingerprint: generationMemberHash(req.UserIDs), MemberManifest: strings.Repeat("b", 64)}, ObservationStoreSourceID: "observed"}
	for at := from; at < to; at += 86400 {
		var scans []string
		for pass := int64(0); pass < 2; pass++ {
			scan, err := monthlysource.NewFundingDayScan(req.SourceIdentity, rules, req.UserIDs, at, at+86400)
			if err != nil {
				t.Fatal(err)
			}
			page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 1000, EndReached: true, StartedAtMS: collected*1000 + pass*300001, CompletedAtMS: collected*1000 + pass*300001 + 1, Rows: []monthlysource.FundingLog{}}
			if at == from {
				page.Rows = []monthlysource.FundingLog{{ID: 1, UserID: 201, CreatedAt: from + 1, Type: 1, Content: "使用在线充值成功，充值金额: $2.000000，支付金额：2.00"}}
				page.Next = monthlysource.FundingCursor{At: from + 1, ID: 1}
			}
			if _, err := monthlysource.SaveFundingPage(ctx, db, page); err != nil {
				t.Fatal(err)
			}
			scans = append(scans, scan.ID)
		}
		o, err := monthlysource.ReviewFundingWindow(ctx, db, scans[0], scans[1])
		if err != nil {
			t.Fatal(err)
		}
		c := o.Comparison()
		p.Windows = append(p.Windows, monthlysource.FundingMonthWindowVersion{From: c.From, To: c.To, OlderScanID: c.OlderScanID, NewerScanID: c.NewerScanID, OlderHeadID: c.OlderHeadID, NewerHeadID: c.NewerHeadID})
		for _, user := range req.UserIDs {
			rows := []monthlysource.UsageWindowRow{}
			used := int64(0)
			if user == 201 {
				used = 500000
				if at == from {
					rows = []monthlysource.UsageWindowRow{{UserID: user, Type: 2, BillingSource: "wallet", Records: 1, Quota: 500000, MinQuota: 500000, MaxQuota: 500000, MinID: 2, MaxID: 2, FirstAt: from + 2, LastAt: from + 2}}
				}
			}
			balance := int64(500000)
			a := monthlysource.UsageWindowObservation{Version: 1, Basis: "database_view_wallet_and_usage_not_month_boundary", Request: monthlysource.UsageWindowRequest{SourceIdentity: req.SourceIdentity, Rules: rules, UserID: user, From: at, To: at + 86400}, StartedAtMS: collected * 1000, CompletedAtMS: collected*1000 + 1, Wallet: monthlysource.WalletUser{UserID: user, Exists: true, BalanceQuota: &balance, UsedQuota: &used}, Rows: rows}
			b := a
			b.StartedAtMS += 300001
			b.CompletedAtMS += 300001
			older, err := monthlysource.SaveUsageWindowObservation(ctx, db, a)
			if err != nil {
				t.Fatal(err)
			}
			newer, err := monthlysource.SaveUsageWindowObservation(ctx, db, b)
			if err != nil {
				t.Fatal(err)
			}
			p.UsageWindows = append(p.UsageWindows, monthlysource.UsageWindowVersion{OlderID: older, NewerID: newer})
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dbPath, 0600); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	input := monitor.MonthlyPreparationInput{Version: 1, Accounts: []monitor.MonthlyPreparationAccount{
		{UserID: 201, GroupID: 1, Username: "active-synthetic", Months: []string{req.Month}, CounterBasis: "net_consumption", BasisSource: "fixture#basis", Baseline: monitor.MonthlyPreparationBalance{UserID: 201, At: from, Source: "fixture#opening", TimeBasis: "source_log_cutoff", CutoffSource: "fixture#cutoff"}, Spans: []monitor.MonthlyPreparationSpan{}, Movements: []monitor.MonthlyPreparationMovement{{UserID: 201, ID: 1, At: from + 1, DeltaQuota: 1000000, Source: "fixture#topup"}}, MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: 201, From: from, To: to, Source: "fixture#funding"}}, Snapshots: []monitor.MonthlyPreparationBalance{{UserID: 201, At: to, BalanceQuota: 500000, UsedQuota: 500000, Source: "fixture#closing", TimeBasis: "source_log_cutoff", CutoffSource: "fixture#cutoff"}}},
		{UserID: 202, GroupID: 1, Username: "later-synthetic", Months: []string{req.Month}, CounterBasis: "net_consumption", BasisSource: "fixture#basis", Baseline: monitor.MonthlyPreparationBalance{UserID: 202, At: to, Source: "fixture#registration-origin"}, Spans: []monitor.MonthlyPreparationSpan{{UserID: 202, From: to, To: collected, Requests: &zero, Tokens: &zero, Source: "fixture#no-later-usage"}}, Movements: []monitor.MonthlyPreparationMovement{{UserID: 202, ID: 700, At: registered, DeltaQuota: 500000, Source: "original#/row"}}, MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: 202, From: to, To: collected, Source: "fixture#future-funding"}}, Snapshots: []monitor.MonthlyPreparationBalance{{UserID: 202, At: collected, BalanceQuota: 500000, Source: "fixture#later-wallet"}}},
	}}
	write := func(name string, value any) monitor.MonthlyPreparationSource {
		t.Helper()
		raw := materialPreparationJSON(t, value)
		file := filepath.Join(root, name+".json")
		if err := os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
		return monitor.MonthlyPreparationSource{ID: name, Path: file, SHA256: generationHash(raw)}
	}
	input.Sources = append(input.Sources, write("fixture", input.Accounts), write("original", map[string]any{"row": monthlysource.FundingLog{ID: 700, UserID: 202, CreatedAt: registered, Type: 4, Content: "新用户注册赠送 $1.000000 额度", Other: ""}}))
	body, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "observed", Path: dbPath, SHA256: generationHash(body)})
	receiverPath := filepath.Join(root, "receiver.sqlite")
	receiver, funding := generationDB(t, receiverPath, false), generationDB(t, dbPath, true)
	if err := InitGenerationReceiver(ctx, receiver); err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(receiver, funding, root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	f := generationFixture{g: g, receiver: receiver, funding: funding, root: root, receiverPath: receiverPath, path: dbPath, plan: p, input: input, sourceHash: generationHash(body)}
	usageInputUpdate(t, &f)
	addTokenArchivesFromObserved(t, &f)
	usage, err := g.PrepareUsageSpans(ctx, f.plan, f.raw, "observed")
	if err != nil {
		t.Fatal("pre-registration zero observations cannot be projected", err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: usage.Plan, Preparation: usage.Preparation})
	tokens, err := g.PrepareTokenUsageInput(ctx, f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: tokens.Plan, Preparation: tokens.Preparation})
	addRetainedScopeFixture(t, &f, func(d *RetainedAutomaticScope) {
		d.Scope.Accounts[1].Registration = &monitor.MonthlyBundleRegistration{At: registered, EventID: 700, Source: "original#/row", NoEarlierActivitySource: "fixture#reviewed-no-activity-before-registration"}
	})
	return f
}

func TestPreRegistrationActualGenerationKeepsLaterMemberAndFutureGift(t *testing.T) {
	f := preRegistrationGenerationFixture(t)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil || artifact.ScopeReview == nil {
		t.Fatal("source-backed pre-registration member blocked actual generation", err)
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata(t.Context(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := monitor.PrepareMonthlyCandidateBundle(f.raw, meta.BundleContext, time.Now())
	if err != nil || bundle.ReadyForReview != 1 || bundle.Blocked != 0 {
		t.Fatal("incomplete organization candidate", err)
	}
	u := bundle.Entries[0].Candidate.Users
	if len(u) != 2 || u[0].ConsumeQuota != 500000 || u[0].ClosingQuota != 500000 || u[1].UserID != 202 || u[1].OpeningQuota != 0 || u[1].ClosingQuota != 0 || u[1].ConsumeQuota != 0 || u[1].RefundQuota != 0 || len(u[1].Events) != 0 || u[1].TokenUsage == nil || len(u[1].TokenUsage.Rows) != 0 {
		t.Fatal("later member omitted, phantom money or future gift booked early")
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(*bundle.Entries[0].Candidate, time.Now().Unix(), time.Now()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		again, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
		if err != nil || !bytes.Equal(materialPreparationJSON(t, again), materialPreparationJSON(t, artifact)) {
			t.Fatal("immutable draft reuse failed", err)
		}
	}
	reopened, err := NewGeneration(f.receiver, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil || !bytes.Equal(materialPreparationJSON(t, again), materialPreparationJSON(t, artifact)) {
		t.Fatal("restart changed the original registration proof or money", err)
	}
	if err := os.WriteFile(filepath.Join(f.root, "original.json"), []byte(`{"row":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Run(t.Context(), job.Request.Owner, job.ID); err == nil {
		t.Fatal("ready cache bypassed a changed original registration source")
	}
}

func TestPreRegistrationHistorySelectionKeepsOriginalLaterOrigin(t *testing.T) {
	f := preRegistrationGenerationFixture(t)
	catalogue := draftCopy(t, f.input)
	later := &catalogue.Accounts[1]
	later.Months, later.TokenMonths = []string{"2026-10"}, nil
	write := func(name string, value any) *monthlysource.MonthlyObservationFile {
		t.Helper()
		body := materialPreparationJSON(t, value)
		file := filepath.Join(f.root, name+".json")
		if err := os.WriteFile(file, body, 0600); err != nil {
			t.Fatal(err)
		}
		return &monthlysource.MonthlyObservationFile{Path: file, SHA256: generationHash(body)}
	}
	roster := monthlysource.OrganizationMonthScope{Version: 1, Mode: monthlysource.OrganizationMonthScopeMode, GroupID: f.plan.Owner.GroupID, AuthVersion: f.plan.Owner.AuthVersion, MemberFingerprint: f.plan.Owner.MemberFingerprint, MemberManifest: f.plan.Owner.MemberManifest, Funding: f.plan.Funding}
	r := ObservationMaterialPreparationRequest{History: write("original-catalogue", catalogue), Organization: write("current-roster", roster)}
	scope, err := f.g.selectedRetainedScope(t.Context(), f.plan, f.input)
	if err != nil {
		t.Fatal(err)
	}
	selected := RetainedAutomaticScope{Version: 1, Owner: f.plan.Owner, Month: f.plan.Funding.Month, SourceIdentity: f.plan.Funding.SourceIdentity, Scope: scope}
	raw, err := f.g.organizationHistoryTemplate(t.Context(), r, monthlysource.MonthlyObservationMaterial{GroupID: f.plan.Owner.GroupID, Funding: f.plan.Funding}, selected)
	if err != nil {
		t.Fatal("later registered original member was dropped or blocked", err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil || len(input.Accounts) != 2 || input.Accounts[1].Months[0] != f.plan.Funding.Month {
		t.Fatal("wrong full organization scope", err)
	}
	got, want := input.Accounts[1], catalogue.Accounts[1]
	got.Months = want.Months
	if !bytes.Equal(materialPreparationJSON(t, got), materialPreparationJSON(t, want)) {
		t.Fatal("history selection invented a pre-origin span or changed future money")
	}
	selected.Scope.Accounts[1].Registration.NoEarlierActivitySource = ""
	if _, err := f.g.organizationHistoryTemplate(t.Context(), r, monthlysource.MonthlyObservationMaterial{GroupID: f.plan.Owner.GroupID, Funding: f.plan.Funding}, selected); err == nil {
		t.Fatal("missing early-activity proof silently became a zero member")
	}
}

func TestPreRegistrationGenerationRejectsMissingOrConflictingProof(t *testing.T) {
	for _, kind := range []string{"missing-registration", "missing-earlier-proof", "legacy-unselected-reference", "wrong-event-id", "nonzero-origin", "gift-in-previous-month", "gap-in-original-observations", "changed-original"} {
		t.Run(kind, func(t *testing.T) {
			f := preRegistrationGenerationFixture(t)
			path := filepath.Join(f.root, "automatic-scope.json")
			switch kind {
			case "missing-registration", "missing-earlier-proof", "legacy-unselected-reference", "wrong-event-id":
				rewriteReconstructionScopeFixture(t, &f, path, func(d *RetainedAutomaticScope) {
					r := d.Scope.Accounts[1].Registration
					switch kind {
					case "missing-registration":
						d.Scope.Accounts[1].Registration = nil
					case "missing-earlier-proof":
						r.NoEarlierActivitySource = ""
					case "legacy-unselected-reference":
						r.Source = "original#arbitrary-label-not-original-row-selector"
					case "wrong-event-id":
						r.EventID++
					}
				})
			case "nonzero-origin":
				f.input.Accounts[1].Baseline.BalanceQuota++
				usageInputUpdate(t, &f)
			case "gift-in-previous-month":
				f.input.Accounts[1].Movements[0].At = f.plan.Windows[0].From + 1
				usageInputUpdate(t, &f)
			case "gap-in-original-observations":
				f.plan.UsageWindows = append(f.plan.UsageWindows[:1:1], f.plan.UsageWindows[2:]...)
			case "changed-original":
				if err := os.WriteFile(filepath.Join(f.root, "original.json"), []byte(`{"row":{}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			job := generationSubmit(t, f)
			if _, err := f.g.Run(t.Context(), job.Request.Owner, job.ID); err == nil {
				t.Fatal("unproven/mismatched zero account reached a ready organization")
			}
		})
	}
}

func TestPreRegistrationRawReferenceCannotBeReplacedByReportJSON(t *testing.T) {
	f := preRegistrationGenerationFixture(t)
	view, release, err := f.g.bindObservationStore(t.Context(), f.plan, f.input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	scope, err := f.g.selectedRetainedScope(t.Context(), f.plan, f.input)
	if err != nil {
		t.Fatal(err)
	}
	windows, err := view.auditWindows(t.Context(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observedAutomaticContext(t.Context(), f.plan, f.raw, windows, scope); err == nil {
		t.Fatal("raw-source registration audit was bypassed by a scope/reference")
	}
	proof, err := f.g.auditPreRegistration(t.Context(), f.plan, f.raw, f.input, scope)
	if err != nil {
		t.Fatal(err)
	}
	scope.Group.Name += " changed after original selection"
	if _, err := observedAutomaticContext(t.Context(), f.plan, f.raw, windows, scope, proof); err == nil {
		t.Fatal("registration proof reused under a different source scope")
	}
}

func TestPreRegistrationEmptyObservationIsNotZeroPriceActivityOrMissing(t *testing.T) {
	a := monitor.MonthlyPreparationAccount{UserID: 202, Baseline: monitor.MonthlyPreparationBalance{At: 200}}
	o := monthlysource.UsageMonthAccountReport{UserID: 202, ObservedSeconds: 100, MatchingSeconds: 100, Totals: &monthlysource.UsageMonthTotals{}, Windows: []monthlysource.UsageMonthWindowReport{{From: 100, To: 200, Matching: true, Rows: []monthlysource.UsageWindowRow{}}}}
	if !emptyObservedBeforeOrigin(a, o, 200) {
		t.Fatal("explicit empty original range rejected")
	}
	o.Windows[0].Rows = []monthlysource.UsageWindowRow{{UserID: 202, Type: 2, BillingSource: "wallet", Quota: 0, Records: 1}}
	if emptyObservedBeforeOrigin(a, o, 200) {
		t.Fatal("zero-price genuine request was hidden as no prior activity")
	}
	o.Windows = nil
	if emptyObservedBeforeOrigin(a, o, 200) {
		t.Fatal("absent windows became an empty observed month")
	}
}

// Opt-in read-only check of retained ORIGINAL registration rows, not source
// capture, actual full-month generation or permission to publish a bill.
func TestPreRegistrationRetainedOriginalInventory(t *testing.T) {
	file, expected := os.Getenv("MONTHLY_REGISTRATION_ORIGINAL"), os.Getenv("MONTHLY_REGISTRATION_SHA256")
	if file == "" {
		t.Skip("explicit pinned private original inventory required")
	}
	body, err := os.ReadFile(file)
	if err != nil || generationHash(body) != expected {
		t.Fatal("original inventory differs from pinned source", err)
	}
	var inventory struct {
		Accounts []struct {
			UserID int64                      `json:"user_id"`
			First  []monthlysource.FundingLog `json:"first_funding"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal(body, &inventory); err != nil {
		t.Fatal(err)
	}
	count := 0
	for index, a := range inventory.Accounts {
		if a.UserID != 143 && a.UserID != 144 {
			continue
		}
		if len(a.First) != 1 {
			t.Fatal("registration selection requires exactly the retained first row")
		}
		l := a.First[0]
		pointer := "/accounts/" + strconv.Itoa(index) + "/first_funding/0"
		r := monitor.MonthlyBundleRegistration{At: l.CreatedAt, EventID: l.ID}
		e, err := registrationFromOriginal(body, pointer, a.UserID, r, monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000})
		if err != nil || e.DeltaQuota != 500000 || e.Kind != "gift" {
			t.Fatal("retained original registration rejected", a.UserID, err)
		}
		count++
	}
	if count != 2 {
		t.Fatal("required retained late-member sources missing")
	}
	after, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(body, after) {
		t.Fatal("original inventory changed", err)
	}
}
