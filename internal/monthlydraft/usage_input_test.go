package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Only synthetic, temporary source receivers are opened writable here. The
// Generation retains its existing mode=ro handle; there are no source writes
// in the business adapter. Every later read must leave these bytes unchanged.
func addUsageInputFixture(t *testing.T, f *generationFixture, scenario string) {
	t.Helper()
	ctx := context.Background()
	db := generationDB(t, f.path, false)
	if err := monthlysource.InitUsageWindowStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	r := f.plan.Funding
	from, to := f.plan.Windows[0].From, f.plan.Windows[len(f.plan.Windows)-1].To
	for _, user := range r.UserIDs {
		for at := from; at < to; at += 86400 {
			rows := []monthlysource.UsageWindowRow{}
			if at == from {
				for i, kind := range []int{2, 6} {
					quota := int64(1500000)
					if kind == 6 {
						quota = 500000
					}
					rows = append(rows, monthlysource.UsageWindowRow{UserID: user, Type: kind, BillingSource: "wallet", Records: 1, Quota: quota,
						MinQuota: quota, MaxQuota: quota, MinID: user*10 + int64(i), MaxID: user*10 + int64(i), FirstAt: at + 1, LastAt: at + 1})
				}
			}
			if user == 201 && at == from && (scenario == "unknown" || scenario == "subscription") {
				rows[0].BillingSource = "unspecified"
				if scenario == "subscription" {
					rows[0].BillingSource = "subscription"
				}
			}
			if at == from && scenario == "precision" {
				for i := range rows {
					rows[i].Quota = 9007199254740993 + int64(i)*2
					rows[i].MinQuota, rows[i].MaxQuota = rows[i].Quota, rows[i].Quota
				}
			}
			balance, used := int64(500000000), int64(1001000000)
			a := monthlysource.UsageWindowObservation{Version: 1, Basis: "database_view_wallet_and_usage_not_month_boundary",
				Request:     monthlysource.UsageWindowRequest{SourceIdentity: r.SourceIdentity, Rules: r.Rules, UserID: user, From: at, To: at + 86400},
				StartedAtMS: (to + 86400) * 1000, CompletedAtMS: (to+86400)*1000 + 1,
				Wallet: monthlysource.WalletUser{UserID: user, Exists: true, BalanceQuota: &balance, UsedQuota: &used}, Rows: rows}
			b := a
			b.StartedAtMS += 300001
			b.CompletedAtMS += 300001
			b.Rows = slices.Clone(a.Rows)
			if user == 201 && at == from {
				switch scenario {
				case "changed":
					b.Rows[0].Quota++
					b.Rows[0].MinQuota++
					b.Rows[0].MaxQuota++
				case "missing_account":
					b.Wallet = monthlysource.WalletUser{UserID: user}
				case "wrong_account":
					b.Request.UserID = 999
					b.Wallet.UserID = 999
					for i := range b.Rows {
						b.Rows[i].UserID = 999
					}
				}
			}
			oldID, err := monthlysource.SaveUsageWindowObservation(ctx, db, a)
			if err != nil {
				t.Fatal(err)
			}
			newID, err := monthlysource.SaveUsageWindowObservation(ctx, db, b)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "gap" && user == 201 && at == from {
				continue
			}
			f.plan.UsageWindows = append(f.plan.UsageWindows, monthlysource.UsageWindowVersion{OlderID: oldID, NewerID: newID})
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.sourceHash = generationHash(raw)
}

func usageInputUpdate(t *testing.T, f *generationFixture) {
	t.Helper()
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
}

func TestUsageInputPinnedPlanReadOnlyDraftReuseAndContext(t *testing.T) {
	f := generationFixtureNew(t)
	legacyPlan, legacyRaw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	addUsageInputFixture(t, &f, "")
	p, raw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil || generationHash(raw) == generationHash(legacyRaw) || generationRequest(p, raw) == generationRequest(legacyPlan, legacyRaw) {
		t.Fatal("usage captures did not change task identity", err)
	}
	reversed := f.plan
	reversed.UsageWindows = slices.Clone(reversed.UsageWindows)
	slices.Reverse(reversed.UsageWindows)
	if _, again, _, err := canonicalGeneration(reversed, f.raw, time.Now()); err != nil || !bytes.Equal(raw, again) {
		t.Fatal("capture order changed canonical identity", err)
	}
	before := draftJSON(t, f.input)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	if !generationHex(artifact.UsageSHA256, 32) || !bytes.Equal(before, draftJSON(t, f.input)) {
		t.Fatal("usage report not pinned, or normalized money changed")
	}
	f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		t.Fatal("cached draft recalculated")
		return Review{}, nil
	}
	if again, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); err != nil || !reflect.DeepEqual(again, artifact) {
		t.Fatal("stable usage draft not reused", err)
	}
	load, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		return automaticScopeFixture(t, f), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := load(context.Background(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.PrepareReviewHandoff(context.Background(), job.Request.Owner, job.ID, meta.BundleContext); err != nil {
		t.Fatal("audited usage did not reach candidate handoff", err)
	}
	if source, err := os.ReadFile(f.path); err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("adapter changed retained sources", err)
	}
}

func TestUsageInputRefusesMismatchWithoutPartialDraftOrPublication(t *testing.T) {
	for _, scenario := range []string{"quota", "same_net", "requests", "wrong_account", "gap", "changed", "missing_account", "unknown", "subscription", "time_shift", "duplicate_span", "window_split"} {
		t.Run(scenario, func(t *testing.T) {
			f := generationFixtureNew(t)
			addUsageInputFixture(t, &f, scenario)
			a := &f.input.Accounts[0]
			switch scenario {
			case "quota":
				a.Spans[0].ConsumeQuota++
			case "same_net":
				a.Spans[0].ConsumeQuota++
				a.Spans[0].RefundQuota++
			case "requests":
				wrong := int64(2)
				a.Spans[0].Requests = &wrong
			case "time_shift", "window_split":
				first, second := a.Spans[0], a.Spans[0]
				first.To = first.From + 86400
				if scenario == "window_split" {
					first.To--
				}
				second.From = first.To
				if scenario == "time_shift" {
					first.ConsumeQuota, first.RefundQuota = 0, 0
				} else {
					second.ConsumeQuota, second.RefundQuota = 0, 0
				}
				a.Spans = []monitor.MonthlyPreparationSpan{first, second}
			case "duplicate_span":
				a.Spans = append(a.Spans, a.Spans[0])
			}
			usageInputUpdate(t, &f)
			job := generationSubmit(t, f)
			var calculations, publishes int
			calculate := f.g.calculate
			f.g.calculate = func(ctx context.Context, req monthlysource.FundingMonthRequest, windows []monthlysource.FundingWindowObservation, input []byte, now time.Time) (Review, error) {
				calculations++
				return calculate(ctx, req, windows, input, now)
			}
			metadata, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
				return automaticScopeFixture(t, f), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if out, err := metadata(context.Background(), job.Request); err == nil || len(out.BundleContext) != 0 {
				t.Fatal("metadata bypassed consumption audit", err)
			}
			worker, err := NewAutomaticLocalWorker(f.g, metadata, func(context.Context, monthlytasks.Request, []byte) error { publishes++; return nil })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := worker.step(context.Background()); err != nil {
				t.Fatal(err)
			}
			var artifacts int
			if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_generation_inputs WHERE artifact IS NOT NULL`).Scan(&artifacts); err != nil || artifacts != 0 || calculations != 0 || publishes != 0 {
				t.Fatal("bad usage calculated, cached or published", calculations, artifacts, publishes, err)
			}
			if source, err := os.ReadFile(f.path); err != nil || generationHash(source) != f.sourceHash {
				t.Fatal("failed audit changed sources", err)
			}
		})
	}
}

func TestUsageInputPlanValidationAndLegacyCompatibility(t *testing.T) {
	f := generationFixtureNew(t)
	_, legacy, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil || bytes.Contains(legacy, []byte("usage_windows")) {
		t.Fatal("legacy canonical bytes changed", err)
	}
	if result, err := f.g.auditUsageInput(context.Background(), f.plan, f.input); err != nil || result != "" {
		t.Fatal("legacy plan required an uninitialized capture store", err)
	}
	for _, scenario := range []string{"malformed", "uppercase", "same_pair", "reuse", "too_many"} {
		p := f.plan
		p.UsageWindows = []monthlysource.UsageWindowVersion{{OlderID: strings.Repeat("a", 64), NewerID: strings.Repeat("b", 64)}}
		switch scenario {
		case "malformed":
			p.UsageWindows[0].OlderID = "bad"
		case "uppercase":
			p.UsageWindows[0].OlderID = strings.Repeat("A", 64)
		case "same_pair":
			p.UsageWindows[0].NewerID = p.UsageWindows[0].OlderID
		case "reuse":
			p.UsageWindows = append(p.UsageWindows, p.UsageWindows[0])
		case "too_many":
			p.UsageWindows = make([]monthlysource.UsageWindowVersion, 4097)
		}
		if _, raw, _, err := canonicalGeneration(p, f.raw, time.Now()); err == nil || raw != nil {
			t.Fatal("invalid capture selection accepted", scenario, err)
		}
	}
	addUsageInputFixture(t, &f, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.g.auditUsageInput(ctx, f.plan, f.input); !errors.Is(err, context.Canceled) || errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("transient cancellation classified as proven bad money", err)
	}
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, planRaw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	artifact.UsageSHA256 = ""
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGenerationArtifact(generationRetained{plan: f.plan, planRaw: planRaw, artifact: raw}); err == nil {
		t.Fatal("cached artifact lost required usage audit digest")
	}
}

func TestUsageInputCachedDraftReopenDamageAndTimeout(t *testing.T) {
	f := generationFixtureNew(t)
	addUsageInputFixture(t, &f, "")
	ctx := context.Background()
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewGeneration(f.receiver, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		t.Fatal("restart recalculated frozen money")
		return Review{}, nil
	}
	if got, err := reopened.Run(ctx, job.Request.Owner, job.ID); err != nil || !reflect.DeepEqual(got, artifact) {
		t.Fatal("restart did not re-audit/reuse exact frozen draft", err)
	}
	f.funding.SetMaxOpenConns(1)
	conn, err := f.funding.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	_, err = f.g.auditUsageInput(short, f.plan, f.input)
	cancel()
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("source pool timeout was changed into permanent bad-money evidence", err)
	}
	if _, err := f.g.auditUsageInput(ctx, f.plan, f.input); err != nil {
		t.Fatal("source audit did not recover", err)
	}
	damaged := generationDB(t, f.path, false)
	if _, err := damaged.Exec(`DROP TRIGGER monthly_usage_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := damaged.Exec(`UPDATE portal_monthly_usage_observations SET payload='{}' WHERE id=?`, f.plan.UsageWindows[0].NewerID); err != nil {
		t.Fatal(err)
	}
	if err := damaged.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Run(ctx, job.Request.Owner, job.ID); err == nil || len(got.Reconciliation.Accounts) != 0 {
		t.Fatal("cached draft bypassed damaged capture", err)
	}
	load, err := NewObservedAutomaticContext(reopened, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		t.Fatal("scope loaded before failed source audit")
		return AutomaticScope{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta, err := load(ctx, job.Request); err == nil || len(meta.BundleContext) != 0 {
		t.Fatal("damaged capture reached automatic context", err)
	}
	if handoff, err := reopened.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, handoffContext(t, f)); err == nil || len(handoff.Bundle) != 0 {
		t.Fatal("handoff bypassed damaged capture", err)
	}
	var stored []byte
	if err := f.receiver.QueryRow(`SELECT artifact FROM portal_monthly_generation_inputs WHERE id=?`, job.Request.InputSHA256).Scan(&stored); err != nil {
		t.Fatal("stored artifact changed", err)
	}
	expected, err := json.Marshal(artifact)
	if err != nil || !bytes.Equal(stored, expected) {
		t.Fatal("failed re-audit overwrote cached money", err)
	}
	after, err := os.ReadFile(f.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed audit repaired or wrote damaged source", err)
	}
}

func TestUsageInputPreparationChecksBeforeSelecting(t *testing.T) {
	f, cfg := preparationFixture(t)
	addUsageInputFixture(t, &f, "")
	ctx := context.Background()
	files := []string{cfg.ReceiverPath, cfg.FundingPath, cfg.AuthorityPath}
	before := map[string]string{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	preview, err := PrepareGenerationInput(ctx, cfg, mustGenerationPlan(t, f), f.raw, "", false)
	if err != nil || preview.Applied || preview.Changed {
		t.Fatal("valid usage preparation inspection failed", err)
	}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != before[path] {
			t.Fatal("usage inspection changed a source or receiver", path, err)
		}
	}
	// A correct net does not make a different pair of gross/refund logs valid.
	f.input.Accounts[0].Spans[0].ConsumeQuota++
	f.input.Accounts[0].Spans[0].RefundQuota++
	usageInputUpdate(t, &f)
	for _, apply := range []bool{false, true} {
		if _, err := PrepareGenerationInput(ctx, cfg, mustGenerationPlan(t, f), f.raw, "", apply); !errors.Is(err, ErrGenerationEvidence) {
			t.Fatal("wrong usage reached selection/application", apply, err)
		}
		preparationCounts(t, f, 0, 0)
	}
	for _, path := range files[1:] {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != before[path] {
			t.Fatal("failed preparation changed source", path, err)
		}
	}
}

func TestUsageInputAlignedSpansAndUnknownStatistics(t *testing.T) {
	f := generationFixtureNew(t)
	addUsageInputFixture(t, &f, "")
	for i := range f.input.Accounts {
		a := &f.input.Accounts[i]
		first, rest := a.Spans[0], a.Spans[0]
		first.To = first.From + 86400
		rest.From = first.To
		rest.ConsumeQuota, rest.RefundQuota = 0, 0
		once, zero := int64(1), int64(0)
		first.Requests, rest.Requests = &once, &zero
		a.Spans = []monitor.MonthlyPreparationSpan{rest, first} // sorted privately
	}
	usageInputUpdate(t, &f)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artifact.Reconciliation.Accounts {
		row := a.Months[0]
		if row.Amounts == nil || row.Amounts.Consume != "3.000000" || row.Amounts.Refund != "1.000000" || row.Requests == nil || *row.Requests != 1 || row.Tokens != nil {
			t.Fatal("aligned money, consumption-only counts or unknown tokens changed", row)
		}
	}
	if f.input.Accounts[0].Spans[0].From == f.input.Accounts[0].Spans[1].From || f.input.Accounts[0].Spans[0].From < f.input.Accounts[0].Spans[1].From {
		t.Fatal("audit reordered caller's input")
	}
}
