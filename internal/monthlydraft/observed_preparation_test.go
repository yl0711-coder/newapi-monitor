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

func observedPreparationFixture(t *testing.T, scenario string) generationFixture {
	t.Helper()
	f := usagePreparationFixture(t, scenario)
	if err := InitGenerationSelections(context.Background(), f.receiver); err != nil {
		t.Fatal(err)
	}
	addWalletInputFixture(t, &f, "")
	clearObservedFundingFixture(t, &f)
	return f
}

func clearObservedFundingFixture(t *testing.T, f *generationFixture) {
	t.Helper()
	for i := range f.input.Accounts {
		f.input.Accounts[i].Movements = []monitor.MonthlyPreparationMovement{}
	}
	usageInputUpdate(t, f)
}

func TestObservedPreparationWholeInputGenerationAndHandoff(t *testing.T) {
	f := observedPreparationFixture(t, "")
	ctx := context.Background()
	original := bytes.Clone(f.raw)
	result, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, "observed_wallet")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, f.raw) || !bytes.Equal(original, draftJSON(t, f.input)) {
		t.Fatal("source template modified")
	}
	preparationCounts(t, f, 0, 0)
	again, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, "observed_wallet")
	if err != nil || !reflect.DeepEqual(result, again) {
		t.Fatal("original template not deterministic", err)
	}
	shuffled := f.plan
	shuffled.Windows = slices.Clone(shuffled.Windows)
	slices.Reverse(shuffled.Windows)
	shuffled.UsageWindows = slices.Clone(shuffled.UsageWindows)
	slices.Reverse(shuffled.UsageWindows)
	again, err = f.g.PrepareObservedInput(ctx, shuffled, f.raw, "observed_wallet")
	if err != nil || !reflect.DeepEqual(result, again) {
		t.Fatal("observation order changed money or identity", err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: result.Plan, Preparation: result.Preparation})
	for i, a := range f.input.Accounts {
		var old monitor.MonthlyPreparationInput
		if err := json.Unmarshal(original, &old); err != nil {
			t.Fatal(err)
		}
		if len(a.Spans) != 30 || len(a.Snapshots) != 3 {
			t.Fatal("usage or wallet preparation missing")
		}
		unchanged := a
		unchanged.Movements, unchanged.Spans = old.Accounts[i].Movements, old.Accounts[i].Spans
		unchanged.Snapshots = unchanged.Snapshots[:1]
		if !reflect.DeepEqual(unchanged, old.Accounts[i]) {
			t.Fatal("independent balance, coverage, history or refund evidence changed")
		}
	}
	a, b := f.input.Accounts[0], f.input.Accounts[1]
	if len(a.Movements) != 2 || len(b.Movements) != 1 || a.Movements[0].DeltaQuota != -400000000 ||
		a.Movements[1].DeltaQuota != 5000000 || b.Movements[0].DeltaQuota != 400000000 ||
		!strings.Contains(a.Movements[0].Source, "#funding:") {
		t.Fatal("transfer sides or credited top-up differ from original source")
	}
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || artifact.FundingSHA256 != result.FundingSHA256 || artifact.UsageSHA256 != result.UsageSHA256 || artifact.WalletSHA256 != result.WalletSHA256 {
		t.Fatal("combined input failed the existing independent audits", err)
	}
	assertGenerationAmounts(t, artifact, 1)
	load, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		return automaticScopeFixture(t, f), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := load(ctx, job.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta.BundleContext); err != nil {
		t.Fatal(err)
	}
	if out, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, "observed_wallet"); err == nil || !reflect.DeepEqual(out, ObservedPreparation{}) {
		t.Fatal("combined preparation overwrote existing money", err)
	}
	if raw, err := os.ReadFile(f.path); err != nil || generationHash(raw) != f.sourceHash {
		t.Fatal("observation database modified", err)
	}
}

func TestObservedPreparationFailureDoesNotReturnPartialOrStage(t *testing.T) {
	for _, scenario := range []string{"funding_head", "existing_money", "history_id_collision", "usage_unknown", "wallet_missing", "wallet_selection", "usage_selection", "wrong_source", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			usage := ""
			if scenario == "usage_unknown" {
				usage = "unknown"
			}
			f := observedPreparationFixture(t, usage)
			ctx := context.Background()
			source := "observed_wallet"
			switch scenario {
			case "funding_head":
				f.plan.Windows[0].NewerHeadID = strings.Repeat("a", 64)
			case "existing_money":
				f.input.Accounts[0].Movements = []monitor.MonthlyPreparationMovement{{UserID: 201, ID: 1, At: f.plan.Windows[0].From, DeltaQuota: -400000000, Source: "fixture#already-present"}}
				usageInputUpdate(t, &f)
			case "history_id_collision":
				f.input.Accounts[1].Movements = []monitor.MonthlyPreparationMovement{{UserID: 202, ID: 1, At: f.plan.Windows[0].From - 1, DeltaQuota: 1, Source: "fixture#conflicting-history"}}
				usageInputUpdate(t, &f)
			case "wallet_missing":
				f.plan.WalletObservations.IDs[0] = strings.Repeat("a", 64)
			case "wallet_selection":
				f.plan.WalletObservations = nil
			case "usage_selection":
				f.plan.UsageWindows = nil
			case "wrong_source":
				source = "observed"
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before := bytes.Clone(f.raw)
			result, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, source)
			if err == nil || !reflect.DeepEqual(result, ObservedPreparation{}) || !bytes.Equal(before, f.raw) {
				t.Fatal("failed preparation returned partial input or changed template", err)
			}
			if scenario == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation lost", err)
			}
			preparationCounts(t, f, 0, 0)
			if raw, err := os.ReadFile(f.path); err != nil || generationHash(raw) != f.sourceHash {
				t.Fatal("failed preparation changed original source", err)
			}
		})
	}
}

func TestObservedPreparationBusyTimeoutAndRecovery(t *testing.T) {
	f := observedPreparationFixture(t, "")
	ctx := context.Background()
	observedPreparationSlots <- struct{}{}
	out, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, "observed_wallet")
	<-observedPreparationSlots
	if !errors.Is(err, ErrGenerationBusy) || !reflect.DeepEqual(out, ObservedPreparation{}) {
		t.Fatal("busy preparation did not stop", err)
	}
	f.funding.SetMaxOpenConns(1)
	conn, err := f.funding.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	out, err = f.g.PrepareObservedInput(short, f.plan, f.raw, "observed_wallet")
	cancel()
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrGenerationEvidence) || !reflect.DeepEqual(out, ObservedPreparation{}) {
		t.Fatal("timeout became financial mismatch or partial output", err)
	}
	if out, err = f.g.PrepareObservedInput(ctx, f.plan, f.raw, "observed_wallet"); err != nil || len(out.Plan) == 0 {
		t.Fatal("timeout did not release preparation capacity", err)
	}
	preparationCounts(t, f, 0, 0)
}

func TestObservedPreparationDoesNotCreateIndependentProof(t *testing.T) {
	f := observedPreparationFixture(t, "")
	for i := range f.input.Accounts {
		a := &f.input.Accounts[i]
		a.WalletMonths = nil
		a.Baseline.TimeBasis, a.Baseline.CutoffSource = "collection_observation", ""
		for j := range a.Snapshots {
			a.Snapshots[j].TimeBasis, a.Snapshots[j].CutoffSource = "collection_observation", ""
		}
	}
	usageInputUpdate(t, &f)
	out, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet")
	if err != nil {
		t.Fatal("source projection should not itself assert financial readiness", err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: out.Plan, Preparation: out.Preparation})
	job := generationSubmit(t, f)
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("automatic projection invented month boundaries or actual refunds", err)
	}
}

func TestObservedPreparationFundingProjectionEmptyAndHistory(t *testing.T) {
	ctx := context.Background()
	before := monitor.MonthlyPreparationMovement{UserID: 201, ID: 90, At: 9, DeltaQuota: -2, Source: "independent#prior"}
	after := monitor.MonthlyPreparationMovement{UserID: 201, ID: 91, At: 20, DeltaQuota: 3, Source: "independent#bridge"}
	input := monitor.MonthlyPreparationInput{Accounts: []monitor.MonthlyPreparationAccount{{UserID: 201, Movements: []monitor.MonthlyPreparationMovement{after, before}}, {UserID: 202}}}
	review := monthlysource.FundingMoneyReview{Windows: monthlysource.FundingMonthReview{From: 10, To: 20}, Accounts: []monthlysource.FundingMoneyAccount{
		{UserID: 201, Status: "observed_month", Totals: &monthlysource.FundingMoneyTotals{}, Movements: []monthlysource.FundingObservedMovement{{LogID: 1, At: 10, DeltaQuota: -9007199254740993}}},
		{UserID: 202, Status: "observed_month", Totals: &monthlysource.FundingMoneyTotals{}},
	}}
	if err := projectObservedFunding(ctx, &input, review, "observed"); err != nil {
		t.Fatal(err)
	}
	m := input.Accounts[0].Movements
	if len(m) != 3 || m[0] != before || m[2] != after || m[1].DeltaQuota != -9007199254740993 || input.Accounts[1].Movements == nil || len(input.Accounts[1].Movements) != 0 {
		t.Fatal("historical money, precision or explicit empty month changed")
	}
	input.Accounts[0].Movements = []monitor.MonthlyPreparationMovement{}
	review.Accounts[1].Totals, review.Accounts[1].Status = nil, "incomplete_observation"
	if err := projectObservedFunding(ctx, &input, review, "observed"); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("unknown funding month turned into empty month", err)
	}
}
