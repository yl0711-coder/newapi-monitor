package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func usagePreparationFixture(t *testing.T, scenario string) generationFixture {
	t.Helper()
	f := generationFixtureNew(t)
	addUsageInputFixture(t, &f, scenario)
	sealUsagePreparationFixture(t, &f)
	return f
}

func sealUsagePreparationFixture(t *testing.T, f *generationFixture) {
	t.Helper()
	// This is a closed synthetic copy, never a backup or write of a live DB.
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.path = filepath.Join(f.root, "sealed-observations.sqlite")
	if err := os.WriteFile(f.path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.funding = generationDB(t, f.path, true)
	f.g.funding = f.funding
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: "observed", Path: f.path, SHA256: generationHash(raw)})
	for i := range f.input.Accounts {
		f.input.Accounts[i].Spans = []monitor.MonthlyPreparationSpan{}
	}
	usageInputUpdate(t, f)
}

func TestUsagePreparationMachineSpansToGeneration(t *testing.T) {
	f := usagePreparationFixture(t, "")
	ctx := context.Background()
	before := bytes.Clone(f.raw)
	result, err := f.g.PrepareUsageSpans(ctx, f.plan, f.raw, "observed")
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(result.Preparation)
	if err != nil || !generationHex(result.UsageSHA256, 32) {
		t.Fatal("machine input or report digest invalid", err)
	}
	if !bytes.Equal(f.raw, before) || !bytes.Equal(draftJSON(t, f.input), before) {
		t.Fatal("caller template mutated")
	}
	var count int
	if err := f.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_generation_inputs").Scan(&count); err != nil || count != 0 {
		t.Fatal("preparation retained a plan or created a task", err)
	}
	var plan GenerationPlan
	if err := json.Unmarshal(result.Plan, &plan); err != nil || plan.PreparationSHA256 != generationHash(result.Preparation) {
		t.Fatal("generated plan not pinned to exact bytes", err)
	}
	for i, a := range input.Accounts {
		if len(a.Spans) != 30 {
			t.Fatal("did not generate full daily coverage")
		}
		for day, s := range a.Spans {
			consume, refund, requests := int64(0), int64(0), int64(0)
			if day == 0 {
				consume, refund, requests = 1500000, 500000, 1
			}
			if s.UserID != a.UserID || s.ConsumeQuota != consume || s.RefundQuota != refund || s.Requests == nil || *s.Requests != requests || s.Tokens != nil {
				t.Fatal("machine generation changed amounts, account or unknown tokens")
			}
		}
		independent := a
		independent.Spans = []monitor.MonthlyPreparationSpan{}
		if !reflect.DeepEqual(independent, f.input.Accounts[i]) {
			t.Fatal("balance, funding, actual refunds or independent scope mutated")
		}
	}
	shuffled := f.plan
	shuffled.UsageWindows = slices.Clone(shuffled.UsageWindows)
	slices.Reverse(shuffled.UsageWindows)
	again, err := f.g.PrepareUsageSpans(ctx, shuffled, f.raw, "observed")
	if err != nil || !reflect.DeepEqual(result, again) {
		t.Fatal("same observations did not produce identical canonical bytes", err)
	}
	// Produced inputs take the real existing draft/handoff path, not a second
	// calculator with weaker validation. No manual usage amounts are supplied.
	job, err := f.g.Submit(ctx, plan, result.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || artifact.UsageSHA256 != result.UsageSHA256 {
		t.Fatal("machine input did not reach audited draft", err)
	}
	for _, a := range artifact.Reconciliation.Accounts {
		m := a.Months[0]
		if m.Amounts == nil || m.Amounts.Consume != "3.000000" || m.Amounts.Refund != "1.000000" || m.Requests == nil || *m.Requests != 1 || m.Tokens != nil {
			t.Fatal("generated bill money/counts differ from source")
		}
	}
	f.input, f.raw, f.plan = input, result.Preparation, plan
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
		t.Fatal("generated input failed unchanged independent evidence gate", err)
	}
	if raw, err := os.ReadFile(f.path); err != nil || generationHash(raw) != f.sourceHash {
		t.Fatal("preparation wrote source database", err)
	}
}

func TestUsagePreparationTimeoutReopenAndRefusesOverwrite(t *testing.T) {
	f := usagePreparationFixture(t, "")
	ctx := context.Background()
	f.funding.SetMaxOpenConns(1)
	conn, err := f.funding.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	result, err := f.g.PrepareUsageSpans(short, f.plan, f.raw, "observed")
	cancel()
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrGenerationEvidence) || !reflect.DeepEqual(result, UsagePreparation{}) {
		t.Fatal("timeout returned partial output or was classified as bad money", err)
	}
	first, err := f.g.PrepareUsageSpans(ctx, f.plan, f.raw, "observed")
	if err != nil {
		t.Fatal("preparation did not recover", err)
	}
	reopened, err := NewGeneration(f.receiver, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.PrepareUsageSpans(ctx, f.plan, f.raw, "observed")
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("reopening changed generated input", err)
	}
	var p GenerationPlan
	if err := json.Unmarshal(first.Plan, &p); err != nil {
		t.Fatal(err)
	}
	if output, err := reopened.PrepareUsageSpans(ctx, p, first.Preparation, "observed"); !errors.Is(err, ErrGenerationInput) || !reflect.DeepEqual(output, UsagePreparation{}) {
		t.Fatal("filled monthly money silently overwritten", err)
	}
}

func TestUsagePreparationPreservesLargeIntegerMoney(t *testing.T) {
	f := usagePreparationFixture(t, "precision")
	result, err := f.g.PrepareUsageSpans(context.Background(), f.plan, f.raw, "observed")
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(result.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range input.Accounts {
		if a.Spans[0].ConsumeQuota != 9007199254740993 || a.Spans[0].RefundQuota != 9007199254740995 {
			t.Fatal("integer money passed through floating-point or netted together")
		}
	}
	if !bytes.Contains(result.Preparation, []byte(`"consume_quota":9007199254740993`)) || !bytes.Contains(result.Preparation, []byte(`"refund_quota":9007199254740995`)) {
		t.Fatal("serialized integer precision lost")
	}
}

func TestUsagePreparationRefusesPartialAndAmbiguousEvidence(t *testing.T) {
	for _, scenario := range []string{"gap", "changed", "missing_account", "unknown", "subscription", "existing", "cross_month", "wrong_source", "wrong_digest", "wrong_file", "hardlink_alias", "wal", "shm", "journal", "no_versions", "wrong_user", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f := usagePreparationFixture(t, scenario)
			id := "observed"
			ctx := context.Background()
			switch scenario {
			case "existing", "cross_month":
				from := f.plan.Windows[0].From
				if scenario == "cross_month" {
					from--
				}
				f.input.Accounts[0].Spans = []monitor.MonthlyPreparationSpan{{UserID: 201, From: from, To: from + 3600, Source: "fixture#usage"}}
			case "wrong_source":
				id = "missing"
			case "wrong_digest":
				f.input.Sources[1].SHA256 = generationHash([]byte("not-the-source"))
			case "wrong_file":
				f.input.Sources[1] = f.input.Sources[0]
				f.input.Sources[1].ID = "observed"
			case "hardlink_alias":
				alias := filepath.Join(f.root, "alias.sqlite")
				if err := os.Link(f.path, alias); err != nil {
					t.Fatal(err)
				}
				f.input.Sources[1].Path = alias
			case "wal", "shm", "journal":
				if err := os.WriteFile(f.path+"-"+scenario, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "no_versions":
				f.plan.UsageWindows = nil
			case "wrong_user":
				f.input.Accounts[1].UserID = 201
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			usageInputUpdate(t, &f)
			before := bytes.Clone(f.raw)
			result, err := f.g.PrepareUsageSpans(ctx, f.plan, f.raw, id)
			if err == nil || !reflect.DeepEqual(result, UsagePreparation{}) || !bytes.Equal(before, f.raw) {
				t.Fatal("failure returned partial output or changed template", err)
			}
			if scenario == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation classified as bad money", err)
			}
			var n int
			if err := f.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_generation_inputs").Scan(&n); err != nil || n != 0 {
				t.Fatal("preparation staged a task on failure", n, err)
			}
		})
	}
}

func TestUsagePreparationPreservesIndependentBridgeAndFailsClosed(t *testing.T) {
	f := usagePreparationFixture(t, "")
	from := f.plan.Windows[0].From
	for i := range f.input.Accounts {
		a := &f.input.Accounts[i]
		a.Baseline.At = from - 3600
		a.Spans = []monitor.MonthlyPreparationSpan{{UserID: a.UserID, From: from - 3600, To: from, Source: "fixture#independent-history"}}
	}
	usageInputUpdate(t, &f)
	result, err := f.g.PrepareUsageSpans(context.Background(), f.plan, f.raw, "observed")
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(result.Preparation)
	if err != nil || !reflect.DeepEqual(input.Accounts[0].Spans[0], f.input.Accounts[0].Spans[0]) {
		t.Fatal("historical span changed", err)
	}
	var p GenerationPlan
	if err := json.Unmarshal(result.Plan, &p); err != nil {
		t.Fatal(err)
	}
	// Successful usage projection is NOT financial approval: this bridge has
	// no funding coverage and must still fail the existing reconciliation gate.
	windows, err := f.g.auditWindows(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	review, err := ReconcileObservedMonth(context.Background(), p.Funding, windows, result.Preparation, time.Now())
	if err == nil && generationAllAccounts(p, review.Reconciliation) {
		t.Fatal("usage projection invented missing independent history")
	}
	if _, ok := usagePreparationAdd(1<<63-1, 1); ok {
		t.Fatal("integer overflow accepted")
	}
	if _, ok := usagePreparationAdd(0, -1); ok {
		t.Fatal("negative amount accepted")
	}
}
