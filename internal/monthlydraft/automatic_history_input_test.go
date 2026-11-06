package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestAutomaticHistoryInputNormalizesOriginalsWithoutManualHistory(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	before, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.sources.History.Path, f.sources.History.Path+".unused"); err != nil {
		t.Fatal(err)
	}
	got, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(got.observed.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Accounts) != 2 || !reflect.DeepEqual(got.resolutionUsers, []int64{201, 202}) {
		t.Fatal("lost per-user origin residuals", got.resolutionUsers)
	}
	for _, a := range input.Accounts {
		if a.Baseline.BalanceQuota != 1000000 || a.Baseline.UsedQuota != 0 || a.Baseline.TimeBasis != "" || a.Baseline.CutoffSource != "" || a.Username != "" || len(a.WalletMonths) != 0 {
			t.Fatal("arithmetic origin claimed as a settled snapshot/directory", a)
		}
		var consume, refund int64
		for _, s := range a.Spans {
			consume += s.ConsumeQuota
			refund += s.RefundQuota
		}
		if consume != 1500000 || refund != 500000 || len(a.TokenMonths) != 1 || len(a.Snapshots) != 2 {
			t.Fatal("source normalizers lost financial detail", a)
		}
		for _, s := range a.Snapshots {
			if s.TimeBasis != "collection_observation" || s.CutoffSource != "" {
				t.Fatal("collection observation retimed", s)
			}
		}
	}
	if got.observed.TokenSHA256 == "" || got.observed.UsageSHA256 == "" || got.observed.WalletSHA256 == "" || got.observed.FundingSHA256 == "" {
		t.Fatal("normalizer audit omitted")
	}
	// Reconstructed producer, not cached diagnostics; exact prepared bytes repeat.
	g := *f.f.g
	again, err := g.prepareAutomaticHistoryInput(t.Context(), ref, f.req)
	if err != nil || !bytes.Equal(got.observed.Plan, again.observed.Plan) || !bytes.Equal(got.observed.Preparation, again.observed.Preparation) {
		t.Fatal("replay changed input", err)
	}
	if _, err := json.Marshal(got); err == nil {
		t.Fatal("intermediate serialized as financial approval")
	}
	if err := json.Unmarshal([]byte(`{}`), &got); err == nil || len(got.observed.Preparation) != 0 {
		t.Fatal("JSON retained old intermediate")
	}
	if raw, err := os.ReadFile(f.sources.History.Path + ".unused"); err != nil || !bytes.Equal(raw, before) {
		t.Fatal("manual history modified", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
	if f.f.receiver.Stats().InUse != 0 || f.wallet.funding.capture.Stats().InUse != 0 {
		t.Fatal("connection retained")
	}
}

func TestAutomaticHistoryInputRequiresFreshOriginalAudit(t *testing.T) {
	f, files, ref := automaticHistoryOriginalFixture(t)
	if _, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, f.req); err != nil {
		t.Fatal(err)
	}
	var m monthlysource.MonthlyObservationMaterial
	raw, err := os.ReadFile(files.Material.Path)
	if err != nil || json.Unmarshal(raw, &m) != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m.Tokens[0].File.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, f.req)
	if err == nil || len(got.observed.Preparation) != 0 || len(got.resolutionUsers) != 0 {
		t.Fatal("cached report bypassed original damage", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryInputRefusesScopeChangesAndCancellation(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	for _, mutate := range []func(*SourcePreparationRequest){
		func(r *SourcePreparationRequest) { r.Month = "2026-08" },
		func(r *SourcePreparationRequest) { r.Owner.AuthVersion++ },
		func(r *SourcePreparationRequest) { r.Owner.GroupID++ },
	} {
		req := f.req
		mutate(&req)
		got, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, req)
		if err == nil || len(got.observed.Preparation) != 0 {
			t.Fatal("changed scope accepted", req, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.f.g.prepareAutomaticHistoryInput(ctx, ref, f.req); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	if _, _, _, err := projectAutomaticHistoryInput(t.Context(), auditedAutomaticHistory{}, f.req); err == nil {
		t.Fatal("empty report accepted")
	}
}

func TestAutomaticHistoryInputRejectsChangedNormalizedHistory(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	got, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*monitor.MonthlyPreparationInput){
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].Baseline.BalanceQuota++ },
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].Spans[0].ConsumeQuota++ },
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].MovementCoverage = nil },
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].Username = "invented" },
		func(i *monitor.MonthlyPreparationInput) { i.Sources[0].SHA256 = i.Sources[1].SHA256 },
	} {
		input, err := monitor.DecodeMonthlyPreparationInput(got.observed.Preparation)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&input)
		bad := got.observed
		bad.Preparation = materialPreparationJSON(t, input)
		var p GenerationPlan
		if json.Unmarshal(bad.Plan, &p) != nil {
			t.Fatal("plan")
		}
		p.PreparationSHA256 = generationHash(bad.Preparation)
		bad.Plan = materialPreparationJSON(t, p)
		if err := verifyAutomaticHistoryInput(t.Context(), bad, got.originals, f.req); err == nil {
			t.Fatal("changed normalized history accepted")
		}
	}
}

func TestAutomaticHistoryInputPreservesEarlierAndLaterMonths(t *testing.T) {
	f, boundary, _ := automaticHistoryOriginalFixture(t, "2026-07")
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	files := []CapturedSourceFiles{}
	for _, month := range []string{"2026-07", "2026-08"} {
		req := SourcePreparationRequest{Owner: f.req.Owner, Month: month}
		seedHistoryEmptyMonth(t, f, req, false)
		if _, _, err := collectCapturedSources(t.Context(), p.raw, p.core, req); err != nil {
			t.Fatal(err)
		}
		file, sealed, err := collectCapturedSources(t.Context(), p.raw, p.core, req)
		if err != nil || !sealed {
			t.Fatal("raw dependency not sealed", err)
		}
		files = append(files, file)
	}
	files = append(files, boundary)
	ref := writeAutomaticHistoryManifest(t, f.f.root, automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: f.req.Owner,
		SourceIdentity: f.wallet.funding.sourceIdentity, Boundary: boundary, Months: files})
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	queries := state.queries.Load()
	for _, month := range []string{"2026-07", "2026-08", "2026-09"} {
		req := SourcePreparationRequest{Owner: f.req.Owner, Month: month}
		got, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, req)
		if err != nil {
			t.Fatal(month, err)
		}
		input, err := monitor.DecodeMonthlyPreparationInput(got.observed.Preparation)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range input.Accounts {
			var consume, refund int64
			for _, span := range a.Spans {
				consume += span.ConsumeQuota
				refund += span.RefundQuota
			}
			if consume != 1500000 || refund != 500000 || len(a.TokenMonths) != 1 || a.TokenMonths[0].Month != month || a.Baseline.BalanceQuota != 1000000 {
				t.Fatal("same originals changed charge/carry across requested months", month, a)
			}
			for _, span := range a.Spans {
				if span.Source == "" {
					t.Fatal("lost original reference")
				}
			}
		}
	}
	if state.queries.Load() != queries || state.writes.Load() != 0 {
		t.Fatal("local projection queried/modified source")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryInputRespectsExistingEvidenceCapacity(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	audited, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	for len(audited.sources) < 513 {
		audited.sources = append(audited.sources, audited.sources[0])
	}
	if _, raw, users, err := projectAutomaticHistoryInput(t.Context(), audited, f.req); err == nil || len(raw) != 0 || len(users) != 0 {
		t.Fatal("evidence budget bypassed")
	}
}

func TestAutomaticHistoryInputKeepsFundingAndTransfersPerAccount(t *testing.T) {
	f, boundary, _ := automaticHistoryOriginalFixture(t, "2026-08")
	req := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	start, _ := time.ParseInLocation("2006-01", req.Month, time.FixedZone("UTC+8", 8*3600))
	rows := []monthlysource.FundingLog{
		{ID: 90000001, UserID: 201, CreatedAt: start.Unix(), Type: 1, Content: "使用在线充值成功，充值金额: $2.000000，支付金额：2.00"},
		{ID: 90000002, UserID: 9, CreatedAt: start.Unix(), Type: 3, Other: `{"op":{"action":"user.quota_subtract","params":{"target_user_id":201,"quota":"8000 点额度"}}}`},
		{ID: 90000003, UserID: 9, CreatedAt: start.Unix(), Type: 3, Other: `{"op":{"action":"user.quota_add","params":{"target_user_id":202,"quota":"8000 点额度"}}}`},
	}
	seedHistoryEmptyMonth(t, f, req, false, rows...)
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := collectCapturedSources(t.Context(), p.raw, p.core, req); err != nil {
		t.Fatal(err)
	}
	older, sealed, err := collectCapturedSources(t.Context(), p.raw, p.core, req)
	if err != nil || !sealed {
		t.Fatal("funding dependency not sealed", err)
	}
	ref := writeAutomaticHistoryManifest(t, f.f.root, automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: req.Owner,
		SourceIdentity: f.wallet.funding.sourceIdentity, Boundary: boundary, Months: []CapturedSourceFiles{older, boundary}})
	for _, month := range []string{"2026-08", "2026-09"} {
		got, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, SourcePreparationRequest{Owner: req.Owner, Month: month})
		if err != nil {
			t.Fatal(month, err)
		}
		input, err := monitor.DecodeMonthlyPreparationInput(got.observed.Preparation)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range input.Accounts {
			var positive, negative int64
			for _, m := range a.Movements {
				if m.UserID != a.UserID {
					t.Fatal("admin operator replaced affected account", m)
				}
				if m.DeltaQuota > 0 {
					positive += m.DeltaQuota
				} else {
					negative += m.DeltaQuota
				}
			}
			if a.UserID == 201 && (positive != 1000000 || negative != -8000 || a.Baseline.BalanceQuota != 8000 || len(a.Movements) != 2) ||
				a.UserID == 202 && (positive != 8000 || negative != 0 || a.Baseline.BalanceQuota != 992000 || len(a.Movements) != 1) {
				t.Fatal("transfer was netted/charged to another user or counted twice", month, a)
			}
		}
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}
