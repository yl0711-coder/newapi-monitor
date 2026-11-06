package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestRetainedPayloadReaderExactReadOnlyColdLookup(t *testing.T) {
	f, old, _, receipt, asOf := completedArchiveFixture(t)
	want, err := f.g.loadRetainedGeneration(t.Context(), old.Request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	completedArchiveEvict(t, f, old, receipt, asOf)
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(generationDB(t, f.receiverPath, true), f.funding, f.root, func(_ context.Context, r monthlytasks.Request) error {
		if r != old.Request {
			return ErrGenerationChanged
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.readOnly = true
	g.receiver.SetMaxOpenConns(1)
	g.receiver.SetMaxIdleConns(1)
	for range 2 {
		got, err := g.loadRetainedGeneration(t.Context(), old.Request, time.Now())
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("cold bytes differ or require a writer", err)
		}
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil || !bytes.Equal(before, after) || evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 {
		t.Fatal("cold lookup restored payload or changed receiver", err)
	}
	if _, err := g.Submit(t.Context(), want.plan, want.inputRaw); err == nil {
		t.Fatal("read-only lookup enabled admission")
	}
	if _, err := f.g.Submit(t.Context(), want.plan, want.inputRaw); !errors.Is(err, ErrGenerationArchived) {
		t.Fatal("writable admission reconstructed an archived input", err)
	}
}

func TestRetainedPayloadReaderRejectsBrokenRevokedAndActiveStates(t *testing.T) {
	for _, kind := range []string{"foreign", "revoked", "revoked_after_read", "invalidated_after_read", "active_task", "orphan_task", "corrupt_archive", "missing_archive", "hot_corrupt", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f, old, _, receipt, asOf := completedArchiveFixture(t)
			completedArchiveEvict(t, f, old, receipt, asOf)
			req, ctx := old.Request, t.Context()
			calls := 0
			switch kind {
			case "foreign":
				req.Owner.GroupID++
			case "revoked":
				f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
			case "revoked_after_read", "invalidated_after_read":
				f.g.verifyCurrent = func(ctx context.Context, _ monthlytasks.Request) error {
					calls++
					if calls == 2 {
						if kind == "revoked_after_read" {
							return ErrGenerationChanged
						}
						return f.g.tasks.Invalidate(ctx, old.Request.Owner, old.ID)
					}
					return nil
				}
			case "active_task":
				if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_tasks SET state='queued',result_sha256='' WHERE id=?`, old.ID); err != nil {
					t.Fatal(err)
				}
			case "orphan_task":
				if _, err := f.receiver.Exec(`DELETE FROM portal_monthly_draft_tasks WHERE id=?`, old.ID); err != nil {
					t.Fatal(err)
				}
			case "corrupt_archive":
				if err := os.WriteFile(filepath.Join(receipt.Directory, old.Request.InputSHA256+".artifact"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing_archive":
				if err := os.Remove(filepath.Join(receipt.Directory, "manifest.json")); err != nil {
					t.Fatal(err)
				}
			case "hot_corrupt":
				saved, _, err := readEvictionArchive(ctx, receipt, old.Request.InputSHA256)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.receiver.Exec(`INSERT INTO portal_monthly_generation_inputs VALUES(?,?,?,?,?)`, old.Request.InputSHA256, saved.planRaw, saved.inputRaw, []byte(`{}`), saved.digest); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			got, err := f.g.loadRetainedGeneration(ctx, req, time.Now())
			if err == nil || !reflect.DeepEqual(got, generationRetained{}) {
				t.Fatal("invalid cold read returned data", kind, err)
			}
			if kind == "hot_corrupt" && !errors.Is(err, ErrGenerationIntegrity) {
				t.Fatal("corrupt hot row silently fell back", err)
			}
		})
	}
}

func TestRetainedPayloadReaderColdPredecessorKeepsSeriesPublicationExact(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	ctx := t.Context()
	target := f.requests[2]
	source, err := f.g.automaticDisplaySource(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	prepare, err := NewSelectedAutomaticDisplaySeries(f.g, f.ranges)
	if err != nil {
		t.Fatal(err)
	}
	hot, err := prepare(ctx, target, source.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	old, err := f.g.Submit(ctx, f.plans[0], f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Run(ctx, old.Request.Owner, old.ID); err != nil {
		t.Fatal(err)
	}
	// SETUP ONLY: synthesize an independent completed receiver version to use
	// the EXISTING superseded-ready archive operation. This does not expand
	// retention to the latest draft or add a production activation/purge path.
	guard := f.g.verifyCurrent
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return nil }
	p := f.plans[0]
	input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.g.root.Name(), "independent-extra-copy-proof.json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "extra-copy-proof", Path: path, SHA256: generationHash([]byte(`{}`))})
	raw := draftJSON(t, input)
	p.PreparationSHA256 = generationHash(raw)
	newer, err := f.g.Submit(ctx, p, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Run(ctx, newer.Request.Owner, newer.ID); err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent = guard
	old, err = f.g.tasks.Get(ctx, old.Request.Owner, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	fixture := generationFixture{g: f.g, receiver: f.g.receiver}
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, fixture, asOf)
	completedArchiveEvict(t, fixture, old, receipt, asOf)
	cold, err := prepare(ctx, target, source.Evidence)
	if err != nil || !bytes.Equal(hot.evidence, cold.evidence) || !bytes.Equal(hot.binding, cold.binding) {
		t.Fatal("cold predecessor changed series money or source binding", err)
	}
	if err := displaySeriesPublisher(f.auth)(ctx, target, cold.evidence, cold.series); err != nil {
		t.Fatal("cold predecessor prevented actual later publication", err)
	}
	var saved monitor.PortalMonthlyBill
	if err := f.auth.First(&saved, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	if !sameAutomaticDisplayEvidence(cold.evidence, []byte(saved.Evidence)) {
		t.Fatal("later saved evidence changed")
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 3 {
		t.Fatal("prefix read secretly restored an archived payload")
	}
	after, err := f.g.tasks.Get(ctx, old.Request.Owner, old.ID)
	if err != nil || after != old {
		t.Fatal("prefix read changed predecessor task/budgets", err)
	}
	// A cache hit still needs the original cold files; no partial output or
	// success flag may hide a damaged archive after publication.
	if err := os.Remove(filepath.Join(receipt.Directory, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if out, err := prepare(ctx, target, source.Evidence); err == nil || len(out.evidence) != 0 {
		t.Fatal("cached series bypassed missing cold archive")
	}
	var unchanged monitor.PortalMonthlyBill
	if err := f.auth.First(&unchanged, "group_id=1 AND month='2026-09'").Error; err != nil || unchanged != saved {
		t.Fatal("failed re-audit rewrote saved customer bill", err)
	}
}

func TestRetainedPayloadReaderScopeAndObservedContextUseColdBytes(t *testing.T) {
	f := generationFixtureNew(t)
	want := automaticScopeFixture(t, f)
	addRetainedScopeFixture(t, &f, nil)
	old := retentionReady(t, f, 1)
	// A separate fully completed version makes the original eligible under
	// the EXISTING explicit superseded-draft retention policy; no new purge.
	newInput := draftCopy(t, f.input)
	path := filepath.Join(f.root, "independent-extra-proof.json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	newInput.Sources = append(newInput.Sources, monitor.MonthlyPreparationSource{ID: "independent-extra", Path: path, SHA256: generationHash([]byte(`{}`))})
	p := f.plan
	raw := draftJSON(t, newInput)
	p.PreparationSHA256 = generationHash(raw)
	newer, err := f.g.Submit(t.Context(), p, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Run(t.Context(), newer.Request.Owner, newer.ID); err != nil {
		t.Fatal(err)
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	before, err := metadata(t.Context(), old.Request)
	if err != nil {
		t.Fatal(err)
	}
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	completedArchiveEvict(t, f, old, receipt, asOf)
	got, err := load(t.Context(), old.Request)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("scope lost after cold release", err)
	}
	after, err := metadata(t.Context(), old.Request)
	if err != nil || !bytes.Equal(before.BundleContext, after.BundleContext) || !reflect.DeepEqual(before.Corrections, after.Corrections) {
		t.Fatal("cold metadata/financial audit changed", err)
	}
	if _, err := f.g.PrepareReviewHandoff(t.Context(), old.Request.Owner, old.ID, before.BundleContext); err != nil {
		t.Fatal("cold review handoff unavailable", err)
	}
}
