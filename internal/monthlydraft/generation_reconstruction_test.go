package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Explicit synthetic historical evidence, not a real customer's attestation.
// Account 201 needs reconstruction; 202 remains on the unchanged exact-wallet
// path, so a successful organization cannot discard an independent member.
func reconstructedGenerationFixture(t *testing.T) (generationFixture, string) {
	t.Helper()
	f := generationFixtureNew(t)
	a := &f.input.Accounts[0]
	end := a.Snapshots[0].At
	a.Baseline.TimeBasis, a.Baseline.CutoffSource = "collection_observation", ""
	a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = "collection_observation", ""
	a.WalletMonths = nil
	zero := int64(0)
	a.Spans = append(a.Spans, monitor.MonthlyPreparationSpan{UserID: a.UserID, From: end, To: end + 3600,
		Requests: &zero, Tokens: &zero, Source: "reconstruction_fixture#independent-bridge"})
	a.MovementCoverage[0].To = end + 3600
	q, used, consume, refund := int64(104000000), int64(1001000000), int64(1500000), int64(500000)
	r := monitor.MonthlySnapshotReconstruction{UserID: a.UserID, Month: f.plan.Funding.Month, ObservedAt: end + 3600,
		SnapshotSource: "reconstruction_fixture#snapshot", ScopeSource: "reconstruction_fixture#view-scope",
		MonthSource: "reconstruction_fixture#month", BridgeSource: "reconstruction_fixture#bridge", FundingSource: "reconstruction_fixture#funding",
		OpeningBasisSource: "fixture#exact-opening", BalanceQuota: &q, UsedQuota: &used,
		MonthConsumeQuota: &consume, MonthRefundQuota: &refund, BridgeConsumeQuota: &zero, BridgeRefundQuota: &zero,
		FundingEventIDs: []int64{1, 3}}
	// Independently retained synthetic facts are separate from their selection.
	proof, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.root, "independent-reconstruction.json")
	if err := os.WriteFile(path, proof, 0600); err != nil {
		t.Fatal(err)
	}
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: "reconstruction_fixture", Path: path, SHA256: generationHash(proof)})
	usageInputUpdate(t, &f)
	scopePath := addRetainedScopeFixture(t, &f, func(doc *RetainedAutomaticScope) {
		doc.Scope.Accounts[0].Reconstructions = []monitor.MonthlySnapshotReconstruction{r}
	})
	return f, scopePath
}

func TestGenerationSelectedHistoricalReconstructionRunReuseAndHandoff(t *testing.T) {
	f, _ := reconstructedGenerationFixture(t)
	ctx := context.Background()
	job := generationSubmit(t, f)
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata(ctx, job.Request)
	if err != nil {
		t.Fatal("historical evidence must first pass the existing independent candidate calculator", err)
	}
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal("valid pinned reconstruction cannot finish the generation task", err)
	}
	if artifact.ScopeReview == nil || !generationHex(artifact.ScopeReview.ContextSHA256, 32) || !generationHex(artifact.ScopeReview.CandidateSHA256, 32) {
		t.Fatal("historical candidate not bound into the immutable artifact")
	}
	if artifact.Reconciliation.Accounts[0].Months[0].Status != "no_matching_balance_anchor" {
		t.Fatal("ordinary observations were rewritten as original historical cutoffs")
	}
	handoff, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta.BundleContext)
	if err != nil {
		t.Fatal("selected reconstruction cannot reach review handoff", err)
	}
	var bundle monitor.MonthlyPreparationBundle
	if json.Unmarshal(handoff.Bundle, &bundle) != nil || bundle.ReadyForReview != 1 || bundle.Blocked != 0 || len(bundle.Entries[0].Candidate.Users) != 2 {
		t.Fatal("partial or invalid reconstructed organization")
	}
	candidate := bundle.Entries[0].Candidate
	if candidate.Users[0].ClosingQuota != 104000000 || candidate.Users[0].ConsumeQuota != 1500000 || candidate.Users[0].RefundQuota != 500000 ||
		candidate.Users[1].ClosingQuota != 899000000 || len(candidate.ReconstructedUsers) != 1 || candidate.ReconstructedUsers[0] != 201 {
		t.Fatal("reconstruction altered independent account money")
	}
	before, _ := json.Marshal(artifact)
	for range 2 {
		again, err := f.g.Run(ctx, job.Request.Owner, job.ID)
		after, _ := json.Marshal(again)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("repeated access recalculated or changed immutable draft", err)
		}
	}
	reopened, err := NewGeneration(f.receiver, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.Run(ctx, job.Request.Owner, job.ID)
	after, _ := json.Marshal(again)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("restart changed the selected historical draft", err)
	}
	var wrongContext monitor.MonthlyBundleContext
	if json.Unmarshal(meta.BundleContext, &wrongContext) != nil {
		t.Fatal("context fixture missing")
	}
	wrongContext.Groups[0].Name += "-different-context"
	wrongRaw, _ := json.Marshal(wrongContext)
	if _, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, wrongRaw); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("alternate context reused a frozen historical candidate", err)
	}
	var count int
	if err := f.receiver.QueryRow("SELECT count(*) FROM portal_monthly_generation_inputs").Scan(&count); err != nil || count != 1 {
		t.Fatal("draft reuse created another input", err)
	}
	// Exercise the actual policy/format builder without writing a bill. This
	// remains local synthetic evidence, never a customer publication callback.
	if _, err := monitor.PrepareAutomaticMonthlyBill(*candidate, time.Now().Unix(), time.Now()); err != nil {
		t.Fatal("existing publication arithmetic rejects reconstructed candidate", err)
	}
}

func rewriteReconstructionScopeFixture(t *testing.T, f *generationFixture, path string, change func(*RetainedAutomaticScope)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	var document RetainedAutomaticScope
	if err != nil || json.Unmarshal(raw, &document) != nil {
		t.Fatal(err)
	}
	change(&document)
	raw, err = json.Marshal(document)
	if err != nil || os.WriteFile(path, raw, 0600) != nil {
		t.Fatal("write synthetic scope", err)
	}
	for i := range f.input.Sources {
		if f.input.Sources[i].ID == syntheticScopeID {
			f.input.Sources[i].SHA256 = generationHash(raw)
		}
	}
	usageInputUpdate(t, f)
}

func TestGenerationSelectedHistoricalReconstructionRejectsIncompleteOrConflicting(t *testing.T) {
	for _, kind := range []string{"not-selected", "balance-one-quota", "valid-anchor-conflicting-selection", "missing-refund", "missing-event", "unknown-wallet", "self-source", "another-member-mismatch", "changed-original", "changed-scope"} {
		t.Run(kind, func(t *testing.T) {
			f, path := reconstructedGenerationFixture(t)
			switch kind {
			case "not-selected":
				f.plan.AutomaticScopeSourceID = ""
			case "balance-one-quota", "valid-anchor-conflicting-selection", "missing-refund", "missing-event", "unknown-wallet", "self-source":
				if kind == "valid-anchor-conflicting-selection" {
					a := &f.input.Accounts[0]
					a.Baseline.TimeBasis, a.Baseline.CutoffSource = "source_log_cutoff", "fixture#opening-cutoff-proof"
					a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = "source_log_cutoff", "fixture#closing-cutoff-proof"
					usageInputUpdate(t, &f)
					windows, err := f.g.auditWindows(context.Background(), f.plan)
					if err != nil {
						t.Fatal(err)
					}
					review, err := ReconcileObservedMonth(context.Background(), f.plan.Funding, windows, f.raw, time.Now())
					if err != nil || !generationAllAccounts(f.plan, review.Reconciliation) {
						t.Fatal("counterexample must first pass the ordinary anchor path", err)
					}
				}
				rewriteReconstructionScopeFixture(t, &f, path, func(doc *RetainedAutomaticScope) {
					r := &doc.Scope.Accounts[0].Reconstructions[0]
					switch kind {
					case "balance-one-quota", "valid-anchor-conflicting-selection":
						*r.BalanceQuota++
					case "missing-refund":
						r.BridgeRefundQuota = nil
					case "missing-event":
						r.FundingEventIDs = []int64{1}
					case "unknown-wallet":
						doc.Scope.Accounts[0].WalletOnly = false
					case "self-source":
						r.ScopeSource = syntheticScopeID + "#not-independent"
					}
				})
			case "another-member-mismatch":
				f.input.Accounts[1].Snapshots[0].BalanceQuota++
				usageInputUpdate(t, &f)
			case "changed-original":
				if err := os.WriteFile(f.input.Sources[0].Path, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "changed-scope":
				if err := os.WriteFile(path, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			job := generationSubmit(t, f)
			artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
			if err == nil || len(artifact.Reconciliation.Accounts) != 0 || artifact.ScopeReview != nil {
				t.Fatal("unproven or partial organization became a completed draft", err)
			}
			saved, loadErr := loadGeneration(context.Background(), f.receiver, job.Request, time.Now())
			if loadErr != nil || len(saved.artifact) != 0 {
				t.Fatal("failure persisted a reusable artifact", loadErr)
			}
		})
	}
}

func TestGenerationSelectedHistoricalReconstructionFormalInspection(t *testing.T) {
	f, path := reconstructedGenerationFixture(t)
	_, authority, authorityPath := usageAuthorityFixture(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), authority, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	rewriteReconstructionScopeFixture(t, &f, path, func(doc *RetainedAutomaticScope) { doc.Owner = owner })
	if err := InitGenerationSelections(context.Background(), f.receiver); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{f.receiverPath, f.path} {
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, canonical, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cfg := DeploymentConfig{ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: authorityPath, EvidenceRoot: f.root,
		Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: f.plan.Funding.Month}}}}
	result, err := PrepareGenerationInput(context.Background(), cfg, canonical, f.raw, "", false)
	if err != nil || result.Applied || result.Changed {
		t.Fatal("formal read-only inspection rejects independently validated reconstruction", err)
	}
	var count int
	if err := f.receiver.QueryRow("SELECT count(*) FROM portal_monthly_generation_inputs").Scan(&count); err != nil || count != 0 {
		t.Fatal("formal read-only inspection retained a task", err)
	}
}

func TestGenerationSelectedHistoricalReconstructionRejectsForgedCachedProof(t *testing.T) {
	for _, kind := range []string{"missing-proof", "different-proof"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := reconstructedGenerationFixture(t)
			job := generationSubmit(t, f)
			artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "missing-proof" {
				artifact.ScopeReview = nil
			} else {
				artifact.ScopeReview.CandidateSHA256 = strings.Repeat("a", 64)
			}
			raw, _ := json.Marshal(artifact)
			// Deliberate corruption of a NEW synthetic receiver, never an actual
			// retained source or bill. Even consistent forged local checksums must
			// not bypass fresh reconstruction of the selected source candidate.
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
				t.Fatal("forged historical proof reused", err)
			}
		})
	}
}

func TestGenerationSelectedHistoricalReconstructionStillChecksAuthorityOnReuse(t *testing.T) {
	f, _ := reconstructedGenerationFixture(t)
	job := generationSubmit(t, f)
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("revoked authority reused historical artifact", err)
	}
}

func TestGenerationSelectedHistoricalReconstructionLocalSaveAndRestart(t *testing.T) {
	f, path := reconstructedGenerationFixture(t)
	auth, readOnly, _ := usageAuthorityFixture(t)
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), readOnly, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	rewriteReconstructionScopeFixture(t, &f, path, func(doc *RetainedAutomaticScope) { doc.Owner = owner })
	f.g.verifyCurrent = func(ctx context.Context, request monthlytasks.Request) error {
		current, _, err := LoadUsageGenerationOwner(ctx, readOnly, request.Owner.GroupID, request.Owner.AuthVersion)
		if err != nil {
			return err
		}
		if current != request.Owner {
			return ErrGenerationChanged
		}
		return nil
	}
	job := generationSubmit(t, f)
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	publications := 0
	// Only the new test-local synthetic organization's database is written.
	// No real customer, real authority, source wallet or production service.
	publish := func(ctx context.Context, request monthlytasks.Request, raw []byte) error {
		publications++
		_, err := monitor.ImportMonthlyBillForAuthorization(ctx, auth, raw, monitor.MonthlyBillExpectedAuthorization{
			GroupID: request.Owner.GroupID, AuthVersion: request.Owner.AuthVersion,
			MemberFingerprint: request.Owner.MemberFingerprint, MemberManifest: request.Owner.MemberManifest}, time.Now())
		return err
	}
	worker, err := NewAutomaticLocalWorker(f.g, metadata, publish)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.step(context.Background()); err != nil || !worked || worker.Status().Published != 1 || publications != 1 {
		t.Fatal("selected reconstruction cannot finish actual local save", worked, err, worker.Status())
	}
	var record monitor.PortalMonthlyBill
	if err := auth.First(&record, "group_id=? AND month=?", 1, job.Request.Month).Error; err != nil {
		t.Fatal(err)
	}
	var view struct {
		Opening string `json:"month_opening_balance"`
		Closing string `json:"month_end_balance"`
		Members []struct {
			UserID  int64  `json:"user_id"`
			Opening string `json:"opening"`
			Closing string `json:"closing"`
		} `json:"members"`
	}
	if json.Unmarshal([]byte(record.Payload), &view) != nil || view.Opening != "2000.000000" || view.Closing != "2006.000000" || len(view.Members) != 2 ||
		view.Members[0].UserID != 201 || view.Members[0].Closing != "208.000000" || view.Members[1].UserID != 202 || view.Members[1].Closing != "1798.000000" {
		t.Fatal("saved customer projection does not match independent accounts")
	}
	for _, marker := range []string{"selected_scope_review", "reconstruction_fixture", "snapshot_source", "reconstructions"} {
		if strings.Contains(record.Payload, marker) {
			t.Fatal("private generation proof leaked into customer view", marker)
		}
	}
	for range 3 {
		if worked, err := worker.step(context.Background()); err != nil || worked {
			t.Fatal("completed local bill unnecessarily regenerated", worked, err)
		}
	}
	restarted, err := NewAutomaticLocalWorker(f.g, metadata, publish)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := restarted.step(context.Background()); err != nil || worked || publications != 1 {
		t.Fatal("restart republished or recalculated immutable bill", worked, err, publications)
	}
	var after monitor.PortalMonthlyBill
	if err := auth.First(&after, "group_id=? AND month=?", 1, job.Request.Month).Error; err != nil || after.Payload != record.Payload || after.PayloadHash != record.PayloadHash || after.PublicationID != record.PublicationID {
		t.Fatal("saved bill changed after repeated access/restart", err)
	}
}
