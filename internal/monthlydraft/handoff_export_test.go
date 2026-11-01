package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func handoffExportFixture(t *testing.T) (generationFixture, DeploymentConfig, HandoffExportRequest) {
	t.Helper()
	f, cfg := preparationFixture(t)
	selection := stageFixture(t, f, "")
	job := generationSubmit(t, f)
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	job, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	output := t.TempDir()
	if err := os.Chmod(output, 0700); err != nil {
		t.Fatal(err)
	}
	return f, cfg, HandoffExportRequest{TaskID: job.ID, ExpectedHead: selection.Head, ExpectedResult: job.ResultSHA256, OutputRoot: output, Context: handoffContext(t, f)}
}

func TestHandoffExportReadOnlyCopyAndExplicitReview(t *testing.T) {
	f, cfg, request := handoffExportFixture(t)
	paths := []string{cfg.ReceiverPath, cfg.FundingPath, cfg.AuthorityPath, f.input.Sources[0].Path}
	hashes := make(map[string]string)
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		hashes[path] = generationHash(b)
	}
	preview, err := ExportReviewHandoff(context.Background(), cfg, request)
	if err != nil || preview.Exported || preview.Directory != "" || preview.ManifestSHA256 == "" {
		t.Fatal(preview, err)
	}
	if files, err := os.ReadDir(request.OutputRoot); err != nil || len(files) != 0 {
		t.Fatal("preview wrote files", files, err)
	}
	request.Export = true
	receipt, err := ExportReviewHandoff(context.Background(), cfg, request)
	if err != nil || !receipt.Exported || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal(receipt, err)
	}
	read := func(name string) []byte {
		t.Helper()
		path := filepath.Join(receipt.Directory, name)
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm() != 0600 {
			t.Fatal("private mode", err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	manifest := read("manifest.json")
	if generationHash(manifest) != receipt.ManifestSHA256 {
		t.Fatal("manifest mismatch")
	}
	input, contextRaw, candidates := read("input.json"), read("context.json"), read("candidates.json")
	if !bytes.Equal(input, f.raw) || !bytes.Equal(contextRaw, request.Context) {
		t.Fatal("input changed")
	}
	var bundle monitor.MonthlyPreparationBundle
	if err := json.Unmarshal(candidates, &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Entries) != 1 || bundle.Entries[0].Status != "needs_review" || bundle.ReadyForReview != 1 {
		t.Fatal("not pending review")
	}
	users := bundle.Entries[0].Candidate.Users
	if len(users) != 2 || users[0].ClosingQuota != 104000000 || users[1].ClosingQuota != 899000000 || users[0].ConsumeQuota != 1500000 || users[0].RefundQuota != 500000 {
		t.Fatal("per-account money changed")
	}
	if _, err := monitor.ReviewMonthlyCandidate(input, contextRaw, candidates, []byte(`{}`), time.Now()); err == nil {
		t.Fatal("missing approval accepted")
	}
	again, err := ExportReviewHandoff(context.Background(), cfg, request)
	if err != nil || again.Directory == receipt.Directory || again.ManifestSHA256 != receipt.ManifestSHA256 {
		t.Fatal("repeat overwrote or changed content", again, err)
	}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil || generationHash(b) != hashes[path] {
			t.Fatal("export changed source", path, err)
		}
	}
	if _, err := os.Stat(cfg.ReceiverPath + ".runtime.lock"); !os.IsNotExist(err) {
		t.Fatal("read-only export created runtime lock", err)
	}
}

func TestHandoffExportRejectsScopePermissionAndIncompleteTask(t *testing.T) {
	f, cfg, request := handoffExportFixture(t)
	for _, change := range []func(*HandoffExportRequest){
		func(r *HandoffExportRequest) { r.ExpectedHead = generationHash([]byte("stale")) },
		func(r *HandoffExportRequest) { r.ExpectedResult = generationHash([]byte("wrong")) },
		func(r *HandoffExportRequest) { r.TaskID = "" },
		func(r *HandoffExportRequest) { r.Context = []byte(`{}`) },
	} {
		r := request
		r.Export = true
		change(&r)
		if _, err := ExportReviewHandoff(context.Background(), cfg, r); err == nil {
			t.Fatal("invalid handoff accepted")
		}
	}
	foreign := cfg
	foreign.Rollout.Targets = []GenerationTarget{{GroupID: 2, Month: "2026-09"}}
	if _, err := ExportReviewHandoff(context.Background(), foreign, request); err == nil {
		t.Fatal("foreign group accepted")
	}
	auth := generationDB(t, cfg.AuthorityPath, false)
	if _, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(cfg.ReceiverPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ExportReviewHandoff(context.Background(), cfg, request); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("permission not rejected", err)
	}
	after, err := os.ReadFile(cfg.ReceiverPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inspection invalidated task", err)
	}
	job, err := f.g.tasks.Get(context.Background(), f.plan.Owner, request.TaskID)
	if err != nil || job.State != monthlytasks.DraftReady {
		t.Fatal(job, err)
	}
	if files, err := os.ReadDir(request.OutputRoot); err != nil || len(files) != 0 {
		t.Fatal(files, err)
	}
	f2, cfg2 := preparationFixture(t)
	selection := stageFixture(t, f2, "")
	queued := generationSubmit(t, f2)
	request.TaskID = queued.ID
	request.ExpectedHead = selection.Head
	request.ExpectedResult = generationHash([]byte("not ready"))
	request.Context = handoffContext(t, f2)
	if _, err := ExportReviewHandoff(context.Background(), cfg2, request); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal(err)
	}
	queued, err = f2.g.tasks.Get(context.Background(), queued.Request.Owner, queued.ID)
	if err != nil || queued.State != monthlytasks.Queued || queued.Attempts != 0 {
		t.Fatal("queued task was run", queued, err)
	}
	d, err := openDeployment(context.Background(), cfg2, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDeploymentTest(t, d)
	if _, err := d.g.Submit(context.Background(), f2.plan, f2.raw); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("read-only submit allowed", err)
	}
	if _, err := d.g.Run(context.Background(), queued.Request.Owner, queued.ID); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("read-only queued run allowed", err)
	}
}

func TestHandoffExportFailureNeverMarksComplete(t *testing.T) {
	for _, kind := range []string{"disk", "corrupt_copy", "permission", "source", "selection", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, request := handoffExportFixture(t)
			request.Export = true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			auth := generationDB(t, cfg.AuthorityPath, false)
			writer := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if kind == "disk" && name == "context.json" {
					return errors.New("synthetic disk full")
				}
				if kind == "corrupt_copy" && name == "input.json" {
					return writeArchiveFile(ctx, root, name, []byte("corrupt"))
				}
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				if name == "manifest.pending" {
					switch kind {
					case "permission":
						_, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`)
						return err
					case "source":
						return os.WriteFile(f.input.Sources[0].Path, []byte("changed"), 0600)
					case "selection":
						_, err := WithdrawGenerationSelection(context.Background(), f.receiver, 1, "2026-09", request.ExpectedHead)
						return err
					case "cancel":
						cancel()
					}
				}
				return nil
			}
			r, err := exportReviewHandoff(ctx, cfg, request, writer)
			if err == nil || r.Exported || r.Directory == "" {
				t.Fatal("failure accepted", r, err)
			}
			if _, err := os.Stat(filepath.Join(r.Directory, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("failed copy marked complete", err)
			}
		})
	}
}

func TestHandoffExportCLIToReviewCommand(t *testing.T) {
	bin, reviewBin := os.Getenv("MONTHLY_STAGE_TEST_BINARY"), os.Getenv("MONTHLY_REVIEW_TEST_BINARY")
	if bin == "" || reviewBin == "" {
		t.Skip("set local stage and review binaries for real CLI chain")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reviewBin) {
		t.Fatal("absolute binaries required")
	}
	_, cfg, r := handoffExportFixture(t)
	contextPath := filepath.Join(t.TempDir(), "context.json")
	if err := os.WriteFile(contextPath, r.Context, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"handoff", "-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath, "-evidence-root", cfg.EvidenceRoot,
		"-group-id", "1", "-month", "2026-09", "-task-id", r.TaskID, "-expected-head", r.ExpectedHead, "-expected-result", r.ExpectedResult, "-context", contextPath, "-output-root", r.OutputRoot}
	for _, export := range []bool{false, true} {
		a := append([]string(nil), args...)
		if export {
			a = append(a, "-export")
		}
		raw, err := exec.CommandContext(t.Context(), bin, a...).CombinedOutput()
		if err != nil {
			t.Fatalf("handoff: %v %s", err, raw)
		}
		var receipt HandoffExportReceipt
		if err := json.Unmarshal(raw, &receipt); err != nil || receipt.Exported != export {
			t.Fatal(receipt, err)
		}
		if !export {
			continue
		}
		bundleRaw, err := os.ReadFile(filepath.Join(receipt.Directory, "candidates.json"))
		if err != nil {
			t.Fatal(err)
		}
		var bundle monitor.MonthlyPreparationBundle
		if err := json.Unmarshal(bundleRaw, &bundle); err != nil {
			t.Fatal(err)
		}
		// Explicit synthetic test approval only. The production exporter never
		// creates this file or a review decision on behalf of an operator.
		approval := monitor.MonthlyCandidateReview{Version: 1, GroupID: 1, Month: "2026-09", BundleSHA256: generationHash(bundleRaw), CandidateSHA256: bundle.Entries[0].CandidateSHA256,
			Decision: "approve", ReviewedBy: "synthetic-cli-test-only", ReviewedAt: time.Now().Unix(), SourcesReviewed: true, MemberScopeReviewed: true, WalletBasisReviewed: true}
		approvalRaw, err := json.Marshal(approval)
		if err != nil {
			t.Fatal(err)
		}
		approvalPath := filepath.Join(t.TempDir(), "test-approval.json")
		if err := os.WriteFile(approvalPath, approvalRaw, 0600); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "reviewed.json")
		reviewArgs := []string{"-input", filepath.Join(receipt.Directory, "input.json"), "-bundle-context", filepath.Join(receipt.Directory, "context.json"), "-review-bundle", filepath.Join(receipt.Directory, "candidates.json"), "-review-request", approvalPath, "-out", output}
		if raw, err := exec.CommandContext(t.Context(), reviewBin, reviewArgs...).CombinedOutput(); err != nil {
			t.Fatalf("review command: %v %s", err, raw)
		}
		input, err := os.ReadFile(filepath.Join(receipt.Directory, "input.json"))
		if err != nil {
			t.Fatal(err)
		}
		want, err := monitor.ReviewMonthlyCandidate(input, r.Context, bundleRaw, approvalRaw, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(output)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("CLI review output differs", err)
		}
	}
}
