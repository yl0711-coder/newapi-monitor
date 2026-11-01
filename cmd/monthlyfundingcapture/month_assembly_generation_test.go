package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Synthetic independent wallet/usage evidence, never a real-money attestation.
// Importing generation is TEST ONLY: the capture command binary stays isolated
// from Monitor, its routes, schedulers and application initialization.
func testAssembledGeneration(t *testing.T, root, output string, p monthAuditPlan, review monthlysource.FundingMoneyReview) {
	t.Helper()
	ctx := context.Background()
	evidenceRoot := filepath.Join(root, "independent-evidence")
	if err := os.Mkdir(evidenceRoot, 0700); err != nil {
		t.Fatal(err)
	}
	input := monitor.MonthlyPreparationInput{Version: 1}
	from, to := review.Windows.From, review.Windows.To
	for i, id := range p.Funding.UserIDs {
		movement := review.Accounts[i].Movements[0]
		input.Accounts = append(input.Accounts, monitor.MonthlyPreparationAccount{
			UserID: id, GroupID: p.GroupID, Username: fmt.Sprintf("synthetic-%d", id), Months: []string{p.Funding.Month}, CounterBasis: "net_consumption", BasisSource: "fixture#counter",
			Baseline:         monitor.MonthlyPreparationBalance{UserID: id, At: from, BalanceQuota: 500000000, UsedQuota: 1000000000, Source: "fixture#opening", TimeBasis: "source_log_cutoff", CutoffSource: "fixture#opening-cutoff"},
			Snapshots:        []monitor.MonthlyPreparationBalance{{UserID: id, At: to, BalanceQuota: 499000000 + movement.DeltaQuota, UsedQuota: 1001000000, Source: "fixture#closing", TimeBasis: "source_log_cutoff", CutoffSource: "fixture#closing-cutoff"}},
			Spans:            []monitor.MonthlyPreparationSpan{{UserID: id, From: from, To: to, ConsumeQuota: 1500000, RefundQuota: 500000, Source: "fixture#usage"}},
			MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: id, From: from, To: to, Source: "fixture#coverage"}},
			Movements:        []monitor.MonthlyPreparationMovement{{UserID: id, ID: movement.LogID, At: movement.At, DeltaQuota: movement.DeltaQuota, Source: "fixture#credited-funding"}},
			WalletMonths:     []monitor.MonthlyPreparationWalletMonth{{UserID: id, Month: p.Funding.Month, RefundQuota: 500000, Source: "fixture#actual-refund"}},
		})
	}
	body, err := json.Marshal(input.Accounts)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(evidenceRoot, "fixture.json")
	if err := exclusiveFile(sourcePath, body); err != nil {
		t.Fatal(err)
	}
	input.Sources = []monitor.MonthlyPreparationSource{{ID: "fixture", Path: sourcePath, SHA256: hash(body)}}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256([]byte("201,202,"))
	owner := monthlytasks.Owner{GroupID: p.GroupID, AuthVersion: 1, MemberFingerprint: hex.EncodeToString(fingerprint[:16]), MemberManifest: strings.Repeat("b", 64)}
	plan := monthlydraft.GenerationPlan{Version: p.Version, Owner: owner, Funding: p.Funding, PreparationSHA256: hash(raw), Windows: review.Windows.Windows}
	receiverPath := filepath.Join(root, "synthetic-generation.sqlite")
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
	funding, err := openReceiver(ctx, filepath.Join(output, "funding.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer funding.Close()
	g, err := monthlydraft.NewGeneration(receiver, funding, evidenceRoot, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	task, err := g.Submit(ctx, plan, raw)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := g.Run(ctx, owner, task.ID)
	if err != nil {
		diagnostic, diagnosticErr := monitor.PrepareMonthlyReconciliation(raw, time.Now())
		t.Fatalf("assembled generation: %v; synthetic preparation: %+v (%v)", err, diagnostic.Accounts, diagnosticErr)
	}
	if artifact.Mode != "local_generation_draft_not_publication" || len(artifact.Reconciliation.Accounts) != 2 {
		t.Fatal("incorrect artifact", artifact.Mode)
	}
	want := []monitor.MonthlyPreparationAmounts{
		{Opening: "1000.000000", Increase: "0.000020", Decrease: "0.000000", Consume: "3.000000", Refund: "1.000000", Closing: "998.000020"},
		{Opening: "1000.000000", Increase: "0.000000", Decrease: "-0.000040", Consume: "3.000000", Refund: "1.000000", Closing: "997.999960"},
	}
	for i, account := range artifact.Reconciliation.Accounts {
		row := account.Months[0]
		if account.Error != "" || row.Status != "reconciled_draft" || row.Amounts == nil || *row.Amounts != want[i] {
			t.Fatal("assembled evidence changed generated bill amounts", account)
		}
	}
	if p.Version == 2 {
		legacy := plan
		legacy.Version = 1
		if _, err := g.Submit(ctx, legacy, raw); !errors.Is(err, monthlydraft.ErrGenerationInput) {
			t.Fatal("daily generation silently downgraded to legacy hour plan", err)
		}
		reused, err := g.Submit(ctx, plan, raw)
		if err != nil || reused.ID != task.ID {
			t.Fatal("daily generation not reused under immutable identity", err)
		}
		again, err := g.Run(ctx, owner, reused.ID)
		beforeRaw, beforeErr := json.Marshal(artifact)
		afterRaw, afterErr := json.Marshal(again)
		if err != nil || beforeErr != nil || afterErr != nil || !bytes.Equal(beforeRaw, afterRaw) {
			t.Fatal("daily draft reuse changed original amounts", err)
		}
	}
	// One quota disagreement must not be papered over by the new assembly.
	input.Accounts[0].Movements[0].DeltaQuota++
	badRaw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	badPlan := plan
	badPlan.PreparationSHA256 = hash(badRaw)
	badTask, err := g.Submit(ctx, badPlan, badRaw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Run(ctx, owner, badTask.ID); !errors.Is(err, monthlydraft.ErrGenerationEvidence) {
		t.Fatal("mismatched funding produced a draft", err)
	}
	t.Log("assembled database generated exact per-account local drafts; one-quota mismatch rejected")
}

func testAssemblyProcessInterruption(t *testing.T, executable string, args []string, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	commandArgs := append(append([]string(nil), args...), "-output", "interrupted", "-create-output")
	cmd := exec.CommandContext(ctx, executable, commandArgs...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() { _ = cmd.Process.Kill() }()
	pending := filepath.Join(root, "interrupted", "funding.pending.sqlite")
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			t.Fatal("assembly finished before interruption exercise", err)
		case <-ctx.Done():
			t.Fatal("did not reach durable partial copy", ctx.Err())
		case <-ticker.C:
			if _, err := os.Stat(pending); err != nil {
				continue
			}
			db, err := openReceiver(ctx, pending, false)
			if err != nil {
				continue
			}
			var pages int
			err = db.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_funding_pages").Scan(&pages)
			_ = db.Close()
			if err != nil || pages < 4 {
				continue
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err == nil {
				t.Fatal("interrupted assembly reported success")
			}
			if _, err := os.Lstat(filepath.Join(root, "interrupted", "assembly.json")); !os.IsNotExist(err) {
				t.Fatal("partial output was sealed")
			}
			if _, err := os.Stat(pending); err != nil {
				t.Fatal("partial evidence was lost", err)
			}
			retry := exec.CommandContext(ctx, executable, commandArgs...)
			if err := retry.Run(); err == nil {
				t.Fatal("retry overwrote interrupted evidence")
			}
			t.Logf("actual process interrupted after at least %d durable pages; no seal; retry refused overwrite", pages)
			return
		}
	}
}
