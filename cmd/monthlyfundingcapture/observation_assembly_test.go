package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func observationAssemblyFixture(t *testing.T) (string, observationAssemblyPlan) {
	t.Helper()
	ctx := context.Background()
	root, err := filepath.EvalSymlinks(auditPrivateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := newMonthBatch(ctx, 1, auditPlanFixture(t).Funding, 86400)
	if err != nil {
		t.Fatal(err)
	}
	p := monthAuditPlan{Version: 2, GroupID: 1, Funding: batch.Funding}
	for _, w := range batch.Windows {
		p.Windows = append(p.Windows, monthAuditWindow{Job: w.Job, ManifestSHA256: w.ManifestSHA256, Version: monthlysource.FundingMonthWindowVersion{
			From: w.From, To: w.To, OlderScanID: w.OlderScanID, NewerScanID: w.NewerScanID}})
	}
	for i := range p.Windows {
		p.Windows[i] = writeAuditWindow(t, root, p, i, nil)
	}
	raw, _ := json.Marshal(p)
	funding, err := assembleMonth(ctx, root, "funding-source", p, raw, true)
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(root, "funding-source", "month-plan.json")
	plan := observationAssemblyPlan{Version: 1, FundingPlan: assemblyFile{planPath, hash(raw)},
		FundingDB: assemblyFile{filepath.Join(root, "funding-source", "funding.sqlite"), funding.DatabaseSHA256}}
	started := time.Now().UnixMilli() - 700000
	for day := range 30 {
		for _, user := range p.Funding.UserIDs {
			var pair assemblyUsagePair
			for round := range 2 {
				r := observationRequest{Version: 1, Kind: "observe-usage", SourceIdentity: p.Funding.SourceIdentity, UserIDs: []int64{user},
					From: int64(1788192000 + day*86400), To: int64(1788192000 + (day+1)*86400)}
				q, used := int64(-1), int64(9007199254740993)
				o := monthlysource.UsageWindowObservation{Version: 1, Basis: "database_view_wallet_and_usage_not_month_boundary", Request: usageObservationRequest(r),
					StartedAtMS: started + int64(round)*301000, CompletedAtMS: started + int64(round)*301000 + 1,
					Wallet: monthlysource.WalletUser{UserID: user, Exists: true, BalanceQuota: &q, UsedQuota: &used}, Rows: []monthlysource.UsageWindowRow{}}
				if day == 0 {
					o.Rows = []monthlysource.UsageWindowRow{{UserID: user, Type: 2, BillingSource: "wallet", Records: 1,
						Quota: 123, MinQuota: 123, MaxQuota: 123, MinID: user, MaxID: user, FirstAt: r.From + 1, LastAt: r.From + 1}}
				}
				b, _ := json.Marshal(o)
				ref := writeAssemblyFixtureCapture(t, root, fmt.Sprintf("usage-%d-%02d-%d", user, day, round), r, b)
				if round == 0 {
					pair.Older = ref
				} else {
					pair.Newer = ref
				}
			}
			plan.Usage = append(plan.Usage, pair)
		}
	}
	q, used := int64(-1), int64(9007199254740993)
	wallet := monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary", SourceIdentity: p.Funding.SourceIdentity,
		StartedAtMS: started, CompletedAtMS: started + 1, Users: []monthlysource.WalletUser{
			{UserID: 201, Exists: true, BalanceQuota: &q, UsedQuota: &used}, {UserID: 202, Exists: true, BalanceQuota: &q, UsedQuota: &used}}}
	walletRaw, _ := json.Marshal(wallet)
	plan.Wallet = []assemblyCapture{writeAssemblyFixtureCapture(t, root, "wallet", observationRequest{Version: 1, Kind: "observe-wallet",
		SourceIdentity: p.Funding.SourceIdentity, UserIDs: []int64{201, 202}}, walletRaw)}
	return root, plan
}

func writeAssemblyFixtureCapture(t *testing.T, root, name string, request observationRequest, raw []byte) assemblyCapture {
	t.Helper()
	receipt, err := observeOne(context.Background(), root, name, "/unused-by-synthetic-collector", request, true,
		func(context.Context, string, observationRequest) ([]byte, error) { return raw, nil })
	if err != nil || !receipt.Complete {
		t.Fatal("synthetic original capture", err)
	}
	return assemblyCapture{receipt.Directory, fileHash(t, filepath.Join(receipt.Directory, "manifest.json"))}
}

func TestObservationAssemblyDefaultPreservationAndReopen(t *testing.T) {
	root, plan := observationAssemblyFixture(t)
	originals := map[string]string{}
	for _, p := range []string{plan.FundingDB.Path, plan.FundingPlan.Path} {
		originals[p] = fileHash(t, p)
	}
	refs := slices.Clone(plan.Wallet)
	for _, pair := range plan.Usage {
		refs = append(refs, pair.Older, pair.Newer)
	}
	for _, ref := range refs {
		for _, name := range []string{"manifest.json", "request.json", "observation.json"} {
			p := filepath.Join(ref.Directory, name)
			originals[p] = fileHash(t, p)
		}
	}
	raw, _ := json.Marshal(plan)
	path := filepath.Join(root, "assembly-plan.json")
	if err := exclusiveFile(path, raw); err != nil {
		t.Fatal(err)
	}
	args := []string{"assemble-observations", "-workspace", root, "-output", "combined", "-plan", path, "-expected-plan", hash(raw)}
	var out bytes.Buffer
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal("memory-only preflight", err)
	}
	var preview observationAssemblyReceipt
	if json.Unmarshal(out.Bytes(), &preview) != nil || preview.Mode != "checked_observation_assembly_no_output" || preview.DatabaseSHA256 != "" || preview.UsageCaptures != 120 || preview.WalletCaptures != 1 || preview.FundingRows != 4 {
		t.Fatal("preflight totals or mode", preview)
	}
	if _, err := os.Stat(filepath.Join(root, "combined")); !os.IsNotExist(err) {
		t.Fatal("preflight created output")
	}
	out.Reset()
	if err := run(context.Background(), append(args, "-create-output"), &out); err != nil {
		t.Fatal("create combined source", err)
	}
	var receipt observationAssemblyReceipt
	if json.Unmarshal(out.Bytes(), &receipt) != nil || receipt.Mode != "assembled_observations_not_bill_approval" || receipt.SelectionSHA256 != preview.SelectionSHA256 || receipt.ReviewSHA256 != preview.ReviewSHA256 {
		t.Fatal("reopened source differs from memory-only preflight")
	}
	output := filepath.Join(root, "combined")
	seal, err := privateRead(filepath.Join(output, "assembly.json"), 4096)
	if err != nil || !bytes.Equal(bytes.TrimSpace(out.Bytes()), seal) {
		t.Fatal("completion marker differs from receipt", err)
	}
	if got, size, err := hashAssemblyDatabase(context.Background(), filepath.Join(output, "observations.sqlite")); err != nil || got != receipt.DatabaseSHA256 || size != receipt.DatabaseBytes {
		t.Fatal("sealed database digest differs", err)
	}
	selectionRaw, _ := privateRead(filepath.Join(output, "selection.json"), 2<<20)
	var selection observationAssemblySelection
	if json.Unmarshal(selectionRaw, &selection) != nil || hash(selectionRaw) != receipt.SelectionSHA256 {
		t.Fatal("invalid persisted selection")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(filepath.Join(output, "observations.sqlite") + suffix); !os.IsNotExist(err) {
			t.Fatal("unsealed source sidecar", suffix)
		}
	}
	for _, name := range []string{"observations.pending.sqlite", "assembly.pending.json"} {
		if _, err := os.Lstat(filepath.Join(output, name)); !os.IsNotExist(err) {
			t.Fatal("pending file remains", name)
		}
	}
	// Immutable originals and exact payload IDs survive a failed overwrite.
	out.Reset()
	if err := run(context.Background(), append(args, "-create-output"), &out); err == nil || out.Len() != 0 {
		t.Fatal("existing output overwritten or success printed")
	}
	for p, want := range originals {
		if fileHash(t, p) != want {
			t.Fatal("original changed", filepath.Base(p))
		}
	}
	shuffled := plan
	shuffled.Usage = slices.Clone(plan.Usage)
	slices.Reverse(shuffled.Usage)
	shuffledRaw, _ := json.Marshal(shuffled)
	again, err := assembleObservations(context.Background(), root, "shuffled", shuffled, shuffledRaw, false)
	if err != nil || again.SelectionSHA256 != receipt.SelectionSHA256 || again.ReviewSHA256 != receipt.ReviewSHA256 {
		t.Fatal("capture order changed selection or money", err)
	}
}

func TestObservationAssemblyRejectsInvalidEvidenceWithoutOutput(t *testing.T) {
	root, original := observationAssemblyFixture(t)
	for _, kind := range []string{"missing-empty-day", "duplicate-day", "swapped-round", "manifest-pin", "funding-digest", "funding-plan-pin", "missing-wallet", "duplicate-wallet", "wrong-version", "relative-database", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			p := original
			p.Usage, p.Wallet = slices.Clone(original.Usage), slices.Clone(original.Wallet)
			ctx := context.Background()
			switch kind {
			case "missing-empty-day":
				p.Usage = append(p.Usage[:4], p.Usage[5:]...)
			case "duplicate-day":
				p.Usage = append(p.Usage, p.Usage[0])
			case "swapped-round":
				p.Usage[0].Older, p.Usage[0].Newer = p.Usage[0].Newer, p.Usage[0].Older
			case "manifest-pin":
				p.Usage[0].Older.ManifestSHA256 = strings.Repeat("b", 64)
			case "funding-digest":
				p.FundingDB.SHA256 = strings.Repeat("b", 64)
			case "funding-plan-pin":
				p.FundingPlan.SHA256 = strings.Repeat("b", 64)
			case "missing-wallet":
				p.Wallet = nil
			case "duplicate-wallet":
				p.Wallet = append(p.Wallet, p.Wallet[0])
			case "wrong-version":
				p.Version = 2
			case "relative-database":
				p.FundingDB.Path = "funding.sqlite"
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw, _ := json.Marshal(p)
			result, err := assembleObservations(ctx, root, kind, p, raw, true)
			if err == nil || !reflect.DeepEqual(result, observationAssemblyReceipt{}) {
				t.Fatal("invalid source produced an assembly receipt", err)
			}
			if _, err := os.Lstat(filepath.Join(root, kind)); !os.IsNotExist(err) {
				t.Fatal("invalid source wrote output before preflight completed")
			}
		})
	}
	// A zero-length journal still invalidates the main-file digest as a seal.
	if err := exclusiveFile(original.FundingDB.Path+"-journal", nil); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(original)
	if _, err := assembleObservations(context.Background(), root, "sidecar", original, raw, true); err == nil {
		t.Fatal("source sidecar ignored")
	}
}

func TestObservationAssemblyCaptureSealCompatibilityAndTamper(t *testing.T) {
	for _, kind := range []string{"official-two-names", "retained-single-name", "third-hardlink", "unrelated-hardlink", "payload-changed", "request-changed", "wrong-kind", "missing-marker"} {
		t.Run(kind, func(t *testing.T) {
			root, request, raw := observationFixture(t, "observe-wallet")
			ref := writeAssemblyFixtureCapture(t, root, "capture", request, raw)
			marker := filepath.Join(ref.Directory, "manifest.json")
			pending := filepath.Join(ref.Directory, "manifest.pending.json")
			wantKind := "observe-wallet"
			switch kind {
			case "retained-single-name":
				if err := os.Remove(pending); err != nil {
					t.Fatal(err)
				}
			case "third-hardlink":
				if err := os.Link(marker, filepath.Join(root, "unexpected-alias")); err != nil {
					t.Fatal(err)
				}
			case "unrelated-hardlink":
				if err := os.Rename(pending, filepath.Join(root, "outside-alias")); err != nil {
					t.Fatal(err)
				}
			case "payload-changed", "request-changed":
				name := "observation.json"
				if kind == "request-changed" {
					name = "request.json"
				}
				if err := os.WriteFile(filepath.Join(ref.Directory, name), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong-kind":
				wantKind = "observe-usage"
			case "missing-marker":
				if err := os.Remove(marker); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readAssemblyCapture(ref, wantKind)
			if kind == "official-two-names" || kind == "retained-single-name" {
				if err != nil || !bytes.Equal(got, raw) {
					t.Fatal("producer's original seal rejected", err)
				}
			} else if err == nil || len(got) != 0 {
				t.Fatal("invalid seal/payload accepted")
			}
		})
	}
}

func TestObservationAssemblyCancellationRetainsUnsealedPartial(t *testing.T) {
	root, plan := observationAssemblyFixture(t)
	raw, _ := json.Marshal(plan)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	output := filepath.Join(root, "interrupted")
	observed := make(chan bool, 1)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				observed <- false
				return
			case <-ticker.C:
				if _, err := os.Stat(filepath.Join(output, "observations.pending.sqlite")); err == nil {
					cancel()
					observed <- true
					return
				}
			}
		}
	}()
	result, err := assembleObservations(ctx, root, "interrupted", plan, raw, true)
	cancel()
	if err == nil || !reflect.DeepEqual(result, observationAssemblyReceipt{}) || !<-observed {
		t.Fatal("cancellation did not interrupt actual disk assembly", err)
	}
	if _, err := os.Stat(filepath.Join(output, "observations.pending.sqlite")); err != nil {
		t.Fatal("partial evidence discarded", err)
	}
	if _, err := os.Lstat(filepath.Join(output, "assembly.json")); !os.IsNotExist(err) {
		t.Fatal("interrupted assembly was sealed")
	}
	if _, err := assembleObservations(context.Background(), root, "interrupted", plan, raw, true); err == nil {
		t.Fatal("retry overwrote interrupted evidence")
	}
}
