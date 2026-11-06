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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func observationMaterialFixture(t *testing.T) (string, observationMonthAssemblyRequest) {
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
		p.Windows = append(p.Windows, monthAuditWindow{Job: w.Job, ManifestSHA256: w.ManifestSHA256,
			Version: monthlysource.FundingMonthWindowVersion{From: w.From, To: w.To, OlderScanID: w.OlderScanID, NewerScanID: w.NewerScanID}})
	}
	for i := range p.Windows {
		p.Windows[i] = writeAuditWindow(t, root, p, i, nil)
	}
	raw, _ := json.Marshal(p)
	if _, err := assembleMonth(ctx, root, "funding-source", p, raw, true); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "funding-source", "assembly.json")
	op := observationMonthPlan{Version: 1, GroupID: p.GroupID, Funding: p.Funding}
	initial, err := initializeObservationMonth(ctx, root, "observation-source", op, true)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := observationMonthJobs(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-3 * time.Hour).Truncate(time.Millisecond)
	for _, job := range jobs {
		if job.Name == "token-0000-newer" {
			now = now.Add(5 * time.Minute)
		}
		r := job.Request
		users := make([]monthlysource.WalletUser, 0, len(r.UserIDs))
		for _, id := range r.UserIDs {
			balance, used := int64(499000010), int64(1001000000)
			if id == 202 {
				balance = 498999980
			}
			users = append(users, monthlysource.WalletUser{UserID: id, Exists: true, BalanceQuota: &balance, UsedQuota: &used})
		}
		var value any
		if r.Kind == "observe-wallet" {
			value = monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary",
				SourceIdentity: r.SourceIdentity, StartedAtMS: now.UnixMilli(), CompletedAtMS: now.UnixMilli(), Users: users}
		} else {
			o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary",
				Request: usageObservationRequest(r), StartedAtMS: now.UnixMilli(), CompletedAtMS: now.UnixMilli(), Wallet: users[0],
				Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
			if r.From == p.Windows[0].Version.From {
				for i, quota := range []int64{1500000, 500000} {
					kind := 2
					if i == 1 {
						kind = 6
					}
					id := r.UserIDs[0]*100 + int64(i)
					row := monthlysource.UsageWindowRow{UserID: r.UserIDs[0], Type: kind, BillingSource: "wallet", Records: 1, Quota: quota,
						MinQuota: quota, MaxQuota: quota, MinID: id, MaxID: id, FirstAt: r.From + 100 + int64(i), LastAt: r.From + 100 + int64(i)}
					o.Totals = append(o.Totals, row)
					o.Rows = append(o.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: row, TokenID: r.UserIDs[0]*10 + int64(i), Name: "synthetic original token"})
				}
			}
			value = o
		}
		body, _ := json.Marshal(value)
		writeAssemblyFixtureCapture(t, filepath.Join(root, "observation-source"), job.Name, r, body)
		now = now.Add(2 * time.Second)
	}
	index, err := sealObservationMonthIndex(ctx, root, "observation-source", initial.PlanSHA256, true)
	if err != nil {
		t.Fatal(err)
	}
	return root, observationMonthAssemblyRequest{Version: 1,
		ObservationIndex: assemblyFile{filepath.Join(root, "observation-source", "observation-index.json"), index.IndexSHA256},
		FundingAssembly:  assemblyCapture{filepath.Join(root, "funding-source"), fileHash(t, manifestPath)}}
}

func readObservationMaterial(t *testing.T, root, name string, receipt observationMonthAssemblyReceipt) monthlysource.MonthlyObservationMaterial {
	t.Helper()
	raw, err := privateRead(filepath.Join(root, name, "materials.json"), 2<<20)
	var material monthlysource.MonthlyObservationMaterial
	if err != nil || hash(raw) != receipt.MaterialSHA256 || json.Unmarshal(raw, &material) != nil || material.Mode != monthlysource.MonthlyObservationMaterialMode ||
		material.Version != 1 || material.FundingVersion != 2 || material.GroupID != 1 || len(material.Tokens) != 2 || len(material.WalletIDs) != 2 || len(material.UsageWindows) != 60 {
		t.Fatal("material handoff missing scope or original sources", err)
	}
	if err := verifyObservationMaterialFiles(context.Background(), material); err != nil {
		t.Fatal(err)
	}
	return material
}

func TestObservationMaterialAssemblySourcePinningAndExactGeneration(t *testing.T) {
	root, request := observationMaterialFixture(t)
	raw, _ := json.Marshal(request)
	before := map[string]string{}
	for _, path := range []string{request.ObservationIndex.Path, filepath.Join(request.FundingAssembly.Directory, "funding.sqlite"), filepath.Join(request.FundingAssembly.Directory, "assembly.json")} {
		before[path] = fileHash(t, path)
	}
	preview, err := assembleObservationMonth(context.Background(), root, "material", raw, false)
	if err != nil || preview.MaterialSHA256 != "" || preview.Accounts != 2 {
		t.Fatal("inspection failed or claimed a created artifact", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "material")); !os.IsNotExist(err) {
		t.Fatal("inspection created output", err)
	}
	created, err := assembleObservationMonth(context.Background(), root, "material", raw, true)
	if err != nil || !validSHA(created.MaterialSHA256) {
		t.Fatal("automatic offline assembly failed", err)
	}
	material := readObservationMaterial(t, root, "material", created)
	for path, digest := range before {
		if fileHash(t, path) != digest {
			t.Fatal("original artifact modified", path)
		}
	}
	if _, err := assembleObservationMonth(context.Background(), root, "material", raw, true); err == nil {
		t.Fatal("existing material overwritten")
	}
	aliased := bytes.Clone(raw)
	write := func(dir *os.Root, name string, body []byte) error {
		if name == "request.json" {
			aliased[0] = 'x'
		}
		return writeObservationFile(dir, name, body)
	}
	detached, err := assembleObservationMonthWithWrite(context.Background(), root, "detached", aliased, true, write)
	if err != nil || detached.RequestSHA256 != hash(raw) {
		t.Fatal("caller mutation changed the pinned input selection", err)
	}
	testObservationMaterialGeneration(t, root, material)
	if binary := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); binary != "" {
		path := filepath.Join(root, "assembly-request.json")
		if err := exclusiveFile(path, raw); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		body, err := exec.CommandContext(ctx, binary, "assemble-observation-month", "-workspace", root, "-output", "actual-cli", "-request", path,
			"-expected-request", hash(raw), "-create-output").Output()
		var actual observationMonthAssemblyReceipt
		if err != nil || json.Unmarshal(body, &actual) != nil {
			t.Fatal("actual assembly command failed", err)
		}
		other := readObservationMaterial(t, root, "actual-cli", actual)
		if other.Database.SHA256 != material.Database.SHA256 || !reflect.DeepEqual(other.FundingWindows, material.FundingWindows) || !reflect.DeepEqual(other.UsageWindows, material.UsageWindows) {
			t.Fatal("actual CLI changed source values or selections")
		}
	}
}

// All independent boundary/refund evidence is deliberately synthetic. Production
// capture command has no import of Monitor/monthlydraft and no approval path.
func testObservationMaterialGeneration(t *testing.T, root string, material monthlysource.MonthlyObservationMaterial) {
	t.Helper()
	from, to := int64(1788192000), int64(1790784000)
	input := monitor.MonthlyPreparationInput{Version: 1}
	for _, id := range material.Funding.UserIDs {
		closing := int64(499000010)
		if id == 202 {
			closing = 498999980
		}
		input.Accounts = append(input.Accounts, monitor.MonthlyPreparationAccount{UserID: id, GroupID: material.GroupID, Username: fmt.Sprintf("synthetic-%d", id),
			Months: []string{material.Funding.Month}, CounterBasis: "net_consumption", BasisSource: "independent#counter",
			Spans: []monitor.MonthlyPreparationSpan{}, Movements: []monitor.MonthlyPreparationMovement{},
			Baseline:         monitor.MonthlyPreparationBalance{UserID: id, At: from, BalanceQuota: 500000000, UsedQuota: 1000000000, Source: "independent#opening", TimeBasis: "source_log_cutoff", CutoffSource: "independent#opening-cutoff"},
			Snapshots:        []monitor.MonthlyPreparationBalance{{UserID: id, At: to, BalanceQuota: closing, UsedQuota: 1001000000, Source: "independent#closing", TimeBasis: "source_log_cutoff", CutoffSource: "independent#closing-cutoff"}},
			MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: id, From: from, To: to, Source: "independent#coverage"}},
			WalletMonths:     []monitor.MonthlyPreparationWalletMonth{{UserID: id, Month: material.Funding.Month, RefundQuota: 500000, Source: "independent#actual-refund"}}})
	}
	independent, _ := json.Marshal(input.Accounts)
	evidencePath := filepath.Join(root, "independent.json")
	if err := exclusiveFile(evidencePath, independent); err != nil {
		t.Fatal(err)
	}
	input.Sources = []monitor.MonthlyPreparationSource{{ID: "independent", Path: evidencePath, SHA256: hash(independent)},
		{ID: "observations", Path: material.Database.Path, SHA256: material.Database.SHA256}}
	memberSum := sha256.Sum256([]byte("201,202,"))
	p := monthlydraft.GenerationPlan{Version: material.FundingVersion, Owner: monthlytasks.Owner{GroupID: material.GroupID, AuthVersion: 1,
		MemberFingerprint: hex.EncodeToString(memberSum[:16]), MemberManifest: strings.Repeat("b", 64)}, Funding: material.Funding,
		Windows: material.FundingWindows, UsageWindows: material.UsageWindows, ObservationStoreSourceID: "observations",
		WalletObservations: &monthlydraft.WalletObservationSelection{SourceID: "observations", IDs: material.WalletIDs}}
	if material.WalletReuse != nil {
		raw, err := privateRead(material.WalletReuse.Path, monthlysource.WalletReuseMaterialLimit)
		if err != nil {
			t.Fatal(err)
		}
		var meta monthlysource.WalletReuseMaterial
		if hash(raw) != material.WalletReuse.SHA256 || json.Unmarshal(raw, &meta) != nil {
			t.Fatal("invalid reuse fixture")
		}
		p.WalletObservations.ReuseSourceID = "wallet-reuse"
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "wallet-reuse", Path: material.WalletReuse.Path, SHA256: material.WalletReuse.SHA256},
			monitor.MonthlyPreparationSource{ID: "wallet-reuse-origin", Path: meta.OriginalMaterial.Path, SHA256: meta.OriginalMaterial.SHA256})
	}
	for _, token := range material.Tokens {
		id := fmt.Sprintf("tokens-%d", token.UserID)
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: id, Path: token.File.Path, SHA256: token.File.SHA256})
		p.TokenUsageSources = append(p.TokenUsageSources, monthlydraft.TokenUsageSourceSelection{UserID: token.UserID, SourceID: id})
	}
	raw, _ := json.Marshal(input)
	p.PreparationSHA256 = hash(raw)
	path := filepath.Join(root, "receiver.sqlite")
	if err := exclusiveFile(path, nil); err != nil {
		t.Fatal(err)
	}
	receiver, err := openReceiver(context.Background(), path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := monthlydraft.InitGenerationReceiver(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	defaultPath := filepath.Join(root, "unrelated.sqlite")
	if err := exclusiveFile(defaultPath, nil); err != nil {
		t.Fatal(err)
	}
	defaultDB, err := openReceiver(context.Background(), defaultPath, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitFundingPageStore(context.Background(), defaultDB); err != nil {
		t.Fatal(err)
	}
	if err := defaultDB.Close(); err != nil {
		t.Fatal(err)
	}
	defaultDB, err = openReceiver(context.Background(), defaultPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer defaultDB.Close()
	g, err := monthlydraft.NewGeneration(receiver, defaultDB, root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	normalized, err := g.PrepareObservedInput(context.Background(), p, raw, "observations")
	if err != nil || json.Unmarshal(normalized.Plan, &p) != nil {
		t.Fatal("assembled material rejected by actual normalizers", err)
	}
	job, err := g.Submit(context.Background(), p, normalized.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := g.Run(context.Background(), p.Owner, job.ID)
	if err != nil || len(artifact.Reconciliation.Accounts) != 2 || artifact.TokenSHA256 == "" {
		t.Fatal("assembled material failed the independent draft audits", err)
	}
	want := []monitor.MonthlyPreparationAmounts{
		{Opening: "1000.000000", Increase: "0.000020", Decrease: "0.000000", Consume: "3.000000", Refund: "1.000000", Closing: "998.000020"},
		{Opening: "1000.000000", Increase: "0.000000", Decrease: "-0.000040", Consume: "3.000000", Refund: "1.000000", Closing: "997.999960"},
	}
	for i, account := range artifact.Reconciliation.Accounts {
		if account.Error != "" || account.Months[0].Amounts == nil || *account.Months[0].Amounts != want[i] {
			t.Fatal("per-account money changed", account)
		}
	}
	again, err := g.Run(context.Background(), p.Owner, job.ID)
	if err != nil || !reflect.DeepEqual(again, artifact) {
		t.Fatal("reused material changed the cached bill", err)
	}
}

func TestObservationMaterialBadInputsAndInterruptedWritesNeverComplete(t *testing.T) {
	root, request := observationMaterialFixture(t)
	for _, kind := range []string{"version", "index_hash", "funding_hash", "unknown_field", "missing_index", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			p := request
			ctx := context.Background()
			switch kind {
			case "version":
				p.Version = 2
			case "index_hash":
				p.ObservationIndex.SHA256 = strings.Repeat("b", 64)
			case "funding_hash":
				p.FundingAssembly.ManifestSHA256 = strings.Repeat("b", 64)
			case "missing_index":
				p.ObservationIndex.Path = filepath.Join(root, "missing", "observation-index.json")
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw, _ := json.Marshal(p)
			if kind == "unknown_field" {
				raw = append(raw[:len(raw)-1], []byte(`,"approved":true}`)...)
			}
			r, err := assembleObservationMonth(ctx, root, "bad-"+kind, raw, true)
			if err == nil || r.MaterialSHA256 != "" {
				t.Fatal("bad selection completed", err)
			}
			if _, err := os.Lstat(filepath.Join(root, "bad-"+kind)); !os.IsNotExist(err) {
				t.Fatal("invalid preflight wrote files", err)
			}
		})
	}
	raw, _ := json.Marshal(request)
	for _, kind := range []string{"disk", "corrupt", "changed_request", "source_wal"} {
		t.Run(kind, func(t *testing.T) {
			write := func(dir *os.Root, name string, body []byte) error {
				if name == "materials.pending.json" && kind == "disk" {
					return errors.New("synthetic disk failure")
				}
				if name == "materials.pending.json" && kind == "corrupt" {
					body = []byte("{}")
				}
				if err := writeObservationFile(dir, name, body); err != nil {
					return err
				}
				if name == "materials.pending.json" && kind == "changed_request" {
					return os.WriteFile(filepath.Join(root, "failed-"+kind, "request.json"), []byte("{}"), 0600)
				}
				if name == "materials.pending.json" && kind == "source_wal" {
					path := filepath.Join(request.FundingAssembly.Directory, "funding.sqlite-wal")
					t.Cleanup(func() { _ = os.Remove(path) })
					return os.WriteFile(path, nil, 0600)
				}
				return nil
			}
			r, err := assembleObservationMonthWithWrite(context.Background(), root, "failed-"+kind, raw, true, write)
			if err == nil || r.MaterialSHA256 != "" {
				t.Fatal("failed output obtained completion", err)
			}
			if _, err := os.Lstat(filepath.Join(root, "failed-"+kind, "materials.json")); !os.IsNotExist(err) {
				t.Fatal("failed output has a completion artifact", err)
			}
			if _, err := assembleObservationMonth(context.Background(), root, "failed-"+kind, raw, true); err == nil {
				t.Fatal("partial output overwritten")
			}
		})
	}
	if !bytes.Equal(raw, mustObservationAssemblyRequest(t, request)) {
		t.Fatal("caller request mutated")
	}
}

func TestObservationMaterialRejectsRehashedButInconsistentEvidence(t *testing.T) {
	root, original := observationMaterialFixture(t)
	for _, kind := range []string{"different_group", "one_quota_drift", "unknown_source", "missing_wallet_user"} {
		t.Run(kind, func(t *testing.T) {
			request := original
			if kind == "different_group" {
				path := filepath.Join(request.FundingAssembly.Directory, "assembly.json")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var receipt monthAssemblyReceipt
				if err := json.Unmarshal(before, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.GroupID = 2
				body, _ := json.Marshal(receipt)
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.WriteFile(path, before, 0600) })
				request.FundingAssembly.ManifestSHA256 = hash(body)
			} else if kind == "missing_wallet_user" {
				request = rewriteMaterialCapture(t, request, func(index *observationMonthSelection) *assemblyCapture { return &index.Wallet[0] },
					func(body []byte) []byte {
						var o monthlysource.WalletObservation
						if err := json.Unmarshal(body, &o); err != nil {
							t.Fatal(err)
						}
						o.Users[0] = monthlysource.WalletUser{UserID: o.Users[0].UserID}
						out, _ := json.Marshal(o)
						return out
					})
			} else {
				mutate := func(body []byte) []byte {
					var o monthlysource.TokenUsageWindowObservation
					if err := json.Unmarshal(body, &o); err != nil {
						t.Fatal(err)
					}
					if kind == "one_quota_drift" {
						o.Rows[0].Quota++
						o.Rows[0].MinQuota++
						o.Rows[0].MaxQuota++
						o.Totals[0] = o.Rows[0].UsageWindowRow
					} else {
						o.Rows[0].BillingSource, o.Totals[0].BillingSource = "unspecified", "unspecified"
					}
					if err := monthlysource.ValidateTokenUsageWindowObservation(o); err != nil {
						t.Fatal("counterexample must remain a valid capture", err)
					}
					out, _ := json.Marshal(o)
					return out
				}
				request = rewriteMaterialCapture(t, request, func(index *observationMonthSelection) *assemblyCapture { return &index.TokenUsage[0].Newer }, mutate)
				if kind == "unknown_source" {
					// Matching unknown sources still must NOT be assumed to be wallet.
					request = rewriteMaterialCapture(t, request, func(index *observationMonthSelection) *assemblyCapture { return &index.TokenUsage[0].Older }, mutate)
				}
			}
			raw, _ := json.Marshal(request)
			r, err := assembleObservationMonth(context.Background(), root, kind, raw, true)
			if err == nil || r.MaterialSHA256 != "" {
				t.Fatal("self-consistent hashes bypassed real source reconciliation", err)
			}
			if _, err := os.Lstat(filepath.Join(root, kind)); !os.IsNotExist(err) {
				t.Fatal("bad semantic evidence reached output creation", err)
			}
		})
	}
}

// Test-owned synthetic files only. Rehash every envelope on purpose so these
// counterexamples exercise semantic audits rather than a trivial SHA mismatch.
func rewriteMaterialCapture(t *testing.T, request observationMonthAssemblyRequest, selectRef func(*observationMonthSelection) *assemblyCapture,
	mutate func([]byte) []byte) observationMonthAssemblyRequest {
	t.Helper()
	indexBefore, err := privateRead(request.ObservationIndex.Path, 2<<20)
	var index observationMonthSelection
	if err != nil || json.Unmarshal(indexBefore, &index) != nil {
		t.Fatal(err)
	}
	ref := selectRef(&index)
	before, err := privateRead(filepath.Join(ref.Directory, "observation.json"), monthlysource.TokenUsageObservationLimit)
	if err != nil {
		t.Fatal(err)
	}
	markerBefore, err := readAssemblyManifest(ref.Directory)
	var marker observationReceipt
	if err != nil || json.Unmarshal(markerBefore, &marker) != nil {
		t.Fatal(err)
	}
	body := mutate(bytes.Clone(before))
	marker.ObservationSHA256 = hash(body)
	markerRaw, _ := json.Marshal(marker)
	directory := ref.Directory
	for _, file := range []struct {
		name string
		raw  []byte
	}{{"observation.json", body}, {"manifest.json", markerRaw}} {
		if err := os.WriteFile(filepath.Join(directory, file.name), file.raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ref.ManifestSHA256 = hash(markerRaw)
	indexRaw, _ := json.Marshal(index)
	if err := os.WriteFile(request.ObservationIndex.Path, indexRaw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(directory, "observation.json"), before, 0600)
		_ = os.WriteFile(filepath.Join(directory, "manifest.json"), markerBefore, 0600)
		_ = os.WriteFile(request.ObservationIndex.Path, indexBefore, 0600)
	})
	request.ObservationIndex.SHA256 = hash(indexRaw)
	return request
}

func mustObservationAssemblyRequest(t *testing.T, request observationMonthAssemblyRequest) []byte {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
