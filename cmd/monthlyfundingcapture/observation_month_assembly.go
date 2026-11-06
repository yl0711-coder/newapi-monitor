package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Both inputs are completed private artifacts with operator-retained hashes.
// No connection configuration or editable monetary values are accepted here.
type observationMonthAssemblyRequest struct {
	Version          int             `json:"version"`
	ObservationIndex assemblyFile    `json:"observation_index"`
	FundingAssembly  assemblyCapture `json:"funding_assembly"`
}

type observationMonthAssemblyReceipt struct {
	Mode           string `json:"mode"`
	RequestSHA256  string `json:"request_sha256"`
	GroupID        int64  `json:"group_id"`
	Month          string `json:"month"`
	Accounts       int    `json:"accounts"`
	UsageWindows   int    `json:"usage_windows"`
	MaterialSHA256 string `json:"material_sha256,omitempty"`
}

type observationMonthAssemblyInputs struct {
	Index       observationMonthSelection
	Funding     monthAuditPlan
	Receipt     monthAssemblyReceipt
	Observation observationAssemblyPlan
	Tokens      []tokenMonthAssemblyPlan
}

func runObservationMonthAssembly(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("assemble-observation-month", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private output workspace")
	name := fs.String("output", "", "NEW output name; never overwritten")
	path := fs.String("request", "", "private canonical artifact selections")
	expected := fs.String("expected-request", "", "retained selection SHA256")
	create := fs.Bool("create-output", false, "create private data-only materials; default read-only inspection")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !validSHA(*expected) {
		return errInput
	}
	raw, err := privateRead(*path, 65536)
	if err != nil || hash(raw) != *expected {
		return errInput
	}
	r, err := assembleObservationMonth(ctx, *workspace, *name, raw, *create)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(r)
}

func loadObservationMonthAssembly(ctx context.Context, raw []byte) (observationMonthAssemblyInputs, error) {
	var in observationMonthAssemblyInputs
	var request observationMonthAssemblyRequest
	if len(raw) == 0 || len(raw) > 65536 || json.Unmarshal(raw, &request) != nil {
		return in, errInput
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, raw) || request.Version != 1 || !validSHA(request.ObservationIndex.SHA256) ||
		!validSHA(request.FundingAssembly.ManifestSHA256) || filepath.Base(request.ObservationIndex.Path) != "observation-index.json" {
		return in, errInput
	}
	indexRaw, err := privateRead(request.ObservationIndex.Path, 2<<20)
	if err != nil || hash(indexRaw) != request.ObservationIndex.SHA256 || json.Unmarshal(indexRaw, &in.Index) != nil {
		return in, errInput
	}
	// Rebuild the index from its original immutable schedule and captures. A
	// matching JSON digest alone is not evidence of completeness or scope.
	state, err := inspectObservationMonth(ctx, filepath.Dir(request.ObservationIndex.Path), in.Index.PlanSHA256, time.Now())
	if err != nil {
		return in, err
	}
	wantIndex, err := observationMonthIndex(state)
	if err != nil || !bytes.Equal(indexRaw, wantIndex) {
		return in, errInput
	}
	dir := request.FundingAssembly.Directory
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil || resolved != dir || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return in, errInput
	}
	if _, err := privateInfo(dir, true); err != nil {
		return in, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "assembly.pending.json")); !os.IsNotExist(err) {
		return in, errInput
	}
	fundingRaw, err := privateRead(filepath.Join(dir, "assembly.json"), 4096)
	if err != nil || hash(fundingRaw) != request.FundingAssembly.ManifestSHA256 || json.Unmarshal(fundingRaw, &in.Receipt) != nil {
		return in, errInput
	}
	canonical, err = json.Marshal(in.Receipt)
	if err != nil || !bytes.Equal(canonical, fundingRaw) || in.Receipt.Version != 1 || in.Receipt.Mode != "assembled_local_evidence_not_bill_approval" ||
		!validSHA(in.Receipt.DatabaseSHA256) || !validSHA(in.Receipt.PlanSHA256) || !validSHA(in.Receipt.ReviewSHA256) {
		return in, errInput
	}
	planPath := filepath.Join(dir, "month-plan.json")
	in.Funding, _, err = readMonthPlan(planPath, in.Receipt.PlanSHA256)
	if err != nil || in.Funding.GroupID != in.Index.GroupID || in.Receipt.GroupID != in.Index.GroupID ||
		in.Receipt.Month != in.Index.Funding.Month || !reflect.DeepEqual(in.Funding.Funding, in.Index.Funding) {
		return in, errInput
	}
	windows, err := validateMonthAuditPlan(ctx, in.Funding)
	if err != nil {
		return in, err
	}
	dbPath := filepath.Join(dir, "funding.sqlite")
	file := assemblyFile{Path: dbPath, SHA256: in.Receipt.DatabaseSHA256}
	if err := sealedAssemblySource(ctx, file); err != nil {
		return in, err
	}
	info, err := privateInfo(dbPath, false)
	if err != nil || info.Size() != in.Receipt.DatabaseBytes {
		return in, errInput
	}
	db, err := openReceiver(ctx, dbPath, false)
	if err != nil {
		return in, err
	}
	review, auditErr := reviewAssembledMonth(ctx, db, in.Funding, windows, in.Receipt.Pages, in.Receipt.Rows)
	if err := errors.Join(auditErr, db.Close()); err != nil {
		return in, err
	}
	check, err := json.Marshal(review)
	storedReview, readErr := privateRead(filepath.Join(dir, "review.json"), 8<<20)
	if err != nil || readErr != nil || hash(check) != in.Receipt.ReviewSHA256 || !bytes.Equal(check, storedReview) {
		return in, errInput
	}
	in.Observation = observationAssemblyPlan{Version: 1, FundingPlan: assemblyFile{planPath, in.Receipt.PlanSHA256}, FundingDB: file,
		TokenUsage: in.Index.TokenUsage, Wallet: in.Index.Wallet}
	for i, id := range in.Index.Funding.UserIDs {
		plan := tokenMonthAssemblyPlan{Version: 1, SourceIdentity: in.Index.Funding.SourceIdentity, Rules: in.Index.Funding.Rules, UserID: id, Month: in.Index.Funding.Month}
		for j := i; j < len(in.Index.TokenUsage); j += len(in.Index.Funding.UserIDs) {
			plan.Windows = append(plan.Windows, in.Index.TokenUsage[j])
		}
		in.Tokens = append(in.Tokens, plan)
	}
	return in, ctx.Err()
}

func assembleObservationMonth(parent context.Context, workspace, name string, raw []byte, create bool) (observationMonthAssemblyReceipt, error) {
	return assembleObservationMonthWithWrite(parent, workspace, name, raw, create, writeObservationFile)
}

func assembleObservationMonthWithWrite(parent context.Context, workspace, name string, raw []byte, create bool,
	writeFile func(*os.Root, string, []byte) error) (observationMonthAssemblyReceipt, error) {
	var empty observationMonthAssemblyReceipt
	if parent == nil || writeFile == nil || len(raw) == 0 || len(raw) > 65536 {
		return empty, errInput
	}
	raw = bytes.Clone(raw) // Keep the pinned selection detached from caller-owned memory.
	ctx, cancel := context.WithTimeout(parent, 35*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	workspacePath, output, err := jobPath(workspace, name)
	if err != nil || workspacePath != workspace {
		return empty, errInput
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return empty, errInput
	}
	in, err := loadObservationMonthAssembly(ctx, raw)
	if err != nil {
		return empty, err
	}
	planRaw, err := json.Marshal(in.Observation)
	if err != nil {
		return empty, err
	}
	preview, err := assembleObservations(ctx, workspace, name, in.Observation, planRaw, false)
	if err != nil {
		return empty, err
	}
	tokenPreviews := make([]tokenMonthAssemblyReceipt, 0, len(in.Tokens))
	for _, token := range in.Tokens {
		body, err := json.Marshal(token)
		if err != nil {
			return empty, err
		}
		r, err := assembleTokenMonth(ctx, workspace, name, token, body, false)
		if err != nil {
			return empty, err
		}
		tokenPreviews = append(tokenPreviews, r)
	}
	result := observationMonthAssemblyReceipt{Mode: "checked_month_observation_material_no_output", RequestSHA256: hash(raw),
		GroupID: in.Index.GroupID, Month: in.Index.Funding.Month, Accounts: len(in.Tokens), UsageWindows: len(in.Index.TokenUsage)}
	if !create {
		return result, ctx.Err()
	}
	lock, err := workspaceLock(workspace, true, true)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return empty, err
	}
	defer root.Close()
	if err := observationRootCurrent(root, workspace); err != nil {
		return empty, err
	}
	if err := root.Mkdir(name, 0700); err != nil {
		return empty, err
	}
	dir, err := root.OpenRoot(name)
	if err != nil {
		return empty, err
	}
	defer dir.Close()
	if err := writeFile(dir, "request.json", raw); err != nil {
		return empty, err
	}
	if err := verifyObservationFile(dir, "request.json", raw); err != nil {
		return empty, err
	}
	observed, err := assembleObservations(ctx, output, "observations", in.Observation, planRaw, true)
	if err != nil || observed.ReviewSHA256 != preview.ReviewSHA256 || observed.SelectionSHA256 != preview.SelectionSHA256 ||
		observed.FundingPages != in.Receipt.Pages || observed.FundingRows != in.Receipt.Rows {
		return empty, errors.Join(errInput, err)
	}
	selectionRaw, err := privateRead(filepath.Join(output, "observations", "selection.json"), 2<<20)
	var selection observationAssemblySelection
	if err != nil || hash(selectionRaw) != observed.SelectionSHA256 || json.Unmarshal(selectionRaw, &selection) != nil {
		return empty, errInput
	}
	material := monthlysource.MonthlyObservationMaterial{Version: 1, Mode: monthlysource.MonthlyObservationMaterialMode, GroupID: selection.GroupID,
		FundingVersion: in.Funding.Version, Funding: selection.Funding, FundingWindows: selection.FundingWindows, UsageWindows: selection.UsageWindows,
		WalletIDs: selection.WalletIDs, Database: monthlysource.MonthlyObservationFile{Path: filepath.Join(output, "observations", "observations.sqlite"), SHA256: observed.DatabaseSHA256}}
	for i, token := range in.Tokens {
		body, err := json.Marshal(token)
		if err != nil {
			return empty, err
		}
		name := fmt.Sprintf("tokens-%d", token.UserID)
		r, err := assembleTokenMonth(ctx, output, name, token, body, true)
		if err != nil || r.ArchiveSHA256 != tokenPreviews[i].ArchiveSHA256 || r.ReportSHA256 != tokenPreviews[i].ReportSHA256 {
			return empty, errors.Join(errInput, err)
		}
		material.Tokens = append(material.Tokens, monthlysource.MonthlyObservationTokenFile{UserID: token.UserID,
			File: monthlysource.MonthlyObservationFile{Path: filepath.Join(output, name, "token-evidence.json"), SHA256: r.ArchiveSHA256}})
	}
	materialRaw, err := json.Marshal(material)
	if err != nil || len(materialRaw) > 2<<20 {
		return empty, errInput
	}
	if err := writeFile(dir, "materials.pending.json", materialRaw); err != nil {
		return empty, err
	}
	if err := verifyObservationFile(dir, "materials.pending.json", materialRaw); err != nil {
		return empty, err
	}
	if err := verifyObservationMaterialFiles(ctx, material); err != nil {
		return empty, err
	}
	// Pin original selections again after all output writes. Never silently
	// update source hashes if inputs changed during a long local assembly.
	if _, err := loadObservationMonthAssembly(ctx, raw); err != nil {
		return empty, err
	}
	for _, part := range []struct {
		name string
		raw  []byte
	}{{"request.json", raw}, {"materials.pending.json", materialRaw}} {
		if err := verifyObservationFile(dir, part.name, part.raw); err != nil {
			return empty, err
		}
	}
	if err := observationRootCurrent(root, workspace); err != nil {
		return empty, err
	}
	if err := observationRootCurrent(dir, output); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := dir.Link("materials.pending.json", "materials.json"); err != nil {
		return empty, err
	}
	if err := dir.Remove("materials.pending.json"); err != nil {
		return empty, err
	}
	result.Mode, result.MaterialSHA256 = "assembled_month_observation_material_not_bill_approval", hash(materialRaw)
	return result, errors.Join(syncObservationRoot(dir), syncObservationRoot(root), ctx.Err())
}

func verifyObservationMaterialFiles(ctx context.Context, material monthlysource.MonthlyObservationMaterial) error {
	if err := sealedAssemblySource(ctx, assemblyFile(material.Database)); err != nil {
		return err
	}
	info, err := privateInfo(material.Database.Path, false)
	if err != nil {
		return err
	}
	total := info.Size()
	for _, token := range material.Tokens {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := privateRead(token.File.Path, monthlysource.TokenUsageArchiveLimit)
		if err != nil || hash(raw) != token.File.SHA256 {
			return errInput
		}
		total += int64(len(raw))
		if total > assemblyDatabaseLimit {
			return errInput
		}
		archive, _, err := monthlysource.DecodeTokenUsageMonthArchive(ctx, raw, time.Now())
		if err != nil || archive.UserID != token.UserID || archive.SourceIdentity != material.Funding.SourceIdentity || archive.Month != material.Funding.Month || archive.Rules != material.Funding.Rules {
			return errInput
		}
	}
	return ctx.Err()
}
