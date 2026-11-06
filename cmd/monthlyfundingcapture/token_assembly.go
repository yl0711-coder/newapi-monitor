package main

// Explicit offline assembly of retained captures only. No credentials,
// source database, Monitor store, task scheduling or publication is used.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type tokenMonthAssemblyPlan struct {
	Version        int                        `json:"version"`
	SourceIdentity string                     `json:"source_identity"`
	Rules          monthlysource.FundingRules `json:"rules"`
	UserID         int64                      `json:"user_id"`
	Month          string                     `json:"month"`
	Windows        []assemblyUsagePair        `json:"windows"`
}

type tokenMonthAssemblyReceipt struct {
	Version       int    `json:"version"`
	Mode          string `json:"mode"`
	PlanSHA256    string `json:"plan_sha256"`
	ReportSHA256  string `json:"report_sha256"`
	ArchiveSHA256 string `json:"archive_sha256"`
	Captures      int    `json:"captures"`
}

func runTokenMonthAssembly(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("assemble-token-usage", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private canonical output workspace")
	name := fs.String("output", "", "NEW output directory; never overwritten")
	path := fs.String("plan", "", "private exact source selection")
	expected := fs.String("expected-plan", "", "explicit source plan SHA256")
	create := fs.Bool("create-output", false, "write new local report; default checks only")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !validSHA(*expected) {
		return errInput
	}
	raw, err := privateRead(*path, 2<<20)
	var p tokenMonthAssemblyPlan
	if err != nil || hash(raw) != *expected || json.Unmarshal(raw, &p) != nil {
		return errInput
	}
	r, err := assembleTokenMonth(ctx, *workspace, *name, p, raw, *create)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(r)
}

func assembleTokenMonth(parent context.Context, workspace, name string, p tokenMonthAssemblyPlan, raw []byte, create bool) (tokenMonthAssemblyReceipt, error) {
	return assembleTokenMonthWithWrite(parent, workspace, name, p, raw, create, writeObservationFile)
}

func assembleTokenMonthWithWrite(parent context.Context, workspace, name string, p tokenMonthAssemblyPlan, raw []byte, create bool, writeFile func(*os.Root, string, []byte) error) (result tokenMonthAssemblyReceipt, err error) {
	var empty tokenMonthAssemblyReceipt
	canonical, err := json.Marshal(p)
	if parent == nil || writeFile == nil || err != nil || !bytes.Equal(raw, canonical) || p.Version != 1 || len(p.Windows) == 0 || len(p.Windows) > 744 {
		return empty, errInput
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	rootPath, output, err := jobPath(workspace, name)
	if err != nil || rootPath != workspace {
		return empty, errInput
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return empty, errInput
	}
	pairs := make([]monthlysource.TokenUsageWindowPair, 0, len(p.Windows))
	bytesRead := 0
	for _, window := range p.Windows {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		var pair monthlysource.TokenUsageWindowPair
		for i, ref := range []assemblyCapture{window.Older, window.Newer} {
			payload, err := readAssemblyCapture(ref, "observe-token-usage")
			if err != nil {
				return empty, err
			}
			bytesRead += len(payload)
			if bytesRead > 64<<20 {
				return empty, errInput
			}
			target := &pair.Older
			if i == 1 {
				target = &pair.Newer
			}
			if err := json.Unmarshal(payload, target); err != nil {
				return empty, err
			}
		}
		pairs = append(pairs, pair)
	}
	request := monthlysource.FundingMonthRequest{Month: p.Month, SourceIdentity: p.SourceIdentity, Rules: p.Rules, UserIDs: []int64{p.UserID}}
	report, err := monthlysource.SummarizeTokenUsageMonth(ctx, request, pairs, time.Now())
	if err != nil {
		return empty, err
	}
	reportRaw, err := json.Marshal(report)
	if err != nil {
		return empty, err
	}
	archive := monthlysource.TokenUsageMonthArchive{Version: 1, Basis: monthlysource.TokenUsageArchiveBasis, SelectionSHA256: hash(raw), SourceIdentity: p.SourceIdentity, Rules: p.Rules, UserID: p.UserID, Month: p.Month, Pairs: pairs}
	archiveRaw, err := json.Marshal(archive)
	if err != nil || len(archiveRaw) > monthlysource.TokenUsageArchiveLimit {
		return empty, errInput
	}
	r := tokenMonthAssemblyReceipt{Version: 1, Mode: "checked_retained_token_month_no_output", PlanSHA256: hash(raw), ReportSHA256: hash(reportRaw), ArchiveSHA256: hash(archiveRaw), Captures: len(p.Windows) * 2}
	if !create {
		return r, nil
	}
	lock, err := workspaceLock(rootPath, true, true)
	if err != nil {
		return empty, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return empty, err
	}
	defer root.Close()
	if err := observationRootCurrent(root, rootPath); err != nil {
		return empty, err
	}
	if _, err := root.Lstat(name); !os.IsNotExist(err) {
		return empty, errInput
	}
	if err := ctx.Err(); err != nil {
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
	for _, file := range []struct {
		name string
		body []byte
	}{{"plan.json", raw}, {"token-month.json", reportRaw}, {"token-evidence.json", archiveRaw}} {
		if err := writeFile(dir, file.name, file.body); err != nil {
			return empty, err
		}
		if err := verifyObservationFile(dir, file.name, file.body); err != nil {
			return empty, err
		}
	}
	if err := observationRootCurrent(root, rootPath); err != nil {
		return empty, err
	}
	before, e1 := dir.Stat(".")
	current, e2 := root.Lstat(name)
	if e1 != nil || e2 != nil || !os.SameFile(before, current) {
		return empty, errInput
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	r.Mode = "sealed_local_token_month_not_bill_approval"
	seal, err := json.Marshal(r)
	if err != nil {
		return empty, err
	}
	if err := writeFile(dir, "manifest.pending.json", seal); err != nil {
		return empty, err
	}
	for _, file := range []struct {
		name string
		body []byte
	}{{"plan.json", raw}, {"token-month.json", reportRaw}, {"token-evidence.json", archiveRaw}, {"manifest.pending.json", seal}} {
		if err := verifyObservationFile(dir, file.name, file.body); err != nil {
			return empty, err
		}
	}
	if err := errors.Join(syncObservationRoot(dir), syncObservationRoot(root), ctx.Err()); err != nil {
		return empty, err
	}
	if err := dir.Link("manifest.pending.json", "manifest.json"); err != nil {
		return empty, err
	}
	return r, errors.Join(syncObservationRoot(dir), syncObservationRoot(root), ctx.Err())
}
