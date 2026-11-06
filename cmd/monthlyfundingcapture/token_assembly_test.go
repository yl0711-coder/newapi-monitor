package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func tokenAssemblyFixture(t *testing.T) (string, tokenMonthAssemblyPlan, []byte) {
	t.Helper()
	workspace, request, raw := tokenObservationFixture(t)
	var seed monthlysource.TokenUsageWindowObservation
	if err := json.Unmarshal(raw, &seed); err != nil {
		t.Fatal(err)
	}
	p := tokenMonthAssemblyPlan{Version: 1, SourceIdentity: request.SourceIdentity, Rules: seed.Request.Rules, UserID: 201, Month: "2026-09", Windows: []assemblyUsagePair{}}
	for day := 0; day < 30; day++ {
		from := int64(1788192000 + day*86400)
		request.From, request.To = from, from+86400
		var refs [2]assemblyCapture
		for repeat := 0; repeat < 2; repeat++ {
			o := seed
			o.Request = usageObservationRequest(request)
			o.StartedAtMS = time.Now().Add(-20*time.Minute).UnixMilli() + int64(day)*1000 + int64(repeat)*300001
			o.CompletedAtMS = o.StartedAtMS + 1
			o.Rows = append([]monthlysource.TokenUsageWindowRow{}, seed.Rows...)
			o.Totals = append([]monthlysource.UsageWindowRow{}, seed.Totals...)
			for i := range o.Rows {
				r := &o.Rows[i]
				r.MinID = int64(day*2 + i + 11)
				r.MaxID = r.MinID
				r.FirstAt = from + int64(i+1)
				r.LastAt = r.FirstAt
				o.Totals[i] = r.UsageWindowRow
			}
			payload, err := json.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			capture := func(context.Context, string, observationRequest) ([]byte, error) { return payload, nil }
			receipt, err := observeOne(context.Background(), workspace, fmt.Sprintf("day-%02d-%d", day+1, repeat), "/synthetic-only", request, true, capture)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := os.ReadFile(filepath.Join(receipt.Directory, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			refs[repeat] = assemblyCapture{Directory: receipt.Directory, ManifestSHA256: hash(manifest)}
		}
		p.Windows = append(p.Windows, assemblyUsagePair{Older: refs[0], Newer: refs[1]})
	}
	plan, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return workspace, p, plan
}

func TestTokenMonthAssemblySealedSourcesDefaultCreateAndNoOverwrite(t *testing.T) {
	workspace, p, raw := tokenAssemblyFixture(t)
	ctx := context.Background()
	dry, err := assembleTokenMonth(ctx, workspace, "result", p, raw, false)
	if err != nil || dry.Mode != "checked_retained_token_month_no_output" || dry.Captures != 60 {
		t.Fatal(dry, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "result")); !os.IsNotExist(err) {
		t.Fatal("default created output")
	}
	created, err := assembleTokenMonth(ctx, workspace, "result", p, raw, true)
	if err != nil || created.ReportSHA256 != dry.ReportSHA256 || created.PlanSHA256 != dry.PlanSHA256 || created.Mode != "sealed_local_token_month_not_bill_approval" {
		t.Fatal(created, err)
	}
	path := filepath.Join(workspace, "result", "token-month.json")
	content, err := privateRead(path, 2<<20)
	if err != nil || hash(content) != created.ReportSHA256 {
		t.Fatal("sealed report changed", err)
	}
	var report monthlysource.TokenUsageMonthReport
	if json.Unmarshal(content, &report) != nil || report.ConsumeQuota != int64(30)*9007199254740993 || report.RefundQuota != 15000000 || len(report.Rows) != 1 || len(report.Sources) != 60 {
		t.Fatal("exact monthly token amounts changed", report)
	}
	archiveRaw, err := privateRead(filepath.Join(workspace, "result", "token-evidence.json"), monthlysource.TokenUsageArchiveLimit)
	if err != nil || hash(archiveRaw) != created.ArchiveSHA256 || created.ArchiveSHA256 != dry.ArchiveSHA256 {
		t.Fatal("original token archive not pinned", err)
	}
	archive, rebuilt, err := monthlysource.DecodeTokenUsageMonthArchive(ctx, archiveRaw, time.Now())
	rebuiltRaw, _ := json.Marshal(rebuilt)
	if err != nil || !bytes.Equal(rebuiltRaw, content) || archive.SelectionSHA256 != created.PlanSHA256 {
		t.Fatal("generation archive cannot rebuild sealed report", err)
	}
	manifest, err := privateRead(filepath.Join(workspace, "result", "plan.json"), 2<<20)
	if err != nil || !bytes.Equal(manifest, raw) {
		t.Fatal("source plan not retained", err)
	}
	if _, err := assembleTokenMonth(ctx, workspace, "result", p, raw, true); err == nil {
		t.Fatal("sealed report overwritten")
	}
	for _, window := range p.Windows {
		for _, ref := range []assemblyCapture{window.Older, window.Newer} {
			if _, err := readAssemblyCapture(ref, "observe-token-usage"); err != nil {
				t.Fatal("assembly changed original capture", err)
			}
		}
	}
}

func TestTokenMonthAssemblyRejectsChangedMissingAndUnpinnedSources(t *testing.T) {
	workspace, p, _ := tokenAssemblyFixture(t)
	for _, scenario := range []string{"missing_day", "wrong_manifest", "duplicate_day", "wrong_identity", "wrong_account", "noncanonical", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			copy := p
			copy.Windows = append([]assemblyUsagePair{}, p.Windows...)
			ctx := context.Background()
			switch scenario {
			case "missing_day":
				copy.Windows = copy.Windows[:29]
			case "wrong_manifest":
				copy.Windows[0].Older.ManifestSHA256 = copy.Windows[1].Older.ManifestSHA256
			case "duplicate_day":
				copy.Windows = append(copy.Windows, copy.Windows[0])
			case "wrong_identity":
				copy.SourceIdentity = "invalid"
			case "wrong_account":
				copy.UserID++
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw, _ := json.Marshal(copy)
			if scenario == "noncanonical" {
				raw = append(raw, ' ')
			}
			if receipt, err := assembleTokenMonth(ctx, workspace, "bad-"+scenario, copy, raw, true); err == nil || receipt.ReportSHA256 != "" {
				t.Fatal("invalid source produced result", receipt, err)
			}
			if _, err := os.Stat(filepath.Join(workspace, "bad-"+scenario, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("bad source sealed")
			}
		})
	}
	writeFailure := func(root *os.Root, name string, body []byte) error {
		if name == "manifest.pending.json" {
			return errors.New("synthetic disk failure after report write")
		}
		return writeObservationFile(root, name, body)
	}
	raw, _ := json.Marshal(p)
	if _, err := assembleTokenMonthWithWrite(context.Background(), workspace, "partial", p, raw, true, writeFailure); err == nil {
		t.Fatal("failed final write was accepted")
	}
	if _, err := os.Stat(filepath.Join(workspace, "partial", "token-month.json")); err != nil {
		t.Fatal("failure did not exercise real partial output", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "partial", "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("failed report received completion seal")
	}
	if _, err := assembleTokenMonth(context.Background(), workspace, "partial", p, raw, true); err == nil {
		t.Fatal("partial output silently overwritten")
	}
}
