//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

func giftSeriesFixture(t *testing.T) (FinanceGiftSeriesPlan, string, string) {
	t.Helper()
	ctx := context.Background()
	m := newFinanceReportTestMonitor(t, "series.example")
	db := m.usageFactsStore()
	hour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	var evidence []financeGiftLocalEvidence
	for i := 0; i < 21; i++ {
		h := hour + int64(i)*3600
		e := FinanceGiftBoundaryEvent{SourceLogID: int64(i + 1), HourTs: h, UserID: 7, EventAt: h + 20, Kind: "usage", Quota: int64(i + 1)}
		e.EvidenceHash = financeGiftBoundaryEventHash(e)
		if _, err = replaceFinanceUserHourFacts(ctx, db, h, "v1", []FinanceUserHourFact{{HourTs: h, UserID: 7, Requests: 1, ConsumeQuota: e.Quota}}, h+3600); err != nil {
			t.Fatal(err)
		}
		if _, err = replaceFinanceGiftBoundaryUserHour(ctx, db, h, 7, h+3600, "v1", []FinanceGiftBoundaryEvent{e}); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(t.TempDir(), "evidence.json")
		giftLargeWriteTestJSON(t, p, map[string]any{"user_id": 7, "hour_ts": h, "rows": []map[string]any{{"id": e.SourceLogID, "user_id": 7, "created_at": e.EventAt, "type": 2, "quota": e.Quota, "group": "business"}}})
		var item financeGiftLocalEvidence
		if _, err = giftLocalReadJSON(p, &item); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
		evidence = append(evidence, item)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err = db.Exec("VACUUM INTO ?", backup).Error; err != nil {
		t.Fatal(err)
	}
	prior, confirmation := giftCandidateJob(t, backup, paths[:1])
	noWait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	if _, err = runFinanceGiftLocalJob(ctx, prior, confirmation, noWait); err != nil {
		t.Fatal(err)
	}
	preview := filepath.Join(t.TempDir(), "preview.db")
	if _, err = giftLocalCopyBackup(filepath.Join(prior, "usage-facts.db"), preview); err != nil {
		t.Fatal(err)
	}
	previewDB, closeDB, err := giftLocalDatabase(preview)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	source, err := giftLocalSource(ctx, evidence)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	plan := FinanceGiftSeriesPlan{Version: 1, PriorDir: prior, PriorSHA256: confirmation, MaxBatches: 2, MaxRows: 20, RuntimeSeconds: 60}
	for stage := 0; stage < 2; stage++ {
		candidate, err := giftLocalSelectCandidates(ctx, previewDB, "v1", time.Now().Unix())
		if err != nil {
			t.Fatal(err)
		}
		readPlan, readHash, err := giftScopeReadPlanFromCandidates(candidate)
		if err != nil {
			t.Fatal(err)
		}
		export := t.TempDir()
		if err := os.Chmod(export, 0700); err != nil {
			t.Fatal(err)
		}
		giftLargeWriteTestJSON(t, filepath.Join(export, "plan.json"), readPlan)
		result := financegiftexport.BatchResult{Status: "complete"}
		var manifest []string
		for j, target := range readPlan.Targets {
			index := int((target.HourTs - hour) / 3600)
			p := filepath.Join(export, giftLocalEvidenceName(j))
			data, err := json.Marshal(evidence[index])
			if err != nil {
				t.Fatal(err)
			}
			if err = giftLocalWriteNew(p, data); err != nil {
				t.Fatal(err)
			}
			manifest = append(manifest, p)
			result.Entries = append(result.Entries, financegiftexport.BatchEntry{BatchTarget: target, File: giftLocalEvidenceName(j), SHA256: giftLocalDigest(data)})
			if _, err = repairFinanceGiftBoundaryScope(ctx, previewDB, source, "v1", target.HourTs, target.UserID, time.Now().Unix()); err != nil {
				t.Fatal(err)
			}
		}
		giftLargeWriteTestJSON(t, filepath.Join(export, "evidence-manifest.json"), manifest)
		giftLargeWriteTestJSON(t, filepath.Join(export, "result.json"), result)
		bytes, err := giftLocalReadJSON(filepath.Join(export, "result.json"), &result)
		if err != nil {
			t.Fatal(err)
		}
		plan.Steps = append(plan.Steps, FinanceGiftSeriesStep{ExportDir: export, ReadSHA256: readHash, ResultSHA256: giftLocalDigest(bytes), NextDir: filepath.Join(t.TempDir(), fmt.Sprintf("batch-%d", stage))})
	}
	path := filepath.Join(t.TempDir(), "series.json")
	giftLargeWriteTestJSON(t, path, plan)
	check, err := InspectFinanceGiftLocalSeries(path)
	if err != nil {
		t.Fatal(err)
	}
	return plan, path, check.Confirmation
}
