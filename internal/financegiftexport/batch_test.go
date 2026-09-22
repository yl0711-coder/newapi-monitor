package financegiftexport

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func batchFixture() (BatchPlan, map[[2]int64][][]driver.Value) {
	p := BatchPlan{SourceEpoch: "test-epoch"}
	data := map[[2]int64][][]driver.Value{}
	for i := int64(0); i < 3; i++ {
		p.Targets = append(p.Targets, BatchTarget{UserID: 7 + i, HourTs: 3600, ExpectedRows: 1, LocalContentHash: strings.Repeat("a", 64)})
		data[[2]int64{7 + i, 3600}] = [][]driver.Value{{i + 1, 7 + i, int64(3610), int64(2), int64(100), "business"}}
	}
	return p, data
}

func TestBatchCompleteAndNoImplicitRetry(t *testing.T) {
	plan, data := batchFixture()
	db, state := giftExportTestDB(t, nil)
	state.targetData = data
	digest, err := BatchConfirmation(plan)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "batch")
	var waits []time.Duration
	got, err := exportBatch(context.Background(), db, plan, digest, dir, func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		// Caller-owned changes must not alter the frozen executing plan.
		plan.Targets[0].UserID = 999
		return ctx.Err()
	})
	if err != nil || got.Status != "complete" || got.Remaining != 0 || len(got.Entries) != 3 || len(state.queries) != 6 {
		t.Fatalf("%+v %v", got, err)
	}
	if !reflect.DeepEqual(waits, []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}) {
		t.Fatal("missing startup/inter-target cooldown", waits)
	}
	var manifest []string
	content, err := os.ReadFile(filepath.Join(dir, "evidence-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(content, &manifest); err != nil || len(manifest) != 3 {
		t.Fatal("invalid complete manifest", err)
	}
	for i, e := range got.Entries {
		if e.UserID != int64(7+i) || len(e.SHA256) != 64 || manifest[i] != filepath.Join(dir, e.File) {
			t.Fatal("wrong exported identity or manifest")
		}
		info, err := os.Stat(manifest[i])
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("evidence permissions", err)
		}
	}
	plan, _ = batchFixture()
	_, err = ExportBatch(context.Background(), db, plan, digest, dir)
	if err == nil || len(state.queries) != 6 {
		t.Fatal("existing batch directory retried source reads")
	}
}

func TestBatchFailureAndCancellationStopFurtherReads(t *testing.T) {
	for _, mode := range []string{"second_count_mismatch", "unsafe_plan", "startup_cancel", "between_cancel"} {
		t.Run(mode, func(t *testing.T) {
			plan, data := batchFixture()
			db, state := giftExportTestDB(t, nil)
			state.targetData = data
			if mode == "second_count_mismatch" {
				state.targetData[[2]int64{8, 3600}] = nil
			}
			if mode == "unsafe_plan" {
				state.planType = "ALL"
			}
			digest, _ := BatchConfirmation(plan)
			dir := filepath.Join(t.TempDir(), "batch")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := 0
			got, err := exportBatch(ctx, db, plan, digest, dir, func(ctx context.Context, d time.Duration) error {
				waits++
				if (mode == "startup_cancel" && waits == 1) || (mode == "between_cancel" && waits == 2) {
					cancel()
				}
				return ctx.Err()
			})
			if err == nil {
				t.Fatal("expected stopped batch")
			}
			wantEntries, wantQueries, wantStatus := 1, 4, "failed"
			switch mode {
			case "unsafe_plan":
				wantEntries, wantQueries = 0, 1
			case "startup_cancel":
				wantEntries, wantQueries, wantStatus = 0, 0, "paused"
			case "between_cancel":
				wantEntries, wantQueries, wantStatus = 1, 2, "paused"
			}
			if got.Status != wantStatus || len(got.Entries) != wantEntries || got.Remaining != 3-wantEntries || len(state.queries) != wantQueries {
				t.Fatalf("%+v queries=%d %v", got, len(state.queries), err)
			}
			if _, err := os.Stat(filepath.Join(dir, "evidence-manifest.json")); !os.IsNotExist(err) {
				t.Fatal("partial batch has complete manifest")
			}
			files, _ := filepath.Glob(filepath.Join(dir, "evidence-*.json"))
			if len(files) != wantEntries {
				t.Fatal("partial target file or lost completed evidence")
			}
			content, err := os.ReadFile(filepath.Join(dir, "result.json"))
			if err != nil {
				t.Fatal(err)
			}
			var persisted BatchResult
			if err = json.Unmarshal(content, &persisted); err != nil || persisted.Status != wantStatus {
				t.Fatal("missing stopped outcome", err)
			}
		})
	}
}

func TestBatchRejectsInvalidPlansBeforeFilesystemOrSource(t *testing.T) {
	for _, mode := range []string{"empty", "duplicate", "too_many", "row_budget", "zero_rows", "open_hour", "hash", "epoch", "confirmation"} {
		t.Run(mode, func(t *testing.T) {
			plan, _ := batchFixture()
			good, _ := BatchConfirmation(plan)
			switch mode {
			case "empty":
				plan.Targets = nil
			case "duplicate":
				plan.Targets[1] = plan.Targets[0]
			case "too_many":
				plan.Targets = make([]BatchTarget, 11)
			case "row_budget":
				plan.Targets[0].ExpectedRows = 3000
			case "zero_rows":
				plan.Targets[0].ExpectedRows = 0
			case "open_hour":
				plan.Targets[0].HourTs = time.Now().Unix() / 3600 * 3600
			case "hash":
				plan.Targets[0].LocalContentHash = "bad"
			case "epoch":
				plan.SourceEpoch = ""
			case "confirmation":
				good = "wrong"
			}
			db, state := giftExportTestDB(t, nil)
			dir := filepath.Join(t.TempDir(), "batch")
			got, err := ExportBatch(context.Background(), db, plan, good, dir)
			if err == nil || got.Status != "rejected" || len(state.queries) != 0 {
				t.Fatalf("invalid plan accepted: %+v %v", got, err)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("rejection created output directory")
			}
		})
	}
}

func TestBatchSourceFailureDoesNotPersistDriverError(t *testing.T) {
	plan, data := batchFixture()
	db, state := giftExportTestDB(t, nil)
	state.targetData = data
	state.readErr = errors.New("private database diagnostic")
	digest, _ := BatchConfirmation(plan)
	dir := filepath.Join(t.TempDir(), "batch")
	got, err := exportBatch(context.Background(), db, plan, digest, dir, func(context.Context, time.Duration) error { return nil })
	if err == nil || got.Status != "failed" {
		t.Fatal("source error lost")
	}
	content, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil || strings.Contains(string(content), "private database diagnostic") {
		t.Fatal("raw driver error persisted", err)
	}
}

func TestBatchRecordFailurePreservesExistingFileAndStops(t *testing.T) {
	plan, data := batchFixture()
	db, state := giftExportTestDB(t, nil)
	state.targetData, state.readErr = data, errors.New("fixture read failure")
	digest, _ := BatchConfirmation(plan)
	dir := filepath.Join(t.TempDir(), "batch")
	sentinel := []byte("existing record must not be overwritten")
	got, err := exportBatch(context.Background(), db, plan, digest, dir, func(context.Context, time.Duration) error {
		return os.WriteFile(filepath.Join(dir, "result.json"), sentinel, 0600)
	})
	if err == nil || got.Status != "record_failed" || len(state.queries) != 2 {
		t.Fatalf("record failure did not stop: %+v %v", got, err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil || !reflect.DeepEqual(content, sentinel) {
		t.Fatal("existing result overwritten", err)
	}
}
