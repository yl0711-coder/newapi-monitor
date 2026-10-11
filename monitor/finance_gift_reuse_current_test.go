//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// This is a finite offline rehearsal, not a production import command. A
// reviewed archive is pinned by its independently recorded hash; all writes
// go to a fresh testing directory. It cannot authorize a live handoff.
func TestFinanceGiftReuseCurrentSnapshot(t *testing.T) {
	mainPath, receiver := os.Getenv("MONITOR_GIFT_REUSE_MAIN"), os.Getenv("MONITOR_GIFT_REUSE_BEFORE")
	archive, archiveHash := os.Getenv("MONITOR_GIFT_REUSE_AFTER"), os.Getenv("MONITOR_GIFT_REUSE_ARCHIVE_SHA256")
	from, to := os.Getenv("MONITOR_GIFT_REUSE_FROM"), os.Getenv("MONITOR_GIFT_REUSE_TO")
	epoch := os.Getenv("MONITOR_GIFT_REUSE_EPOCH")
	if mainPath == "" || receiver == "" || archive == "" || archiveHash == "" || from == "" || to == "" || epoch == "" {
		t.Skip("requires closed current backups, reviewed archive/hash, epoch and inclusive report dates")
	}
	paths := []string{mainPath, receiver, archive}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	if hashes[2] != archiveHash {
		t.Fatal("reviewed archive hash mismatch")
	}
	t.Cleanup(func() {
		for i, path := range paths {
			if giftLocalFileHash(t, path) != hashes[i] {
				t.Error("input changed", i)
			}
		}
	})
	mainDB, closeMain, err := giftLocalReadonlyDatabase(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	source, closeSource, err := giftLocalReadonlyDatabase(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	copyPath := filepath.Join(t.TempDir(), "receiver.db")
	if _, err := giftLocalCopyBackup(receiver, copyPath); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	m := &Monitor{storeDB: mainDB, usageFactsDB: db, cfg: Settings{
		FinanceEnabled: true, FinanceStartDate: from, LocalSnapshotOnly: true, UsageFactsHistorySourceEpoch: epoch,
	}, backgroundCtx: ctx}
	defer m.financeGiftEvidenceGuard.close()
	defer func() {
		cancel()
		deadline := time.Now().Add(5 * time.Second)
		for m.financeReportRefreshRunning.Load() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if m.financeReportRefreshRunning.Load() {
			t.Error("background refresh did not stop")
		}
	}()
	before := giftReuseReadReport(t, m, from, to)
	moneyBefore := giftMixedMonetarySnapshot(t, db)
	seed, err := financeStartHour(from)
	if err != nil {
		t.Fatal(err)
	}
	end, err := financeStartHour(to)
	if err != nil {
		t.Fatal(err)
	}
	end += 24 * 3600
	updated, targets, batches := 0, 0, 0
	complete := false
	const maxBatches = 100 // At most 1,000 user-hours; offline acceptance only.
	for batch := 0; batch < maxBatches; batch++ {
		inspection, err := inspectFinanceGiftRelevantScope(ctx, m, epoch, seed, end)
		if err != nil {
			t.Fatal(err)
		}
		if inspection.Coverage.Complete {
			complete = true
			break
		}
		if inspection.Candidates.Status != "ready" || len(inspection.Candidates.Entries) == 0 {
			t.Fatalf("non-scope blocker: %+v", inspection.Coverage)
		}
		var batchTargets []financeGiftScopeTarget
		var verified [][]FinanceGiftBoundaryEvent
		for _, entry := range inspection.Candidates.Entries {
			target := financeGiftScopeTarget{SourceEpoch: epoch, HourTs: entry.HourTs, UserID: entry.UserID}
			proof, err := loadFinanceGiftScopeSnapshot(ctx, source, epoch, target.HourTs, target.UserID)
			if err != nil {
				t.Fatalf("archive lacks candidate proof: %v", err)
			}
			for _, event := range proof.Events {
				if !event.GroupKnown {
					t.Fatal("archive evidence incomplete")
				}
			}
			batchTargets = append(batchTargets, target)
			verified = append(verified, proof.Events)
		}
		preview, err := previewFinanceGiftHandoffTargets(ctx, db, batchTargets, verified)
		if err != nil || preview.BlockedTargets > 0 || preview.RowsToUpdate == 0 {
			t.Fatalf("archive/current matching blocked: %+v %v", preview, err)
		}
		batchUpdated := 0
		for i, target := range batchTargets {
			digest := financeGiftBoundaryContentHash(verified[i])
			result, err := applyFinanceGiftScopeHandoff(ctx, db, target, verified[i], digest, time.Now().Unix())
			if err != nil {
				t.Fatal(err)
			}
			batchUpdated += result.RowsUpdated
			replay, err := applyFinanceGiftScopeHandoff(ctx, db, target, verified[i], digest, time.Now().Unix())
			if err != nil || replay.RowsUpdated != 0 {
				t.Fatal("non-idempotent replay", err)
			}
		}
		if batchUpdated != preview.RowsToUpdate {
			t.Fatal("preview/application mismatch")
		}
		updated += batchUpdated
		targets += len(batchTargets)
		batches++
	}
	if !complete {
		t.Fatal("finite offline budget exhausted")
	}
	if giftMixedMonetarySnapshot(t, db) != moneyBefore {
		t.Fatal("money, identities or synchronization state changed")
	}
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("integrity", err)
	}
	after := giftReuseReadReport(t, m, from, to)
	if !after.GiftCoverage.Complete || after.GiftCoverage.ScopeUnknownEvents != 0 || after.Statement.OperatingRevenue == nil || after.Statement.RegistrationGiftConsumption == nil {
		t.Fatal("gift deduction or revenue still unavailable")
	}
	if !reflect.DeepEqual(before.Statement.UserConsumption, after.Statement.UserConsumption) || !reflect.DeepEqual(before.Statement.UserRefunds, after.Statement.UserRefunds) || !reflect.DeepEqual(before.Statement.KnownRawCorrectedUpstreamCost, after.Statement.KnownRawCorrectedUpstreamCost) {
		t.Fatal("gift reuse changed unrelated report amounts")
	}
	assertGiftReuseBreakdown(t, after.Statement, after.Periods, after.Days)
	if after.Statement.AWSInfrastructureCost == nil && after.Statement.OperatingProfit != nil {
		t.Fatal("missing cost became complete profit")
	}
	t.Logf("current snapshot: unknown=%d -> 0; updated=%d targets=%d batches=%d; months=%d days=%d; replay=0; original snapshots unchanged; profit_complete=%t", before.GiftCoverage.ScopeUnknownEvents, updated, targets, batches, len(after.Periods), len(after.Days), after.Statement.OperatingProfit != nil)
}

func giftReuseReadReport(t *testing.T, m *Monitor, from, to string) financeOperatingReport {
	t.Helper()
	var previous financeOperatingReport
	previousHit, staleObserved := false, false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/finance/report?from="+from+"&to="+to, nil)
		m.serveFinanceOperatingReport(c)
		if w.Code != http.StatusOK {
			t.Fatalf("report HTTP %d", w.Code)
		}
		var current financeOperatingReport
		if err := json.Unmarshal(w.Body.Bytes(), &current); err != nil {
			t.Fatal(err)
		}
		status := w.Header().Get("X-Monitor-Finance-Cache")
		switch status {
		case "hit":
			if previousHit {
				if !reflect.DeepEqual(previous, current) {
					t.Fatal("report cache mismatch")
				}
				t.Logf("report settled: stale_while_revalidate=%t", staleObserved)
				return current
			}
		case "miss", "refresh":
		case "stale-refreshing":
			staleObserved = true
		default:
			t.Fatalf("unexpected cache state: %s", status)
		}
		previous = current
		previousHit = status == "hit"
		// Match the designed stale-while-revalidate behavior: stale is usable,
		// but must eventually be replaced after evidence changes.
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("report did not settle within the bounded refresh window")
	return previous
}
