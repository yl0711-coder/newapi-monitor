//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// This finite local acceptance requires an explicit inventory. It neither
// discovers production sources nor authorizes an actual production repair.
func TestFinanceGiftReuseRetainedSeries(t *testing.T) {
	giftReuseRetainedSeries(t, os.Getenv("MONITOR_GIFT_REUSE_MAIN"))
}

func giftReuseRetainedSeries(t *testing.T, mainPath string) *Monitor {
	t.Helper()
	manifest, root := os.Getenv("MONITOR_GIFT_RETAINED_MANIFEST"), os.Getenv("MONITOR_GIFT_RETAINED_ROOT")
	backup := os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	epoch, from, to := os.Getenv("MONITOR_GIFT_REUSE_EPOCH"), os.Getenv("MONITOR_GIFT_REUSE_FROM"), os.Getenv("MONITOR_GIFT_REUSE_TO")
	if manifest == "" || root == "" || backup == "" || mainPath == "" || epoch == "" || from == "" || to == "" {
		t.Skip("requires explicit retained inventory and closed main/facts backups with report scope")
	}
	var inventory struct {
		Version int `json:"version"`
		Jobs    []struct {
			Dir    string `json:"dir"`
			SHA256 string `json:"sha256"`
		} `json:"jobs"`
	}
	if _, err := giftLocalReadJSON(manifest, &inventory); err != nil || inventory.Version != 1 || len(inventory.Jobs) == 0 || len(inventory.Jobs) > 50 {
		t.Fatal("invalid finite retained inventory", err)
	}
	originalHashes := []string{giftLocalFileHash(t, backup), giftLocalFileHash(t, mainPath), giftLocalFileHash(t, manifest)}
	t.Cleanup(func() {
		for i, path := range []string{backup, mainPath, manifest} {
			if giftLocalFileHash(t, path) != originalHashes[i] {
				t.Error("original input changed", i)
			}
		}
	})
	// All receiver writes (facts and authorizations) are confined to new copies.
	m, _, _ := giftPreviewHTTPMonitor(t, "", "", backup, epoch)
	attachGiftAuthorizationStore(t, m)
	m.cfg.FinanceGiftHandoffLiveExecutionEnabled = true
	m.cfg.ProdDSN, m.cfg.NewAPIBaseURL = "must-not-connect", "https://invalid.example"
	mainDB, closeMain, err := giftLocalReadonlyDatabase(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	reportMonitor := &Monitor{storeDB: mainDB, usageFactsDB: m.usageFactsDB, cfg: Settings{
		FinanceEnabled: true, FinanceStartDate: from, LocalSnapshotOnly: true, UsageFactsHistorySourceEpoch: epoch,
	}, backgroundCtx: ctx}
	defer reportMonitor.financeGiftEvidenceGuard.close()
	defer func() {
		cancel()
		deadline := time.Now().Add(5 * time.Second)
		for reportMonitor.financeReportRefreshRunning.Load() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if reportMonitor.financeReportRefreshRunning.Load() {
			t.Error("background report refresh did not stop")
		}
	}()
	before := giftReuseReadReport(t, reportMonitor, from, to)
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	updated, targets, readyJobs := 0, 0, 0
	seen := map[string]bool{}
	for index, item := range inventory.Jobs {
		if !filepath.IsLocal(item.Dir) || seen[item.Dir] {
			t.Fatal("unsafe or duplicate retained job")
		}
		seen[item.Dir] = true
		if !t.Run(fmt.Sprintf("batch-%02d", index), func(t *testing.T) {
			job, digest := giftRebuildRetainedBatch(t, filepath.Join(root, item.Dir), item.SHA256, backup)
			// A fresh worker models a restart between finite batches; durable
			// authorization/commit records and the receiver are shared, not locks.
			worker := &Monitor{cfg: m.cfg, storeDB: m.storeDB, usageFactsDB: m.usageFactsDB}
			worker.financeFactsReadDB.Store(m.financeFactsReadDB.Load())
			worker.cfg.FinanceGiftHandoffPreviewDir, worker.cfg.FinanceGiftHandoffPreviewSHA256 = job, digest
			preview, err := previewFinanceGiftConfiguredJob(ctx, worker.financeFactsReadStore(), worker.cfg)
			if err != nil || preview.BlockedTargets > 0 {
				t.Fatalf("blocked retained batch: %+v %v", preview, err)
			}
			if preview.ReadyTargets == 0 {
				t.Log("already matched; no authorization or writes")
				return
			}
			a := giftAuthorizeReuseBatch(t, worker, digest, index)
			result, err := worker.runFinanceGiftAuthorizedLive(ctx, a.TaskID, handoffNoWait)
			if err != nil || result.Status != "complete" || result.RowsUpdated != preview.RowsToUpdate || result.CommittedTargets != preview.ReadyTargets {
				t.Fatal("authorization/application mismatch", result, preview, err)
			}
			repeat, err := worker.runFinanceGiftAuthorizedLive(ctx, a.TaskID, handoffNoWait)
			if err != nil || !reflect.DeepEqual(result, repeat) {
				t.Fatal("non-idempotent replay", repeat, err)
			}
			receipt, err := readFinanceGiftCommitProgress(ctx, worker.financeFactsReadStore(), a.TaskID, a.Authorization)
			if err != nil || receipt.RowsUpdated != result.RowsUpdated || receipt.CommittedTargets != result.CommittedTargets {
				t.Fatal("receipt mismatch", receipt, err)
			}
			updated += result.RowsUpdated
			targets += result.CommittedTargets
			readyJobs++
			t.Logf("authorized=%d updated=%d replay=0 receipt=matched", result.CommittedTargets, result.RowsUpdated)
		}) {
			t.FailNow()
		}
	}
	if money != giftMixedMonetarySnapshot(t, m.usageFactsDB) {
		t.Fatal("money or unrelated synchronization facts changed")
	}
	after := giftReuseReadReport(t, reportMonitor, from, to)
	t.Logf("audited series: jobs=%d ready_jobs=%d targets=%d updated=%d relevant_unknown=%d -> %d", len(inventory.Jobs), readyJobs, targets, updated, before.GiftCoverage.ScopeUnknownEvents, after.GiftCoverage.ScopeUnknownEvents)
	if !after.GiftCoverage.Complete {
		start, err := financeStartHour(from)
		if err != nil {
			t.Fatal(err)
		}
		end, err := financeStartHour(to)
		if err != nil {
			t.Fatal(err)
		}
		remaining, err := inspectFinanceGiftRelevantScope(ctx, reportMonitor, epoch, start, end+24*3600)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("retained inventory incomplete: remaining_hours=%d candidates=%+v", remaining.RelevantUserHours, remaining.Candidates)
	}
	assertGiftReuseBreakdown(t, after.Statement, after.Periods, after.Days)
	if !reflect.DeepEqual(before.Statement.UserConsumption, after.Statement.UserConsumption) ||
		!reflect.DeepEqual(before.Statement.UserRefunds, after.Statement.UserRefunds) ||
		!reflect.DeepEqual(before.Statement.KnownRawCorrectedUpstreamCost, after.Statement.KnownRawCorrectedUpstreamCost) {
		t.Fatal("gift handoff changed unrelated statement amounts")
	}
	if after.Statement.AWSInfrastructureCost == nil && after.Statement.OperatingProfit != nil {
		t.Fatal("missing AWS cost became complete profit")
	}
	var integrity string
	if err := m.usageFactsDB.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("receiver integrity", integrity, err)
	}
	if output := os.Getenv("MONITOR_GIFT_RETAINED_RESULT"); output != "" {
		data, err := json.MarshalIndent(struct {
			Mode                        string `json:"mode"`
			Updated, Targets, ReadyJobs int
			Before, After               financeOperatingReport
		}{"local_retained_gift_rehearsal_not_publishable", updated, targets, readyJobs, before, after}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("months=%d days=%d money=unchanged quick_check=ok profit_complete=%t", len(after.Periods), len(after.Days), after.Statement.OperatingProfit != nil)
	return m
}

func giftAuthorizeReuseBatch(t *testing.T, m *Monitor, digest string, index int) giftAuthorizationResponse {
	t.Helper()
	router := gin.New()
	m.RegisterRoutes(router)
	body := fmt.Sprintf(`{"confirmation":%q,"request_id":"%032x","max_hours":10}`, digest, index+1)
	return giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, router, "POST", giftAuthorizationURL, body, "offline-acceptance", roleRoot))
}

func giftRebuildRetainedBatch(t *testing.T, oldJob, digest, backup string) (string, string) {
	t.Helper()
	plan, _, err := giftLocalLoadConfirmedInputs(oldJob, digest)
	if err != nil {
		t.Fatal(err)
	}
	audit := filepath.Join(oldJob, "audit.jsonl")
	if err := giftLocalValidateAudit(audit, plan); err != nil {
		t.Fatal(err)
	}
	auditHash := giftLocalFileHash(t, audit)
	paths := make([]string, len(plan.Targets))
	for i := range paths {
		paths[i] = filepath.Join(oldJob, giftLocalEvidenceName(i))
	}
	job, rebuiltDigest := completedGiftAuthorizationSource(t, backup, paths)
	rebuilt, _, err := giftLocalLoadConfirmedInputs(job, rebuiltDigest)
	if err != nil || !reflect.DeepEqual(plan.Targets, rebuilt.Targets) || !reflect.DeepEqual(plan.EvidenceSHA256, rebuilt.EvidenceSHA256) || !reflect.DeepEqual(plan.Rows, rebuilt.Rows) {
		t.Fatal("rebuilt source differs from retained evidence", err)
	}
	t.Cleanup(func() {
		if _, _, err := giftLocalLoadConfirmedInputs(oldJob, digest); err != nil || giftLocalFileHash(t, audit) != auditHash {
			t.Error("retained inputs changed", err)
		}
	})
	return job, rebuiltDigest
}
