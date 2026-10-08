//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func completedGiftAuthorizationSource(t *testing.T, backup string, paths []string) (string, string) {
	t.Helper()
	job := filepath.Join(t.TempDir(), "source")
	_, digest, err := PrepareFinanceGiftLocalJob(context.Background(), backup, job, paths)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runFinanceGiftLocalJob(context.Background(), job, digest, handoffNoWait); err != nil {
		t.Fatal(err)
	}
	return job, digest
}

func giftAuthorizedLocalFixture(t *testing.T) (*Monitor, *gin.Engine, giftAuthorizationResponse) {
	t.Helper()
	backup, paths, _ := giftMixedLocalFixture(t)
	job, digest := completedGiftAuthorizationSource(t, backup, paths)
	var plan FinanceGiftLocalPlan
	if _, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan); err != nil {
		t.Fatal(err)
	}
	m, r, _ := giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
	attachGiftAuthorizationStore(t, m)
	m.cfg.LocalSnapshotOnly = true
	if err := m.usageFactsDB.AutoMigrate(&financeGiftHandoffCommit{}); err != nil {
		t.Fatal(err)
	}
	body := `{"confirmation":"` + digest + `","request_id":"` + giftAuthorizationNonce + `"}`
	a := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "local-root", roleRoot))
	return m, r, a
}

func giftAuthorizedLocalProgress(t *testing.T, m *Monitor, id string) financeGiftAuthorizedProgress {
	t.Helper()
	_, p, err := readFinanceGiftAuthorization(context.Background(), m.storeDB, id)
	if err != nil {
		t.Fatal(err)
	}
	input, err := loadFinanceGiftConfiguredEvidence(context.Background(), m.cfg)
	if err != nil {
		t.Fatal(err)
	}
	targets, verified, err := selectFinanceGiftAuthorizedEvidence(p, input)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := inspectFinanceGiftAuthorizedProgress(context.Background(), m.usageFactsDB, id, p, targets, verified)
	if err != nil {
		t.Fatal(err)
	}
	return progress
}

func TestFinanceGiftAuthorizedLocalScopeAndRestart(t *testing.T) {
	m, _, a := giftAuthorizedLocalFixture(t)
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	sourceHash := giftLocalFileHash(t, filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "usage-facts.db"))
	var before []FinanceGiftBoundaryState
	if err := m.usageFactsDB.Order("source_epoch,hour_ts,user_id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	result, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait)
	wantRows := 0
	for _, target := range a.Authorization.Targets {
		wantRows += target.RowsToUpdate
	}
	if err != nil || result.Status != "complete" || result.AuthorizedTargets != 2 || result.CommittedTargets != 2 || result.RowsUpdated != wantRows {
		t.Fatal(result, err)
	}
	for _, prior := range before {
		approved := false
		for _, target := range a.Authorization.Targets {
			if target.HourTs == prior.HourTs && target.UserID == prior.UserID {
				approved = true
			}
		}
		if approved {
			continue
		}
		var after FinanceGiftBoundaryState
		if err := m.usageFactsDB.Where("source_epoch=? AND hour_ts=? AND user_id=?", prior.SourceEpoch, prior.HourTs, prior.UserID).First(&after).Error; err != nil || !reflect.DeepEqual(prior, after) {
			t.Fatal("unapproved target changed", err)
		}
	}
	beforeRepeat := giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
	// Fresh instance and new DB handles, without any in-memory execution state.
	main, closeMain, err := giftLocalDatabase(m.cfg.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	facts, closeFacts, err := giftLocalDatabase(m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFacts()
	restarted := &Monitor{cfg: m.cfg, storeDB: main, usageFactsDB: facts}
	repeat, err := restarted.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait)
	if err != nil || repeat != result || beforeRepeat != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) {
		t.Fatal("restart duplicated repair", repeat, err)
	}
	if money != giftMixedMonetarySnapshot(t, m.usageFactsDB) || sourceHash != giftLocalFileHash(t, filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "usage-facts.db")) {
		t.Fatal("money or source changed")
	}
}

func TestFinanceGiftAuthorizedLocalRevocationAndInterrupt(t *testing.T) {
	for _, mode := range []string{"revoke", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			m, r, a := giftAuthorizedLocalFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := 0
			_, err := m.runFinanceGiftAuthorizedLocal(ctx, a.TaskID, func(ctx context.Context, _ time.Duration) error {
				waits++
				if waits == 2 {
					if mode == "cancel" {
						cancel()
					} else {
						giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/revoke", "{}", "local-root", roleRoot))
					}
				}
				return ctx.Err()
			})
			if err == nil {
				t.Fatal("interruption did not stop")
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("lost cancellation cause", err)
			}
			progress := giftAuthorizedLocalProgress(t, m, a.TaskID)
			if progress.Status != "partial" || progress.CommittedTargets != 1 || progress.MatchedTargets != 1 {
				t.Fatal(progress)
			}
			resumed, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait)
			if mode == "revoke" {
				if err == nil || giftAuthorizedLocalProgress(t, m, a.TaskID) != progress {
					t.Fatal("revoked task continued", resumed, err)
				}
			} else if err != nil || resumed.Status != "complete" || resumed.CommittedTargets != 2 {
				t.Fatal(resumed, err)
			}
		})
	}
}

func TestFinanceGiftAuthorizedLocalRejectsUnsafeExecution(t *testing.T) {
	for _, mode := range []string{"not-local", "production-dsn", "source-changed", "expired", "missing-ledger", "ledger-insert-failure", "aggregate-conflict"} {
		t.Run(mode, func(t *testing.T) {
			m, _, a := giftAuthorizedLocalFixture(t)
			switch mode {
			case "not-local":
				m.cfg.LocalSnapshotOnly = false
			case "production-dsn":
				m.cfg.ProdDSN = "must-not-connect"
			case "source-changed":
				m.cfg.FinanceGiftHandoffPreviewSHA256 = "wrong"
			case "expired":
				p := a.Authorization
				p.ApprovedAt = time.Now().Add(-time.Hour).Unix()
				p.ExpiresAt = p.ApprovedAt + 1800
				b, _ := json.Marshal(p)
				if err := m.storeDB.Model(&financeGiftHandoffAuthorization{}).Where("id=?", a.TaskID).Updates(map[string]any{"payload": string(b), "payload_hash": giftLocalDigest(b)}).Error; err != nil {
					t.Fatal(err)
				}
			case "missing-ledger":
				if err := m.usageFactsDB.Migrator().DropTable(&financeGiftHandoffCommit{}); err != nil {
					t.Fatal(err)
				}
			case "ledger-insert-failure":
				if err := m.usageFactsDB.Exec(`CREATE TRIGGER fail_authorized_commit BEFORE INSERT ON local_gift_handoff_commits BEGIN SELECT RAISE(ABORT,'injected'); END`).Error; err != nil {
					t.Fatal(err)
				}
			case "aggregate-conflict":
				target := a.Authorization.Targets[1]
				if err := m.usageFactsDB.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", target.HourTs, target.UserID).Update("consume_quota", -1).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
			proofBefore := giftBoundaryFingerprint(t, m.usageFactsDB)
			if got, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait); err == nil {
				t.Fatal("unsafe execution accepted", got)
			}
			if before != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) || proofBefore != giftBoundaryFingerprint(t, m.usageFactsDB) {
				t.Fatal("rejected/rolled-back run changed facts")
			}
		})
	}
}

func TestFinanceGiftAuthorizedAtomicRowAllowance(t *testing.T) {
	db, _, target, evidence := handoffConcurrentFixture(t)
	before, err := loadFinanceGiftScopeSnapshot(context.Background(), db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyFinanceGiftHandoffWithCommitLimit(context.Background(), db, "limited", 0, target, evidence, time.Now().Unix(), 1); err == nil {
		t.Fatal("row growth exceeded approval")
	}
	after, err := loadFinanceGiftScopeSnapshot(context.Background(), db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("allowance rejection did not roll back", err)
	}
	var count int64
	if err := db.Model(&financeGiftHandoffCommit{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func giftTestDBPath(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var rows []struct{ Name, File string }
	if err := db.Raw("PRAGMA database_list").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Name == "main" && row.File != "" {
			return row.File
		}
	}
	t.Fatal("missing file database")
	return ""
}

func TestFinanceGiftAuthorizedLocalReportCache(t *testing.T) {
	m, evidence, _, _ := giftReportLocalFixture(t)
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := m.usageFactsDB.Exec("VACUUM INTO ?", backup).Error; err != nil {
		t.Fatal(err)
	}
	paths := make([]string, len(evidence))
	for i, e := range evidence {
		paths[i] = filepath.Join(t.TempDir(), "evidence.json")
		giftLargeWriteTestJSON(t, paths[i], e)
	}
	job, digest := completedGiftAuthorizationSource(t, backup, paths)
	m.cfg.StorePath = giftTestDBPath(t, m.storeDB)
	m.cfg.UsageFactsStorePath = giftTestDBPath(t, m.usageFactsDB)
	m.cfg.FinanceGiftHandoffPreviewDir = job
	m.cfg.FinanceGiftHandoffPreviewSHA256 = digest
	m.cfg.FinanceFactsReadIsolationEnabled = true
	m.cfg.FinanceGiftHandoffApprovalEnabled = true
	m.cfg.SessionSecret = "local-test"
	m.cfg.UsageFactsHistorySourceEpoch = "v1"
	if err := m.storeDB.AutoMigrate(&financeGiftHandoffAuthorization{}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsDB.AutoMigrate(&financeGiftHandoffCommit{}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsDB.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	m.RegisterRoutes(r)
	body := `{"confirmation":"` + digest + `","request_id":"` + giftAuthorizationNonce + `","max_hours":4}`
	a := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "local-root", roleRoot))
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	before := giftReadReportHTTP(t, m)
	giftAssertReportAmounts(t, before.Report, 8)
	if hit := giftReadReportHTTP(t, m); hit.Cache != "hit" {
		t.Fatal("cache did not warm")
	}
	base := giftPeriodCacheSnapshot(m)
	result, err := m.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait)
	if err != nil || result.Status != "complete" || result.RowsUpdated != 8 {
		t.Fatal(result, err)
	}
	stale := giftReadReportHTTP(t, m)
	if stale.Cache != "stale-refreshing" || !reflect.DeepEqual(stale.Report, before.Report) {
		t.Fatal("lost usable cached report")
	}
	after := giftWaitReportRefresh(t, m, 0)
	giftAssertReportAmounts(t, after.Report, 0)
	if !reflect.DeepEqual(base, giftPeriodCacheSnapshot(m)) || money != giftMixedMonetarySnapshot(t, m.usageFactsDB) {
		t.Fatal("unrelated month or monetary facts changed")
	}
}

// Read through SQLite so WAL-backed changes cannot hide behind an unchanged
// main-file hash in rejection/rollback tests.
func giftBoundaryFingerprint(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var states []FinanceGiftBoundaryState
	var events []FinanceGiftBoundaryEvent
	if err := db.Order("source_epoch,hour_ts,user_id").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Order("source_epoch,hour_ts,user_id,source_log_id").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal([]any{states, events})
	if err != nil {
		t.Fatal(err)
	}
	return giftLocalDigest(b)
}
