//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestFinanceRechargeGuardedRetainedApipros(t *testing.T) {
	manifestPath, fixturePath := os.Getenv("MONITOR_RECHARGE_REUSE_MANIFEST"), os.Getenv("MONITOR_FINANCE_BOUNDED_RECHARGE_FIXTURE")
	if manifestPath == "" || fixturePath == "" {
		t.Skip("requires retained sealed backup and reviewed finite corrections")
	}
	var manifest struct{ Source, SourceSHA256 string }
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	var fixture financeBoundedRechargeFixture
	data, err = os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil || !fixture.NotAnImportPlan {
		t.Fatal("invalid local evidence", err)
	}
	if manifest.SourceSHA256 == "" || giftLocalFileHash(t, manifest.Source) != manifest.SourceSHA256 {
		t.Fatal("sealed input identity mismatch")
	}
	t.Cleanup(func() {
		if giftLocalFileHash(t, manifest.Source) != manifest.SourceSHA256 {
			t.Error("sealed original changed")
		}
	})
	path := filepath.Join(t.TempDir(), "receiver.db")
	if _, err := giftLocalCopyBackup(manifest.Source, path); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	before := guardedVersionRows(t, db)
	var settingsBefore []ChannelDomainCost
	if err := db.Order("domain").Find(&settingsBefore).Error; err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	count, added := 0, 0
	var delta int64
	var committed []financeRechargeGuardPlan
	for _, domain := range fixture.Domains {
		if domain.Domain != "apipros.cc" {
			continue
		}
		for _, c := range domain.Corrections {
			if c.Paid != 1 || c.Credit != 1 || c.ToTs-c.FromTs != 3600 {
				t.Fatal("outside owner-confirmed scope")
			}
			start := time.Unix(c.FromTs, 0).In(loc)
			from := start.Format("2006-01-02")
			midnight, err := time.ParseInLocation("2006-01-02", from, loc)
			if err != nil {
				t.Fatal(err)
			}
			r := FinanceRechargeRehearsalRequest{FromDate: from, ToDate: midnight.AddDate(0, 0, 1).Format("2006-01-02"), Corrections: []FinanceRechargeCorrection{c}, RebuildEconomics: true}
			plan, err := prepareFinanceRechargeGuardPlan(context.Background(), db, r, time.Now().Unix())
			if err != nil {
				t.Fatal(err)
			}
			hash, err := financeRechargeGuardHash(plan)
			if err != nil {
				t.Fatal(err)
			}
			job := filepath.Join(t.TempDir(), "reviewed-repair")
			confirmation, err := createFinanceRechargeTask(context.Background(), db, job, financeRechargeTaskPlan{Repair: &plan})
			if err != nil {
				t.Fatal(err)
			}
			if err := approveFinanceRechargeTask(context.Background(), db, job, confirmation, "local-acceptance", time.Now().Unix()); err != nil {
				t.Fatal(err)
			}
			status, err := runFinanceRechargeTask(context.Background(), db, job, confirmation, "local-acceptance", time.Now().Unix())
			if err != nil || !status.Committed || !status.ReceiptAvailable || status.Receipt == nil {
				t.Fatal(status, err)
			}
			receipt := *status.Receipt
			added += receipt.AddedVersions
			committed = append(committed, plan)
			count++
			delta += plan.Preview.Domains[0].Total.KnownAfterMicro - plan.Preview.Domains[0].Total.KnownBeforeMicro
			if receipt, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix()); err != nil || receipt.Status != "already_applied" {
				t.Fatal("real replay failed", receipt, err)
			}
		}
	}
	if count != 3 || delta != 298907 {
		t.Fatal("real corrected amount differs", count, delta)
	}
	after := guardedVersionRows(t, db)
	if len(after) != len(before)+added || !reflect.DeepEqual(before, after[:len(before)]) {
		t.Fatal("real original versions changed")
	}
	var settingsAfter []ChannelDomainCost
	if err := db.Order("domain").Find(&settingsAfter).Error; err != nil || !reflect.DeepEqual(settingsBefore, settingsAfter) {
		t.Fatal("current settings changed", err)
	}
	t.Logf("retained apipros PASS: 3 bounded corrections, delta_micro=%d, appended_versions=%d, replay no-op, original versions/settings unchanged", delta, added)
	// Recover newest first. Each recovery retains all committed history and
	// rebuilds the affected accounting window, rather than replacing the DB.
	for i := len(committed) - 1; i >= 0; i-- {
		original := committed[i]
		hash, _ := financeRechargeGuardHash(original)
		plan, err := prepareFinanceRechargeRecovery(context.Background(), db, original, hash, time.Now().Unix())
		if err != nil {
			t.Fatal("real recovery preview", err)
		}
		if plan.Preview.Domains[0].Total.KnownAfterMicro != original.Preview.Domains[0].Total.KnownBeforeMicro {
			t.Fatal("real recovery amount differs", plan.Preview)
		}
		approved, _ := financeRechargeGuardHash(plan)
		job := filepath.Join(t.TempDir(), "reviewed-recovery")
		confirmation, err := createFinanceRechargeTask(context.Background(), db, job, financeRechargeTaskPlan{Recovery: &plan})
		if err != nil {
			t.Fatal(err)
		}
		if err := approveFinanceRechargeTask(context.Background(), db, job, confirmation, "local-acceptance", time.Now().Unix()); err != nil {
			t.Fatal(err)
		}
		if status, err := runFinanceRechargeTask(context.Background(), db, job, confirmation, "local-acceptance", time.Now().Unix()); err != nil || !status.Committed || !status.ReceiptAvailable {
			t.Fatal("real recovery task failed", status, err)
		}
		if receipt, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, time.Now().Unix()); err != nil || receipt.Status != "already_applied" {
			t.Fatal("real recovery replay failed", receipt, err)
		}
	}
	recovered := guardedVersionRows(t, db)
	if !reflect.DeepEqual(after, recovered[:len(after)]) {
		t.Fatal("recovery changed existing real history")
	}
	t.Log("retained apipros recovery PASS: three reviewed windows restored, original and repair history preserved, replay no-op")
}

func TestFinanceRechargeGuardedBoundariesAndPostCommitDrift(t *testing.T) {
	db, plan, hash, _ := guardedRechargeFixture(t)
	request := plan.Request
	request.ToDate = "2026-05-09"
	if _, err := prepareFinanceRechargeGuardPlan(context.Background(), db, request, time.Now().Unix()); err == nil {
		t.Fatal("over-seven-day request accepted")
	}
	request = plan.Request
	request.RebuildEconomics = false
	if _, err := prepareFinanceRechargeGuardPlan(context.Background(), db, request, time.Now().Unix()); err == nil {
		t.Fatal("disabled economics validation accepted")
	}
	// Balance polling is unrelated to the reviewed historical accounting input.
	if err := db.Model(&ChannelUpstreamAccount{}).Where("domain=?", plan.Request.Corrections[0].Domain).Update("balance_usd", 123).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix()); err != nil {
		t.Fatal("balance polling wrongly blocked historical repair", err)
	}
	if err := db.Model(&ChannelUpstreamUsageHour{}).Where("domain=?", plan.Request.Corrections[0].Domain).Update("cost_usd", 55).Error; err != nil {
		t.Fatal(err)
	}
	before := guardedVersionRows(t, db)
	if _, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix()); err == nil {
		t.Fatal("post-commit drift silently replayed")
	}
	if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
		t.Fatal("post-commit drift rewrote committed history")
	}
}

func guardedRechargeFixture(t *testing.T) (*gorm.DB, financeRechargeGuardPlan, string, string) {
	t.Helper()
	source, request := rechargeRehearsalFixture(t)
	hash := giftLocalFileHash(t, source)
	t.Cleanup(func() {
		if giftLocalFileHash(t, source) != hash {
			t.Error("original input changed")
		}
	})
	path := filepath.Join(t.TempDir(), "receiver.db")
	if _, err := giftLocalCopyBackup(source, path); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeDB)
	request.SourceSHA256 = ""
	request.RebuildEconomics = true
	plan, err := prepareFinanceRechargeGuardPlan(context.Background(), db, request, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	digest, err := financeRechargeGuardHash(plan)
	if err != nil {
		t.Fatal(err)
	}
	return db, plan, digest, path
}

func guardedVersionRows(t *testing.T, db *gorm.DB) []ChannelFinanceVersion {
	t.Helper()
	var rows []ChannelFinanceVersion
	if err := db.Order("id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestFinanceRechargeGuardedApplyReplayAndRestart(t *testing.T) {
	db, plan, hash, path := guardedRechargeFixture(t)
	before := guardedVersionRows(t, db)
	var bills []ChannelUpstreamUsageHour
	if err := db.Order("domain,hour_ts").Find(&bills).Error; err != nil {
		t.Fatal(err)
	}
	receipt, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix())
	if err != nil || receipt.Status != "applied" || receipt.AddedVersions != 3 {
		t.Fatal("apply failed", receipt, err)
	}
	after := guardedVersionRows(t, db)
	if len(after) != len(before)+3 || !reflect.DeepEqual(before, after[:len(before)]) {
		t.Fatal("original history changed")
	}
	if after[len(after)-1].SnapshotJSON != before[len(before)-1].SnapshotJSON {
		t.Fatal("current anchor changed")
	}
	var billsAfter []ChannelUpstreamUsageHour
	if err := db.Order("domain,hour_ts").Find(&billsAfter).Error; err != nil || !reflect.DeepEqual(bills, billsAfter) {
		t.Fatal("source money changed", err)
	}
	reopened, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	receipt, err = executeFinanceRechargeGuardPlan(context.Background(), reopened, plan, hash, time.Now().Unix())
	if err != nil || receipt.Status != "already_applied" || receipt.AddedVersions != 0 || !reflect.DeepEqual(after, guardedVersionRows(t, db)) {
		t.Fatal("lost-response/restart retry duplicated work", receipt, err)
	}
}

func TestFinanceRechargeGuardedRejectsDriftAndTampering(t *testing.T) {
	for _, name := range []string{"approval", "expired", "request", "rows", "preview", "bill", "config", "account", "history", "cancel"} {
		t.Run(name, func(t *testing.T) {
			db, plan, hash, _ := guardedRechargeFixture(t)
			now := time.Now().Unix()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			domain := plan.Request.Corrections[0].Domain
			switch name {
			case "approval":
				hash = strings.Repeat("0", 64)
			case "expired":
				now = plan.ExpiresAt + 1
			case "request":
				plan.Request.Corrections[0].Credit = 10
			case "rows":
				plan.Rows[0].SnapshotJSON = `{}`
				hash, _ = financeRechargeGuardHash(plan)
			case "preview":
				plan.Preview.Domains[0].Total.KnownAfterMicro++
				hash, _ = financeRechargeGuardHash(plan)
			case "bill":
				if err := db.Model(&ChannelUpstreamUsageHour{}).Where("domain=?", domain).Update("cost_usd", 77).Error; err != nil {
					t.Fatal(err)
				}
			case "config":
				if err := db.Create(&ChannelDomainCost{Domain: domain, RechargePaid: 1, RechargeCredit: 9}).Error; err != nil {
					t.Fatal(err)
				}
			case "account":
				if err := db.Model(&ChannelUpstreamAccount{}).Where("domain=?", domain).Update("user_id", 999).Error; err != nil {
					t.Fatal(err)
				}
			case "history":
				if err := db.Model(&ChannelFinanceVersion{}).Where("domain=?", domain).Update("updated_by", "another-editor").Error; err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			}
			before := guardedVersionRows(t, db)
			if result, err := executeFinanceRechargeGuardPlan(ctx, db, plan, hash, now); err == nil || result.Status != "" {
				t.Fatal("unsafe repair accepted", name, result, err)
			}
			if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
				t.Fatal("rejection wrote history", name)
			}
		})
	}
}

func TestFinanceRechargeGuardedFailureAtomicity(t *testing.T) {
	for _, stage := range []string{"insert", "economics"} {
		t.Run(stage, func(t *testing.T) {
			db, plan, hash, _ := guardedRechargeFixture(t)
			before := guardedVersionRows(t, db)
			if stage == "insert" {
				if err := db.Exec(`CREATE TRIGGER reject_guarded BEFORE INSERT ON channel_finance_versions WHEN NEW.version>=4 BEGIN SELECT RAISE(ABORT,'injected failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			} else {
				if err := db.Callback().Query().Before("gorm:query").Register("test:guarded-economics", func(tx *gorm.DB) {
					if tx.Statement.Table == "channel_upstream_cost_hour_states" {
						_ = tx.AddError(errors.New("injected economics failure"))
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			if result, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix()); err == nil || result.Status != "" {
				t.Fatal("failure returned success", result, err)
			}
			if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
				t.Fatal("failure left partial versions")
			}
			if stage == "insert" {
				if err := db.Exec("DROP TRIGGER reject_guarded").Error; err != nil {
					t.Fatal(err)
				}
			} else {
				_ = db.Callback().Query().Remove("test:guarded-economics")
			}
			if result, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix()); err != nil || result.Status != "applied" {
				t.Fatal("retry after rollback failed", result, err)
			}
		})
	}
}

func TestFinanceRechargeGuardedConcurrentRetry(t *testing.T) {
	db, plan, hash, _ := guardedRechargeFixture(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	applied := 0
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix())
			if err == nil && r.Status == "applied" {
				mu.Lock()
				applied++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if applied != 1 {
		t.Fatal("expected exactly one committed execution", applied)
	}
	r, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix())
	if err != nil || r.Status != "already_applied" {
		t.Fatal("post-contention retry failed", r, err)
	}
}
