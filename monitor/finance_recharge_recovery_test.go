//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func committedRechargeFixture(t *testing.T) (*gorm.DB, financeRechargeGuardPlan, string) {
	t.Helper()
	db, plan, hash, _ := guardedRechargeFixture(t)
	if _, err := executeFinanceRechargeGuardPlan(context.Background(), db, plan, hash, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	return db, plan, hash
}

func TestFinanceRechargeRecoveryPreservesLaterData(t *testing.T) {
	db, original, hash := committedRechargeFixture(t)
	c := original.Request.Corrections[0]
	rows := guardedVersionRows(t, db)
	later := ChannelFinanceVersion{Domain: c.Domain, Version: rows[len(rows)-1].Version + 1, EffectiveAt: c.ToTs + 86400, CreatedAt: time.Now().Unix(), UpdatedBy: "later-legitimate-editor", SnapshotJSON: `{"domain":"hour.example","upstream_recharge_paid":1,"upstream_recharge_credit":9,"future_field":{"keep":77}}`}
	if err := db.Create(&later).Error; err != nil {
		t.Fatal(err)
	}
	setting := ChannelDomainCost{Domain: c.Domain, RechargePaid: 1, RechargeCredit: 9, EffectiveAt: later.EffectiveAt, UpdatedBy: "later-legitimate-editor"}
	if err := db.Create(&setting).Error; err != nil {
		t.Fatal(err)
	}
	bill := ChannelUpstreamUsageHour{Domain: c.Domain, HourTs: c.ToTs + 3600, BucketSeconds: 3600, CostUSD: 123, Provider: upstreamProviderNewAPI}
	if err := db.Create(&bill).Error; err != nil {
		t.Fatal(err)
	}
	before := guardedVersionRows(t, db)
	var billsBefore []ChannelUpstreamUsageHour
	if err := db.Order("domain,hour_ts").Find(&billsBefore).Error; err != nil {
		t.Fatal(err)
	}
	// Recovery can be reviewed after the original execution authorization expires.
	now := original.ExpiresAt + 1
	plan, err := prepareFinanceRechargeRecovery(context.Background(), db, original, hash, now)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Preview.Domains[0].Total.KnownAfterMicro != original.Preview.Domains[0].Total.KnownBeforeMicro || plan.Preview.Domains[0].Total.RemainingUnknown != 1 {
		t.Fatal("recovery did not restore old amount/unknown state", plan.Preview)
	}
	approved, err := financeRechargeGuardHash(plan)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, now)
	if err != nil || receipt.Status != "applied" {
		t.Fatal(receipt, err)
	}
	after := guardedVersionRows(t, db)
	if !reflect.DeepEqual(before, after[:len(before)]) || after[len(after)-1].SnapshotJSON != later.SnapshotJSON {
		t.Fatal("recovery replaced history or later current anchor")
	}
	var billsAfter []ChannelUpstreamUsageHour
	if err := db.Order("domain,hour_ts").Find(&billsAfter).Error; err != nil || !reflect.DeepEqual(billsBefore, billsAfter) {
		t.Fatal("later/source bills changed", err)
	}
	var settingAfter ChannelDomainCost
	if err := db.Where("domain=?", c.Domain).First(&settingAfter).Error; err != nil || settingAfter != setting {
		t.Fatal("later settings changed", err)
	}
	terms := correctionTerms(t, after)
	if _, _, status := rechargeTermsForBucket(terms, c.FromTs, c.FromTs+3600); status != upstreamAdjustedCostMissingHistory {
		t.Fatal("unknown original history invented price", status)
	}
	if paid, credit, status := rechargeTermsForBucket(terms, later.EffectiveAt, later.EffectiveAt+3600); paid != 1 || credit != 9 || status != upstreamAdjustedCostComplete {
		t.Fatal("later pricing changed")
	}
	if receipt, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, now); err != nil || receipt.Status != "already_applied" || !reflect.DeepEqual(after, guardedVersionRows(t, db)) {
		t.Fatal("recovery replay changed data", receipt, err)
	}
	// Optional synthetic-only artifact for a separately compiled old-tag reader.
	// VACUUM INTO snapshots committed SQLite state and refuses existing files.
	if dir := os.Getenv("MONITOR_RECOVERY_COMPAT_OUTPUT_DIR"); dir != "" {
		path := filepath.Join(dir, "synthetic-recovered.db")
		if err := db.Exec("VACUUM INTO ?", path).Error; err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFinanceRechargeRecoveryConcurrentAndRestart(t *testing.T) {
	db, original, hash, path := guardedRechargeFixture(t)
	now := time.Now().Unix()
	if _, err := executeFinanceRechargeGuardPlan(context.Background(), db, original, hash, now); err != nil {
		t.Fatal(err)
	}
	plan, err := prepareFinanceRechargeRecovery(context.Background(), db, original, hash, now)
	if err != nil {
		t.Fatal(err)
	}
	approved, _ := financeRechargeGuardHash(plan)
	var wg sync.WaitGroup
	results := make(chan financeRechargeGuardReceipt, 2)
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, now)
			results <- r
			errors <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	applied, replay := 0, 0
	for r := range results {
		switch r.Status {
		case "applied":
			applied++
		case "already_applied":
			replay++
		}
	}
	if applied != 1 || replay != 1 {
		t.Fatal("duplicate concurrent recovery", applied, replay)
	}
	before := guardedVersionRows(t, db)
	conn, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	if r, err := executeFinanceRechargeRecovery(context.Background(), reopened, plan, approved, now); err != nil || r.Status != "already_applied" || !reflect.DeepEqual(before, guardedVersionRows(t, reopened)) {
		t.Fatal("recovery restart replay failed", r, err)
	}
}

func TestFinanceRechargeRecoveryRejectsConflicts(t *testing.T) {
	for _, name := range []string{"overlap", "original_history", "receipt", "changed_after_preview", "approval", "expired", "tamper"} {
		t.Run(name, func(t *testing.T) {
			db, original, hash := committedRechargeFixture(t)
			c := original.Request.Corrections[0]
			now := time.Now().Unix()
			switch name {
			case "overlap":
				rows := guardedVersionRows(t, db)
				if err := db.Create(&ChannelFinanceVersion{Domain: c.Domain, Version: rows[len(rows)-1].Version + 1, EffectiveAt: c.FromTs + 3600, SnapshotJSON: `{"domain":"hour.example"}`}).Error; err != nil {
					t.Fatal(err)
				}
			case "original_history":
				if err := db.Model(&ChannelFinanceVersion{}).Where("domain=? AND version=1", c.Domain).Update("snapshot_json", `{}`).Error; err != nil {
					t.Fatal(err)
				}
			case "receipt":
				if err := db.Model(&ChannelFinanceVersion{}).Where("domain=? AND version=?", c.Domain, original.Rows[0].Version).Update("updated_by", "tampered").Error; err != nil {
					t.Fatal(err)
				}
			}
			before := guardedVersionRows(t, db)
			plan, err := prepareFinanceRechargeRecovery(context.Background(), db, original, hash, now)
			if name == "overlap" || name == "original_history" || name == "receipt" {
				if err == nil {
					t.Fatal("unsafe recovery planned")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			approved, err := financeRechargeGuardHash(plan)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "changed_after_preview":
				if err := db.Model(&ChannelUpstreamUsageHour{}).Where("domain=?", c.Domain).Update("cost_usd", 100).Error; err != nil {
					t.Fatal(err)
				}
			case "approval":
				approved = "wrong"
			case "expired":
				now = plan.ExpiresAt + 1
			case "tamper":
				plan.Rows[0].SnapshotJSON = `{}`
				approved, _ = financeRechargeGuardHash(plan)
			}
			if _, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, now); err == nil {
				t.Fatal("unsafe recovery executed")
			}
			if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
				t.Fatal("rejected recovery wrote history")
			}
		})
	}
}

func TestFinanceRechargeRecoveryFailureAtomicity(t *testing.T) {
	for _, stage := range []string{"insert", "economics"} {
		t.Run(stage, func(t *testing.T) {
			db, original, hash := committedRechargeFixture(t)
			now := time.Now().Unix()
			plan, err := prepareFinanceRechargeRecovery(context.Background(), db, original, hash, now)
			if err != nil {
				t.Fatal(err)
			}
			approved, _ := financeRechargeGuardHash(plan)
			before := guardedVersionRows(t, db)
			if stage == "insert" {
				if err := db.Exec(`CREATE TRIGGER reject_recovery BEFORE INSERT ON channel_finance_versions WHEN NEW.updated_by LIKE 'guarded-recovery:%' BEGIN SELECT RAISE(ABORT,'injected recovery failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			} else {
				if err := db.Callback().Query().Before("gorm:query").Register("test:recovery-economics", func(tx *gorm.DB) {
					if tx.Statement.Table == "channel_upstream_cost_hour_states" {
						_ = tx.AddError(errors.New("injected recovery rebuild failure"))
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			if r, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, now); err == nil || r.Status != "" {
				t.Fatal("failed recovery reported success", r, err)
			}
			if !reflect.DeepEqual(before, guardedVersionRows(t, db)) {
				t.Fatal("failed recovery left partial rows")
			}
			if stage == "insert" {
				if err := db.Exec("DROP TRIGGER reject_recovery").Error; err != nil {
					t.Fatal(err)
				}
			} else {
				_ = db.Callback().Query().Remove("test:recovery-economics")
			}
			if r, err := executeFinanceRechargeRecovery(context.Background(), db, plan, approved, now); err != nil || r.Status != "applied" {
				t.Fatal("recovery retry failed", r, err)
			}
		})
	}
}

func TestFinanceRechargeRecoveryHistoryCapacity(t *testing.T) {
	c := FinanceRechargeCorrection{Domain: "example.com", FromTs: 3600, ToTs: 7200, Paid: 1, Credit: 1, Reason: "synthetic capacity test"}
	for _, n := range []int{financeRechargeCorrectionVersionLimit - 6, financeRechargeCorrectionVersionLimit - 5} {
		var history []ChannelFinanceVersion
		for i := 1; i <= n; i++ {
			history = append(history, ChannelFinanceVersion{Domain: c.Domain, Version: int64(i), EffectiveAt: 86400, SnapshotJSON: `{"domain":"example.com","upstream_recharge_paid":1,"upstream_recharge_credit":7}`})
		}
		added, err := buildFinanceRechargeCorrection(history, c, 172800)
		if err != nil {
			t.Fatal(err)
		}
		err = validateFinanceRechargeRecoveryReserve(history, added, c, 172800)
		if (err == nil) != (n == financeRechargeCorrectionVersionLimit-6) {
			t.Fatal("recovery capacity boundary", n, len(added), err)
		}
	}
}
