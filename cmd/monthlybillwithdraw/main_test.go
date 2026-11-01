package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestWithdrawalCLIReadOnlyAndExplicitApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(&monitor.CustomerGroup{}, &monitor.TrackedUser{}, &monitor.UsageMemberAudit{}, &monitor.UsageMemberControl{}, &monitor.UsageMemberControlMigration{}, &monitor.PortalMonthlyBill{}, &monitor.PortalMonthlyBillWithdrawal{}); err != nil {
		t.Fatal(err)
	}
	group := monitor.CustomerGroup{ID: 1, Name: "synthetic", PortalEmail: "test@example.invalid"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&monitor.TrackedUser{UserID: 43, GroupID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, loc)
	if err := db.Create(&monitor.UsageMemberControlMigration{ID: 1, Version: 1, ManifestHash: "synthetic-cli-fixture", InitializedAt: now.Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, loc)
	e := monitor.MonthlyBillEvidence{Version: 2, GroupID: 1, Month: "2026-09", ReviewedBy: "reviewer", ReviewedAt: now.Unix(), SourceReference: "synthetic-cli-test", Users: []monitor.MonthlyBillUserEvidence{{
		UserID: 43, Complete: true, WalletOnly: true, Through: start.AddDate(0, 1, 0).Unix(), ExpectedCreditEvents: 1, ClosingQuota: 400000, ReverseClosingQuota: 400000, ConsumeQuota: 100000, Requests: 1, Tokens: 100,
		Accounting: &monitor.MonthlyBillAccountingEvidence{Basis: "independent_wallet_reconciliation", OpeningSource: "synthetic opening", ClosingSource: "synthetic closing", MovementSource: "synthetic movements", RefundSource: "synthetic refunds", ReconciliationSource: "synthetic independent reconciliation", LogConsumeQuota: 100000},
		Events:     []monitor.MonthlyBillCreditEvidence{{ID: 1, At: start.Unix() + 1, DeltaQuota: 500000, Kind: "recharge", Method: "在线充值"}}, Reversals: []monitor.MonthlyBillReversal{},
	}}}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := monitor.ImportMonthlyBill(context.Background(), db, raw, true, now); err != nil {
		t.Fatal(err)
	}
	version, err := monitor.MonthlyBillPublicationVersion(context.Background(), db, 1, e.Month, now)
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"-db", path, "-group-id", "1", "-month", e.Month}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(base, &out, now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "publication="+version) || !strings.Contains(out.String(), "未写入") {
		t.Fatal(out.String())
	}
	args := append(append([]string{}, base...), "-expected-publication", version, "-actor", "operator", "-reason", "reviewed correction")
	out.Reset()
	if err := run(args, &out, now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "核验通过，未写入") {
		t.Fatal(out.String())
	}
	after, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("read-only preflight changed database", err)
	}
	for _, bad := range [][]string{
		append(append([]string{}, base...), "-apply"),
		append(append([]string{}, base...), "-expected-publication", version, "-apply"),
		append(append([]string{}, base...), "unrecognized"),
		{"-db", path, "-group-id", "1", "-month", "2026-10"},
	} {
		if err := run(bad, &out, now); err == nil {
			t.Fatal("invalid CLI accepted", bad)
		}
	}
	out.Reset()
	if err := run(append(args, "-apply"), &out, now); err != nil {
		t.Fatal(err)
	}
	var active, archives int64
	if err := db.Model(&monitor.PortalMonthlyBill{}).Count(&active).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&monitor.PortalMonthlyBillWithdrawal{}).Count(&archives).Error; err != nil {
		t.Fatal(err)
	}
	if active != 0 || archives != 1 || !strings.Contains(out.String(), "已撤回") {
		t.Fatal("wrong result", active, archives, out.String())
	}
	if err := run(append(args, "-apply"), &out, now); err != nil {
		t.Fatal("safe replay", err)
	}
}

func TestWithdrawalCLIDoesNotCreateMissingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	var out bytes.Buffer
	if err := run([]string{"-db", path, "-group-id", "1", "-month", "2026-09"}, &out, time.Now()); err == nil {
		t.Fatal("missing DB accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("missing DB created", err)
	}
}
