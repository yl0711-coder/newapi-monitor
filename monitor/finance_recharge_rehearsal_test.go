//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func rechargeRehearsalFixture(t *testing.T) (string, FinanceRechargeRehearsalRequest) {
	t.Helper()
	path := rechargeEvidenceClosedFixture(t, func(m *Monitor, account ChannelUpstreamAccount, scope stabilityScope) {
		createChannelRechargeVersion(t, m, account.Domain, 2, scope.ToTs, 1, 7.14)
	})
	from, _ := financeStartHour("2026-05-01")
	return path, FinanceRechargeRehearsalRequest{SourceSHA256: giftLocalFileHash(t, path), FromDate: "2026-05-01", ToDate: "2026-05-03",
		Corrections: []FinanceRechargeCorrection{{Domain: "hour.example", FromTs: from, ToTs: from + 2*86400, Paid: 1, Credit: 1, Reason: "test proposal only"}}}
}

func TestFinanceRechargeRehearsalFreshCopyPreservesOriginalAndBalances(t *testing.T) {
	source, request := rechargeRehearsalFixture(t)
	output := filepath.Join(t.TempDir(), "rehearsal")
	result, err := RehearseFinanceRechargeCorrections(context.Background(), source, output, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != "unconfirmed_local_rehearsal" || result.Publishable || result.AddedVersions != 3 ||
		result.SourceSHA256 != request.SourceSHA256 || giftLocalFileHash(t, source) != request.SourceSHA256 || result.OutputSHA256 == request.SourceSHA256 {
		t.Fatal("incorrect rehearsal/source state", result)
	}
	v := result.Domains[0]
	if v.Total.NewlyCosted != 1 || v.Total.Repriced != 1 || v.Total.RemainingUnknown != 0 ||
		v.Total.KnownBeforeMicro != 280112 || v.Total.KnownAfterMicro != 3_000_000 || !v.BillComplete {
		t.Fatal("missing and already priced buckets were not both repaired", v)
	}
	for _, periods := range [][]FinanceRechargeRehearsalAmounts{v.Days, v.Months} {
		var total FinanceRechargeRehearsalAmounts
		for _, period := range periods {
			if err := addRechargeRehearsalAmounts(&total, period); err != nil {
				t.Fatal(err)
			}
		}
		total.Period = "total"
		if total != v.Total {
			t.Fatal("day/month totals differ", total, v.Total)
		}
	}
	original, closeOriginal, err := giftLocalReadonlyDatabase(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeOriginal()
	copy, closeCopy, err := giftLocalReadonlyDatabase(filepath.Join(output, "monitor-rehearsal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeCopy()
	var old, next []ChannelFinanceVersion
	if err := original.Order("id").Find(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := copy.Where("id<=?", old[len(old)-1].ID).Order("id").Find(&next).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, next) {
		t.Fatal("an original historical version was overwritten")
	}
	for _, table := range []string{"channel_upstream_usage_hours", "channel_upstream_accounts", "channel_upstream_fund_events", "channel_domain_costs", "channel_finance_channel_costs"} {
		var a, b []map[string]any
		if err := original.Table(table).Find(&a).Error; err != nil {
			t.Fatal(err)
		}
		if err := copy.Table(table).Find(&b).Error; err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, b) {
			t.Fatal("unrelated source/settings changed", table)
		}
	}
	for _, name := range []string{"unconfirmed-proposal.json", "rehearsal-result.json", "monitor-rehearsal.db"} {
		info, err := os.Stat(filepath.Join(output, name))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("private output permission", name, err)
		}
	}
	data, err := json.Marshal(result)
	if err != nil || strings.Contains(string(data), "private-") {
		t.Fatal("rehearsal leaked account data", err)
	}
	if _, err := RehearseFinanceRechargeCorrections(context.Background(), source, output, request); err == nil {
		t.Fatal("rehearsal overwrote an existing directory")
	}
	if giftLocalFileHash(t, filepath.Join(output, "monitor-rehearsal.db")) != result.OutputSHA256 {
		t.Fatal("retry changed existing copy")
	}
}

func TestFinanceRechargeRehearsalRejectsStaleUnsafeAndIncompleteProposals(t *testing.T) {
	source, base := rechargeRehearsalFixture(t)
	for _, name := range []string{"stale", "canceled", "overlap", "outside", "invalid", "empty"} {
		t.Run(name, func(t *testing.T) {
			request := base
			request.Corrections = append([]FinanceRechargeCorrection(nil), base.Corrections...)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "stale":
				request.SourceSHA256 = strings.Repeat("0", 64)
			case "canceled":
				cancel()
			case "overlap":
				request.Corrections = append(request.Corrections, request.Corrections[0])
			case "outside":
				request.Corrections[0].FromTs -= 3600
			case "invalid":
				request.Corrections[0].Credit = -1
			case "empty":
				request.Corrections = nil
			}
			output := filepath.Join(t.TempDir(), "invalid")
			if result, err := RehearseFinanceRechargeCorrections(ctx, source, output, request); err == nil || result.Mode != "" {
				t.Fatal("accepted unsafe proposal", result, err)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatal("invalid proposal created output")
			}
		})
	}
	if giftLocalFileHash(t, source) != base.SourceSHA256 {
		t.Fatal("negative cases mutated source")
	}
}

func TestFinanceRechargeRehearsalDoesNotDisguiseRealMidBucketChange(t *testing.T) {
	source, request := rechargeRehearsalFixture(t)
	// Only half of the first nonzero bucket is covered. Its unknown amount
	// cannot be filled by treating the missing half as zero or prorating it.
	request.Corrections[0].FromTs += 1800
	result, err := RehearseFinanceRechargeCorrections(context.Background(), source, filepath.Join(t.TempDir(), "partial"), request)
	if err != nil {
		t.Fatal(err)
	}
	v := result.Domains[0].Total
	if v.NewlyCosted != 0 || v.RemainingUnknown != 1 || v.KnownAfterMicro != 2_000_000 {
		t.Fatal("partial bucket turned into confirmed full cost", v)
	}
}

func TestFinanceRechargeRehearsalReportsAbsentEconomicsWithoutInventingIt(t *testing.T) {
	source, request := rechargeRehearsalFixture(t)
	request.RebuildEconomics = true
	result, err := RehearseFinanceRechargeCorrections(context.Background(), source, filepath.Join(t.TempDir(), "with-economics"), request)
	if err != nil || len(result.Economics) != 1 {
		t.Fatal("local economics linkage failed", result.Economics, err)
	}
	row := result.Economics[0]
	if row.Status != "no_verified_cost_evidence" || row.WindowHours != 48 || row.WithoutVerifiedHours != 48 || row.RebuiltHours != 0 || result.Publishable {
		t.Fatal("missing channel evidence was fabricated", row)
	}
	if giftLocalFileHash(t, source) != request.SourceSHA256 {
		t.Fatal("source mutated")
	}
}

func TestFinanceRechargeRehearsalRollsBackFailedAppend(t *testing.T) {
	source := rechargeEvidenceClosedFixture(t, func(m *Monitor, account ChannelUpstreamAccount, scope stabilityScope) {
		createChannelRechargeVersion(t, m, account.Domain, 2, scope.ToTs, 1, 7.14)
		if err := m.storeDB.Exec(`CREATE TRIGGER reject_rehearsal BEFORE INSERT ON channel_finance_versions
			WHEN NEW.version>=4 BEGIN SELECT RAISE(ABORT,'test failure'); END`).Error; err != nil {
			t.Fatal(err)
		}
	})
	from, _ := financeStartHour("2026-05-01")
	request := FinanceRechargeRehearsalRequest{SourceSHA256: giftLocalFileHash(t, source), FromDate: "2026-05-01", ToDate: "2026-05-03",
		Corrections: []FinanceRechargeCorrection{{Domain: "hour.example", FromTs: from, ToTs: from + 2*86400, Paid: 1, Credit: 1, Reason: "rollback test"}}}
	output := filepath.Join(t.TempDir(), "failure")
	if result, err := RehearseFinanceRechargeCorrections(context.Background(), source, output, request); err == nil || result.Mode != "" {
		t.Fatal("failed write published success", result, err)
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(output, "monitor-rehearsal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var count int64
	if err := db.Model(&ChannelFinanceVersion{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("failed append left partial history", count, err)
	}
	if _, err := os.Stat(filepath.Join(output, "rehearsal-result.json")); !os.IsNotExist(err) {
		t.Fatal("failed write created success receipt")
	}
	if giftLocalFileHash(t, source) != request.SourceSHA256 {
		t.Fatal("failed rehearsal modified source")
	}
}
