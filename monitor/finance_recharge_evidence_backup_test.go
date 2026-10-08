//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func rechargeEvidenceClosedFixture(t *testing.T, change func(*Monitor, ChannelUpstreamAccount, stabilityScope)) string {
	t.Helper()
	m, scope, _ := dailyBillFixture(t)
	if err := m.storeDB.AutoMigrate(&ChannelUpstreamFundEvent{}, &UpstreamFundSyncState{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "hour.example", Provider: upstreamProviderNewAPI, UsageSyncEnabled: true,
		BaseURL: "https://hour.example", Account: "private-account", Credential: "private-credential"}
	if err := m.storeDB.Save(&account).Error; err != nil {
		t.Fatal(err)
	}
	createChannelRechargeVersion(t, m, account.Domain, 1, scope.FromTs+86400, 1, 7.14)
	base := ChannelUpstreamFundEvent{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), EventKey: "current",
		OccurredAt: scope.FromTs + 3600, Provider: account.Provider, SourceType: 1, Kind: upstreamFundKindTopup, Direction: "credit",
		PaidAmount: 1, PaidKnown: true, UpstreamAmount: 7.14, UpstreamAmountKnown: true, ObservedCount: 1,
		Confidence: upstreamFundConfidenceLegacyText, ParserVersion: upstreamFundParserVersion,
		RawJSON: "private-original", Content: "private-content", RequestID: "private-request"}
	if err := m.storeDB.Create(&base).Error; err != nil {
		t.Fatal(err)
	}
	base.AccountEpoch, base.EventKey, base.PaidAmount = "old-account", "old", 7
	if err := m.storeDB.Create(&base).Error; err != nil {
		t.Fatal(err)
	}
	state := UpstreamFundSyncState{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), Status: "ok",
		CoverageFrom: scope.FromTs, TailSyncedUntil: scope.ToTs, BackfillDone: true}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if change != nil {
		change(m, account, scope)
	}
	path := filepath.Join(t.TempDir(), "closed.db")
	if err := m.storeDB.Exec("VACUUM INTO ?", path).Error; err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFinanceRechargeEvidenceBackupIsPrivateIsolatedAndRepeatable(t *testing.T) {
	path := rechargeEvidenceClosedFixture(t, nil)
	before := giftLocalFileHash(t, path)
	read := func() (FinanceRechargeEvidenceInspection, error) {
		return InspectFinanceRechargeEvidenceBackup(context.Background(), path, "2026-05-01", "2026-05-03", []string{"hour.example"})
	}
	first, err := read()
	if err != nil || first.SourceSnapshotSHA256 != before || first.AutoApplyAllowed || len(first.Domains) != 1 {
		t.Fatal("private read did not produce a pinned, non-authorizing review", first, err)
	}
	domain := first.Domains[0]
	if !domain.BillComplete || domain.Funds.ExcludedAccountRows != 1 || domain.Funds.ReferenceTopups != 1 || !domain.Funds.HistoryCoversQuery ||
		domain.ReferencePreview == nil || domain.ReferencePreview.Total.Buckets != 1 || domain.ReferencePreview.Publishable {
		t.Fatal("evidence identities/completeness/sensitivity conflated", domain)
	}
	data, err := json.Marshal(first)
	if err != nil || strings.Contains(string(data), "private-") || strings.Contains(string(data), "old-account") || strings.Contains(string(data), "raw_json") {
		t.Fatal("sensitive evidence leaked into review", err)
	}
	second, err := read()
	if err != nil || !reflect.DeepEqual(first, second) || giftLocalFileHash(t, path) != before {
		t.Fatal("review changed original data or report inputs", err)
	}
	for _, suffix := range []string{"-wal", "-journal", "-shm"} {
		if _, err := os.Lstat(path + suffix); !os.IsNotExist(err) {
			t.Fatal("offline read created a sidecar", suffix, err)
		}
	}
}

func TestFinanceRechargeEvidenceBackupNeverRepairsUnverifiedBills(t *testing.T) {
	for _, mode := range []string{"invalid", "provisional", "missing_funds", "recent_history", "partial_history", "bad_interval"} {
		t.Run(mode, func(t *testing.T) {
			path := rechargeEvidenceClosedFixture(t, func(m *Monitor, account ChannelUpstreamAccount, scope stabilityScope) {
				var err error
				switch mode {
				case "invalid":
					err = m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", account.Domain, scope.FromTs).Update("cost_usd", -1).Error
				case "provisional":
					err = m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", account.Domain, scope.FromTs).Update("provisional", true).Error
				case "missing_funds":
					err = m.storeDB.Where("domain=?", account.Domain).Delete(&ChannelUpstreamFundEvent{}).Error
					if err == nil {
						err = m.storeDB.Where("domain=?", account.Domain).Delete(&UpstreamFundSyncState{}).Error
					}
				case "recent_history":
					err = m.storeDB.Model(&UpstreamFundSyncState{}).Where("domain=?", account.Domain).Update("history_scope", "provider_recent").Error
				case "partial_history":
					err = m.storeDB.Model(&UpstreamFundSyncState{}).Where("domain=?", account.Domain).Update("coverage_from", scope.FromTs+3600).Error
				case "bad_interval":
					err = m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", account.Domain, scope.FromTs).Update("bucket_seconds", 7200).Error
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			value, err := InspectFinanceRechargeEvidenceBackup(context.Background(), path, "2026-05-01", "2026-05-03", []string{"hour.example"})
			if mode == "bad_interval" {
				if err == nil || value.Mode != "" {
					t.Fatal("invalid interval produced a usable review")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			domain := value.Domains[0]
			if (mode == "invalid" || mode == "provisional") && domain.ReferencePreview != nil {
				t.Fatal("unverified bill produced a hypothetical cost")
			}
			if mode == "missing_funds" && (domain.Funds.Rows != 0 || len(domain.Funds.References) != 0 || domain.Funds.SyncStateKnown) {
				t.Fatal("missing history was fabricated")
			}
			if (mode == "missing_funds" || mode == "recent_history" || mode == "partial_history") && domain.Funds.HistoryCoversQuery {
				t.Fatal("partial fund collection claimed all-history coverage")
			}
		})
	}
}

func TestFinanceRechargeEvidenceBackupRejectsUnsafeInputs(t *testing.T) {
	path := rechargeEvidenceClosedFixture(t, nil)
	for _, domains := range [][]string{nil, {"hour.example", "hour.example"}, {"Hour.example"}, {"https://hour.example"}, {"missing.example"}, make([]string, 9)} {
		if value, err := InspectFinanceRechargeEvidenceBackup(context.Background(), path, "2026-05-01", "2026-05-03", domains); err == nil || value.Mode != "" {
			t.Fatal("ambiguous domain scope produced a review", domains)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if value, err := InspectFinanceRechargeEvidenceBackup(ctx, path, "2026-05-01", "2026-05-03", []string{"hour.example"}); err == nil || value.Mode != "" {
		t.Fatal("canceled read produced a usable review")
	}
	if err := os.WriteFile(path+"-wal", []byte("pending"), 0600); err != nil {
		t.Fatal(err)
	}
	if value, err := InspectFinanceRechargeEvidenceBackup(context.Background(), path, "2026-05-01", "2026-05-03", []string{"hour.example"}); err == nil || value.Mode != "" {
		t.Fatal("live/writable backup was accepted")
	}
}

func TestFinanceRechargeEvidenceBackupDoesNotPublishTruncatedFunds(t *testing.T) {
	path := rechargeEvidenceClosedFixture(t, func(m *Monitor, account ChannelUpstreamAccount, scope stabilityScope) {
		rows := make([]ChannelUpstreamFundEvent, financeRechargeEvidenceRowLimit)
		for i := range rows {
			rows[i] = ChannelUpstreamFundEvent{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account),
				EventKey: fmt.Sprintf("budget-%d", i), OccurredAt: scope.FromTs + 3600}
		}
		if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
			t.Fatal(err)
		}
	})
	before := giftLocalFileHash(t, path)
	value, err := InspectFinanceRechargeEvidenceBackup(context.Background(), path, "2026-05-01", "2026-05-03", []string{"hour.example"})
	if err == nil || value.Mode != "" || giftLocalFileHash(t, path) != before {
		t.Fatal("truncated evidence became a usable review or changed its source", err)
	}
}
