//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Opt-in, offline acceptance of previously reviewed finite proposals against
// a newer closed backup. It never overwrites the input or creates live authority.
func TestFinanceRechargeReuseCurrentSnapshot(t *testing.T) {
	manifestPath := os.Getenv("MONITOR_RECHARGE_REUSE_MANIFEST")
	if manifestPath == "" {
		t.Skip("requires an explicit private manifest of closed backups and reviewed proposals")
	}
	rechargeReuseCurrentSnapshot(t, manifestPath)
}

func rechargeReuseCurrentSnapshot(t *testing.T, manifestPath string) (string, []FinanceRechargeRehearsalRequest) {
	t.Helper()
	var manifest struct {
		Source, SourceSHA256   string
		Archive, ArchiveSHA256 string
		Plans                  []struct{ Path, SHA256 string }
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Plans) == 0 || len(manifest.Plans) > 8 {
		t.Fatal("requires 1 to 8 reviewed batches")
	}
	inputs := map[string]string{manifest.Source: manifest.SourceSHA256, manifest.Archive: manifest.ArchiveSHA256}
	for _, plan := range manifest.Plans {
		if _, duplicate := inputs[plan.Path]; duplicate {
			t.Fatal("duplicate input")
		}
		inputs[plan.Path] = plan.SHA256
	}
	for path, hash := range inputs {
		if !filepath.IsAbs(path) || hash == "" || giftLocalFileHash(t, path) != hash {
			t.Fatal("input identity mismatch", filepath.Base(path))
		}
	}
	t.Cleanup(func() {
		for path, hash := range inputs {
			if giftLocalFileHash(t, path) != hash {
				t.Error("original input changed", filepath.Base(path))
			}
		}
	})
	baseline, closeBase, err := giftLocalReadonlyDatabase(manifest.Source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBase()
	archive, closeArchive, err := giftLocalReadonlyDatabase(manifest.Archive)
	if err != nil {
		t.Fatal(err)
	}
	defer closeArchive()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root, source, sourceHash := t.TempDir(), manifest.Source, manifest.SourceSHA256
	var requests []FinanceRechargeRehearsalRequest
	var results []FinanceRechargeRehearsalResult
	totalAdded := 0
	for i, plan := range manifest.Plans {
		data, err := os.ReadFile(plan.Path)
		if err != nil {
			t.Fatal(err)
		}
		var request FinanceRechargeRehearsalRequest
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatal(err)
		}
		for _, correction := range request.Corrections {
			rechargeReuseIdentity(t, baseline, archive, correction)
		}
		// Refresh only the receiver identity. Exact domain, interval and ratio
		// remain the reviewed values. Ledger rebuilding is a separate acceptance.
		request.SourceSHA256, request.RebuildEconomics = sourceHash, false
		output := filepath.Join(root, fmt.Sprintf("batch-%02d", i))
		result, err := RehearseFinanceRechargeCorrections(ctx, source, output, request)
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
		if result.Publishable || result.Mode != "unconfirmed_local_rehearsal" {
			t.Fatal("local rehearsal gained publication authority")
		}
		for _, domain := range result.Domains {
			for _, periods := range [][]FinanceRechargeRehearsalAmounts{domain.Days, domain.Months} {
				var sum FinanceRechargeRehearsalAmounts
				for _, value := range periods {
					if err := addRechargeRehearsalAmounts(&sum, value); err != nil {
						t.Fatal(err)
					}
				}
				sum.Period = "total"
				if sum != domain.Total {
					t.Fatal("daily/monthly money or counts do not reconcile", domain.Domain)
				}
			}
		}
		requests, results = append(requests, request), append(results, result)
		totalAdded += result.AddedVersions
		// Remove only this test's previous intermediate DB; retain receipts and
		// never delete either input. At most two large temporary copies coexist.
		if i > 0 {
			if err := os.Remove(source); err != nil {
				t.Fatal(err)
			}
		}
		source, sourceHash = filepath.Join(output, "monitor-rehearsal.db"), result.OutputSHA256
	}
	revised, closeRevised, err := giftLocalReadonlyDatabase(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeRevised()
	for _, request := range requests {
		from, err := financeStartHour(request.FromDate)
		if err != nil {
			t.Fatal(err)
		}
		to, err := financeStartHour(request.ToDate)
		if err != nil {
			t.Fatal(err)
		}
		_, added, err := prepareFinanceRechargeRehearsal(ctx, revised, request, stabilityScope{FromTs: from, ToTs: to}, time.Now().Unix())
		if err != nil || len(added) != 0 {
			t.Fatal("combined plan replay is not idempotent", err, len(added))
		}
	}
	rechargeReuseUnchangedTables(t, revised, manifest.Source, totalAdded)
	fromDate, toDate := requests[0].FromDate, requests[0].ToDate
	for _, request := range requests {
		fromDate, toDate = min(fromDate, request.FromDate), max(toDate, request.ToDate)
	}
	beforeGaps, err := InspectFinanceRechargeBackup(ctx, manifest.Source, fromDate, toDate, FinanceRechargeInspectionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	afterGaps, err := InspectFinanceRechargeBackup(ctx, source, fromDate, toDate, FinanceRechargeInspectionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeGaps.BillCoverage, afterGaps.BillCoverage) || !reflect.DeepEqual(beforeGaps.Skipped, afterGaps.Skipped) {
		t.Fatal("ratio repair changed original bill coverage or eligible-source boundary")
	}
	var missingBefore, missingAfter int64
	for i, inspection := range []FinanceRechargeGapInspection{beforeGaps, afterGaps} {
		var missing int64
		for _, domain := range inspection.Domains {
			for _, gap := range domain.Gaps {
				missing += gap.NonzeroCostBuckets
			}
		}
		if i == 0 {
			missingBefore = missing
		} else {
			missingAfter = missing
		}
	}
	if missingAfter >= missingBefore {
		t.Fatal("reviewed plans did not reduce ratio gaps")
	}
	t.Logf("eligible nonzero bill buckets missing terms: %d -> %d; remaining domains=%d; bill coverage unchanged", missingBefore, missingAfter, len(afterGaps.Domains))
	if path := os.Getenv("MONITOR_RECHARGE_REUSE_RESULT"); path != "" {
		data, err := json.MarshalIndent(struct {
			Batches []FinanceRechargeRehearsalResult `json:"batches"`
			Before  FinanceRechargeGapInspection     `json:"before"`
			After   FinanceRechargeGapInspection     `json:"after"`
		}{results, beforeGaps, afterGaps}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(path, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("PASS: %d reviewed batches; %d append-only versions; original facts/settings unchanged; day/month conservation and combined replay verified", len(results), totalAdded)
	return source, requests
}

func rechargeReuseIdentity(t *testing.T, current, archive *gorm.DB, correction FinanceRechargeCorrection) {
	t.Helper()
	var accounts [2]ChannelUpstreamAccount
	for i, db := range []*gorm.DB{current, archive} {
		if err := db.Select("domain", "provider", "base_url", "account", "user_id", "usage_adapter").Where("domain=?", correction.Domain).First(&accounts[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	if newAPIUpstreamAccountEpoch(accounts[0]) != newAPIUpstreamAccountEpoch(accounts[1]) || accounts[0].UsageAdapter != accounts[1].UsageAdapter {
		t.Fatal("account identity/adapter changed; reviewed historical scope requires separate review", correction.Domain)
	}
	// A new production edit inside the reviewed history cannot be silently
	// overridden by an old local proposal. Compare non-rehearsal versions.
	type recorded struct {
		Version, EffectiveAt int64
		SnapshotJSON         string
	}
	var histories [2][]recorded
	for i, db := range []*gorm.DB{current, archive} {
		if err := db.Model(&ChannelFinanceVersion{}).Select("version", "effective_at", "snapshot_json").
			Where("domain=? AND effective_at<=? AND COALESCE(updated_by,'')<>?", correction.Domain, correction.ToTs, "local-recharge-rehearsal").
			Order("effective_at,version").Scan(&histories[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(histories[0], histories[1]) {
		t.Fatal("recorded historical configuration changed since review", correction.Domain)
	}
}

func rechargeReuseUnchangedTables(t *testing.T, revised *gorm.DB, original string, added int) {
	t.Helper()
	uri := (&url.URL{Scheme: "file", Path: original, RawQuery: "mode=ro&immutable=1"}).String()
	if err := revised.Exec("ATTACH DATABASE ? AS original", uri).Error; err != nil {
		t.Fatal(err)
	}
	defer revised.Exec("DETACH DATABASE original")
	var tables []string
	if err := revised.Raw("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").Scan(&tables).Error; err != nil {
		t.Fatal(err)
	}
	for _, table := range tables {
		quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		var oldCount, newCount, diff int64
		if err := revised.Raw("SELECT COUNT(*) FROM original." + quoted).Scan(&oldCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := revised.Raw("SELECT COUNT(*) FROM main." + quoted).Scan(&newCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := revised.Raw("SELECT COUNT(*) FROM (SELECT * FROM original." + quoted + " EXCEPT SELECT * FROM main." + quoted + ")").Scan(&diff).Error; err != nil {
			t.Fatal(err)
		}
		if diff != 0 {
			t.Fatal("original records changed or removed", table)
		}
		if table == "channel_finance_versions" {
			if newCount-oldCount != int64(added) {
				t.Fatal("unexpected historical version count")
			}
			// MAX(version) remains an unchanged current-configuration anchor.
			var changed int64
			query := `SELECT COUNT(*) FROM original.channel_finance_versions o
			WHERE o.version=(SELECT MAX(version) FROM original.channel_finance_versions WHERE domain=o.domain)
			AND NOT EXISTS (SELECT 1 FROM main.channel_finance_versions n WHERE n.domain=o.domain
			AND n.version=(SELECT MAX(version) FROM main.channel_finance_versions WHERE domain=o.domain)
			AND n.snapshot_json=o.snapshot_json AND n.effective_at=o.effective_at)`
			if err := revised.Raw(query).Scan(&changed).Error; err != nil || changed != 0 {
				t.Fatal("current configuration anchor changed", err)
			}
		} else if newCount != oldCount {
			t.Fatal("unrelated table gained rows", table)
		}
	}
	t.Logf("unchanged original rows verified across %d tables", len(tables))
}
