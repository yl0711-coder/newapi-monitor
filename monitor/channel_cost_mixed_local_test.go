//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const (
	mixedLocalFrom      = int64(1786377600) // 2026-08-11 00:00 CST, inclusive.
	mixedLocalTo        = int64(1786636800) // 2026-08-14 00:00 CST, exclusive.
	mixedLocalFactsHash = "af417cf4b86d879ea02a988beb0bac4c9d56dc15db4f0dfc73da5c5f6d6b56be"
)

var mixedLocalCandidates = []struct {
	Channel         int
	Source          string
	Hours, Requests int64
}{
	{59, "e10c30d28bb916c7ace12b8397e5e63669f22402a15d02905a2c0feefc095e06", 55, 27386},
	{60, "347bbddd8a3921d4cf5d8f626307e98d59e204c5161f18610ad62bae3f1cde4c", 42, 424},
}

// A bounded, hypothetical attribution on a NEW local copy, never a production
// plan. Original usage facts are read-only; no source client or worker starts.
func TestChannelCostMixedTrafficSealedLocalRehearsal(t *testing.T) {
	mainPath, factsPath := os.Getenv("MONITOR_MIXED_ACCEPTANCE_DB"), os.Getenv("MONITOR_MIXED_ACCEPTANCE_FACTS")
	if mainPath == "" || factsPath == "" {
		t.Skip("requires both sealed local fixtures; never uses a running database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if giftLocalFileHash(t, factsPath) != mixedLocalFactsHash {
		t.Fatal("usage fixture hash differs from the inspected closed backup")
	}
	facts, err := openGiftRelevantBackup(factsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer facts.close()
	db, output := openAttributionLocalCopyAt(t, mainPath, os.Getenv("MONITOR_MIXED_ACCEPTANCE_OUTPUT"))
	m := &Monitor{storeDB: db, usageFactsDB: facts.db, cfg: Settings{
		LocalSnapshotOnly: true, FinanceEnabled: true, FinanceStartDate: "2026-05-01",
		UsageFactsHistorySourceEpoch: "newapi-hotlogs-complete-20260817-v1",
		ChannelCostClosureEnabled:    true, ChannelCostClosureDomains: []string{attributionLocalDomain},
	}}
	var account ChannelUpstreamAccount
	if err := db.Select("domain", "provider", "base_url", "user_id", "account").First(&account, "domain=?", attributionLocalDomain).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: mixedLocalFrom, ToTs: mixedLocalTo}
	plans, hours := mixedLocalPlans(ctx, t, m, account, scope)
	protected := attributionProtectedChannelQueries(t, db, account, hours, []int{0, 59, 60})
	beforeProtected := attributionQueryDigests(ctx, t, db, protected)
	beforeTotals := attributionHourTotals(t, db, account, hours)
	beforeRows := mixedLocalRows(t, db, account, hours)
	beforeViews := mixedLocalViews(ctx, t, m, scope)
	t.Log("sealed baseline, internal-account scope and bounded candidates verified")

	mixedLocalSaveAndRetry(ctx, t, m, plans, hours)
	bindingGuard := attributionLocalQuery{name: "created_mixed_bindings", sql: "SELECT * FROM channel_cost_source_bindings ORDER BY rowid"}
	protected = append(protected, bindingGuard)
	beforeProtected[bindingGuard.name] = attributionQueryDigest(ctx, t, db, bindingGuard)
	checkAttributionLocalRollback(ctx, t, m, account, hours[0])
	now := time.Now().Unix() + 120
	for attempt := 0; attributionRowCount(t, db, "channel_economics_dirty_hours") > 0 && attempt <= len(hours); attempt++ {
		if err := m.publishDueChannelEconomicsBatch(ctx, account, now); err != nil {
			t.Fatal("local publication queue failed", err)
		}
	}
	if attributionRowCount(t, db, "channel_economics_dirty_hours") != 0 {
		t.Fatal("bounded local queue did not drain")
	}
	afterTotals := attributionHourTotals(t, db, account, hours)
	if !reflect.DeepEqual(beforeTotals, afterTotals) {
		t.Fatal("attribution changed raw/corrected total cost or local revenue")
	}
	afterRows := mixedLocalRows(t, db, account, hours)
	verifyMixedLocalRows(t, db, account, hours, beforeRows, afterRows)
	afterViews := mixedLocalViews(ctx, t, m, scope)
	verifyMixedLocalViews(t, beforeViews, afterViews)
	if !reflect.DeepEqual(beforeProtected, attributionQueryDigests(ctx, t, db, protected)) {
		t.Fatal("out-of-scope facts or immutable historical revisions changed")
	}
	checkAttributionLocalPublicationRetry(ctx, t, m, account, hours, now)
	retryViews := mixedLocalViews(ctx, t, m, scope)
	if !reflect.DeepEqual(afterViews, retryViews) {
		t.Fatal("a repeated read/publication changed finance projections")
	}
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("local copy integrity check failed", err)
	}
	if err := facts.unchanged(); err != nil || giftLocalFileHash(t, factsPath) != mixedLocalFactsHash || giftLocalFileHash(t, mainPath) != attributionLocalSHA256 {
		t.Fatal("an original fixture was changed", err)
	}
	receipt := map[string]any{
		"mode": "unconfirmed_local_mixed_traffic_rehearsal", "from": scope.FromTs, "to": scope.ToTs,
		"source_sha256": attributionLocalSHA256, "facts_sha256": mixedLocalFactsHash,
		"channels": []int{59, 60}, "rebuilt_hours": hours, "before_totals": beforeTotals, "after_totals": afterTotals,
		"before_views": beforeViews, "after_views": afterViews, "target_rows": afterRows,
		"protected_queries": len(protected), "scope_unchanged": true, "totals_conserved": true,
		"old_publications_preserved": true, "retry_noop": true, "manifest_failure_rolled_back": true,
		"historical_ownership_confirmed": false, "production_applicable": false,
		"notes": []string{"The two current-key matches are assumed ONLY in this 72-hour private copy.",
			"Mixed customer/test costs are not split by request count, revenue or tokens.",
			"The earlier fixture's unconfirmed recharge ratios are unchanged; this is not a verified profit report."},
	}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := giftLocalWriteNew(filepath.Join(output, "acceptance.json"), encoded); err != nil {
		t.Fatal(err)
	}
	t.Logf("PASS: %d local hours; costs conserved; mixed costs not estimated; %d scope guards; output %s", len(hours), len(protected), output)
}
