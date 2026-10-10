//go:build unix

package monitor

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"
)

// These candidates reuse the same guarded copy/retry/rollback/conservation
// suite as earlier rehearsals, with a separately pinned, rate-corrected input.
// Exact hour/model aggregates support review; they do not confirm ownership.
const rateCorrectedAttributionSHA = "f20c4395499e8e3a442c602b72da03e8b093ca5d5bf5759e8f6514bd07a6f7bc"

func TestChannelCostRateCorrectedCodexSealedRehearsal(t *testing.T) {
	rehearseSealedCostCandidate(t, sealedCostCandidate{
		snapshotSHA256: rateCorrectedAttributionSHA, databaseEnv: "MONITOR_RATE_CORRECTED_ACCEPTANCE_DB",
		channel: 59, sourceRef: "e10c30d28bb916c7ace12b8397e5e63669f22402a15d02905a2c0feefc095e06",
		from:             1786420800, // Aug 11, 12–14 CST, exclusive end.
		upstreamRequests: []int64{2, 11}, charges: []int64{95931, 51545},
		customerRequests: []int64{2, 11}, revenue: []int64{338672, 218068},
		expectUnallocated: true, // The other source must remain unresolved.
	})
}

func TestChannelCostRateCorrectedClaudeSealedRehearsal(t *testing.T) {
	rehearseSealedCostCandidate(t, sealedCostCandidate{
		snapshotSHA256: rateCorrectedAttributionSHA, databaseEnv: "MONITOR_RATE_CORRECTED_ACCEPTANCE_DB",
		channel: 60, sourceRef: "347bbddd8a3921d4cf5d8f626307e98d59e204c5161f18610ad62bae3f1cde4c",
		from:             1786420800,
		upstreamRequests: []int64{4, 3}, charges: []int64{43477, 16309},
		customerRequests: []int64{4, 3}, revenue: []int64{113040, 42404},
		expectUnallocated: true,
	})
}

func TestChannelCostSharedHistorySealedRehearsal(t *testing.T) {
	source := os.Getenv("MONITOR_RATE_CORRECTED_ACCEPTANCE_DB")
	if source == "" {
		t.Skip("requires the pinned sealed local snapshot")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, _ := openAttributionLocalCopyWithHashAt(t, source, "", rateCorrectedAttributionSHA)
	m := &Monitor{storeDB: db.WithContext(ctx), cfg: Settings{LocalSnapshotOnly: true,
		ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}}}
	var account ChannelUpstreamAccount
	if err := db.Select("domain", "provider", "base_url", "user_id", "account").First(&account, "domain=?", "4sapi.com").Error; err != nil {
		t.Fatal(err)
	}
	hours := []int64{1786420800, 1786424400}
	plan, status, err := m.planChannelCostHistoricalBinding(ctx, channelCostHistoricalBindingInput{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), AllocationMode: "shared",
		SourceRef: "e10c30d28bb916c7ace12b8397e5e63669f22402a15d02905a2c0feefc095e06",
		ValidFrom: historyRangeHour(hours[0]), ValidTo: historyRangeHour(hours[1] + 3600),
		Reason: "HYPOTHETICAL LOCAL SHARED TEST ONLY: not a confirmed historical ownership record",
	}, "local-test-only")
	if err != nil || status != 200 || plan.EvidenceHours != 2 || plan.EvidenceRequests != 13 ||
		plan.EvidenceBilledCost.MicroUSD != "294952" || plan.WillQueueHours != 2 || plan.Binding.LocalChannelID != 0 {
		t.Fatal("pinned shared preview mismatch", plan, status, err)
	}
	protected := attributionProtectedChannelQueries(t, db, account, hours, []int{0})
	before := attributionQueryDigests(ctx, t, db, protected)
	beforeTotals := attributionHourTotals(t, db, account, hours)
	checkAttributionLocalBindingRetry(ctx, t, m, plan.Binding, hours)
	for range hours {
		if err := m.publishOneDueChannelEconomicsHourOrdered(ctx, account, time.Now().Unix()+120, true); err != nil {
			t.Fatal(err)
		}
	}
	if attributionRowCount(t, db, "channel_economics_dirty_hours") != 0 ||
		!reflect.DeepEqual(beforeTotals, attributionHourTotals(t, db, account, hours)) ||
		!reflect.DeepEqual(before, attributionQueryDigests(ctx, t, db, protected)) {
		t.Fatal("shared marking changed amounts, protected facts or left work queued")
	}
	var falseProfits int64
	if err := currentEconomicsPublicationQuery(db).Where("p.domain=? AND p.hour_ts IN ? AND p.profit_known=1", account.Domain, hours).Count(&falseProfits).Error; err != nil || falseProfits != 0 {
		t.Fatal("shared evidence invented channel profit", falseProfits, err)
	}
	checkAttributionLocalPublicationRetry(ctx, t, m, account, hours, time.Now().Unix()+180)
	if giftLocalFileHash(t, source) != rateCorrectedAttributionSHA {
		t.Fatal("original snapshot changed")
	}
	t.Log("PASS hypothetical shared marking: exact costs conserved; no channel profit, duplicate rows, outside-scope writes or original snapshot changes")
}
