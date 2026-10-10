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

// A log-correlated candidate is not a confirmed historical identity. This
// acceptance always creates a private copy and never changes the input file.
func TestChannelCostShortSplitSealedLocalRehearsal(t *testing.T) {
	rehearseSealedCostCandidate(t, sealedCostCandidate{
		channel: 86, sourceRef: "83402a1f7f5fc0782d298a2706c7184be41a1f62d53dee1fb4b4b60b90c0365d",
		from:             1787724000, // Aug 26, 14–16 CST.
		upstreamRequests: []int64{6, 8}, charges: []int64{474, 178777},
		customerRequests: []int64{0, 3}, revenue: []int64{0, 509520},
		testHours: 2, testRequests: 11, expectUnallocated: true,
	})
}

func TestChannelCostContinuousSealedLocalRehearsal(t *testing.T) {
	// An independently inspected, contiguous no-test segment. Do not stretch
	// this interval across mismatching hours or the later routing split.
	rehearseSealedCostCandidate(t, sealedCostCandidate{
		channel: 59, sourceRef: "e10c30d28bb916c7ace12b8397e5e63669f22402a15d02905a2c0feefc095e06",
		from:             1785625200, // Aug 2, 07–17 CST.
		upstreamRequests: []int64{1, 2, 2, 4, 4, 4, 3, 2, 1, 11},
		charges:          []int64{17398, 6974, 6860, 13858, 14569, 19833, 13311, 8700, 4287, 201676},
		customerRequests: []int64{1, 2, 2, 4, 4, 4, 3, 2, 1, 11},
		revenue:          []int64{61958, 26336, 25804, 52252, 55120, 75252, 50730, 33008, 16210, 623824},
	})
}

func TestChannelCostFailedRequestsSealedLocalRehearsal(t *testing.T) {
	// These three hours have 38 additional local failed records (also present
	// in the problem ledger). They must not subtract from actual upstream cost.
	rehearseSealedCostCandidate(t, sealedCostCandidate{
		channel: 59, sourceRef: "e10c30d28bb916c7ace12b8397e5e63669f22402a15d02905a2c0feefc095e06",
		from:             1785927600, // Aug 5, 19–22 CST.
		upstreamRequests: []int64{153, 1, 7}, charges: []int64{7627053, 965, 14670},
		customerRequests: []int64{157, 7, 35}, revenue: []int64{18512364, 2926, 210306},
		failedRequests: 38,
	})
}

type sealedCostCandidate struct {
	snapshotSHA256, databaseEnv                          string
	channel                                              int
	sourceRef                                            string
	from                                                 int64
	upstreamRequests, charges, customerRequests, revenue []int64
	testHours, testRequests                              int64
	failedRequests                                       int64
	expectUnallocated                                    bool
}

func rehearseSealedCostCandidate(t *testing.T, fixture sealedCostCandidate) {
	t.Helper()
	databaseEnv := fixture.databaseEnv
	if databaseEnv == "" {
		databaseEnv = "MONITOR_SPLIT_ACCEPTANCE_DB"
	}
	source := os.Getenv(databaseEnv)
	if source == "" {
		t.Skip("requires a pinned sealed local snapshot")
	}
	digest := fixture.snapshotSHA256
	if digest == "" {
		digest = "94b5ee4e2bdce45228676eb61ce4e54c0dc5e95dc8baa00f618eaf9c7f37f0b2"
	}
	from, to := fixture.from, fixture.from+int64(len(fixture.charges))*3600
	var upstreamRequests, customerRequests, revenue, sourceCost, activeHours int64
	hours := make([]int64, len(fixture.charges))
	for i, charges := range fixture.charges {
		hours[i] = from + int64(i)*3600
		upstreamRequests += fixture.upstreamRequests[i]
		customerRequests += fixture.customerRequests[i]
		revenue += fixture.revenue[i]
		sourceCost += charges * 2 // Both pinned examples have 500,000 units and a 1:1 ratio.
		if fixture.customerRequests[i] > 0 {
			activeHours++
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	outputPath := os.Getenv("MONITOR_SPLIT_ACCEPTANCE_OUTPUT")
	if outputPath != "" {
		outputPath = filepath.Join(outputPath, t.Name())
	}
	db, output := openAttributionLocalCopyWithHashAt(t, source, outputPath, digest)
	db = db.WithContext(ctx)
	m := &Monitor{storeDB: db, cfg: Settings{LocalSnapshotOnly: true,
		ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}}}
	var account ChannelUpstreamAccount
	if err := db.Select("domain", "provider", "base_url", "user_id", "account").First(&account, "domain=?", "4sapi.com").Error; err != nil {
		t.Fatal(err)
	}
	plan, status, err := m.planChannelCostHistoricalBinding(ctx, channelCostHistoricalBindingInput{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SourceRef: fixture.sourceRef,
		LocalChannelID: fixture.channel, ValidFrom: historyRangeHour(from), ValidTo: historyRangeHour(to),
		Reason: "UNCONFIRMED LOCAL CANDIDATE ONLY: log-correlated finite interval; no production or current preview authorization",
	}, "local-test-only")
	if err != nil || status != 200 || plan.Binding.ValidFrom != from || plan.Binding.ValidTo != to ||
		plan.EvidenceHours != int64(len(hours)) || plan.EvidenceRequests != upstreamRequests || plan.LocalActiveHours != activeHours || plan.LocalRequests != customerRequests ||
		plan.LocalTestActiveHours != fixture.testHours || plan.LocalTestRequests != fixture.testRequests || plan.EvidenceBilledCost != economicsMoney(sourceCost) ||
		plan.LocalFailedRequests != fixture.failedRequests || plan.LocalTestFailedRequests != 0 {
		t.Fatal("candidate differs from independently inspected log dimensions", plan, status, err)
	}
	protected := attributionProtectedChannelQueries(t, db, account, hours, []int{0, fixture.channel})
	beforeProtected := attributionQueryDigests(ctx, t, db, protected)
	beforeTotals := attributionHourTotals(t, db, account, hours)
	t.Log("pinned snapshot and finite candidate verified")
	checkAttributionLocalBindingRetry(ctx, t, m, plan.Binding, hours)
	checkAttributionLocalRollback(ctx, t, m, account, hours[0])
	for i := 0; i < len(hours); i++ {
		if err := m.publishOneDueChannelEconomicsHourOrdered(ctx, account, time.Now().Unix()+120, true); err != nil {
			t.Fatal(err)
		}
	}
	if attributionRowCount(t, db, "channel_economics_dirty_hours") != 0 {
		t.Fatal("finite queue not drained")
	}
	afterTotals := attributionHourTotals(t, db, account, hours)
	if !reflect.DeepEqual(beforeTotals, afterTotals) {
		t.Fatal("assignment changed total expense or local income", beforeTotals, afterTotals)
	}
	var rows []ChannelEconomicsHourPublication
	if err := currentEconomicsPublicationQuery(db).Select("p.*").Where(
		"p.domain=? AND p.account_epoch=? AND p.local_channel_id=? AND p.hour_ts IN ? AND p.semantics_version=?",
		account.Domain, newAPIUpstreamAccountEpoch(account), fixture.channel, hours, channelEconomicsSemanticsVersion).Order("p.hour_ts").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(hours) {
		t.Fatal("candidate must produce exactly the finite target hours")
	}
	for i, row := range rows {
		requests, charges := fixture.upstreamRequests[i], fixture.charges[i]
		localRequests, revenue := fixture.customerRequests[i], fixture.revenue[i]
		if row.HourTs != hours[i] || row.UpstreamRequests != requests || row.UpstreamChargeUnits != charges ||
			row.LocalRequests != localRequests || row.RevenueMicroUSD != revenue || !row.CorrectedCostKnown ||
			row.UpstreamCostMicroUSD != charges*2 || row.CorrectedCostMicroUSD != charges*2 {
			t.Fatal("target differs from original cost/customer-only income; no proration permitted", row)
		}
	}
	// Check the actual downstream deduction gate. Configured internal-user
	// facts are not supplied, so do not claim this is a complete finance report.
	evidence, err := m.loadFinanceInternalTestCostEvidence(ctx, stabilityScope{FromTs: from, ToTs: to}, financeConfiguredInternalEvidence{})
	if err != nil || evidence.Complete || evidence.Total.CorrectedCostMicroUSD != 0 {
		t.Fatal("mixed/unverified costs became exact test deductions", evidence, err)
	}
	var manifests []ChannelEconomicsHourManifestPublication
	if err := db.Table("channel_economics_hour_manifest_current mc").Select("mp.*").Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").Where("mc.domain=? AND mc.hour_ts IN ?", account.Domain, hours).Scan(&manifests).Error; err != nil {
		t.Fatal(err)
	}
	if len(manifests) != len(hours) {
		t.Fatal("missing current manifests")
	}
	for _, head := range manifests {
		if fixture.expectUnallocated && (head.ProfitKnown || head.CoverageStatus != "unallocated_cost") {
			t.Fatal("other unresolved sources became complete", head)
		}
		if !fixture.expectUnallocated && (!head.ProfitKnown || head.CoverageStatus != "verified_complete") {
			t.Fatal("fully matched no-test candidate failed finite publication", head)
		}
	}
	if got := attributionQueryDigests(ctx, t, db, protected); !reflect.DeepEqual(beforeProtected, got) {
		for name, hash := range beforeProtected {
			if got[name] != hash {
				t.Errorf("changed protected data: %s", name)
			}
		}
		t.Fatal("candidate changed unrelated or immutable rows")
	}
	checkAttributionLocalPublicationRetry(ctx, t, m, account, hours, time.Now().Unix()+180)
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("integrity failure", err)
	}
	if giftLocalFileHash(t, source) != digest {
		t.Fatal("input snapshot changed")
	}
	receipt := map[string]any{"mode": "unconfirmed_local_finite_candidate_rehearsal", "source_sha256": digest,
		"local_channel_id": fixture.channel, "from_ts": from, "to_ts_exclusive": to, "upstream_requests": upstreamRequests, "customer_requests": customerRequests, "test_requests": fixture.testRequests,
		"source_cost_micro": sourceCost, "customer_revenue_micro": revenue, "failed_records": fixture.failedRequests, "before_totals": beforeTotals, "after_totals": afterTotals,
		"totals_conserved": true, "old_and_outside_rows_unchanged": true, "rollback_verified": true, "retry_noop": true,
		"historical_ownership_confirmed": false, "production_applicable": false, "full_finance_report_validated": false,
		"internal_deduction_complete": evidence.Complete, "target_rows": rows}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = giftLocalWriteNew(filepath.Join(output, "acceptance.json"), encoded); err != nil {
		t.Fatal(err)
	}
	t.Logf("PASS local %d-hour candidate: conserved costs/income, no test revenue, retry/rollback and scope checks; output=%s", len(hours), output)
}
