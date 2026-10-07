package monitor

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestFinancePairingHoursSeparateActivityEmptyAndAttribution(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture economicsCoverageFixture
		want    financePairingHourView
	}{
		{"paired business", economicsCoverageFixture{true, true, true, true, true, false}, financePairingHourView{PairedActivityHours: 1}},
		{"verified empty", economicsCoverageFixture{false, false, false, true, true, false}, financePairingHourView{VerifiedEmptyHours: 1}},
		{"unallocated cost", economicsCoverageFixture{true, false, true, true, true, false}, financePairingHourView{UnallocatedCostHours: 1}},
		{"local activity upstream zero", economicsCoverageFixture{false, false, true, true, true, false}, financePairingHourView{UpstreamZeroCheckHours: 1}},
		{"missing price", economicsCoverageFixture{true, true, true, true, false, false}, financePairingHourView{OtherIncompleteHours: 1}},
		{"unverified local", economicsCoverageFixture{true, true, true, false, true, false}, financePairingHourView{OtherIncompleteHours: 1}},
		{"unallocated refund", economicsCoverageFixture{true, true, true, true, true, true}, financePairingHourView{OtherIncompleteHours: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := publishEconomicsCoverageFixture(t, tc.fixture)
			rows, err := loadFinancePairingHours(context.Background(), db, stabilityScope{FromTs: 3600, ToTs: 7200})
			tc.want.Domain, tc.want.PublishedHours = "4sapi.com", 1
			if err != nil || !reflect.DeepEqual(rows, []financePairingHourView{tc.want}) {
				t.Fatalf("hour diagnosis mismatch: got=%+v want=%+v err=%v", rows, tc.want, err)
			}
		})
	}
}

func TestFinancePairingHoursDoNotTrustStaleZeroOrBrokenManifest(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"changed evidence", "UPDATE channel_upstream_cost_hour_states SET content_hash='changed'"},
		{"unverified evidence", "UPDATE channel_upstream_cost_hour_states SET status='pending'"},
		{"mismatched evidence", "UPDATE channel_upstream_cost_hour_states SET reconcile_status='mismatch'"},
		{"nonzero control", "UPDATE channel_upstream_cost_hour_states SET control_charge_units=1"},
		{"nonzero evidence", "UPDATE channel_upstream_cost_hour_states SET evidence_charge_units=1"},
		{"free requests still activity", "UPDATE channel_upstream_cost_hour_states SET requests=1"},
		{"evidence exists", "UPDATE channel_upstream_cost_hour_states SET evidence_rows=1"},
		{"reconciliation delta", "UPDATE channel_upstream_cost_hour_states SET reconcile_delta=1"},
		{"missing state", "DELETE FROM channel_upstream_cost_hour_states"},
		{"missing child", "DELETE FROM channel_economics_hour_current"},
		{"child count mismatch", "UPDATE channel_economics_hour_manifest_publications SET row_count=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := publishEconomicsCoverageFixture(t, economicsCoverageFixture{false, false, true, true, true, false})
			if err := db.Exec(tc.sql).Error; err != nil {
				t.Fatal(err)
			}
			rows, err := loadFinancePairingHours(context.Background(), db, stabilityScope{FromTs: 3600, ToTs: 7200})
			if err != nil || len(rows) != 1 || rows[0].OtherIncompleteHours != 1 || rows[0].UpstreamZeroCheckHours != 0 {
				t.Fatalf("unproven evidence promoted: %+v err=%v", rows, err)
			}
		})
	}
}

func TestFinancePairingHoursFreeRequestsAreNotEmpty(t *testing.T) {
	db, _ := publishEconomicsCoverageFixture(t, economicsCoverageFixture{true, true, true, true, true, false})
	if err := db.Exec(`UPDATE channel_economics_hour_publications SET local_consume_quota=0,
		local_net_quota=0,upstream_charge_units=0,revenue_micro_usd=0,
		upstream_cost_micro_usd=0,corrected_cost_micro_usd=0,profit_micro_usd=0`).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := loadFinancePairingHours(context.Background(), db, stabilityScope{FromTs: 3600, ToTs: 7200})
	if err != nil || len(rows) != 1 || rows[0].PairedActivityHours != 1 || rows[0].VerifiedEmptyHours != 0 {
		t.Fatal("free activity was counted as an empty hour", rows, err)
	}
}

func TestFinancePairingFailedUnchargedRequestsDoNotInventChannelCost(t *testing.T) {
	db, _ := publishEconomicsCoverageFixture(t, economicsCoverageFixture{false, false, true, true, true, false})
	if err := db.Exec("UPDATE stability_hour_samples SET success=0,failed=10,quota=0").Error; err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain}}}
	if err := m.publishChannelEconomicsHour(context.Background(), account, 3600, "failed_local", 7200); err != nil {
		t.Fatal(err)
	}
	var row ChannelEconomicsHourPublication
	if err := db.Table("channel_economics_hour_current c").Select("p.*").
		Joins("JOIN channel_economics_hour_publications p ON p.publication_id=c.publication_id").Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	// Local failures prove no local income, not that this upstream account
	// covered the channel's historic credentials or incurred no actual cost.
	if row.RevenueMicroUSD != 0 || row.CorrectedCostKnown || row.ProfitKnown || row.CoverageStatus != "upstream_cost_missing" {
		t.Fatal("failed request was promoted to known free upstream cost", row)
	}
	rows, err := loadFinancePairingHours(context.Background(), db, stabilityScope{FromTs: 3600, ToTs: 7200})
	if err != nil || len(rows) != 1 || rows[0].UpstreamZeroCheckHours != 1 || rows[0].PairedActivityHours != 0 {
		t.Fatal("unresolved zero was counted as paired activity", rows, err)
	}
}

func TestFinancePairingHoursUseOnlyAuthoritativeHeadAndHalfOpenScope(t *testing.T) {
	db, publications := publishEconomicsCoverageFixture(t, economicsCoverageFixture{true, true, true, true, true, false})
	original := publications[0]
	stale := original
	stale.AccountEpoch, stale.PublicationID, stale.LogicalKey = "old-account", "old-pub", "old-logical"
	stale.CoverageStatus, stale.ProfitKnown = "unallocated_cost", false
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelEconomicsHourCurrent{LogicalKey: stale.LogicalKey, PublicationID: stale.PublicationID}).Error; err != nil {
		t.Fatal(err)
	}
	// Old revisions in the same epoch remain immutable but are not current.
	stale.AccountEpoch, stale.PublicationID, stale.LogicalKey = original.AccountEpoch, "old-revision", original.LogicalKey
	stale.Revision = 0
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	before := []ChannelEconomicsHourPublication{}
	if err := db.Order("publication_id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		rows, err := loadFinancePairingHours(context.Background(), db, stabilityScope{FromTs: 3600, ToTs: 7200})
		if err != nil || len(rows) != 1 || rows[0].PublishedHours != 1 || rows[0].PairedActivityHours != 1 || rows[0].UnallocatedCostHours != 0 {
			t.Fatal("stale epoch/revision inflated progress", rows, err)
		}
	}
	for _, scope := range []stabilityScope{{FromTs: 0, ToTs: 3600}, {FromTs: 7200, ToTs: 10800}} {
		if rows, err := loadFinancePairingHours(context.Background(), db, scope); err != nil || len(rows) != 0 {
			t.Fatal("query crossed scope boundary", rows, err)
		}
	}
	after := []ChannelEconomicsHourPublication{}
	if err := db.Order("publication_id").Find(&after).Error; err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("diagnostic changed financial facts", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loadFinancePairingHours(ctx, db, stabilityScope{FromTs: 3600, ToTs: 7200}); err == nil {
		t.Fatal("cancellation ignored")
	}
	var plan []struct{ Detail string }
	if err := db.Raw("EXPLAIN QUERY PLAN "+financePairingHoursSQL, channelEconomicsSemanticsVersion, 3600, 7200).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	indexed := false
	for _, step := range plan {
		indexed = indexed || strings.Contains(step.Detail, "SEARCH p ") && strings.Contains(step.Detail, "hour_ts=?")
	}
	if !indexed {
		t.Fatalf("lost bounded hour lookup: %+v", plan)
	}
}

// Opt-in against the sealed, unconfirmed local rehearsal, never a live DB.
func TestFinancePairingHoursLocalSnapshot(t *testing.T) {
	path := os.Getenv("MONITOR_PAIRING_HOURS_ACCEPTANCE_DB")
	if path == "" {
		t.Skip("requires sealed local recharge rehearsal")
	}
	backup, err := openGiftRelevantBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.close()
	ctx := context.Background()
	before, err := financeRechargeBackupHash(ctx, backup)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		rows, err := loadFinancePairingHours(ctx, backup.db, stabilityScope{FromTs: 1785402000, ToTs: 1786499634})
		want := financePairingHourView{Domain: "4sapi.com", PublishedHours: 305, VerifiedEmptyHours: 142,
			UnallocatedCostHours: 156, UpstreamZeroCheckHours: 7}
		if err != nil || !reflect.DeepEqual(rows, []financePairingHourView{want}) {
			t.Fatalf("real snapshot diagnosis mismatch: %+v err=%v", rows, err)
		}
	}
	after, err := financeRechargeBackupHash(ctx, backup)
	if err != nil || before != after {
		t.Fatal("read-only diagnosis changed the source", err)
	}
}
