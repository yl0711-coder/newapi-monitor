package monitor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func financeRetargetFixture(t *testing.T) (*Monitor, int64) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "old.example")
	hour, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{ID: 59, BaseDomain: "old.example", UpdatedAt: hour + 3600 + 10}}, hour+3600+10); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		h := hour + int64(i)*3600
		for _, row := range []any{
			&StabilityHourSample{HourTs: h, ChannelID: 59, Grp: "business", ModelName: "m", Success: 3, Quota: 1_500_000, RefundQuota: 250_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
			&StabilityHourIngestState{HourTs: h, Status: "complete", Requests: 3, TrafficClassVersion: stabilityTrafficClassificationVersion},
		} {
			if err := m.storeDB.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	return m, hour
}

func TestFinanceChannelRetargetPreservesHistoryAndInternalExclusion(t *testing.T) {
	m, hour := financeRetargetFixture(t)
	ctx := context.Background()
	scope := stabilityScope{FromTs: hour, ToTs: hour + 10800}
	internal := financeConfiguredInternalEvidence{Complete: true, NetQuota: 1_000_000, Requests: 2, Rows: []FinanceInternalAccountHourFact{
		{HourTs: hour, ChannelID: 59, Grp: "business", Requests: 1, ConsumeQuota: 500_000},
		{HourTs: hour + 7200, ChannelID: 59, Grp: "business", Requests: 1, ConsumeQuota: 500_000},
	}}
	before, _, _, err := m.loadFinanceUserFacts(ctx, scope, scope.ToTs+7200, internal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{ID: 59, BaseDomain: " NEW.EXAMPLE ", UpdatedAt: hour + 3600 + 20}}, hour+3600+20); err != nil {
		t.Fatal(err)
	}
	after, coverage, domains, err := m.loadFinanceUserFacts(ctx, scope, scope.ToTs+7200, internal, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || before.KnownUserConsumption != after.KnownUserConsumption || after.KnownUserConsumption.MicroUSD != "5500000" {
		t.Fatalf("retarget lost whole-site money: before=%+v after=%+v", before, after)
	}
	for domain, want := range map[string]financeDomainUserFact{
		"old.example":             {Requests: 2, ConsumeQuota: 1_000_000, RefundQuota: 250_000},
		"new.example":             {Requests: 2, ConsumeQuota: 1_000_000, RefundQuota: 250_000},
		financeUnattributedDomain: {Requests: 3, ConsumeQuota: 1_500_000, RefundQuota: 250_000},
	} {
		got := domains[domain]
		if got.Requests != want.Requests || got.ConsumeQuota != want.ConsumeQuota || got.RefundQuota != want.RefundQuota {
			t.Fatalf("wrong historical attribution for %s: %+v", domain, got)
		}
	}
	// A closed interval before the change keeps its fast aggregate and owner.
	_, _, historical, err := m.loadFinanceUserFacts(ctx, stabilityScope{FromTs: hour, ToTs: hour + 3600}, scope.ToTs+7200,
		financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil || len(historical) != 1 || historical["old.example"].ConsumeQuota != 1_500_000 {
		t.Fatalf("old hour moved to current supplier: %+v %v", historical, err)
	}
	starts, err := m.loadFinanceUpstreamActivityStarts(ctx, scope.ToTs)
	if err != nil || !reflect.DeepEqual(starts, map[string]int64{"old.example": hour, financeUnattributedDomain: hour + 3600, "new.example": hour + 7200}) {
		t.Fatalf("coverage lifecycle follows current supplier instead of history: %v %v", starts, err)
	}
}

func TestFinanceChannelDomainSurvivesDisableDeleteRestoreAndRepeatedRetarget(t *testing.T) {
	m, hour := financeRetargetFixture(t)
	refresh := func(domain string, at int64, status int) {
		t.Helper()
		if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{ID: 59, Name: "renamed", BaseDomain: domain, Status: status, UpdatedAt: at}}, at); err != nil {
			t.Fatal(err)
		}
	}
	refresh(" OLD.EXAMPLE ", hour+3600+15, 2)
	if err := m.replaceChannelSnapsAuthoritative(nil, hour+3600+16); err != nil {
		t.Fatal(err)
	}
	refresh("old.example", hour+3600+17, 1)
	var count int64
	if err := m.storeDB.Model(&FinanceChannelDomainPeriod{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("non-ownership lifecycle changes created accounting boundaries")
	}
	refresh("middle.example", hour+3600+20, 1)
	refresh("new.example", hour+3600+20, 1) // Same-second changes still have one final owner.
	domains, err := loadFinanceChannelDomains(context.Background(), m.storeDB)
	if err != nil || domains.at(59, hour) != "old.example" || domains.at(59, hour+3600) != financeUnattributedDomain || domains.at(59, hour+7200) != "new.example" {
		t.Fatalf("repeated retarget left a false intermediate owner: %+v %v", domains, err)
	}
	if err := m.storeDB.Model(&FinanceChannelDomainPeriod{}).Count(&count).Error; err != nil || count != 3 {
		t.Fatalf("normal refresh grows the history unnecessarily: %d %v", count, err)
	}
}

func TestFinanceChannelDomainTransactionRollbackAndCancellation(t *testing.T) {
	m, hour := financeRetargetFixture(t)
	if err := m.storeDB.Exec(`CREATE TRIGGER reject_channel_update BEFORE UPDATE ON channel_snaps BEGIN SELECT RAISE(ABORT,'test snapshot failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{ID: 59, BaseDomain: "new.example", UpdatedAt: hour + 3600 + 20}}, hour+3600+20); err == nil {
		t.Fatal("injected snapshot failure did not abort")
	}
	domains, err := loadFinanceChannelDomains(context.Background(), m.storeDB)
	if err != nil || len(domains.periods[59]) != 1 || domains.at(59, hour+7200) != "old.example" || domains.current[59] != "old.example" {
		t.Fatal("snapshot rollback left independent accounting history behind")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loadFinanceChannelDomains(ctx, m.storeDB); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled attribution lookup silently fell back: %v", err)
	}
}

func TestFinanceChannelDomainFingerprintFollowsHistoricalScope(t *testing.T) {
	m, hour := financeRetargetFixture(t)
	ctx := context.Background()
	fingerprint := func(from, to int64, full bool) string {
		t.Helper()
		v, err := m.financeReportSourceFingerprintForScope(ctx, from, to, full)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	closed := fingerprint(hour, hour+3600, false)
	mixed := fingerprint(hour, hour+10800, false)
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{ID: 59, BaseDomain: "new.example", UpdatedAt: hour + 3600 + 20}}, hour+3600+20); err != nil {
		t.Fatal(err)
	}
	if closed != fingerprint(hour, hour+3600, false) || mixed == fingerprint(hour, hour+10800, false) {
		t.Fatal("historical cache scope ignored ownership or invalidated unrelated old hours")
	}
	for _, full := range []bool{false, true} {
		before := fingerprint(hour, hour+3600, full)
		if err := m.storeDB.Model(&FinanceChannelDomainPeriod{}).Where("channel_id=? AND from_hour_ts=0", 59).Update("domain", "corrected.example").Error; err != nil {
			t.Fatal(err)
		}
		if before == fingerprint(hour, hour+3600, full) {
			t.Fatalf("historical attribution correction left stale report, full=%v", full)
		}
		if err := m.storeDB.Model(&FinanceChannelDomainPeriod{}).Where("channel_id=? AND from_hour_ts=0", 59).Update("domain", "old.example").Error; err != nil {
			t.Fatal(err)
		}
	}
}

func TestFinanceChannelDomainSQLAndIndexedActivityMatchReader(t *testing.T) {
	m, hour := financeRetargetFixture(t)
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{{ID: 59, BaseDomain: "new.example", UpdatedAt: hour + 3600 + 20}}, hour+3600+20); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	domains, err := loadFinanceChannelDomains(ctx, m.storeDB)
	if err != nil {
		t.Fatal(err)
	}
	expr, err := financeChannelDomainSQL(m.storeDB, "c", "s.hour_ts")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		HourTs int64
		Domain string
	}
	if err := m.storeDB.Raw("SELECT s.hour_ts," + expr + " domain FROM stability_hour_samples s JOIN channel_snaps c ON c.id=s.channel_id ORDER BY s.hour_ts").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Domain != domains.at(59, row.HourTs) {
			t.Fatalf("publisher and report disagree: %+v", row)
		}
	}
	var plan []struct{ Detail string }
	if err := m.storeDB.Raw("EXPLAIN QUERY PLAN "+financeHistoricalUpstreamActivityQuery, financeUpstreamActivityArgs(hour+10800)...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	var steps []string
	for _, step := range plan {
		steps = append(steps, step.Detail)
	}
	for _, index := range []string{"SEARCH s USING INDEX idx_stability_channel_hour", "SEARCH t USING INDEX idx_channel_test_channel_hour"} {
		if !strings.Contains(strings.Join(steps, "\n"), index) {
			t.Fatalf("historical activity lost indexed seek: %v", steps)
		}
	}
	// A pre-upgrade closed snapshot remains readable without migration/writes.
	if err := m.storeDB.Migrator().DropTable(&FinanceChannelDomainPeriod{}); err != nil {
		t.Fatal(err)
	}
	domains, err = loadFinanceChannelDomains(ctx, m.storeDB)
	if err != nil || domains.at(59, hour) != "new.example" {
		t.Fatal("legacy snapshot cannot be read")
	}
}

func TestFinanceChannelDomainWriterRejectsInvalidTimeAtomically(t *testing.T) {
	m, _ := financeRetargetFixture(t)
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return recordFinanceChannelDomains(tx, []ChannelSnap{{ID: 59, BaseDomain: "new.example"}}, 0)
	})
	if err == nil {
		t.Fatal("invalid observation timestamp became a historical fact")
	}
	domains, err := loadFinanceChannelDomains(context.Background(), m.storeDB)
	if err != nil || len(domains.periods[59]) != 1 {
		t.Fatal("invalid transition mutated the baseline")
	}
}

func TestFinanceChannelHistoricalPublicationAndRepairKeepOriginalSupplier(t *testing.T) {
	db, publications := publishEconomicsCoverageFixture(t, economicsCoverageFixture{
		withCost: true, allocatedCost: true, withLocal: true, localVerified: true, withFinance: true,
	})
	if err := db.AutoMigrate(&FinanceChannelDomainPeriod{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain, "new.example"}}}
	ctx := context.Background()
	for _, snap := range []ChannelSnap{
		{ID: 59, BaseDomain: account.Domain, UpdatedAt: 7205},
		{ID: 59, BaseDomain: "new.example", UpdatedAt: 7250},
	} {
		if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{snap}, snap.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.publishChannelEconomicsHour(ctx, account, 3600, "retarget", 10800); err != nil {
		t.Fatal(err)
	}
	var current []ChannelEconomicsHourPublication
	if err := db.Find(&current).Error; err != nil || !reflect.DeepEqual(current, publications) {
		t.Fatal("current directory retarget rewrote an unchanged historical publication")
	}
	if err := db.Where("1=1").Delete(&ChannelEconomicsDirtyHour{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return m.enqueueChangedEconomicsLocalHoursTx(tx, 0, 10800, 10) }); err != nil {
		t.Fatal(err)
	}
	var dirty []ChannelEconomicsDirtyHour
	if err := db.Find(&dirty).Error; err != nil || len(dirty) != 0 {
		t.Fatal("retarget created a false repair of unchanged historical revenue")
	}
	if err := db.Model(&StabilityHourSample{}).Where("channel_id=59 AND hour_ts=3600").Update("quota", 2_000_000).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return m.enqueueChangedEconomicsLocalHoursTx(tx, 0, 10800, 10) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Find(&dirty).Error; err != nil || len(dirty) != 1 || dirty[0].Domain != account.Domain || dirty[0].HourTs != 3600 {
		t.Fatalf("historical correction enqueued the wrong supplier: %+v %v", dirty, err)
	}
	if err := m.publishChannelEconomicsHour(ctx, account, 3600, "late_fact", 10801); err != nil {
		t.Fatal(err)
	}
	if err := db.Order("revision DESC").First(&current).Error; err != nil || len(current) != 1 || current[0].RevenueMicroUSD != 4_000_000 || current[0].Domain != account.Domain {
		t.Fatalf("late correction lost original supplier revenue: %+v %v", current, err)
	}
}
