package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"testing"
)

func economicsReportTestMonitor(t *testing.T, domains ...string) *Monitor {
	t.Helper()
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamAccount{}, &ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{
		ChannelCostClosureEnabled: true, ChannelEconomicsReportEnabled: true,
		ChannelCostClosureDomains: domains,
	}}
	for i, domain := range domains {
		if err := db.Create(&ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: "https://" + domain, UserID: int64(i + 1), BalanceUnit: quotaPerUSD}).Error; err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func insertEconomicsReportHour(t *testing.T, m *Monitor, domain, epoch string, hour int64, channelID int, revenue, rawCost, corrected, profit int64) ChannelEconomicsHourPublication {
	t.Helper()
	logical := economicsLogicalKey(domain, epoch, hour, channelID)
	publication := ChannelEconomicsHourPublication{
		PublicationID: strings.Repeat(string(rune('a'+channelID%20)), 63) + "1", LogicalKey: logical, Revision: 1,
		Domain: domain, AccountEpoch: epoch, HourTs: hour, LocalChannelID: channelID,
		SemanticsVersion: channelEconomicsSemanticsVersion, RevenueMicroUSD: revenue,
		UpstreamCostMicroUSD: rawCost, CorrectedCostMicroUSD: corrected, ProfitMicroUSD: profit,
		CorrectedCostKnown: true, ProfitKnown: true, CoverageStatus: "verified_complete",
	}
	if err := m.storeDB.Create(&publication).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelEconomicsHourCurrent{LogicalKey: logical, PublicationID: publication.PublicationID, Revision: 1}).Error; err != nil {
		t.Fatal(err)
	}
	return publication
}

func insertEconomicsReportManifest(t *testing.T, m *Monitor, domain, epoch string, hour int64, publications ...ChannelEconomicsHourPublication) {
	t.Helper()
	ids := make([]string, 0, len(publications))
	for _, publication := range publications {
		ids = append(ids, publication.PublicationID)
	}
	// The production manifest hashes the canonical publication-id set, not
	// caller order. Keep the fixture contract identical so a multi-row hour
	// does not accidentally simulate manifest corruption.
	sort.Strings(ids)
	digest := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	logical := economicsManifestLogicalKey(domain, hour)
	manifest := ChannelEconomicsHourManifestPublication{
		ManifestID: strings.Repeat("f", 56) + hex.EncodeToString(digest[:4]), LogicalKey: logical, Revision: 1,
		Domain: domain, HourTs: hour, SemanticsVersion: channelEconomicsSemanticsVersion,
		AuthoritativeEpoch: epoch, RowCount: int64(len(publications)), PublicationSetHash: hex.EncodeToString(digest[:]),
		CoverageStatus: "verified_complete", ProfitKnown: true, SourceHash: strings.Repeat("e", 64),
	}
	if err := m.storeDB.Create(&manifest).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelEconomicsHourManifestCurrent{Domain: domain, HourTs: hour, SemanticsVersion: channelEconomicsSemanticsVersion, ManifestID: manifest.ManifestID, Revision: 1}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestChannelEconomicsReportUsesManifestAuthoritativeEpoch(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com")
	hour := int64(3600)
	oldRow := insertEconomicsReportHour(t, m, "4sapi.com", strings.Repeat("1", 64), hour, 59, 9_000_000, 3_000_000, 3_000_000, 6_000_000)
	_ = oldRow
	newRow := insertEconomicsReportHour(t, m, "4sapi.com", strings.Repeat("2", 64), hour, 60, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", strings.Repeat("2", 64), hour, newRow)
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Totals.ProfitKnown || !report.Totals.RevenueKnown || report.Totals.Revenue == nil || report.Totals.Revenue.MicroUSD != "2000000" || report.Totals.Profit == nil || report.Totals.Profit.MicroUSD != "1000000" {
		t.Fatalf("manifest authority was not respected: %+v", report.Totals)
	}
	if len(report.Domains) != 1 || len(report.Domains[0].Channels) != 1 || report.Domains[0].Channels[0].ChannelID != 60 {
		t.Fatalf("stale epoch leaked into report: %+v", report.Domains)
	}
}

func TestChannelEconomicsReportManifestProvesZeroHour(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com")
	hour := int64(7200)
	insertEconomicsReportManifest(t, m, "4sapi.com", strings.Repeat("3", 64), hour)
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, "4sapi.com")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Coverage.Complete || !report.Totals.ProfitKnown || report.Totals.Profit == nil || report.Totals.Profit.MicroUSD != "0" || len(report.Domains[0].Hourly) != 1 {
		t.Fatalf("verified zero hour was treated as missing: %+v", report)
	}
}

func TestChannelEconomicsReportFailsClosedOnMissingHour(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com")
	hour := int64(10_800)
	row := insertEconomicsReportHour(t, m, "4sapi.com", strings.Repeat("4", 64), hour, 59, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", strings.Repeat("4", 64), hour, row)
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 7200}, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage.MissingHours != 1 || report.Totals.RevenueKnown || report.Totals.Revenue != nil || report.Totals.KnownRevenue.MicroUSD != "2000000" || report.Totals.ProfitKnown || report.Totals.Profit != nil || report.Totals.UnknownReason != "publication_missing" {
		t.Fatalf("missing hour was exposed as exact profit: %+v %+v", report.Coverage, report.Totals)
	}
	if len(report.Domains) != 1 || len(report.Domains[0].Channels) != 1 || report.Domains[0].Channels[0].Totals.ProfitKnown || report.Domains[0].Channels[0].Totals.Profit != nil || report.Domains[0].Channels[0].Coverage.MissingHours != 1 {
		t.Fatalf("missing domain hour leaked exact per-channel profit: %+v", report.Domains)
	}
}

func TestChannelEconomicsReportCountsGlobalRefundOnce(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com", "codeyu.shop")
	hour := int64(14_400)
	for i, domain := range []string{"4sapi.com", "codeyu.shop"} {
		epoch := strings.Repeat(string(rune('5'+i)), 64)
		row := insertEconomicsReportHour(t, m, domain, epoch, hour, 59+i, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
		insertEconomicsReportManifest(t, m, domain, epoch, hour, row)
	}
	if err := m.storeDB.Create(&ChannelEconomicsGlobalHourFact{HourTs: hour, SemanticsVersion: channelEconomicsSemanticsVersion, UnallocatedRefundRecords: 1, UnallocatedRefundQuota: quotaPerUSD}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.GlobalRefund.Amount.MicroUSD != "1000000" || !report.Totals.RevenueKnown || report.Totals.Revenue == nil || report.Totals.Revenue.MicroUSD != "3000000" || report.Totals.ProfitKnown || report.Totals.Profit != nil {
		t.Fatalf("global refund was duplicated or profit did not fail closed: refund=%+v totals=%+v", report.GlobalRefund, report.Totals)
	}
}

func TestChannelEconomicsDomainReportDoesNotAssignGlobalRefund(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com", "codeyu.shop")
	hour := int64(18_000)
	for i, domain := range []string{"4sapi.com", "codeyu.shop"} {
		epoch := strings.Repeat(string(rune('7'+i)), 64)
		row := insertEconomicsReportHour(t, m, domain, epoch, hour, 70+i, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
		insertEconomicsReportManifest(t, m, domain, epoch, hour, row)
	}
	if err := m.storeDB.Create(&ChannelEconomicsGlobalHourFact{HourTs: hour, SemanticsVersion: channelEconomicsSemanticsVersion, UnallocatedRefundRecords: 1, UnallocatedRefundQuota: quotaPerUSD}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, "4sapi.com")
	if err != nil {
		t.Fatal(err)
	}
	// 全局退款无法分摊给某一域名：单域名只能返回“已知部分”，
	// 区间净收入和利润均必须不可判定，不能把整笔退款错扣到它头上。
	if report.Totals.RevenueKnown || report.Totals.Revenue != nil || report.Totals.KnownRevenue.MicroUSD != "2000000" || report.Totals.ProfitKnown || report.Totals.UnknownReason != "refund_unallocated" {
		t.Fatalf("global refund was incorrectly allocated to one domain: %+v", report.Totals)
	}
}

func TestChannelEconomicsReportAllMissingDoesNotPretendZero(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com")
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: 21_600, ToTs: 25_200}, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.RevenueKnown || report.Totals.Revenue != nil || report.Totals.UpstreamCostKnown || report.Totals.UpstreamCost != nil || report.Totals.ProfitKnown || report.Totals.Profit != nil {
		t.Fatalf("missing range was presented as exact zero: %+v", report.Totals)
	}
	if report.Totals.KnownRevenue.MicroUSD != "0" || report.Totals.KnownUpstreamCost.MicroUSD != "0" || report.Coverage.MissingHours != 1 {
		t.Fatalf("missing range known-part contract is wrong: totals=%+v coverage=%+v", report.Totals, report.Coverage)
	}
}

func TestChannelEconomicsReportManifestMismatchFailsClosedAtEveryLevel(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com")
	hour := int64(28_800)
	epoch := strings.Repeat("9", 64)
	row := insertEconomicsReportHour(t, m, "4sapi.com", epoch, hour, 59, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, hour, row)
	if err := m.storeDB.Model(&ChannelEconomicsHourManifestPublication{}).Where("domain = ? AND hour_ts = ?", "4sapi.com", hour).Update("row_count", 2).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, "")
	if err != nil {
		t.Fatal(err)
	}
	assertClosed := func(label string, totals channelEconomicsTotalsView) {
		t.Helper()
		if totals.RevenueKnown || totals.Revenue != nil || totals.UpstreamCostKnown || totals.UpstreamCost != nil || totals.CorrectedCostKnown || totals.CorrectedCost != nil || totals.ProfitKnown || totals.Profit != nil {
			t.Fatalf("%s leaked exact money after manifest corruption: %+v", label, totals)
		}
	}
	assertClosed("site", report.Totals)
	assertClosed("domain", report.Domains[0].Totals)
	assertClosed("channel", report.Domains[0].Channels[0].Totals)
}

func TestChannelEconomicsReportFinanceVersionConflictHidesCorrectedCost(t *testing.T) {
	m := economicsReportTestMonitor(t, "4sapi.com")
	hour := int64(32_400)
	epoch := strings.Repeat("a", 64)
	first := insertEconomicsReportHour(t, m, "4sapi.com", epoch, hour, 59, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
	second := insertEconomicsReportHour(t, m, "4sapi.com", epoch, hour, 60, 3_000_000, 2_000_000, 2_000_000, 1_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, hour, first, second)
	if err := m.storeDB.Model(&ChannelEconomicsHourManifestPublication{}).Where("domain = ? AND hour_ts = ?", "4sapi.com", hour).
		Updates(map[string]any{"coverage_status": "finance_version_conflict", "profit_known": false}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, "")
	if err != nil {
		t.Fatal(err)
	}
	for label, totals := range map[string]channelEconomicsTotalsView{
		"site": report.Totals, "domain": report.Domains[0].Totals,
		"channel-59": report.Domains[0].Channels[0].Totals, "channel-60": report.Domains[0].Channels[1].Totals,
	} {
		if !totals.RevenueKnown || totals.Revenue == nil || !totals.UpstreamCostKnown || totals.UpstreamCost == nil {
			t.Fatalf("%s hid unaffected raw money: %+v", label, totals)
		}
		if totals.CorrectedCostKnown || totals.CorrectedCost != nil || totals.ProfitKnown || totals.Profit != nil {
			t.Fatalf("%s leaked corrected money across finance version conflict: %+v", label, totals)
		}
	}
}
