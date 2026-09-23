package monitor

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Opt-in release acceptance against a copied, closed production snapshot.
// Both SQLite handles are immutable/read-only; this test never connects to
// NewAPI, an upstream provider, or the running Monitor instance.
func TestFinanceLocalSnapshotReadOnlyParity(t *testing.T) {
	mainPath := os.Getenv("MONITOR_FINANCE_ACCEPTANCE_MAIN_SNAPSHOT")
	factsPath := os.Getenv("MONITOR_FINANCE_ACCEPTANCE_FACTS_SNAPSHOT")
	baselinePath := os.Getenv("MONITOR_FINANCE_ACCEPTANCE_BASELINE_JSON")
	if mainPath == "" || factsPath == "" || baselinePath == "" {
		t.Skip("requires closed local SQLite snapshots and an earlier report JSON")
	}
	openReadOnly := func(path string) *gorm.DB {
		t.Helper()
		path, err := filepath.Abs(path)
		if err != nil {
			t.Fatal(err)
		}
		uri := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&immutable=1"}).String()
		db, err := gorm.Open(sqlite.Open(uri), &gorm.Config{})
		if err != nil {
			t.Fatalf("open immutable %s: %v", filepath.Base(path), err)
		}
		conn, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return db
	}
	baselineBytes, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	var prior struct {
		Finance  financeOperatingReport  `json:"finance"`
		Channels ChannelManagementReport `json:"channels"`
	}
	if err := json.Unmarshal(baselineBytes, &prior); err != nil {
		t.Fatal(err)
	}
	if prior.Finance.From == 0 || prior.Finance.To <= prior.Finance.From {
		t.Fatal("baseline report has no closed time range")
	}
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: openReadOnly(mainPath), usageFactsDB: openReadOnly(factsPath), cfg: Settings{
		FinanceEnabled: true, FinanceStartDate: "2026-05-01", ChannelEconomicsReportEnabled: true,
		UsageFactsReadEnabled: true, UsageFactsHistorySourceMode: "complete",
		UsageFactsHistorySourceEpoch: "newapi-hotlogs-complete-20260817-v1",
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	got, err := m.buildFinanceOperatingReport(ctx, time.Unix(prior.Finance.From, 0).In(loc), time.Unix(prior.Finance.To, 0).In(loc))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Statement, prior.Finance.Statement) || len(got.Days) != len(prior.Finance.Days) ||
		len(got.Periods) != len(prior.Finance.Periods) {
		t.Fatalf("statement/period shape changed on immutable snapshot: days=%d/%d periods=%d/%d",
			len(got.Days), len(prior.Finance.Days), len(got.Periods), len(prior.Finance.Periods))
	}
	for i := range got.Days {
		if got.Days[i].Date != prior.Finance.Days[i].Date || !reflect.DeepEqual(got.Days[i].Statement, prior.Finance.Days[i].Statement) {
			t.Fatalf("daily money/status changed on immutable snapshot: date=%s", got.Days[i].Date)
		}
	}
	for i := range got.Periods {
		if !reflect.DeepEqual(got.Periods[i].Statement, prior.Finance.Periods[i].Statement) {
			t.Fatalf("monthly money/status changed on immutable snapshot: index=%d", i)
		}
	}
	if !reflect.DeepEqual(got.UpstreamCoverage, prior.Finance.UpstreamCoverage) ||
		!reflect.DeepEqual(got.GiftCoverage, prior.Finance.GiftCoverage) {
		t.Fatal("evidence coverage changed on immutable snapshot")
	}
	channelScope := stabilityScope{FromTs: prior.Channels.Meta.FromTs, ToTs: prior.Channels.Meta.ToTs}
	channels, err := m.buildChannelManagementReport(ctx, channelScope, prior.Channels.Meta.GeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(channels.Summary.Usage, prior.Channels.Summary.Usage) || len(channels.Domains) != len(prior.Channels.Domains) {
		t.Fatalf("channel usage summary changed on immutable snapshot: domains=%d/%d", len(channels.Domains), len(prior.Channels.Domains))
	}
	priorDomains := make(map[string]ChannelManagementDomain, len(prior.Channels.Domains))
	for _, domain := range prior.Channels.Domains {
		priorDomains[domain.Key] = domain
	}
	for _, domain := range channels.Domains {
		old, ok := priorDomains[domain.Key]
		if !ok || !reflect.DeepEqual(domain.Usage, old.Usage) || !reflect.DeepEqual(domain.UpstreamUsage, old.UpstreamUsage) {
			t.Fatalf("channel consumption changed on immutable snapshot: domain=%s", domain.Key)
		}
	}
}
