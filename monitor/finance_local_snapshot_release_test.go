package monitor

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Counts statements without logging SQL text or private accounting values.
type financeAcceptanceSQLCounter struct {
	logger.Interface
	queries       atomic.Int64
	ledgerScans   atomic.Int64
	billScans     atomic.Int64
	activityScans atomic.Int64
	rechargeReads atomic.Int64
	elapsedNanos  atomic.Int64
	activityNanos atomic.Int64
	billNanos     atomic.Int64
}

func (c *financeAcceptanceSQLCounter) Trace(_ context.Context, started time.Time, query func() (string, int64), _ error) {
	c.queries.Add(1)
	elapsed := time.Since(started).Nanoseconds()
	c.elapsedNanos.Add(elapsed)
	sql, _ := query()
	if strings.Contains(sql, "SELECT p.publication_id,p.domain,p.account_epoch,p.hour_ts") {
		c.ledgerScans.Add(1)
	}
	if strings.Contains(sql, "ORDER BY domain ASC,hour_ts ASC") {
		c.billScans.Add(1)
		c.billNanos.Add(elapsed)
	}
	if strings.Contains(sql, "SELECT domain, MIN(hour_ts) first_ts") {
		c.activityScans.Add(1)
		c.activityNanos.Add(elapsed)
	}
	if strings.Contains(sql, "FROM `channel_finance_versions`") && strings.Contains(sql, "domain IN") {
		c.rechargeReads.Add(1)
	}
}

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

// Opt-in cold-build diagnostic. It uses only immutable local copies and keeps
// the timing separate from source collection or production request latency.
func TestFinanceLocalSnapshotColdBuild(t *testing.T) {
	mainPath := os.Getenv("MONITOR_FINANCE_ACCEPTANCE_MAIN_SNAPSHOT")
	factsPath := os.Getenv("MONITOR_FINANCE_ACCEPTANCE_FACTS_SNAPSHOT")
	if mainPath == "" || factsPath == "" {
		t.Skip("requires closed local SQLite snapshots")
	}
	counter := &financeAcceptanceSQLCounter{Interface: logger.Default.LogMode(logger.Silent)}
	openReadOnly := func(path string) *gorm.DB {
		t.Helper()
		path, err := filepath.Abs(path)
		if err != nil {
			t.Fatal(err)
		}
		uri := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&immutable=1"}).String()
		if info, statErr := os.Stat(path + "-wal"); statErr == nil && info.Size() > 0 {
			t.Fatal("requires a closed snapshot without nonempty WAL")
		}
		db, err := gorm.Open(sqlite.Open(uri), &gorm.Config{Logger: counter})
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return db
	}
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	from, err := time.ParseInLocation("2006-01-02", "2026-05-01", loc)
	if err != nil {
		t.Fatal(err)
	}
	// A closed range within this snapshot; it never contacts the live source.
	to, err := time.ParseInLocation("2006-01-02 15:04", "2026-09-20 16:00", loc)
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
	fingerprintStarted := time.Now()
	fingerprint, err := m.financeReportSourceFingerprint(ctx, from.Unix(), to.Unix())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("local immutable finance source fingerprint: %s", time.Since(fingerprintStarted))
	started := time.Now()
	counter.queries.Store(0)
	counter.ledgerScans.Store(0)
	counter.billScans.Store(0)
	counter.activityScans.Store(0)
	counter.rechargeReads.Store(0)
	counter.elapsedNanos.Store(0)
	counter.activityNanos.Store(0)
	counter.billNanos.Store(0)
	report, err := m.buildFinanceOperatingReport(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("local immutable finance cold build: %s; days=%d months=%d gift_unknown=%d", time.Since(started), len(report.Days), len(report.Periods), report.GiftCoverage.ScopeUnknownEvents)
	t.Logf("cold build queries=%d ledger_scans=%d", counter.queries.Load(), counter.ledgerScans.Load())
	t.Logf("cold build bill_scans=%d activity_checks=%d recharge_reads=%d", counter.billScans.Load(), counter.activityScans.Load(), counter.rechargeReads.Load())
	t.Logf("cold build SQL trace elapsed=%s activity_elapsed=%s bill_open_elapsed=%s (Scan includes decoding; Rows excludes later cursor consumption)",
		time.Duration(counter.elapsedNanos.Load()), time.Duration(counter.activityNanos.Load()), time.Duration(counter.billNanos.Load()))
	if path := os.Getenv("MONITOR_FINANCE_COLD_BASELINE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var before financeOperatingReport
		if err := json.Unmarshal(raw, &before); err != nil {
			t.Fatal(err)
		}
		// Generation wall time is not a financial change. Compare all public
		// fields, including missing amounts, costs and coverage, after JSON round-trip.
		before.GeneratedAt = report.GeneratedAt
		afterRaw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		var after financeOperatingReport
		if err := json.Unmarshal(afterRaw, &after); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatal("cold report differs from prior revision")
		}
	}
	if path := os.Getenv("MONITOR_FINANCE_COLD_BASELINE_OUTPUT"); path != "" {
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.Write(raw)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("save local baseline: %v %v", writeErr, closeErr)
		}
	}
	// The only writes in this opt-in test go to a new temporary snapshot cache,
	// never to the immutable source databases. Compare the complete real-sized
	// payload byte-for-byte after taking the fast display path.
	payload, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	m.cfg.StorePath = filepath.Join(t.TempDir(), "cache-only.db")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	request := financeReportRequest{from: from, to: to, configurationHash: "immutable-acceptance"}
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), fingerprint, payload, time.Now()); err != nil {
		t.Fatal(err)
	}
	fastStarted := time.Now()
	got, status, ok := m.financeFastSnapshotPayload(request, time.Now())
	if !ok || status != "fast-snapshot-stale" || !reflect.DeepEqual(got, payload) {
		t.Fatalf("fast snapshot changed report or lost its visible stale status: ok=%t status=%q", ok, status)
	}
	t.Logf("local immutable finance fast read: %s; payload_bytes=%d", time.Since(fastStarted), len(payload))
}
