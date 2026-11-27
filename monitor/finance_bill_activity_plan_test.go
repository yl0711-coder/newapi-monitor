package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const originalFinanceBillActivity = `SELECT LOWER(TRIM(domain)) domain, MIN(hour_ts) hour_ts
	FROM channel_upstream_usage_hours
	WHERE hour_ts<? AND (requests<>0 OR tokens<>0 OR quota<>0 OR cost_usd<>0)
	GROUP BY LOWER(TRIM(domain))`

// Candidates only until result and cost comparisons support adoption. Both
// leave the outer normalized-domain MIN authoritative for alias collisions.
const groupedFinanceBillActivityCandidate = `SELECT LOWER(TRIM(b.domain)) domain, MIN(b.hour_ts) hour_ts
	FROM channel_upstream_usage_hours b
	WHERE b.hour_ts<? AND (b.requests<>0 OR b.tokens<>0 OR b.quota<>0 OR b.cost_usd<>0)
	GROUP BY b.domain`

const seekingFinanceBillActivityCandidate = `SELECT LOWER(TRIM(d.domain)) domain,
	(SELECT b.hour_ts FROM channel_upstream_usage_hours b
	 WHERE b.domain IS d.domain AND b.hour_ts<?
	 AND (b.requests<>0 OR b.tokens<>0 OR b.quota<>0 OR b.cost_usd<>0)
	 ORDER BY b.hour_ts LIMIT 1) hour_ts
	FROM (SELECT DISTINCT domain FROM channel_upstream_usage_hours) d`

func financeActivityVariant(t *testing.T, query, fragment string) string {
	t.Helper()
	if strings.Count(query, originalFinanceBillActivity) != 1 {
		t.Fatal("activity query baseline changed")
	}
	return strings.Replace(query, originalFinanceBillActivity, fragment, 1)
}

func newFinanceActivityPlanDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, e := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		t.Fatal(e)
	}
	pool, e := db.DB()
	if e != nil {
		t.Fatal(e)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = pool.Close() })
	if e = db.AutoMigrate(&ChannelSnap{}, &StabilityHourSample{}, &ChannelTestHourSample{}, &ChannelUpstreamUsageHour{}, &FinanceChannelDomainPeriod{}); e != nil {
		t.Fatal(e)
	}
	if e = db.Exec("PRAGMA temp_store=MEMORY").Error; e != nil {
		t.Fatal(e)
	}
	return db
}

type financeActivityPlanRow struct {
	Domain  sql.NullString
	FirstTs sql.NullInt64
}

func readFinanceActivityPlanRows(t *testing.T, db *gorm.DB, query string, to int64) []financeActivityPlanRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows := []financeActivityPlanRow{}
	if e := db.WithContext(ctx).Raw(query, financeUpstreamActivityArgs(to)...).Scan(&rows).Error; e != nil {
		t.Fatal(e)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Domain.Valid != rows[j].Domain.Valid {
			return !rows[i].Domain.Valid
		}
		return rows[i].Domain.String < rows[j].Domain.String
	})
	return rows
}

func TestFinanceBillActivityCandidatesPreserveHistoricalAndNullSemantics(t *testing.T) {
	db := newFinanceActivityPlanDB(t)
	for _, v := range []any{
		&[]ChannelSnap{{ID: 1, BaseDomain: "supplier.example"}, {ID: 2, BaseDomain: "disabled.example", Status: 2, DeletedAt: 200}},
		&[]StabilityHourSample{{ChannelID: 1, HourTs: 100, Success: 1, TrafficClassVersion: 6}, {ChannelID: 1, HourTs: 300, RefundQuota: 1, TrafficClassVersion: 7}, {ChannelID: 2, HourTs: 250, Quota: -1, TrafficClassVersion: 7}},
		&[]ChannelTestHourSample{{ChannelID: 1, HourTs: 50, Requests: 1, TrafficClassVersion: 7}},
		&[]FinanceChannelDomainPeriod{{ChannelID: 1, FromHourTs: 0, Domain: "old.example"}, {ChannelID: 1, FromHourTs: 200, Domain: "new.example"}},
	} {
		if e := db.Create(v).Error; e != nil {
			t.Fatal(e)
		}
	}
	rng := rand.New(rand.NewSource(561))
	domains := []any{"supplier.example", " SUPPLIER.example ", "supplier.EXAMPLE", "another.example", "", "  ", "\tx.example\t", "Å.example", "å.example", nil}
	for i := 0; i < 220; i++ {
		var values [4]any
		for j := range values {
			if rng.Intn(4) != 0 {
				values[j] = rng.Intn(3) - 1
			}
		}
		if e := db.Exec("INSERT INTO channel_upstream_usage_hours(domain,hour_ts,requests,tokens,quota,cost_usd) VALUES(?,?,?,?,?,?)", domains[i%len(domains)], i*10-20, values[0], values[1], values[2], values[3]).Error; e != nil {
			t.Fatal(e)
		}
	}
	for _, query := range []string{financeUpstreamActivityQuery, financeHistoricalUpstreamActivityQuery} {
		for _, to := range []int64{-50, -20, 0, 1, 50, 51, 200, 201, 900, 10000} {
			want := readFinanceActivityPlanRows(t, db, query, to)
			for _, fragment := range []string{groupedFinanceBillActivityCandidate, seekingFinanceBillActivityCandidate} {
				got := readFinanceActivityPlanRows(t, db, financeActivityVariant(t, query, fragment), to)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("activity rows changed at boundary %d: want=%v got=%v", to, want, got)
				}
			}
		}
	}
}

func TestFinanceBillActivitySQLiteExperiment(t *testing.T) {
	if os.Getenv("MONITOR_SQL_ACTIVITY_EXPERIMENT") != "1" {
		t.Skip("explicit synthetic performance run required")
	}
	type measurement struct {
		Name                                   string
		OriginalUS, GroupedUS, SeekingUS       []int64
		OriginalPlan, GroupedPlan, SeekingPlan []string
	}
	var results []measurement
	for _, c := range []struct {
		name           string
		domains, hours int
		zeros          bool
		to             int64
	}{{"early-active", 80, 750, false, 10000}, {"all-zero", 80, 750, true, 10000}, {"many-domains", 15000, 4, false, 10000}, {"short-prefix", 80, 750, false, 20}} {
		db := newFinanceActivityPlanDB(t)
		batch := make([]ChannelUpstreamUsageHour, 0, 500)
		for d := 0; d < c.domains; d++ {
			for h := 0; h < c.hours; h++ {
				r := ChannelUpstreamUsageHour{Domain: fmt.Sprintf("vendor-%05d.example", d), HourTs: int64(h)}
				if !c.zeros && (h%11 == 0) {
					r.Requests = 1
				}
				batch = append(batch, r)
				if len(batch) == 500 {
					if e := db.Create(&batch).Error; e != nil {
						t.Fatal(e)
					}
					batch = batch[:0]
				}
			}
		}
		if len(batch) > 0 {
			if e := db.Create(&batch).Error; e != nil {
				t.Fatal(e)
			}
		}
		r := measurement{Name: c.name}
		base := financeHistoricalUpstreamActivityQuery
		grouped := financeActivityVariant(t, base, groupedFinanceBillActivityCandidate)
		seeking := financeActivityVariant(t, base, seekingFinanceBillActivityCandidate)
		want := readFinanceActivityPlanRows(t, db, base, c.to)
		for _, v := range []int{0, 1, 2, 2, 1, 0} {
			q := base
			if v == 1 {
				q = grouped
			}
			if v == 2 {
				q = seeking
			}
			started := time.Now()
			got := readFinanceActivityPlanRows(t, db, q, c.to)
			us := time.Since(started).Microseconds()
			if !reflect.DeepEqual(got, want) {
				t.Fatal("experimental activity aggregate changed result")
			}
			switch v {
			case 0:
				r.OriginalUS = append(r.OriginalUS, us)
			case 1:
				r.GroupedUS = append(r.GroupedUS, us)
			case 2:
				r.SeekingUS = append(r.SeekingUS, us)
			}
		}
		for i, q := range []string{base, grouped, seeking} {
			var plan []struct{ Detail string }
			if e := db.Raw("EXPLAIN QUERY PLAN "+q, financeUpstreamActivityArgs(c.to)...).Scan(&plan).Error; e != nil {
				t.Fatal(e)
			}
			var details []string
			for _, s := range plan {
				details = append(details, s.Detail)
			}
			switch i {
			case 0:
				r.OriginalPlan = details
			case 1:
				r.GroupedPlan = details
			case 2:
				r.SeekingPlan = details
			}
		}
		t.Logf("%s rows=%d original_us=%v grouped_us=%v seeking_us=%v; in-memory synthetic only", c.name, c.domains*c.hours, r.OriginalUS, r.GroupedUS, r.SeekingUS)
		results = append(results, r)
	}
	b, e := json.MarshalIndent(results, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("../docs/sql-audit-20261126/finance-activity-experiment.json", append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}
