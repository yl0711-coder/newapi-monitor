package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Keep the old query as an independent equivalence oracle, not another call
// through the optimized helper. No timing thresholds: assert its index plan.
func legacyEconomicsPublicationQuery(db *gorm.DB) *gorm.DB {
	return db.Table("channel_economics_hour_manifest_current mc").
		Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Joins("JOIN channel_economics_hour_publications p ON p.domain=mp.domain AND p.hour_ts=mp.hour_ts AND p.account_epoch=mp.authoritative_epoch AND p.semantics_version=mp.semantics_version").
		Joins("JOIN channel_economics_hour_current c ON c.publication_id=p.publication_id")
}

func economicsCurrentQueryFixture(t *testing.T) *gorm.DB {
	t.Helper()
	db := newChannelCostTestStore(t)
	create := func(value any) {
		t.Helper()
		if err := db.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, domain := range []string{"a.example", "b.example"} {
		for _, hour := range []int64{3600, 7200, 10800} {
			for _, epoch := range []string{"old", "new"} {
				for channel := 1; channel <= 2; channel++ {
					logical := economicsLogicalKey(domain, epoch, hour, channel)
					for revision := int64(1); revision <= 2; revision++ {
						id := economicsPublicationID(logical, "fixture", revision)
						create(&ChannelEconomicsHourPublication{
							PublicationID: id, LogicalKey: logical, Revision: revision,
							Domain: domain, AccountEpoch: epoch, HourTs: hour, LocalChannelID: channel,
							SemanticsVersion: channelEconomicsSemanticsVersion, FinanceVersion: revision,
							RevenueMicroUSD: hour + revision, CorrectedCostMicroUSD: int64(channel),
							CoverageStatus: "upstream_cost_missing", // Must reach downstream proof checks.
						})
						if revision == 2 {
							create(&ChannelEconomicsHourCurrent{LogicalKey: logical, PublicationID: id, Revision: revision})
						}
					}
				}
			}
			for revision := int64(1); revision <= 2; revision++ {
				logical := economicsManifestLogicalKey(domain, hour)
				id := economicsManifestPublicationID(logical, "fixture", revision)
				epoch := "old"
				if revision == 2 {
					epoch = "new"
				}
				create(&ChannelEconomicsHourManifestPublication{ManifestID: id, LogicalKey: logical,
					Revision: revision, Domain: domain, HourTs: hour, SemanticsVersion: channelEconomicsSemanticsVersion,
					AuthoritativeEpoch: epoch})
				if revision == 2 {
					create(&ChannelEconomicsHourManifestCurrent{Domain: domain, HourTs: hour,
						SemanticsVersion: channelEconomicsSemanticsVersion, ManifestID: id, Revision: revision})
				}
			}
		}
	}
	return db
}

func TestEconomicsCurrentQueryEquivalentAndBounded(t *testing.T) {
	db := economicsCurrentQueryFixture(t)
	for _, tc := range []struct {
		name, domain string
		from, to     int64
		limit, count int
	}{
		{"whole", "", 0, 14400, 100, 12},
		{"half_open", "a.example", 3600, 10800, 100, 4},
		{"limit", "", 0, 14400, 1, 1},
		{"empty", "missing.example", 0, 14400, 100, 0},
		{"outside", "", 14400, 18000, 100, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var expected []channelEconomicsReportRow
			for i, base := range []func(*gorm.DB) *gorm.DB{legacyEconomicsPublicationQuery, currentEconomicsPublicationQuery} {
				q := base(db).Select("p.*").Where("p.semantics_version=? AND p.hour_ts>=? AND p.hour_ts<?", channelEconomicsSemanticsVersion, tc.from, tc.to)
				if tc.domain != "" {
					q = q.Where("p.domain IN ?", []string{tc.domain})
				}
				var rows []channelEconomicsReportRow
				if err := q.Order("p.domain,p.hour_ts,p.account_epoch,p.local_channel_id").Limit(tc.limit).Scan(&rows).Error; err != nil {
					t.Fatal(err)
				}
				if len(rows) != tc.count {
					t.Fatalf("variant %d: got %d rows, want %d", i, len(rows), tc.count)
				}
				for _, row := range rows {
					if row.AccountEpoch != "new" || row.FinanceVersion != 2 || row.CoverageStatus != "upstream_cost_missing" {
						t.Fatal("stale epoch/revision or premature proof filtering")
					}
				}
				if i == 0 {
					expected = rows
				} else if !reflect.DeepEqual(rows, expected) {
					t.Fatal("optimized query differs from original")
				}
			}
		})
	}
}

func TestEconomicsCurrentQueryPlanUsesExactKeys(t *testing.T) {
	db := economicsCurrentQueryFixture(t)
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprint(filtered), func(t *testing.T) {
			q := currentEconomicsPublicationQuery(db.Session(&gorm.Session{DryRun: true})).Select("p.*").
				Where("p.semantics_version=? AND p.hour_ts>=? AND p.hour_ts<?", channelEconomicsSemanticsVersion, 0, 14400)
			if filtered {
				q = q.Where("p.domain IN ?", []string{"a.example"}).Order("p.domain,p.hour_ts,p.account_epoch,p.local_channel_id")
			}
			stmt := q.Limit(maxChannelEconomicsReportRows + 1).Find(&[]channelEconomicsReportRow{}).Statement
			var plan []struct{ Detail string }
			if err := db.Raw("EXPLAIN QUERY PLAN "+stmt.SQL.String(), stmt.Vars...).Scan(&plan).Error; err != nil {
				t.Fatal(err)
			}
			manifestLookup, hourLookup := false, false
			for _, step := range plan {
				manifestLookup = manifestLookup || strings.Contains(step.Detail, "SEARCH mp ") && strings.Contains(step.Detail, "manifest_id=?")
				hourLookup = hourLookup || strings.Contains(step.Detail, "SEARCH p ") && strings.Contains(step.Detail, "hour_ts=?")
			}
			if !manifestLookup || !hourLookup {
				t.Fatalf("lost exact-key lookups: %+v", plan)
			}
		})
	}
}

func TestEconomicsCurrentQueryCancellationReleasesPool(t *testing.T) {
	db := economicsCurrentQueryFixture(t)
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var rows []channelEconomicsReportRow
	if err := currentEconomicsPublicationQuery(db.WithContext(ctx)).Select("p.*").Scan(&rows).Error; err == nil {
		t.Fatal("canceled query succeeded")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := currentEconomicsPublicationQuery(db.WithContext(ctx)).Select("p.*").Limit(1).Scan(&rows).Error; err != nil || len(rows) != 1 {
		t.Fatalf("connection not reusable after cancellation: %v", err)
	}
}
