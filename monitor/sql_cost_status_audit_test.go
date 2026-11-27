package monitor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

const legacyUnallocatedCostSourcesSQL = `SELECT COUNT(*) FROM (
 SELECT DISTINCT e.domain,e.account_epoch,e.source_ref
 FROM channel_upstream_cost_hour_evidence e
 LEFT JOIN channel_cost_source_bindings b
 ON b.domain=e.domain AND b.account_epoch=e.account_epoch AND b.source_ref=e.source_ref
 AND b.status='confirmed' AND b.valid_to=0
 WHERE e.semantics_version=? AND e.domain IN ? AND b.source_ref IS NULL
)`

const dedupUnallocatedCostSourcesSQL = `SELECT COUNT(*) FROM (
 SELECT DISTINCT domain,account_epoch,source_ref FROM channel_upstream_cost_hour_evidence
 WHERE semantics_version=? AND domain IN ?
) e WHERE NOT EXISTS (
 SELECT 1 FROM channel_cost_source_bindings b WHERE b.domain=e.domain AND b.account_epoch=e.account_epoch
 AND b.source_ref=e.source_ref AND b.status='confirmed' AND b.valid_to=0
)`

const groupedUnallocatedCostSourcesSQL = `SELECT COUNT(*) FROM (
 SELECT e.domain,e.account_epoch,e.source_ref FROM channel_upstream_cost_hour_evidence e
 WHERE e.semantics_version=? AND e.domain IN ?
 GROUP BY e.domain,e.account_epoch,e.source_ref
 HAVING NOT EXISTS (SELECT 1 FROM channel_cost_source_bindings b WHERE b.domain=e.domain
 AND b.account_epoch=e.account_epoch AND b.source_ref=e.source_ref AND b.status='confirmed' AND b.valid_to=0)
)`

func TestSQLAuditCostStatusAllocationScale(t *testing.T) {
	db, _ := sqlScaleDB(t, &ChannelUpstreamCostHourEvidence{}, &ChannelCostSourceBinding{})
	if e := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<99999)
 INSERT INTO channel_upstream_cost_hour_evidence(domain,account_epoch,hour_ts,semantics_version,source_ref,dimension_hash)
 SELECT CASE WHEN n%10<7 THEN 'd1' ELSE 'd2' END,printf('ep%d',n%3),(n/1000)*3600,
 CASE WHEN n%17=0 THEN 2 ELSE 1 END,printf('s%04d',n%1000),printf('dim%06d',n) FROM seq;
 WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<2999)
 INSERT INTO channel_cost_source_bindings(domain,account_epoch,source_ref,valid_from,valid_to,status)
 SELECT CASE WHEN n%10<7 THEN 'd1' ELSE 'd2' END,printf('ep%d',n/1000),printf('s%04d',n%1000),0,
 CASE WHEN n%7=0 THEN 99 ELSE 0 END,CASE WHEN n%4=0 THEN 'pending' ELSE 'confirmed' END FROM seq`).Error; e != nil {
		t.Fatal(e)
	}
	type result struct {
		Distribution string
		Domains      []string
		Variant      string
		Count        int64
		US           []int64
		Plan         []string
	}
	var results []result
	for _, dist := range []string{"repeated_source", "unique_source"} {
		if dist == "unique_source" {
			if e := db.Exec("UPDATE channel_upstream_cost_hour_evidence SET source_ref=dimension_hash").Error; e != nil {
				t.Fatal(e)
			}
		}
		for _, domains := range [][]string{{"missing"}, {"d1"}, {"d1", "d2"}} {
			start := len(results)
			variants := []struct{ name, q string }{{"legacy", legacyUnallocatedCostSourcesSQL}, {"dedup_first", dedupUnallocatedCostSourcesSQL}, {"group_having", groupedUnallocatedCostSourcesSQL}}
			var want int64
			if e := db.Raw(variants[0].q, channelCostEvidenceSemanticsVersion, domains).Scan(&want).Error; e != nil {
				t.Fatal(e)
			}
			for _, v := range variants {
				results = append(results, result{Distribution: dist, Domains: domains, Variant: v.name, Count: want})
			}
			for round := 0; round < 7; round++ {
				for j := range variants {
					i := j
					if round%2 != 0 {
						i = len(variants) - 1 - j
					}
					var got int64
					at := time.Now()
					e := db.Raw(variants[i].q, channelCostEvidenceSemanticsVersion, domains).Scan(&got).Error
					us := time.Since(at).Microseconds()
					if e != nil || got != want {
						t.Fatalf("allocation count differs %s %d %d %v", variants[i].name, got, want, e)
					}
					results[start+i].US = append(results[start+i].US, us)
				}
			}
			for j, v := range variants {
				var p []struct{ Detail string }
				if e := db.Raw("EXPLAIN QUERY PLAN "+v.q, channelCostEvidenceSemanticsVersion, domains).Scan(&p).Error; e != nil {
					t.Fatal(e)
				}
				for _, x := range p {
					results[start+j].Plan = append(results[start+j].Plan, x.Detail)
				}
			}
		}
	}
	source, e := os.ReadFile("channel_cost_status.go")
	if e != nil {
		t.Fatal(e)
	}
	sqlScaleWrite(t, "cost-status-allocation-candidates-20261127.json", struct {
		Warning      string
		Results      []result
		SourceSHA256 string
	}{"100000 synthetic rows with existing actual model indexes, 7 alternating warm in-memory SQLite samples; read-only equivalent scalar count, not production latency or all endpoint performance.", results, fmt.Sprintf("%x", sha256.Sum256(source))})
}

func TestSQLAuditCostStatusActualQueries(t *testing.T) {
	db := newChannelCostTestStore(t)
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelEconomicsReportEnabled: true, ChannelCostClosureDomains: []string{"d1"}}}
	for _, enabled := range []bool{false, true} {
		m.cfg.ChannelCostClosureEnabled = enabled
		got, e := m.channelCostSyncStatus(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if got.Enabled != enabled || got.UnallocatedSources != 0 {
			t.Fatal("empty status changed", got)
		}
	}
	for i := 0; i < 12; i++ {
		if e := db.Create(&ChannelUpstreamCostHourEvidence{Domain: "d1", AccountEpoch: "ep", HourTs: int64(i * 3600), SemanticsVersion: channelCostEvidenceSemanticsVersion, SourceRef: fmt.Sprintf("s%d", i%3), DimensionHash: fmt.Sprint(i)}).Error; e != nil {
			t.Fatal(e)
		}
	}
	if e := db.Create(&ChannelCostSourceBinding{Domain: "d1", AccountEpoch: "ep", SourceRef: "s1", Status: "confirmed"}).Error; e != nil {
		t.Fatal(e)
	}
	got, e := m.channelCostSyncStatus(context.Background())
	if e != nil || got.UnallocatedSources != 2 {
		t.Fatalf("actual status count %d %v", got.UnallocatedSources, e)
	}
	var old int64
	if e := db.Raw(legacyUnallocatedCostSourcesSQL, channelCostEvidenceSemanticsVersion, []string{"d1"}).Scan(&old).Error; e != nil || old != got.UnallocatedSources {
		t.Fatal("legacy result differs", old, got.UnallocatedSources, e)
	}
	before := got
	// Irrelevant account periods, semantics and historical/unconfirmed bindings
	// must not silently make an unallocated source look confirmed.
	for _, b := range []ChannelCostSourceBinding{{Domain: "d2", AccountEpoch: "ep", SourceRef: "s0", Status: "confirmed"}, {Domain: "d1", AccountEpoch: "old", SourceRef: "s0", Status: "confirmed"}, {Domain: "d1", AccountEpoch: "ep", SourceRef: "s0", Status: "confirmed", ValidFrom: 1, ValidTo: 99}, {Domain: "d1", AccountEpoch: "ep", SourceRef: "s2", Status: "pending"}} {
		if e := db.Create(&b).Error; e != nil {
			t.Fatal(e)
		}
	}
	got, e = m.channelCostSyncStatus(context.Background())
	if e != nil || !reflect.DeepEqual(got, before) {
		t.Fatal("scope/status semantics changed", e)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = m.channelCostSyncStatus(cancelled); e == nil {
		t.Fatal("cancel ignored")
	}
	if e := db.Exec("DROP TABLE channel_cost_source_bindings").Error; e != nil {
		t.Fatal(e)
	}
	if _, e = m.channelCostSyncStatus(context.Background()); e == nil || !strings.Contains(e.Error(), "成本来源归属状态") {
		t.Fatal("failure hidden", e)
	}
}
