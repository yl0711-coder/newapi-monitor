package monitor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const legacyUpstreamPauseSQL = `SELECT 1 FROM channel_upstream_retirements r
	WHERE r.retiring = 1 AND r.domain = LOWER(TRIM(%s))
	AND NOT EXISTS (SELECT 1 FROM channel_snaps s
		WHERE s.deleted_at = 0 AND s.status = 1 AND LOWER(TRIM(s.base_domain)) = r.domain)
	AND NOT EXISTS (SELECT 1 FROM channel_snaps s JOIN stability_hour_samples h ON h.channel_id = s.id
		WHERE LOWER(TRIM(s.base_domain)) = r.domain
		AND h.hour_ts >= CAST(strftime('%%s','now') AS INTEGER) - 172800
		AND h.success + h.anomaly + h.failed > 0)`

func TestSQLAuditUpstreamPauseRecentActivityScale(t *testing.T) {
	db, _ := sqlScaleDB(t, &ChannelUpstreamRetirement{}, &ChannelSnap{}, &StabilityHourSample{}, &ChannelUpstreamAccount{})
	now := time.Now().Unix()
	if err := db.Exec(`INSERT INTO channel_upstream_retirements(domain,retiring) VALUES
	('quiet',1),('busy',1),('active',1),('mixed',1),('many',1),('unmarked',0),('zero',1),('future',1);
	INSERT INTO channel_snaps(id,base_domain,status,deleted_at) VALUES
	(1,'quiet',0,0),(2,'busy',0,0),(3,'active',1,0),(4,' MIXED ',0,1),(5,'zero',0,0),(6,'future',0,0);
	WITH RECURSIVE seq(n) AS (SELECT 10 UNION ALL SELECT n+1 FROM seq WHERE n<109)
	INSERT INTO channel_snaps(id,base_domain,status,deleted_at) SELECT n,'many',0,0 FROM seq;
	WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<100000)
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version,success,anomaly,failed)
	SELECT ?-(n%48)*3600,10+n%100,printf('m%d',n),'g',7,1,0,0 FROM seq;
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version,success,anomaly,failed) VALUES
	(?-172801,1,'old','g',7,1,0,0),(?-60,2,'recent','g',7,1,0,0),(?-60,4,'mixed','g',7,0,1,0),
	(?-60,5,'zero','g',7,0,0,0),(?+86400,6,'future','g',7,0,0,1);`, now, now, now, now, now, now).Error; err != nil {
		t.Fatal(err)
	}
	candidate := strings.Replace(legacyUpstreamPauseSQL, "channel_snaps s JOIN stability_hour_samples h", "channel_snaps s CROSS JOIN stability_hour_samples h", 1)
	if strings.Join(strings.Fields(candidate), " ") != strings.Join(strings.Fields(upstreamSyncPausedDomainSQL), " ") {
		t.Fatal("tested candidate is not the actual product fragment")
	}
	type observation struct {
		Statistics, Domain, Variant string
		Paused                      int
		US                          []int64
		Plan                        []string
	}
	var results []observation
	for _, stats := range []bool{false, true} {
		if stats {
			if err := db.Exec("ANALYZE").Error; err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			domain string
			want   int
		}{{"quiet", 1}, {"busy", 0}, {"active", 0}, {"mixed", 0}, {"many", 0}, {"unmarked", 0}, {"zero", 1}, {"future", 0}, {"absent", 0}} {
			variants := []struct{ name, q string }{{"legacy", legacyUpstreamPauseSQL}, {"domain_first", candidate}}
			start := len(results)
			for _, v := range variants {
				q := "SELECT EXISTS(" + fmt.Sprintf(v.q, "?") + ") AS paused"
				var plans []struct{ Detail string }
				if err := db.Raw("EXPLAIN QUERY PLAN "+q, tc.domain).Scan(&plans).Error; err != nil {
					t.Fatal(err)
				}
				r := observation{Statistics: fmt.Sprint(stats), Domain: tc.domain, Variant: v.name, Paused: tc.want}
				for _, p := range plans {
					r.Plan = append(r.Plan, p.Detail)
				}
				results = append(results, r)
			}
			for round := 0; round < 5; round++ {
				for j := range variants {
					i := j
					if round%2 != 0 {
						i = 1 - j
					}
					var paused int
					at := time.Now()
					err := db.Raw("SELECT EXISTS("+fmt.Sprintf(variants[i].q, "?")+") AS paused", tc.domain).Scan(&paused).Error
					us := time.Since(at).Microseconds()
					if err != nil || paused != tc.want {
						t.Fatal("pause semantics changed", tc.domain, paused, err)
					}
					results[start+i].US = append(results[start+i].US, us)
				}
			}
		}
	}
	if err := db.Exec("INSERT INTO channel_upstream_accounts(domain) SELECT domain FROM channel_upstream_retirements").Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	attachSQLAuditFixture(t, m)
	paused, err := m.upstreamSyncPausedDomains(context.Background())
	if err != nil || len(paused) != 2 || !paused["quiet"] || !paused["zero"] {
		t.Fatal("actual pause projection changed", paused, err)
	}
	source, err := os.ReadFile("channel_upstream_retirement.go")
	if err != nil {
		t.Fatal(err)
	}
	sqlScaleWrite(t, "upstream-pause-activity-scale-20261127.json", struct {
		Warning, SourceSHA256 string
		Results               []observation
	}{
		"100000 synthetic recent hour samples, actual model indexes, 5 alternating memory-SQLite samples, with/without ANALYZE. Old grace cutoff (including existing future-row behavior), active/retired/normalized/deleted domains unchanged. Not production latency.", fmt.Sprintf("%x", sha256.Sum256(source)), results})
}
