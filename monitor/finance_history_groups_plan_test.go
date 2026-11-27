package monitor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"gorm.io/gorm"
)

const legacyFinanceHistoryGroupsSQL = `SELECT DISTINCT s.grp FROM stability_hour_samples s
	JOIN channel_snaps c ON c.id=s.channel_id
	WHERE c.base_domain=? AND s.grp<>'' AND s.traffic_class_version IN ? LIMIT ?`

const domainFirstFinanceHistoryGroupsSQL = `SELECT DISTINCT s.grp FROM channel_snaps c
	CROSS JOIN stability_hour_samples s ON s.channel_id=c.id
	WHERE c.base_domain=? AND s.grp<>'' AND s.traffic_class_version IN ? LIMIT ?`

const memberSetFinanceHistoryGroupsSQL = `SELECT DISTINCT s.grp FROM stability_hour_samples s
	WHERE s.channel_id IN (SELECT id FROM channel_snaps WHERE base_domain=?)
	AND s.grp<>'' AND s.traffic_class_version IN ? LIMIT ?`

const directMembersFinanceHistoryGroupsSQL = `SELECT DISTINCT s.grp FROM stability_hour_samples s
	WHERE s.channel_id IN ? AND s.grp<>'' AND s.traffic_class_version IN ? LIMIT ?`

func TestFinanceHistoryGroupsRoutingKeepsLiveOwnership(t *testing.T) {
	db, trace := sqlScaleDB(t, &ChannelSnap{}, &StabilityHourSample{})
	if err := db.Exec(`INSERT INTO channel_snaps(id,base_domain,groups) VALUES(1,'a','configured'),(2,'b','configured');
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version) VALUES
	(1,1,'m','old-owner',7),(1,2,'m','new-owner',6),(2,2,'m','unsupported',5);`).Error; err != nil {
		t.Fatal(err)
	}
	// The first directory read may have observed one channel. Before the
	// second SELECT, ownership can change and more channels can appear.
	chosen := channelFinanceHistoryGroupsSQL(1)
	if err := db.Exec(`UPDATE channel_snaps SET base_domain=CASE id WHEN 1 THEN 'b' ELSE 'a' END;
	INSERT INTO channel_snaps(id,base_domain,groups) VALUES(3,'a','configured');
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version) VALUES(1,3,'m','new-channel',7);`).Error; err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{legacyFinanceHistoryGroupsSQL, chosen, channelFinanceHistoryGroupsSQL(2)} {
		var groups []string
		if err := db.Raw(q, "a", accountingTrafficVersions(), maxChannelFinanceGroups+1).Scan(&groups).Error; err != nil {
			t.Fatal(err)
		}
		sort.Strings(groups)
		if fmt.Sprint(groups) != "[new-channel new-owner]" {
			t.Fatal("ownership was frozen or expanded", groups)
		}
	}
	m := &Monitor{storeDB: db}
	attachSQLAuditFixture(t, m)
	before := trace.selects
	got, err := m.allowedChannelFinanceGroups(context.Background(), "a")
	if err != nil || !reflect.DeepEqual(got, map[string]bool{"configured": true, "new-channel": true, "new-owner": true}) {
		t.Fatal("actual live group set changed", got, err)
	}
	if activeSQLPlanCapture == nil && trace.selects-before != 2 {
		t.Fatal("routing added source SELECTs", trace.selects-before)
	}
}

func TestSQLAuditFinanceHistoryGroupsScale(t *testing.T) {
	db, _ := sqlScaleDB(t, &ChannelSnap{}, &StabilityHourSample{})
	if err := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<102)
	INSERT INTO channel_snaps(id,base_domain,groups)
	SELECT n,CASE WHEN n<=2 THEN 'small' ELSE 'wide' END,'configured' FROM seq;
	INSERT INTO channel_snaps(id,base_domain,groups) VALUES(103,'single','configured');
	WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<100000)
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version)
	SELECT n*3600,3+n%100,'model',printf('historic%d',n%7),CASE WHEN n%11=0 THEN 5 WHEN n%2=0 THEN 6 ELSE 7 END FROM seq;
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version) VALUES
	(1,1,'m','history-small',6),(2,2,'m','history-peer',7),(3,1,'m','excluded-old',5),(4,1,'m','',7),(5,103,'m','history-single',7);`).Error; err != nil {
		t.Fatal(err)
	}
	type result struct {
		Statistics, Domain, Variant string
		Groups                      []string
		US                          []int64
		Plan                        []string
	}
	var results []result
	// All data, plans, variants and semantic assertions run in both modes.
	// Repeated wall-clock sampling is meaningful only without instrumentation.
	rounds := 7
	if sqlAuditRaceEnabled {
		rounds = 1
	}
	measure := func(stats string) {
		for _, domain := range []string{"single", "small", "wide", "absent"} {
			var ids []int
			if err := db.Model(&ChannelSnap{}).Where("base_domain=?", domain).Pluck("id", &ids).Error; err != nil {
				t.Fatal(err)
			}
			variants := []struct {
				name, q  string
				selector any
			}{{"legacy", legacyFinanceHistoryGroupsSQL, domain}, {"domain_first", domainFirstFinanceHistoryGroupsSQL, domain}, {"member_set", memberSetFinanceHistoryGroupsSQL, domain}, {"direct_members", directMembersFinanceHistoryGroupsSQL, ids}, {"guarded_single_channel", channelFinanceHistoryGroupsSQL(len(ids)), domain}}
			start := len(results)
			var want []string
			for i, v := range variants {
				var groups []string
				if err := db.Raw(v.q, v.selector, accountingTrafficVersions(), maxChannelFinanceGroups+1).Scan(&groups).Error; err != nil {
					t.Fatal(err)
				}
				sort.Strings(groups)
				if i == 0 {
					want = groups
				} else if !reflect.DeepEqual(want, groups) {
					t.Fatal("history groups changed", stats, domain, want, groups)
				}
				if domain == "small" && fmt.Sprint(groups) != "[history-peer history-small]" {
					t.Fatal("source version/domain/blank filter changed", groups)
				}
				dry := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
					return tx.Raw(v.q, v.selector, accountingTrafficVersions(), maxChannelFinanceGroups+1).Scan(&[]string{})
				})
				var plan []struct{ Detail string }
				if err := db.Raw("EXPLAIN QUERY PLAN " + dry).Scan(&plan).Error; err != nil {
					t.Fatal(err)
				}
				r := result{Statistics: stats, Domain: domain, Variant: v.name, Groups: groups}
				for _, p := range plan {
					r.Plan = append(r.Plan, p.Detail)
				}
				results = append(results, r)
			}
			for round := 0; round < rounds; round++ {
				for j := range variants {
					i := j
					if round%2 != 0 {
						i = len(variants) - 1 - j
					}
					var groups []string
					at := time.Now()
					err := db.Raw(variants[i].q, variants[i].selector, accountingTrafficVersions(), maxChannelFinanceGroups+1).Scan(&groups).Error
					us := time.Since(at).Microseconds()
					sort.Strings(groups)
					if err != nil || !reflect.DeepEqual(groups, want) {
						t.Fatal("timed result differs", err)
					}
					results[start+i].US = append(results[start+i].US, us)
				}
			}
		}
	}
	measure("no_statistics")
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	measure("analyzed")
	// Current accounting versions may be rare among retained legacy samples.
	// A forced domain-first plan must not hide this legitimate counterexample.
	if err := db.Exec(`UPDATE stability_hour_samples SET traffic_class_version=CASE WHEN hour_ts/3600%10000=0 THEN 7 ELSE 5 END WHERE channel_id>=3 AND channel_id<=102;ANALYZE;`).Error; err != nil {
		t.Fatal(err)
	}
	measure("rare_versions_analyzed")
	// The actual entry also unions configured groups with historical ones.
	m := &Monitor{storeDB: db}
	attachSQLAuditFixture(t, m)
	allowed, err := m.allowedChannelFinanceGroups(context.Background(), "small")
	if err != nil || !reflect.DeepEqual(allowed, map[string]bool{"configured": true, "history-small": true, "history-peer": true}) {
		t.Fatal("actual group permissions differ", allowed, err)
	}
	if _, err := m.allowedChannelFinanceGroups(context.Background(), "absent"); err == nil {
		t.Fatal("unknown domain accepted")
	}
	if x, err := m.allowedChannelFinanceGroups(context.Background(), "single"); err != nil || !reflect.DeepEqual(x, map[string]bool{"configured": true, "history-single": true}) {
		t.Fatal("single-channel domain changed", x, err)
	}
	if err := db.Exec(`INSERT INTO channel_snaps(id,base_domain,groups) VALUES(1001,'cap','configured');
	WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<500)
	INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version)
	SELECT n,1001,'model',printf('cap%04d',n),7 FROM seq;`).Error; err != nil {
		t.Fatal(err)
	}
	if x, err := m.allowedChannelFinanceGroups(context.Background(), "cap"); err != nil || len(x) != maxChannelFinanceGroups+1 {
		t.Fatal("exact historical cap must still be allowed plus configured groups", len(x), err)
	}
	if err := db.Exec(`INSERT INTO stability_hour_samples(hour_ts,channel_id,model_name,grp,traffic_class_version) VALUES(501,1001,'model','cap0501',7)`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.allowedChannelFinanceGroups(context.Background(), "cap"); err == nil {
		t.Fatal("over-cap history accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.allowedChannelFinanceGroups(ctx, "small"); err == nil {
		t.Fatal("cancellation ignored")
	}
	source, err := os.ReadFile("channel_finance.go")
	if err != nil {
		t.Fatal(err)
	}
	if sqlAuditRaceEnabled {
		t.Log("all scale/plan/semantic/boundary/cancellation checks passed; race timings do not replace native performance evidence")
		return
	}
	sqlScaleWrite(t, "finance-history-groups-candidate-20261127.json", struct {
		Warning      string
		SourceSHA256 string
		Results      []result
	}{"100000 synthetic hour rows, actual model indexes, before/after ANALYZE, 7 alternating warm in-memory SQLite samples; not production latency. Candidate keeps complete groups or the original over-cap failure, not a new historical time cutoff.", fmt.Sprintf("%x", sha256.Sum256(source)), results})
}
