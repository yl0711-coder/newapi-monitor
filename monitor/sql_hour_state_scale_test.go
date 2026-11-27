package monitor

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestSQLScaleHourlyCostStatePlans(t *testing.T) {
	db, _ := sqlScaleDB(t, &ChannelUpstreamCostHourState{})
	if err := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<99999)
INSERT INTO channel_upstream_cost_hour_states(domain,account_epoch,hour_ts,semantics_version,status,reconcile_status)
SELECT printf('d%03d',n/500),printf('epoch-%d',n%5),((n%500)/5)*3600,CASE WHEN n%13=0 THEN 2 ELSE 1 END,
CASE WHEN n%20=0 THEN 'pending' ELSE 'verified' END,CASE WHEN n%19=0 THEN 'waiting' ELSE 'matched' END FROM seq`).Error; err != nil {
		t.Fatal(err)
	}
	const predicate = "hour_ts = ? AND semantics_version = ? AND status = 'verified' AND reconcile_status = 'matched' AND domain IN ?"
	source, err := os.ReadFile("channel_economics.go")
	if err != nil || !strings.Contains(string(source), predicate) {
		t.Fatal("source predicate changed")
	}
	type pair struct{ Domain, AccountEpoch string }
	type observation struct {
		Distribution string
		Domains      int
		Variant      string
		Rows         int
		US           []int64
		Plan         []string
	}
	type maintenance struct {
		Distribution                  string
		ExtraPages                    int64
		InsertUSBefore, InsertUSAfter []int64
	}
	var results []observation
	var costs []maintenance
	insertCost := func() []int64 {
		var out []int64
		for n := 0; n < 3; n++ {
			tx := db.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			start := time.Now()
			err := tx.Exec(`INSERT INTO channel_upstream_cost_hour_states(domain,account_epoch,hour_ts,semantics_version,status,reconcile_status)
SELECT 'copy-'||domain,account_epoch,hour_ts,semantics_version,status,reconcile_status FROM channel_upstream_cost_hour_states WHERE rowid<=1000`).Error
			if err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err = tx.Rollback().Error; err != nil {
				t.Fatal(err)
			}
			out = append(out, time.Since(start).Microseconds())
		}
		return out
	}
	usedPages := func() int64 {
		var total, free int64
		if err := db.Raw("PRAGMA page_count").Scan(&total).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Raw("PRAGMA freelist_count").Scan(&free).Error; err != nil {
			t.Fatal(err)
		}
		return total - free
	}
	for _, distribution := range []string{"mostly_verified", "rare_verified"} {
		if distribution == "rare_verified" {
			if err := db.Exec("UPDATE channel_upstream_cost_hour_states SET status=CASE WHEN rowid%10000=251 THEN 'verified' ELSE 'pending' END").Error; err != nil {
				t.Fatal(err)
			}
		}
		mc := maintenance{Distribution: distribution, InsertUSBefore: insertCost()}
		before := usedPages()
		for _, indexed := range []bool{false, true} {
			if indexed {
				if err := db.Exec("CREATE INDEX audit_cost_state_hour_cover ON channel_upstream_cost_hour_states(hour_ts,semantics_version,domain,account_epoch,status,reconcile_status)").Error; err != nil {
					t.Fatal(err)
				}
				mc.ExtraPages = usedPages() - before
				mc.InsertUSAfter = insertCost()
			}
			for _, domains := range []int{2, 200} {
				ids := make([]string, domains)
				for n := range ids {
					ids[n] = fmt.Sprintf("d%03d", n)
				}
				if domains == 2 {
					ids[1] = "d199"
				}
				var dry []pair
				q := db.Session(&gorm.Session{DryRun: true}).Model(&ChannelUpstreamCostHourState{}).Select("domain, account_epoch").Where(predicate, int64(180000), channelCostEvidenceSemanticsVersion, ids).Group("domain, account_epoch").Find(&dry)
				original := q.Statement.SQL.String()
				args := q.Statement.Vars
				variants := []struct{ name, sql string }{{"native_existing", original}, {"force_existing_primary", strings.Replace(original, "FROM `channel_upstream_cost_hour_states`", "FROM `channel_upstream_cost_hour_states` INDEXED BY sqlite_autoindex_channel_upstream_cost_hour_states_1", 1)}}
				if indexed {
					variants = []struct{ name, sql string }{{"native_with_experimental_index", original}, {"force_experimental_hour_cover", strings.Replace(original, "FROM `channel_upstream_cost_hour_states`", "FROM `channel_upstream_cost_hour_states` INDEXED BY audit_cost_state_hour_cover", 1)}}
				}
				var expected []pair
				if err := db.Raw(original, args...).Scan(&expected).Error; err != nil {
					t.Fatal(err)
				}
				startResult := len(results)
				for _, v := range variants {
					results = append(results, observation{Distribution: distribution, Domains: domains, Variant: v.name, Rows: len(expected)})
				}
				for round := 0; round < 7; round++ {
					for n := range variants {
						i := n
						if round%2 != 0 {
							i = len(variants) - 1 - n
						}
						var got []pair
						start := time.Now()
						err := db.Raw(variants[i].sql, args...).Scan(&got).Error
						us := time.Since(start).Microseconds()
						if err != nil || !reflect.DeepEqual(got, expected) {
							t.Fatal("state set/order changed", err)
						}
						results[startResult+i].US = append(results[startResult+i].US, us)
					}
				}
				for n, v := range variants {
					var plan []struct{ Detail string }
					if err := db.Raw("EXPLAIN QUERY PLAN "+v.sql, args...).Scan(&plan).Error; err != nil {
						t.Fatal(err)
					}
					for _, p := range plan {
						results[startResult+n].Plan = append(results[startResult+n].Plan, p.Detail)
					}
				}
			}
		}
		costs = append(costs, mc)
		if err := db.Exec("DROP INDEX audit_cost_state_hour_cover").Error; err != nil {
			t.Fatal(err)
		}
	}
	sqlScaleWrite(t, "hourly-cost-state-scale-20261127.json", struct {
		Warning     string
		Rows        int
		Results     []observation
		Maintenance []maintenance
	}{"Existing actual model and matching source predicate; independently rebuilt read shape, not whole enqueue transaction. 7 alternating in-memory samples; experimental indexes only here. Rolled-back INSERT is not durability/fsync evidence; extra pages are logical allocation, not physical SSD writes. No ANALYZE so statistics differ from analyzed installations.", 100000, results, costs})
}
