package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Only short-lived, memory SQLite fixtures. Trace counts SELECT invocations;
// it does not retain interpolated values or add an EXPLAIN to timed requests.
type sqlScaleTrace struct{ selects int }

func (s *sqlScaleTrace) LogMode(logger.LogLevel) logger.Interface { return s }
func (s *sqlScaleTrace) Info(context.Context, string, ...any)     {}
func (s *sqlScaleTrace) Warn(context.Context, string, ...any)     {}
func (s *sqlScaleTrace) Error(context.Context, string, ...any)    {}
func (s *sqlScaleTrace) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	q, _ := fc()
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(q)), "SELECT") {
		s.selects++
	}
}
func sqlScaleDB(t *testing.T, models ...any) (*gorm.DB, *sqlScaleTrace) {
	t.Helper()
	tr := &sqlScaleTrace{}
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: tr})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { pool.Close() })
	if err = db.AutoMigrate(models...); err != nil {
		t.Fatal(err)
	}
	return db, tr
}
func sqlScalePlan(t *testing.T, db, q *gorm.DB) []string {
	t.Helper()
	stmt := q.Statement
	var plan []struct{ Detail string }
	if err := db.Raw("EXPLAIN QUERY PLAN "+stmt.SQL.String(), stmt.Vars...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(plan))
	for n := range plan {
		out[n] = plan[n].Detail
	}
	return out
}
func sqlScaleWrite(t *testing.T, name string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/"+name, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

// Frozen pre-change HTTP algorithm, independent of any new helper.
func legacyHealthGroupList(m *Monitor, c *gin.Context) {
	var groups []CustomerHealthGroup
	if err := m.storeDB.Order("created_at ASC, id ASC").Find(&groups).Error; err != nil {
		c.JSON(500, gin.H{"error": "读取客户维护公司失败"})
		return
	}
	type row struct {
		CustomerHealthGroup
		Members int64 `json:"members"`
	}
	out := make([]row, 0, len(groups))
	for _, g := range groups {
		var count int64
		if err := m.storeDB.Model(&CustomerHealthMember{}).Where("group_id = ?", g.ID).Count(&count).Error; err != nil {
			c.JSON(500, gin.H{"error": "读取客户维护成员失败"})
			return
		}
		out = append(out, row{g, count})
	}
	c.JSON(200, gin.H{"groups": out})
}

func currentHealthGroupList(m *Monitor, c *gin.Context) { m.listCustomerHealthGroups(c) }

func runHealthList(t *testing.T, m *Monitor, handler func(*Monitor, *gin.Context)) (int, string) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/customer-health/groups", nil)
	handler(m, c)
	return w.Code, w.Body.String()
}

func TestSQLScaleHealthGroupCountIntegrated(t *testing.T) {
	gin.SetMode(gin.TestMode)
	type observation struct {
		Groups                 int
		OldQueries, NewQueries int
		OldUS, NewUS           []int64
		Plan                   []string
	}
	var all []observation
	for _, size := range []int{0, 1, 80, 1100} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db, tr := sqlScaleDB(t, &CustomerHealthGroup{}, &CustomerHealthMember{})
			groups := make([]CustomerHealthGroup, size)
			members := make([]CustomerHealthMember, 0, size*3+10000)
			for n := range groups {
				groups[n] = CustomerHealthGroup{ID: int64(3*n + 1), Name: fmt.Sprintf("客户-%04d", n), Note: "合成 · 不是用量分组", CreatedAt: int64(n % 5)}
				// Include zero-member groups and repeated created_at sort ties.
				for j := 0; j < n%7; j++ {
					members = append(members, CustomerHealthMember{UserID: int64(len(members) + 1), GroupID: groups[n].ID})
				}
			}
			for n := 0; n < 10000; n++ {
				members = append(members, CustomerHealthMember{UserID: int64(len(members) + 1), GroupID: 999999})
			}
			if len(groups) > 0 {
				if err := db.CreateInBatches(groups, 200).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := db.CreateInBatches(members, 200).Error; err != nil {
				t.Fatal(err)
			}
			m := &Monitor{storeDB: db}
			o := observation{Groups: size}
			var expected string
			for round := 0; round < 7; round++ {
				for n := 0; n < 2; n++ {
					i := n
					if round%2 != 0 {
						i = 1 - n
					}
					handler := legacyHealthGroupList
					if i == 1 {
						handler = currentHealthGroupList
					}
					tr.selects = 0
					start := time.Now()
					status, body := runHealthList(t, m, handler)
					us := time.Since(start).Microseconds()
					if status != 200 {
						t.Fatal("unexpected HTTP error", body)
					}
					if round == 0 && n == 0 {
						expected = body
					}
					if body != expected {
						t.Fatal("HTTP fields/order/count changed")
					}
					if i == 0 {
						o.OldQueries = tr.selects
						o.OldUS = append(o.OldUS, us)
					} else {
						o.NewQueries = tr.selects
						o.NewUS = append(o.NewUS, us)
					}
				}
			}
			wantQueries := 1 + (size+499)/500
			if o.OldQueries != size+1 || o.NewQueries != wantQueries {
				t.Fatalf("unexpected real SELECT count %+v", o)
			}
			if size > 0 {
				ids := []int64{groups[0].ID}
				if size > 1 {
					ids = append(ids, groups[len(groups)-1].ID)
				}
				var out []struct{ GroupID, Members int64 }
				q := db.Session(&gorm.Session{DryRun: true}).Model(&CustomerHealthMember{}).Select("group_id,COUNT(*) AS members").Where("group_id IN ?", ids).Group("group_id").Find(&out)
				o.Plan = sqlScalePlan(t, db, q)
				if !strings.Contains(strings.Join(o.Plan, "\n"), "SEARCH customer_health_members USING COVERING INDEX") {
					t.Fatal("batch scans unrelated members", o.Plan)
				}
			}
			all = append(all, o)
		})
	}
	sqlScaleWrite(t, "health-group-count-integrated-20261127.json", struct {
		Warning string
		Results []observation
	}{"Same HTTP bytes and all fields, 10000 unrelated members, existing indexes only. Seven alternating in-memory samples, no network/production/p95 claim.", all})
}

// Compare the exact whole persisted record before touching the production
// historical price reader. The metadata subquery retains the full old order.
func financeVersionMetadataCandidate(tx *gorm.DB, domain string, at int64) *gorm.DB {
	pick := tx.Model(&ChannelFinanceVersion{}).Select("id").Where("domain = ? AND effective_at <= ?", domain, at).Order("effective_at DESC, version DESC, id ASC").Limit(1)
	return tx.Where("id = (?)", pick)
}

func TestSQLScaleFinanceVersionProjectionCandidate(t *testing.T) {
	type observation struct {
		Domain           string
		At               int64
		OldUS, NewUS     []int64
		OldPlan, NewPlan []string
	}
	var results []observation
	db, _ := sqlScaleDB(t, &ChannelFinanceVersion{})
	// 40k rows / 200 versions each. 2KiB synthetic snapshots test late body
	// lookup without persisting a production copy or fetching real settings.
	snapshot := `{"upstream_recharge_paid":1,"upstream_recharge_credit":2,"padding":"` + strings.Repeat("p", 2048) + `"}`
	if err := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<39999)
INSERT INTO channel_finance_versions(domain,version,snapshot_json,effective_at,created_at,updated_by)
SELECT printf('d%03d',n/200),n%200+1,?,100+(n%200)/2,1,'synthetic' FROM seq`, snapshot).Error; err != nil {
		t.Fatal(err)
	}
	for _, domain := range []string{"one-version", "two-versions"} {
		for v := int64(1); v <= 2; v++ {
			if domain == "one-version" && v == 2 {
				continue
			}
			if err := db.Create(&ChannelFinanceVersion{Domain: domain, Version: v, SnapshotJSON: snapshot, EffectiveAt: 100, CreatedAt: 1}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, domain := range []string{"d000", "d199", "one-version", "two-versions", "missing"} {
		for _, at := range []int64{99, 150, 999} {
			o := observation{Domain: domain, At: at}
			var expected ChannelFinanceVersion
			var expectedErr error
			for round := 0; round < 7; round++ {
				for n := 0; n < 2; n++ {
					i := n
					if round%2 != 0 {
						i = 1 - n
					}
					var got ChannelFinanceVersion
					q := db.Where("domain = ? AND effective_at <= ?", domain, at).Order("effective_at DESC, version DESC")
					if i == 1 {
						q = financeVersionMetadataCandidate(db, domain, at)
					}
					start := time.Now()
					err := q.First(&got).Error
					us := time.Since(start).Microseconds()
					if round == 0 && n == 0 {
						expected, expectedErr = got, err
					}
					if !reflect.DeepEqual(got, expected) || (err == nil) != (expectedErr == nil) || (err != nil && err.Error() != expectedErr.Error()) {
						t.Fatal("historical version or missing behavior changed", domain, at, err, expectedErr)
					}
					if i == 0 {
						o.OldUS = append(o.OldUS, us)
					} else {
						o.NewUS = append(o.NewUS, us)
					}
				}
			}
			for n := 0; n < 2; n++ {
				var out ChannelFinanceVersion
				q := db.Session(&gorm.Session{DryRun: true}).Where("domain = ? AND effective_at <= ?", domain, at).Order("effective_at DESC, version DESC")
				if n == 1 {
					q = financeVersionMetadataCandidate(db.Session(&gorm.Session{DryRun: true}), domain, at)
				}
				q = q.First(&out)
				if n == 0 {
					o.OldPlan = sqlScalePlan(t, db, q)
				} else {
					o.NewPlan = sqlScalePlan(t, db, q)
				}
			}
			results = append(results, o)
		}
	}
	sqlScaleWrite(t, "finance-version-projection-candidate-20261127.json", struct {
		Warning string
		Results []observation
	}{"40k synthetic rows, identical full persisted record/error, 7 alternating in-memory samples. Existing indexes; no production scale/p95 claim.", results})
}

func sqlScaleMedian(a []int64) int64 {
	b := append([]int64(nil), a...)
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	return b[len(b)/2]
}
