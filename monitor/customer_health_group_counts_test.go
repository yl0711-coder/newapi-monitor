package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestHealthGroupCountsFaultsNeverReturnPartialList(t *testing.T) {
	for _, mode := range []string{"missing_groups", "missing_members", "empty_missing_members", "second_batch_failure", "cancelled_request", "cancel_between_batches", "closed_store"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := sqlScaleDB(t, &CustomerHealthGroup{}, &CustomerHealthMember{})
			requestCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode != "empty_missing_members" {
				groups := make([]CustomerHealthGroup, 501)
				for n := range groups {
					groups[n] = CustomerHealthGroup{ID: int64(n + 1), Name: fmt.Sprintf("g-%d", n)}
				}
				if err := db.CreateInBatches(groups, 100).Error; err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "missing_groups":
				if err := db.Migrator().DropTable(&CustomerHealthGroup{}); err != nil {
					t.Fatal(err)
				}
			case "missing_members", "empty_missing_members":
				if err := db.Migrator().DropTable(&CustomerHealthMember{}); err != nil {
					t.Fatal(err)
				}
			case "second_batch_failure":
				calls := 0
				if err := db.Callback().Row().Before("gorm:row").Register("test:fail-second-member-batch", func(tx *gorm.DB) {
					if tx.Statement.Table == "customer_health_members" {
						calls++
						if calls == 2 {
							_ = tx.AddError(errors.New("synthetic second batch failure"))
						}
					}
				}); err != nil {
					t.Fatal(err)
				}
			case "cancel_between_batches":
				calls := 0
				if err := db.Callback().Row().Before("gorm:row").Register("test:cancel-between-member-batches", func(tx *gorm.DB) {
					if tx.Statement.Table == "customer_health_members" {
						calls++
						if calls == 2 {
							cancel()
						}
					}
				}); err != nil {
					t.Fatal(err)
				}
			case "closed_store":
				pool, err := db.DB()
				if err != nil {
					t.Fatal(err)
				}
				if err = pool.Close(); err != nil {
					t.Fatal(err)
				}
			}
			m := &Monitor{storeDB: db}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/customer-health/groups", nil)
			if mode == "cancelled_request" {
				cancel()
			}
			c.Request = c.Request.WithContext(requestCtx)
			m.listCustomerHealthGroups(c)
			pool, err := db.DB()
			if err != nil || pool.Stats().InUse != 0 {
				t.Fatal("failure leaked a connection", err)
			}
			if mode == "empty_missing_members" {
				if w.Code != 200 || w.Body.String() != `{"groups":[]}` {
					t.Fatal("empty list gained unnecessary member query", w.Code, w.Body.String())
				}
				return
			}
			message := "读取客户维护成员失败"
			if mode == "missing_groups" || mode == "cancelled_request" || mode == "closed_store" {
				message = "读取客户维护公司失败"
			}
			if w.Code != 500 || w.Body.String() != `{"error":"`+message+`"}` || strings.Contains(w.Body.String(), `"groups"`) {
				t.Fatal("error response changed or partial results escaped", w.Code, w.Body.String())
			}
			if mode == "cancelled_request" || mode == "cancel_between_batches" || mode == "second_batch_failure" {
				status, body := runHealthList(t, m, currentHealthGroupList)
				if status != 200 {
					t.Fatal("failed request poisoned subsequent read", status, body)
				}
			}
		})
	}
}

func TestHealthGroupCountsPreserveLargeIDsNullsAndIsolation(t *testing.T) {
	db, _ := sqlScaleDB(t, &CustomerHealthGroup{}, &CustomerHealthMember{}, &CustomerGroup{}, &TrackedUser{})
	const large = int64(9007199254740993)
	if err := db.Create(&[]CustomerHealthGroup{{ID: large, Name: "公司甲", CreatedAt: 3}, {ID: 7, Name: "公司乙", CreatedAt: 3}, {ID: 12, Name: "空公司", CreatedAt: 1}}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO customer_health_members(user_id,group_id) VALUES (1,?),(2,7),(3,NULL),(4,0),(5,999)", large).Error; err != nil {
		t.Fatal(err)
	}
	// Seed the unrelated usage projection directly: its normal lifecycle hook
	// requires audit tables and is not part of this read-only list test.
	if err := db.Exec("INSERT INTO tracked_users(user_id,group_id) VALUES(123,?)", large).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	attachSQLAuditFixture(t, m)
	oldStatus, old := runHealthList(t, m, legacyHealthGroupList)
	status, got := runHealthList(t, m, currentHealthGroupList)
	if oldStatus != 200 || status != oldStatus || got != old {
		t.Fatal("large ID, empty/null/unrelated membership or order changed", status, got, old)
	}
	// Existing non-HTTP test callers without a request must remain safe.
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	m.listCustomerHealthGroups(c)
	if w.Code != 200 || w.Body.String() != old {
		t.Fatal("nil request regression")
	}
}

func TestHealthGroupCountsExactBatchBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 500, 501} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db, tr := sqlScaleDB(t, &CustomerHealthGroup{}, &CustomerHealthMember{})
			groups := make([]CustomerHealthGroup, size)
			var members []CustomerHealthMember
			for n := range groups {
				groups[n] = CustomerHealthGroup{ID: int64(n*3 + 1), Name: fmt.Sprintf("公司%d", n), CreatedAt: int64(n % 4)}
				if n%3 != 0 {
					members = append(members, CustomerHealthMember{UserID: int64(n + 1), GroupID: groups[n].ID})
				}
			}
			if size > 0 {
				if err := db.CreateInBatches(groups, 100).Error; err != nil {
					t.Fatal(err)
				}
			}
			if len(members) > 0 {
				if err := db.CreateInBatches(members, 100).Error; err != nil {
					t.Fatal(err)
				}
			}
			m := &Monitor{storeDB: db}
			attachSQLAuditFixture(t, m)
			status, expected := runHealthList(t, m, legacyHealthGroupList)
			if status != 200 {
				t.Fatal(status, expected)
			}
			tr.selects = 0
			status, got := runHealthList(t, m, currentHealthGroupList)
			if status != 200 || got != expected || tr.selects != 1+(size+customerHealthGroupCountBatchSize-1)/customerHealthGroupCountBatchSize {
				t.Fatal("batch boundary changed output/budget", size, status, tr.selects, got)
			}
			pool, err := db.DB()
			if err != nil || pool.Stats().InUse != 0 {
				t.Fatal("connection not released", err)
			}
		})
	}
}
