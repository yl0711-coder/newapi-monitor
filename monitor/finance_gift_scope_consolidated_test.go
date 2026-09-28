//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Explicit offline acceptance of already verified small/large jobs together.
// All three databases are opened readonly; no source connection or repair is
// created by this test. Private paths and evidence never belong in fixtures.
func TestFinanceGiftConsolidatedRealSnapshot(t *testing.T) {
	beforePath, afterPath := os.Getenv("MONITOR_GIFT_COMBINED_BEFORE"), os.Getenv("MONITOR_GIFT_COMBINED_AFTER")
	mainPath, targetsPath := os.Getenv("MONITOR_GIFT_COMBINED_MAIN"), os.Getenv("MONITOR_GIFT_COMBINED_TARGETS")
	if beforePath == "" || afterPath == "" || mainPath == "" || targetsPath == "" {
		t.Skip("requires closed private before/after/main snapshots and verified targets")
	}
	var targets []financeGiftScopeTarget
	if _, err := giftLocalReadJSON(targetsPath, &targets); err != nil {
		t.Fatal(err)
	}
	if len(targets) == 0 {
		t.Fatal("empty acceptance scope")
	}
	mainDB, closeMain, err := giftLocalReadonlyDatabase(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeMain)
	ctx := context.Background()
	policies, err := loadChannelBusinessGroupPolicies(ctx, mainDB)
	if err != nil {
		t.Fatal(err)
	}
	settings := Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", LocalSnapshotOnly: true}
	base := &Monitor{storeDB: mainDB, cfg: settings}
	accounts, err := base.loadFinanceInternalAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	excluded := map[int64]bool{}
	for _, account := range accounts {
		excluded[account.UserID] = true
	}
	seed, err := financeStartHour(settings.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	for _, target := range targets {
		end = max(end, target.HourTs+3600)
	}
	var allocations []financeGiftAllocationResult
	var fullKeys, baseKeys []string
	var reports []financeOperatingReport
	var expectedDelta int64
	for index, path := range []string{beforePath, afterPath} {
		db, closeDB, err := giftLocalReadonlyDatabase(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeDB)
		m := &Monitor{storeDB: mainDB, usageFactsDB: db, cfg: settings}
		if index == 0 && channelBusinessGroupsExcluded(policies) {
			for _, target := range targets {
				if excluded[target.UserID] {
					continue
				}
				var count int64
				if err := db.Model(&FinanceGiftBoundaryEvent{}).Where("source_epoch=? AND hour_ts=? AND user_id=? AND COALESCE(group_known,0)=0 AND quota<>0", target.SourceEpoch, target.HourTs, target.UserID).Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				expectedDelta += count
			}
		}
		allocation, err := m.loadFinanceGiftAllocationForScope(ctx, seed, seed, end, policies, excluded)
		if err != nil {
			t.Fatal(err)
		}
		allocations = append(allocations, allocation)
		for _, full := range []bool{true, false} {
			key, err := m.financeReportSourceFingerprintForScope(ctx, seed, end, full)
			if err != nil {
				t.Fatal(err)
			}
			if full {
				fullKeys = append(fullKeys, key)
			} else {
				baseKeys = append(baseKeys, key)
			}
		}
		var previous financeOperatingReport
		for attempt := 0; attempt < 2; attempt++ {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/finance/report?from=2026-09-21&to=2026-09-22", nil)
			started := time.Now()
			m.serveFinanceOperatingReport(c)
			if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatalf("report status=%d body=%s", w.Code, w.Body.String())
			}
			var report financeOperatingReport
			if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			if !report.GiftCoverage.Complete && (report.Statement.OperatingRevenue != nil || report.Statement.RegistrationGiftConsumption != nil) {
				t.Fatal("partial coverage published final revenue")
			}
			if attempt == 1 && (w.Header().Get("X-Monitor-Finance-Cache") != "hit" || !reflect.DeepEqual(previous, report)) {
				t.Fatal("repeat request did not reuse the verified report")
			}
			t.Logf("snapshot=%d request=%d HTTP=200 cache=%s elapsed=%s gift_complete=%t unknown=%d", index, attempt, w.Header().Get("X-Monitor-Finance-Cache"), time.Since(started), report.GiftCoverage.Complete, report.GiftCoverage.ScopeUnknownEvents)
			previous = report
		}
		reports = append(reports, previous)
	}
	if allocations[0].Coverage.ScopeUnknownEvents-allocations[1].Coverage.ScopeUnknownEvents != expectedDelta {
		t.Fatal("coverage improvement differs from actual non-internal monetary evidence")
	}
	if fullKeys[0] == fullKeys[1] || baseKeys[0] != baseKeys[1] {
		t.Fatal("repair must invalidate full report but preserve base components")
	}
	if !reflect.DeepEqual(reports[0].Statement.UserConsumption, reports[1].Statement.UserConsumption) || !reflect.DeepEqual(reports[0].Statement.UserRefunds, reports[1].Statement.UserRefunds) {
		t.Fatal("scope repair changed report consumption/refund amounts")
	}
	if output := os.Getenv("MONITOR_GIFT_COMBINED_REPORT_OUTPUT"); output != "" {
		data, err := json.Marshal(reports[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("financial unknown evidence %d -> %d; exact scope delta=%d; full cache invalidates, base cache preserved", allocations[0].Coverage.ScopeUnknownEvents, allocations[1].Coverage.ScopeUnknownEvents, expectedDelta)
}
