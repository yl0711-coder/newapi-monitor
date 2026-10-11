//go:build unix

package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// This optional acceptance test reads sealed local snapshots only. It verifies
// the effect of already repaired evidence, not permission to import it into a
// running service. Raw-evidence identity and incremental handoff have separate
// tests; a successful report is not a replacement for either check.
func TestFinanceGiftReusedArchiveReport(t *testing.T) {
	mainPath := os.Getenv("MONITOR_GIFT_REUSE_MAIN")
	beforePath := os.Getenv("MONITOR_GIFT_REUSE_BEFORE")
	afterPath := os.Getenv("MONITOR_GIFT_REUSE_AFTER")
	from, to := os.Getenv("MONITOR_GIFT_REUSE_FROM"), os.Getenv("MONITOR_GIFT_REUSE_TO")
	epoch := os.Getenv("MONITOR_GIFT_REUSE_EPOCH")
	if mainPath == "" || beforePath == "" || afterPath == "" || from == "" || to == "" || epoch == "" {
		t.Skip("requires closed main/before/after snapshots, source epoch and an explicit report range")
	}
	paths := []string{mainPath, beforePath, afterPath}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	t.Cleanup(func() {
		for i, path := range paths {
			if giftLocalFileHash(t, path) != hashes[i] {
				t.Error("input snapshot changed", i)
			}
		}
	})
	mainDB, closeMain, err := giftLocalReadonlyDatabase(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	var reports []financeOperatingReport
	var monetary string
	for i, path := range []string{beforePath, afterPath} {
		facts, closeFacts, err := giftLocalReadonlyDatabase(path)
		if err != nil {
			t.Fatal(err)
		}
		defer closeFacts()
		money := giftMixedMonetarySnapshot(t, facts)
		if i == 0 {
			monetary = money
		} else if money != monetary {
			t.Fatal("archived repair changed monetary facts, credits, or synchronization state")
		}
		m := &Monitor{storeDB: mainDB, usageFactsDB: facts, cfg: Settings{
			FinanceEnabled: true, FinanceStartDate: from, LocalSnapshotOnly: true, UsageFactsHistorySourceEpoch: epoch,
		}}
		defer m.financeGiftEvidenceGuard.close()
		var report financeOperatingReport
		for attempt := 0; attempt < 2; attempt++ {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/finance/report?from="+from+"&to="+to, nil)
			started := time.Now()
			m.serveFinanceOperatingReport(c)
			if w.Code != http.StatusOK {
				t.Fatalf("snapshot=%d HTTP=%d response=%s", i, w.Code, w.Body.String())
			}
			var current financeOperatingReport
			if err := json.Unmarshal(w.Body.Bytes(), &current); err != nil {
				t.Fatal(err)
			}
			if attempt > 0 && (w.Header().Get("X-Monitor-Finance-Cache") != "hit" || !reflect.DeepEqual(current, report)) {
				t.Fatal("repeat report did not reuse the verified snapshot")
			}
			report = current
			t.Logf("snapshot=%d request=%d duration=%s cache=%s gift_complete=%t unknown=%d", i, attempt,
				time.Since(started), w.Header().Get("X-Monitor-Finance-Cache"), report.GiftCoverage.Complete, report.GiftCoverage.ScopeUnknownEvents)
		}
		reports = append(reports, report)
	}
	before, after := reports[0], reports[1]
	if before.GiftCoverage.ScopeUnknownEvents == 0 || before.GiftCoverage.Complete ||
		before.Statement.RegistrationGiftConsumption != nil || before.Statement.OperatingRevenue != nil {
		t.Fatal("before snapshot must have a genuine gift scope blocker")
	}
	if !after.GiftCoverage.Complete || after.GiftCoverage.ScopeUnknownEvents != 0 ||
		after.Statement.RegistrationGiftConsumption == nil || after.Statement.OperatingRevenue == nil {
		t.Fatalf("complete reused gift evidence did not restore gift deduction and operating revenue: gift=%+v revenue=%+v user=%+v user_coverage=%+v internal=%+v", after.Statement.RegistrationGiftConsumption, after.Statement.OperatingRevenue, after.Statement.UserConsumption, after.UserCoverage, after.InternalAccounts)
	}
	if !reflect.DeepEqual(before.Statement.UserConsumption, after.Statement.UserConsumption) ||
		!reflect.DeepEqual(before.Statement.UserRefunds, after.Statement.UserRefunds) ||
		!reflect.DeepEqual(before.Statement.KnownRawCorrectedUpstreamCost, after.Statement.KnownRawCorrectedUpstreamCost) {
		t.Fatal("gift-only evidence changed customer consumption, refunds, or upstream cost")
	}
	assertGiftReuseBreakdown(t, after.Statement, after.Periods, after.Days)
	if after.Statement.AWSInfrastructureCost == nil && after.Statement.OperatingProfit != nil {
		t.Fatal("missing AWS cost became a complete operating profit")
	}
	t.Logf("unknown=%d -> 0; gift=%s revenue=%s; months=%d days=%d; profit_complete=%t",
		before.GiftCoverage.ScopeUnknownEvents, after.Statement.RegistrationGiftConsumption.Display,
		after.Statement.OperatingRevenue.Display, len(after.Periods), len(after.Days), after.Statement.OperatingProfit != nil)
}

func assertGiftReuseBreakdown(t *testing.T, total financeStatementView, months []financePeriodView, days []financeDailyView) {
	t.Helper()
	for _, byMonth := range []bool{true, false} {
		var rows []financeStatementView
		if byMonth {
			for _, month := range months {
				rows = append(rows, month.Statement)
			}
		} else {
			for _, day := range days {
				rows = append(rows, day.Statement)
			}
		}
		if len(rows) == 0 {
			t.Fatal("missing report breakdown")
		}
		var gift, revenue int64
		for _, row := range rows {
			if row.RegistrationGiftConsumption == nil || row.OperatingRevenue == nil {
				t.Fatal("complete gift evidence left a breakdown empty")
			}
			if _, err := financeAddMoney(&gift, *row.RegistrationGiftConsumption); err != nil {
				t.Fatal(err)
			}
			if _, err := financeAddMoney(&revenue, *row.OperatingRevenue); err != nil {
				t.Fatal(err)
			}
		}
		if economicsMoney(gift) != *total.RegistrationGiftConsumption || economicsMoney(revenue) != *total.OperatingRevenue {
			t.Fatal("daily/monthly gift deduction and revenue do not reconcile to the report total")
		}
	}
}
