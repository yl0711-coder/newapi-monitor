package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// source_epoch must NOT be added to the filter just to use the composite PK:
// cross-epoch proofs are part of the existing integrity checks.
func TestFinanceGiftBoundaryRangePlanKeepsAllEpochs(t *testing.T) {
	m := newTestMonitor(t)
	// Reproduce an existing database from before this local index migration.
	if err := m.storeDB.Exec("DROP INDEX idx_finance_gift_state_hour_user").Error; err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []string{"old", "current"} {
		for i := 0; i < 3; i++ {
			if err := m.storeDB.Create(&FinanceGiftBoundaryState{SourceEpoch: epoch, HourTs: int64(3600 * i), UserID: 31, Status: "complete", Rows: int64(i + 1)}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	var before []FinanceGiftBoundaryState
	if err := m.storeDB.Where("hour_ts>=? AND hour_ts<? AND user_id IN ?", 3600, 10800, []int64{31}).Order("hour_ts,user_id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	planText := func(q string, args ...any) string {
		t.Helper()
		var plan []struct{ Detail string }
		if err := m.storeDB.Raw("EXPLAIN QUERY PLAN "+q, args...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		var text strings.Builder
		for _, p := range plan {
			text.WriteString(p.Detail)
			text.WriteByte('\n')
		}
		return text.String()
	}
	query := "SELECT * FROM finance_gift_boundary_states WHERE hour_ts>=? AND hour_ts<? AND user_id IN (?) ORDER BY hour_ts,user_id"
	legacyPlan := planText(query, 3600, 10800, 31)
	if !strings.Contains(legacyPlan, "SCAN finance_gift_boundary_states") {
		t.Fatalf("legacy schema did not reproduce the scan: %s", legacyPlan)
	}
	// Exercise the actual migration twice: existing rows survive and repeated
	// startup does not try to build duplicate indexes.
	for i := 0; i < 2; i++ {
		if err := m.storeDB.AutoMigrate(&FinanceGiftBoundaryState{}); err != nil {
			t.Fatal(err)
		}
	}
	after, err := loadFinanceGiftBoundaryStates(context.Background(), m.storeDB, 3600, 10800, []int64{31})
	if err != nil {
		t.Fatal(err)
	}
	// Same ordering ties as the pre-existing query; compare all identities and
	// payload rather than introducing a new source-epoch selection rule.
	keyed := func(rows []FinanceGiftBoundaryState) map[string]FinanceGiftBoundaryState {
		out := map[string]FinanceGiftBoundaryState{}
		for _, r := range rows {
			out[fmt.Sprintf("%s/%d/%d", r.SourceEpoch, r.HourTs, r.UserID)] = r
		}
		return out
	}
	if len(after) != 4 || !reflect.DeepEqual(keyed(before), keyed(after)) {
		t.Fatal("boundary query lost an epoch or changed proof values")
	}
	for _, q := range []string{
		query,
		"SELECT COUNT(*),MAX(updated_at),SUM(rows) FROM finance_gift_boundary_states WHERE hour_ts>=? AND hour_ts<? AND user_id IN (?)",
		"SELECT hour_ts,user_id,content_hash FROM finance_gift_boundary_states WHERE hour_ts>=? AND hour_ts<? AND user_id IN (?)",
	} {
		text := planText(q, 3600, 10800, 31)
		if !strings.Contains(text, "SEARCH finance_gift_boundary_states USING INDEX idx_finance_gift_state_hour_user") || strings.Contains(text, "SCAN finance_gift_boundary_states") {
			t.Fatalf("missing hour/user range index: %s", text)
		}
		t.Logf("legacy=%s after=%s", strings.TrimSpace(legacyPlan), strings.TrimSpace(text))
	}
}
