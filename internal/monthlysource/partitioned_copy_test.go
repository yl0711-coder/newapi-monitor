package monthlysource

import (
	"encoding/json"
	"testing"
)

func TestPartitionedCopyRollbackKeepsNoPartialUsageRows(t *testing.T) {
	source, _, key, scope := capturedSelectionFixture(t, false)
	a, err := BuildPartitionedCapturedMonthSelection(t.Context(), source, key, scope, false)
	if err != nil {
		t.Fatal(err)
	}
	var s CapturedMonthSelection
	if err := json.Unmarshal(a.Selection, &s); err != nil {
		t.Fatal(err)
	}
	destination, _ := usageTestStore(t)
	if _, err := destination.Exec(`CREATE TRIGGER synthetic_copy_failure BEFORE INSERT ON portal_monthly_usage_observations WHEN (SELECT count(*) FROM portal_monthly_usage_observations)>0 BEGIN SELECT RAISE(ABORT,'synthetic copy failure'); END`); err != nil {
		t.Fatal(err)
	}
	changes := adaptiveTokenCount(t, source, `SELECT total_changes()`)
	if err := copyPartitionedUsageRows(t.Context(), source, destination, s); err == nil {
		t.Fatal("partial copy reported success")
	}
	if adaptiveTokenCount(t, destination, `SELECT count(*) FROM portal_monthly_usage_observations`) != 0 || destination.Stats().InUse != 0 || adaptiveTokenCount(t, source, `SELECT total_changes()`) != changes {
		t.Fatal("copy partially committed, leaked a connection or wrote original source")
	}
}
