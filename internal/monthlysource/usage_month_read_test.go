package monthlysource

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestUsageMonthBatchKeepsOriginalsAndCumulativeBudget(t *testing.T) {
	db, _ := usageTestStore(t)
	r, from, to := fundingMonthFixture(t, "2026-09", []int64{201})
	versions := usageMonthDays(t, db, r, 201, from, to, false)
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	used := 123
	originals, err := loadUsageMonthBatch(t.Context(), tx, versions, &used)
	if err != nil || len(originals) != len(versions)*2 {
		t.Fatal("complete batch missing originals", len(originals), err)
	}
	expectedBytes := 123
	for _, v := range versions {
		for _, id := range []string{v.OlderID, v.NewerID} {
			raw, err := loadUsageWindow(t.Context(), tx, id)
			if err != nil {
				t.Fatal(err)
			}
			var expected UsageWindowObservation
			if err := json.Unmarshal(raw, &expected); err != nil || !reflect.DeepEqual(originals[id], expected) {
				t.Fatal("batch differs from the independently read original", id, err)
			}
			expectedBytes += len(raw)
		}
	}
	if used != expectedBytes {
		t.Fatal("batch reset or double-counted the cumulative byte budget", used, expectedBytes)
	}
	for _, scenario := range []string{"bytes", "duplicate", "missing", "batch_limit"} {
		t.Run(scenario, func(t *testing.T) {
			input, bytesUsed := versions[:1], 0
			switch scenario {
			case "bytes":
				bytesUsed = usageMonthBytesLimit - 1
			case "duplicate":
				input = []UsageWindowVersion{{OlderID: versions[0].OlderID, NewerID: versions[0].OlderID}}
			case "missing":
				input = []UsageWindowVersion{{OlderID: versions[0].OlderID, NewerID: strings.Repeat("a", 64)}}
			case "batch_limit":
				input = make([]UsageWindowVersion, 65)
			}
			out, err := loadUsageMonthBatch(t.Context(), tx, input, &bytesUsed)
			if err == nil || out != nil {
				t.Fatal("invalid batch returned complete/partial observations", scenario, err)
			}
		})
	}
}
