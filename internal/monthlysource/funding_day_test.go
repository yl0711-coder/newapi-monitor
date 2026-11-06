package monthlysource

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFundingDayVersionBoundariesAndLegacyBytes(t *testing.T) {
	const from int64 = 1788192000
	pin := strings.Repeat("a", 64)
	day, err := NewFundingDayScan(pin, fundingTestRules(), []int64{202, 201}, from, from+86400)
	if err != nil || day.Version != 2 || !reflect.DeepEqual(day.UserIDs, []int64{201, 202}) {
		t.Fatal("explicit day scan", err)
	}
	for _, tc := range []struct {
		name       string
		v          int
		start, end int64
	}{
		{"legacy-day", 1, from, from + 86400},
		{"legacy-over-hour", 1, from, from + 3601},
		{"day-hour", 2, from, from + 3600},
		{"day-over", 2, from, from + 86401},
		{"day-short", 2, from, from + 86399},
		{"utc-not-beijing", 2, from + 28800, from + 115200},
		{"day-shifted", 2, from + 1, from + 86401},
		{"unknown-version", 3, from, from + 86400},
		{"reverse", 2, from + 86400, from},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := day
			bad.Version, bad.From, bad.To = tc.v, tc.start, tc.end
			if ValidateFundingScan(bad) == nil {
				t.Fatal("invalid version/window accepted")
			}
		})
	}
	if _, err := NewFundingScan(pin, fundingTestRules(), []int64{201}, from, from+86400); err == nil {
		t.Fatal("old constructor silently upgraded to days")
	}
	old, err := NewFundingScan(pin, fundingTestRules(), []int64{201}, from, from+3600)
	if err != nil || old.Version != 1 {
		t.Fatal("legacy hour changed", err)
	}
	raw, err := json.Marshal(old)
	if err != nil || !strings.HasPrefix(string(raw), `{"version":1,`) {
		t.Fatal("legacy wire format changed", err)
	}
	var loaded FundingScan
	if json.Unmarshal(raw, &loaded) != nil || ValidateFundingScan(loaded) != nil || !sameFundingScan(old, loaded) {
		t.Fatal("legacy round trip changed")
	}
}

func TestFundingDayCapturePaginationAndUnchangedLimits(t *testing.T) {
	db, d, old := fundingTestSource(t, 201)
	const from int64 = 1788192000
	scan, err := NewFundingDayScan(old.SourceIdentity, old.Rules, old.UserIDs, from, from+86400)
	if err != nil {
		t.Fatal(err)
	}
	for i := range d.s.data {
		d.s.data[i][2] = from + 86399 // include the last second, not next midnight
	}
	first, err := CaptureFundingPage(context.Background(), db, scan, nil)
	if err != nil || first.Scan.Version != 2 || first.Version != 1 || first.EndReached || len(first.Rows) != 200 {
		t.Fatal("daily first page truncated or format changed", err)
	}
	if len(d.s.queries) != 2 || !strings.Contains(d.s.queries[0], "@@server_uuid") || !strings.Contains(d.s.queries[0], "MAX(id)") ||
		!strings.Contains(d.s.queries[1], "MAX_EXECUTION_TIME(2000)") || !strings.Contains(d.s.queries[1], "LIMIT 201") ||
		d.s.args[1][0].Value != from || d.s.args[1][1].Value != from+86400 {
		t.Fatal("daily query relaxed timeout/row/range limits")
	}
	if !strings.Contains(d.s.queries[1], "UNION ALL") || strings.Count(d.s.queries[1], "FORCE INDEX") != 2 ||
		len(d.s.args[1]) != 14 || !reflect.DeepEqual(d.s.args[1][0:6], renumberFundingArgs(d.s.args[1][6:12])) ||
		d.s.args[1][12].Value != int64(201) || d.s.args[1][13].Value != int64(202) {
		t.Fatal("daily branches changed range/fence/cursor or account bindings")
	}
	d.s.data = d.s.data[200:]
	d.fence = 9000 // Continuation must NOT replace its first-page fence.
	d.s.queries, d.s.args = nil, nil
	second, err := CaptureFundingPage(context.Background(), db, scan, &first)
	if err != nil || !second.EndReached || len(second.Rows) != 1 || second.Rows[0].ID != 201 || second.After != first.Next || second.FenceID != first.FenceID {
		t.Fatal("daily same-second lookahead lost row", err)
	}
	if len(d.s.queries) != 2 || d.s.queries[0] != "SELECT @@server_uuid, DATABASE()" || d.s.args[1][2].Value != first.FenceID || d.s.args[1][8].Value != first.FenceID {
		t.Fatal("continuation repinned identity/fence or widened one branch")
	}
	local, _ := fundingTestStore(t)
	if _, err := SaveFundingPage(context.Background(), local, first); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveFundingPage(context.Background(), local, second); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadLatestFundingPage(context.Background(), local, scan.ID)
	if err != nil || !reflect.DeepEqual(loaded, second) {
		t.Fatal("day continuation not retained unchanged", err)
	}
	// A source returning the exclusive-end row is a capture failure, not a
	// silently filtered success or a complete empty day.
	d.s.data = [][]driver.Value{{int64(1), int64(201), scan.To, int64(4), "新用户注册赠送 1 点额度", ""}}
	if p, err := CaptureFundingPage(context.Background(), db, scan, nil); err == nil || p.Version != 0 {
		t.Fatal("exclusive-end row accepted as completed day")
	}
	if db.Stats().InUse != 0 {
		t.Fatal("daily transaction/connection leaked")
	}
}

func TestFundingDayCombinedMetadataFailureNeverReadsBodies(t *testing.T) {
	for _, kind := range []string{"wrong-source", "negative-fence", "metadata-error"} {
		t.Run(kind, func(t *testing.T) {
			db, d, old := fundingTestSource(t, 1)
			scan, err := NewFundingDayScan(old.SourceIdentity, old.Rules, old.UserIDs, 1788192000, 1788278400)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "wrong-source":
				d.s.uuid = "unexpected-other-source"
			case "negative-fence":
				d.fence = -1
			case "metadata-error":
				d.fenceErr = context.DeadlineExceeded
			}
			p, err := CaptureFundingPage(context.Background(), db, scan, nil)
			if err == nil || p.Version != 0 || len(d.s.queries) != 1 || !strings.Contains(d.s.queries[0], "MAX(id)") || db.Stats().InUse != 0 {
				t.Fatal("metadata failure read bodies, retained partial data or leaked source", err)
			}
		})
	}
}

func renumberFundingArgs(args []driver.NamedValue) []driver.NamedValue {
	copy := append([]driver.NamedValue(nil), args...)
	for i := range copy {
		copy[i].Ordinal = i + 1
	}
	return copy
}
