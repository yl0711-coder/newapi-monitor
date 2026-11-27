package monitor

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestUsageLogsStatSplitPreservesValuesAndQueryCount(t *testing.T) {
	db, counts := newCountedUsageLogsDB(t)
	now := time.Now().Unix()
	// The counting fixture has a different column order; always name columns.
	if _, err := db.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens)
	 VALUES (1,31,?,2,100,10,5),(2,31,?,2,200,1,1),(3,31,?,5,999,99,99)`, now-20, now-7200, now-10); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{prodDB: db}
	type data struct {
		Quota, RPM, TPM *int64
		QuotaSkipped    bool `json:"quota_skipped"`
		RatesSkipped    bool `json:"rates_skipped"`
	}
	get := func(mode string) (data, int64) {
		t.Helper()
		counts.reset()
		path := "/usage-logs/stat?start_timestamp=" + strconv.FormatInt(now-86400, 10) + "&type=5&request_id=absent&upstream_request_id=absent" + mode
		w := usageLogsGET(t, m, m.serveUsageLogsStat, path, nil)
		var r struct {
			Success bool
			Data    data
		}
		if e := json.Unmarshal(w.Body.Bytes(), &r); e != nil || !r.Success {
			t.Fatalf("%s: %s", mode, w.Body.String())
		}
		return r.Data, counts.logs.Load()
	}
	full, n := get("")
	if n != 2 || full.Quota == nil || *full.Quota != 300 || *full.RPM != 1 || *full.TPM != 15 {
		t.Fatalf("legacy %+v queries=%d", full, n)
	}
	quota, nq := get("&quota_only=1")
	rates, nr := get("&skip_quota=1")
	if nq != 1 || nr != 1 || nq+nr != n {
		t.Fatalf("split increased aggregation scans: %d + %d vs %d", nq, nr, n)
	}
	if quota.Quota == nil || *quota.Quota != *full.Quota || !quota.RatesSkipped || quota.RPM != nil || quota.TPM != nil || quota.QuotaSkipped {
		t.Fatalf("quota %+v", quota)
	}
	if rates.Quota != nil || !rates.QuotaSkipped || *rates.RPM != *full.RPM || *rates.TPM != *full.TPM {
		t.Fatalf("rates %+v", rates)
	}
	counts.reset()
	w := usageLogsGET(t, m, m.serveUsageLogsStat, "/usage-logs/stat?skip_quota=1&quota_only=1", nil)
	if !strings.Contains(w.Body.String(), `"success":false`) || counts.logs.Load() != 0 {
		t.Fatal("contradictory modes must fail before SQL")
	}
	if len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 0 {
		t.Fatal("split leaked query admission")
	}
}

func TestUsageLogsStatQuotaOnlyEmptyIsExactZeroNotMissingRates(t *testing.T) {
	m := &Monitor{prodDB: newUsageLogsFakeProd(t)}
	w := usageLogsGET(t, m, m.serveUsageLogsStat, "/usage-logs/stat?quota_only=1&start_timestamp=100&end_timestamp=200", nil)
	var r struct {
		Success bool
		Data    struct {
			Quota    *int64
			RPM, TPM *int64
			Skipped  bool `json:"rates_skipped"`
		}
	}
	if e := json.Unmarshal(w.Body.Bytes(), &r); e != nil || !r.Success || r.Data.Quota == nil || *r.Data.Quota != 0 || r.Data.RPM != nil || r.Data.TPM != nil || !r.Data.Skipped {
		t.Fatalf("%s", w.Body.String())
	}
}
