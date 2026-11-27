package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func diagnosticFixture() []struct {
	typ            int
	content, other string
} {
	return []struct {
		typ            int
		content, other string
	}{
		{2, "ok", `{}`},
		{2, "", `{"admin_info":{"reject_reason":"claude_stop_reason=refusal"}}`},
		{5, "status_code=400, Invalid value: 'ultra'", `{}`},
		{2, "ok", `{"reasoning_effort":"high"}`},
		{2, "", `{"reject_reason":"legacy blocked","admin_info":null}`},
		{2, "", `{"reject_reason":"legacy must not win","admin_info":{"reject_reason":null}}`},
		{2, "", `{invalid`},
		{2, "", `{"admin_info":{"reject_reason":" \t\r\n"}}`},
		{2, "", `{"reject_reason":true}`},
		{2, "", `null`},
		{6, "refund", `{"admin_info":{"reject_reason":"refund note"}}`},
		{2, "", `{"admin_info":{"reject_reason":"literal 50%_! <script>"}}`},
		{2, "", `{"reject_reason":"legacy available","admin_info":"legacy-format"}`},
		{2, "", `{"reject_reason":"legacy must not win","admin_info":{"reject_reason":false}}`},
	}
}

func TestUsageLogsDiagnosticDeferredPageCannotBypassHeavyAdmission(t *testing.T) {
	db, counts := newCountedUsageLogsDB(t)
	m := &Monitor{prodDB: db}
	m.initUsageLogsGates()
	m.usageLogsBackgroundGate <- struct{}{}
	defer func() { <-m.usageLogsBackgroundGate }()
	for _, params := range []string{"diagnostic_scope=issues", "diagnostic_scope=rejected", "message=refusal", "diagnostic_scope=issues&bounded_search=1"} {
		counts.reset()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/list?defer_total=1&start_timestamp=99&end_timestamp=120&"+params, nil).WithContext(ctx)
		m.serveUsageLogsList(c)
		cancel()
		if counts.logs.Load() != 0 || !strings.Contains(w.Body.String(), `"success":false`) || len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 1 {
			t.Fatalf("diagnostic scan bypassed admission/leaked a slot: %s %s", params, w.Body.String())
		}
	}
}

func TestUsageLogsDiagnosticListCountAndScope(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	for i, r := range diagnosticFixture() {
		if _, err := db.Exec("INSERT INTO logs(id,user_id,created_at,type,content,username,quota,channel_id,other) VALUES(?,31,?,?,?,'fixture',10,7,?)", i+1, 100+i/2, r.typ, r.content, r.other); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO logs(id,created_at,type,other) VALUES(99,1000,2,'{"reject_reason":"out-of-range"}')`); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{prodDB: db}
	read := func(q url.Values) usageLogsListResp {
		t.Helper()
		q.Set("start_timestamp", "99")
		q.Set("end_timestamp", "120")
		q.Set("page_size", "100")
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+q.Encode(), nil)
		var response usageLogsListResp
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || !response.Success {
			t.Fatal(w.Body.String(), err)
		}
		return response
	}
	for _, tc := range []struct {
		scope, message, typ string
		want                []int64
	}{
		{"issues", "", "", []int64{13, 12, 11, 5, 3, 2}},
		{"rejected", "", "", []int64{13, 12, 11, 5, 2}},
		{"rejected", "", "2", []int64{13, 12, 5, 2}},
		{"", "invalid value", "5", []int64{3}},
		{"issues", "refusal", "", []int64{2}},
		{"", "50%_!", "", []int64{12}},
		{"", "no-match", "", []int64{}},
	} {
		q := url.Values{"diagnostic_scope": {tc.scope}, "message": {tc.message}, "type": {tc.typ}}
		r := read(q)
		got := []int64{}
		for _, it := range r.Data.Items {
			got = append(got, it.ID)
			if it.ID == 2 && (it.Type != 2 || it.Quota != 10) {
				t.Fatal("refusal changed billing/type")
			}
		}
		if !reflect.DeepEqual(got, tc.want) || r.Data.Total != int64(len(tc.want)) {
			t.Fatal(tc, got, r.Data.Total)
		}
		w := usageLogsGET(t, m, m.serveUsageLogsCount, "/usage-logs/count?"+q.Encode(), nil)
		var count struct {
			Success bool `json:"success"`
			Data    struct {
				Total int64 `json:"total"`
			} `json:"data"`
		}
		if json.Unmarshal(w.Body.Bytes(), &count) != nil || !count.Success || count.Data.Total != r.Data.Total {
			t.Fatal("count/list scope mismatch", w.Body.String())
		}
	}
	// A refusal filter is not a new consume/refund rule. Statistics ignore it.
	w := usageLogsGET(t, m, m.serveUsageLogsStat, "/usage-logs/stat?start_timestamp=99&end_timestamp=120&quota_only=1&diagnostic_scope=rejected&message=no-match", nil)
	var stats struct {
		Success bool `json:"success"`
		Data    struct {
			Quota int64 `json:"quota"`
		} `json:"data"`
	}
	if json.Unmarshal(w.Body.Bytes(), &stats) != nil || !stats.Success || stats.Data.Quota != 120 {
		t.Fatal("diagnostics changed existing consume quota", w.Body.String())
	}
	// Full-result filtering, not client-side filtering of a page. Walk tie IDs.
	var ids []int64
	var cursor *usageLogCursor
	for page := 1; page < 6; page++ {
		q := url.Values{"start_timestamp": {"99"}, "end_timestamp": {"120"}, "diagnostic_scope": {"issues"}, "page_size": {"2"}, "p": {fmt.Sprint(page)}, "defer_total": {"1"}}
		if cursor != nil {
			q.Set("before_created_at", fmt.Sprint(cursor.CreatedAt))
			q.Set("before_id", fmt.Sprint(cursor.ID))
		}
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+q.Encode(), nil)
		var r usageLogsListResp
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || !r.Success {
			t.Fatal(w.Body.String())
		}
		for _, it := range r.Data.Items {
			ids = append(ids, it.ID)
		}
		if !r.Data.HasMore {
			break
		}
		cursor = r.Data.Next
	}
	if fmt.Sprint(ids) != "[13 12 11 5 3 2]" {
		t.Fatal("diagnostic cursor lost/duplicated rows", ids)
	}
}

func TestUsageLogsDiagnosticValidationAndNativePlanning(t *testing.T) {
	for _, f := range []usageLogFilter{{DiagnosticScope: "bad", Start: 1, End: 2}, {DiagnosticScope: "issues"}, {Message: "x", Start: 1, End: 26*3600 + 2}, {Message: strings.Repeat("x", 129), Start: 1, End: 2}} {
		if _, err := buildUsageLogWhere(f, usageLogWhereList); err == nil {
			t.Fatal("unsafe/unknown diagnostic scope accepted", f)
		}
	}
	f := usageLogFilter{Start: 1, End: 26*3600 + 1, Username: "fixture", Channel: 7, DiagnosticScope: "issues", Message: "%_!' OR 1=1 --"}
	w, err := buildUsageLogWhere(f, usageLogWhereList)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(w.sql(), "OR 1=1 --") || !strings.Contains(w.sql(), "JSON_VALID") {
		t.Fatal("message not parameterized or JSON not guarded")
	}
	if usageLogsUseCountSpan(f) || usageLogsUseQuotaSpan(f) {
		t.Fatal("text diagnostics entered aggregate probe path")
	}
	plain := f
	plain.DiagnosticScope = ""
	plain.Message = ""
	for _, scope := range []usageLogWhereScope{usageLogWhereStat, usageLogWhereRate} {
		a, _ := buildUsageLogWhere(f, scope)
		b, _ := buildUsageLogWhere(plain, scope)
		if a.sql() != b.sql() || !reflect.DeepEqual(a.args, b.args) {
			t.Fatal("statistics scope changed")
		}
	}
	// Rejected ranges fail before trying to connect to any source.
	m := &Monitor{}
	reply := usageLogsGET(t, m, m.serveUsageLogsCount, "/usage-logs/count?diagnostic_scope=issues", nil)
	if !strings.Contains(reply.Body.String(), "26小时") {
		t.Fatal(reply.Body.String())
	}
}
