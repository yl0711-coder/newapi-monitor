package monitor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Mixed source generations and malformed rows must remain isolated within one
// real handler response/export. Source facts are authoritative even on mismatch.
func TestPortalUsageMixedLogExportAcceptance(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	g := CustomerGroup{Name: "mixed-local", PortalEmail: "mixed@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "synthetic", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	fixtures := []struct {
		typ        int
		quota      int64
		raw, state string
	}{
		{2, 110, `{"model_ratio":1,"group_ratio":1,"completion_ratio":1}`, "matched"},
		{2, 111, `{"model_ratio":1,"group_ratio":1,"completion_ratio":1}`, "mismatch"},
		{2, 123, `{"model_ratio":`, "incomplete"},
		{2, 124, ``, "incomplete"},
		{2, 125, `{"audio":true,"audio_input":20,"audio_output":0}`, "unsupported"},
		{2, 126, `{"tool_surcharges":[{"name":"private-tool","count":1,"price":10}]}`, "unsupported"},
		{5, 127, `{"frt":0,"stream_status":{"status":"error","end_reason":"client_gone","end_error":"Authorization: Bearer private-stream-error"}}`, ""},
		{6, 128, `{"task_id":"local-task","reason":"local-refund"}`, ""},
	}
	created := time.Date(2026, 10, 14, 12, 0, 0, 0, usageCST).Unix()
	for i, f := range fixtures {
		_, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,is_stream,other,request_id) VALUES(?,101,?,?,'mixed-model',?,100,10,1,?,?)`, i+1, created+int64(i), f.typ, f.quota, f.raw, fmt.Sprintf("mixed-%d", i+1))
		if err != nil {
			t.Fatal(err)
		}
	}
	ck := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	query := "from=2026-10-14&to=2026-10-14"
	page := portalDo(portal, http.MethodGet, "/api/logs?"+query, "", ck)
	var result struct {
		Rows []LogRow `json:"rows"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &result) != nil || len(result.Rows) != len(fixtures) {
		t.Fatalf("mixed page: %d %s", page.Code, page.Body.String())
	}
	byRequest := make(map[string]LogRow)
	for _, r := range result.Rows {
		f := fixtures[r.ID-1]
		if r.PromptTokens != 100 || r.CompletionTokens != 10 {
			t.Fatalf("tokens changed: %d", r.ID)
		}
		if f.typ == 2 {
			if r.CostUSD != float64(f.quota)/quotaPerUSD || r.UsageDetails == nil || r.UsageDetails.Billing.State != f.state {
				t.Fatalf("wrong original cost or evidence state: %+v", r)
			}
			if f.state != "matched" && r.BillingProcess != "" {
				t.Fatalf("unverified formula: %d", r.ID)
			}
		} else if r.UsageDetails != nil {
			t.Fatalf("non-consumption evidence: %d", r.ID)
		}
		byRequest[r.RequestID] = r
	}
	prepared := portalDo(portal, http.MethodGet, "/api/logs/export/prepare?"+query, "", ck)
	var prep struct {
		Ticket string `json:"ticket"`
	}
	if prepared.Code != 200 || json.Unmarshal(prepared.Body.Bytes(), &prep) != nil || prep.Ticket == "" {
		t.Fatal("mixed export prepare failed")
	}
	download := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+url.QueryEscape(prep.Ticket), "", ck)
	if download.Code != 200 {
		t.Fatalf("mixed export: %d", download.Code)
	}
	records, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(download.Body.String(), "\ufeff"))).ReadAll()
	if err != nil || len(records) != len(fixtures)+1 {
		t.Fatalf("mixed CSV: rows=%d err=%v", len(records), err)
	}
	seen := make(map[string]bool)
	for _, record := range records[1:] {
		if len(record) != portalCSVColumnCount {
			t.Fatal("CSV width changed")
		}
		r, ok := byRequest[record[11]]
		if !ok || seen[record[11]] {
			t.Fatal("unknown or duplicate export row")
		}
		seen[record[11]] = true
		f := fixtures[r.ID-1]
		wantCost := ""
		if portalLogHasRecordedAmount(f.typ) {
			wantCost = strconv.FormatFloat(float64(f.quota)/quotaPerUSD, 'f', 6, 64)
		}
		if record[7] != "100" || record[8] != "10" || record[9] != wantCost || record[portalCSVBillingColumn] != portalCSVReadableBilling(r) {
			t.Fatalf("CSV facts differ: %v", record)
		}
	}
	for _, output := range []string{page.Body.String(), download.Body.String()} {
		if strings.Contains(output, "private-tool") || strings.Contains(output, "private-stream-error") {
			t.Fatal("private field leaked")
		}
	}
	for i, f := range fixtures {
		var raw string
		var q, p, c int64
		if err := m.prodDB.QueryRow("SELECT quota,prompt_tokens,completion_tokens,other FROM logs WHERE id=?", i+1).Scan(&q, &p, &c, &raw); err != nil || q != f.quota || p != 100 || c != 10 || raw != f.raw {
			t.Fatalf("source row %d modified: %v", i+1, err)
		}
	}
}

// End-to-end handler regression: the same customer scope and model filter
// must drive the page data, export preflight and downloaded billing details.
func TestPortalUsageLogFilterExportAcceptance(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	g := CustomerGroup{Name: "synthetic", PortalEmail: "local@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "synthetic", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	raw := `{"model_ratio":1,"group_ratio":1,"completion_ratio":4,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25,"request_path":"/v1/responses","frt":0,"stream_status":{"status":"ok","end_reason":"done","error_count":0,"end_error":"private-error-must-not-leak"}}`
	createdAt := time.Date(2026, 10, 14, 12, 0, 0, 0, usageCST).Unix()
	for _, row := range []struct {
		id, uid int
		model   string
	}{{1, 101, "cache-model"}, {2, 101, "other-model"}, {3, 202, "cache-model"}} {
		_, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,is_stream,other,request_id) VALUES(?,?,?,2,?,132,100,10,1,?,?)", row.id, row.uid, createdAt, row.model, raw, "synthetic-request")
		if err != nil {
			t.Fatal(err)
		}
	}
	ck := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	query := "from=2026-10-14&to=2026-10-14&model=cache-model"
	page := portalDo(portal, http.MethodGet, "/api/logs?"+query, "", ck)
	var listed struct {
		OK   bool     `json:"ok"`
		Rows []LogRow `json:"rows"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &listed) != nil || !listed.OK || len(listed.Rows) != 1 || listed.Rows[0].ID != 1 {
		t.Fatalf("wrong scope/filter: %d %s", page.Code, page.Body.String())
	}
	if listed.Rows[0].UsageDetails.Billing.State != "matched" {
		t.Fatal("cache explanation not verified")
	}
	if strings.Contains(page.Body.String(), "private-error-must-not-leak") {
		t.Fatal("private raw stream detail leaked")
	}
	denied := portalDo(portal, http.MethodGet, "/api/logs?"+query+"&member=202", "", ck)
	if denied.Code == 200 {
		t.Fatal("cross-customer selection allowed")
	}
	prepared := portalDo(portal, http.MethodGet, "/api/logs/export/prepare?"+query, "", ck)
	var prep struct {
		OK     bool   `json:"ok"`
		Ticket string `json:"ticket"`
		Total  int64  `json:"total"`
	}
	if prepared.Code != 200 || json.Unmarshal(prepared.Body.Bytes(), &prep) != nil || !prep.OK || prep.Total != 1 || prep.Ticket == "" {
		t.Fatalf("wrong preflight: %d %s", prepared.Code, prepared.Body.String())
	}
	// The signed snapshot/filter cannot be widened by download query fields.
	download := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+url.QueryEscape(prep.Ticket)+"&model=other-model&member=202", "", ck)
	if download.Code != 200 {
		t.Fatalf("download: %d %s", download.Code, download.Body.String())
	}
	records, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(download.Body.String(), "\ufeff"))).ReadAll()
	if err != nil || len(records) != 2 || len(records[0]) != portalCSVColumnCount || len(records[1]) != portalCSVColumnCount {
		t.Fatalf("CSV shape: %v", err)
	}
	r := records[1]
	if r[5] != "cache-model" || r[7] != "100" || r[8] != "10" || r[9] != "0.000264" || r[12] != "20" || r[13] != "40" {
		t.Fatalf("CSV facts altered: %v", r)
	}
	if r[portalCSVBillingColumn] != portalCSVReadableBilling(listed.Rows[0]) || r[portalCSVFRTColumn] != "" || listed.Rows[0].RequestPath != "" || strings.Contains(strings.Join(r, "\n"), "/v1/responses") || r[portalCSVModeColumn] != "流式" {
		t.Fatalf("CSV/page detail mismatch: %v", r)
	}
	if strings.Contains(download.Body.String(), "private-error-must-not-leak") {
		t.Fatal("private details leaked to download")
	}
	var quota int64
	if err := m.prodDB.QueryRow("SELECT quota FROM logs WHERE id=1").Scan(&quota); err != nil || quota != 132 {
		t.Fatal("read-only explanation changed source charge")
	}
}
