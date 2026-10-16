package monitor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPortalCustomerLocalRequestIDOnly(t *testing.T) {
	original := LogRow{RequestID: "local-request", UpstreamRequestID: "internal-upstream-request", RequestPath: "/v1/private-admin-path", SourceFieldNotice: "IP / 上游请求 ID 字段暂未取得，不影响原始用量和费用。"}
	customer := portalCustomerLogRow(original)
	if customer.RequestID != original.RequestID || customer.UpstreamRequestID != "" || customer.RequestPath != "" || strings.Contains(customer.SourceFieldNotice, "上游") {
		t.Fatal("customer row must use only the local correlation ID")
	}
	if original.UpstreamRequestID != "internal-upstream-request" || original.RequestPath != "/v1/private-admin-path" || !strings.Contains(original.SourceFieldNotice, "上游") {
		t.Fatal("customer projection mutated the internal source row")
	}
	encoded, err := json.Marshal(customer)
	if err != nil || strings.Contains(string(encoded), "upstream_request_id") || strings.Contains(string(encoded), original.UpstreamRequestID) || strings.Contains(string(encoded), "request_path") || strings.Contains(string(encoded), original.RequestPath) {
		t.Fatal("upstream correlation published in customer JSON")
	}
	for _, cell := range portalCSVRecord(original) {
		if strings.Contains(cell, original.UpstreamRequestID) || strings.Contains(cell, "上游请求 ID") || strings.Contains(cell, original.RequestPath) {
			t.Fatal("upstream correlation published in customer CSV")
		}
	}
}

func TestPortalSubscriptionOnlyRecordedFields(t *testing.T) {
	for _, tc := range []struct{ raw, settlement, remain string }{
		{`{"billing_source":"subscription"}`, "", ""},
		{`{"billing_source":"subscription","subscription_pre_consumed":100,"subscription_post_delta":-10}`, "预扣：100 额度\n结算差额：-10 额度", ""},
		{`{"billing_source":"subscription","subscription_consumed":0,"subscription_remain":0}`, "最终抵扣：0 额度", "0 额度"},
		{`{"billing_source":"subscription","subscription_remain":10,"subscription_total":100}`, "", "10/100 额度"},
		{`{"billing_source":"subscription","subscription_post_delta":0}`, "", ""},
		{`{"billing_source":"subscription","subscription_consumed":"invalid"}`, "", ""},
		{`{"billing_source":"subscription","subscription_consumed":100,"subscription_consumed":0}`, "", ""},
	} {
		r := LogRow{Type: 2, CostUSD: .15, PromptTokens: 100, CompletionTokens: 10, billingQuota: 75000}
		populateExpandFields(&r, parseLogOther(tc.raw), "")
		populatePortalLogEvidence(&r, tc.raw)
		if r.SubSettlement != tc.settlement || r.SubRemain != tc.remain {
			t.Fatalf("%s: settlement=%q remain=%q", tc.raw, r.SubSettlement, r.SubRemain)
		}
		if r.CostUSD != .15 || r.billingQuota != 75000 || r.PromptTokens != 100 {
			t.Fatal("display changed original charge or counts")
		}
		if !strings.Contains(portalCSVRecord(r)[portalCSVSupplementColumn], tc.settlement) {
			t.Fatal("subscription export differs")
		}
	}
}

func TestPortalToolSurchargeOfficialMarker(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{`{}`, false},
		{`{"tool_surcharges":[{"name":"web_search","count":1,"price":10}]}`, true},
		{`{"tool_surcharges":[{"name":"web_search","count":0,"price":10}]}`, false},
		{`{"tool_surcharges":[{"name":"","count":1,"price":10}]}`, false},
		{`{"web_search":true,"web_search_call_count":1,"web_search_price":10}`, true},
		{`{"web_search_call_count":1,"web_search_price":10}`, false},
		{`{"file_search":true,"file_search_call_count":1,"file_search_price":10}`, true},
		{`{"image_generation_call":true,"image_generation_call_price":1}`, true},
		{`{"image_generation_call":true,"image_generation_call_price":"1"}`, false},
	} {
		r := LogRow{Type: 2, CostUSD: .15}
		populatePortalLogEvidence(&r, tc.raw)
		if r.HasToolSurcharge != tc.want || r.CostUSD != .15 {
			t.Fatalf("marker=%v want=%v source=%s", r.HasToolSurcharge, tc.want, tc.raw)
		}
	}
}

func TestPortalRefundLoginAndOptionalFieldsEndToEnd(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	for _, column := range []string{"ip TEXT", "upstream_request_id TEXT"} {
		if _, err := m.prodDB.Exec("ALTER TABLE logs ADD COLUMN " + column); err != nil {
			t.Fatal(err)
		}
	}
	g := CustomerGroup{Name: "local-owner", PortalEmail: "local-owner@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "local-owner", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 10, 14, 12, 0, 0, 0, usageCST).Unix()
	login := `{"login_method":"password","user_agent":"Local acceptance browser","op":{"action":"login","params":{"method":"password"}},"admin_info":{"secret":"do-not-export"},"audit_info":{"secret":"do-not-export"}}`
	for i, typ := range []int{2, 6, 7, 3} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,other,request_id,ip,upstream_request_id) VALUES(?,101,?,?,75000,100,10,?,?,'192.0.2.1','up-local')`, i+1, stamp, typ, login, "req-local"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,other,ip) VALUES(99,202,?,7,999,?,'192.0.2.99')`, stamp, login); err != nil {
		t.Fatal(err)
	}
	ck := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	query := "from=2026-10-14&to=2026-10-14&type=7"
	w := portalDo(portal, http.MethodGet, "/api/logs?"+query, "", ck)
	var result struct {
		Rows []LogRow `json:"rows"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Rows) != 1 || result.Rows[0].Type != 7 || result.Rows[0].ID != 3 {
		t.Fatalf("login filter/scope: %d %s", w.Code, w.Body.String())
	}
	r := result.Rows[0]
	if r.Detail != "登录成功（通过 password）" || r.IP != "192.0.2.1" || r.UpstreamRequestID != "" || r.RequestID != "req-local" || len(r.RequestDetails) != 2 || strings.Contains(w.Body.String(), "upstream_request_id") || strings.Contains(w.Body.String(), "up-local") {
		t.Fatalf("login display incomplete: %+v", r)
	}
	prepared := portalDo(portal, http.MethodGet, "/api/logs/export/prepare?"+query, "", ck)
	var prep struct {
		Ticket string `json:"ticket"`
	}
	if prepared.Code != 200 || json.Unmarshal(prepared.Body.Bytes(), &prep) != nil || prep.Ticket == "" {
		t.Fatal("prepare failed")
	}
	download := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+url.QueryEscape(prep.Ticket), "", ck)
	records, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(download.Body.String(), "\ufeff"))).ReadAll()
	if download.Code != 200 || err != nil || len(records) != 2 || records[1][4] != "登录" || records[1][9] != "" || !strings.Contains(records[1][portalCSVSupplementColumn], "Local acceptance browser") {
		t.Fatalf("login CSV differs: status=%d records=%v err=%v", download.Code, records, err)
	}
	if strings.Contains(download.Body.String(), "192.0.2.1") || strings.Contains(download.Body.String(), "IP 地址") || strings.Contains(download.Body.String(), "up-local") || strings.Contains(download.Body.String(), "上游 Request ID") || strings.Contains(download.Body.String(), "请求路径") || len(records[0]) != 21 || len(records[1]) != 21 || records[1][11] != "req-local" {
		t.Fatal("customer CSV must omit IP while preserving column alignment")
	}
	var sourceID string
	if err := m.prodDB.QueryRow("SELECT upstream_request_id FROM logs WHERE id=3").Scan(&sourceID); err != nil || sourceID != "up-local" {
		t.Fatal("customer projection modified source upstream ID")
	}
	for _, data := range []string{w.Body.String(), download.Body.String()} {
		if strings.Contains(data, "do-not-export") || strings.Contains(data, "192.0.2.99") {
			t.Fatal("customer scope or admin field leaked")
		}
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{101}, stamp-1, stamp+1, 0, 6, "", "", "", "", "", 0, 10)
	if err != nil || len(rows) != 1 || rows[0].CostUSD != .15 || portalCSVRecord(rows[0])[9] != "0.150000" || rows[0].IP != "" {
		t.Fatalf("refund original amount/visibility: %v %v", rows, err)
	}
	var quota int64
	if err := m.prodDB.QueryRow("SELECT quota FROM logs WHERE id=2").Scan(&quota); err != nil || quota != 75000 {
		t.Fatal("source quota changed")
	}
}

func TestPortalOptionalColumnsOldTableAndRefresh(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	first := m.portalLogOptionalColumns(context.Background())
	if first.ip || first.upstreamID || first.unavailable {
		t.Fatal("old schema must be valid")
	}
	if _, err := m.prodDB.Exec("ALTER TABLE logs ADD COLUMN upstream_request_id TEXT"); err != nil {
		t.Fatal(err)
	}
	if m.portalLogOptionalColumns(context.Background()).upstreamID {
		t.Fatal("schema metadata not cached")
	}
	m.portalLogColumns.expires = time.Time{}
	if !m.portalLogOptionalColumns(context.Background()).upstreamID {
		t.Fatal("in-place schema upgrade not discovered")
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,upstream_request_id) VALUES(1,101,1000,2,75000,'up-one')`); err != nil {
		t.Fatal(err)
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{101}, 0, 2000, 0, 0, "", "", "", "", "", 0, 10)
	if err != nil || len(rows) != 1 || rows[0].UpstreamRequestID != "up-one" || rows[0].IP != "" {
		t.Fatalf("partial optional schema: %v %v", rows, err)
	}
}

func TestPortalStreamErrorsRedactionAndLimits(t *testing.T) {
	for _, value := range []string{
		`Authorization: Bearer local-only-secret`, `{"api_key":"local-only-secret"}`, `access_token=local-only-secret`,
		`{"instructions":"local-only-secret"}`, `request body: local-only-secret`, `https://up.invalid/local-only-secret`,
	} {
		if strings.Contains(portalCustomerErrorText(value), "local-only-secret") {
			t.Fatalf("sensitive error retained: %s", value)
		}
	}
	if portalCustomerErrorText("context canceled") != "context canceled" {
		t.Fatal("plain upstream error changed")
	}
	if len([]rune(portalCustomerErrorText(strings.Repeat("长", 2000)))) > 1040 {
		t.Fatal("unbounded error")
	}
}

func TestPortalOptionalColumnsFailureIsBoundedAndRecoverable(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("ALTER TABLE logs ADD COLUMN ip TEXT"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,ip) VALUES(1,101,1000,2,75000,'192.0.2.1')`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if columns := m.portalLogOptionalColumns(ctx); !columns.unavailable || columns.ip {
		t.Fatal("failed metadata became valid evidence")
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{101}, 0, 2000, 0, 0, "", "", "", "", "", 0, 10)
	if err != nil || len(rows) != 1 || rows[0].CostUSD != .15 || rows[0].SourceFieldNotice == "" {
		t.Fatalf("optional failure broke base facts: %v %v", rows, err)
	}
	m.portalLogColumns.expires = time.Time{}
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if columns := m.portalLogOptionalColumns(context.Background()); !columns.ip || columns.unavailable {
				t.Error("discovery did not recover")
			}
		}()
	}
	wg.Wait()
}
