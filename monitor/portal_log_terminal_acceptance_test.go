package monitor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Exercise the public customer protocol over real local TLS rather than only
// calling projection helpers. All identities and source rows are synthetic.
func TestPortalCustomerLogsTLSBlackBoxAcceptance(t *testing.T) {
	m, _, router := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	for _, column := range []string{"ip TEXT", "upstream_request_id TEXT"} {
		if _, err := m.prodDB.Exec("ALTER TABLE logs ADD COLUMN " + column); err != nil {
			t.Fatal(err)
		}
	}
	password := "local-blackbox-password"
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	groups := []CustomerGroup{{Name: "Owner", PortalEmail: "owner@example.invalid", PortalPwAdmin: hash}, {Name: "Foreign", PortalEmail: "foreign@example.invalid", PortalPwAdmin: hash}}
	for i := range groups {
		if err := m.storeDB.Create(&groups[i]).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&TrackedUser{UserID: int64(101 + i), Username: groups[i].Name, GroupID: groups[i].ID}).Error; err != nil {
			t.Fatal(err)
		}
	}
	stamp := time.Date(2026, 10, 14, 12, 0, 0, 0, usageCST).Unix()
	raw := `{"model_ratio":1,"completion_ratio":1,"group_ratio":1,"request_path":"/v1/PRIVATE_ADMIN_PATH","admin_info":{"secret":"PRIVATE_ADMIN_MARKER","use_channel":[99,100]},"audit_info":{"secret":"PRIVATE_AUDIT_MARKER"},"reject_reason":"PRIVATE_REJECT_MARKER","request_conversion":["PRIVATE_CONVERSION_MARKER"],"channel_name":"PRIVATE_CHANNEL_MARKER","api_key":"PRIVATE_KEY_MARKER"}`
	for _, typ := range []int{1, 2, 3, 4, 5, 6, 7} {
		_, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,model_name,content,other,request_id,ip,upstream_request_id) VALUES(?,101,?,?,110,100,10,'Owner','owner-model','customer-visible-content',?,?,'192.0.2.101','PRIVATE_UPSTREAM_MARKER')`, typ, stamp+int64(typ), typ, raw, fmt.Sprintf("owner-%d", typ))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,username,model_name,content,request_id) VALUES(99,102,?,2,900,'PRIVATE_FOREIGN_MARKER','PRIVATE_FOREIGN_MODEL','PRIVATE_FOREIGN_CONTENT','foreign-request')`, stamp+99); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(router)
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	request := func(method, path, body string, cookie *http.Cookie) (int, string, *http.Response) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Accept", "application/json")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if cookie != nil {
			req.AddCookie(cookie)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, string(data), response
	}
	login := func(email string) *http.Cookie {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"email": email, "password": password})
		status, _, response := request("POST", "/login", string(body), nil)
		if status != 200 {
			t.Fatalf("local login status=%d", status)
		}
		for _, cookie := range response.Cookies() {
			if cookie.Name == portalSessionCookie {
				if !cookie.Secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode {
					t.Fatal("customer TLS cookie protections missing")
				}
				return cookie
			}
		}
		t.Fatal("customer cookie missing")
		return nil
	}
	query := "from=2026-10-14&to=2026-10-14"
	for _, path := range []string{"/api/logs?" + query, "/api/logs/export/prepare?" + query, "/api/logs/export?" + query, "/api/logs/export/v1?" + query} {
		if status, _, _ := request("GET", path, "", nil); status != 401 {
			t.Fatalf("anonymous customer endpoint status=%d", status)
		}
	}
	owner, foreign := login(groups[0].PortalEmail), login(groups[1].PortalEmail)
	admin := &http.Cookie{Name: portalSessionCookie, Value: m.signSession("root", roleRoot, time.Now().Unix())}
	if status, _, _ := request("GET", "/api/logs?"+query, "", admin); status != 401 {
		t.Fatal("admin session accepted as customer session")
	}
	status, body, _ := request("GET", "/api/logs?"+query, "", owner)
	var page struct {
		Rows     []LogRow `json:"rows"`
		Contract int      `json:"log_contract"`
	}
	if status != 200 || json.Unmarshal([]byte(body), &page) != nil || page.Contract != portalLogContractVersion || len(page.Rows) != 7 || strings.Contains(body, "PRIVATE_") || strings.Contains(body, `"request_path"`) || strings.Contains(body, `"upstream_request_id"`) {
		t.Fatal("customer all-type response leaked internal/foreign fields or lost rows")
	}
	for _, typ := range []int{2, 6, 7} {
		status, body, _ := request("GET", "/api/logs?"+query+fmt.Sprintf("&type=%d", typ), "", owner)
		var filtered struct {
			Rows     []LogRow `json:"rows"`
			Contract int      `json:"log_contract"`
		}
		if status != 200 || json.Unmarshal([]byte(body), &filtered) != nil || filtered.Contract != portalLogContractVersion || len(filtered.Rows) != 1 || filtered.Rows[0].Type != typ {
			t.Fatal("versioned log response lost type filter")
		}
	}
	for _, row := range page.Rows {
		if row.RequestID != fmt.Sprintf("owner-%d", row.Type) || row.PromptTokens != 100 || row.CompletionTokens != 10 || (portalLogHasRecordedAmount(row.Type) && row.CostUSD != .00022) {
			t.Fatal("customer original facts changed")
		}
	}
	if status, _, _ := request("GET", "/api/logs?"+query+"&member=102", "", owner); status != 404 {
		t.Fatal("cross-customer member selector was not rejected")
	}
	for _, filter := range []string{"request_id=foreign-request", "model=PRIVATE_FOREIGN_MODEL", "request_id=" + url.QueryEscape("' OR 1=1 --")} {
		status, body, _ := request("GET", "/api/logs?"+query+"&"+filter, "", owner)
		var result struct {
			Rows     []LogRow `json:"rows"`
			Contract int      `json:"log_contract"`
		}
		if status != 200 || json.Unmarshal([]byte(body), &result) != nil || result.Contract != portalLogContractVersion || len(result.Rows) != 0 || strings.Contains(body, "PRIVATE_") {
			t.Fatal("filter escaped customer scope")
		}
	}
	status, body, _ = request("GET", "/api/logs/export/prepare?"+query, "", owner)
	var prep struct {
		Ticket   string `json:"ticket"`
		Contract int    `json:"log_contract"`
	}
	if status != 200 || json.Unmarshal([]byte(body), &prep) != nil || prep.Contract != portalLogContractVersion || prep.Ticket == "" {
		t.Fatal("customer export prepare failed")
	}
	path := "/api/logs/export/v1?ticket=" + url.QueryEscape(prep.Ticket)
	if status, _, _ := request("GET", path, "", foreign); status != 400 {
		t.Fatal("foreign session accepted owner export ticket")
	}
	if status, _, _ := request("GET", path+"x", "", owner); status != 400 {
		t.Fatal("tampered export ticket accepted")
	}
	status, body, response := request("GET", path, "", owner)
	records, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(body, "\ufeff"))).ReadAll()
	if status != 200 || err != nil || len(records) != 8 || !strings.Contains(response.Header.Get("Content-Type"), "text/csv") || strings.Contains(body, "PRIVATE_") || strings.Contains(body, "192.0.2.101") {
		t.Fatal("customer export scope or format failed")
	}
	for _, row := range records {
		if len(row) != portalCSVColumnCount {
			t.Fatal("CSV column alignment failed")
		}
	}
	for _, field := range []string{"IP 地址", "上游 Request ID", "请求路径"} {
		for _, heading := range records[0] {
			if heading == field {
				t.Fatal("internal/removed column published")
			}
		}
	}
	var count int
	if m.prodDB.QueryRow("SELECT COUNT(*) FROM logs").Scan(&count) != nil || count != 8 {
		t.Fatal("customer workflow changed source rows")
	}
}

func TestPortalLogContractEmptyCustomer(t *testing.T) {
	m, _, router := newPortalTestMonitor(t)
	g := CustomerGroup{Name: "empty-local", PortalEmail: "empty@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	response := portalDo(router, "GET", "/api/logs?from=2026-10-14&to=2026-10-14&type=7", "", cookie)
	var result struct {
		Contract int      `json:"log_contract"`
		OK       bool     `json:"ok"`
		Rows     []LogRow `json:"rows"`
		HasMore  bool     `json:"has_more"`
	}
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.OK || result.Contract != portalLogContractVersion || result.Rows == nil || len(result.Rows) != 0 || result.HasMore {
		t.Fatal("memberless customer must receive a supported empty response without querying the source")
	}
}
