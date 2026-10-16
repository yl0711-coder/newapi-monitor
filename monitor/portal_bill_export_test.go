package monitor

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func billSampleRow(t *testing.T, prompt, output, read, write, five, hour, quota int64) LogRow {
	t.Helper()
	r := LogRow{Type: 2, Member: "sample-user", TokenName: "sample-token", Group: "sample-group", ModelName: "claude-opus-5-5", RequestID: "local-request-1", PromptTokens: prompt, CompletionTokens: output, billingQuota: quota, billingFactsValid: true, CreatedAt: time.Date(2026, 10, 13, 9, 40, 0, 0, usageCST).Unix()}
	fields := map[string]any{"billing_mode": "tiered_expr", "expr_b64": base64.StdEncoding.EncodeToString([]byte(`tier("base", p * 4 + cr * 0.2 + cc * 5 + cc1h * 8 + c * 20)`)), "matched_tier": "base", "billing_source": "wallet", "usage_semantic": "anthropic", "group_ratio": 0.6, "cache_tokens": read, "cache_creation_tokens": write, "cache_creation_tokens_5m": five, "cache_creation_tokens_1h": hour, "request_path": "/v1/messages"}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	populatePortalLogEvidence(&r, string(raw))
	return r
}

func TestPortalBillSampleAndMissingTTL(t *testing.T) {
	// Customer-approved same-variable reference: unsplit writes remain visible,
	// but are not assigned an invented TTL or added to the original charge.
	r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
	before, _ := json.Marshal(r)
	got, err := portalBillRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 15 || got[5] != "local-request-1" || got[6] != "文本" || got[7] != "0" || got[10] != "237815" || got[11] != "$0.004040" || got[12] != "¥0.028280" || got[13] != "¥0.002424" || got[14] != "0.086" {
		t.Fatalf("%v", got)
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) || r.billEvidence.Numbers["cache_creation_tokens"].Text != "237815" {
		t.Fatal("source mutated")
	}
	if strings.Contains(string(after), "expr_b64") || strings.Contains(string(after), "billEvidence") {
		t.Fatal("private evidence leaked")
	}
	if portalBillHeader()[4] != "模型" || portalBillHeader()[5] != "Request ID" || portalBillHeader()[6] != "类型" {
		t.Fatal("wrong column order")
	}
}

func TestPortalBillReferenceFailsClosed(t *testing.T) {
	for _, name := range []string{"charge", "price", "tool", "semantic", "split", "group", "path"} {
		t.Run(name, func(t *testing.T) {
			r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
			switch name {
			case "charge":
				r.billingQuota++
			case "price":
				r.billEvidence.Expression.Text = `tier("base", p * 4 + cr * 0.2 + cc * 5 + cc1h * 8 + c * 21)`
			case "tool":
				r.billEvidence.ExtraBilling = true
			case "semantic":
				r.billEvidence.Strings["usage_semantic"] = portalEvidenceField{State: portalEvidenceMissing}
			case "split":
				r.billEvidence.Numbers["cache_creation_tokens_5m"] = portalEvidenceField{State: portalEvidenceValid, Text: "237816"}
			case "group":
				r.billEvidence.Numbers["user_group_ratio"] = portalEvidenceField{State: portalEvidenceValid, Text: "0.7"}
			case "path":
				r.billEvidence.Strings["request_path"] = portalEvidenceField{State: portalEvidenceValid, Text: "/v1/images/generations"}
			}
			got, err := portalBillRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimPrefix(got[12], "¥") != "" || got[14] != "" || strings.TrimPrefix(got[13], "¥") == "" {
				t.Fatalf("unverified price or lost charge: %v", got)
			}
		})
	}
}

func TestPortalBillScopeAndTextSafety(t *testing.T) {
	for _, typ := range []int{0, 1, 3, 4, 5, 6, 7, 99} {
		r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
		r.Type = typ
		if _, err := portalBillRecord(r); err == nil {
			t.Fatalf("non-consumption type %d accepted", typ)
		}
	}
	for _, value := range []string{" =SUM(1)", "\n@SUM(1)", "\t+1", "-1", "\r=1"} {
		if !strings.HasPrefix(portalBillSafeText(value), "'") {
			t.Fatal("whitespace formula injection")
		}
	}
	for _, source := range []string{"subscription"} {
		r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
		r.BillingSource = source
		if _, err := portalBillRecord(r); err == nil {
			t.Fatal("non-wallet accepted")
		}
	}
	r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
	r.Type = 6
	if _, err := portalBillRecord(r); err == nil {
		t.Fatal("refund accepted")
	}
	r.Type = 2
	r.billingFactsValid = false
	if _, err := portalBillRecord(r); err == nil {
		t.Fatal("NULL facts accepted")
	}
	r.billingFactsValid = true
	r.Member = "=HYPERLINK(\"secret\")"
	r.RequestID = "@SUM(1)"
	r.UpstreamRequestID = "secret-upstream"
	r.IP = "192.0.2.9"
	got, err := portalBillRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got[1], "'") || !strings.HasPrefix(got[5], "'") || strings.Contains(strings.Join(got, " "), "secret-upstream") || strings.Contains(strings.Join(got, " "), r.IP) {
		t.Fatal("unsafe columns")
	}
}

func TestPortalBillHistoricalAmountIndependentOfMetadata(t *testing.T) {
	for _, raw := range []string{"", "null", "{broken", `{}`, `{"billing_source":""}`, `{"billing_source":"unknown"}`, `{"usage_semantic":"anthropic","cache_tokens":0,"cache_creation_tokens":0}`} {
		t.Run(raw, func(t *testing.T) {
			r := LogRow{Type: 2, billingQuota: 123, SourceFacts: &portalLogSourceFacts{QuotaPresent: true}}
			populatePortalLogEvidence(&r, raw)
			got, err := portalBillRecord(r)
			if err != nil || strings.TrimPrefix(got[13], "¥") != "0.000246" || got[7] != "" || got[8] != "" || strings.TrimPrefix(got[12], "¥") != "" || got[14] != "" {
				t.Fatalf("historical amount lost or NULL tokens invented: %v %v", got, err)
			}
		})
	}
	for _, raw := range []string{`{"subscription_id":42}`, `{"subscription_plan_id":42}`, `{"billing_source":"subscription"}`} {
		r := LogRow{Type: 2, billingQuota: 123, billingFactsValid: true}
		populatePortalLogEvidence(&r, raw)
		if _, err := portalBillRecord(r); err == nil {
			t.Fatal("subscription interpreted as wallet charge")
		}
	}
	r := LogRow{Type: 2, billingQuota: 0, SourceFacts: &portalLogSourceFacts{QuotaPresent: true}}
	got, err := portalBillRecord(r)
	if err != nil || strings.TrimPrefix(got[13], "¥") != "0.000000" {
		t.Fatal("true zero must remain zero")
	}
	r.SourceFacts.QuotaPresent = false
	if _, err := portalBillRecord(r); err == nil {
		t.Fatal("missing quota became zero")
	}
}

func TestPortalBillVerifiedDefaultTierTokenCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, extra string
		quota       int64
		complete    bool
	}{
		{"omitted zero", "", 100, true},
		{"explicit zero", `,"cache_creation_tokens":0`, 100, true},
		{"null creation", `,"cache_creation_tokens":null`, 100, false},
		{"ambiguous Claude", `,"claude":true`, 100, false},
		{"null semantic", `,"usage_semantic":null`, 100, false},
		{"unknown semantic", `,"usage_semantic":"future"`, 100, false},
		{"split without total", `,"cache_creation_tokens_5m":1`, 100, false},
		{"wrong charge does not erase counts", "", 101, true},
		{"zero charge does not erase counts", "", 0, true},
		{"price only rule", `,"request_rules":[]`, 101, true},
		{"redundant counts match", `,"input_tokens_total":100,"cache_write_tokens":0`, 100, true},
		{"total input conflict", `,"input_tokens_total":99`, 100, false},
		{"total input null", `,"input_tokens_total":null`, 100, false},
		{"total write conflict", `,"cache_write_tokens":1`, 100, false},
		{"total write null", `,"cache_write_tokens":null`, 100, false},
		{"negative cache", `,"cache_creation_tokens":-1`, 100, false},
		{"fractional cache", `,"cache_creation_tokens":1.5`, 100, false},
		{"overflow cache", `,"cache_creation_tokens":9223372036854775808`, 100, false},
		{"cache exceeds input", `,"cache_creation_tokens":81`, 100, false},
		{"audio flag", `,"audio":true`, 100, false},
		{"media count", `,"audio_input":0`, 100, false},
		{"task", `,"task_id":"task"`, 100, false},
		{"unsupported tool", `,"tool_surcharges":[]`, 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, PromptTokens: 100, CompletionTokens: 10, billingQuota: tc.quota, billingFactsValid: true}
			raw := `{"billing_mode":"tiered_expr","billing_source":"wallet","group_ratio":1,"cache_tokens":20,"request_path":"/v1/responses","matched_tier":"base","expr_b64":"` + base64.StdEncoding.EncodeToString([]byte(`tier("base", p * 2 + c * 4 + cr * 0)`)) + `"` + tc.extra + `}`
			populatePortalLogEvidence(&r, raw)
			got, err := portalBillRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			if tc.complete {
				if got[7] != "80" || got[9] != "20" || got[10] != "0" {
					t.Fatalf("default token contract differs: %v", got)
				}
			} else if got[7] != "" {
				t.Fatal("ambiguous input inferred")
			}
			if strings.TrimPrefix(got[13], "¥") != new(big.Rat).SetFrac(big.NewInt(tc.quota), big.NewInt(500000)).FloatString(6) {
				t.Fatal("actual changed")
			}
		})
	}
}

func TestPortalBillDefaultTokensRequireSourcePresence(t *testing.T) {
	r := LogRow{Type: 2, BillingSource: "wallet"}
	e := parsePortalLogEvidence(`{"billing_mode":"tiered_expr","billing_source":"wallet","cache_tokens":0,"request_path":"/v1/responses","matched_tier":"base","expr_b64":"` + base64.StdEncoding.EncodeToString([]byte(`tier("base", p * 2 + c * 4)`)) + `"}`)
	if portalBillDefaultTierTokenContract(r, e) {
		t.Fatal("missing source presence became proven zero")
	}
	r.SourceFacts = &portalLogSourceFacts{PromptPresent: true, QuotaPresent: true}
	if !portalBillDefaultTierTokenContract(r, e) {
		t.Fatal("recorded zero should not depend on output or charge proof")
	}
}

func TestPortalBillDefaultTokensIndependentOfChargeAndOutput(t *testing.T) {
	expr := base64.StdEncoding.EncodeToString([]byte(`tier("base", p * 2 + c * 4 + cr * 0)`))
	for prompt := int64(0); prompt <= 12; prompt++ {
		for read := int64(0); read <= prompt; read++ {
			for write := int64(0); write <= prompt-read; write++ {
				raw := fmt.Sprintf(`{"billing_mode":"tiered_expr","billing_source":"wallet","cache_tokens":%d,"cache_creation_tokens":%d,"request_path":"/v1/responses","matched_tier":"base","expr_b64":%q}`, read, write, expr)
				r := LogRow{Type: 2, PromptTokens: prompt, billingQuota: 123, SourceFacts: &portalLogSourceFacts{PromptPresent: true, QuotaPresent: true}}
				populatePortalLogEvidence(&r, raw)
				before, _ := json.Marshal(r.billEvidence)
				got, err := portalBillRecord(r)
				if err != nil || got[7] != strconv.FormatInt(prompt-read-write, 10) || got[8] != "" || got[9] != strconv.FormatInt(read, 10) || got[10] != strconv.FormatInt(write, 10) || strings.TrimPrefix(got[13], "¥") != "0.000246" || strings.TrimPrefix(got[12], "¥") != "" || got[14] != "" {
					t.Fatalf("token conservation or independent charge failed: %v %v", got, err)
				}
				after, _ := json.Marshal(r.billEvidence)
				if string(before) != string(after) || r.PromptTokens != prompt || r.billingQuota != 123 {
					t.Fatal("source mutated")
				}
				r.SourceFacts.PromptPresent = false
				got, err = portalBillRecord(r)
				if err != nil || got[7] != "" {
					t.Fatal("missing source input inferred")
				}
			}
		}
	}
}

func TestPortalBillScopeAlwaysConsumptionAndLegacyRowsRetained(t *testing.T) {
	m, p, ck, _ := billHTTPFixture(t)
	if _, err := m.prodDB.Exec(`UPDATE logs SET other='',prompt_tokens=NULL,completion_tokens=NULL WHERE id=101`); err != nil {
		t.Fatal(err)
	}
	w := billGet(p, ck, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&type=6&member=101&format=bill-v1")
	var prep struct {
		Ticket string `json:"ticket"`
		Total  int    `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &prep); err != nil || w.Code != 200 || prep.Total != 1 {
		t.Fatalf("prepare scope: %d %s", w.Code, w.Body.String())
	}
	w = billGet(p, ck, "/api/logs/bill/v1?ticket="+prep.Ticket)
	if w.Code != 200 {
		t.Fatalf("legacy export: %d %s", w.Code, w.Body.String())
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(w.Body.String(), "\ufeff"))).ReadAll()
	if err != nil || len(rows) != 2 || strings.TrimPrefix(rows[1][13], "¥") != "0.000246" || rows[1][7] != "" || rows[1][8] != "" {
		t.Fatalf("legacy row lost or mutated: %v %v", rows, err)
	}
}

func TestPortalBillPreflightCapCannotBeConfirmedAway(t *testing.T) {
	m, p, ck, _ := billHTTPFixture(t)
	_, err := m.prodDB.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 50000)
	INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,other)
	SELECT n+1000,101,?,2,1,1,1,'{"billing_source":"wallet"}' FROM seq`, time.Date(2026, 10, 13, 12, 0, 0, 0, usageCST).Unix())
	if err != nil {
		t.Fatal(err)
	}
	w := billGet(p, ck, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&format=bill-v1&confirm=1")
	if w.Code != 413 || strings.Contains(w.Body.String(), "\"ticket\"") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func billHTTPFixture(t *testing.T) (*Monitor, http.Handler, *http.Cookie, int64) {
	t.Helper()
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	g := CustomerGroup{Name: "bill-local", PortalEmail: "bill@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "alice", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 10, 13, 12, 0, 0, 0, usageCST).Unix()
	for _, u := range []int{101, 202} {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,model_name,other,request_id) VALUES(?,?,?,2,123,10,5,?,'unknown-model',?,?)", u, u, stamp, "account-"+strconv.Itoa(u), `{"billing_source":"wallet","usage_semantic":"openai","cache_tokens":2,"cache_creation_tokens":3}`, "req"); err != nil {
			t.Fatal(err)
		}
	}
	// Recharges, admin/system events, errors, refunds and login events are not consumption.
	for _, typ := range []int{1, 3, 4, 5, 6, 7} {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,created_at,type,quota) VALUES(?,101,?,?,999999)", 300+typ, stamp, typ); err != nil {
			t.Fatal(err)
		}
	}
	ck := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	return m, portal, ck, g.ID
}

func TestPortalBillRequestCategoryIsNotLogType(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{`{"request_path":"/v1/messages"}`, "文本"},
		{`{"request_path":"/v1/responses"}`, "文本"},
		{`{"request_path":"/v1/chat/completions"}`, "文本"},
		{`{}`, "未确认"},
		{`{"request_path":null}`, "未确认"},
		{`{"request_path":"/v1/images/generations"}`, "未确认"},
		{`{"request_path":"/v1/responses","audio":true}`, "未确认"},
		{`{"request_path":"/v1/responses","image_generation_call":true}`, "未确认"},
		{`{"request_path":"/v1/responses","task_id":"task-1"}`, "未确认"},
	} {
		if got := portalBillRequestType(parsePortalLogEvidence(tc.raw)); got != tc.want {
			t.Fatalf("got %s want %s", got, tc.want)
		}
	}
}

func billPrepare(t *testing.T, p http.Handler, ck *http.Cookie) string {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&format=bill-v1", nil)
	req.AddCookie(ck)
	p.ServeHTTP(w, req)
	var out struct {
		Ticket string `json:"ticket"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != 200 || out.Ticket == "" {
		t.Fatalf("prepare %d %s", w.Code, w.Body.String())
	}
	return out.Ticket
}

func billGet(p http.Handler, ck *http.Cookie, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", path, nil)
	if ck != nil {
		r.AddCookie(ck)
	}
	p.ServeHTTP(w, r)
	return w
}

func TestPortalBillHTTPIsolationAndContract(t *testing.T) {
	m, p, ck, _ := billHTTPFixture(t)
	ticket := billPrepare(t, p, ck)
	if w := billGet(p, nil, "/api/logs/bill/v1?ticket="+ticket); w.Code != 401 {
		t.Fatal("anonymous allowed")
	}
	if w := billGet(p, ck, "/api/logs/bill/v1"); w.Code != 400 {
		t.Fatal("ticket bypass")
	}
	if w := billGet(p, ck, "/api/logs/export/v1?ticket="+ticket); w.Code != 400 {
		t.Fatal("cross-format ticket accepted")
	}
	if w := billGet(p, ck, "/api/logs/bill/v1?ticket="+ticket+"x"); w.Code != 400 {
		t.Fatal("tampering accepted")
	}
	w := billGet(p, ck, "/api/logs/bill/v1?ticket="+ticket+"&member=202&format=other")
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Disposition"), "usage-bill-") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(w.Body.String(), "\ufeff"))).ReadAll()
	if err != nil || len(rows) != 2 || !reflect.DeepEqual(rows[0], portalBillHeader()) || rows[1][7] != "5" || strings.TrimPrefix(rows[1][13], "¥") != "0.000246" || strings.TrimPrefix(rows[1][12], "¥") != "" {
		t.Fatalf("%v %v", rows, err)
	}
	if err := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", 101).Update("group_id", 0).Error; err != nil {
		t.Fatal(err)
	}
	if w := billGet(p, ck, "/api/logs/bill/v1?ticket="+ticket); w.Code != 409 {
		t.Fatalf("stale scope %d", w.Code)
	}
}

func TestPortalBillRejectsPartialAndNonWallet(t *testing.T) {
	for _, kind := range []string{"deleted", "subscription", "over-cap", "wrong-type"} {
		t.Run(kind, func(t *testing.T) {
			m, p, ck, _ := billHTTPFixture(t)
			ticket := billPrepare(t, p, ck)
			want := 409
			switch kind {
			case "deleted":
				_, err := m.prodDB.Exec("DELETE FROM logs WHERE id=101")
				if err != nil {
					t.Fatal(err)
				}
			case "subscription":
				_, err := m.prodDB.Exec(`UPDATE logs SET other='{"billing_source":"subscription"}' WHERE id=101`)
				if err != nil {
					t.Fatal(err)
				}
				want = 422
			default:
				claim, ok := m.verifyPortalExportClaim(ticket, time.Now().Unix())
				if !ok {
					t.Fatal("claim")
				}
				if kind == "over-cap" {
					claim.Total = portalExportCap + 1
				} else {
					claim.LogType = 6
				}
				var err error
				ticket, err = m.signPortalExportClaim(claim)
				if err != nil {
					t.Fatal(err)
				}
				want = 400
			}
			w := billGet(p, ck, "/api/logs/bill/v1?ticket="+ticket+"&confirm=1")
			if w.Code != want || strings.Contains(w.Header().Get("Content-Type"), "text/csv") || strings.Contains(w.Body.String(), "\ufeff") {
				t.Fatalf("partial leak %d %s", w.Code, w.Body.String())
			}
		})
	}
}
