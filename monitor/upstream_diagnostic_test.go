package monitor

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestDiagnosticAddressRejectsInternalAndUnsafeURLs(t *testing.T) {
	for _, raw := range []string{"http://example.com", "https://u:p@example.com", "https://example.com?token=secret", "https://example.com:6379", "https://127.0.0.1", "https://169.254.169.254", "https://[::ffff:127.0.0.1]", "https://10.0.0.1"} {
		if _, err := diagnosticBaseURL(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"127.0.0.1", "169.254.169.254", "100.100.100.200", "0.0.0.0", "10.0.0.2", "172.26.4.11", "::1", "::ffff:127.0.0.1", "64:ff9b::a00:1", "2002:a00:1::", "224.0.0.1"} {
		if diagnosticPublicIP(net.ParseIP(raw)) {
			t.Errorf("accepted IP %s", raw)
		}
	}
	if !diagnosticPublicIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public IP rejected")
	}
	client := newDiagnosticHTTPClient()
	defer client.CloseIdleConnections()
	tr := client.Transport.(*http.Transport)
	if tr.Proxy != nil || tr.TLSClientConfig != nil && tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("unsafe transport")
	}
	if _, err := tr.DialContext(context.Background(), "tcp", "127.0.0.1:443"); !errors.Is(err, errDiagnosticAddress) {
		t.Fatalf("unsafe dial: %v", err)
	}
}

func TestDiagnosticPublicSignaturesAndUnknown(t *testing.T) {
	cases := []struct{ name, path, body, provider string }{
		{"newapi", "/api/status", `{"success":true,"data":{"version":"v1","quota_per_unit":500000}}`, "newapi"},
		{"sub2api", "/api/v1/settings/public", `{"code":0,"data":{"registration_enabled":true,"turnstile_enabled":false}}`, "sub2api"},
		{"html", "/api/status", `<html>login</html>`, ""},
		{"generic", "/api/status", `{"success":true,"data":{}}`, ""},
		{"failed", "/api/status", `{"success":false,"data":{"version":"v1","quota_per_unit":1}}`, ""},
		{"null newapi", "/api/status", `{"success":true,"data":{"version":null,"quota_per_unit":null}}`, ""},
		{"wrong version", "/api/status", `{"success":true,"data":{"version":1,"quota_per_unit":1}}`, ""},
		{"empty version", "/api/status", `{"success":true,"data":{"version":"  ","quota_per_unit":1}}`, ""},
		{"string quota", "/api/status", `{"success":true,"data":{"version":"v1","quota_per_unit":"1"}}`, ""},
		{"zero quota", "/api/status", `{"success":true,"data":{"version":"v1","quota_per_unit":0}}`, ""},
		{"negative quota", "/api/status", `{"success":true,"data":{"version":"v1","quota_per_unit":-1}}`, ""},
		{"overflow quota", "/api/status", `{"success":true,"data":{"version":"v1","quota_per_unit":1e999}}`, ""},
		{"null sub2api", "/api/v1/settings/public", `{"code":0,"data":{"registration_enabled":null,"turnstile_enabled":null}}`, ""},
		{"wrong boolean", "/api/v1/settings/public", `{"code":0,"data":{"registration_enabled":"false","turnstile_enabled":false}}`, ""},
		{"false flags valid", "/api/v1/settings/public", `{"code":0,"data":{"registration_enabled":false,"email_verify_enabled":false}}`, "sub2api"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := diagnosticSignature(tc.path, []byte(tc.body)); got != tc.provider {
				t.Fatalf("got %s", got)
			}
		})
	}
	for _, body := range []string{"<html>unknown</html>", "<html>TokenForce</html>", "<html>AICodeWith</html>", "<html>Sub2API New-API</html>"} {
		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if r.Header.Get("Authorization") != "" || r.Method != "GET" {
				t.Error("public probe carried credentials")
			}
			writeDiagnosticTestBody(t, w, body)
		}))
		report := upstreamDiagnosticReport{Provider: "unknown"}
		diagnosePublicUpstream(context.Background(), srv.Client(), srv.URL, &report)
		srv.Close()
		if calls > 3 || len(report.Checks) == 0 {
			t.Fatalf("unbounded/empty probe: %+v", report)
		}
		if strings.Contains(body, "unknown") || strings.Contains(body, "Sub2API New") {
			if report.Provider != "unknown" {
				t.Fatal("ambiguous evidence auto-identified")
			}
		} else if report.Confidence != "suspected" {
			t.Fatal("branding must only be suspected")
		}
	}
}

func TestDiagnosticNullSignaturesCannotOverrideSavedProvider(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeDiagnosticTestBody(t, w, `{"success":true,"code":0,"data":{"version":null,"quota_per_unit":null,"registration_enabled":null,"turnstile_enabled":null}}`)
	}))
	defer srv.Close()
	report := upstreamDiagnosticReport{Provider: upstreamProviderAICodeWith}
	diagnosePublicUpstream(context.Background(), srv.Client(), srv.URL, &report)
	if report.Confidence == "compatible" || report.Provider != upstreamProviderAICodeWith {
		t.Fatalf("null fields promoted to provider evidence: %+v", report)
	}
}

func TestDiagnosticFailureAdviceNeverLeaksUpstreamText(t *testing.T) {
	cases := []struct {
		err  error
		code string
	}{
		{&upstreamHTTPError{Status: 400, Message: "turnstile verification failed secret-123"}, "challenge"},
		{&upstreamHTTPError{Status: 401, Message: "invalid refresh token secret-123"}, "refresh_expired"},
		{&upstreamHTTPError{Status: 401, Message: "secret-123"}, "unauthorized"},
		{&upstreamHTTPError{Status: 403}, "forbidden"},
		{&upstreamHTTPError{Status: 429}, "rate_limited"},
		{&upstreamHTTPError{Status: 404}, "route_missing"},
		{&upstreamHTTPError{Status: 302}, "redirect"},
		{&upstreamHTTPError{Status: 503}, "upstream_error"},
		{&tokenForceAPIError{Status: 10025, Message: "secret-123"}, "unauthorized"},
		{x509.HostnameError{Host: "secret-123", Certificate: &x509.Certificate{}}, "tls"},
		{context.DeadlineExceeded, "timeout"},
		{&upstreamCircuitOpenError{RetryAt: time.Now().Add(time.Minute).Unix()}, "rate_limited"},
		{&net.DNSError{Err: "secret-123", Name: "host"}, "dns"},
	}
	for _, tc := range cases {
		c := diagnosticFailure("test", tc.err)
		b, _ := json.Marshal(c)
		if c.Code != tc.code || strings.Contains(string(b), "secret-123") || c.Action == "" {
			t.Fatalf("bad advice %+v", c)
		}
	}
}

func TestDiagnosticSpringRecordModeReadsOnlyFirstKeyFirstPage(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamAICodeWithRecordsEnabled = true
	calls := 0
	var firstKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer "+firstKey {
			t.Error("probe wrote data or tested an unexpected key")
		}
		if r.URL.Path == "/api/v1/balance" {
			writeDiagnosticTestBody(t, w, `{"balance":10,"currency":"USD"}`)
			return
		}
		q := r.URL.Query()
		if r.URL.Path != "/api/v1/api-keys/usage" || q.Get("group_by") != "record" || q.Get("limit") != "100" || q.Get("cursor") != "" {
			t.Errorf("unexpected probe %s", r.URL.Path)
		}
		day := cstDayStart(time.Now().Unix()) - 86400
		_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, 7, []map[string]any{recordTestItem(day, 1, 0, "0.1", 10)}, true, "must-not-follow"))
	}))
	defer srv.Close()
	row := ChannelUpstreamAccount{Domain: "record-diagnostic.test", Provider: upstreamProviderAICodeWith, BaseURL: srv.URL, CredentialVersion: upstreamCredentialVersion}
	cred := aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{
		{SlotID: "acw_first", Secret: "sk-acw-first"},
		{SlotID: "acw_second", Secret: "sk-acw-second"},
	}}
	keys, err := aiCodeWithCredentialKeys(cred)
	if err != nil || len(keys) != 2 {
		t.Fatalf("invalid fixture: %v", err)
	}
	firstKey = keys[0] // Saved keys have a canonical ordering, not input ordering.
	row.Credential, err = m.sealUpstreamCredential(row.Domain, row.Provider, cred)
	if err != nil {
		t.Fatal(err)
	}
	report := upstreamDiagnosticReport{}
	if err := m.probeSavedUpstream(context.Background(), srv.Client(), row, &report); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || report.Checks[len(report.Checks)-1].Code != "usage_readable" {
		t.Fatalf("calls=%d report=%+v", calls, report)
	}
	mode := false
	for _, check := range report.Checks {
		mode = mode || check.Code == "record_mode"
	}
	if !mode {
		t.Fatal("record mode not disclosed")
	}
	for _, model := range []any{&AICodeWithRecordCheckpoint{}, &AICodeWithRecordSeen{}, &ChannelUpstreamUsageHour{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("probe persisted accounting state: %T count=%d err=%v", model, count, err)
		}
	}
}

func TestDiagnosticSavedAdaptersUseOnlyReadRequests(t *testing.T) {
	for _, provider := range []string{"sub2api", "tokenforce", "aicodewith"} {
		t.Run(provider, func(t *testing.T) {
			m := newChannelUpstreamTestMonitor(t)
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "GET" {
					t.Error("write/refresh request")
				}
				switch r.URL.Path {
				case "/api/v1/user/profile":
					writeDiagnosticTestBody(t, w, `{"code":0,"data":{"balance":10}}`)
				case "/api/v1/usage/dashboard/trend":
					http.NotFound(w, r)
				case "/api/v1/usage/stats":
					writeDiagnosticTestBody(t, w, `{"code":0,"data":{"total_requests":0,"total_tokens":0,"total_actual_cost":0}}`)
				case "/api/orgs/7/balance":
					writeDiagnosticTestBody(t, w, `{"status":0,"result":{"currentBalance":70}}`)
				case "/api/usages/detail":
					writeDiagnosticTestBody(t, w, `{"status":0,"result":{"content":[],"totalElements":0}}`)
				case "/api/v1/balance":
					writeDiagnosticTestBody(t, w, `{"balance":10,"currency":"USD"}`)
				case "/api/v1/api-keys/usage":
					q := r.URL.Query()
					writeDiagnosticTestBody(t, w, fmt.Sprintf(`{"data":{"api_key_id":7,"group_by":"day","period":{"start":%q,"end":%q},"summary":{"cost":0,"total_tokens":0,"requests":0},"daily":[]}}`, q.Get("start"), q.Get("end")))
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			row := ChannelUpstreamAccount{Domain: "adapter.example", Provider: provider, BaseURL: srv.URL, UserID: 7, BalanceUnit: 7, CredentialVersion: upstreamCredentialVersion}
			var cred any = sub2APICredential{AccessToken: "access", RefreshToken: "never-rotate", ExpiresAt: time.Now().Add(time.Hour).Unix()}
			if provider == "aicodewith" {
				row.BalanceUnit = 1 // Spring is 1:1; TokenForce retains its configured unit.
				cred = aiCodeWithCredential{APIKey: "sk-acw-test"}
			}
			row.Credential, _ = m.sealUpstreamCredential(row.Domain, provider, cred)
			report := upstreamDiagnosticReport{}
			if err := m.probeSavedUpstream(context.Background(), srv.Client(), row, &report); err != nil {
				t.Fatal(err)
			}
			if calls > 3 || report.Checks[len(report.Checks)-1].Code != "usage_readable" {
				t.Fatalf("calls=%d report=%+v", calls, report)
			}
		})
	}
}

func TestDiagnosticRouteRequiresRoot(t *testing.T) {
	m, router, _ := newPortalTestMonitor(t)
	for _, role := range []int{0, roleAdmin, roleRoot} {
		var cookies []*http.Cookie
		if role != 0 {
			cookies = append(cookies, &http.Cookie{Name: sessionCookie, Value: m.signSession("admin", role, time.Now().Unix())})
		}
		w := portalDo(router, http.MethodPost, "/channels/upstream/diagnose", `{"domain":"absent.example"}`, cookies...)
		want := 400
		if role == 0 {
			want = 401
		} else if role == roleAdmin {
			want = 403
		}
		if w.Code != want {
			t.Fatalf("role=%d got %d want %d: %s", role, w.Code, want, w.Body.String())
		}
	}
}

func TestDiagnosticDoesNotFollowRedirectOrAcceptOversize(t *testing.T) {
	destinationCalls := 0
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls++ }))
	defer dst.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/large" {
			writeDiagnosticTestBody(t, w, strings.Repeat("x", diagnosticBodyLimit+1))
			return
		}
		http.Redirect(w, r, dst.URL, 302)
	}))
	defer src.Close()
	client := newUpstreamHTTPClient(time.Second)
	defer client.CloseIdleConnections()
	if _, err := diagnosticGET(context.Background(), client, src.URL, map[string]string{"Authorization": "secret"}); err == nil || destinationCalls != 0 {
		t.Fatal("redirect followed")
	}
	if _, err := diagnosticGET(context.Background(), client, src.URL+"/large", nil); !errors.Is(err, errDiagnosticShape) {
		t.Fatalf("oversize: %v", err)
	}
}

func TestDiagnosticSavedNewAPIReadsSinglePageWithoutPublishing(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	calls := []string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		if r.Method != "GET" {
			t.Error("diagnostic must not write/refresh")
		}
		switch r.URL.Path {
		case "/api/user/self":
			if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("New-Api-User") != "7" {
				t.Error("auth headers")
			}
			writeDiagnosticTestBody(t, w, `{"success":true,"data":{"quota":123}}`)
		case "/api/status":
			writeDiagnosticTestBody(t, w, `{"success":true,"data":{"quota_per_unit":500000}}`)
		case "/api/log/self":
			writeDiagnosticTestBody(t, w, `{"success":true,"data":{"items":[],"total":0}}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	row := ChannelUpstreamAccount{Domain: "diagnostic.example", Provider: "newapi", BaseURL: srv.URL, UserID: 7, CredentialVersion: upstreamCredentialVersion, Enabled: true, UsageSyncEnabled: true}
	row.Credential, _ = m.sealUpstreamCredential(row.Domain, row.Provider, newAPICredential{AccessToken: "secret"})
	report := upstreamDiagnosticReport{}
	if err := m.probeSavedUpstream(context.Background(), srv.Client(), row, &report); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || len(report.Checks) != 2 || report.Checks[1].Code != "usage_readable" {
		t.Fatalf("%v %+v", calls, report)
	}
	var count int64
	m.storeDB.Model(&ChannelUpstreamUsageHour{}).Count(&count)
	if count != 0 {
		t.Fatal("diagnostic published usage")
	}
}

func TestDiagnosticExpiredSessionDoesNotRotate(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	for _, provider := range []string{"sub2api", "tokenforce"} {
		row := ChannelUpstreamAccount{Domain: "expired.example", Provider: provider, CredentialVersion: upstreamCredentialVersion}
		// Both session envelopes use the same keys.
		row.Credential, _ = m.sealUpstreamCredential(row.Domain, provider, sub2APICredential{AccessToken: "old", RefreshToken: "must-not-rotate", ExpiresAt: 1})
		report := upstreamDiagnosticReport{}
		if err := m.probeSavedUpstream(context.Background(), nil, row, &report); err != nil {
			t.Fatal(err)
		}
		if len(report.Checks) != 1 || report.Checks[0].Code != "session_refresh_needed" {
			t.Fatalf("%+v", report)
		}
	}
}

func TestDiagnosticLocalSnapshotAndUnknownDomainNeverDial(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.LocalSnapshotOnly = true
	if err := m.storeDB.Create(&ChannelSnap{ID: 888, BaseDomain: "diagnostic.example"}).Error; err != nil {
		t.Fatal(err)
	}
	for _, domain := range []string{"diagnostic.example", "not-configured.example"} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("POST", "/channels/upstream/diagnose", strings.NewReader(`{"domain":"`+domain+`","base_url":"https://diagnostic.example"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		m.diagnoseChannelUpstreamHandler(c)
		if domain == "diagnostic.example" {
			if rec.Code != 200 || !strings.Contains(rec.Body.String(), "local_snapshot") {
				t.Fatalf("%d %s", rec.Code, rec.Body.String())
			}
		} else if rec.Code != 400 {
			t.Fatal("unknown domain accepted")
		}
	}
}

func TestUsageProductionCacheIgnoresLegacyRedisConfiguration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cache := newUsageResultCache(Settings{UsageRedisAddr: listener.Addr().String(), UsageRedisPassword: "unused"})
	defer cache.Close()
	fills := 0
	var out int
	for i := 0; i < 2; i++ {
		if err := cache.DoJSON(context.Background(), "same", time.Minute, &out, func() (any, error) { fills++; return 42, nil }); err != nil {
			t.Fatal(err)
		}
	}
	s := cache.Stats(time.Now())
	if cache.remote != nil || s.RemoteConfigured || s.RemoteErrors != 0 || s.RemoteHits != 0 || s.RemoteMisses != 0 || s.LocalHits != 1 || fills != 1 || out != 42 {
		t.Fatalf("%+v fills=%d", s, fills)
	}
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if conn, err := listener.Accept(); err == nil {
		conn.Close()
		t.Fatal("unexpected Redis connection")
	}
}

func writeDiagnosticTestBody(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	if _, err := io.WriteString(w, body); err != nil {
		t.Error(err)
	}
}
