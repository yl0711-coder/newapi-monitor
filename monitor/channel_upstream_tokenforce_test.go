package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestTokenForceRefreshAndBalanceNormalizeCNY(t *testing.T) {
	now := time.Now().Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/sys/login/refresh":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refreshToken"] != "refresh-old" {
				t.Fatalf("unexpected refresh body: %+v err=%v", body, err)
			}
			_, _ = w.Write([]byte(`{"status":0,"message":"Success","result":{"token":"access-new","refreshToken":"refresh-new","expires":` + strconv.FormatInt(now+3600, 10) + `}}`))
		case "/api/orgs/123/balance":
			if r.Header.Get("Authorization") != "Bearer access-new" {
				t.Fatalf("unexpected auth header: %q", r.Header.Get("Authorization"))
			}
			_, _ = w.Write([]byte(`{"status":0,"message":"Success","result":{"currentBalance":"720.00","currentBalanceWithoutGift":700,"currentGiftBalance":20,"creditLimit":0,"currentDebt":0}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Provider: upstreamProviderTokenForce, BaseURL: server.URL, UserID: 123, BalanceUnit: 7.2}
	credential, err := importTokenForceSession(context.Background(), server.Client(), row, "refresh-old")
	if err != nil || credential.AccessToken != "access-new" || credential.RefreshToken != "refresh-new" {
		t.Fatalf("credential=%+v err=%v", credential, err)
	}
	result, updated, err := syncTokenForceBalance(context.Background(), server.Client(), row, credential)
	if err != nil || updated.RefreshToken != "refresh-new" || result.BalanceRaw != 720 || math.Abs(result.BalanceUSD-100) > 1e-9 || result.BalanceUnit != 7.2 {
		t.Fatalf("result=%+v updated=%+v err=%v", result, updated, err)
	}
}

func TestTokenForceRefreshAcceptsWhiteLabelAccessTokenField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":0,"result":{"accessToken":"access-alias","refreshToken":"refresh-new","expiresIn":3600}}`))
	}))
	defer server.Close()
	credential, err := refreshTokenForce(context.Background(), server.Client(), ChannelUpstreamAccount{BaseURL: server.URL}, tokenForceCredential{RefreshToken: "refresh-old"})
	if err != nil || credential.AccessToken != "access-alias" || credential.RefreshToken != "refresh-new" || credential.ExpiresAt <= time.Now().Unix()+3500 {
		t.Fatalf("credential=%+v err=%v", credential, err)
	}
}

func TestTokenForceUsageWindowStrictlyAggregatesAndNormalizes(t *testing.T) {
	from := time.Date(2026, 9, 3, 20, 0, 0, 0, cstLocation).Unix()
	to := from + 3600
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/usages/detail" || r.URL.Query().Get("orgId") != "123" || r.URL.Query().Get("page") != "0" || r.URL.Query().Get("size") != "100" {
			t.Fatalf("unexpected request: %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.URL.Query().Get("beginTime") != time.Unix(from, 0).UTC().Format(time.RFC3339) || r.URL.Query().Get("endTime") != time.Unix(to, 0).UTC().Format(time.RFC3339) {
			t.Fatalf("unexpected time range: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":0,"result":{"content":[` +
			`{"requestId":"req-1","requestTime":"2026-09-03T12:10:00Z","inputTokens":100,"outputTokens":10,"inputCachedTokens":50,"outputReasoningTokens":4,"costInCny":"7.2"},` +
			`{"requestId":"req-2","requestTime":"2026-09-03T12:20:00Z","inputTokens":"50","outputTokens":"5","costInCny":14.4}` +
			`],"totalElements":2}}`))
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Domain: "hainahn.com", Provider: upstreamProviderTokenForce, BaseURL: server.URL, UserID: 123, BalanceUnit: 7.2}
	cred := tokenForceCredential{AccessToken: "access", RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	result, err := fetchTokenForceUsageWindow(context.Background(), server.Client(), row, cred, from, to, newUpstreamUsageRequestPacer(5, 0))
	if err != nil || result.Adapter != upstreamUsageAdapterTokenForce || result.DataUntil != to || len(result.Hours) != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	hour := result.Hours[0]
	if hour.HourTs != from || hour.BucketSeconds != 3600 || hour.Requests != 2 || hour.Tokens != 165 || math.Abs(hour.Quota-21.6) > 1e-9 || math.Abs(hour.CostUSD-3) > 1e-9 {
		t.Fatalf("unexpected hour: %+v", hour)
	}
}

func TestTokenForceUsageRejectsOutOfRangeRows(t *testing.T) {
	from := time.Date(2026, 9, 3, 20, 0, 0, 0, cstLocation).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":0,"result":{"content":[{"requestId":"req-outside","requestTime":"2026-09-03T13:00:00Z","inputTokens":1,"outputTokens":1,"costInCny":1}],"totalElements":1}}`))
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "hainahn.com", Provider: upstreamProviderTokenForce, BaseURL: server.URL, UserID: 123, BalanceUnit: 7.2}
	_, err := fetchTokenForceUsageWindow(context.Background(), server.Client(), row, tokenForceCredential{AccessToken: "access"}, from, from+3600, newUpstreamUsageRequestPacer(5, 0))
	if err == nil {
		t.Fatal("out-of-range usage row must fail the whole window")
	}
}

func TestTokenForceUsageRejectsDuplicateRequestIDsAfterSplit(t *testing.T) {
	items := []tokenForceUsageItem{{RequestID: "same"}, {RequestID: "same"}}
	if err := validateUniqueTokenForceUsageItems(items, "跨窗口"); err == nil {
		t.Fatal("cross-window duplicate request IDs must be rejected")
	}
}

func TestTokenForceUsageRefreshesOnceAfterUnauthorized(t *testing.T) {
	from := time.Date(2026, 9, 3, 20, 0, 0, 0, cstLocation).Unix()
	refreshCalls, usageCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/usages/detail":
			usageCalls++
			if r.Header.Get("Authorization") == "Bearer expired" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"status":10025,"message":"You must log in first","result":null}`))
				return
			}
			if r.Header.Get("Authorization") != "Bearer renewed" {
				t.Fatalf("unexpected authorization: %q", r.Header.Get("Authorization"))
			}
			_, _ = w.Write([]byte(`{"status":0,"result":{"content":[],"totalElements":0}}`))
		case "/api/sys/login/refresh":
			refreshCalls++
			_, _ = w.Write([]byte(`{"status":0,"result":{"token":"renewed","refreshToken":"rotated","expires":4102444800}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "hainahn.com", Provider: upstreamProviderTokenForce, BaseURL: server.URL, UserID: 123, BalanceUnit: 7.2}
	result, updated, err := syncTokenForceUsage(context.Background(), server.Client(), row,
		tokenForceCredential{AccessToken: "expired", RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour).Unix()},
		from, from+3600, newUpstreamUsageRequestPacer(5, 0))
	if err != nil || refreshCalls != 1 || usageCalls != 2 || updated.AccessToken != "renewed" || updated.RefreshToken != "rotated" || len(result.Hours) != 1 {
		t.Fatalf("result=%+v updated=%+v refresh=%d usage=%d err=%v", result, updated, refreshCalls, usageCalls, err)
	}
}

func TestTokenForceBusinessAuthErrorIsIsolated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":10025,"message":"You must log in first","result":null}`))
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Provider: upstreamProviderTokenForce, BaseURL: server.URL, UserID: 123, BalanceUnit: 7.2}
	_, err := tokenForceBalance(context.Background(), server.Client(), row, tokenForceCredential{AccessToken: "expired"})
	var authErr *upstreamAuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("business auth status must be classified as reconnect, err=%v", err)
	}
}

func TestValidateTokenForceConfigurationRequiresOrgRefreshAndCurrencyUnit(t *testing.T) {
	in := channelUpstreamSaveInput{Domain: "hainahn.com", Provider: upstreamProviderTokenForce, BaseURL: "https://maas.hainahn.com", UserID: 123, RefreshToken: "refresh", UnitPerUSD: 7.2}
	if err := validateChannelUpstreamInput(&in); err != nil {
		t.Fatalf("valid TokenForce configuration rejected: %v", err)
	}
	in.UnitPerUSD = 0
	if err := validateChannelUpstreamInput(&in); err == nil {
		t.Fatal("missing CNY/USD conversion must be rejected")
	}
}

func TestTokenForceCurrencyUnitChangeArchivesAndRebuildsUsageNamespace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/sys/login/refresh":
			_, _ = w.Write([]byte(`{"status":0,"result":{"token":"access","refreshToken":"rotated","expires":` + strconv.FormatInt(now+3600, 10) + `}}`))
		case "/api/orgs/123/balance":
			_, _ = w.Write([]byte(`{"status":0,"result":{"currentBalance":720}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 777, Name: "tokenforce", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	usageEnabled := true
	initial := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderTokenForce, BaseURL: server.URL, UserID: 123, RefreshToken: "imported", UnitPerUSD: 7.2, UsageSyncEnabled: &usageEnabled}
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", initial); w.Code != http.StatusOK {
		t.Fatalf("initial save status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: domain, HourTs: 3600, BucketSeconds: 3600, Requests: 1, Quota: 72, CostUSD: 10, Provider: upstreamProviderTokenForce}).Error; err != nil {
		t.Fatal(err)
	}
	changed := initial
	changed.RefreshToken = ""
	changed.UnitPerUSD = 8
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", changed); w.Code != http.StatusOK {
		t.Fatalf("unit change status=%d body=%s", w.Code, w.Body.String())
	}
	var row ChannelUpstreamAccount
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.BalanceUnit != 8 || row.BalanceRaw != 720 || row.BalanceUSD != 90 || row.UsageBackfillDone || row.UsageDataUntil != 0 {
		t.Fatalf("unexpected account after unit change: %+v", row)
	}
	var liveCount, archiveCount int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", domain).Count(&liveCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamUsageArchive{}).Where("domain = ?", domain).Count(&archiveCount).Error; err != nil {
		t.Fatal(err)
	}
	if liveCount != 0 || archiveCount != 1 {
		t.Fatalf("unit change did not atomically archive current usage: live=%d archive=%d", liveCount, archiveCount)
	}
}

func TestTokenForceDoesNotClaimPricingLedgerCapability(t *testing.T) {
	account := ChannelUpstreamAccount{Provider: upstreamProviderTokenForce, UsageAdapter: upstreamUsageAdapterTokenForce}
	if pricingLedgerProviderSupported(account.Provider) || pricingLedgerAccountSupported(account) {
		t.Fatal("TokenForce usage cost is not evidence of an upstream group multiplier")
	}
}

func TestTokenForceConfigurationUIExposesOnlyRequiredNonPasswordFields(t *testing.T) {
	page, js := pageHTML, string(channelManagementJS)
	for _, marker := range []string{
		`value="tokenforce"`, `id="cmUpstreamOrgID"`, `id="cmUpstreamUnitPerUSD"`,
		`id="cmUpstreamTokenForceRefreshToken"`, `provider==='tokenforce'`,
		`payload.unit_per_usd`, `payload.refresh_token`,
	} {
		if !strings.Contains(page, marker) && !strings.Contains(js, marker) {
			t.Fatalf("TokenForce configuration UI missing %q", marker)
		}
	}
	if strings.Contains(page, `cm-upstream-tokenforce"><span>密码`) {
		t.Fatal("TokenForce must not ask Monitor to retain an interactive-login password")
	}
}
