package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newChannelUpstreamTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	m := newStabilityTestMonitor(t)
	m.cfg.SessionSecret = "fixed-channel-upstream-test-secret"
	m.cfg.UpstreamSyncTimeoutSec = 3
	m.upstreamCredentialPersistent = true
	m.upstreamClient = newUpstreamHTTPClient(upstreamSyncTimeout(m.cfg))
	t.Cleanup(m.upstreamClient.CloseIdleConnections)
	return m
}

func TestChannelUpstreamCredentialEncryptionAndURLValidation(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	const token = "secret-token-that-must-not-appear"
	sealed, err := m.sealUpstreamCredential("last-api.ai", upstreamProviderNewAPI, newAPICredential{AccessToken: token})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, token) {
		t.Fatal("credential ciphertext contains plaintext token")
	}
	row := ChannelUpstreamAccount{Domain: "last-api.ai", Provider: upstreamProviderNewAPI, Credential: sealed, CredentialVersion: upstreamCredentialVersion}
	var decoded newAPICredential
	if err := m.openUpstreamCredential(row, &decoded); err != nil || decoded.AccessToken != token {
		t.Fatalf("decrypt credential: token=%q err=%v", decoded.AccessToken, err)
	}
	row.Domain = "other.example"
	if err := m.openUpstreamCredential(row, &decoded); err == nil {
		t.Fatal("ciphertext must be bound to domain/provider through AEAD AAD")
	}

	valid := channelUpstreamSaveInput{Domain: "last-api.ai", Provider: upstreamProviderNewAPI, BaseURL: "https://panel.last-api.ai/", UserID: 7}
	if err := validateChannelUpstreamInput(&valid); err != nil || valid.BaseURL != "https://panel.last-api.ai" {
		t.Fatalf("valid upstream URL rejected or not normalized: base=%q err=%v", valid.BaseURL, err)
	}
	for _, input := range []channelUpstreamSaveInput{
		{Domain: "last-api.ai", Provider: upstreamProviderNewAPI, BaseURL: "http://panel.last-api.ai", UserID: 7},
		{Domain: "last-api.ai", Provider: upstreamProviderNewAPI, BaseURL: "https://evil.example", UserID: 7},
		{Domain: "last-api.ai", Provider: upstreamProviderNewAPI, BaseURL: "https://panel.last-api.ai?token=x", UserID: 7},
	} {
		if err := validateChannelUpstreamInput(&input); err == nil {
			t.Fatalf("unsafe/mismatched upstream URL accepted: %+v", input)
		}
	}
}

func TestSyncNewAPIBalanceUsesUserTokenAndPublishedUnit(t *testing.T) {
	const token = "newapi-access"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/user/self":
			if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("New-Api-User") != "23" {
				http.Error(w, `{"message":"bad auth"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":"1230000"}}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":600000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 23}
	result, _, err := syncNewAPIBalance(context.Background(), newUpstreamHTTPClient(3*time.Second), row, newAPICredential{AccessToken: token})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(result.BalanceUSD-2.05) > 1e-12 || result.BalanceRaw != 1230000 || result.BalanceUnit != 600000 || result.UnitAssumed {
		t.Fatalf("unexpected NewAPI balance result: %+v", result)
	}
}

func TestSub2APILoginRefreshAndBalance(t *testing.T) {
	var refreshCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/login":
			var input map[string]string
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input["email"] != "ops@example.com" || input["password"] != "one-time-password" {
				http.Error(w, `{"message":"bad login"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"access-1","refresh_token":"refresh-1","expires_in":3600}}`))
		case "/api/v1/auth/refresh":
			refreshCalls.Add(1)
			var input map[string]string
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input["refresh_token"] != "refresh-1" {
				http.Error(w, `{"message":"bad refresh"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"access-2","refresh_token":"refresh-2","expires_in":3600}}`))
		case "/api/v1/user/profile":
			if r.Header.Get("Authorization") != "Bearer access-1" && r.Header.Get("Authorization") != "Bearer access-2" {
				http.Error(w, `{"message":"bad bearer"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"balance":"42.75"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newUpstreamHTTPClient(3 * time.Second)
	defer client.CloseIdleConnections()
	row := ChannelUpstreamAccount{Provider: upstreamProviderSub2API, BaseURL: server.URL, Account: "ops@example.com"}

	credential, err := loginSub2API(context.Background(), client, row, "one-time-password")
	if err != nil {
		t.Fatal(err)
	}
	result, credential, err := syncSub2APIBalance(context.Background(), client, row, credential)
	if err != nil || math.Abs(result.BalanceUSD-42.75) > 1e-12 || refreshCalls.Load() != 0 {
		t.Fatalf("initial Sub2API sync result=%+v credential=%+v refreshes=%d err=%v", result, credential, refreshCalls.Load(), err)
	}
	credential.ExpiresAt = time.Now().Add(-time.Minute).Unix()
	result, credential, err = syncSub2APIBalance(context.Background(), client, row, credential)
	if err != nil || credential.AccessToken != "access-2" || credential.RefreshToken != "refresh-2" || refreshCalls.Load() != 1 {
		t.Fatalf("rotating refresh failed: result=%+v credential=%+v refreshes=%d err=%v", result, credential, refreshCalls.Load(), err)
	}
}

func upstreamRouteRequest(t *testing.T, m *Monitor, router http.Handler, role int, method, target string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, target, &body)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", role, time.Now().Unix())})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestChannelUpstreamNewAPIHandlersProtectSecretsAndPreserveLastBalance(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if fail.Load() {
			reflected := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			http.Error(w, `{"message":"temporary failure `+reflected+`"}`, http.StatusBadGateway)
			return
		}
		switch r.URL.Path {
		case "/api/user/self":
			if r.Header.Get("Authorization") != "Bearer handler-secret-token" || r.Header.Get("New-Api-User") != "9" {
				http.Error(w, `{"message":"bad auth"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":2500000}}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "mock", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.GET("/channels/upstream", m.requireRole(roleRoot), m.getChannelUpstreamHandler)
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	router.POST("/channels/upstream/sync", m.requireRole(roleRoot), m.syncChannelUpstreamHandler)
	payload := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 9, AccessToken: "handler-secret-token"}

	if w := upstreamRouteRequest(t, m, router, roleAdmin, http.MethodPost, "/channels/upstream", payload); w.Code != http.StatusForbidden {
		t.Fatalf("admin must not edit upstream credentials: status=%d body=%s", w.Code, w.Body.String())
	}
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload)
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "handler-secret-token") || !strings.Contains(w.Body.String(), `"balance_usd":5`) {
		t.Fatalf("save response leaked secret or has wrong balance: status=%d body=%s", w.Code, w.Body.String())
	}
	var row ChannelUpstreamAccount
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(row.Credential, "handler-secret-token") || !row.BalanceKnown || row.BalanceUSD != 5 {
		t.Fatalf("invalid stored upstream state: %+v", row)
	}
	var credential newAPICredential
	if err := m.openUpstreamCredential(row, &credential); err != nil || credential.AccessToken != "handler-secret-token" {
		t.Fatalf("stored token not recoverable: credential=%+v err=%v", credential, err)
	}

	get := upstreamRouteRequest(t, m, router, roleRoot, http.MethodGet, "/channels/upstream?domain="+domain, nil)
	if get.Code != http.StatusOK || strings.Contains(get.Body.String(), "handler-secret-token") || !strings.Contains(get.Body.String(), `"user_id":9`) {
		t.Fatalf("config GET leaked secret or omitted account: status=%d body=%s", get.Code, get.Body.String())
	}

	fail.Store(true)
	w = upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream/sync", channelUpstreamSyncInput{Domain: domain})
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"sync_error"`) || strings.Contains(w.Body.String(), "handler-secret-token") {
		t.Fatalf("transient sync failure must be represented without erasing state: status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if !row.BalanceKnown || row.BalanceUSD != 5 || row.Status != upstreamStatusError || strings.Contains(row.LastError, "handler-secret-token") {
		t.Fatalf("last known balance was not preserved: %+v", row)
	}

	sealedBeforeWrongKey := row.Credential
	m.cfg.UpstreamCredentialSecret = "temporarily-wrong-key"
	w = upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream/sync", channelUpstreamSyncInput{Domain: domain})
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"sync_error"`) {
		t.Fatalf("wrong key must mark reconnect without destructive overwrite: status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.Credential != sealedBeforeWrongKey || row.BalanceUSD != 5 || row.Status != upstreamStatusReconnect {
		t.Fatalf("decrypt failure overwrote recoverable state: %+v", row)
	}

	// 改成另一个账户后，即使首次同步失败，也绝不能继续展示前一个账户的余额。
	m.cfg.UpstreamCredentialSecret = ""
	switched := channelUpstreamSaveInput{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL,
		UserID: 10, AccessToken: "new-account-token",
	}
	w = upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", switched)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"sync_error"`) || strings.Contains(w.Body.String(), "new-account-token") {
		t.Fatalf("new identity transient failure must persist unknown state: status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.UserID != 10 || row.BalanceKnown || row.BalanceUSD != 0 || row.LastSuccessAt != 0 || strings.Contains(row.LastError, "new-account-token") {
		t.Fatalf("new account inherited stale balance from previous identity: %+v", row)
	}
}

func TestChannelUpstreamSub2APILoginErrorRedactsReflectedPassword(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const password = "password-that-must-never-leak"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v1/auth/login" {
			http.NotFound(w, r)
			return
		}
		var input map[string]string
		_ = json.NewDecoder(r.Body).Decode(&input)
		http.Error(w, `{"message":"invalid password: `+input["password"]+`"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 3, Name: "sub2-error", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	payload := channelUpstreamSaveInput{
		Domain: domain, Provider: upstreamProviderSub2API, BaseURL: server.URL,
		Email: "finance@example.com", Password: password,
	}
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload)
	if w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), password) || !strings.Contains(w.Body.String(), "[REDACTED]") {
		t.Fatalf("reflected password was not redacted: status=%d body=%s", w.Code, w.Body.String())
	}
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", domain).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("failed login must not persist an account: count=%d err=%v", count, err)
	}
}

func TestChannelUpstreamSub2APISaveNeverPersistsPassword(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/login":
			var input map[string]string
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input["email"] != "finance@example.com" || input["password"] != "never-store-this-password" {
				http.Error(w, `{"message":"bad login"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"sub-access","refresh_token":"sub-refresh","expires_in":3600}}`))
		case "/api/v1/user/profile":
			_, _ = w.Write([]byte(`{"code":0,"data":{"balance":88.5}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 2, Name: "sub2", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	payload := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderSub2API, BaseURL: server.URL, Email: "finance@example.com", Password: "never-store-this-password"}
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload)
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "never-store-this-password") || strings.Contains(w.Body.String(), "sub-access") {
		t.Fatalf("Sub2API response leaked credentials: status=%d body=%s", w.Code, w.Body.String())
	}
	var row ChannelUpstreamAccount
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(row.Credential, "never-store-this-password") || strings.Contains(row.Credential, "sub-access") {
		t.Fatalf("Sub2API credential is not encrypted: %q", row.Credential)
	}
	var credential sub2APICredential
	if err := m.openUpstreamCredential(row, &credential); err != nil {
		t.Fatal(err)
	}
	if credential.AccessToken != "sub-access" || credential.RefreshToken != "sub-refresh" {
		t.Fatalf("wrong stored Sub2API token pair: %+v", credential)
	}
	encoded, _ := json.Marshal(credential)
	if strings.Contains(string(encoded), "never-store-this-password") {
		t.Fatal("Sub2API password persisted in encrypted credential payload")
	}
}

func TestUpstreamHTTPClientDoesNotForwardTokenAcrossRedirect(t *testing.T) {
	var targetHits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client := newUpstreamHTTPClient(3 * time.Second)
	defer client.CloseIdleConnections()
	_, err := doUpstreamJSON(context.Background(), client, http.MethodGet, redirect.URL, map[string]string{"Authorization": "Bearer must-not-forward"}, nil)
	if err == nil || targetHits.Load() != 0 {
		t.Fatalf("redirect was followed with credential-bearing request: hits=%d err=%v", targetHits.Load(), err)
	}
}

func TestSyncDueUpstreamAccountsOnlyRunsDueEnabledRows(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/user/self":
			hits.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":500000}}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	now := time.Now().Unix()
	rows := []ChannelUpstreamAccount{
		{Domain: "due.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 1, Enabled: true, NextSyncAt: 0},
		{Domain: "future.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 2, Enabled: true, NextSyncAt: now + 3600},
		{Domain: "disabled.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 3, Enabled: false, NextSyncAt: 0},
	}
	for i := range rows {
		sealed, err := m.sealUpstreamCredential(rows[i].Domain, rows[i].Provider, newAPICredential{AccessToken: "token"})
		if err != nil {
			t.Fatal(err)
		}
		rows[i].Credential = sealed
		rows[i].CredentialVersion = upstreamCredentialVersion
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}

	m.syncDueUpstreamAccounts(context.Background())
	if hits.Load() != 1 {
		t.Fatalf("后台同步只应请求到期且启用的一个账户，实际 %d", hits.Load())
	}
	var due, future, disabled ChannelUpstreamAccount
	if err := m.storeDB.First(&due, "domain = ?", "due.example").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&future, "domain = ?", "future.example").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&disabled, "domain = ?", "disabled.example").Error; err != nil {
		t.Fatal(err)
	}
	if due.Status != upstreamStatusOK || !due.BalanceKnown || due.BalanceUSD != 1 || due.NextSyncAt <= now {
		t.Fatalf("到期账户未正常同步: %+v", due)
	}
	if future.LastAttemptAt != 0 || disabled.LastAttemptAt != 0 {
		t.Fatalf("未到期或停用账户不应被请求: future=%+v disabled=%+v", future, disabled)
	}
}
