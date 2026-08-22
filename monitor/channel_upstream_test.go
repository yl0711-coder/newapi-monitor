package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	guard := newUpstreamHostGuard(m.storeDB, upstreamHostGuardOptions{
		Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0,
	})
	m.upstreamClient = installUpstreamHostGuardForTest(newUpstreamHTTPClient(upstreamSyncTimeout(m.cfg)), m.storeDB, guard)
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

func TestLegacyUpstreamCredentialsAreTransactionallyRotatedToDedicatedSecret(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	const (
		domain    = "legacy-upstream.example"
		oldSecret = "legacy-session-secret-for-upstream"
		newSecret = "dedicated-upstream-secret-after-upgrade"
		token     = "legacy-access-token"
	)
	m.cfg.SessionSecret = oldSecret
	m.cfg.UpstreamCredentialSecret = ""
	sealed, err := m.sealUpstreamCredential(domain, upstreamProviderNewAPI, newAPICredential{AccessToken: token})
	if err != nil {
		t.Fatal(err)
	}
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, Credential: sealed,
		CredentialVersion: upstreamCredentialVersion,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}

	m.cfg.UpstreamCredentialSecret = newSecret
	if err := m.migrateLegacyUpstreamCredentialEncryption(); err != nil {
		t.Fatal(err)
	}
	var rotated ChannelUpstreamAccount
	if err := m.storeDB.First(&rotated, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if rotated.Credential == sealed {
		t.Fatal("legacy credential was not re-sealed with the dedicated secret")
	}
	var decoded newAPICredential
	if err := m.openUpstreamCredential(rotated, &decoded); err != nil || decoded.AccessToken != token {
		t.Fatalf("dedicated secret cannot open rotated credential: token=%q err=%v", decoded.AccessToken, err)
	}

	legacy := &Monitor{cfg: m.cfg}
	legacy.cfg.UpstreamCredentialSecret = ""
	decoded = newAPICredential{}
	if err := legacy.openUpstreamCredential(rotated, &decoded); err == nil {
		t.Fatal("legacy session secret must not open a credential after rotation")
	}

	firstRotation := rotated.Credential
	if err := m.migrateLegacyUpstreamCredentialEncryption(); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&rotated, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if rotated.Credential != firstRotation {
		t.Fatal("idempotent migration unexpectedly re-encrypted an already rotated credential")
	}
}

func TestLegacyUpstreamCredentialRotationRollsBackAllRowsOnCorruption(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	const (
		oldSecret = "legacy-session-secret-for-rollback"
		newSecret = "dedicated-upstream-secret-for-rollback"
	)
	m.cfg.SessionSecret = oldSecret
	m.cfg.UpstreamCredentialSecret = ""
	sealed, err := m.sealUpstreamCredential("a-valid.example", upstreamProviderNewAPI, newAPICredential{AccessToken: "valid-token"})
	if err != nil {
		t.Fatal(err)
	}
	rows := []ChannelUpstreamAccount{
		{Domain: "a-valid.example", Provider: upstreamProviderNewAPI, Credential: sealed, CredentialVersion: upstreamCredentialVersion},
		{Domain: "z-corrupt.example", Provider: upstreamProviderNewAPI, Credential: "not-valid-base64!", CredentialVersion: upstreamCredentialVersion},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}

	m.cfg.UpstreamCredentialSecret = newSecret
	if err := m.migrateLegacyUpstreamCredentialEncryption(); err == nil {
		t.Fatal("corrupt credential must abort startup key rotation")
	}
	var after ChannelUpstreamAccount
	if err := m.storeDB.First(&after, "domain = ?", "a-valid.example").Error; err != nil {
		t.Fatal(err)
	}
	if after.Credential != sealed {
		t.Fatal("transaction committed a partial key rotation before encountering corruption")
	}
	legacy := &Monitor{cfg: m.cfg}
	legacy.cfg.UpstreamCredentialSecret = ""
	var decoded newAPICredential
	if err := legacy.openUpstreamCredential(after, &decoded); err != nil || decoded.AccessToken != "valid-token" {
		t.Fatalf("rollback did not preserve the legacy credential: token=%q err=%v", decoded.AccessToken, err)
	}
}

func TestOpenStoreAutomaticallyRotatesLegacyUpstreamCredentials(t *testing.T) {
	const (
		oldSecret = "legacy-session-secret-used-by-old-release"
		newSecret = "dedicated-upstream-secret-used-by-candidate"
		domain    = "startup-rotation.example"
	)
	dir := t.TempDir()
	mainPath := dir + "/nexus_monitor.db"
	factsPath := dir + "/usage-facts.db"
	legacy := &Monitor{cfg: Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath, SessionSecret: oldSecret,
	}}
	if err := legacy.openStore(mainPath); err != nil {
		t.Fatal(err)
	}
	sealed, err := legacy.sealUpstreamCredential(domain, upstreamProviderNewAPI, newAPICredential{AccessToken: "startup-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, Credential: sealed,
		CredentialVersion: upstreamCredentialVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	legacyMainSQL, err := legacy.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	legacyFactsSQL, err := legacy.usageFactsDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyMainSQL.Close(); err != nil {
		t.Fatal(err)
	}
	if err := legacyFactsSQL.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := &Monitor{cfg: Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath,
		SessionSecret: oldSecret, UpstreamCredentialSecret: newSecret,
	}}
	if err := upgraded.openStore(mainPath); err != nil {
		t.Fatalf("candidate startup did not rotate legacy credentials: %v", err)
	}
	t.Cleanup(func() {
		if db, err := upgraded.storeDB.DB(); err == nil {
			_ = db.Close()
		}
		if db, err := upgraded.usageFactsDB.DB(); err == nil {
			_ = db.Close()
		}
	})
	var row ChannelUpstreamAccount
	if err := upgraded.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.Credential == sealed {
		t.Fatal("openStore returned without rotating the legacy ciphertext")
	}
	var credential newAPICredential
	if err := upgraded.openUpstreamCredential(row, &credential); err != nil || credential.AccessToken != "startup-token" {
		t.Fatalf("rotated startup credential is unavailable: token=%q err=%v", credential.AccessToken, err)
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

func TestSyncAICodeWithBalanceUsesAPIKeyAndUSDResponse(t *testing.T) {
	const apiKey = "sk-acw-balance-test-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/balance" || r.Header.Get("Authorization") != "Bearer "+apiKey {
			http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"bad key `+apiKey+`"}}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"balance":"123.4567","currency":"USD"}}`))
	}))
	defer server.Close()
	client := newUpstreamHTTPClient(3 * time.Second)
	defer client.CloseIdleConnections()
	result, _, err := syncAICodeWithBalance(context.Background(), client, ChannelUpstreamAccount{
		Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}, aiCodeWithCredential{APIKey: apiKey})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(result.BalanceUSD-123.4567) > 1e-12 || result.BalanceRaw != result.BalanceUSD || result.BalanceUnit != 1 || result.UnitAssumed {
		t.Fatalf("unexpected AICodeWith balance: %+v", result)
	}
	if got := aiCodeWithKeyIdentity([]string{apiKey}); strings.Contains(got, apiKey) || !strings.HasPrefix(got, "keys:1:") {
		t.Fatalf("API key identity is not a safe fingerprint: %q", got)
	}
}

func TestSyncAICodeWithBalanceKeepsCNYLedgerAtContractOneToOne(t *testing.T) {
	const apiKey = "sk-acw-cny-balance-test"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"balance":"700.00","currency":"CNY"}}`))
	}))
	defer server.Close()

	result, _, err := syncAICodeWithBalance(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}, aiCodeWithCredential{APIKey: apiKey})
	if err != nil {
		t.Fatal(err)
	}
	if result.BalanceRaw != 700 || result.BalanceUnit != 1 || math.Abs(result.BalanceUSD-700) > 1e-12 {
		t.Fatalf("unexpected 1:1 CNY balance: %+v", result)
	}
}

func TestMigrateAICodeWithContractLedgerUnitIsAtomicAndIdempotent(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: "aicodewith.com", Provider: upstreamProviderAICodeWith,
		BalanceKnown: true, BalanceRaw: 700, BalanceUSD: 100, BalanceUnit: 7,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: 1, CostUSD: 10, Quota: 70}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&AICodeWithUsageStage{Domain: row.Domain, RoundID: "r1", SlotID: "acw_1", HourTs: 1, CostUSD: 5, Quota: 35}).Error; err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		if err := m.migrateAICodeWithContractLedgerUnit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.First(&row, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	var usage ChannelUpstreamUsageHour
	var stage AICodeWithUsageStage
	if err := m.storeDB.First(&usage, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&stage, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.BalanceUSD != 700 || row.BalanceUnit != 1 || usage.CostUSD != 70 || stage.CostUSD != 35 {
		t.Fatalf("unexpected migrated 1:1 ledger: row=%+v usage=%+v stage=%+v", row, usage, stage)
	}
}

func TestAICodeWithNestedErrorMessageIsParsedWithoutLeakingKey(t *testing.T) {
	const apiKey = "sk-acw-reflected-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"invalid `+apiKey+`"}}`, http.StatusUnauthorized)
	}))
	defer server.Close()
	_, _, err := syncAICodeWithBalance(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}, aiCodeWithCredential{APIKey: apiKey})
	if err == nil {
		t.Fatal("unauthorized response unexpectedly succeeded")
	}
	message := sanitizeUpstreamErrorWithSecrets(err, apiKey)
	if strings.Contains(message, apiKey) || !strings.Contains(message, "[REDACTED]") {
		t.Fatalf("nested upstream error leaked API key: %q", message)
	}
}

func TestSyncAICodeWithBalanceRejectsKeysFromDifferentAccounts(t *testing.T) {
	keys := []string{"sk-acw-account-a", "sk-acw-account-b"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		balance := "10.00"
		if r.Header.Get("Authorization") == "Bearer "+keys[1] {
			balance = "20.00"
		}
		_, _ = fmt.Fprintf(w, `{"data":{"balance":%q,"currency":"USD"}}`, balance)
	}))
	defer server.Close()
	_, _, err := syncAICodeWithBalance(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}, aiCodeWithCredential{APIKeys: keys})
	if err == nil || !strings.Contains(err.Error(), "余额不一致") {
		t.Fatalf("keys from different balance accounts must not be merged: %v", err)
	}
}

func TestAICodeWithDynamicKeyListSafetyBoundaryAndMaskedCount(t *testing.T) {
	keys := make([]string, maxAICodeWithAPIKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("sk-acw-dynamic-%03d", i)
	}
	normalized, err := normalizeAICodeWithAPIKeys("", keys)
	if err != nil || len(normalized) != maxAICodeWithAPIKeys {
		t.Fatalf("dynamic key list rejected: count=%d err=%v", len(normalized), err)
	}
	tooMany := append(append([]string(nil), keys...), "sk-acw-over-limit")
	if _, err := normalizeAICodeWithAPIKeys("", tooMany); err == nil {
		t.Fatal("oversized key list must retain a bounded request/credential safety limit")
	}
	view := upstreamAccountView(ChannelUpstreamAccount{
		Provider: upstreamProviderAICodeWith,
		Account:  aiCodeWithKeyIdentity(keys),
	})
	if view.APIKeyCount != len(keys) || view.AccountMasked != fmt.Sprintf("%d 把 API Key", len(keys)) {
		t.Fatalf("masked key count = %+v", view)
	}
}

func TestStoredAICodeWithBalanceRefreshDoesNotGrowWithKeyCount(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/api/v1/balance" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer sk-acw-") {
			http.Error(w, `{"message":"bad request"}`, http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"balance":"19.25","currency":"USD"}`))
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	domain := normalizeChannelBaseDomain(server.URL)
	keys := []string{"sk-acw-one", "sk-acw-two", "sk-acw-three", "sk-acw-four"}
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
		Account: aiCodeWithKeyIdentity(keys), Enabled: true, Status: upstreamStatusPending,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, aiCodeWithCredential{APIKeys: keys}); err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamAccount(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || !synced.BalanceKnown || synced.BalanceUSD != 19.25 {
		t.Fatalf("periodic account snapshot should use one verified key: requests=%d row=%+v", requests.Load(), synced)
	}
	var stored aiCodeWithCredential
	if err := m.openUpstreamCredential(synced, &stored); err != nil {
		t.Fatal(err)
	}
	storedKeys, err := aiCodeWithCredentialKeys(stored)
	if err != nil || len(storedKeys) != len(keys) {
		t.Fatalf("periodic balance refresh lost configured keys: keys=%v err=%v", storedKeys, err)
	}
}

func TestStoredAICodeWithBalanceFallsBackOnlyAfterExpiredKey(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.Header.Get("Authorization") {
		case "Bearer sk-acw-expired":
			http.Error(w, `{"message":"expired"}`, http.StatusUnauthorized)
		case "Bearer sk-acw-healthy":
			_, _ = w.Write([]byte(`{"balance":"27.50","currency":"USD"}`))
		default:
			http.Error(w, `{"message":"unexpected"}`, http.StatusForbidden)
		}
	}))
	defer server.Close()
	result, normalized, err := syncAICodeWithBalanceSnapshot(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}, aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{
		{SlotID: "acw_slot_01", Secret: "sk-acw-expired"},
		{SlotID: "acw_slot_02", Secret: "sk-acw-healthy"},
		{SlotID: "acw_slot_03", Secret: "sk-acw-unused"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	keys, keyErr := aiCodeWithCredentialKeys(normalized)
	if keyErr != nil || len(keys) != 3 || result.BalanceUSD != 27.5 || requests.Load() != 2 {
		t.Fatalf("expired-key fallback result=%+v keys=%d requests=%d err=%v", result, len(keys), requests.Load(), keyErr)
	}
}

func TestFetchNewAPIUsageWindowAggregatesLocallyAndKeepsZeroHours(t *testing.T) {
	const token = "usage-access"
	from := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC).Unix()
	to := from + 3*3600
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/log/" || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("New-Api-User") != "31" {
			http.Error(w, `{"message":"bad request"}`, http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("type") != "2" || r.URL.Query().Get("p") != "1" || r.URL.Query().Get("page_size") != "100" || r.URL.Query().Get("cursor") != "" || r.URL.Query().Get("start_timestamp") != strconv.FormatInt(from, 10) || r.URL.Query().Get("end_timestamp") != strconv.FormatInt(to-1, 10) || r.URL.Query().Get("before_id") != "" || r.URL.Query().Get("skip_total") != "" {
			http.Error(w, `{"message":"bad range"}`, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(fmt.Sprintf(`{"success":true,"data":{"total":2,"items":[{"id":1,"created_at":%d,"quota":500000,"prompt_tokens":10,"completion_tokens":2},{"id":2,"created_at":%d,"quota":"250000","prompt_tokens":"3","completion_tokens":"1"}]}}`, from+30, from+3700)))
	}))
	defer server.Close()

	result, err := fetchNewAPIUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "example.com", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
	}, newAPICredential{AccessToken: token}, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hours) != 3 { // 包括无消费的第三个完整小时。
		t.Fatalf("hours=%d, want 3", len(result.Hours))
	}
	if result.Hours[0].Requests != 1 || result.Hours[0].Tokens != 12 || result.Hours[0].CostUSD != 1 {
		t.Fatalf("first hour=%+v", result.Hours[0])
	}
	if result.Hours[1].Requests != 1 || result.Hours[1].Tokens != 4 || result.Hours[1].CostUSD != .5 {
		t.Fatalf("second hour=%+v", result.Hours[1])
	}
	if result.Hours[2].Requests != 0 || result.Hours[2].Quota != 0 || result.Hours[2].CostUSD != 0 {
		t.Fatalf("zero hour=%+v", result.Hours[2])
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

	// A replacement credential for the same account is an explicit recovery
	// action for both balance and usage auth isolation. Existing usage facts and
	// history cursor remain intact; only the retry gates are reopened.
	m.cfg.UpstreamCredentialSecret = ""
	fail.Store(false)
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", domain).Updates(map[string]any{
		"usage_sync_enabled":               true,
		"usage_status":                     upstreamStatusReconnect,
		"usage_next_sync_at":               upstreamAccountIsolatedUntil,
		"usage_backfill_next_sync_at":      upstreamAccountIsolatedUntil,
		"usage_consecutive_fails":          3,
		"usage_backfill_consecutive_fails": 2,
		"usage_last_error":                 "authorization failed",
		"usage_backfill_last_error":        "authorization failed",
		"usage_backfill_cursor":            int64(123456),
	}).Error; err != nil {
		t.Fatal(err)
	}
	usageEnabled := true
	payload.UsageSyncEnabled = &usageEnabled
	w = upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload)
	if w.Code != http.StatusOK {
		t.Fatalf("replacement credential did not recover account: status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.UsageStatus != upstreamStatusPending || row.UsageNextSyncAt != 0 || row.UsageBackfillNextSyncAt != 0 ||
		row.UsageConsecutiveFails != 0 || row.UsageBackfillConsecutiveFails != 0 || row.UsageBackfillCursor != 123456 {
		t.Fatalf("replacement credential did not reopen usage without losing progress: %+v", row)
	}

	// 改成另一个账户后，即使首次同步失败，也绝不能继续展示前一个账户的余额。
	fail.Store(true)
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

func TestChannelUpstreamAICodeWithHandlerEncryptsKeyAndEnablesUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const apiKey = "sk-acw-handler-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/balance" || r.Header.Get("Authorization") != "Bearer "+apiKey {
			http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"invalid `+apiKey+`"}}`, http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"balance":"9.75","currency":"USD"}`))
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 77, Name: "aicodewith", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	usageEnabled := true
	payload := channelUpstreamSaveInput{
		Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
		AddAPIKeySlots: []aicodeWithKeyAdditionInput{{Name: "主账号", APIKey: apiKey}}, UsageSyncEnabled: &usageEnabled,
	}
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload)
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), apiKey) || !strings.Contains(w.Body.String(), `"balance_usd":9.75`) || !strings.Contains(w.Body.String(), `"name":"主账号"`) {
		t.Fatalf("save response invalid or leaked API key: status=%d body=%s", w.Code, w.Body.String())
	}
	var row ChannelUpstreamAccount
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if row.Provider != upstreamProviderAICodeWith || !row.UsageSyncEnabled || row.Account != aiCodeWithKeyIdentity([]string{apiKey}) || strings.Contains(row.Credential, apiKey) {
		t.Fatalf("invalid stored AICodeWith account: %+v", row)
	}
	var credential aiCodeWithCredential
	if err := m.openUpstreamCredential(row, &credential); err != nil {
		t.Fatalf("encrypted AICodeWith key cannot be recovered: credential=%+v err=%v", credential, err)
	}
	keys, keysErr := aiCodeWithCredentialKeys(credential)
	if keysErr != nil || len(keys) != 1 || keys[0] != apiKey {
		t.Fatalf("encrypted AICodeWith key cannot be recovered: credential=%+v err=%v", credential, keysErr)
	}
	if len(credential.Slots) != 1 || credential.Slots[0].Name != "主账号" {
		t.Fatalf("AICodeWith key name was not stored with the encrypted slot: %+v", credential.Slots)
	}
}

func TestChannelUpstreamAICodeWithRenameDoesNotCallUpstreamOrResetProgress(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"balance":"99","currency":"USD"}`))
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 79, Name: "aicodewith-label", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	cred, err := normalizeAICodeWithCredential(aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{{SlotID: "acw_primary", Secret: "sk-acw-existing-valid"}}})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := aiCodeWithCredentialIdentity(cred)
	if err != nil {
		t.Fatal(err)
	}
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL, Account: identity,
		Enabled: true, BalanceUSD: 88.5, BalanceRaw: 88.5, BalanceUnit: 1, BalanceKnown: true,
		Status: upstreamStatusOK, LastAttemptAt: 101, LastSuccessAt: 100, NextSyncAt: 999,
		UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageLastSuccessAt: 90,
		CreatedAt: 1, UpdatedAt: 2,
	}
	if err := m.sealUpstreamAccountCredential(&row, cred); err != nil {
		t.Fatal(err)
	}
	if err := m.persistAICodeWithAccountChange(context.Background(), &row, cred, false); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&AICodeWithKeySyncState{}).Where("domain = ? AND slot_id = ?", domain, "acw_primary").Updates(map[string]any{
		"status": upstreamStatusOK, "last_success_at": int64(77), "backfill_cursor": int64(66), "backfill_done": true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	usageEnabled, enabled := true, true
	payload := channelUpstreamSaveInput{
		Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL, Enabled: &enabled, UsageSyncEnabled: &usageEnabled,
		RenameAPIKeySlots: []aicodeWithKeyRenameInput{{SlotID: "acw_primary", Name: "  Claude 主线路  "}},
	}
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload)
	if w.Code != http.StatusOK || requests.Load() != 0 || strings.Contains(w.Body.String(), "sk-acw-") || !strings.Contains(w.Body.String(), "Claude 主线路") {
		t.Fatalf("rename response=%d requests=%d body=%s", w.Code, requests.Load(), w.Body.String())
	}
	var stored ChannelUpstreamAccount
	if err := m.storeDB.First(&stored, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if stored.BalanceUSD != 88.5 || stored.Status != upstreamStatusOK || stored.LastSuccessAt != 100 || stored.NextSyncAt != 999 {
		t.Fatalf("rename changed account sync state: %+v", stored)
	}
	var opened aiCodeWithCredential
	if err := m.openUpstreamCredential(stored, &opened); err != nil {
		t.Fatal(err)
	}
	if len(opened.Slots) != 1 || opened.Slots[0].SlotID != "acw_primary" || opened.Slots[0].Name != "Claude 主线路" || opened.Slots[0].Secret != "sk-acw-existing-valid" {
		t.Fatalf("renamed credential=%+v", opened.Slots)
	}
	var state AICodeWithKeySyncState
	if err := m.storeDB.First(&state, "domain = ? AND slot_id = ?", domain, "acw_primary").Error; err != nil {
		t.Fatal(err)
	}
	if state.LastSuccessAt != 77 || state.BackfillCursor != 66 || !state.BackfillDone {
		t.Fatalf("rename reset key progress: %+v", state)
	}
}

func TestChannelUpstreamAICodeWithRejectedKeyChangeKeepsPublishedConfiguration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const oldKey = "sk-acw-existing-valid"
	const badKey = "sk-acw-rejected-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/balance" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") == "Bearer "+badKey {
			http.Error(w, `{"error":{"message":"invalid `+badKey+`"}}`, http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+oldKey {
			http.Error(w, `{"error":{"message":"unknown key"}}`, http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"balance":"9.75","currency":"USD"}`))
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 78, Name: "aicodewith", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	usageEnabled := true
	initial := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL, APIKeys: []string{oldKey}, UsageSyncEnabled: &usageEnabled}
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", initial); w.Code != http.StatusOK {
		t.Fatalf("initial save=%d %s", w.Code, w.Body.String())
	}
	var before ChannelUpstreamAccount
	if err := m.storeDB.First(&before, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	usage := ChannelUpstreamUsageHour{Domain: domain, HourTs: 123456, BucketSeconds: 86400, CostUSD: 7, Provider: upstreamProviderAICodeWith}
	if err := m.storeDB.Create(&usage).Error; err != nil {
		t.Fatal(err)
	}
	change := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL, AddAPIKeys: []string{badKey}, UsageSyncEnabled: &usageEnabled}
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", change)
	if w.Code == http.StatusOK || strings.Contains(w.Body.String(), badKey) || !strings.Contains(w.Body.String(), "原配置未修改") {
		t.Fatalf("failed key change was not rejected safely: status=%d body=%s", w.Code, w.Body.String())
	}
	var after ChannelUpstreamAccount
	if err := m.storeDB.First(&after, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if after.Credential != before.Credential || after.Account != before.Account || after.BalanceUSD != before.BalanceUSD || after.Status != before.Status {
		t.Fatalf("rejected key change modified the published account: before=%+v after=%+v", before, after)
	}
	var credential aiCodeWithCredential
	if err := m.openUpstreamCredential(after, &credential); err != nil {
		t.Fatal(err)
	}
	keys, err := aiCodeWithCredentialKeys(credential)
	if err != nil || len(keys) != 1 || keys[0] != oldKey {
		t.Fatalf("rejected key entered encrypted set: keys=%v err=%v", keys, err)
	}
	var usageCount int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ? AND hour_ts = ?", domain, usage.HourTs).Count(&usageCount).Error; err != nil || usageCount != 1 {
		t.Fatalf("rejected key change modified published usage: count=%d err=%v", usageCount, err)
	}
}

func TestUpstreamIdentityAndUsageNamespaceChangeCommitAtomically(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	domain := "identity-atomic.example"
	existing := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: "https://" + domain,
		Account: "7", UserID: 7, Enabled: true, Status: upstreamStatusOK,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &existing, newAPICredential{AccessToken: "old-token"}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{
		Domain: domain, HourTs: 123456, BucketSeconds: 3600, Requests: 9,
		Provider: upstreamProviderNewAPI,
	}).Error; err != nil {
		t.Fatal(err)
	}
	// Force the second half of the account+namespace transaction to fail. The
	// account upsert must roll back with it; otherwise the new identity would be
	// shown with the previous identity's local usage rows.
	if err := m.storeDB.Exec(`CREATE TRIGGER reject_identity_usage_reset
		BEFORE DELETE ON channel_upstream_usage_hours
		WHEN OLD.domain = 'identity-atomic.example'
		BEGIN SELECT RAISE(ABORT, 'injected usage reset failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	replacement := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: "https://" + domain,
		Account: aiCodeWithKeyIdentity([]string{"sk-acw-new-identity"}), Enabled: true, Status: upstreamStatusOK,
	}
	if err := m.sealUpstreamAccountCredential(&replacement, aiCodeWithCredential{APIKeys: []string{"sk-acw-new-identity"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.persistUpstreamAccountIdentityChange(context.Background(), &replacement, true); err == nil {
		t.Fatal("injected usage reset failure unexpectedly committed identity change")
	}
	var stored ChannelUpstreamAccount
	if err := m.storeDB.First(&stored, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Provider != upstreamProviderNewAPI || stored.UserID != 7 || stored.Account != "7" {
		t.Fatalf("account identity committed without its usage reset: %+v", stored)
	}
	var usageRows int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", domain).Count(&usageRows).Error; err != nil {
		t.Fatal(err)
	}
	if usageRows != 1 {
		t.Fatalf("failed identity transaction changed usage rows: count=%d", usageRows)
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

func TestUpstreamSchedulesNeverShortenConfiguredInterval(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC).Unix()
	for i := 0; i < 512; i++ {
		domain := fmt.Sprintf("upstream-%d.example", i)
		balanceDue := nextUpstreamSyncAt(Settings{UpstreamSyncMinutes: 5}, domain, now, 0)
		if balanceDue < now+5*60 || balanceDue > now+5*60+30 {
			t.Fatalf("balance schedule shortened/exceeded jitter: domain=%s due=%d", domain, balanceDue-now)
		}
		usageDue := nextUpstreamUsageSyncAt(Settings{UpstreamUsageSyncMinutes: 30}, domain, now, 0)
		if usageDue < now+30*60 || usageDue > now+30*60+45 {
			t.Fatalf("usage schedule shortened/exceeded jitter: domain=%s due=%d", domain, usageDue-now)
		}
		backfillDue := nextUpstreamUsageBackfillAt(Settings{UpstreamUsageSyncMinutes: 30}, domain, now, 0)
		if backfillDue < now+30*60 || backfillDue > now+30*60+45 {
			t.Fatalf("backfill schedule shortened/exceeded jitter: domain=%s due=%d", domain, backfillDue-now)
		}
	}
}
