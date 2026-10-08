package monitor

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func openOxFixtureItem(id int64, at int64) openOxUsageItem {
	tokens, estimated := int64(20), false
	return openOxUsageItem{ID: id, UserID: 7, CreatedAt: time.Unix(at, 0).UTC().Format(time.RFC3339Nano), TotalTokens: &tokens, Estimated: &estimated, Cost: json.RawMessage(`"0.25"`), SubscriptionCredit: json.RawMessage(`"0.25"`)}
}

func TestOpenOxHistoryBudgetCoversBoundedPageAndVerification(t *testing.T) {
	if got := upstreamUsageRequestBudget(upstreamProviderOpenOx, upstreamUsageLaneHistory); got != openOxMaxPages+1 {
		t.Fatalf("OpenOx history budget=%d, want %d", got, openOxMaxPages+1)
	}
	if got := upstreamUsageRequestBudget(upstreamProviderNewAPI, upstreamUsageLaneHistory); got != upstreamUsageHistoryMaxRequestsPerRun {
		t.Fatalf("unrelated provider history budget changed: %d", got)
	}
	if got := upstreamUsageRequestBudget(upstreamProviderOpenOx, upstreamUsageLaneTail); got != upstreamUsageMaxRequestsPerRun {
		t.Fatalf("OpenOx tail budget changed: %d", got)
	}
	from := time.Date(2026, 9, 28, 0, 0, 0, 0, cstLocation).Unix()
	for _, count := range []int{381, openOxPageSize * openOxMaxPages} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			items := make([]openOxUsageItem, count)
			for i := range items {
				items[i] = openOxFixtureItem(int64(i+1), from+60)
			}
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				if page < 1 || page > openOxMaxPages {
					t.Errorf("unexpected page %d", page)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				start := (page - 1) * openOxPageSize
				end := min(start+openOxPageSize, count)
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{
					"items": items[start:end], "total": count, "page": page, "size": openOxPageSize,
				}})
			}))
			defer s.Close()
			row := ChannelUpstreamAccount{Domain: "openox.test", Provider: upstreamProviderOpenOx, BaseURL: s.URL, UserID: 7}
			pacer := newUpstreamUsageRequestPacer(upstreamUsageRequestBudget(row.Provider, upstreamUsageLaneHistory), 0)
			got, err := fetchOpenOxUsageWindow(t.Context(), s.Client(), row, openOxCredential{AccessToken: "fixture"}, from, from+86400, pacer)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := (count+openOxPageSize-1)/openOxPageSize + 1
			if calls != wantCalls || len(got.Hours) != 24 || got.Hours[0].Requests != int64(count) {
				t.Fatalf("requests=%d want=%d result=%+v", calls, wantCalls, got)
			}
		})
	}
}

func TestOpenOxProfileStrictAndReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantError  bool
	}{
		{"wallet", `{"success":true,"data":{"user":{"id":7},"spendable_balance":"4","display_balance":"99"}}`, 200, false},
		{"zero", `{"success":true,"data":{"user":{"id":7},"spendable_balance":"0"}}`, 200, false},
		{"missing", `{"success":true,"data":{"user":{"id":7},"balance":4}}`, 200, true},
		{"null", `{"success":true,"data":{"user":{"id":7},"spendable_balance":null}}`, 200, true},
		{"nan", `{"success":true,"data":{"user":{"id":7},"spendable_balance":"NaN"}}`, 200, true},
		{"wrong_user", `{"success":true,"data":{"user":{"id":8},"spendable_balance":4}}`, 200, true},
		{"failure", `{"success":false,"data":{"user":{"id":7},"spendable_balance":4}}`, 200, true},
		{"expired", `{"message":"fixture-secret"}`, 401, true},
		{"forbidden", `{"message":"fixture-secret"}`, 403, true},
		{"limited", `{"message":"fixture-secret"}`, 429, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/api/v1/profile" || r.Header.Get("Authorization") != "Bearer fixture-secret" || r.Header.Get("Cookie") != "" {
					t.Error("unexpected request")
				}
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer s.Close()
			result, id, err := readOpenOxProfile(t.Context(), s.Client(), ChannelUpstreamAccount{BaseURL: s.URL, UserID: 7}, openOxCredential{AccessToken: "fixture-secret"})
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected result: %+v %v", result, err)
			}
			if err != nil && strings.Contains(err.Error(), "fixture-secret") {
				t.Fatal("secret leaked")
			}
			if tc.name == "wallet" && (result.BalanceUSD != 4 || result.BalanceUnit != 1 || id != 7) {
				t.Fatal("wrong balance basis")
			}
			if tc.status == 401 || tc.status == 403 {
				var auth *upstreamAuthError
				if !errors.As(err, &auth) {
					t.Fatal("not isolated as auth failure")
				}
			}
			if tc.status == 429 && upstreamRetryAt(err) <= time.Now().Unix() {
				t.Fatal("lost retry-after")
			}
		})
	}
}

func TestOpenOxUsagePaginationAndBoundaries(t *testing.T) {
	from := time.Date(2026, 9, 29, 10, 0, 0, 0, cstLocation).Unix()
	for _, mode := range []string{"ok", "empty", "duplicate", "short", "changing", "bad_total", "budget", "wrong_user", "outside_date", "missing_cost", "estimated", "partial"} {
		t.Run(mode, func(t *testing.T) {
			items := make([]openOxUsageItem, 21)
			for i := range items {
				items[i] = openOxFixtureItem(int64(i+1), from+60)
			}
			// Outside the requested hour, but inside the server's natural day.
			items[20].CreatedAt = time.Unix(from-3600, 0).UTC().Format(time.RFC3339)
			if mode == "duplicate" {
				items[20].ID = items[0].ID
			}
			if mode == "wrong_user" {
				items[0].UserID = 8
			}
			if mode == "outside_date" {
				items[0].CreatedAt = time.Unix(from-86400, 0).UTC().Format(time.RFC3339)
			}
			if mode == "missing_cost" {
				items[0].Cost = nil
			}
			if mode == "estimated" {
				yes := true
				items[0].Estimated = &yes
				items[0].Cost = json.RawMessage(`"0"`)
				items[0].SubscriptionCredit = json.RawMessage(`"0"`)
			}
			if mode == "empty" {
				items = []openOxUsageItem{}
			}
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "GET" || r.URL.Path != "/api/v1/usage" || r.URL.Query().Get("date_from") != "2026-09-29" || r.URL.Query().Get("date_to") != "2026-09-29" {
					t.Error("wrong query bounds")
				}
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				total := len(items)
				start := (page - 1) * 20
				end := min(start+20, total)
				part := append([]openOxUsageItem{}, items[start:end]...)
				if mode == "short" && page == 1 {
					part = part[:1]
				}
				if mode == "changing" && calls > 2 {
					part[0].Cost = json.RawMessage(`"99"`)
				}
				var totalValue any = total
				if mode == "bad_total" {
					totalValue = nil
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"items": part, "total": totalValue, "page": page, "size": 20}})
			}))
			defer s.Close()
			budget := 5
			if mode == "budget" {
				budget = 1
			}
			to := from + 3600
			if mode == "partial" {
				to = from + 120
			}
			got, err := fetchOpenOxUsageWindow(t.Context(), s.Client(), ChannelUpstreamAccount{Domain: "openox.test", Provider: upstreamProviderOpenOx, BaseURL: s.URL, UserID: 7}, openOxCredential{AccessToken: "fixture"}, from, to, newUpstreamUsageRequestPacer(budget, 0))
			good := mode == "ok" || mode == "empty" || mode == "estimated" || mode == "partial"
			if (err == nil) != good {
				t.Fatalf("result=%+v err=%v", got, err)
			}
			if !good {
				if len(got.Hours) != 0 {
					t.Fatal("partial result escaped")
				}
				return
			}
			if len(got.Hours) != 1 || got.DataUntil != to || got.Hours[0].BucketSeconds != to-from {
				t.Fatalf("wrong coverage: %+v", got)
			}
			wantCost, wantRequests := 5.0, int64(20)
			if mode == "empty" {
				wantCost, wantRequests = 0, 0
			}
			if mode == "estimated" {
				wantCost = 4.75
			}
			if got.Hours[0].CostUSD != wantCost || got.Hours[0].Requests != wantRequests || got.Hours[0].Tokens != wantRequests*20 || got.Hours[0].Provisional != (mode == "estimated") {
				t.Fatalf("incorrect aggregation: %+v", got.Hours)
			}
		})
	}
}

func TestOpenOxCostAndRunwayFailClosed(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	rows, err := m.loadChannelUpstreamUsageRows(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	versions := map[string][]channelRechargeVersion{}
	for domain, a := range accounts {
		a.Provider = upstreamProviderOpenOx
		accounts[domain] = a
		versions[domain] = []channelRechargeVersion{{EffectiveAt: 1, Valid: true, Paid: 1, Credit: 1}}
	}
	for i := range rows {
		rows[i].Provider = upstreamProviderOpenOx
	}
	metrics, amounts, err := projectFinanceBillWindow(rows, scope, scope.ToTs, accounts, versions)
	if err != nil {
		t.Fatal(err)
	}
	for domain, metric := range metrics {
		if metric.AdjustedCostAvailable || metric.AdjustedCostUSD != 0 || metric.AdjustedCostStatus != openOxSubscriptionCostUnknown {
			t.Fatalf("subscription converted to cash: %+v", metric)
		}
		if amounts[domain].Raw.MicroUSD == "" || amounts[domain].RechargeCorrected.MicroUSD != "" {
			t.Fatal("raw amount missing or invented cash correction")
		}
	}
	a := assessUpstreamBalance(ChannelUpstreamAccountView{Configured: true, Provider: upstreamProviderOpenOx}, upstreamBurnEstimate{}, upstreamBalancePolicy{}, time.Now().Unix(), 5)
	if a.Status != "unavailable" || !strings.Contains(a.Reason, "订阅") {
		t.Fatalf("wrong runway %+v", a)
	}
}

func TestOpenOxSaveEncryptIdentityAndDisable(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/profile" {
			t.Error("unexpected write or endpoint")
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"user":{"id":7},"spendable_balance":"4"}}`))
	}))
	defer s.Close()
	m := newChannelUpstreamTestMonitor(t)
	t.Cleanup(m.Close)
	domain := normalizeChannelBaseDomain(s.URL)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "fixture", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(s.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	input := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderOpenOx, BaseURL: s.URL, AccessToken: "fixture-only-secret", UserID: 999}
	res := upstreamRouteRequest(t, m, r, roleRoot, "POST", "/channels/upstream", input)
	if res.Code != 200 || strings.Contains(res.Body.String(), input.AccessToken) {
		t.Fatalf("save failed: %d %s", res.Code, res.Body.String())
	}
	stored := loadBalanceAccount(t, m, domain)
	if stored.UserID != 7 || !stored.BalanceKnown || stored.BalanceUSD != 4 || strings.Contains(stored.Credential, input.AccessToken) {
		t.Fatal("identity or encryption failure")
	}
	cred, err := m.credentialForAccount(stored)
	if err != nil || cred.(openOxCredential).AccessToken != input.AccessToken {
		t.Fatal("credential roundtrip failure")
	}
	no := false
	input.Enabled = &no
	input.AccessToken = ""
	res = upstreamRouteRequest(t, m, r, roleRoot, "POST", "/channels/upstream", input)
	if res.Code != 200 {
		t.Fatalf("disable: %s", res.Body.String())
	}
	if current := loadBalanceAccount(t, m, domain); current.Enabled || current.UserID != 7 || current.Credential != stored.Credential {
		t.Fatal("disable lost credential")
	}
}

func TestOpenOxRetryReplacesWindowAndFailurePreservesIt(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	t.Cleanup(m.Close)
	from := time.Date(2026, 9, 29, 10, 0, 0, 0, cstLocation).Unix()
	account := ChannelUpstreamAccount{Domain: "openox.example", Provider: upstreamProviderOpenOx, UserID: 7}
	result, err := aggregateOpenOxUsage(account, []openOxUsageItem{openOxFixtureItem(1, from+60)}, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.persistUpstreamUsageWindow(t.Context(), account.Domain, from, from+3600, result.Hours, from+7200); err != nil {
			t.Fatal(err)
		}
	}
	var rows []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", account.Domain).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Requests != 1 || rows[0].CostUSD != 0.25 {
		t.Fatalf("retry duplicated bill: %+v", rows)
	}
	bad := openOxFixtureItem(2, from+60)
	bad.Cost = json.RawMessage(`null`)
	if _, err := aggregateOpenOxUsage(account, []openOxUsageItem{bad}, from, from+3600); err == nil {
		t.Fatal("bad bill accepted")
	}
	var after ChannelUpstreamUsageHour
	if err := m.storeDB.First(&after, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if after != rows[0] {
		t.Fatal("failed read overwrote previous snapshot")
	}
	plan := planUpstreamUsageSync(ChannelUpstreamAccount{Provider: upstreamProviderOpenOx, UsageDataUntil: from}, from+3601, 30)
	if plan.tailFrom != cstDayStart(from)-86400 || plan.tailTo != from+3601 {
		t.Fatalf("late settlement recheck missing: %+v", plan)
	}
}

func TestOpenOxInputRejectsOtherCredentials(t *testing.T) {
	for _, token := range []string{"sk-model-key", "Bearer secret", "line\nbreak", strings.Repeat("x", (16<<10)+1)} {
		if validateOpenOxInput(&channelUpstreamSaveInput{AccessToken: token}) == nil {
			t.Fatal("invalid management credential accepted")
		}
	}
	if validateOpenOxInput(&channelUpstreamSaveInput{Password: "fixture"}) == nil {
		t.Fatal("password accepted")
	}
	if validateOpenOxInput(&channelUpstreamSaveInput{AccessToken: ""}) != nil {
		t.Fatal("existing account cannot retain its credential")
	}
	if len(upstreamCredentialSecrets(&openOxCredential{AccessToken: "fixture"})) != 1 {
		t.Fatal("redaction lost token")
	}
}

func TestOpenOxDiagnosticReadsOnePageWithoutPublishing(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	t.Cleanup(m.Close)
	for _, mode := range []string{"valid", "bad_cost", "expired"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodGet {
					t.Error("diagnostic performed a write")
				}
				if mode == "expired" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.URL.Path == "/api/v1/profile" {
					_, _ = w.Write([]byte(`{"success":true,"data":{"user":{"id":7},"spendable_balance":"4"}}`))
					return
				}
				if r.URL.Path != "/api/v1/usage" || r.URL.Query().Get("page") != "1" {
					t.Error("unexpected probe")
				}
				item := openOxFixtureItem(1, cstDayStart(time.Now().Unix())-3600)
				if mode == "bad_cost" {
					item.Cost = nil
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"items": []openOxUsageItem{item}, "total": 1, "page": 1, "size": 20}})
			}))
			defer s.Close()
			row := ChannelUpstreamAccount{Domain: "openox.test", Provider: upstreamProviderOpenOx, UserID: 7, BaseURL: s.URL, CredentialVersion: upstreamCredentialVersion}
			var err error
			row.Credential, err = m.sealUpstreamCredential(row.Domain, row.Provider, openOxCredential{AccessToken: "fixture"})
			if err != nil {
				t.Fatal(err)
			}
			report := upstreamDiagnosticReport{}
			if err := m.probeSavedUpstream(t.Context(), s.Client(), row, &report); err != nil {
				t.Fatal(err)
			}
			failed := false
			for _, check := range report.Checks {
				failed = failed || check.Status == "error"
			}
			if failed != (mode != "valid") {
				t.Fatalf("wrong probe result: %+v", report)
			}
			wantCalls := 2
			if mode == "expired" {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("unexpected calls %d", calls)
			}
			var rows int64
			if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Count(&rows).Error; err != nil || rows != 0 {
				t.Fatal("probe published data", err)
			}
		})
	}
}
