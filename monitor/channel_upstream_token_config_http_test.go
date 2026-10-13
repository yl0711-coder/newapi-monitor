package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func tokenConfigRouter(m *Monitor) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	g := r.Group("/channels", m.requireRole(roleRoot))
	g.GET("/upstream/token-config", m.getUpstreamTokenConfigHandler)
	g.GET("/upstream/token-check", m.getUpstreamTokenReconciliationHandler)
	g.POST("/upstream/token-config", m.saveUpstreamTokenConfigHandler)
	g.POST("/finance/domain-rates", m.saveChannelFinanceDomainRatesHandler)
	return r
}

func tokenConfigRequest(m *Monitor, r http.Handler, method, path, body string, role int) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if role > 0 {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", role, time.Now().Unix())})
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestUpstreamTokenConfigHTTPAuthorizationAndValidation(t *testing.T) {
	m, account := tokenConfigFixture(t)
	m.cfg.SessionSecret = "token-config-test-secret"
	r := tokenConfigRouter(m)
	path := "/channels/upstream/token-config"
	getPath := path + "?domain=" + account.Domain + "&channel_id=185"
	for _, role := range []int{0, roleAdmin} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			w := tokenConfigRequest(m, r, method, getPath, "{}", role)
			if w.Code != http.StatusUnauthorized && w.Code != http.StatusForbidden {
				t.Fatalf("non-root accepted: role=%d method=%s code=%d", role, method, w.Code)
			}
		}
	}
	w := tokenConfigRequest(m, r, http.MethodGet, getPath, "", roleRoot)
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "secret-not-for-response") {
		t.Fatalf("GET response: %d %s", w.Code, w.Body.String())
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	body := fmt.Sprintf(`{"domain":"sub2.example","channel_id":185,"account_epoch":%q,"expected_revision":0,"upstream_token_id":"575"}`, epoch)
	w = tokenConfigRequest(m, r, http.MethodPost, path, body, roleRoot)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"state":"pending_verification"`) || !strings.Contains(w.Body.String(), `"attribution_verified":false`) {
		t.Fatalf("save must report unverified intent: %d %s", w.Code, w.Body.String())
	}
	w = tokenConfigRequest(m, r, http.MethodPost, path, body, roleRoot)
	if w.Code != http.StatusConflict {
		t.Fatalf("stale revision accepted: %d %s", w.Code, w.Body.String())
	}
	for _, tt := range []struct {
		body string
		code int
	}{
		{strings.Replace(body, `"575"`, `575`, 1), http.StatusBadRequest},
		{strings.Replace(body, `"575"`, `"sk-mistaken-secret"`, 1), http.StatusBadRequest},
		{`{`, http.StatusBadRequest},
		{strings.Repeat(" ", 4097), http.StatusRequestEntityTooLarge},
	} {
		w := tokenConfigRequest(m, r, http.MethodPost, path, tt.body, roleRoot)
		if w.Code != tt.code || strings.Contains(w.Body.String(), "sk-mistaken-secret") {
			t.Fatalf("invalid body response: %d %s", w.Code, w.Body.String())
		}
	}
	for _, tokenField := range []string{"", `,"upstream_token_id":null`} {
		body := fmt.Sprintf(`{"domain":"sub2.example","channel_id":185,"account_epoch":%q,"expected_revision":1%s}`, epoch, tokenField)
		w := tokenConfigRequest(m, r, http.MethodPost, path, body, roleRoot)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"upstream_token_id":"575"`) || !strings.Contains(w.Body.String(), `"unchanged":true`) {
			t.Fatalf("omitted/null ID changed config: %d %s", w.Code, w.Body.String())
		}
	}
	m.cfg.LocalSnapshotOnly = true
	w = tokenConfigRequest(m, r, http.MethodPost, path, body, roleRoot)
	if w.Code != http.StatusForbidden {
		t.Fatalf("readonly save accepted: %d", w.Code)
	}
	w = tokenConfigRequest(m, r, http.MethodGet, getPath, "", roleRoot)
	if w.Code != http.StatusOK {
		t.Fatalf("readonly GET unavailable: %d", w.Code)
	}
}

func TestUpstreamTokenConfigLegacyRateSavePreservesToken(t *testing.T) {
	m := newStabilityTestMonitor(t)
	sqlDB, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	m.cfg.SessionSecret = "token-config-test-secret"
	account := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: "https://sub2.example", UserID: 31}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 185, Name: "test", BaseDomain: account.Domain, Groups: "group-a,group-b", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	before, err := m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(account, 185, "575", 0), "tester", 100)
	if err != nil {
		t.Fatal(err)
	}
	input := channelFinanceDomainRatesSaveInput{Domain: account.Domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 10,
		Rates: []channelFinanceChannelRateInput{{ChannelID: 185, UpstreamGroupName: "same-group", UpstreamMultiplier: 0.15, UpstreamDiscountFactor: 1}}}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	w := tokenConfigRequest(m, tokenConfigRouter(m), http.MethodPost, "/channels/finance/domain-rates", string(encoded), roleRoot)
	if w.Code != http.StatusOK {
		t.Fatalf("legacy save: %d %s", w.Code, w.Body.String())
	}
	after, err := m.loadUpstreamTokenConfig(context.Background(), account.Domain, 185)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("legacy rate save changed token config: %+v %+v %v", before, after, err)
	}
	var rates []ChannelFinanceChannelCost
	if err := m.storeDB.Where("channel_id = ?", 185).Find(&rates).Error; err != nil || len(rates) != 2 {
		t.Fatalf("old rate save behavior changed: %v", err)
	}
	var versionsBefore int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Count(&versionsBefore).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(account, 185, "574", 1), "tester", 101); err != nil {
		t.Fatal(err)
	}
	var versionsAfter int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Count(&versionsAfter).Error; err != nil || versionsAfter != versionsBefore {
		t.Fatalf("token-only edit appended a rate version: %d %d %v", versionsBefore, versionsAfter, err)
	}
}
