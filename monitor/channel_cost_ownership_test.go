package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestChannelCostKeyFingerprintNormalizesNewAPIPrefixAndMultiKey(t *testing.T) {
	if channelCostKeyFingerprint("sk-secret") != channelCostKeyFingerprint("secret") {
		t.Fatal("NewAPI sk- display prefix was not normalized")
	}
	keys := channelCostChannelKeys("sk-alpha\n beta \nalpha")
	if len(keys) != 2 {
		t.Fatalf("newline multi-key normalization=%d want 2", len(keys))
	}
	jsonKeys := channelCostChannelKeys(`["sk-alpha","beta"]`)
	if len(jsonKeys) != 2 || jsonKeys[0] != keys[0] || jsonKeys[1] != keys[1] {
		t.Fatalf("JSON multi-key normalization mismatch: %+v vs %+v", jsonKeys, keys)
	}
}

func TestInspectChannelCostOwnershipMatchesOnlyExactKeysWithoutPersistingSecrets(t *testing.T) {
	const accessToken = "monitor-access-token"
	keys := map[int]string{10: "alpha-secret", 11: "beta-secret", 14: "unmatched-secret"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+accessToken || r.Header.Get("New-Api-User") != "7" {
			t.Errorf("missing upstream auth headers")
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[{"id":10},{"id":11},{"id":14}],"total":3}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/batch/keys":
			var request struct {
				IDs []int `json:"ids"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			responseKeys := map[string]string{}
			for _, id := range request.IDs {
				responseKeys[strconv.Itoa(id)] = keys[id]
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"keys": responseKeys}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	m.cfg.ChannelCostHMACKey = strings.Repeat("unit-test-hmac-", 3)
	m.cfg.ChannelCostHMACKeyID = "key-v1"
	account := ChannelUpstreamAccount{Domain: normalizeChannelBaseDomain(server.URL), Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 7}
	epoch := newAPIUpstreamAccountEpoch(account)
	for index, tokenID := range []int{10, 11, 12, 14} {
		sourceRef, err := channelCostSourceRef([]byte(m.cfg.ChannelCostHMACKey), account.Provider, epoch, channelCostSourceKindNewAPIToken, strconv.Itoa(tokenID))
		if err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&ChannelUpstreamCostHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: 3600,
			SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef:        sourceRef, DimensionHash: strings.Repeat(strconv.Itoa(index+1), 64),
			Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: m.cfg.ChannelCostHMACKeyID,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}

	prod, err := sql.Open("sqlite", t.TempDir()+"/prod.db")
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if _, err := prod.Exec("CREATE TABLE channels (id INTEGER PRIMARY KEY, name TEXT, status INTEGER, `key` TEXT, base_url TEXT)"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO channels VALUES (59,'unique',1,'sk-alpha-secret','` + server.URL + `')`,
		`INSERT INTO channels VALUES (60,'shared-a',1,'beta-secret','` + server.URL + `')`,
		`INSERT INTO channels VALUES (61,'shared-b',2,'sk-beta-secret','` + server.URL + `')`,
		`INSERT INTO channels VALUES (99,'other-domain',1,'unmatched-secret','https://other.example')`,
	} {
		if _, err := prod.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	m.prodDB = prod
	report, err := m.inspectChannelCostOwnership(context.Background(), account, newAPICredential{AccessToken: accessToken})
	if err != nil {
		t.Fatal(err)
	}
	if report.Sources != 4 || report.ExactUnique != 1 || report.ExactShared != 1 || report.Unmatched != 1 || report.TokenUnavailable != 1 {
		t.Fatalf("ownership summary mismatch: %+v", report)
	}
	states := map[string]channelCostOwnershipMatch{}
	for _, match := range report.Matches {
		states[match.State] = match
	}
	if len(states["exact_unique"].Candidates) != 1 || states["exact_unique"].Candidates[0].ChannelID != 59 {
		t.Fatalf("unique exact match mismatch: %+v", states["exact_unique"])
	}
	if len(states["exact_shared"].Candidates) != 2 || states["exact_shared"].Candidates[0].ChannelID != 60 || states["exact_shared"].Candidates[1].ChannelID != 61 {
		t.Fatalf("shared exact match mismatch: %+v", states["exact_shared"])
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range append([]string{accessToken}, mapsValues(keys)...) {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("ownership report leaked secret %q", secret)
		}
	}
	var bindings int64
	if err := m.storeDB.Model(&ChannelCostSourceBinding{}).Count(&bindings).Error; err != nil || bindings != 0 {
		t.Fatalf("read-only ownership inspection wrote bindings=%d err=%v", bindings, err)
	}
}

func TestInspectChannelCostOwnershipHandlerFailsClosedAndUsesAccountGate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handlerResponse := func(t *testing.T, m *Monitor, ctx context.Context, domain string) *httptest.ResponseRecorder {
		t.Helper()
		router := gin.New()
		router.POST("/channels/cost/source-ownership/inspect", m.inspectChannelCostOwnershipHandler)
		req := httptest.NewRequest(http.MethodPost, "/channels/cost/source-ownership/inspect?domain="+domain, nil).WithContext(ctx)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}

	localOnly := &Monitor{cfg: Settings{LocalSnapshotOnly: true}}
	response := handlerResponse(t, localOnly, context.Background(), "4sapi.com")
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("local snapshot ownership guard mismatch: status=%d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
	}

	disabled := &Monitor{}
	response = handlerResponse(t, disabled, context.Background(), "4sapi.com")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled rollout reached ownership inspector: status=%d body=%s", response.Code, response.Body.String())
	}

	const domain = "4sapi.com"
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.ChannelCostClosureEnabled = true
	m.cfg.ChannelCostClosureDomains = []string{domain}
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	release, err := m.acquireUpstreamAccountAdmin(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	requestContext, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	response = handlerResponse(t, m, requestContext, domain)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "正在执行其他操作") {
		t.Fatalf("concurrent account operation was not rejected safely: status=%d body=%s", response.Code, response.Body.String())
	}
}

func mapsValues(values map[int]string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}
