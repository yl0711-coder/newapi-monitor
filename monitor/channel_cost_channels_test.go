package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func channelCostDirectoryFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelSnap{}, &ChannelUpstreamAccount{}, &StabilityHourSample{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "directory.example", Provider: upstreamProviderNewAPI, BaseURL: "https://directory.example", UserID: 1}
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	channels := []ChannelSnap{
		{ID: 59, Name: "current", BaseDomain: account.Domain, Status: 1},
		{ID: 64, Name: "internal-only", BaseDomain: account.Domain, Status: 2, Groups: "private-group-marker", Models: "private-model-marker"},
		{ID: 69, Name: "retired", BaseDomain: account.Domain, Status: 2, DeletedAt: 7200},
		{ID: 70, Name: "other-domain", BaseDomain: "other.example", Status: 1},
	}
	if err := db.Create(&channels).Error; err != nil {
		t.Fatal(err)
	}
	source, epoch := strings.Repeat("a", 64), newAPIUpstreamAccountEpoch(account)
	evidence := ChannelUpstreamCostHourEvidence{Domain: account.Domain, AccountEpoch: epoch, HourTs: 3600,
		SemanticsVersion: channelCostEvidenceSemanticsVersion, SourceRef: source, DimensionHash: strings.Repeat("b", 64),
		Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "test-key-v1", Requests: 1,
		ChargeUnits: 500_000, ChargeUnitsPerUSD: "500000", ChargeUnit: channelCostChargeUnitNewAPIQuota}
	if err := db.Create(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	state := ChannelUpstreamCostHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: 3600,
		SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched"}
	if err := db.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	return &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain}}}, account, source
}

func getChannelCostDirectory(t *testing.T, m *Monitor, domain string) *httptest.ResponseRecorder {
	t.Helper()
	router := gin.New()
	router.GET("/channels/cost/sources", m.listChannelCostSourcesHandler)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/channels/cost/sources?domain="+domain, nil))
	return response
}

func assertNoCostDirectoryWrites(t *testing.T, m *Monitor) {
	t.Helper()
	for _, model := range []any{&ChannelCostSourceBinding{}, &ChannelEconomicsDirtyHour{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("read-only/rejected operation changed %T: count=%d err=%v", model, count, err)
		}
	}
}

func TestChannelCostHistoricalDirectoryIsIndependentAndDomainScoped(t *testing.T) {
	m, account, _ := channelCostDirectoryFixture(t)
	m.cfg.LocalSnapshotOnly = true
	m.cfg.ChannelCostClosureEnabled = false // Existing local read permission, not rollout permission.
	response := getChannelCostDirectory(t, m, account.Domain)
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	var payload struct {
		Channels  []channelCostHistoricalChannelView `json:"historical_channels"`
		Truncated bool                               `json:"historical_channels_truncated"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	want := []channelCostHistoricalChannelView{{59, "current", true}, {64, "internal-only", true}, {69, "retired", false}}
	if payload.Truncated || !reflect.DeepEqual(payload.Channels, want) {
		t.Fatalf("wrong historical directory: %+v", payload)
	}
	for _, unwanted := range []string{"other-domain", "private-group-marker", "private-model-marker"} {
		if strings.Contains(response.Body.String(), unwanted) {
			t.Fatalf("unrelated channel metadata was exported: %s", unwanted)
		}
	}
	assertNoCostDirectoryWrites(t, m)
}

func TestChannelCostHistoricalDirectoryLimitsAndFailures(t *testing.T) {
	m, account, _ := channelCostDirectoryFixture(t)
	ctx := context.Background()
	empty, truncated, err := loadChannelCostHistoricalChannels(ctx, m.storeDB, "absent.example")
	if err != nil || truncated || empty == nil || len(empty) != 0 {
		t.Fatal("empty directory must be an explicit empty array", err)
	}
	rows := make([]ChannelSnap, channelCostHistoricalChannelLimit+1)
	for i := range rows {
		rows[i] = ChannelSnap{ID: i + 1000, BaseDomain: "large.example"}
	}
	if err := m.storeDB.CreateInBatches(&rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	large, truncated, err := loadChannelCostHistoricalChannels(ctx, m.storeDB, "large.example")
	if err != nil || !truncated || len(large) != channelCostHistoricalChannelLimit || large[0].Name != "渠道 #1000" {
		t.Fatal("directory limit or fallback label failed", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := loadChannelCostHistoricalChannels(cancelled, m.storeDB, account.Domain); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read was not stopped", err)
	}
	if err := m.storeDB.Migrator().DropTable(&ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	response := getChannelCostDirectory(t, m, account.Domain)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "读取历史归属渠道目录失败") {
		t.Fatal("directory error was hidden as an empty success", response.Code)
	}
	assertNoCostDirectoryWrites(t, m)
}

func TestChannelCostHistoricalChoiceDoesNotPermitRetiredFutureBinding(t *testing.T) {
	m, account, source := channelCostDirectoryFixture(t)
	epoch := newAPIUpstreamAccountEpoch(account)
	future := channelCostBindingInput{Domain: account.Domain, AccountEpoch: epoch, SourceRef: source,
		SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "test-key-v1", LocalChannelID: 69,
		AllocationMode: "allocated", Reason: "test retired future rejection"}
	if got := postChannelCostBinding(t, m, future); got.Code != http.StatusBadRequest {
		t.Fatal("retired channel accepted for a future mapping", got.Code, got.Body.String())
	}
	assertNoCostDirectoryWrites(t, m)
	history := channelCostHistoricalBindingInput{Domain: account.Domain, AccountEpoch: epoch, SourceRef: source,
		LocalChannelID: 69, Reason: "synthetic historical evidence confirmed for this test only"}
	if got := previewChannelCostHistoricalBinding(t, m, history); got.Code != http.StatusOK {
		t.Fatal("retired channel historical preview rejected", got.Code, got.Body.String())
	}
	assertNoCostDirectoryWrites(t, m)
	if got := postChannelCostHistoricalBinding(t, m, history); got.Code != http.StatusOK {
		t.Fatal("retired channel finite history rejected", got.Code, got.Body.String())
	}
	var binding ChannelCostSourceBinding
	if err := m.storeDB.First(&binding).Error; err != nil || binding.ValidFrom != 3600 || binding.ValidTo != 7200 || binding.LocalChannelID != 69 {
		t.Fatal("historical write was not finite", err)
	}
}

func TestChannelCostDirectoryLocalSnapshotRejectsBothWritePaths(t *testing.T) {
	m, account, source := channelCostDirectoryFixture(t)
	m.cfg.LocalSnapshotOnly = true
	epoch := newAPIUpstreamAccountEpoch(account)
	future := channelCostBindingInput{Domain: account.Domain, AccountEpoch: epoch, SourceRef: source,
		SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "test-key-v1", LocalChannelID: 59,
		AllocationMode: "allocated", Reason: "snapshot write guard"}
	if got := postChannelCostBinding(t, m, future); got.Code != http.StatusServiceUnavailable {
		t.Fatal("snapshot accepted future mapping", got.Code)
	}
	history := channelCostHistoricalBindingInput{Domain: account.Domain, AccountEpoch: epoch, SourceRef: source,
		LocalChannelID: 69, Reason: "snapshot write guard"}
	if got := postChannelCostHistoricalBinding(t, m, history); got.Code != http.StatusServiceUnavailable {
		t.Fatal("snapshot accepted historical mapping", got.Code)
	}
	if got := previewChannelCostHistoricalBinding(t, m, history); got.Code != http.StatusOK {
		t.Fatal("snapshot historical preview no longer works", got.Code)
	}
	assertNoCostDirectoryWrites(t, m)
}
