package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestBuildGroupGovernanceSnapshotUserBindingsAndRejections(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	rows, users, state, err := buildGroupGovernanceSnapshot(groupGovernanceInput{
		Ratios:     map[string]float64{"vip": 0, "orphan": 1, "plan-only": 1},
		Displays:   map[string]string{"vip": "VIP", "orphan": "Orphan", "plan-only": "Plan"},
		Selectable: map[string]bool{"vip": true, "orphan": true, "plan-only": true},
		ConfigSources: map[string]map[string]struct{}{
			"vip": {"GroupRatio": {}, "UserUsableGroups": {}}, "orphan": {"GroupRatio": {}}, "plan-only": {"GroupRatio": {}},
		},
		Channels: []ChannelSnap{{ID: 9, Name: "primary", Groups: " vip, vip ", Status: 1}},
		Users: []GroupGovernanceUser{
			{Grp: "vip", UserID: 1, Username: "enabled", DisplayName: "Enabled", Status: 1, Role: 1},
			{Grp: "vip", UserID: 2, Username: "disabled", DisplayName: "Disabled", Status: 2, Role: 10},
			{Grp: "missing", UserID: 3, Username: "missing-ratio", Status: 1, Role: 1},
		},
		Subscriptions: map[string][]groupGovernanceSubscription{"plan-only": {{ID: 8, Title: "VIP Plan", Enabled: true}}},
		Activity: map[string]groupGovernanceActivity{
			"vip":    {Delivered7d: 2, Delivered30d: 4, Rejected7d: 3, Rejected30d: 5, LastAt: now.Unix() - 3600},
			"legacy": {Delivered30d: 1, LastAt: now.Unix() - 86400},
		},
		HistoryComplete: true, SubscriptionChecked: true,
		CoverageStartAt: now.Unix() - 31*86400, CoverageEndAt: now.Unix() - 3600,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	byGroup := map[string]GroupGovernanceGroup{}
	for _, row := range rows {
		byGroup[row.Grp] = row
	}
	vip := byGroup["vip"]
	if !vip.RatioConfigured || vip.Ratio != 0 {
		t.Fatalf("GroupRatio=0 must remain a configured value: %+v", vip)
	}
	if vip.UserCount != 2 || vip.EnabledUserCount != 1 || vip.DisabledUserCount != 1 {
		t.Fatalf("unexpected user binding counts: %+v", vip)
	}
	if vip.EnabledChannels != 1 {
		t.Fatalf("duplicate channel group entries must be deduplicated: %+v", vip)
	}
	if vip.Requests7d != 5 || vip.Requests30d != 9 || vip.PreRouteRejections30d != 5 {
		t.Fatalf("recent use must include attributable pre-route rejections: %+v", vip)
	}
	if byGroup["missing"].Status != "high" {
		t.Fatalf("a user-bound group without GroupRatio must be high risk: %+v", byGroup["missing"])
	}
	if byGroup["plan-only"].CleanupCandidate {
		t.Fatal("an enabled subscription plan reference must block cleanup-candidate tagging")
	}
	if !byGroup["legacy"].HistoricalOnly || byGroup["legacy"].Current || byGroup["legacy"].Status != "observe" {
		t.Fatalf("historical-only group semantics are wrong: %+v", byGroup["legacy"])
	}
	if len(users) != 3 || state.CurrentGroupCount != 4 || state.HistoricalGroupCount != 1 {
		t.Fatalf("unexpected snapshot summary: users=%d state=%+v", len(users), state)
	}
}

func TestBuildGroupGovernanceSnapshotCleanupRequiresCoverageAndSubscriptionCheck(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	base := groupGovernanceInput{
		Ratios:        map[string]float64{"unused": 1},
		ConfigSources: map[string]map[string]struct{}{"unused": {"GroupRatio": {}}},
		Selectable:    map[string]bool{}, HistoryComplete: true, SubscriptionChecked: true,
	}
	rows, _, state, err := buildGroupGovernanceSnapshot(base, now)
	if err != nil || len(rows) != 1 || !rows[0].CleanupCandidate || state.CleanupCandidateCount != 1 {
		t.Fatalf("expected a bounded cleanup candidate: rows=%+v state=%+v err=%v", rows, state, err)
	}
	base.HistoryComplete = false
	rows, _, _, _ = buildGroupGovernanceSnapshot(base, now)
	if rows[0].CleanupCandidate {
		t.Fatal("incomplete 30-day history must block cleanup candidate")
	}
	base.HistoryComplete, base.SubscriptionChecked = true, false
	rows, _, _, _ = buildGroupGovernanceSnapshot(base, now)
	if rows[0].CleanupCandidate {
		t.Fatal("unverified subscription plans must block cleanup candidate")
	}
	base.SubscriptionChecked = true
	base.Displays = map[string]string{"unused": "Unused"}
	base.Selectable = map[string]bool{"unused": true}
	base.ConfigSources["unused"]["UserUsableGroups"] = struct{}{}
	rows, _, _, _ = buildGroupGovernanceSnapshot(base, now)
	if rows[0].CleanupCandidate || rows[0].Status != "high" {
		t.Fatalf("a globally selectable group without a channel must be high risk, not cleanup: %+v", rows[0])
	}
}

func TestGroupGovernanceRiskAndNamingRules(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	input := groupGovernanceInput{
		Ratios: map[string]float64{
			"normal": 1, "named-0.5x": 0.7, "Foo-Bar": 1, "foo_bar": 1,
			"selectable-no-channel": 1, "default": 1,
		},
		Displays: map[string]string{
			"normal": "Normal", "named-0.5x": "Named", "Foo-Bar": "Foo", "foo_bar": "foo",
			"selectable-no-channel": "No channel", "default": "Default",
		},
		Selectable: map[string]bool{"normal": true, "selectable-no-channel": true},
		Auto:       map[string]bool{"auto-no-channel": true},
		ConfigSources: map[string]map[string]struct{}{
			"normal": {"GroupRatio": {}}, "named-0.5x": {"GroupRatio": {}}, "Foo-Bar": {"GroupRatio": {}},
			"foo_bar": {"GroupRatio": {}}, "selectable-no-channel": {"GroupRatio": {}, "UserUsableGroups": {}},
			"default": {"GroupRatio": {}}, "auto-no-channel": {"AutoGroups": {}},
		},
		Channels:        []ChannelSnap{{ID: 1, Name: "normal", Status: 1, Groups: "normal, missing-channel"}},
		Users:           []GroupGovernanceUser{{Grp: "missing-user", UserID: 2, Username: "u", Status: 1}},
		Tokens:          map[string]groupGovernanceTokenStats{"missing-token": {Total: 1, Enabled: 1}, "auto": {Total: 1, Enabled: 1}, "": {Total: 99}},
		HistoryComplete: true, SubscriptionChecked: true,
	}
	rows, _, _, err := buildGroupGovernanceSnapshot(input, now)
	if err != nil {
		t.Fatal(err)
	}
	byGroup := map[string]GroupGovernanceGroup{}
	for _, row := range rows {
		byGroup[row.Grp] = row
	}
	for _, group := range []string{"missing-channel", "missing-user", "missing-token", "selectable-no-channel"} {
		if byGroup[group].Status != "high" {
			t.Fatalf("%s status=%q want high: %+v", group, byGroup[group].Status, byGroup[group])
		}
	}
	if !byGroup["named-0.5x"].NameHasRatio || byGroup["named-0.5x"].Status != "pending" || !strings.Contains(byGroup["named-0.5x"].IssuesJSON, "不一致") {
		t.Fatalf("ratio-in-name mismatch was not identified: %+v", byGroup["named-0.5x"])
	}
	for _, group := range []string{"Foo-Bar", "foo_bar"} {
		if !strings.Contains(byGroup[group].IssuesJSON, "疑似重复") {
			t.Fatalf("canonical duplicate not identified for %s: %+v", group, byGroup[group])
		}
	}
	if _, exists := byGroup[""]; exists {
		t.Fatal("empty token group must not create a governance group")
	}
	if _, exists := byGroup["auto"]; !exists || byGroup["default"].CleanupCandidate || byGroup["auto"].CleanupCandidate || byGroup["auto"].Status == "high" {
		t.Fatal("special default/auto identifiers must never become cleanup candidates")
	}
}

func TestGroupGovernanceActivityUsesCompletenessLedgerAndIncludesRejections(t *testing.T) {
	m := newTestMonitor(t)
	now := time.Unix(1_800_000_000, 0)
	hour := now.Unix()/3600*3600 - 3600
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: hour, ChannelID: 1, ModelName: "gpt", Grp: "vip",
		TrafficClassVersion: userTrafficClassificationVersion, Success: 2,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityRejectHour{HourTs: hour, Node: "master", Reason: "no_channel", Model: "gpt", Grp: "vip", Count: 3}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityRejectHour{HourTs: hour, Node: "slave", Reason: "invalid_token", Model: "", Grp: "", Count: 4}).Error; err != nil {
		t.Fatal(err)
	}
	from := (now.Unix() - groupGovernanceHistoryDays*86400) / 3600 * 3600
	to := finalizedStabilityHourTo(now.Unix()) / 3600 * 3600
	states := make([]StabilityHourIngestState, 0, (to-from)/3600)
	for ts := from; ts < to; ts += 3600 {
		requests := int64(0)
		if ts == hour {
			requests = 2
		}
		states = append(states, StabilityHourIngestState{HourTs: ts, Status: "complete", Requests: requests, TrafficClassVersion: userTrafficClassificationVersion})
	}
	if err := m.storeDB.CreateInBatches(states, 200).Error; err != nil {
		t.Fatal(err)
	}
	activity, coverageStart, coverageEnd, complete, unattributed7d, unattributed30d, err := m.loadGroupGovernanceActivity(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || coverageStart != from || coverageEnd != to {
		t.Fatalf("coverage must come from the completeness ledger: complete=%v range=%d..%d want=%d..%d", complete, coverageStart, coverageEnd, from, to)
	}
	if got := activity["vip"]; got.Delivered7d != 2 || got.Rejected7d != 3 || got.Delivered30d != 2 || got.Rejected30d != 3 {
		t.Fatalf("unexpected activity: %+v", got)
	}
	if unattributed7d != 4 || unattributed30d != 4 {
		t.Fatalf("unexpected unattributed rejections: %d/%d", unattributed7d, unattributed30d)
	}
}

func TestGroupGovernanceHandlersReadOnlyLocalSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = true
	// An unusable production handle proves both handlers stay on Monitor SQLite.
	m.prodDB = &sql.DB{}
	now := time.Unix(1_800_000_000, 0)
	groups := []GroupGovernanceGroup{{Grp: "vip", Current: true, RatioConfigured: true, Ratio: 1, UserCount: 1, Status: "normal", IssuesJSON: "[]", ConfigSourcesJSON: `["users.group"]`, ChannelsJSON: "[]", SubscriptionsJSON: "[]", SyncedAt: now.Unix()}}
	users := []GroupGovernanceUser{{Grp: "vip", UserID: 7, Username: "alice", DisplayName: "Alice", Status: 1, Role: 1}}
	state := GroupGovernanceState{ID: groupGovernanceStateID, Revision: "r1", LastSuccessAt: now.Unix(), Complete: true, CurrentGroupCount: 1, SourceErrorsJSON: "[]"}
	if err := m.publishGroupGovernanceSnapshot(context.Background(), groups, users, state); err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/group-governance/report", nil)
	m.serveGroupGovernanceReport(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("report status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var report groupGovernanceReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil || len(report.Groups) != 1 || report.Groups[0].Grp != "vip" {
		t.Fatalf("unexpected local report: %+v err=%v", report, err)
	}

	recorder = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/group-governance/users?group=vip&q=alice", nil)
	m.serveGroupGovernanceUsers(c)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"username":"alice"`) {
		t.Fatalf("users status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupGovernanceFailureKeepsLastGoodRows(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = true
	state := GroupGovernanceState{ID: groupGovernanceStateID, Revision: "good", LastSuccessAt: 100, Complete: true, SourceErrorsJSON: "[]"}
	rows := []GroupGovernanceGroup{{Grp: "keep-me", Current: true, RatioConfigured: true, Ratio: 1, Status: "normal", IssuesJSON: "[]", ConfigSourcesJSON: "[]", ChannelsJSON: "[]", SubscriptionsJSON: "[]"}}
	if err := m.publishGroupGovernanceSnapshot(context.Background(), rows, nil, state); err != nil {
		t.Fatal(err)
	}
	m.markGroupGovernanceFailure(context.DeadlineExceeded)
	report, err := m.loadGroupGovernanceReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Groups) != 1 || report.Groups[0].Grp != "keep-me" || report.State.LastSuccessAt != 100 || report.State.Complete {
		t.Fatalf("last-good snapshot was not preserved: %+v", report)
	}
}

func TestGroupGovernanceDisabledDoesNotTouchProductionSource(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = false
	m.prodDB = &sql.DB{} // any attempted query on this zero handle would fail or panic
	if err := m.syncGroupGovernance(context.Background()); err != nil {
		t.Fatalf("disabled governance must be a no-op: %v", err)
	}
	var count int64
	if err := m.storeDB.Model(&GroupGovernanceState{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("disabled governance wrote state: count=%d err=%v", count, err)
	}
}

func TestGroupGovernanceRoutesAllowAdminAndRootOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = true
	m.cfg.SessionSecret = "group-governance-role-test"
	router := gin.New()
	m.RegisterRoutes(router)
	now := time.Now().Unix()
	request := func(role int) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/group-governance/report", nil)
		req.Header.Set("Accept", "application/json")
		if role > 0 {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", role, now)})
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}
	if got := request(0).Code; got != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d want=401", got)
	}
	for _, role := range []int{roleAdmin, roleRoot} {
		response := request(role)
		if response.Code != http.StatusOK {
			t.Fatalf("role %d status=%d want=200 body=%s", role, response.Code, response.Body.String())
		}
		if got := response.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
			t.Fatalf("role %d response is cacheable: %q", role, got)
		}
	}
}

func TestGroupGovernanceCSVFormulaInjectionProtection(t *testing.T) {
	for _, raw := range []string{"=1+1", "+cmd", "-2+3", "@SUM(A1:A2)", "  =1+1"} {
		if got := groupGovernanceCSVSafe(raw); !strings.HasPrefix(got, "'") {
			t.Fatalf("formula-like value %q was not neutralized: %q", raw, got)
		}
	}
	if got := groupGovernanceCSVSafe("normal"); got != "normal" {
		t.Fatalf("ordinary value changed: %q", got)
	}
}

func TestGroupGovernanceSettingsAndIntervalGuard(t *testing.T) {
	t.Setenv("MONITOR_GROUP_GOVERNANCE_ENABLED", "true")
	t.Setenv("MONITOR_GROUP_GOVERNANCE_SYNC_MINUTES", "12")
	cfg := LoadSettings()
	if !cfg.GroupGovernanceEnabled || cfg.GroupGovernanceSyncMinutes != 12 {
		t.Fatalf("settings not loaded: %+v", cfg)
	}
	if got := groupGovernanceInterval(0); got != 5*time.Minute {
		t.Fatalf("minimum interval=%v", got)
	}
	if got := groupGovernanceInterval(2000); got != 24*time.Hour {
		t.Fatalf("maximum interval=%v", got)
	}
}

func TestGroupGovernanceTokenAggregationIsOnlyFullGroupBySafe(t *testing.T) {
	if strings.Contains(groupGovernanceTokenStatsSQL, "GROUP BY BINARY grp") {
		t.Fatal("token aggregation must not group by an alias under ONLY_FULL_GROUP_BY")
	}
	if !strings.Contains(groupGovernanceTokenStatsSQL, "GROUP BY BINARY TRIM(COALESCE(`group`, ''))") {
		t.Fatal("token aggregation must group by the same case-sensitive expression it selects")
	}
}

func TestGroupGovernanceTabWiring(t *testing.T) {
	for _, want := range []string{
		`data-tab="group-governance"`, `id="tab-group-governance"`, `/group-governance.js?v=1`,
		`window.groupGovernanceActivate`, `group-governance)$`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("page.html missing group-governance wiring %q", want)
		}
	}
}
