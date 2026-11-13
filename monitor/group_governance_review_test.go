package monitor

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGGChannelFreshnessMatchesActualCadence(t *testing.T) {
	now := time.Unix(10000, 0)
	m := newTestMonitor(t)
	m.cfg.LogChainOnlySource = true
	if m.groupGovernanceChannelMaxAge() != 12*time.Minute {
		t.Fatal(m.groupGovernanceChannelMaxAge())
	}
	for _, age := range []time.Duration{5 * time.Minute, 10 * time.Minute, 12 * time.Minute} {
		if !m.groupGovernanceChannelCurrent(now.Add(-age).Unix(), now) {
			t.Fatal("premature expiry", age)
		}
	}
	if m.groupGovernanceChannelCurrent(now.Add(-12*time.Minute-time.Second).Unix(), now) {
		t.Fatal("stale accepted")
	}
	m.cfg.logChainChannelSyncInterval = 2 * time.Minute
	if m.groupGovernanceChannelMaxAge() != 150*time.Second {
		t.Fatal("override ignored")
	}
	m.cfg.LogChainOnlySource = false
	m.cfg.SampleSeconds = 60
	if m.groupGovernanceChannelMaxAge() != 90*time.Second {
		t.Fatal("full sampler cadence ignored")
	}
	if m.refreshChannelsContext(context.Background()) == nil || !m.channelSnapshotSyncFailed.Load() {
		t.Fatal("failed channel sync not exposed")
	}
	if m.groupGovernanceChannelCurrent(now.Unix(), now) {
		t.Fatal("failed refresh was treated as current")
	}
}

func TestGGConfigExpiryPreservesEvidenceAndTimestampDoesNotReset(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	m.cfg.GroupGovernanceEnabled = true
	m.cfg.LogChainOnlySource = true
	c := ggTestChannel(1)
	c.UpdatedAt = modelFRTTestNow.Add(-9 * time.Minute).Unix()
	m.storeDB.Create(&c)
	m.storeDB.Create(&GroupGovernanceState{ID: 1, LastSuccessAt: modelFRTTestNow.Unix()})
	m.storeDB.Create(&GroupGovernanceGroup{Grp: "g", Current: true})
	read := func() ggRealtimeReport {
		t.Helper()
		if e := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow); e != nil {
			t.Fatal(e)
		}
		var s GroupGovernanceRealtimeState
		m.storeDB.First(&s, 1)
		var r ggRealtimeReport
		if err := json.Unmarshal([]byte(s.ReportJSON), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := read()
	if !r.ConfigReady || r.Through != end {
		t.Fatal("nine-minute snapshot unavailable", r.ConfigReady, r.Note)
	}
	m.channelSnapshotSyncFailed.Store(true)
	r = read()
	if r.AdviceReady || r.ConfigReady || len(r.Channels) == 0 || !strings.Contains(r.Note, "渠道配置尚未更新") {
		t.Fatal("failure became zero healthy advice", r.Note)
	}
	m.channelSnapshotSyncFailed.Store(false)
	r = read()
	if !r.ConfigReady {
		t.Fatal("recovery not reflected")
	}
	p := groupGovernancePolicy()
	control := ggControl{}
	rr := ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
	ggRecommend(&rr, &control, "same-config", p)
	before, _ := json.Marshal(control)
	rr = ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
	for i := range rr.Metrics {
		rr.Metrics[i].channel.UpdatedAt += 600
		rr.Metrics[i].ConfigAt += 600
	}
	ggRecommend(&rr, &control, "same-config", p)
	after, _ := json.Marshal(control)
	if string(before) != string(after) {
		t.Fatal("snapshot timestamp reset history")
	}
}

func ggGrowingReplay(end int64, slow bool) ggRealtimeReport {
	var rows []modelFRTRow
	var channels []ChannelSnap
	for id := 1; id <= 5; id++ {
		channels = append(channels, ggTestChannel(id))
		for at := end - 3600; at < end; at += 60 {
			n := int64(10)
			if at >= 6000 {
				n = 30
			}
			ms := int64(1000)
			if slow && at >= end-900 {
				ms = 4000
			}
			rows = append(rows, ggTestRow(at, id, n, ms))
		}
	}
	return ggRealtimeReport{Metrics: ggBuildMetrics(rows, nil, channels, end, ggTestCoverage(end), groupGovernancePolicy())}
}

func TestGGNormalGrowthUpdatesBaselineAndFaultRecovery(t *testing.T) {
	p := groupGovernancePolicy()
	control := ggControl{}
	var r ggRealtimeReport
	for end := int64(6000); end <= 8400; end += 60 {
		r = ggGrowingReplay(end, false)
		ggRecommend(&r, &control, "config", p)
		for _, v := range r.Metrics {
			if v.Baseline.Frozen || v.Quality != "normal" || v.Status != "unchanged" || v.Decision.Label != "维持现状" {
				t.Fatalf("normal growth frozen at %d: %+v", end, v.Decision)
			}
		}
		if len(r.Actions) > 0 {
			t.Fatal("normal growth created action")
		}
		for _, c := range r.Channels {
			if c.Baseline.Frozen || c.Status != "unchanged" {
				t.Fatal("normal channel growth frozen", c.Decision)
			}
		}
	}
	if r.Metrics[0].Baseline.RPM == nil || *r.Metrics[0].Baseline.RPM < 20 {
		t.Fatal("normal baseline did not follow growth", r.Metrics[0].Baseline)
	}
	if r.Channels[0].Baseline.RPM == nil || *r.Channels[0].Baseline.RPM < 25 {
		t.Fatal("normal channel baseline did not follow growth", r.Channels[0].Baseline)
	}
	r = ggGrowingReplay(8460, true)
	ggRecommend(&r, &control, "config", p)
	key := ggMemoryKey(&r.Metrics[0])
	frozen := append([]ggPoint(nil), control.Metrics[key].Normal...)
	channelKey := ggMemoryKey(&r.Channels[0].ggMetrics)
	channelFrozen := append([]ggPoint(nil), control.Channels[channelKey].Normal...)
	for end := int64(8520); end <= 8640; end += 60 {
		r = ggGrowingReplay(end, true)
		ggRecommend(&r, &control, "config", p)
		if !r.Metrics[0].Baseline.Frozen || !reflect.DeepEqual(frozen, control.Metrics[key].Normal) {
			t.Fatal("fault learned into baseline")
		}
		if !r.Channels[0].Baseline.Frozen || !reflect.DeepEqual(channelFrozen, control.Channels[channelKey].Normal) {
			t.Fatal("channel fault learned into baseline")
		}
	}
	for end := int64(8700); end <= 8880; end += 60 {
		r = ggGrowingReplay(end, false)
		ggRecommend(&r, &control, "config", p)
	}
	if r.Metrics[0].Baseline.Frozen || r.Metrics[0].Quality != "normal" {
		t.Fatal("normal service did not resume after recovery", r.Metrics[0].Baseline)
	}
	if r.Channels[0].Baseline.Frozen || r.Channels[0].Quality != "normal" {
		t.Fatal("channel baseline did not resume after recovery", r.Channels[0].Baseline)
	}
	r = ggGrowingReplay(8940, false)
	r.Metrics[0].Pending = 1
	ggRecommend(&r, &control, "config", p)
	if !r.Metrics[0].Baseline.Frozen || r.Metrics[0].Decision.Label != "暂无法判断" {
		t.Fatal("unknown quality labelled normal")
	}
}

func TestGGIncreaseBindsTriggerAndMergesOperations(t *testing.T) {
	r, g := ggActionFixture()
	r.Metrics[0].Rules = nil
	r.Metrics[0].Requests = 10000
	for i := range r.Metrics {
		r.Metrics[i].channel.BaseHost = "shared.example"
	}
	v := &r.Metrics[1]
	v.Rules = []ggRuleResult{{ID: "capacity_limit", Eligible: true, Hit: true, Active: true, Actual: ggFloat(3), Threshold: ggFloat(1), Hits: 2}, {ID: "unavailable", Eligible: true, Hit: true, Active: true, Actual: ggFloat(2), Threshold: ggFloat(2), Hits: 2}}
	ggPlanActions(&r, g, groupGovernancePolicy())
	if len(r.Actions) != 1 || r.IncreaseCount != 1 || r.AdjustmentCount != 1 || r.PauseCount != 0 {
		t.Fatal("duplicate operations", r.Actions)
	}
	a := r.Actions[0]
	if a.ChannelID != 0 || a.Group != "g" || len(a.Models) != 1 || a.Models[0] != "m" || len(a.Subtypes) != 2 || len(a.Triggers) != 2 {
		t.Fatal(a)
	}
	for _, tr := range a.Triggers {
		if tr.ChannelID != v.ChannelID || tr.From != v.From || tr.To != v.To || tr.Rule.Actual == nil || tr.Rule.Threshold == nil {
			t.Fatal("wrong trigger", tr)
		}
	}
	if len(r.Metrics[0].Actions) > 0 || len(v.Actions) != 1 {
		t.Fatal("action attached to busiest bystander")
	}
}

func TestGGPressureAndSlowChannelsAllAppearInEvidence(t *testing.T) {
	r, g := ggActionFixture()
	for i := range r.Metrics {
		r.Metrics[i].Rules = nil
	}
	r.Metrics[0].Rules = []ggRuleResult{{ID: "pressure", Eligible: true, Active: true}}
	for i := 1; i <= 2; i++ {
		r.Metrics[i].Rules = []ggRuleResult{{ID: "frt_attention", Eligible: true, Active: true}}
	}
	ggPlanActions(&r, g, groupGovernancePolicy())
	if len(r.Actions) != 1 || len(r.Actions[0].Triggers) != 3 {
		t.Fatal(r.Actions)
	}
	for _, v := range r.Metrics {
		if len(v.Actions) != 1 {
			t.Fatal("missing participating channel")
		}
	}
}

func TestGGExplanationUsesExactRuleReferenceAndMissingKinds(t *testing.T) {
	v := ggTestMetrics(1, 2000)[0]
	v.Baseline.MeanMs = ggFloat(1000)
	v.Baseline.RPM = ggFloat(10)
	r := ggEvaluate(&v, groupGovernancePolicy())[0]
	ggDescribeRule(&v, &r, groupGovernancePolicy())
	if r.Comparison == nil || *r.Comparison != 1000 || r.Threshold == nil || *r.Threshold != 1300 || !strings.Contains(r.Description, "1.30 秒") {
		t.Fatal(r)
	}
	v.ConfigCurrent = false
	v.Quality = "unknown"
	v.Rules = []ggRuleResult{r}
	ggExplainMetric(&v, &ggRealtimeReport{}, groupGovernancePolicy())
	if !strings.Contains(v.Decision.Reason, "渠道配置尚未更新") || !strings.Contains(strings.Join(v.Decision.Gaps, " "), "可比较渠道不足") {
		t.Fatal(v.Decision)
	}
}
