package monitor

import (
	"math"
	"strings"
	"testing"
)

func TestGGChannelWindowSelectionAndSameWindowPeers(t *testing.T) {
	p := groupGovernancePolicy()
	rows := []modelFRTRow{ggTestRow(5940, 1, 30, 1000), ggTestRow(4800, 1, 90, 3000), ggTestRow(5940, 2, 5, 2000), ggTestRow(4800, 2, 20, 2000), ggTestRow(4800, 3, 21, 1000), ggTestRow(5940, 4, 1, 1000)}
	// An idle model cannot expand a busy channel or its whole group.
	c1 := ggTestChannel(1)
	c1.Models = "m,idle"
	other := ggTestRow(5940, 1, 1000, 1000)
	other.Row.Group = "other"
	rows = append(rows, other)
	r := ggRealtimeReport{Metrics: ggBuildMetrics(rows, nil, []ChannelSnap{c1, ggTestChannel(2), ggTestChannel(3), ggTestChannel(4)}, 6000, ggTestCoverage(6000), p)}
	ggRecommend(&r, &ggControl{}, "config", p)
	for _, c := range r.Channels {
		if c.Group != "g" {
			continue
		}
		want := 30
		if c.ChannelID == 1 {
			want = 15
		}
		if c.WindowMinutes != want || c.To-c.From != int64(want)*60 {
			t.Fatal("wrong channel window", c.ChannelID, c.WindowMinutes)
		}
		if c.ChannelID == 1 && (c.Requests != 30 || c.RPM != 2 || c.PeakRPM != 30 || c.SharePct == nil || math.Abs(*c.SharePct-100.0*30/36) > 1e-8) {
			t.Fatal("short totals mixed with long or other group", c.Requests, c.SharePct)
		}
		if c.ChannelID == 2 && (c.Requests != 25 || c.SharePct == nil || math.Abs(*c.SharePct-100.0*25/167) > 1e-8) {
			t.Fatal("long share used selected short denominator", c.Requests, c.SharePct)
		}
		if c.ChannelID == 3 && c.Requests != 21 {
			t.Fatal("zero recent requests hid previous fifteen minutes")
		}
		if c.ChannelID == 4 && (!strings.Contains(c.WindowReason, "近 15 分钟") || !strings.Contains(strings.Join(c.Decision.Gaps, ";"), "请求样本不足")) {
			t.Fatal(c.WindowReason, c.Decision)
		}
	}
	for _, m := range r.Metrics {
		if m.Group == "g" && m.ChannelID == 2 && m.Model == "m" {
			// Same-model peers at 30m: channel 1=2500ms, channel 3=1000ms.
			if m.Comparison.PeerMedian == nil || *m.Comparison.PeerMedian != 1750 || m.Comparison.Count != 3 {
				t.Fatal("peer used fifteen minute mean", m.Comparison)
			}
		}
	}
}

func TestGGWindowHistoryIsolatedAndOldPolicyReset(t *testing.T) {
	p := groupGovernancePolicy()
	c := ggControl{Version: "gg-realtime-2026-11-11.1", Metrics: map[string]ggMemory{"old": {Last: 6000, Rules: map[string]ggRuleMemory{"unavailable": {Active: true}}}}}
	r := ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
	ggRecommend(&r, &c, "config", p)
	if _, ok := c.Metrics["old"]; ok {
		t.Fatal("previous window policy survived")
	}
	short, long := r.Metrics[0], r.Metrics[0]
	long.WindowMinutes = 30
	if ggMemoryKey(&short) == ggMemoryKey(&long) {
		t.Fatal("window histories share identity")
	}
	if len(c.Metrics[ggMemoryKey(&short)].Rules) == 0 || len(c.Metrics[ggMemoryKey(&long)].Rules) == 0 {
		t.Fatal("both comparison windows must retain quality history")
	}
	before := c.Metrics[ggMemoryKey(&short)].Rules["pressure"].Last
	r = ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
	ggRecommend(&r, &c, "config", p)
	if after := c.Metrics[ggMemoryKey(&short)].Rules["pressure"]; after.Last != before || len(after.History) != 1 {
		t.Fatal("refresh counted another minute", after)
	}
}

func TestGGLowVolumeFaultRateNeedsSamplesButConcreteFailureDoesNot(t *testing.T) {
	v := ggMetrics{Requests: 3, Errors: 3, ErrorsComplete: true, FaultRate: ggFloat(100), FaultP50: ggFloat(0), FaultPeers: 5, FailureRun: ggFailureRun{Attempts: 3, Distinct: 2}}
	r := ggEvaluate(&v, groupGovernancePolicy())
	for _, rule := range r {
		if rule.ID == "fault_attention" && rule.Eligible {
			t.Fatal("ratio compared before request sample floor")
		}
		if rule.ID == "unavailable" && (!rule.Eligible || !rule.Hit) {
			t.Fatal("concrete failure hidden by sample floor")
		}
	}
}

func TestGGContextTriggersRequireSelectedScopeAndWindow(t *testing.T) {
	leaf := func(model string, channel, minutes int, ids ...string) ggMetrics {
		v := ggMetrics{Key: ggKey(ggMetricKey{"g", model, channel}), Group: "g", Model: model, ChannelID: channel, ChannelStatus: 1, ConfigCurrent: true, WindowMinutes: minutes, From: 6000 - int64(minutes)*60, To: 6000}
		for _, id := range ids {
			v.Rules = append(v.Rules, ggRuleResult{ID: id, Active: true, Eligible: true, Hit: true})
		}
		return v
	}
	for _, model := range []string{"m", "unrelated"} {
		t.Run(model, func(t *testing.T) {
			// Same group, but no selected trigger in the context window/model.
			// Map iteration may visit either window first; neither can leak evidence.
			for i := 0; i < 20; i++ {
				r := ggRealtimeReport{Metrics: []ggMetrics{leaf("m", 1, 15, "capacity_limit")}}
				a, b := leaf(model, 2, 30, "pressure", "frt_severe"), leaf(model, 3, 30, "frt_severe")
				r.comparisonMetrics = []*ggMetrics{&r.Metrics[0], &a, &b}
				ggPlanActions(&r, nil, groupGovernancePolicy())
				if len(r.Actions) != 1 || len(r.Actions[0].Triggers) != 1 || r.Actions[0].Triggers[0].Rule.ID != "capacity_limit" {
					t.Fatal("unselected scope/window attached to operation", r.Actions)
				}
			}
		})
	}
	r := ggRealtimeReport{Metrics: []ggMetrics{leaf("m", 1, 30, "pressure")}}
	a, b := leaf("m", 2, 30, "frt_severe"), leaf("m", 3, 30, "frt_severe")
	r.comparisonMetrics = []*ggMetrics{&r.Metrics[0], &a, &b}
	ggPlanActions(&r, nil, groupGovernancePolicy())
	if len(r.Actions) != 1 || len(r.Actions[0].Triggers) != 3 || len(a.Actions)+len(b.Actions) != 0 {
		t.Fatal("same-window supporting evidence lost or assigned to unselected channels", r.Actions)
	}
}

func TestGGChannelRollingWindowSwitchRetainsIndependentHistory(t *testing.T) {
	p, control := groupGovernancePolicy(), ggControl{}
	rows := []modelFRTRow{ggTestRow(4800, 1, 25, 2000), ggTestRow(5100, 1, 1, 1000), ggTestRow(5880, 1, 19, 1000)}
	for at := int64(2400); at < 6120; at += 60 {
		rows = append(rows, ggTestRow(at, 2, 5, 1000))
	}
	for i, end := range []int64{6000, 6060, 6120} {
		if i == 2 {
			rows = append(rows, ggTestRow(6060, 1, 1, 1000))
		}
		r := ggRealtimeReport{Metrics: ggBuildMetrics(rows, nil, []ChannelSnap{ggTestChannel(1), ggTestChannel(2)}, end, ggTestCoverage(end), p)}
		ggRecommend(&r, &control, "same-config", p)
		for _, c := range r.Channels {
			want, attempts := 15, int64(20)
			if c.ChannelID == 2 {
				attempts = 75
			} else if i == 1 {
				want, attempts = 30, 45
			}
			if c.WindowMinutes != want || c.Requests != attempts || c.FRT.Observed == nil || *c.FRT.Observed != attempts || c.RPM != float64(attempts)/float64(want) {
				t.Fatal("rolling window totals or cadence incorrect", end, c.ChannelID, c.WindowMinutes, c.Requests, c.RPM)
			}
			for _, n := range []int{15, 30} {
				leaf := c.ggMetrics
				leaf.WindowMinutes = n
				if control.Channels[ggMemoryKey(&leaf)].Last != end {
					t.Fatal("window history did not advance independently", end, n)
				}
			}
		}
	}
}
