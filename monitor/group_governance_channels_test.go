package monitor

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestGGNoFirstByteSamplesDoNotEraseKnownErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int64
		complete bool
	}{
		{"complete_no_samples", 0, true}, {"complete_17_confirmed_errors", 17, true}, {"actual_collection_gap", 17, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := modelFRTRow{BucketTs: 5940, Success: 17 - tc.failures, Failed: tc.failures, Row: modelStatisticsRow{Group: "g", Model: "m", ChannelID: 1}}
			var errors []GroupGovernanceErrorMinute
			if tc.failures > 0 {
				b, _ := json.Marshal([]ggErrorReason{{RuleID: "LC-test", Reason: "上游 HTTP/2 流错误", Responsibility: "上游侧", Attributed: true, Count: tc.failures}})
				errors = append(errors, GroupGovernanceErrorMinute{BucketTs: 5940, Grp: "g", Model: "m", ChannelID: 1, Version: groupGovernanceErrorVersion, Failed: tc.failures, FailedRead: tc.failures, ReasonsJSON: string(b)})
			}
			coverage := ggTestCoverage(6000)
			coverage[30] = ggWindowCoverage{tc.complete, tc.complete, 6000}
			r := ggRealtimeReport{Metrics: ggBuildMetrics([]modelFRTRow{row}, errors, []ChannelSnap{ggTestChannel(1)}, 6000, coverage, groupGovernancePolicy())}
			ggRecommend(&r, &ggControl{}, "config", groupGovernancePolicy())
			m, c := r.Metrics[0], r.Channels[0]
			if c.FRT.MeanMs != nil || !strings.Contains(c.Decision.FRT, "无有效首字耗时样本，暂不能比较速度") {
				t.Fatal(c.Decision)
			}
			if tc.complete {
				for _, s := range []string{m.Baseline.FreezeReason, m.Decision.Reason, c.Baseline.FreezeReason, c.Decision.Reason, strings.Join(c.Decision.Gaps, ";")} {
					if strings.Contains(s, "采集落后") {
						t.Fatal("sample absence mislabeled as collection lag", s)
					}
				}
				if tc.failures > 0 && (c.Quality != "abnormal" || c.Errors != 17 || !strings.Contains(c.Decision.Errors, "上游 HTTP/2")) {
					t.Fatal(c.Decision)
				}
			} else if c.Status != "stale" || !strings.Contains(strings.Join(c.Decision.Gaps, ";"), "采集落后") {
				t.Fatal(c.Decision)
			}
		})
	}
}

func TestGGChannelTotalsUseAlignedFactsAndMinutePeaks(t *testing.T) {
	row := func(at int64, id int, model, group string, n, ms int64) modelFRTRow {
		r := ggTestRow(at, id, n, ms)
		r.Row.Model = model
		r.Row.Group = group
		return r
	}
	rows := []modelFRTRow{row(5940, 1, "a", "g", 20, 1000), row(5940, 1, "b", "g", 1, 9000), row(5400, 1, "a", "g", 30, 2000), row(5880, 1, "b", "g", 10, 3000), row(5940, 2, "a", "g", 20, 1000), row(5940, 1, "a", "other", 500, 10000)}
	rows[3].Failed = 4
	reasons, _ := json.Marshal([]ggErrorReason{{Reason: "上游限流", Responsibility: "上游侧", Attributed: true, Count: 3, Samples: []ggErrorSample{{LogID: 1, RequestID: "exact-one"}}}, {Reason: "待补错误原文", Pending: true, Count: 1}})
	errors := []GroupGovernanceErrorMinute{{BucketTs: 5880, ChannelID: 1, Grp: "g", Model: "b", Failed: 4, FailedRead: 4, Version: groupGovernanceErrorVersion, ReasonsJSON: string(reasons)}}
	c1, c2 := ggTestChannel(1), ggTestChannel(2)
	c1.Models = "a,b"
	c2.Models = "a"
	r := ggRealtimeReport{Metrics: ggBuildMetrics(rows, errors, []ChannelSnap{c1, c2}, 6000, ggTestCoverage(6000), groupGovernancePolicy())}
	ggRecommend(&r, &ggControl{}, "config", groupGovernancePolicy())
	var count int
	for _, c := range r.Channels {
		if c.Group == "g" && c.ChannelID == 1 {
			count++
			if c.WindowMinutes != 15 || c.From != 5100 || c.Requests != 65 || c.Errors != 3 || c.Pending != 1 || c.PeakRPM != 30 || c.FaultRate == nil || math.Abs(*c.FaultRate-300.0/65) > 1e-10 || c.FRT.MeanMs == nil || math.Abs(*c.FRT.MeanMs-119000.0/61) > 1e-10 {
				t.Fatalf("channel totals: %+v", c)
			}
			if c.Reasons[0].Samples[0].Model != "b" {
				t.Fatal("lost exact sample model")
			}
		}
	}
	if count != 1 {
		t.Fatal("channel repeated", count)
	}
	for _, m := range r.Metrics {
		if m.Group == "g" && m.WindowMinutes != 15 {
			t.Fatal("mixed model windows", m.Model)
		}
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var public map[string]json.RawMessage
	if err := json.Unmarshal(b, &public); err != nil {
		t.Fatal(err)
	}
	if _, ok := public["metrics"]; ok {
		t.Fatal("internal model totals leaked as public rows")
	}
	if len(public["channels"]) == 0 {
		t.Fatal("missing channel publication")
	}
}

func TestGGChannelSpeedComparisonPreservesModelMix(t *testing.T) {
	var rows []modelFRTRow
	var channels []ChannelSnap
	for id := 1; id <= 5; id++ {
		c := ggTestChannel(id)
		c.Models = "fast,slow"
		channels = append(channels, c)
		for _, model := range []string{"fast", "slow"} {
			n, ms := int64(100), int64(1000)
			if model == "slow" {
				n, ms = 20, 10000
			}
			if id == 1 {
				n = 120 - n
			}
			r := ggTestRow(5940, id, n, ms)
			r.Row.Model = model
			rows = append(rows, r)
		}
	}
	r := ggRealtimeReport{Metrics: ggBuildMetrics(rows, nil, channels, 6000, ggTestCoverage(6000), groupGovernancePolicy())}
	ggRecommend(&r, &ggControl{}, "config", groupGovernancePolicy())
	for _, c := range r.Channels {
		if c.ReferenceChange == nil || math.Abs(*c.ReferenceChange) > 1e-10 || c.SpeedState != "normal" || c.Status != "unchanged" {
			t.Fatal("model mix became slowdown", c.Decision, c.ReferenceChange)
		}
	}
	if len(r.Actions) > 0 {
		t.Fatal("normal model mix created operation")
	}
}

func TestGGIncreaseOperationMergesModelsAndKeepsTriggerChannels(t *testing.T) {
	r, _ := ggActionFixture()
	for i := range r.Metrics {
		r.Metrics[i].Rules = nil
	}
	r.Metrics[1].Rules = []ggRuleResult{{ID: "capacity_limit", Eligible: true, Hit: true, Active: true, Actual: ggFloat(3), Threshold: ggFloat(1)}}
	other := r.Metrics[1]
	other.Model = "other-model"
	other.Key = ggKey(ggMetricKey{other.Group, other.Model, other.ChannelID})
	r.Metrics = append(r.Metrics, other)
	// Appending can relocate the slice; rebuild all pointers.
	groups := map[[2]string][]*ggMetrics{}
	for i := range r.Metrics {
		v := &r.Metrics[i]
		groups[[2]string{v.Group, v.Model}] = append(groups[[2]string{v.Group, v.Model}], v)
	}
	ggPlanActions(&r, groups, groupGovernancePolicy())
	if len(r.Actions) != 1 || r.IncreaseCount != 1 || len(r.Actions[0].Models) != 2 || len(r.Actions[0].Triggers) != 2 || len(r.Actions[0].Scope) != 2 {
		t.Fatal(r.Actions)
	}
	for _, tr := range r.Actions[0].Triggers {
		if tr.ChannelID != r.Metrics[1].ChannelID {
			t.Fatal("wrong triggering channel")
		}
	}
	if len(r.Metrics[0].Actions) > 0 {
		t.Fatal("unrelated busiest channel acquired operation")
	}
}

func TestGGChannelMissingEvidenceNamesActualGap(t *testing.T) {
	rows := []modelFRTRow{ggTestRow(5940, 1, 5, 1000)}
	coverage := ggTestCoverage(6000)
	// The current window is complete, but the preceding normal baseline is
	// missing. Complete zero-request history would correctly give a zero RPM.
	coverage[60] = ggWindowCoverage{false, false, 0}
	r := ggRealtimeReport{Metrics: ggBuildMetrics(rows, nil, []ChannelSnap{ggTestChannel(1)}, 6000, coverage, groupGovernancePolicy())}
	ggRecommend(&r, &ggControl{}, "config", groupGovernancePolicy())
	c := r.Channels[0]
	if !strings.Contains(c.Decision.FRT, "样本不足 20 个") || strings.Contains(strings.Join(c.Decision.Gaps, ";"), "采集落后") || strings.Contains(c.Decision.Traffic, "为零") {
		t.Fatal("missing sample and missing baseline were conflated", c.Decision)
	}
	c.Baseline.RPM = ggFloat(0)
	ggExplainChannel(&c, &r)
	if !strings.Contains(c.Decision.Traffic, "正常请求量为零") || c.GrowthPct != nil {
		t.Fatal("zero baseline was turned into growth", c.Decision)
	}
}
