package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func ggTestRow(at int64, id int, n, ms int64) modelFRTRow {
	r := modelFRTRow{BucketTs: at, Success: n, FRTSumMs: n * ms, FRTSumObserved: n, Row: modelStatisticsRow{Group: "g", Model: "m", ChannelID: id, TtftObserved: n, TtftMaxMs: int(ms)}}
	switch {
	case ms <= 500:
		r.Row.Ttft500 = n
	case ms <= 1000:
		r.Row.Ttft1k = n
	case ms <= 2000:
		r.Row.Ttft2k = n
	case ms <= 5000:
		r.Row.Ttft5k = n
	case ms <= 10000:
		r.Row.Ttft10k = n
	default:
		r.Row.TtftInf = n
	}
	if ms > 3000 {
		r.Row.TtftOver3s = n
	}
	return r
}
func ggTestCoverage(end int64) map[int]ggWindowCoverage {
	return map[int]ggWindowCoverage{15: {true, true, end}, 30: {true, true, end}, 45: {true, true, end}, 60: {true, true, end}}
}
func ggTestChannel(id int) ChannelSnap {
	return ChannelSnap{ID: id, Name: fmt.Sprintf("c%d", id), Groups: "g", Models: "m", Status: 1, BaseHost: fmt.Sprintf("c%d.example.net", id), UpdatedAt: 6000}
}
func ggTestMetrics(n int, mean float64) []ggMetrics {
	rows := []modelFRTRow{}
	channels := []ChannelSnap{}
	for id := 1; id <= n; id++ {
		channels = append(channels, ggTestChannel(id))
		for at := int64(6000 - 60*60); at < 6000; at += 60 {
			ms := int64(1000)
			if at >= 5100 {
				ms = int64(mean)
			}
			rows = append(rows, ggTestRow(at, id, 10, ms))
		}
	}
	return ggBuildMetrics(rows, nil, channels, 6000, ggTestCoverage(6000), groupGovernancePolicy())
}

func TestGGWeightedMeansHistogramAndAlignedExpansion(t *testing.T) {
	p := groupGovernancePolicy()
	rows := []modelFRTRow{ggTestRow(5940, 1, 1, 12000), ggTestRow(5880, 1, 99, 1000), ggTestRow(5940, 2, 2, 7000)}
	got := ggBuildMetrics(rows, nil, []ChannelSnap{ggTestChannel(1), ggTestChannel(2), ggTestChannel(3)}, 6000, ggTestCoverage(6000), p)
	for _, v := range got {
		want := 30
		if v.ChannelID == 1 {
			want = 15
		}
		if v.WindowMinutes != want || v.To != 6000 {
			t.Fatal("mixed comparison windows", v)
		}
	}
	a := got[0]
	if a.FRT.MeanMs == nil || *a.FRT.MeanMs != 1110 || *a.Over5s != 1 || *a.Over10s != 1 || a.Requests != 100 {
		t.Fatalf("weighted values %+v", a)
	}
	if got[1].Status != "low_sample" || got[2].Status != "no_requests" {
		t.Fatal(got[1].Status, got[2].Status)
	}
	channels := []ChannelSnap{ggTestChannel(1)}
	channels[0].Status = 2
	disabled := ggBuildMetrics(rows[:2], nil, channels, 6000, ggTestCoverage(6000), p)
	if disabled[0].Requests != 100 || disabled[0].ChannelStatus != 2 {
		t.Fatal("disabled history disappeared")
	}
}

func TestGGQueryParityWithModelFRTAndRequests(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	m.cfg.GroupGovernanceEnabled = true
	for i, entry := range []struct{ n, ms int64 }{{1, 12000}, {99, 1000}, {20, 6000}} {
		r := ggTestRow(end-int64(i+1)*60, 1, entry.n, entry.ms)
		s := CapacityUserMinuteSample{BucketTs: r.BucketTs, UserID: int64(i + 1), ChannelID: 1, Grp: "g", ModelName: "m", Success: entry.n, TtftObserved: entry.n, TtftMaxMs: int(entry.ms), Ttft500: r.Row.Ttft500, Ttft1k: r.Row.Ttft1k, Ttft2k: r.Row.Ttft2k, Ttft5k: r.Row.Ttft5k, Ttft10k: r.Row.Ttft10k, TtftInf: r.Row.TtftInf, TtftOver3s: r.Row.TtftOver3s, FRTSumMs: &r.FRTSumMs, FRTSumObserved: entry.n, FRTSumVersion: frtSumSemanticsVersion}
		if err := m.storeDB.Create(&s).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	minute, err := m.modelFRTRowsGrouped(ctx, end-300, end, ModelFRTFilter{}, true)
	if err != nil {
		t.Fatal(err)
	}
	whole, err := m.modelFRTRows(ctx, end-300, end, ModelFRTFilter{})
	if err != nil {
		t.Fatal(err)
	}
	got, want := ggAggregate(minute).view(true), ggAggregate(whole).view(true)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("different FRT: %+v %+v", got, want)
	}
	var independent int64
	if err = m.storeDB.Raw("SELECT SUM(success+anomaly+failed) FROM capacity_user_minute_samples WHERE bucket_ts>=? AND bucket_ts<?", end-300, end).Scan(&independent).Error; err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, r := range minute {
		total += r.Success + r.Anomaly + r.Failed
	}
	if total != independent || total != 120 {
		t.Fatal(total, independent)
	}
}

func TestGGErrorProjectionUsesDiagnosticsAndExactDenominator(t *testing.T) {
	batch := []CapacityUserMinuteSample{{BucketTs: 5940, ChannelID: 1, Grp: "g", ModelName: "m", Success: 10, Failed: 4, Anomaly: 1}}
	p := newGGErrorProjection(batch)
	cases := []struct{ body, other string }{{"status_code=502, request failed", `{"error_code":"array_above_max_length"}`}, {"status_code=503, system cpu overloaded (current: 95%, threshold: 90%)", `{}`}, {"status_code=502, openai_error", `{"error_code":"upstream_http2_stream_error"}`}, {"request failed", `{}`}}
	for i, c := range cases {
		r := customerHealthDiagnosticRow(LogChainRow{ID: int64(i + 1), CreatedAt: 5941, ChannelID: 1, Group: "g", ModelName: "m", Type: 5, RequestID: fmt.Sprintf("r%d", i), Content: c.body}, c.other)
		p.observe(r, logChainAttributeFaultWithEvidence(r, nil))
	}
	m := newTestMonitor(t)
	if err := p.publish(m.storeDB, 6000); err != nil {
		t.Fatal(err)
	}
	var stored []GroupGovernanceErrorMinute
	if err := m.storeDB.Find(&stored).Error; err != nil {
		t.Fatal(err)
	}
	r := ggTestRow(5940, 1, 10, 1000)
	r.Failed = 4
	r.Anomaly = 1
	v := ggBuildMetrics([]modelFRTRow{r}, stored, []ChannelSnap{ggTestChannel(1)}, 6000, ggTestCoverage(6000), groupGovernancePolicy())[0]
	if v.Requests != 15 || v.Pending < 2 || v.FaultRate != nil || v.ErrorsComplete {
		t.Fatalf("partial coverage became a fault rate: %+v", v)
	}
	for _, reason := range v.Reasons {
		if strings.Contains(reason.Reason, "数组") || strings.Contains(reason.Reason, "CPU") || strings.Contains(reason.Reason, "cpu") {
			if reason.Attributed {
				t.Fatal("customer/platform error became channel fault", reason)
			}
		}
	}
	// Finishing delivery classification refreshes the same minute projection.
	raw := customerHealthDiagnosticRow(LogChainRow{ID: 9, CreatedAt: 5942, ChannelID: 1, Group: "g", ModelName: "m", Type: 2, EndReason: "timeout"}, `{}`)
	raw.AnomalyTags = []string{"stream"}
	p.observe(raw, logChainAttributeFaultWithEvidence(raw, raw.AnomalyTags))
	if err := p.publish(m.storeDB, 6060); err != nil {
		t.Fatal(err)
	}
	m.storeDB.Find(&stored)
	v = ggBuildMetrics([]modelFRTRow{r}, stored, []ChannelSnap{ggTestChannel(1)}, 6000, ggTestCoverage(6000), groupGovernancePolicy())[0]
	if !v.ErrorsComplete || v.FaultRate == nil || len(stored) != 1 {
		t.Fatal("late evidence failed to update bounded numerator", v.ErrorsComplete, len(stored))
	}
}

func TestGGContinuityRecoveryStaleAndRestart(t *testing.T) {
	p := groupGovernancePolicy()
	old := ggRuleMemory{}
	var r ggRuleResult
	for _, at := range []int64{600, 600, 600} {
		old, r = ggUpdateRule(old, ggRuleResult{Eligible: true, Hit: true}, at, p, false)
	}
	if r.Active || r.Hits != 1 {
		t.Fatal("refresh counted as a minute", r)
	}
	old, r = ggUpdateRule(old, ggRuleResult{Eligible: true}, 660, p, false)
	old, r = ggUpdateRule(old, ggRuleResult{Eligible: true, Hit: true}, 720, p, false)
	if !r.Active || r.Hits != 2 {
		t.Fatal(r)
	}
	b, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	var restored ggRuleMemory
	if err := json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	for _, at := range []int64{780, 840} {
		restored, r = ggUpdateRule(restored, ggRuleResult{Eligible: true, recovered: true}, at, p, false)
		if !r.Active {
			t.Fatal("recovered too soon")
		}
	}
	restored, r = ggUpdateRule(restored, ggRuleResult{Eligible: false, recovered: true}, 900, p, false)
	if r.Recoveries != 0 || !r.Active {
		t.Fatal("missing data counted as recovery")
	}
	for _, at := range []int64{960, 1020, 1080} {
		restored, r = ggUpdateRule(restored, ggRuleResult{Eligible: true, recovered: true}, at, p, false)
	}
	if r.Active {
		t.Fatal("three recovery minutes did not withdraw", r)
	}
	restored, r = ggUpdateRule(restored, ggRuleResult{Eligible: true, Hit: true}, 1500, p, false)
	if r.Hits != 1 || r.Active {
		t.Fatal("old hits survived gap", r)
	}
}

func TestGGLateEvidenceDoesNotCountRefreshAsRecovery(t *testing.T) {
	p := groupGovernancePolicy()
	hit := ggRuleResult{Eligible: true, Hit: true}
	old, _ := ggUpdateRule(ggRuleMemory{}, hit, 600, p, true)
	old, _ = ggUpdateRule(old, ggRuleResult{}, 660, p, true)
	old, r := ggUpdateRule(old, ggRuleResult{}, 660, p, true)
	if !r.Active || r.Recoveries != 0 {
		t.Fatal("missing same-minute evidence erased a known failure", r)
	}
	for _, at := range []int64{720, 780} {
		for repeat := 0; repeat < 3; repeat++ {
			old, r = ggUpdateRule(old, ggRuleResult{Eligible: true, recovered: true}, at, p, true)
			if !r.Active {
				t.Fatal("refresh withdrew failure before three new recovery minutes", r)
			}
		}
	}
	old, r = ggUpdateRule(old, ggRuleResult{Eligible: true, recovered: true}, 780, p, true)
	if r.Recoveries != 2 {
		t.Fatal("same minute recovery counted twice", r)
	}
	// A corrected recovery must not remain in the consecutive recovery count.
	old, r = ggUpdateRule(old, hit, 780, p, true)
	if r.Recoveries != 0 || !r.Active {
		t.Fatal("late evidence did not correct recovery", r)
	}
	for _, at := range []int64{840, 900, 960} {
		old, r = ggUpdateRule(old, ggRuleResult{Eligible: true, recovered: true}, at, p, true)
	}
	if r.Active {
		t.Fatal("three genuine recovery minutes did not withdraw", r)
	}
	ordinary, _ := ggUpdateRule(ggRuleMemory{}, hit, 600, p, false)
	ordinary, _ = ggUpdateRule(ordinary, ggRuleResult{}, 660, p, false)
	ordinary, r = ggUpdateRule(ordinary, hit, 660, p, false)
	if !r.Active || r.Hits != 2 {
		t.Fatal("late evidence failed to activate the second distinct hit", r)
	}
	_, r = ggUpdateRule(ordinary, ggRuleResult{Eligible: true}, 660, p, false)
	if r.Active {
		t.Fatal("corrected provisional hit kept unsupported advice", r)
	}
}

func TestGGErrorSamplesRetainDistinctRequestsAcrossRetries(t *testing.T) {
	p := newGGErrorProjection([]CapacityUserMinuteSample{{BucketTs: 600, ChannelID: 1, Grp: "g", ModelName: "m", Failed: 4}})
	for i, id := range []string{"one", "one", "one", "two"} {
		p.observe(LogChainRow{ID: int64(i + 1), CreatedAt: 601, Type: 5, ChannelID: 1, Group: "g", ModelName: "m", RequestID: id}, logChainFault{Reason: "same cause"})
	}
	for _, reasons := range p.reasons {
		for _, r := range reasons {
			if r.Count != 4 || len(r.Samples) != 2 || r.Samples[1].RequestID != "two" {
				t.Fatal("retries displaced distinct request evidence", r)
			}
		}
	}
}

func TestGGComparisonsSmallPopulationAndLongTail(t *testing.T) {
	p := groupGovernancePolicy()
	ms := ggTestMetrics(5, 1000)
	ms[4].FRT.MeanMs = ggFloat(4000)
	vs := []*ggMetrics{}
	for i := range ms {
		vs = append(vs, &ms[i])
	}
	ggCompare(vs, p)
	// R-7 interpolation in floating point: arm64 fuses the multiply-add and
	// yields 2800.0000000000005, so compare with a tolerance.
	if math.Abs(*ms[4].Comparison.P90-2800) > 1e-6 || ms[4].Comparison.Count != 5 {
		t.Fatal(ms[4].Comparison)
	}
	ms[4].FRT.P50Ms = ggFloat(1000)
	ms[4].Over5sPct = ggFloat(2)
	ggCompare(vs, p)
	if !ms[4].LongTail || ggEvaluate(&ms[4], p)[1].Hit {
		t.Fatal("isolated tail suggested severe slowdown")
	}
	ggCompare(vs[:2], p)
	if vs[0].Comparison.P90 != nil {
		t.Fatal("small group used P90")
	}
	ggCompare(vs[:1], p)
	if ggEvaluate(vs[0], p)[1].Eligible {
		t.Fatal("single channel invented baseline")
	}
	vs[0].Baseline.MeanMs = ggFloat(500)
	if !ggEvaluate(vs[0], p)[0].Hit {
		t.Fatal("single channel did not use baseline")
	}
}

func TestGGNormalBaselineFreezesAndPressureAloneDoesNotExpand(t *testing.T) {
	p := groupGovernancePolicy()
	r := ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
	control := ggControl{}
	ggRecommend(&r, &control, "config", p)
	if len(r.Actions) != 0 {
		t.Fatal("normal generated actions", r.Actions)
	}
	for _, v := range r.Metrics {
		if v.Baseline.Minutes != 30 || v.Baseline.MeanMs == nil || *v.Baseline.MeanMs != 1000 {
			t.Fatal(v.Baseline)
		}
	}
	for step := 1; step <= 3; step++ {
		r = ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
		for i := range r.Metrics {
			r.Metrics[i].To += int64(step * 60)
			r.Metrics[i].RPM = 30
		}
		ggRecommend(&r, &control, "config", p)
	}
	if len(r.Actions) != 0 {
		t.Fatal("pressure alone expanded capacity", r.Actions)
	}
	before := control.Metrics[ggMemoryKey(&r.Metrics[0])].Normal
	r = ggRealtimeReport{Metrics: ggTestMetrics(5, 3000)}
	for i := range r.Metrics {
		r.Metrics[i].To += 240
	}
	ggRecommend(&r, &control, "config", p)
	if !r.Metrics[0].Baseline.Frozen || !reflect.DeepEqual(before, control.Metrics[ggMemoryKey(&r.Metrics[0])].Normal) {
		t.Fatal("abnormal data contaminated baseline")
	}
}

func ggActionFixture() (ggRealtimeReport, map[[2]string][]*ggMetrics) {
	r := ggRealtimeReport{Metrics: ggTestMetrics(3, 1000)}
	g := map[[2]string][]*ggMetrics{}
	for i := range r.Metrics {
		v := &r.Metrics[i]
		v.Baseline.PeakRPM = ggFloat(100)
		v.RPM = 10
		v.normal = make([]ggPoint, 30)
		for j := range v.normal {
			v.normal[j] = ggPoint{At: int64(j * 60), Requests: 100}
		}
		g[[2]string{v.Group, v.Model}] = append(g[[2]string{v.Group, v.Model}], v)
	}
	r.Metrics[0].Rules = []ggRuleResult{{ID: "frt_severe", Active: true, Eligible: true}}
	return r, g
}
func TestGGPauseScopeAlternativesAndDeduplication(t *testing.T) {
	p := groupGovernancePolicy()
	r, g := ggActionFixture()
	ggPlanActions(&r, g, p)
	if r.PauseCount != 1 || r.AdjustmentCount != 1 || len(r.Metrics[0].Alternatives) == 0 {
		t.Fatal(r.Actions)
	}
	r, g = ggActionFixture()
	r.Metrics[0].ChannelStatus = 2
	ggPlanActions(&r, g, p)
	if r.PauseCount != 0 {
		t.Fatal("disabled paused again")
	}
	r, g = ggActionFixture()
	for i := 1; i < len(r.Metrics); i++ {
		r.Metrics[i].channel.BaseHost = r.Metrics[0].channel.BaseHost
	}
	ggPlanActions(&r, g, p)
	if r.PauseCount != 0 || r.IncreaseCount != 1 {
		t.Fatal("same failure source accepted", r.Actions)
	}
	r, g = ggActionFixture()
	r.Metrics[1].Rules = []ggRuleResult{{ID: "frt_severe", Active: true, Eligible: true}}
	r.Metrics[2].normal = nil
	ggPlanActions(&r, g, p)
	if r.PauseCount != 0 || r.IncreaseCount != 1 {
		t.Fatal("cyclic substitutes or duplicate additions", r.Actions)
	}
	r, g = ggActionFixture()
	extra := r.Metrics[0]
	extra.Key = "other"
	extra.Group = "other"
	extra.Model = "unsupported"
	extra.Rules = nil
	r.Metrics = append(r.Metrics, extra)
	g[[2]string{"other", "unsupported"}] = []*ggMetrics{&r.Metrics[len(r.Metrics)-1]}
	ggPlanActions(&r, g, p)
	if r.PauseCount != 0 {
		t.Fatal("cross-group model was not checked")
	}
}

func TestGGExplicitLowVolumeFailureAndIdentity(t *testing.T) {
	v := ggTestMetrics(1, 1000)[0]
	v.Success = 0
	v.Requests = 2
	v.Errors = 2
	v.Reasons = []ggErrorReason{{RuleID: "LC-146", Unavailable: true, Attributed: true, Count: 2, Samples: []ggErrorSample{{RequestID: "one"}, {RequestID: "two"}}}}
	p := groupGovernancePolicy()
	rules := ggEvaluate(&v, p)
	found := false
	for _, r := range rules {
		if r.ID == "unavailable" {
			_, out := ggUpdateRule(ggRuleMemory{}, r, v.To, p, true)
			found = out.Active
		}
	}
	if !found {
		t.Fatal("low-volume explicit failure was suppressed")
	}
	v.Unavailable = false
	v.Reasons[0].Samples[1].RequestID = "one"
	ggEvaluate(&v, p)
	if v.Unavailable {
		t.Fatal("retry treated as a distinct request")
	}
	v.Reasons[0].Samples[1].RequestID = "two"
	v.Success = 1
	ggEvaluate(&v, p)
	if v.Unavailable {
		t.Fatal("nonconsecutive errors became unavailable")
	}
}

func TestGGConsecutiveUnavailableAfterEarlierSuccess(t *testing.T) {
	rows := []modelFRTRow{ggTestRow(5760, 1, 30, 1000), ggTestRow(5820, 1, 0, 0), ggTestRow(5880, 1, 0, 0)}
	rows[1].Failed, rows[2].Failed = 1, 1
	var errs []GroupGovernanceErrorMinute
	for i, at := range []int64{5820, 5880} {
		reason := []ggErrorReason{{RuleID: "LC-146", Reason: "上游认证失败", Attributed: true, Unavailable: true, Count: 1, Samples: []ggErrorSample{{RequestID: fmt.Sprintf("distinct-%d", i), CreatedAt: at + 1}}}}
		b, _ := json.Marshal(reason)
		errs = append(errs, GroupGovernanceErrorMinute{BucketTs: at, ChannelID: 1, Grp: "g", Model: "m", Failed: 1, FailedRead: 1, Version: groupGovernanceErrorVersion, ReasonsJSON: string(b)})
	}
	build := func() ggMetrics {
		return ggBuildMetrics(rows, errs, []ChannelSnap{ggTestChannel(1)}, 6000, ggTestCoverage(6000), groupGovernancePolicy())[0]
	}
	v := build()
	ggEvaluate(&v, groupGovernancePolicy())
	if !v.Unavailable || v.Success != 30 || v.FailureRun.Distinct != 2 {
		t.Fatal("earlier successes hid consecutive explicit failure", v.FailureRun)
	}
	rows = append(rows, ggTestRow(5940, 1, 1, 1000))
	v = build()
	ggEvaluate(&v, groupGovernancePolicy())
	if v.Unavailable || v.FailureRun.Distinct != 0 {
		t.Fatal("later success failed to interrupt the unavailable run", v.FailureRun)
	}
}

func TestGGRealtimePublicationAndConfigurationReset(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	m.cfg.GroupGovernanceEnabled = true
	channel := ggTestChannel(1)
	channel.UpdatedAt = modelFRTTestNow.Unix()
	m.storeDB.Create(&channel)
	m.storeDB.Create(&GroupGovernanceState{ID: 1, LastSuccessAt: modelFRTTestNow.Unix()})
	m.storeDB.Create(&GroupGovernanceGroup{Grp: "g", Current: true})
	r := ggTestRow(end-60, 1, 50, 1000)
	sample := CapacityUserMinuteSample{BucketTs: end - 60, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", Success: 50, TtftObserved: 50, Ttft1k: 50, TtftMaxMs: 1000, FRTSumMs: &r.FRTSumMs, FRTSumObserved: 50, FRTSumVersion: frtSumSemanticsVersion}
	m.storeDB.Create(&sample)
	if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow); err != nil {
		t.Fatal(err)
	}
	var a GroupGovernanceRealtimeState
	m.storeDB.First(&a, 1)
	if a.Through != end || a.Generation != 1 {
		t.Fatal(a.Through, a.Generation)
	}
	var published ggRealtimeReport
	if err := json.Unmarshal([]byte(a.ReportJSON), &published); err != nil {
		t.Fatal(err)
	}
	if len(published.Channels) != 1 || published.Channels[0].Requests != 50 {
		t.Fatal(published)
	}
	legacy := map[string]any{"status": "ready", "advice_ready": true, "metrics": []map[string]any{{"group": "g", "model": "m", "channel_id": 1, "requests": 50}}, "actions": []ggAction{{ID: "old", Kind: "increase"}}, "adjustment_count": 1}
	legacyJSON, _ := json.Marshal(legacy)
	m.storeDB.Model(&GroupGovernanceRealtimeState{}).Where("id=1").Updates(map[string]any{"report_json": string(legacyJSON), "updated_at": time.Now().Unix()})
	legacyRead, err := m.loadGroupGovernanceRealtime(context.Background())
	if err != nil || legacyRead.Status != "pending" || legacyRead.AdviceReady || len(legacyRead.Actions) != 0 || len(legacyRead.Channels) != 0 {
		t.Fatal("legacy model publication relabeled as channel totals", legacyRead, err)
	}
	m.storeDB.Model(&GroupGovernanceRealtimeState{}).Where("id=1").Update("report_json", a.ReportJSON)
	m.storeDB.Model(&GroupGovernanceState{}).Where("id=1").Update("last_success_at", 1)
	if err := m.syncGroupGovernanceRealtime(context.Background(), modelFRTTestNow); err != nil {
		t.Fatal(err)
	}
	m.storeDB.First(&a, 1)
	if err := json.Unmarshal([]byte(a.ReportJSON), &published); err != nil {
		t.Fatal(err)
	}
	if published.Status != "pending" || len(published.Actions) > 0 || published.Channels[0].Status == "unchanged" {
		t.Fatal("stale configuration became healthy/executable advice", published.Status)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.syncGroupGovernanceRealtime(ctx, modelFRTTestNow.Add(time.Minute)); err == nil {
		t.Fatal("cancelled publication")
	}
	var b GroupGovernanceRealtimeState
	m.storeDB.First(&b, 1)
	if b.Generation != a.Generation {
		t.Fatal("cancel advanced durable state")
	}
	control := ggControl{}
	rr := ggRealtimeReport{Metrics: ggTestMetrics(1, 2000)}
	ggRecommend(&rr, &control, "a", groupGovernancePolicy())
	rr = ggRealtimeReport{Metrics: ggTestMetrics(1, 2000)}
	ggRecommend(&rr, &control, "b", groupGovernancePolicy())
	for _, mem := range control.Metrics {
		for _, rule := range mem.Rules {
			if len(rule.History) != 1 {
				t.Fatal("config change kept old continuity")
			}
		}
	}
}

func TestGGQuantileInterpolation(t *testing.T) {
	v := ggQuantile([]float64{1, 2, 3, 4, 10}, .9)
	if v == nil || math.Abs(*v-7.6) > 1e-9 {
		t.Fatal(v)
	}
}

func TestGGCollectorEnrichmentIsOptionalAndReplayIdempotent(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = true
	m.prodDB = frtRecomputeProdDB(t)
	from := time.Now().Unix()/60*60 - 600
	frtRecomputeInsertLog(t, m.prodDB, 1, from+1, 1, 2, 1, 1, "g", "m", `{"frt":1000}`, 1)
	// The existing collector includes every type=5 retry attempt in Failed.
	for id := 2; id <= 3; id++ {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,created_at,user_id,channel_id,model_name,`group`,type,content,other,request_id) VALUES(?,?,?,?,?,?,5,?,?,?)", id, from+int64(id), 1, 1, "m", "g", "status_code=502, request failed", `{"error_code":"array_above_max_length"}`, fmt.Sprintf("retry-%d", id)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := m.sampleCustomerHealthRangeLow(context.Background(), from, from+60); err != nil {
			t.Fatal(err)
		}
	}
	var errors []GroupGovernanceErrorMinute
	if err := m.storeDB.Find(&errors).Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	m.storeDB.Model(&GroupGovernanceErrorMinute{}).Count(&count)
	if len(errors) != 1 || count != 1 || errors[0].Failed != 2 || errors[0].FailedRead != 2 {
		t.Fatal(errors, count)
	}
	var sample CapacityUserMinuteSample
	m.storeDB.First(&sample)
	if sample.Success+sample.Anomaly+sample.Failed != 3 || sample.CustomerHealthFailed != 0 {
		t.Fatal("shared denominator or customer stability changed", sample)
	}
	// A missing optional projection must leave independent minute ingestion usable.
	if err := m.storeDB.Migrator().DropTable(&GroupGovernanceErrorMinute{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.sampleCustomerHealthRangeLow(context.Background(), from, from+60); err != nil {
		t.Fatal("optional enrichment blocked FRT", err)
	}
}

func TestGGSharedAlternativeCapacityReservedOnce(t *testing.T) {
	r, g := ggActionFixture()
	r.Metrics[0].RPM = 40
	r.Metrics[1].RPM = 40
	r.Metrics[1].Rules = []ggRuleResult{{ID: "frt_severe", Eligible: true, Active: true}}
	ggPlanActions(&r, g, groupGovernancePolicy())
	if r.PauseCount != 1 || r.IncreaseCount != 1 {
		t.Fatalf("combined proposals overbooked the 70 RPM fallback: %+v", r.Actions)
	}
}

func TestGGCommonSlowdownDoesNotPauseEveryChannel(t *testing.T) {
	p := groupGovernancePolicy()
	control := ggControl{}
	seed := ggRealtimeReport{Metrics: ggTestMetrics(5, 1000)}
	ggRecommend(&seed, &control, "config", p)
	var r ggRealtimeReport
	for step := 1; step <= 3; step++ {
		r = ggRealtimeReport{Metrics: ggTestMetrics(5, 3000)}
		for i := range r.Metrics {
			r.Metrics[i].To += int64(step * 60)
			r.Metrics[i].RPM = 30
		}
		ggRecommend(&r, &control, "config", p)
	}
	if r.PauseCount != 0 {
		t.Fatal("common failure paused every channel", r.Actions)
	}
	if r.IncreaseCount != 1 {
		t.Fatal("group-wide pressure plus slowdown did not generate one capacity need", r.Actions)
	}
	for _, v := range r.Metrics {
		if !ggActive(&v, "group_slow") {
			t.Fatal("shared deterioration hidden by ranking", v.Rules)
		}
	}
}

func TestGGSmallChannelActualGapAndBaselineExclusion(t *testing.T) {
	p := groupGovernancePolicy()
	values := ggTestMetrics(2, 1000)
	values[1].FRT.MeanMs = ggFloat(1600)
	values[1].FRT.P50Ms = ggFloat(1500)
	values[1].Baseline.MeanMs = ggFloat(1000)
	ggCompare([]*ggMetrics{&values[0], &values[1]}, p)
	if !ggEvaluate(&values[1], p)[1].Hit {
		t.Fatal("60 percent actual gap lost in two-channel median")
	}
	v := values[1]
	v.From = 5100
	mem := ggMemory{Last: 6000, Normal: []ggPoint{{At: 5000}, {At: 5200}, {At: 5400}}}
	ggPrepareBaseline(&v, &mem, p)
	if len(mem.Normal) != 1 {
		t.Fatal("expanded window contaminated normal baseline", mem.Normal)
	}
	v.minutes = []ggPoint{{At: 5000, Complete: true, Errors: 1}}
	ggPrepareBaseline(&v, &mem, p)
	if len(mem.Normal) != 0 {
		t.Fatal("late error correction remained in the normal baseline", mem.Normal)
	}
}

func TestGGCorrelatedFailureAndRetiredPrimary(t *testing.T) {
	p := groupGovernancePolicy()
	r, g := ggActionFixture()
	r.Metrics[0].Rules = nil
	r.Metrics[1].Rules = nil
	for i := 0; i < 2; i++ {
		v := &r.Metrics[i]
		v.channel.BaseHost = "shared.example.net"
		v.Errors = 5
		v.Reasons = []ggErrorReason{{RuleID: "LC-080", Attributed: true, Count: 5}}
	}
	ggCompare(g[[2]string{"g", "m"}], p)
	if !r.Metrics[0].SharedFailure || !r.Metrics[1].SharedFailure {
		t.Fatal("shared error origin not detected")
	}
	r.Metrics[0].Rules = []ggRuleResult{{ID: "shared_failure", Active: true, Eligible: true}}
	r.Metrics[1].Rules = []ggRuleResult{{ID: "shared_failure", Active: true, Eligible: true}}
	ggPlanActions(&r, g, p)
	if r.PauseCount != 0 || r.IncreaseCount != 1 {
		t.Fatal(r.Actions)
	}
	r, g = ggActionFixture()
	r.Metrics[0].Rules = nil
	r.Metrics[0].ChannelStatus = 2
	r.Metrics[0].Requests = 500
	r.Metrics[1].Success = 0
	r.Metrics[2].Success = 0
	ggPlanActions(&r, g, p)
	if r.PauseCount != 0 || r.IncreaseCount != 1 {
		t.Fatal("retired primary backup need missing", r.Actions)
	}
}

// A failed attempt keeps the previous snapshot, so advice continues while
// that snapshot is recent; only age makes it stale (2026-11-13 queue timeouts
// paused advice for minutes although the configuration was minutes old).
func TestGGConfigStaysReadyAfterFailedAttemptWhileSnapshotRecent(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.GroupGovernanceSyncMinutes = 30
	now := time.Now()
	report := &ggRealtimeReport{Status: "ready", ChannelConfigAt: now.Unix() - 10}
	m.applyGroupGovernanceConfig(report, GroupGovernanceState{LastSuccessAt: now.Unix() - 600, LastError: "后台同步超时，已保留上次快照"}, now)
	if !report.ConfigReady || report.Status != "ready" {
		t.Fatalf("recent snapshot after one failed attempt must stay ready: %+v", report)
	}
	old := &ggRealtimeReport{Status: "ready", ChannelConfigAt: now.Unix() - 10}
	m.applyGroupGovernanceConfig(old, GroupGovernanceState{LastSuccessAt: now.Unix() - 2*30*60 - 1}, now)
	if old.ConfigReady || old.Status != "pending" {
		t.Fatalf("a snapshot older than two intervals is stale: %+v", old)
	}
}
