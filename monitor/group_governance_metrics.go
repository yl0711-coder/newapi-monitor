package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Every threshold and resource bound for this rule revision is defined here.
// Changes require a version bump so persisted hit history cannot mix policies.
type ggPolicy struct {
	Version                                                                         string `json:"version"`
	WindowMinutes, ExpandedMinutes, BaselineMinutes, MinSamples, PercentileChannels int
	FRTAttention, FRTSevere, Pressure, GroupSlow, RecoveryRatio                     float64
	FaultDelta, RecoveryFaultDelta                                                  float64
	MinErrors, HitMinutes, HitRequired, RecoveryMinutes                             int
	MinSlowShare, CapacityReserve                                                   float64
	StaleSeconds, PollSeconds                                                       int64
}

func groupGovernancePolicy() ggPolicy {
	return ggPolicy{
		Version: "gg-realtime-2026-11-11.2", WindowMinutes: 15, ExpandedMinutes: 30, BaselineMinutes: 30, MinSamples: 20, PercentileChannels: 5,
		FRTAttention: 1.3, FRTSevere: 1.5, Pressure: 1.5, GroupSlow: 1.5, RecoveryRatio: 1.2, FaultDelta: 5, RecoveryFaultDelta: 2,
		MinErrors: 3, HitMinutes: 3, HitRequired: 2, RecoveryMinutes: 3, MinSlowShare: 20, CapacityReserve: 0.8, StaleSeconds: 180, PollSeconds: 30,
	}
}

const groupGovernanceMaxMinuteRows = 60000
const groupGovernanceReadBudget = 6 * time.Second

type ggPoint struct {
	At       int64    `json:"at"`
	Requests int64    `json:"requests"`
	Success  int64    `json:"success"`
	MeanMs   *float64 `json:"mean_ms"`
	Observed int64    `json:"observed"`
	SumMs    int64    `json:"sum_ms"`
	Errors   int64    `json:"errors"`
	Pending  int64    `json:"pending"`
	Complete bool     `json:"complete"`
}
type ggComparison struct {
	Count          int      `json:"count"`
	Method         string   `json:"method"`
	P50            *float64 `json:"p50_ms"`
	P80            *float64 `json:"p80_ms"`
	P90            *float64 `json:"p90_ms"`
	PeerMedian     *float64 `json:"other_channel_median_ms"`
	Position       int      `json:"position"`
	AboveMedianPct *float64 `json:"above_median_pct"`
}
type ggBaseline struct {
	MeanMs          *float64 `json:"mean_ms"`
	RPM             *float64 `json:"rpm"`
	PeakRPM         *float64 `json:"peak_rpm"`
	Minutes         int      `json:"minutes"`
	From            int64    `json:"from_ts"`
	To              int64    `json:"to_ts"`
	Frozen          bool     `json:"frozen"`
	Source          string   `json:"source"`
	FreezeReason    string   `json:"freeze_reason,omitempty"`
	ResumeCondition string   `json:"resume_condition,omitempty"`
}
type ggFailureRun struct {
	From     int64  `json:"from_ts"`
	Attempts int64  `json:"attempts"`
	Distinct int    `json:"distinct_requests"`
	RuleID   string `json:"rule_id"`
}
type ggMetrics struct {
	Key                  string          `json:"key"`
	Group                string          `json:"group"`
	ChannelID            int             `json:"channel_id"`
	ChannelName          string          `json:"channel_name"`
	ChannelStatus        int             `json:"channel_status"`
	Model                string          `json:"model"`
	From                 int64           `json:"from_ts"`
	To                   int64           `json:"to_ts"`
	WindowMinutes        int             `json:"window_minutes"`
	WindowReason         string          `json:"window_reason,omitempty"`
	Status               string          `json:"status"`
	FRT                  ModelFRTMetrics `json:"frt"`
	Over5s               *int64          `json:"over_5s"`
	Over10s              *int64          `json:"over_10s"`
	Over5sPct            *float64        `json:"over_5s_pct"`
	Over10sPct           *float64        `json:"over_10s_pct"`
	Comparison           ggComparison    `json:"comparison"`
	Baseline             ggBaseline      `json:"baseline"`
	FRTChangePct         *float64        `json:"frt_change_pct"`
	Requests             int64           `json:"requests"`
	Success              int64           `json:"success"`
	Errors               int64           `json:"attributed_errors"`
	Pending              int64           `json:"pending_errors"`
	KnownOther           int64           `json:"other_errors"`
	FaultRate            *float64        `json:"fault_rate_pct"`
	FaultP50             *float64        `json:"fault_p50_pct"`
	FaultP90             *float64        `json:"fault_p90_pct"`
	FaultPeers           int             `json:"fault_peers"`
	Reasons              []ggErrorReason `json:"error_reasons"`
	RPM                  float64         `json:"rpm"`
	PeakRPM              int64           `json:"peak_rpm"`
	GrowthPct            *float64        `json:"growth_pct"`
	SharePct             *float64        `json:"share_pct"`
	Trend                []ggPoint       `json:"trend"`
	Rules                []ggRuleResult  `json:"rules"`
	Actions              []string        `json:"actions"`
	Alternatives         []ggAlternative `json:"alternatives"`
	Checks               []string        `json:"checks"`
	Missing              []string        `json:"missing"`
	RequestsComplete     bool            `json:"requests_complete"`
	ErrorsComplete       bool            `json:"errors_complete"`
	FRTThrough           int64           `json:"frt_through_ts"`
	RequestsThrough      int64           `json:"requests_through_ts"`
	ErrorsThrough        int64           `json:"errors_through_ts"`
	ErrorCapturedAt      int64           `json:"error_captured_at"`
	ConfigAt             int64           `json:"config_at"`
	ConfigCurrent        bool            `json:"config_current"`
	Stale                bool            `json:"stale"`
	LongTail             bool            `json:"long_tail"`
	GroupSlow            bool            `json:"group_slow"`
	SharedFailure        bool            `json:"shared_failure"`
	Unavailable          bool            `json:"unavailable"`
	FailureRun           ggFailureRun    `json:"failure_run"`
	CapacityLimited      bool            `json:"capacity_limited"`
	Decision             ggDecision      `json:"decision"`
	Quality              string          `json:"quality"`
	FRTSourceComplete    bool            `json:"frt_source_complete"`
	frtAggregate         modelFRTAggregate
	groupMean, groupBase *float64
	minutes              []ggPoint
	normal               []ggPoint
	channel              ChannelSnap
	windowMetrics        []*ggMetrics // Shared, local-only comparison populations for both windows.
}
type ggRealtimeReport struct {
	RequestsComplete  bool               `json:"requests_complete"`
	Version           string             `json:"rule_version"`
	Policy            ggPolicy           `json:"policy"`
	GeneratedAt       int64              `json:"generated_at"`
	Through           int64              `json:"metrics_through_ts"`
	Target            int64              `json:"target_ts"`
	ConfigAt          int64              `json:"config_snapshot_at"`
	ChannelConfigAt   int64              `json:"channel_snapshot_at"`
	FinalizeDelay     int64              `json:"finalize_delay_seconds"`
	Status            string             `json:"status"`
	Note              string             `json:"note"`
	Metrics           []ggMetrics        `json:"-"` // Internal same-model comparisons; never repeated as public channel rows.
	Channels          []ggChannelMetrics `json:"channels"`
	Actions           []ggAction         `json:"actions"`
	AdjustmentCount   int                `json:"adjustment_count"`
	PauseCount        int                `json:"pause_count"`
	IncreaseCount     int                `json:"increase_count"`
	AdviceReady       bool               `json:"advice_ready"`
	ConfigReady       bool               `json:"config_ready"`
	ChannelMaxAge     int64              `json:"channel_max_age_seconds"`
	comparisonMetrics []*ggMetrics
}
type ggWindowCoverage struct {
	requests, frt bool
	through       int64
}

func ggKey(k ggMetricKey) string { b, _ := json.Marshal(k); return string(b) }

func ggAggregate(rows []modelFRTRow) modelFRTAggregate {
	var a modelFRTAggregate
	for _, r := range rows {
		b := modelFRTAggregate{sumMs: r.FRTSumMs, sumObserved: r.FRTSumObserved, meanUnsupported: r.MeanUnsupportedRows > 0, meanInvalid: r.MeanInvalidRows > 0, historical: r.HistoricalRows > 0, invalid: r.InvalidRows > 0}
		mergeModelStatisticsTTFT(&b.hist, r.Row)
		a.merge(b)
	}
	return a
}

// Read a single, bounded SQLite snapshot. Same aligned right edge for every
// channel and metric; historical gaps never become coverage certificates.
func (m *Monitor) readGroupGovernanceMetrics(ctx context.Context, now time.Time) (ggRealtimeReport, error) {
	p := groupGovernancePolicy()
	r := ggRealtimeReport{Version: p.Version, Policy: p, GeneratedAt: now.Unix(), Status: "pending", Metrics: []ggMetrics{}, Actions: []ggAction{}}
	target, end, proofFrom, err := m.groupGovernanceWindow(ctx, now)
	r.Target = target
	r.FinalizeDelay = now.Unix()/60*60 - target
	if err != nil {
		return r, err
	}
	r.Through = end
	if end <= 0 {
		r.Note = "尚无已提交的完整分钟"
		return r, nil
	}
	start := end - int64(p.ExpandedMinutes+p.BaselineMinutes)*60
	rows, err := m.modelFRTRowsGrouped(ctx, start, end, ModelFRTFilter{}, true)
	if err != nil {
		return r, err
	}
	var channels []ChannelSnap
	if err = m.storeDB.WithContext(ctx).Limit(modelFRTMaxLeaves + 1).Find(&channels).Error; err != nil {
		return r, err
	}
	if len(channels) > modelFRTMaxLeaves {
		return r, errModelFRTLimit
	}
	var errors []GroupGovernanceErrorMinute
	if err = m.storeDB.WithContext(ctx).Where("bucket_ts>=? AND bucket_ts<?", start, end).Limit(groupGovernanceMaxMinuteRows + 1).Find(&errors).Error; err != nil {
		return r, err
	}
	if len(errors) > groupGovernanceMaxMinuteRows {
		return r, errModelFRTLimit
	}
	coverage := map[int]ggWindowCoverage{}
	for _, n := range []int{p.WindowMinutes, p.ExpandedMinutes, p.WindowMinutes + p.BaselineMinutes, p.ExpandedMinutes + p.BaselineMinutes} {
		from := end - int64(n)*60
		if !m.cfg.CustomerHealthSourceEnabled {
			complete := proofFrom > 0 && proofFrom <= from && end <= target
			// Per-row FRT validity and missing sums are still evaluated by
			// ggAggregate. No histogram sample is invented by this proof.
			coverage[n] = ggWindowCoverage{complete, complete, end}
			continue
		}
		requests, _, through, _ := m.modelStatisticsFactsCoverage(ctx, from, end)
		frt, readErr := m.modelFRTCoverage(ctx, from, end)
		if readErr != nil {
			return r, readErr
		}
		coverage[n] = ggWindowCoverage{requests, frt.Complete, through}
	}
	r.Metrics = ggBuildMetrics(rows, errors, channels, end, coverage, p)
	r.RequestsComplete = coverage[p.WindowMinutes].requests
	for _, v := range r.Metrics {
		r.RequestsComplete = r.RequestsComplete && v.RequestsComplete
	}
	for _, c := range channels {
		if c.DeletedAt == 0 && (r.ChannelConfigAt == 0 || c.UpdatedAt < r.ChannelConfigAt) {
			r.ChannelConfigAt = c.UpdatedAt
		}
	}
	r.Status = "ready"
	if target-end > p.StaleSeconds || m.modelReportCollectionFailed(false) {
		r.Status = "stale"
		r.Note = "采集落后或来源失败；保留已知指标，不以旧值确认健康或执行调整"
	}
	for i := range r.Metrics {
		v := &r.Metrics[i]
		v.Stale = r.Status == "stale"
		if v.Stale {
			v.Status = "stale"
		}
	}
	return r, ctx.Err()
}

func ggBuildWindowMetrics(rows []modelFRTRow, errors []GroupGovernanceErrorMinute, channels []ChannelSnap, end int64, coverage map[int]ggWindowCoverage, p ggPolicy, n int) []ggMetrics {
	byKey := map[ggMetricKey][]modelFRTRow{}
	byChannel := map[int]ChannelSnap{}
	for _, c := range channels {
		byChannel[c.ID] = c
		if c.DeletedAt == 0 {
			for _, g := range splitList(c.Groups) {
				for _, model := range splitList(c.Models) {
					byKey[ggMetricKey{g, model, c.ID}] = nil
				}
			}
		}
	}
	for _, r := range rows {
		k := ggMetricKey{strings.TrimSpace(r.Row.Group), strings.TrimSpace(r.Row.Model), r.Row.ChannelID}
		byKey[k] = append(byKey[k], r)
	}
	errorMap := map[ggMinuteKey]GroupGovernanceErrorMinute{}
	for _, e := range errors {
		errorMap[ggMinuteKey{ggMetricKey{e.Grp, e.Model, e.ChannelID}, e.BucketTs}] = e
	}
	result := make([]ggMetrics, 0, len(byKey))
	for k, rs := range byKey {
		if len(result) >= modelFRTMaxLeaves {
			break
		}
		proof := coverage[n]
		from := end - int64(n)*60
		c := byChannel[k.Channel]
		v := ggMetrics{Key: ggKey(k), Group: k.Group, Model: k.Model, ChannelID: k.Channel, ChannelName: c.Name, ChannelStatus: c.Status, From: from, To: end, WindowMinutes: n, Status: "pending", ConfigAt: c.UpdatedAt, ConfigCurrent: c.ID > 0 && c.DeletedAt == 0 && ggHasString(splitList(c.Groups), k.Group) && ggHasString(splitList(c.Models), k.Model), channel: c, RequestsComplete: proof.requests, ErrorsComplete: proof.requests, RequestsThrough: proof.through, Missing: []string{}, Checks: []string{}, Actions: []string{}, Rules: []ggRuleResult{}, Alternatives: []ggAlternative{}, Reasons: []ggErrorReason{}}
		var current []modelFRTRow
		minuteRows := map[int64]modelFRTRow{}
		for _, r := range rs {
			minuteRows[r.BucketTs] = r
			if r.BucketTs >= from && r.BucketTs < end {
				current = append(current, r)
			}
		}
		reasons := map[string]*ggErrorReason{}
		runKey := ""
		runIDs := map[string]bool{}
		for t := end - int64(p.ExpandedMinutes+p.BaselineMinutes)*60; t < end; t += 60 {
			r := minuteRows[t]
			agg := ggAggregate([]modelFRTRow{r})
			fv := agg.view(true)
			point := ggPoint{At: t, Requests: r.Success + r.Anomaly + r.Failed, Success: r.Success, MeanMs: fv.MeanMs, Observed: r.FRTSumObserved, SumMs: r.FRTSumMs, Complete: coverage[p.ExpandedMinutes+p.BaselineMinutes].requests || (t >= from-int64(p.BaselineMinutes)*60 && coverage[n+p.BaselineMinutes].requests)}
			if t >= from {
				point.Complete = proof.requests
			}
			e, ok := errorMap[ggMinuteKey{k, t}]
			complete := ok && e.Version == groupGovernanceErrorVersion && e.Failed == r.Failed && e.Anomaly == r.Anomaly && e.FailedRead == r.Failed && e.AnomalyRead == r.Anomaly
			// A proven zero error population needs no diagnostic record.
			if r.Failed+r.Anomaly == 0 {
				complete = true
			}
			var known []ggErrorReason
			decodeErr := json.Unmarshal([]byte(e.ReasonsJSON), &known)
			var reasonCount int64
			validReasons := decodeErr == nil
			for _, why := range known {
				if why.Count <= 0 || (why.Attributed && why.Pending) {
					validReasons = false
				}
				reasonCount += why.Count
			}
			if reasonCount != e.FailedRead+e.AnomalyRead {
				validReasons = false
			}
			if r.Failed+r.Anomaly > 0 {
				complete = complete && validReasons
			}
			var read int64
			if validReasons && e.Version == groupGovernanceErrorVersion && e.Failed <= r.Failed && e.Anomaly <= r.Anomaly {
				for _, why := range known {
					read += why.Count
					if why.Attributed {
						point.Errors += why.Count
					} else if why.Pending {
						point.Pending += why.Count
					}
					if t >= from {
						key := why.RuleID + "\x00" + why.Code + "\x00" + why.Reason + "\x00" + why.Responsibility
						if reasons[key] == nil {
							x := why
							x.Count = 0
							x.Samples = nil
							reasons[key] = &x
						}
						dest := reasons[key]
						dest.Count += why.Count
						for _, s := range why.Samples {
							duplicate := false
							for _, existing := range dest.Samples {
								if s.RequestID != "" && existing.RequestID == s.RequestID {
									duplicate = true
								}
							}
							if !duplicate && len(dest.Samples) < 6 {
								dest.Samples = append(dest.Samples, s)
							}
						}
					}
				}
			}
			point.Pending += max(int64(0), r.Failed+r.Anomaly-read)
			point.Complete = point.Complete && complete
			v.minutes = append(v.minutes, point)
			if t >= from {
				// A complete trailing minute sequence can establish consecutive
				// failures even when an earlier minute had successful requests. A
				// mixed/incomplete minute breaks the proof; sampled errors alone
				// never establish ordering around an unseen success.
				if !point.Complete || point.Requests > 0 {
					if point.Complete && len(known) == 1 && known[0].Attributed && known[0].Unavailable && known[0].Count == point.Requests {
						why := known[0]
						identity := why.RuleID + "\x00" + why.Code + "\x00" + why.Reason
						if identity != runKey {
							v.FailureRun = ggFailureRun{From: t, RuleID: why.RuleID}
							runIDs = map[string]bool{}
							runKey = identity
						}
						v.FailureRun.Attempts += point.Requests
						for _, s := range why.Samples {
							if s.RequestID != "" {
								runIDs[s.RequestID] = true
							}
						}
						v.FailureRun.Distinct = len(runIDs)
					} else {
						v.FailureRun, runKey, runIDs = ggFailureRun{}, "", map[string]bool{}
					}
				}
				v.Trend = append(v.Trend, point)
				v.Requests += point.Requests
				v.Success += point.Success
				v.Errors += point.Errors
				v.Pending += point.Pending
				v.KnownOther += r.Failed + r.Anomaly - point.Errors - point.Pending
				v.PeakRPM = max(v.PeakRPM, point.Requests)
				v.ErrorsComplete = v.ErrorsComplete && complete
				v.ErrorCapturedAt = max(v.ErrorCapturedAt, e.CapturedAt)
			}
		}
		a := ggAggregate(current)
		v.frtAggregate = a
		v.FRTSourceComplete = proof.frt
		v.FRT = a.view(proof.frt)
		if proof.frt {
			v.FRTThrough = end
		}
		if v.ErrorsComplete {
			v.ErrorsThrough = end
		}
		if v.FRT.Observed != nil && *v.FRT.Observed > 0 {
			n5, n10 := a.hist.ttft10k+a.hist.ttftInf, a.hist.ttftInf
			v.Over5s = &n5
			v.Over10s = &n10
			pct5, pct10 := float64(n5)*100/float64(*v.FRT.Observed), float64(n10)*100/float64(*v.FRT.Observed)
			v.Over5sPct = &pct5
			v.Over10sPct = &pct10
		}
		v.RPM = float64(v.Requests) / float64(n)
		if v.ErrorsComplete && v.Requests > 0 {
			rate := float64(v.Errors) * 100 / float64(v.Requests)
			v.FaultRate = &rate
		}
		for _, reason := range reasons {
			v.Reasons = append(v.Reasons, *reason)
		}
		sort.Slice(v.Reasons, func(i, j int) bool {
			if v.Reasons[i].Count != v.Reasons[j].Count {
				return v.Reasons[i].Count > v.Reasons[j].Count
			}
			return v.Reasons[i].Reason < v.Reasons[j].Reason
		})
		if !v.RequestsComplete {
			v.Missing = append(v.Missing, "请求分钟覆盖不完整：窗口数值仅代表已采集尝试")
		}
		if !v.ErrorsComplete {
			v.Missing = append(v.Missing, "错误归责未完整：增量采集补取 logs.content/other；已发现错误保留，故障率不计算")
		}
		if v.FRT.MeanMs == nil {
			v.Missing = append(v.Missing, ggFRTMissing(v.FRT.MeanUnavailableReason))
		}
		if !proof.frt {
			v.Missing = append(v.Missing, "FRT 分钟覆盖尚未完整")
		}
		switch {
		case !v.RequestsComplete:
			v.Status = "pending"
		case v.Requests == 0:
			v.Status = "no_requests"
		case v.FRT.Observed == nil || *v.FRT.Observed < int64(p.MinSamples):
			v.Status = "low_sample"
		case v.Pending > 0 || !v.ErrorsComplete || !proof.frt:
			v.Status = "pending"
		default:
			v.Status = "unchanged"
		}
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}

func ggValidateMetricLimit(rows []ggMetrics) error {
	if len(rows) >= modelFRTMaxLeaves {
		return fmt.Errorf("分组模型维度达到安全上限；本轮未发布部分建议")
	}
	return nil
}
