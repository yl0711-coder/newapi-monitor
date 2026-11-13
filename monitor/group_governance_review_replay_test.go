package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// The current local minute tables, error projections and durable histories are
// read in one snapshot. No migration, write, collector or production connection.
func TestGGReviewReadOnlyReport(t *testing.T) {
	file, out := os.Getenv("GG_REVIEW_STORE"), os.Getenv("GG_REVIEW_OUT")
	if file == "" || out == "" {
		t.Skip("explicit read-only replay not requested")
	}
	db, err := gorm.Open(sqlite.Open("file:"+file+"?mode=ro"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), groupGovernanceReadBudget)
	defer cancel()
	var envelope groupGovernanceReport
	var candidate ggRealtimeReport
	var control ggControl
	var audit []ggChannelAudit
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		m := &Monitor{storeDB: tx, cfg: Settings{GroupGovernanceEnabled: true, LogChainOnlySource: true, CustomerHealthSourceEnabled: true, SampleSeconds: 60, GroupGovernanceSyncMinutes: 10}}
		var e error
		envelope, e = m.loadGroupGovernanceReport(ctx)
		if e != nil {
			return e
		}
		var prior GroupGovernanceRealtimeState
		if e = tx.Where("id=1").First(&prior).Error; e != nil {
			return e
		}
		if e = json.Unmarshal([]byte(prior.ControlJSON), &control); e != nil {
			return e
		}
		candidate, e = m.readGroupGovernanceMetrics(ctx, time.Now())
		if e != nil {
			return e
		}
		m.applyGroupGovernanceConfig(&candidate, envelope.State, time.Now())
		groups := make([]GroupGovernanceGroup, 0, len(envelope.Groups))
		for _, g := range envelope.Groups {
			groups = append(groups, g.GroupGovernanceGroup)
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i].Grp < groups[j].Grp })
		ggRecommend(&candidate, &control, ggConfigurationKey(groups), groupGovernancePolicy())
		candidate.AdviceReady = candidate.Status == "ready" && candidate.ConfigReady && candidate.RequestsComplete
		if os.Getenv("GG_REVIEW_AUDIT") != "" {
			audit, e = ggAuditChannelFacts(tx, candidate)
			if e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope.Realtime = &candidate
	b, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := json.Marshal(control)
	if len(b)+len(c) > 16<<20 {
		t.Fatalf("publication exceeded bound: %d", len(b)+len(c))
	}
	if err = os.WriteFile(out, b, 0600); err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("GG_REVIEW_AUDIT"); output != "" {
		b, _ := json.MarshalIndent(audit, "", "  ")
		if err := os.WriteFile(output, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("local replay leaves=%d actions=%d ready=%t report_bytes=%d control_bytes=%d", len(candidate.Metrics), len(candidate.Actions), candidate.AdviceReady, len(b), len(c))
	t.Logf("independent same-snapshot channel checks=%d", len(audit))
}

type ggChannelAudit struct {
	Group                                                        string `json:"group"`
	ChannelID                                                    int    `json:"channel_id"`
	From, To                                                     int64
	Requests, Success, PeakRPM, SumMs, Observed, Errors, Pending int64
	MeanMs, FaultRate                                            *float64
	ErrorsComplete                                               bool
}

// Two bounded whole-window queries independently aggregate raw local facts by
// group/channel/minute, without reusing the candidate's model merge. The same
// read transaction keeps late updates from changing either side of the audit.
func ggAuditChannelFacts(db *gorm.DB, report ggRealtimeReport) ([]ggChannelAudit, error) {
	var result []ggChannelAudit
	seen := map[ggMetricKey]bool{}
	for _, n := range []int{15, 30} {
		from := report.Through - int64(n)*60
		var minutes []struct {
			Grp                                          string
			ChannelID                                    int
			BucketTs, Attempts, Success, SumMs, Observed int64
		}
		if err := db.Raw(`SELECT TRIM(grp) AS grp,channel_id,bucket_ts,SUM(success+anomaly+failed) AS attempts,SUM(success) AS success,SUM(COALESCE(frt_sum_ms,0)) AS sum_ms,SUM(ttft_observed) AS observed FROM capacity_user_minute_samples WHERE bucket_ts>=? AND bucket_ts<? GROUP BY TRIM(grp),channel_id,bucket_ts LIMIT 60001`, from, report.Through).Scan(&minutes).Error; err != nil {
			return nil, err
		}
		if len(minutes) > 60000 {
			return nil, fmt.Errorf("audit minute bound")
		}
		totals := map[ggMetricKey]ggChannelAudit{}
		for _, r := range minutes {
			k := ggMetricKey{Group: r.Grp, Channel: r.ChannelID}
			a := totals[k]
			a.Group, a.ChannelID, a.From, a.To = r.Grp, r.ChannelID, from, report.Through
			a.Requests += r.Attempts
			a.Success += r.Success
			a.SumMs += r.SumMs
			a.Observed += r.Observed
			a.PeakRPM = max(a.PeakRPM, r.Attempts)
			totals[k] = a
		}
		var errors []GroupGovernanceErrorMinute
		if err := db.Where("bucket_ts>=? AND bucket_ts<? AND version=?", from, report.Through, groupGovernanceErrorVersion).Limit(60001).Find(&errors).Error; err != nil {
			return nil, err
		}
		if len(errors) > 60000 {
			return nil, fmt.Errorf("audit error bound")
		}
		for _, r := range errors {
			k := ggMetricKey{Group: strings.TrimSpace(r.Grp), Channel: r.ChannelID}
			a := totals[k]
			var reasons []ggErrorReason
			if json.Unmarshal([]byte(r.ReasonsJSON), &reasons) != nil {
				continue
			}
			for _, why := range reasons {
				if why.Attributed {
					a.Errors += why.Count
				}
				if why.Pending {
					a.Pending += why.Count
				}
			}
			totals[k] = a
		}
		for _, c := range report.Channels {
			if c.WindowMinutes != n {
				continue
			}
			k := ggMetricKey{Group: c.Group, Channel: c.ChannelID}
			if seen[k] {
				return nil, fmt.Errorf("duplicate channel %v", k)
			}
			seen[k] = true
			a := totals[k]
			a.Group, a.ChannelID, a.From, a.To = c.Group, c.ChannelID, c.From, c.To
			a.ErrorsComplete = c.ErrorsComplete
			if a.Requests != c.Requests || a.Success != c.Success || a.PeakRPM != c.PeakRPM {
				return nil, fmt.Errorf("channel totals differ %v: facts=%+v candidate requests=%d success=%d peak=%d", k, a, c.Requests, c.Success, c.PeakRPM)
			}
			if c.FRT.MeanMs != nil {
				if a.Observed <= 0 {
					return nil, fmt.Errorf("no raw samples %v", k)
				}
				a.MeanMs = ggFloat(float64(a.SumMs) / float64(a.Observed))
				if math.Abs(*a.MeanMs-*c.FRT.MeanMs) > 1e-8 {
					return nil, fmt.Errorf("weighted mean differs %v", k)
				}
			}
			if c.ErrorsComplete && (a.Errors != c.Errors || a.Pending != c.Pending) {
				return nil, fmt.Errorf("channel error counts differ %v", k)
			}
			if c.FaultRate != nil {
				a.FaultRate = ggFloat(float64(a.Errors) * 100 / float64(a.Requests))
				if math.Abs(*a.FaultRate-*c.FaultRate) > 1e-8 {
					return nil, fmt.Errorf("channel fault rate differs %v", k)
				}
			}
			result = append(result, a)
		}
	}
	if len(result) != len(report.Channels) {
		return nil, fmt.Errorf("audit missed selected channel windows: %d/%d", len(result), len(report.Channels))
	}
	return result, nil
}
