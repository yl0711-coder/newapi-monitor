package monitor

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestOwnedPathsRetainedReports(t *testing.T) {
	file, out := os.Getenv("OWNED_PATHS_STORE"), os.Getenv("OWNED_PATHS_OUT")
	if file == "" || out == "" {
		t.Skip("requires explicit retained read-only store")
	}
	db, err := gorm.Open(sqlite.Open("file:"+file+"?mode=ro"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	m := &Monitor{storeDB: db, cfg: Settings{CapacityEnabled: true, RetentionDays: 7, SampleSeconds: 60}}
	now := time.Now()
	to := metricFinalizeTarget(now.Unix())
	from := to - 86400
	selection, err := modelReportSelectionFor(url.Values{"from": {time.Unix(from, 0).UTC().Format(time.RFC3339)}, "to": {time.Unix(to, 0).UTC().Format(time.RFC3339)}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	report, err := m.buildModelStatisticsSelected(ctx, selection, now)
	if err != nil {
		t.Fatal(err)
	}
	optimizedMS := time.Since(start).Milliseconds()
	start = time.Now()
	prior, err := m.buildModelStatisticsSelectedRead(ctx, selection, now)
	if err != nil {
		t.Fatal(err)
	}
	priorMS := time.Since(start).Milliseconds()
	if !reflect.DeepEqual(report.Models, prior.Models) || !reflect.DeepEqual(report.Source, prior.Source) {
		t.Fatal("report changed after query reuse")
	}
	start = time.Now()
	frt, err := m.buildModelFRTSelected(ctx, selection, ModelFRTFilter{}, now)
	if err != nil {
		t.Fatal(err)
	}
	frtMS := time.Since(start).Milliseconds()
	var direct struct{ Requests, SumMs, Observed int64 }
	if err = db.Raw(`SELECT SUM(success+anomaly+failed) AS requests,SUM(COALESCE(frt_sum_ms,0)) AS sum_ms,SUM(frt_sum_observed) AS observed FROM capacity_user_minute_samples WHERE bucket_ts>=? AND bucket_ts<? AND traffic_class_version=?`, from, to, stabilityTrafficClassificationVersion).Scan(&direct).Error; err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, model := range report.Models {
		for _, g := range model.Groups {
			for _, c := range g.Channels {
				if !c.IsUnavailable {
					total += c.Requests
				}
			}
		}
	}
	if total != direct.Requests {
		t.Fatalf("window counts differ report=%d raw=%d", total, direct.Requests)
	}
	data, _ := json.MarshalIndent(map[string]any{"from": from, "to": to, "source": "retained local store, readonly; production mode configuration", "model_ms": optimizedMS, "uncached_model_ms": priorMS, "frt_ms": frtMS, "models": len(report.Models), "channels": len(frt.Channels), "raw_totals": direct, "requests_match": true, "report_content_equal": true}, "", "  ")
	if err = os.WriteFile(out, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
}

func TestOwnedPathsRetainedCustomerAmount(t *testing.T) {
	root := os.Getenv("OWNED_PATHS_SOURCE_EVIDENCE")
	if root == "" {
		t.Skip("requires retained production log pages")
	}
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = true
	const from = int64(1794326400)
	const to = int64(1794362580)
	seen := map[int64]bool{}
	byMinute := map[int64]*CapacityUserMinuteSample{}
	var net int64
	for _, name := range []string{"online-customer-source-sample-41.json", "online-customer-source-sample-41-page2.json"} {
		var page struct {
			Rows []LogChainRow `json:"rows"`
		}
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(b, &page); err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Rows {
			if seen[r.ID] {
				t.Fatal("duplicate source identity")
			}
			seen[r.ID] = true
			if r.CreatedAt < from || r.CreatedAt >= to {
				t.Fatal("source outside fixed window")
			}
			if r.UserID != 151 || r.Type != 2 {
				t.Fatal("unexpected source customer/type")
			}
			quota := int64(math.Round(r.CostUSD * quotaPerUSD))
			net += quota
			bucket := r.CreatedAt / 60 * 60
			if byMinute[bucket] == nil {
				byMinute[bucket] = &CapacityUserMinuteSample{BucketTs: bucket, UserID: 151, ChannelID: 1, ModelName: "retained", Grp: "retained", TrafficClassVersion: stabilityTrafficClassificationVersion}
			}
			byMinute[bucket].Quota += quota
			byMinute[bucket].Success++
		}
	}
	var refunds struct {
		Rows    []LogChainRow `json:"rows"`
		HasMore bool          `json:"has_more"`
	}
	b, err := os.ReadFile(filepath.Join(root, "online-customer-source-sample-41-refunds.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &refunds); err != nil {
		t.Fatal(err)
	}
	if len(refunds.Rows) != 0 || refunds.HasMore {
		t.Fatal("fixture refund proof changed")
	}
	for _, r := range byMinute {
		if err = m.storeDB.Create(r).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err = m.saveCustomerHealthSourceCursor(from, to); err != nil {
		t.Fatal(err)
	}
	rows := []CustomerHealthRow{{Members: []CustomerHealthMember{{UserID: 151}}}}
	m.attachCustomerHealthCollectedSpendBatch(context.Background(), rows, []int64{151}, from, to)
	if len(seen) != 124 || rows[0].TodaySpendUSD == nil || math.Abs(*rows[0].TodaySpendUSD-float64(net)/quotaPerUSD) > 1e-12 {
		t.Fatal("retained source amount mismatch", len(seen), net, rows)
	}
	t.Logf("retained production rows=%d source_net_quota=%d amount=%.6f through=%d; Usage enabled", len(seen), net, *rows[0].TodaySpendUSD, rows[0].SpendThroughTs)
}
