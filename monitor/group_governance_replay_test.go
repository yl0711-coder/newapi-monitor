package monitor

// Opt-in bounded audit against an existing read-only local snapshot. It never
// starts collectors or migrates the mounted database. Optional production
// verification uses one closed minute and one read-only connection.
import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGGReadOnlyReplay(t *testing.T) {
	path, out := os.Getenv("GG_REPLAY_STORE"), os.Getenv("GG_REPLAY_OUT")
	if path == "" || out == "" {
		t.Skip("explicit read-only replay paths not supplied")
	}
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=ro"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("read-only local snapshot open failed")
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	m := &Monitor{storeDB: db, cfg: Settings{CustomerHealthSourceEnabled: true, GroupGovernanceEnabled: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	now := time.Now()
	target := customerHealthSourceFinalizedThrough(now)
	end, err := m.modelStatisticsAvailableEnd(ctx, target, false)
	if err != nil {
		t.Fatal(err)
	}
	from := end - 60*60
	minutes, err := m.modelFRTRowsGrouped(ctx, from, end, ModelFRTFilter{}, true)
	if err != nil {
		t.Fatal(err)
	}
	var channels []ChannelSnap
	if err = db.Limit(10001).Find(&channels).Error; err != nil || len(channels) > 10000 {
		t.Fatal("bounded channel snapshot read failed")
	}
	coverage := map[int]ggWindowCoverage{}
	for _, n := range []int{15, 30, 45, 60} {
		complete, _, through, _ := m.modelStatisticsFactsCoverage(ctx, end-int64(n)*60, end)
		frt, e := m.modelFRTCoverage(ctx, end-int64(n)*60, end)
		if e != nil {
			t.Fatal(e)
		}
		coverage[n] = ggWindowCoverage{complete, frt.Complete, through}
	}
	metrics := ggBuildMetrics(minutes, nil, channels, end, coverage, groupGovernancePolicy())
	// Independent whole-window queries through the existing FRT endpoint's
	// store path must equal the new minute merge for every leaf and window.
	checked := 0
	for _, n := range []int{15, 30} {
		whole, e := m.modelFRTRows(ctx, end-int64(n)*60, end, ModelFRTFilter{})
		if e != nil {
			t.Fatal(e)
		}
		by := map[ggMetricKey]ModelFRTMetrics{}
		for _, r := range whole {
			by[ggMetricKey{strings.TrimSpace(r.Row.Group), strings.TrimSpace(r.Row.Model), r.Row.ChannelID}] = ggAggregate([]modelFRTRow{r}).view(coverage[n].frt)
		}
		for _, v := range metrics {
			if v.WindowMinutes != n || v.Requests == 0 {
				continue
			}
			key := ggMetricKey{v.Group, v.Model, v.ChannelID}
			if !reflect.DeepEqual(v.FRT, by[key]) {
				t.Fatalf("FRT mismatch channel=%d model=%s", v.ChannelID, v.Model)
			}
			checked++
		}
	}
	evidence := map[string]any{"from_ts": from, "through_ts": end, "target_ts": target, "minute_rows": len(minutes), "frt_leaf_comparisons": checked, "local_input": minutes, "channels": channels}
	if dsn := os.Getenv("GG_REPLAY_DSN"); dsn != "" {
		c, e := mysql.ParseDSN(dsn)
		if e != nil || c.DBName != "nexusapi" || (c.User != "nexus_ro" && c.User != "monitor_ro") {
			t.Fatal("read-only source identity rejected")
		}
		c.Timeout = 3 * time.Second
		c.ReadTimeout = 4 * time.Second
		prod, e := sql.Open("mysql", c.FormatDSN())
		if e != nil {
			t.Fatal("source open failed")
		}
		defer prod.Close()
		prod.SetMaxOpenConns(1)
		grants, e := prod.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
		if e != nil {
			t.Fatal("source grant check unavailable")
		}
		for grants.Next() {
			var grant string
			if grants.Scan(&grant) != nil {
				t.Fatal("grant scan")
			}
			for _, s := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "ALL PRIVILEGES"} {
				if strings.Contains(strings.ToUpper(grant), s) {
					t.Fatal("source is not read only")
				}
			}
		}
		e = grants.Err()
		grants.Close()
		if e != nil {
			t.Fatal("source grant check incomplete")
		}
		m.prodDB = prod
		options, e := m.fetchGroupGovernanceOptions(ctx)
		if e != nil {
			evidence["configuration_access"] = "options read unavailable"
		} else {
			evidence["configuration_access"] = "seven configured option keys readable"
			evidence["configuration_groups"] = len(options.ConfigSources)
		}
		// One minute behind the report window reduces normal delayed-log races.
		auditFrom, auditTo := end-300, end-240
		query := sampleCustomerHealthWindowSQL()
		plan, e := prod.QueryContext(ctx, "EXPLAIN "+query, auditFrom, auditTo)
		if e != nil {
			t.Fatal("source plan unavailable")
		}
		columns, _ := plan.Columns()
		plans := []map[string]string{}
		for plan.Next() {
			values := make([]sql.NullString, len(columns))
			ptrs := make([]any, len(values))
			for i := range ptrs {
				ptrs[i] = &values[i]
			}
			if plan.Scan(ptrs...) != nil {
				t.Fatal("plan decode")
			}
			r := map[string]string{}
			for i, k := range columns {
				r[k] = values[i].String
			}
			if r["type"] != "range" && r["type"] != "ref" {
				t.Fatal("source plan not bounded")
			}
			plans = append(plans, r)
		}
		e = plan.Err()
		plan.Close()
		if e != nil || len(plans) == 0 {
			t.Fatal("source plan incomplete")
		}
		evidence["source_plan"] = plans
		rows, e := prod.QueryContext(ctx, query, auditFrom, auditTo)
		if e != nil {
			t.Fatal("bounded source aggregate unavailable")
		}
		var batch []CapacityUserMinuteSample
		for rows.Next() {
			var r CapacityUserMinuteSample
			var username, grp sql.NullString
			if e = rows.Scan(&r.BucketTs, &r.ChannelID, &r.ModelName, &grp, &r.UserID, &username, &r.Success, &r.Anomaly, &r.Failed, &r.CustomerHealthAnomaly, &r.CustomerHealthFailed, &r.Tokens, &r.Quota, &r.RefundQuota, &r.Ttft500, &r.Ttft1k, &r.Ttft2k, &r.Ttft5k, &r.Ttft10k, &r.TtftInf, &r.TtftMaxMs, &r.TtftObserved, &r.TtftOver3s, &r.FRTSumMs); e != nil {
				t.Fatal("source aggregate decode failed")
			}
			r.Grp = grp.String
			batch = append(batch, r)
			if len(batch) > 6000 {
				t.Fatal("source aggregate cap")
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal("source aggregate incomplete")
		}
		projection := newGGErrorProjection(batch)
		if e = classifyCustomerHealthErrors(ctx, prod, auditFrom, auditTo, batch, projection.observe); e != nil {
			t.Fatal("bounded error classification unavailable")
		}
		projection.collectDelivery(ctx, prod, auditFrom, auditTo)
		var sourceTotal, sourceFailed, sourceAnomaly, sourceSum, sourceObserved int64
		spend := map[int64]int64{}
		for _, r := range batch {
			sourceTotal += r.Success + r.Anomaly + r.Failed
			sourceFailed += r.Failed
			sourceAnomaly += r.Anomaly
			if r.FRTSumMs == nil {
				t.Fatal("source exact FRT sum absent")
			}
			sourceSum += *r.FRTSumMs
			sourceObserved += r.TtftObserved
			spend[r.UserID] += r.Quota - r.RefundQuota
		}
		var localTotal, localSum, localObserved int64
		for _, r := range minutes {
			if r.BucketTs >= auditFrom && r.BucketTs < auditTo {
				localTotal += r.Success + r.Anomaly + r.Failed
				localSum += r.FRTSumMs
				localObserved += r.Row.TtftObserved
			}
		}
		evidence["source_window"] = map[string]any{"from_ts": auditFrom, "to_ts": auditTo, "source_attempts": sourceTotal, "local_attempts": localTotal, "source_failed": sourceFailed, "source_anomaly": sourceAnomaly, "attempt_difference": sourceTotal - localTotal}
		evidence["source_frt"] = map[string]any{"source_sum_ms": sourceSum, "local_sum_ms": localSum, "source_observed": sourceObserved, "local_observed": localObserved}
		if sourceTotal != localTotal || sourceSum != localSum || sourceObserved != localObserved {
			t.Error("source/local minute request or exact FRT totals differ; raw evidence retained")
		}
		user := int64(0)
		for id, net := range spend {
			if net > spend[user] || net == spend[user] && id < user {
				user = id
			}
		}
		if user > 0 {
			m.cfg.UsageFactsEnabled = true
			m.cfg.UsageFactsReadEnabled = true
			actual := ggReadCustomerMinute(t, ctx, m, user, auditFrom, auditTo)
			evidence["source_customer_amount"] = map[string]any{"user_id": user, "from_ts": auditFrom, "to_ts": auditTo, "source_net_quota": spend[user], "local_usd": actual, "usage_enabled": true}
			if actual == nil || math.Abs(*actual-float64(spend[user])/quotaPerUSD) > 1e-10 {
				t.Error("same-customer source net amount differs; evidence retained")
			}
		}
		var reasons []ggErrorReason
		for _, rs := range projection.reasons {
			for _, r := range rs {
				reasons = append(reasons, *r)
			}
		}
		evidence["source_reasons"] = reasons
	}
	r := ggRealtimeReport{Version: groupGovernancePolicy().Version, Policy: groupGovernancePolicy(), GeneratedAt: now.Unix(), Through: end, Target: target, Status: "replay", Metrics: metrics, Actions: []ggAction{}}
	ggRecommend(&r, &ggControl{}, "real-replay", groupGovernancePolicy())
	evidence["report"] = r
	b, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(out, b, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("real local replay: %d minute rows, %d FRT leaf comparisons, bounded to 60 minutes", len(minutes), checked)
}

// The production amount API has a day-prefix contract. Compare two proven
// prefixes to audit a closed minute; never pretend a minute starts a new day.
func ggReadCustomerMinute(t *testing.T, ctx context.Context, m *Monitor, user, from, to int64) *float64 {
	t.Helper()
	var proof CustomerHealthSourceCursor
	if err := m.storeDB.WithContext(ctx).First(&proof, 1).Error; err != nil || proof.DayTs > from || proof.ThroughTs < to {
		t.Fatal("customer day coverage does not contain audit window", err)
	}
	read := func(end int64) *float64 {
		rows := []CustomerHealthRow{{Members: []CustomerHealthMember{{UserID: user}}}}
		m.attachCustomerHealthCollectedSpendBatch(ctx, rows, []int64{user}, proof.DayTs, end)
		if rows[0].SpendThroughTs != end {
			t.Error("customer amount prefix has wrong cutoff", rows[0].SpendThroughTs, end, rows[0].SpendNote)
		}
		return rows[0].TodaySpendUSD
	}
	before, after := read(from), read(to)
	if before == nil || after == nil {
		return nil
	}
	return ggFloat(*after - *before)
}

func TestGGRecordedSourceCustomerMinute(t *testing.T) {
	store, input := os.Getenv("GG_REPLAY_STORE"), os.Getenv("GG_RETAINED_SOURCE")
	if store == "" || input == "" {
		t.Skip("requires retained source minute and read-only local facts")
	}
	var record struct {
		Amount struct {
			User  int64 `json:"user_id"`
			From  int64 `json:"from_ts"`
			To    int64 `json:"to_ts"`
			Quota int64 `json:"source_net_quota"`
		} `json:"source_customer_amount"`
	}
	data, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	a := record.Amount
	if a.User <= 0 || a.To-a.From != 60 {
		t.Fatal("invalid retained minute")
	}
	db, err := gorm.Open(sqlite.Open("file:"+store+"?mode=ro"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	m := &Monitor{storeDB: db, cfg: Settings{CustomerHealthSourceEnabled: true, UsageFactsEnabled: true, UsageFactsReadEnabled: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actual := ggReadCustomerMinute(t, ctx, m, a.User, a.From, a.To)
	if actual == nil || math.Abs(*actual-float64(a.Quota)/quotaPerUSD) > 1e-10 {
		t.Fatal("retained source amount differs", actual)
	}
	t.Logf("retained minute [%d,%d): source net quota=%d independent amount=%.8f; Usage enabled; no additional source query", a.From, a.To, a.Quota, *actual)
}

func TestGGRecordedPopulationPublicationBudget(t *testing.T) {
	input := os.Getenv("GG_RECORDED_INPUT")
	if input == "" {
		t.Skip("recorded production input not supplied")
	}
	var fixture struct {
		Through  int64         `json:"through_ts"`
		Rows     []modelFRTRow `json:"local_input"`
		Channels []ChannelSnap `json:"channels"`
	}
	b, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Rows) == 0 || fixture.Through == 0 {
		t.Fatal("invalid recorded population")
	}
	now := time.Unix(fixture.Through+120, 0)
	m := modelStatisticsWindowFixture(t, true, now, fixture.Through, fixture.Through, fixture.Through)
	m.cfg.GroupGovernanceEnabled = true
	var samples []CapacityUserMinuteSample
	for _, r := range fixture.Rows {
		s := CapacityUserMinuteSample{BucketTs: r.BucketTs, UserID: 1, ChannelID: r.Row.ChannelID, Grp: r.Row.Group, ModelName: r.Row.Model, Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed, TtftObserved: r.Row.TtftObserved, TtftMaxMs: r.Row.TtftMaxMs, Ttft500: r.Row.Ttft500, Ttft1k: r.Row.Ttft1k, Ttft2k: r.Row.Ttft2k, Ttft5k: r.Row.Ttft5k, Ttft10k: r.Row.Ttft10k, TtftInf: r.Row.TtftInf, TtftOver3s: r.Row.TtftOver3s, FRTSumMs: &r.FRTSumMs, FRTSumObserved: r.FRTSumObserved, FRTSumVersion: frtSumSemanticsVersion}
		samples = append(samples, s)
	}
	if err = m.storeDB.CreateInBatches(samples, 100).Error; err != nil {
		t.Fatal(err)
	}
	groupNames := map[string]bool{}
	for i := range fixture.Channels {
		c := &fixture.Channels[i]
		c.UpdatedAt = now.Unix()
		for _, g := range splitList(c.Groups) {
			groupNames[g] = true
		}
	}
	if err = m.storeDB.CreateInBatches(fixture.Channels, 100).Error; err != nil {
		t.Fatal(err)
	}
	for g := range groupNames {
		m.storeDB.Create(&GroupGovernanceGroup{Grp: g, Current: true})
	}
	m.storeDB.Create(&GroupGovernanceState{ID: 1, LastSuccessAt: now.Unix()})
	start := time.Now()
	if err = m.syncGroupGovernanceRealtime(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var publication GroupGovernanceRealtimeState
	if err = m.storeDB.First(&publication, 1).Error; err != nil {
		t.Fatal(err)
	}
	var control ggControl
	if err = json.Unmarshal([]byte(publication.ControlJSON), &control); err != nil {
		t.Fatal(err)
	}
	if len(publication.ReportJSON)+len(publication.ControlJSON) > 16<<20 {
		t.Fatal("unbounded publication")
	}
	if out := os.Getenv("GG_RECORDED_OUT"); out != "" {
		if err = os.WriteFile(out, []byte(publication.ReportJSON), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("recorded rows=%d channels=%d report_bytes=%d control_bytes=%d active_histories=%d elapsed_ms=%d", len(samples), len(fixture.Channels), len(publication.ReportJSON), len(publication.ControlJSON), len(control.Metrics), time.Since(start).Milliseconds())
}
