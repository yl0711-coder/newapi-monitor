package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func modelFRTMeanInsert(t *testing.T, m *Monitor, end int64, channel int, group, model string, count, ms int64) {
	t.Helper()
	modelFRTInsert(t, m, end, channel, group, model, count, ms)
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ? AND channel_id = ? AND grp = ? AND model_name = ?", end-60, channel, group, model).UpdateColumns(map[string]any{"frt_sum_ms": count * ms, "frt_sum_observed": count, "frt_sum_version": frtSumSemanticsVersion}).Error; err != nil {
		t.Fatal(err)
	}
}
func TestModelFRTMeanWeightedTreeAndStableDescendingOrder(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTMeanInsert(t, m, end, 1, "a", "fast", 99, 400)
	modelFRTMeanInsert(t, m, end, 1, "a", "slow", 1, 4000)
	modelFRTMeanInsert(t, m, end, 1, "b", "medium", 100, 4000)
	modelFRTMeanInsert(t, m, end, 2, "a", "fast", 10, 400)
	modelFRTInsert(t, m, end, 0, "unknown", "historical", 1, 400)
	for i := 0; i < 3; i++ {
		r := modelFRTBuild(t, m, "24h", "")
		if len(r.Channels) != 3 || r.Channels[0].ChannelID != 1 || r.Channels[1].ChannelID != 2 || r.Channels[2].ChannelID != 0 {
			t.Fatal(r)
		}
		c := r.Channels[0]
		g := c.Groups[1]
		if *c.FRT.MeanMs != 2218 || c.Groups[0].Group != "b" || g.Group != "a" || *g.FRT.MeanMs != 436 || g.Models[0].Model != "slow" || *g.Models[0].FRT.MeanMs != 4000 || g.Models[1].Model != "fast" {
			t.Fatal(c)
		}
		if !g.FRT.MeanCoverageComplete || *g.FRT.Observed != 100 || *g.FRT.P95Ms != computeTTFTMetricView([6]int64{99, 0, 0, 1, 0, 0}, 100, 1, 4000).P95Ms {
			t.Fatal(g)
		}
	}
	// Equal means use the existing stable identifier order, including absent values.
	modelFRTMeanInsert(t, m, end, 3, "z", "z", 1, 400)
	modelFRTMeanInsert(t, m, end, 3, "a", "b", 1, 400)
	modelFRTMeanInsert(t, m, end, 3, "a", "a", 1, 400)
	modelFRTInsert(t, m, end, 3, "a", "missing", 1, 400)
	r := modelFRTBuild(t, m, "24h", "")
	c := r.Channels[3]
	if c.ChannelID != 3 || c.Groups[0].Group != "z" || c.Groups[1].Group != "a" || c.Groups[1].Models[0].Model != "a" || c.Groups[1].Models[1].Model != "b" || c.Groups[1].Models[2].Model != "missing" {
		t.Fatal(c)
	}
}
func TestModelFRTMeanHistoricalInvalidAndIncomplete(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		updates      map[string]any
	}{
		{"missing", "historical_sum_unavailable", map[string]any{"frt_sum_ms": nil}},
		{"version", "historical_sum_unavailable", map[string]any{"frt_sum_version": 0}},
		{"future", "historical_sum_unavailable", map[string]any{"frt_sum_version": 2}},
		{"zero", "invalid_sum", map[string]any{"frt_sum_ms": 0}},
		{"negative", "invalid_sum", map[string]any{"frt_sum_ms": -1}},
		{"count", "invalid_sum", map[string]any{"frt_sum_observed": 1}},
		{"count-null", "invalid_sum", map[string]any{"frt_sum_observed": nil}},
		{"above-max", "invalid_sum", map[string]any{"frt_sum_ms": 801}},
		{"below-max", "invalid_sum", map[string]any{"frt_sum_ms": 399}},
		{"fractional", "invalid_sum", map[string]any{"frt_sum_ms": 799.5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, end := modelFRTFixture(t, true)
			modelFRTMeanInsert(t, m, end, 1, "g", "m", 2, 400)
			modelFRTMeanInsert(t, m, end, 1, "g", "known", 1, 4000)
			if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("model_name = ?", "m").UpdateColumns(tc.updates).Error; err != nil {
				t.Fatal(err)
			}
			r := modelFRTBuild(t, m, "24h", "")
			c := r.Channels[0]
			g := c.Groups[0]
			leaf := g.Models[1]
			for _, v := range []ModelFRTMetrics{c.FRT, g.FRT, leaf.FRT} {
				if v.MeanMs != nil || v.MeanCoverageComplete || v.MeanUnavailableReason != tc.reason || v.P95Ms == nil || !v.CoverageComplete {
					t.Fatal(v)
				}
			}
			if g.Models[0].Model != "known" || *g.Models[0].FRT.MeanMs != 4000 {
				t.Fatal(g)
			}
		})
	}
	m, end := modelFRTFixture(t, false)
	modelFRTMeanInsert(t, m, end, 1, "g", "m", 2, 400)
	m.cfg.CustomerHealthSourceEnabled = false
	m.cfg.CapacityEnabled = false
	r := modelFRTBuild(t, m, "24h", "")
	v := r.Channels[0].FRT
	if v.MeanMs == nil || *v.MeanMs != 400 || v.MeanCoverageComplete || r.Coverage.Complete {
		t.Fatal(r)
	}
}
func TestModelFRTMeanMissingRowsCannotCancelAndEmptyDoesNotPoison(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTMeanInsert(t, m, end, 1, "g", "m", 2, 400)
	modelFRTInsert(t, m, end-60, 1, "g", "m", 0, 0)
	r := modelFRTBuild(t, m, "24h", "")
	if v := r.Channels[0].FRT; v.MeanMs == nil || *v.MeanMs != 400 {
		t.Fatal(v)
	}
	modelFRTMeanInsert(t, m, end-120, 1, "g", "m", 2, 400)
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", end-60).UpdateColumn("frt_sum_ms", -1).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", end-180).UpdateColumn("frt_sum_ms", 1601).Error; err != nil {
		t.Fatal(err)
	}
	r = modelFRTBuild(t, m, "24h", "")
	if r.Channels[0].FRT.MeanMs != nil || r.Channels[0].FRT.MeanUnavailableReason != "invalid_sum" {
		t.Fatal(r)
	}
}
func TestModelFRTMeanMergeOverflowAndUnknown(t *testing.T) {
	for _, src := range []MetricSample{
		{TtftObserved: 1},
		{TtftObserved: 1, FRTSumMs: ptrTokenMetering(-1), FRTSumObserved: 1, FRTSumVersion: 1},
		{TtftObserved: 1, FRTSumMs: ptrTokenMetering(math.MaxInt64), FRTSumObserved: 1, FRTSumVersion: 1},
	} {
		dst := MetricSample{TtftObserved: 1, FRTSumMs: ptrTokenMetering(400), FRTSumObserved: 1, FRTSumVersion: 1}
		mergeMetricSample(&dst, src)
		mergeMetricSample(&dst, MetricSample{TtftObserved: 1, FRTSumMs: ptrTokenMetering(400), FRTSumObserved: 1, FRTSumVersion: 1})
		if dst.FRTSumMs != nil {
			t.Fatal("unknown or overflow replaced by partial total", dst)
		}
	}
	a := modelFRTAggregate{sumMs: math.MaxInt64, sumObserved: 1}
	a.merge(modelFRTAggregate{sumMs: 1, sumObserved: 1})
	if !a.meanInvalid {
		t.Fatal("tree overflow hidden")
	}
}

// Execute the actual sampler and Scan/persistence path with only MySQL DIV
// translated for the local SQLite source. No fabricated query results.
type modelFRTSQLDriver struct{ driver.Driver }
type modelFRTSQLConn struct{ driver.Conn }

func (d modelFRTSQLDriver) Open(name string) (driver.Conn, error) {
	c, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return modelFRTSQLConn{c}, nil
}
func (c modelFRTSQLConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	inner, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return inner.QueryContext(ctx, strings.ReplaceAll(q, "created_at DIV 60", "created_at / 60"), args)
}
func TestModelFRTMeanSourceProjectionPersistenceAndIdempotence(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	source := newFakeProdDB(t)
	var id int
	var name, path string
	if err := source.QueryRow("PRAGMA database_list").Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	driverName := fmt.Sprintf("frt-sql-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, modelFRTSQLDriver{source.Driver()})
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m.prodDB = db
	rows := []struct {
		user, typ, stream int
		other             string
	}{
		{1, 2, 1, `{"frt":100}`}, {1, 2, 1, `{"frt":300}`}, {2, 2, 1, `{"frt":4000}`},
		{2, 2, 0, `{"frt":9000}`}, {2, 5, 1, `{"frt":8000}`}, {2, 6, 1, `{"frt":7000}`},
		{2, 2, 1, `{"frt":0}`}, {2, 2, 1, `{"frt":-1}`}, {2, 2, 1, `{"frt":null}`},
		{2, 2, 1, `{}`}, {2, 2, 1, `{"frt":"invalid"}`}, {2, 2, 1, `not-json`},
	}
	for i, r := range rows {
		if _, err := db.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`group`,is_stream,other) VALUES(?,?,1,?,?,'m',1,1,1,1,'g',?,?)", i+1, r.user, end-60+int64(i), r.typ, r.stream, r.other); err != nil {
			t.Fatal(err)
		}
	}
	for _, capacity := range []bool{true, false, true} {
		if _, err := m.sampleRangeWithPriorityOptions(context.Background(), end-60, end, false, capacity, true); err != nil {
			t.Fatal(err)
		}
		var metric MetricSample
		if err := m.storeDB.First(&metric).Error; err != nil {
			t.Fatal(err)
		}
		if metric.FRTSumMs == nil || *metric.FRTSumMs != 4400 || metric.FRTSumObserved != 3 || metric.TtftObserved != 3 || metric.TtftOver3s != 1 || metric.FRTSumVersion != 1 {
			t.Fatal(metric)
		}
		r := modelFRTBuild(t, m, "24h", "")
		v := r.Channels[0].FRT
		if v.MeanMs == nil || math.Abs(*v.MeanMs-4400.0/3) > 1e-9 || *v.Observed != 3 {
			t.Fatal(v)
		}
		var facts []CapacityUserMinuteSample
		if err := m.storeDB.Find(&facts).Error; err != nil || len(facts) != 2 {
			t.Fatal(facts, err)
		}
		if *facts[0].FRTSumMs+*facts[1].FRTSumMs != *metric.FRTSumMs {
			t.Fatal("minute projections disagree")
		}
	}
}

func TestModelFRTMeanMigrationBackupAndHistory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "monitor.db")
	cfg := Settings{StorePath: path, StoreBackupDir: filepath.Join(root, "backups"), SessionSecret: "frt-migration-test"}
	old := &Monitor{cfg: cfg}
	if err := old.openStore(path); err != nil {
		t.Fatal(err)
	}
	for _, model := range []any{&MetricSample{}, &CapacityUserMinuteSample{}} {
		for _, col := range []string{"frt_sum_ms", "frt_sum_observed", "frt_sum_version"} {
			if err := old.storeDB.Migrator().DropColumn(model, col); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := old.storeDB.Exec("INSERT INTO capacity_user_minute_samples(bucket_ts,user_id,channel_id,model_name,grp,success,ttft_observed,ttft_500,ttft_max_ms,ttft_semantics_version) VALUES(60,1,1,'history','g',1,1,1,400,?)", ttftCoverageSemanticsVersion).Error; err != nil {
		t.Fatal(err)
	}
	old.Close()
	m := &Monitor{cfg: cfg}
	if err := m.openStore(path); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var row CapacityUserMinuteSample
	if err := m.storeDB.First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.FRTSumMs != nil || row.FRTSumVersion != 0 || row.TtftObserved != 1 || row.TTFTSemanticsVersion != ttftCoverageSemanticsVersion {
		t.Fatal(row)
	}
	snapshots := preMigrationSnapshotDirs(t, cfg.StoreBackupDir)
	if len(snapshots) != 1 {
		t.Fatal(snapshots)
	}
	saved := openReadOnlyTestStore(t, filepath.Join(snapshots[0], preMigrationMainSnapshotName))
	if sqliteHasColumn(t, saved, "capacity_user_minute_samples", "frt_sum_ms") {
		t.Fatal("rollback snapshot was taken after schema migration")
	}
	if !strings.Contains(preMigrationPlanID, "v62-frt-sum-v1") || !strings.Contains(preMigrationCombinedPlanID, "v62-frt-sum-v1") {
		t.Fatal("both plans must advance")
	}
}

func TestModelFRTMeanLegacyReadOnlySchema(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTInsert(t, m, end, 1, "g", "m", 1, 400)
	for _, col := range []string{"frt_sum_ms", "frt_sum_observed", "frt_sum_version"} {
		if err := m.storeDB.Migrator().DropColumn(&CapacityUserMinuteSample{}, col); err != nil {
			t.Fatal(err)
		}
	}
	r := modelFRTBuild(t, m, "24h", "")
	v := r.Channels[0].FRT
	if v.MeanMs != nil || v.MeanUnavailableReason != "historical_sum_unavailable" || v.P95Ms == nil || *v.Observed != 1 {
		t.Fatal(v)
	}
}
