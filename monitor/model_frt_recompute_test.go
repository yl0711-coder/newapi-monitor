package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// 建一个按 SQLite 可执行的假生产库，仅翻译 MySQL DIV；不伪造查询结果。
func frtRecomputeProdDB(t *testing.T) *sql.DB {
	t.Helper()
	source := newFakeProdDB(t)
	var id int
	var name, path string
	if err := source.QueryRow("PRAGMA database_list").Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	driverName := fmt.Sprintf("frt-recompute-sql-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, modelFRTSQLDriver{source.Driver()})
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// frtSourceQueryFailures lets retry tests fail the actual production query
// after the sampler has built the local target rows. The wrapper still runs
// the real SQL and Scan path on every successful attempt.
type frtSourceQueryFailures struct {
	remaining   atomic.Int64
	always      atomic.Bool
	calls       atomic.Int64
	beforeQuery func()
}

func (f *frtSourceQueryFailures) failQuery() bool {
	f.calls.Add(1)
	if f.always.Load() {
		return true
	}
	for {
		n := f.remaining.Load()
		if n <= 0 {
			return false
		}
		if f.remaining.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

type frtFailingSQLDriver struct {
	driver.Driver
	failures *frtSourceQueryFailures
}

type frtFailingSQLConn struct {
	modelFRTSQLConn
	failures *frtSourceQueryFailures
}

func (d frtFailingSQLDriver) Open(name string) (driver.Conn, error) {
	c, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return frtFailingSQLConn{modelFRTSQLConn{c}, d.failures}, nil
}

func (c frtFailingSQLConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(strings.ToUpper(q), "FROM LOGS") && c.failures != nil {
		if c.failures.beforeQuery != nil {
			c.failures.beforeQuery()
		}
		if c.failures.failQuery() {
			return nil, errors.New("connection refused: injected FRT source failure")
		}
	}
	return c.modelFRTSQLConn.QueryContext(ctx, q, args)
}

func frtRecomputeProdDBWithFailures(t *testing.T, failures *frtSourceQueryFailures) *sql.DB {
	t.Helper()
	source := newFakeProdDB(t)
	var id int
	var name, path string
	if err := source.QueryRow("PRAGMA database_list").Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	driverName := fmt.Sprintf("frt-recompute-failing-sql-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, frtFailingSQLDriver{Driver: modelFRTSQLDriver{source.Driver()}, failures: failures})
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func frtRecomputeRetryWithoutDelay(t *testing.T) {
	t.Helper()
	previous := frtRecomputeRetryWait
	frtRecomputeRetryWait = func(context.Context, time.Duration) bool { return true }
	t.Cleanup(func() { frtRecomputeRetryWait = previous })
}

// insertLog 写一条生产日志。other 里的 frt 是唯一的 FRT 来源。
func frtRecomputeInsertLog(t *testing.T, db *sql.DB, id int, at int64, user, typ, stream, channel int, group, model, other string, useTime int) {
	t.Helper()
	if _, err := db.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`group`,is_stream,other) VALUES(?,?,?,?,?,?,1,1,1,?,?,?,?)",
		id, user, channel, at, typ, model, useTime, group, stream, other); err != nil {
		t.Fatal(err)
	}
}

// 回算必须用真实 FRT 总和与有效样本数填平既有行，且与采集结果一致。
func TestFRTSumRecomputeFillsExistingRowsWithRealSums(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	db := frtRecomputeProdDB(t)
	m.prodDB = db
	bucket := end - 60

	// 两个用户、同渠道同分组同模型：用户 1 两条有效 FRT(100,300)，用户 2 一条(4000)。
	// 其余各类无效样本都不得进入总和或样本数。
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	frtRecomputeInsertLog(t, db, 2, bucket+1, 1, 2, 1, 1, "g", "m", `{"frt":300}`, 9)
	frtRecomputeInsertLog(t, db, 3, bucket+2, 2, 2, 1, 1, "g", "m", `{"frt":4000}`, 11)
	frtRecomputeInsertLog(t, db, 4, bucket+3, 2, 2, 0, 1, "g", "m", `{"frt":9000}`, 13) // 非流式
	frtRecomputeInsertLog(t, db, 5, bucket+4, 2, 5, 1, 1, "g", "m", `{"frt":8000}`, 15) // 失败
	frtRecomputeInsertLog(t, db, 6, bucket+5, 2, 2, 1, 1, "g", "m", `{"frt":0}`, 17)    // FRT=0
	frtRecomputeInsertLog(t, db, 7, bucket+6, 2, 2, 1, 1, "g", "m", `not-json`, 19)     // 非法 JSON

	// 先用真实采集建立分钟行与直方图，但抹掉三个总和列，模拟"列后加、水位已过"。
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("1 = 1").
		UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_observed": 0, "frt_sum_version": 0}).Error; err != nil {
		t.Fatal(err)
	}
	before := modelFRTBuild(t, m, "24h", "")
	if before.Channels[0].FRT.MeanMs != nil || before.Channels[0].FRT.MeanUnavailableReason != "historical_sum_unavailable" {
		t.Fatalf("前置条件：抹掉总和后应不可计算：%+v", before.Channels[0].FRT)
	}
	// 请求数与直方图必须仍然完好，否则后面证明不了"回算没破坏它们"。
	if *before.Channels[0].FRT.Observed != 3 {
		t.Fatalf("前置条件：有效 FRT 样本数应为 3，实为 %v", before.Channels[0].FRT.Observed)
	}

	state, err := m.RecomputeFRTSum(context.Background(), bucket, end)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.RowsUpdated != 2 || state.FailedSlices != 0 {
		t.Fatalf("回算状态异常：%+v", state)
	}

	// 真实总和 100+300+4000=4400，有效样本 3，均值 1466.67。
	after := modelFRTBuild(t, m, "24h", "")
	v := after.Channels[0].FRT
	if v.MeanMs == nil {
		t.Fatalf("回算后应可计算：%q", v.MeanUnavailableReason)
	}
	if math.Abs(*v.MeanMs-4400.0/3) > 1e-9 {
		t.Fatalf("均值必须是真实总和 ÷ 有效样本数 = 4400/3，实为 %v", *v.MeanMs)
	}
	if *v.Observed != 3 {
		t.Fatalf("样本数不得改变：%v", *v.Observed)
	}

	// 三级结果一致：渠道、分组、模型均值相同（同一组数据只有一条路径）。
	g := after.Channels[0].Groups[0]
	if g.FRT.MeanMs == nil || *g.FRT.MeanMs != *v.MeanMs {
		t.Fatalf("分组层均值应与渠道一致：%+v", g.FRT.MeanMs)
	}
	if g.Models[0].FRT.MeanMs == nil || *g.Models[0].FRT.MeanMs != *v.MeanMs {
		t.Fatalf("模型层均值应与渠道一致：%+v", g.Models[0].FRT.MeanMs)
	}
}

// 回算只动三列，不得破坏请求数、Token、金额与客户维护统计。
func TestFRTSumRecomputePreservesRequestsTokensQuotaAndHealth(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	db := frtRecomputeProdDB(t)
	m.prodDB = db
	bucket := end - 60
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	frtRecomputeInsertLog(t, db, 2, bucket+1, 1, 2, 1, 1, "g", "m", `{"frt":300}`, 9)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	var before CapacityUserMinuteSample
	if err := m.storeDB.First(&before).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("1 = 1").
		UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_observed": 0, "frt_sum_version": 0}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.RecomputeFRTSum(context.Background(), bucket, end); err != nil {
		t.Fatal(err)
	}
	var after CapacityUserMinuteSample
	if err := m.storeDB.First(&after).Error; err != nil {
		t.Fatal(err)
	}
	// 逐项核对非 FRT 总和字段：任何一项变化都说明回算越界了。
	for name, pair := range map[string][2]int64{
		"success":                 {before.Success, after.Success},
		"anomaly":                 {before.Anomaly, after.Anomaly},
		"failed":                  {before.Failed, after.Failed},
		"tokens":                  {before.Tokens, after.Tokens},
		"quota":                   {before.Quota, after.Quota},
		"refund_quota":            {before.RefundQuota, after.RefundQuota},
		"customer_health_anomaly": {before.CustomerHealthAnomaly, after.CustomerHealthAnomaly},
		"customer_health_failed":  {before.CustomerHealthFailed, after.CustomerHealthFailed},
		"ttft_observed":           {before.TtftObserved, after.TtftObserved},
		"ttft_over_3s":            {before.TtftOver3s, after.TtftOver3s},
		"ttft_500":                {before.Ttft500, after.Ttft500},
		"ttft_5k":                 {before.Ttft5k, after.Ttft5k},
	} {
		if pair[0] != pair[1] {
			t.Fatalf("%s 不得被回算改动：%d -> %d", name, pair[0], pair[1])
		}
	}
	if before.Username != after.Username || before.Grp != after.Grp || before.ModelName != after.ModelName {
		t.Fatalf("维度字段不得改变：%+v -> %+v", before, after)
	}
	if before.TTFTSemanticsVersion != after.TTFTSemanticsVersion || before.CustomerHealthVersion != after.CustomerHealthVersion {
		t.Fatal("语义版本不得被回算改动")
	}
	if after.FRTSumMs == nil || *after.FRTSumMs != 400 || after.FRTSumObserved != 2 || after.FRTSumVersion != frtSumSemanticsVersion {
		t.Fatalf("三个总和列应被填平：%v %d %d", after.FRTSumMs, after.FRTSumObserved, after.FRTSumVersion)
	}
}

// 幂等与重启恢复：重复执行结果不变，不累计；中断后可从水位续跑。
func TestFRTSumRecomputeIdempotentAndResumable(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	db := frtRecomputeProdDB(t)
	m.prodDB = db
	bucket := end - 60
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	frtRecomputeInsertLog(t, db, 2, bucket+1, 1, 2, 1, 1, "g", "m", `{"frt":300}`, 9)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("1 = 1").
		UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_observed": 0, "frt_sum_version": 0}).Error; err != nil {
		t.Fatal(err)
	}
	// 连跑三次：总和必须恒为 400，不得叠加成 800/1200。
	for i := 0; i < 3; i++ {
		if _, err := m.RecomputeFRTSum(context.Background(), bucket, end); err != nil {
			t.Fatal(err)
		}
		var row CapacityUserMinuteSample
		if err := m.storeDB.First(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.FRTSumMs == nil || *row.FRTSumMs != 400 || row.FRTSumObserved != 2 {
			t.Fatalf("第 %d 次执行后总和应恒定为 400/2，实为 %v/%d", i+1, row.FRTSumMs, row.FRTSumObserved)
		}
	}
	// 重启恢复：把水位改回中途，再次执行应从该处继续并正常完成。
	var state FRTSumRecomputeState
	if err := m.storeDB.Where("id = ?", 1).First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.DoneThroughTs != end {
		t.Fatalf("完成后水位应到窗口右端：%d != %d", state.DoneThroughTs, end)
	}
	if err := m.storeDB.Model(&FRTSumRecomputeState{}).Where("id = ?", 1).
		UpdateColumn("done_through_ts", bucket).Error; err != nil {
		t.Fatal(err)
	}
	resumed, err := m.RecomputeFRTSum(context.Background(), bucket, end)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != "complete" || resumed.DoneThroughTs != end {
		t.Fatalf("续跑应完成到右端：%+v", resumed)
	}
}

// 无法回算的行必须保持"不可计算"，不得补零；也不得插入缺请求事实的新行。
func TestFRTSumRecomputeNeverZeroFillsOrInsertsRows(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	db := frtRecomputeProdDB(t)
	m.prodDB = db
	bucket := end - 60

	// 日志里有有效 FRT，但本地没有对应分钟行（请求事实缺失）。
	frtRecomputeInsertLog(t, db, 1, bucket, 9, 2, 1, 9, "ghost", "ghost-model", `{"frt":500}`, 7)
	var countBefore int64
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Count(&countBefore).Error; err != nil {
		t.Fatal(err)
	}
	state, err := m.RecomputeFRTSum(context.Background(), bucket, end)
	if err != nil {
		t.Fatal(err)
	}
	var countAfter int64
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Count(&countAfter).Error; err != nil {
		t.Fatal(err)
	}
	if countAfter != countBefore {
		t.Fatalf("不得插入新行：%d -> %d", countBefore, countAfter)
	}
	if state.RowsMissing != 1 {
		t.Fatalf("缺失的请求事实必须单独计数上报：%+v", state)
	}
	if state.RowsUpdated != 0 {
		t.Fatalf("没有可更新的行：%+v", state)
	}

	// 另一种不可回算：直方图样本数与重算样本数不一致（口径变更过）。
	frtRecomputeInsertLog(t, db, 2, bucket+1, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("channel_id = ?", 1).
		UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_observed": 0, "frt_sum_version": 0,
			"ttft_observed": 5}).Error; err != nil {
		t.Fatal(err)
	}
	state, err = m.RecomputeFRTSum(context.Background(), bucket, end)
	if err != nil {
		t.Fatal(err)
	}
	if state.RowsSampleMismatch != 1 || state.RowsMissing != 0 {
		t.Fatalf("样本数不一致必须单独计数，不能算缺行：%+v", state)
	}
	var row CapacityUserMinuteSample
	if err := m.storeDB.Where("channel_id = ?", 1).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.FRTSumVersion != 0 || row.FRTSumMs != nil {
		t.Fatalf("样本数不一致的行必须留不可计算，不得写入总和：%v ver=%d", row.FRTSumMs, row.FRTSumVersion)
	}
}

// 回算绝不推进请求覆盖水位；三类状态分别记录。
func TestFRTSumRecomputeDoesNotAdvanceRequestCoverage(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	db := frtRecomputeProdDB(t)
	m.prodDB = db
	bucket := end - 60
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	var finalizeBefore MetricFinalizeState
	_ = m.storeDB.Where("id = ?", 1).Limit(1).Find(&finalizeBefore).Error
	var cursorBefore []CustomerHealthSourceCursor
	_ = m.storeDB.Find(&cursorBefore).Error
	var dayBefore []CustomerHealthDayCoverage
	_ = m.storeDB.Find(&dayBefore).Error

	if _, err := m.RecomputeFRTSum(context.Background(), bucket, end); err != nil {
		t.Fatal(err)
	}

	var finalizeAfter MetricFinalizeState
	_ = m.storeDB.Where("id = ?", 1).Limit(1).Find(&finalizeAfter).Error
	if finalizeBefore != finalizeAfter {
		t.Fatalf("不得改动封口/覆盖状态：%+v -> %+v", finalizeBefore, finalizeAfter)
	}
	var cursorAfter []CustomerHealthSourceCursor
	_ = m.storeDB.Find(&cursorAfter).Error
	if len(cursorBefore) != len(cursorAfter) {
		t.Fatalf("不得新增/删除请求覆盖水位行：%d -> %d", len(cursorBefore), len(cursorAfter))
	}
	for i := range cursorBefore {
		if cursorBefore[i] != cursorAfter[i] {
			t.Fatalf("不得推进请求覆盖水位：%+v -> %+v", cursorBefore[i], cursorAfter[i])
		}
	}
	var dayAfter []CustomerHealthDayCoverage
	_ = m.storeDB.Find(&dayAfter).Error
	if len(dayBefore) != len(dayAfter) {
		t.Fatalf("不得新增/删除日覆盖行：%d -> %d", len(dayBefore), len(dayAfter))
	}
	for i := range dayBefore {
		if dayBefore[i] != dayAfter[i] {
			t.Fatalf("不得推进 FRT 样本覆盖水位：%+v -> %+v", dayBefore[i], dayAfter[i])
		}
	}
	// 本任务的独立状态必须存在且只属于自己。
	var own FRTSumRecomputeState
	if err := m.storeDB.Where("id = ?", 1).First(&own).Error; err != nil {
		t.Fatal(err)
	}
	if own.Status != "complete" || own.Version != frtSumSemanticsVersion {
		t.Fatalf("独立状态应记录完成与版本：%+v", own)
	}
}

// 右边界随时间前移时必须续跑，不得因"窗口变了"从头重扫。
// 这是 2026-10-21 在 8204 真实数据上踩到的：7 天任务每次重试右边界前移几分钟，
// 旧实现判成新窗口从 1% 重来，任务永远跑不完。
func TestFRTSumRecomputeResumesWhenEndMovesForward(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	db := frtRecomputeProdDB(t)
	m.prodDB = db
	bucket := end - 60
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	from := bucket - 3600
	// 先人工制造一个"跑到一半"的状态。
	half := from + 1800
	if err := m.saveFRTSumRecomputeState(context.Background(), FRTSumRecomputeState{
		ID: 1, FromTs: from, ToTs: end, DoneThroughTs: half,
		Version: frtSumSemanticsVersion, Status: "failed", LastReason: "来源 worker 未就绪",
	}); err != nil {
		t.Fatal(err)
	}
	// 右边界前移 5 分钟后重试：必须从 half 续跑，不得回到 from。
	moved := end + 300
	state, err := m.RecomputeFRTSum(context.Background(), from, moved)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.DoneThroughTs != moved {
		t.Fatalf("右边界前移后应续跑至新右端：%+v", state)
	}
	if state.ToTs != moved {
		t.Fatalf("右边界应更新为新值，否则进度分母是旧的：%+v", state)
	}
	// 从 half 续跑意味着只处理剩下的区间；若从头重扫，分片数会接近两倍。
	expected := int((moved - half + frtRecomputeSliceSeconds - 1) / frtRecomputeSliceSeconds)
	if state.Slices != expected {
		t.Fatalf("应只处理剩余区间（%d 片），实为 %d 片——可能从头重扫了", expected, state.Slices)
	}

	// 右边界回退时不得续跑：那会漏掉已跳过的尾部区间。
	if err := m.saveFRTSumRecomputeState(context.Background(), FRTSumRecomputeState{
		ID: 1, FromTs: from, ToTs: moved, DoneThroughTs: half, Version: frtSumSemanticsVersion,
	}); err != nil {
		t.Fatal(err)
	}
	back, err := m.RecomputeFRTSum(context.Background(), from, end)
	if err != nil {
		t.Fatal(err)
	}
	backExpected := int((end - from + frtRecomputeSliceSeconds - 1) / frtRecomputeSliceSeconds)
	if back.Slices != backExpected {
		t.Fatalf("右边界回退应重新开始（%d 片），实为 %d 片", backExpected, back.Slices)
	}
}

// 瞬时来源失败必须有界重试后成功，而不是让整个长任务停下。
func TestFRTSumRecomputeRetriesTransientSourceFailure(t *testing.T) {
	frtRecomputeRetryWithoutDelay(t)
	m, end := modelFRTFixture(t, true)
	failures := &frtSourceQueryFailures{}
	db := frtRecomputeProdDBWithFailures(t, failures)
	m.prodDB = db
	bucket := end - 60
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	frtRecomputeInsertLog(t, db, 2, bucket+1, 1, 2, 1, 1, "g", "m", `{"frt":300}`, 9)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("1 = 1").
		UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_observed": 0, "frt_sum_version": 0}).Error; err != nil {
		t.Fatal(err)
	}
	// 让第一次真正的来源聚合失败；第二次必须执行同一 SQL 并成功。
	failures.calls.Store(0)
	failures.remaining.Store(1)
	state, err := m.RecomputeFRTSum(context.Background(), bucket, end)
	if err != nil {
		t.Fatal(err)
	}
	if failures.calls.Load() != 2 {
		t.Fatalf("应为第一次失败、第二次成功两次来源查询，实际 %d", failures.calls.Load())
	}
	if state.Status != "complete" || state.FailedSlices != 0 {
		t.Fatalf("瞬时来源失败重试后应完成：%+v", state)
	}
	if state.LastReason != "" {
		t.Fatalf("成功后不得残留失败原因：%q", state.LastReason)
	}
	var row CapacityUserMinuteSample
	if err := m.storeDB.First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.FRTSumMs == nil || *row.FRTSumMs != 400 {
		t.Fatalf("重试机制不得影响结果正确性：%v", row.FRTSumMs)
	}
}

// 连续来源失败必须在重试上限耗尽后失败并保留未推进的切片水位，不能
// 把查询错误吞掉后报告完成。
func TestFRTSumRecomputeRetryExhaustionIsDurableFailure(t *testing.T) {
	frtRecomputeRetryWithoutDelay(t)
	m, end := modelFRTFixture(t, true)
	failures := &frtSourceQueryFailures{}
	db := frtRecomputeProdDBWithFailures(t, failures)
	m.prodDB = db
	bucket := end - 60
	frtRecomputeInsertLog(t, db, 1, bucket, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), bucket, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("1 = 1").
		UpdateColumns(map[string]any{"frt_sum_ms": nil, "frt_sum_observed": 0, "frt_sum_version": 0}).Error; err != nil {
		t.Fatal(err)
	}

	failures.calls.Store(0)
	failures.always.Store(true)
	state, err := m.RecomputeFRTSum(context.Background(), bucket, end)
	if err == nil {
		t.Fatal("连续来源失败耗尽重试后必须返回错误")
	}
	if state == nil || state.Status != "failed" || state.FailedSlices != 1 || state.DoneThroughTs != bucket {
		t.Fatalf("失败切片必须持久化且不得推进水位：state=%+v err=%v", state, err)
	}
	wantQueries := int64(frtRecomputeSliceRetries + 1)
	if got := failures.calls.Load(); got != wantQueries {
		t.Fatalf("应执行初次查询加 %d 次重试，共 %d 次；实际 %d", frtRecomputeSliceRetries, wantQueries, got)
	}
}

func TestFRTSumRecomputeRenewsLeaseBeforeRetry(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	failures := &frtSourceQueryFailures{}
	failures.remaining.Store(1)
	m.prodDB = frtRecomputeProdDBWithFailures(t, failures)
	previousWait := frtRecomputeRetryWait
	frtRecomputeRetryWait = func(context.Context, time.Duration) bool {
		// Simulate time spent in a slow attempt/backoff without sleeping. The
		// second query must see a renewed lease, not this nearly expired one.
		if err := m.storeDB.Model(&FRTSumRecomputeState{}).Where("id = ?", 1).
			Update("lease_until_ts", time.Now().Unix()+2).Error; err != nil {
			t.Fatal(err)
		}
		return true
	}
	t.Cleanup(func() { frtRecomputeRetryWait = previousWait })
	failures.beforeQuery = func() {
		if failures.calls.Load() != 1 {
			return
		}
		var state FRTSumRecomputeState
		if err := m.storeDB.First(&state, "id = ?", 1).Error; err != nil {
			t.Fatal(err)
		}
		if state.LeaseUntilTs < time.Now().Unix()+frtRecomputeLeaseSeconds-2 {
			t.Fatalf("第二次查询前未续租：%+v", state)
		}
	}
	state, err := m.RecomputeFRTSum(context.Background(), end-60, end)
	if err != nil || state.Status != "complete" || failures.calls.Load() != 2 {
		t.Fatalf("重试续租失败：state=%+v err=%v queries=%d", state, err, failures.calls.Load())
	}
}

func TestFRTSumRecomputeLostLeaseCannotWriteFacts(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	failures := &frtSourceQueryFailures{}
	m.prodDB = frtRecomputeProdDBWithFailures(t, failures)
	modelFRTInsert(t, m, end, 1, "g", "m", 1, 100)
	frtRecomputeInsertLog(t, m.prodDB, 1, end-60, 1, 2, 1, 1, "g", "m", `{"frt":100}`, 7)
	failures.beforeQuery = func() {
		now := time.Now().Unix()
		if err := m.storeDB.Model(&FRTSumRecomputeState{}).Where("id = ?", 1).
			Update("lease_until_ts", now-1).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.claimFRTRecomputeLease(context.Background(), "new-process", now); err != nil {
			t.Fatal(err)
		}
	}
	_, err := m.RecomputeFRTSum(context.Background(), end-60, end)
	if !errors.Is(err, errFRTRecomputeLeaseHeld) {
		t.Fatalf("租约被接管后必须停止：%v", err)
	}
	var row CapacityUserMinuteSample
	if err := m.storeDB.First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.FRTSumMs != nil || row.FRTSumVersion != 0 {
		t.Fatalf("旧进程不得在租约丢失后修改事实：%+v", row)
	}
	var checkpoint FRTSumRecomputeState
	if err := m.storeDB.First(&checkpoint, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.LeaseOwner != "new-process" || checkpoint.DoneThroughTs != end-60 || failures.calls.Load() != 1 {
		t.Fatalf("旧进程不得覆盖新 owner 或推进水位：%+v queries=%d", checkpoint, failures.calls.Load())
	}
}

func TestFRTSumRecomputeSQLUsesSameFRTSourceAsSampler(t *testing.T) {
	q := frtRecomputeWindowSQL()
	if !strings.Contains(q, frtSourceExpr) {
		t.Fatal("回算必须复用 frtSourceExpr，不得另写一份 FRT 取值口径")
	}
	if !strings.Contains(sampleWindowUserSQL(), frtSourceExpr) {
		t.Fatal("采集也必须使用同一个 frtSourceExpr，否则两处会漂移")
	}
	for _, forbidden := range []string{"use_time", "p95", "P95", "percentile", "ttft_500", "ttft_5k"} {
		if strings.Contains(q, forbidden) {
			t.Fatalf("回算 SQL 不得出现 %q：既不能用请求总耗时，也不能用分位数或直方图桶", forbidden)
		}
	}
	// 必须按分钟、用户、渠道、分组、模型五个维度聚合，与主键一致。
	for _, dim := range []string{"user_id", "channel_id", "model_name", "grp", "bucket"} {
		if !strings.Contains(q, dim) {
			t.Fatalf("回算 SQL 缺少维度 %q，会丢失用户维度", dim)
		}
	}
	// 只读成功流式请求；失败/退款类型不进入 FRT 总和。
	if !strings.Contains(q, "type = 2") {
		t.Fatal("回算只能统计 type=2 的成功请求")
	}
}

// 窗口统计必须分别报告行数、样本、总和、版本与可计算渠道数。
func TestFRTSumWindowReportSeparatesCoverageFromComputability(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTMeanInsert(t, m, end, 1, "g", "ok", 1, 400)
	modelFRTInsert(t, m, end, 2, "g", "nosum", 1, 400)

	s, err := m.FRTSumWindowReport(context.Background(), "24h", end-86400, end)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rows != 2 || s.RowsWithObserved != 2 {
		t.Fatalf("应有 2 行、均有有效样本：%+v", s)
	}
	if s.TtftObserved != 2 {
		t.Fatalf("有效 FRT 样本合计应为 2：%+v", s)
	}
	// 一行有总和、一行没有：两者必须分别计数，不能混成一个"覆盖率"。
	if s.RowsCurrentVer != 1 || s.RowsZeroVer != 1 {
		t.Fatalf("有总和与无总和的行必须分别报数：%+v", s)
	}
	if s.FRTSumMs != 400 || s.FRTSumObserved != 1 {
		t.Fatalf("总和只统计可回算的那一行：%+v", s)
	}
	// 可计算渠道数必须与页面判据一致：渠道 1 有总和可算均值，渠道 2 没有。
	// 若这里另写一条 SQL 判据，口径一变就会和页面显示脱节。
	if s.TotalChannel != 2 || s.ComputableChannel != 1 {
		t.Fatalf("应为 2 个渠道中 1 个可计算：%+v", s)
	}
	report := modelFRTBuild(t, m, "24h", "")
	pageComputable := int64(0)
	for _, c := range report.Channels {
		if c.FRT.MeanMs != nil {
			pageComputable++
		}
	}
	if pageComputable != s.ComputableChannel || int64(len(report.Channels)) != s.TotalChannel {
		t.Fatalf("对账数字必须等于页面实际可计算渠道数：统计 %d/%d vs 页面 %d/%d",
			s.ComputableChannel, s.TotalChannel, pageComputable, len(report.Channels))
	}
}

// 状态接口必须能把窗口钉在显式右边界上，否则对账时两次请求会错开几分钟，
// 样本数对不上会被误读成"数据不一致"。同时右边界不得越过已定稿水位。
// 两个独立的 Monitor 连接必须由同一条数据库租约互斥；仅靠进程内
// atomic flag 无法覆盖这个场景。
func TestFRTSumWindowReportRejectsInvalidRowsLikePage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values map[string]any
	}{
		{"old_sum_version", map[string]any{"frt_sum_version": 0}},
		{"invalid_sum", map[string]any{"frt_sum_ms": -1}},
		{"mismatched_samples", map[string]any{"frt_sum_observed": 2}},
		{"old_histogram", map[string]any{"ttft_semantics_version": 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, end := modelFRTFixture(t, true)
			modelFRTMeanInsert(t, m, end, 1, "g", "bad", 1, 400)
			modelFRTMeanInsert(t, m, end, 1, "g", "otherwise_valid", 1, 4000)
			modelFRTMeanInsert(t, m, end, 2, "g", "valid_channel", 1, 400)
			if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("model_name = ?", "bad").UpdateColumns(tc.values).Error; err != nil {
				t.Fatal(err)
			}
			stats, err := m.FRTSumWindowReport(context.Background(), "24h", end-86400, end)
			if err != nil {
				t.Fatal(err)
			}
			if stats.TotalChannel != 2 || stats.ComputableChannel != 1 || stats.FRTSumMs != 400 || stats.FRTSumObserved != 1 {
				t.Fatalf("状态不得汇总页面不可计算渠道的部分总和：%+v", stats)
			}
		})
	}
}

func TestFRTRecomputeDatabaseLeasePreventsConcurrentOwners(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.db")
	m1 := &Monitor{cfg: Settings{StorePath: path}}
	if err := m1.openStore(path); err != nil {
		t.Fatal(err)
	}
	m2 := &Monitor{cfg: Settings{StorePath: path}}
	if err := m2.openStore(path); err != nil {
		m1.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { m1.Close(); m2.Close() })

	now := int64(1_800_000_000)
	owner1, owner2 := "instance-one", "instance-two"
	if err := m1.claimFRTRecomputeLease(context.Background(), owner1, now); err != nil {
		t.Fatalf("首个实例应取得数据库租约：%v", err)
	}
	if err := m2.claimFRTRecomputeLease(context.Background(), owner2, now); !errors.Is(err, errFRTRecomputeLeaseHeld) {
		t.Fatalf("租约有效期内第二个实例必须冲突：%v", err)
	}
	if err := m2.claimFRTRecomputeLease(context.Background(), owner2, now+frtRecomputeLeaseSeconds+1); err != nil {
		t.Fatalf("租约过期后应允许接管：%v", err)
	}
	var state FRTSumRecomputeState
	if err := m1.storeDB.First(&state, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.LeaseOwner != owner2 {
		t.Fatalf("接管后租约 owner 错误：%+v", state)
	}
	if err := m1.saveFRTSumRecomputeStateOwned(context.Background(), state, owner1); !errors.Is(err, errFRTRecomputeLeaseHeld) {
		t.Fatalf("旧 owner 在租约被接管后不得覆盖状态：%v", err)
	}
	m2.releaseFRTRecomputeLease(context.Background(), owner2)
}

// HTTP 重试发生在进程重启之后时，必须复用持久化 TaskID/左边界，只把
// 已定稿的右边界向前推进；否则每次重启都会从新的 wall-clock 窗口重扫。
func TestFRTSumRecomputeHTTPResumesStableTaskAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.db")
	m1 := &Monitor{cfg: Settings{StorePath: path}}
	if err := m1.openStore(path); err != nil {
		t.Fatal(err)
	}
	target := metricFinalizeTarget(time.Now().Unix())
	hours := 168
	oldTo := target - frtRecomputeSliceSeconds
	from := oldTo - int64(hours)*3600
	const taskID = "frt-task-http-restart-regression"
	if err := m1.saveFRTSumRecomputeState(context.Background(), FRTSumRecomputeState{
		ID: 1, TaskID: taskID, RequestedHours: hours, FromTs: from, ToTs: oldTo,
		DoneThroughTs: oldTo, Version: frtSumSemanticsVersion, Status: "failed",
		LastReason: "进程在切片边界退出",
	}); err != nil {
		m1.Close()
		t.Fatal(err)
	}
	// This is the process boundary: close the first local store and construct a
	// fresh Monitor against the same durable state file.
	m1.Close()
	m2 := &Monitor{cfg: Settings{StorePath: path}}
	if err := m2.openStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m2.Close() })
	m2.prodDB = frtRecomputeProdDB(t)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/admin/frt-sum/recompute", m2.serveFRTSumRecompute)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/frt-sum/recompute?hours=168", nil))
	if w.Code != http.StatusAccepted {
		t.Fatalf("重启后 HTTP 重试应接受任务：code=%d body=%s", w.Code, w.Body.String())
	}
	var accepted struct {
		TaskID string `json:"task_id"`
		FromTs int64  `json:"from_ts"`
		ToTs   int64  `json:"to_ts"`
		Reused bool   `json:"reused"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if !accepted.Reused || accepted.TaskID != taskID || accepted.FromTs != from || accepted.ToTs <= oldTo {
		t.Fatalf("HTTP 未复用稳定任务窗口：accepted=%+v old_to=%d", accepted, oldTo)
	}

	deadline := time.Now().Add(5 * time.Second)
	var state FRTSumRecomputeState
	for {
		if err := m2.storeDB.First(&state, "id = ?", 1).Error; err != nil {
			t.Fatal(err)
		}
		if state.Status == "complete" {
			break
		}
		if state.Status == "failed" || time.Now().After(deadline) {
			t.Fatalf("HTTP 续跑未完成：%+v", state)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if state.TaskID != taskID || state.FromTs != from || state.ToTs != accepted.ToTs || state.DoneThroughTs != accepted.ToTs {
		t.Fatalf("重启续跑改变了任务窗口或水位：%+v accepted=%+v", state, accepted)
	}
	wantSlices := int((accepted.ToTs - oldTo + frtRecomputeSliceSeconds - 1) / frtRecomputeSliceSeconds)
	if state.Slices != wantSlices {
		t.Fatalf("HTTP 续跑应只处理尾部 %d 个切片，实际 %d：%+v", wantSlices, state.Slices, state)
	}
}

func TestFRTSumRecomputeStatusPinsExplicitEndAndRejectsUnfinalized(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTMeanInsert(t, m, end, 1, "g", "ok", 2, 400)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/admin/frt-sum/recompute", m.serveFRTSumRecomputeStatus)

	get := func(query string) (int, map[string]any) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/frt-sum/recompute"+query, nil))
		var body map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		return w.Code, body
	}

	// 默认右边界取已定稿水位；显式传同一时刻必须得到同样的窗口。
	finalized := metricFinalizeTarget(time.Now().Unix())
	if m.cfg.CustomerHealthSourceEnabled {
		finalized = customerHealthSourceFinalizedThrough(time.Now())
	}
	pinned := time.Unix(finalized, 0).UTC().Format(time.RFC3339)
	code, body := get("?to=" + url.QueryEscape(pinned))
	if code != http.StatusOK {
		t.Fatalf("显式右边界应被接受：code=%d body=%+v", code, body)
	}
	windows, _ := body["windows"].([]any)
	if len(windows) != 3 {
		t.Fatalf("应返回 24h/3d/7d 三个窗口：%+v", body["windows"])
	}
	for _, raw := range windows {
		w, _ := raw.(map[string]any)
		if int64(w["to_ts"].(float64)) != finalized {
			t.Fatalf("每个窗口的右边界都必须等于显式传入值：%+v", w)
		}
	}

	// 超过已定稿水位必须拒绝：那会把未闭合的当前分钟算进来。
	future := time.Unix(finalized+3600, 0).UTC().Format(time.RFC3339)
	if code, body = get("?to=" + url.QueryEscape(future)); code != http.StatusBadRequest {
		t.Fatalf("超过已定稿水位必须 400：code=%d body=%+v", code, body)
	}
	// 非整分钟、非 UTC 一律拒绝，避免窗口边界含义含糊。
	for _, bad := range []string{"2026-10-21T09:59:30Z", "2026-10-21T09:59:00+08:00", "not-a-time"} {
		if code, _ = get("?to=" + url.QueryEscape(bad)); code != http.StatusBadRequest {
			t.Fatalf("非法右边界 %q 必须 400，实为 %d", bad, code)
		}
	}
}
