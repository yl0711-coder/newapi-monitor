package monitor

// 上游错误日志采集的**调度层**约束。
//
// 与 channel_upstream_errorlog_test.go（解析与落库）分开：那边测「拿到数据后对不对」，
// 这边测「该不该去拿、失败了怎么办」。两者失效方式不同——解析对但调度永不触发，
// 或调度正常但失败后把水位推过去（那会永久漏行），编译和对方的用例都不会报。

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestSyncDueUpstreamErrorLogsRespectsGlobalGate 全局开关关闭时一步都不走。
func TestSyncDueUpstreamErrorLogsRespectsGlobalGate(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UpstreamErrorLogSyncEnabled = false
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: "a.example", Provider: upstreamProviderNewAPI,
		Enabled: true, UsageSyncEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}

	m.syncDueUpstreamErrorLogs(context.Background())

	var n int64
	m.storeDB.Model(&UpstreamErrorLogSyncState{}).Count(&n)
	if n != 0 {
		t.Errorf("灰度关闭时不应产生任何调度状态，got=%d 行", n)
	}
}

// TestSyncOneUpstreamErrorLogMarksUnsupportedProviders ★ 本组最要紧的一条 ★
//
// sub2api / aicodewith 的端点是聚合/计价语义，没有日志接口。必须落 unsupported
// 状态且不再排期——但**状态一定要落库**：页面要能区分
// 「该上游无日志接口」与「该上游没有错误」。两者都是空，含义相反。
func TestSyncOneUpstreamErrorLogMarksUnsupportedProviders(t *testing.T) {
	for _, provider := range []string{upstreamProviderSub2API, upstreamProviderAICodeWith} {
		t.Run(provider, func(t *testing.T) {
			m := newTestMonitor(t)
			m.cfg.UpstreamErrorLogSyncEnabled = true
			row := ChannelUpstreamAccount{
				Domain: provider + ".example", Provider: provider,
				Enabled: true, UsageSyncEnabled: true,
			}

			m.syncOneUpstreamErrorLog(context.Background(), row, 1000)

			var state UpstreamErrorLogSyncState
			if err := m.storeDB.First(&state, "domain = ?", row.Domain).Error; err != nil {
				t.Fatalf("未落状态：页面将无法区分「无日志接口」与「没有错误」: %v", err)
			}
			if state.Status != upstreamStatusUnsupported {
				t.Errorf("状态应为 %s，got=%s", upstreamStatusUnsupported, state.Status)
			}
			if state.NextSyncAt != 0 {
				t.Errorf("不支持的 provider 不该排期重试，got NextSyncAt=%d", state.NextSyncAt)
			}
			if !strings.Contains(state.LastError, "无日志接口") {
				t.Errorf("原因需说清是接口不支持而非出错: %q", state.LastError)
			}
		})
	}
}

// TestSyncOneUpstreamErrorLogSkipsUnsupportedOnLaterRuns 已标不支持后不再重复写库。
// 每分钟一轮、每轮都更新一次同样的状态是纯粹的写放大。
func TestSyncOneUpstreamErrorLogSkipsUnsupportedOnLaterRuns(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UpstreamErrorLogSyncEnabled = true
	row := ChannelUpstreamAccount{
		Domain: "s.example", Provider: upstreamProviderSub2API,
		Enabled: true, UsageSyncEnabled: true,
	}
	m.syncOneUpstreamErrorLog(context.Background(), row, 1000)

	var first UpstreamErrorLogSyncState
	if err := m.storeDB.First(&first, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	// 第二轮：时刻推后，但状态不该被改写。
	m.syncOneUpstreamErrorLog(context.Background(), row, 9000)

	var second UpstreamErrorLogSyncState
	if err := m.storeDB.First(&second, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if second.UpdatedAt != first.UpdatedAt {
		t.Errorf("已标 unsupported 后不应重复写库: first=%d second=%d",
			first.UpdatedAt, second.UpdatedAt)
	}
}

// TestFailUpstreamErrorLogStateDoesNotAdvanceWatermark ★ 失败绝不能推进水位 ★
//
// 推进了就等于把失败那段永久跳过——而那段里正是出错的时候，最需要日志。
func TestFailUpstreamErrorLogStateDoesNotAdvanceWatermark(t *testing.T) {
	m := newTestMonitor(t)
	state := UpstreamErrorLogSyncState{Domain: "d.example", SyncedUntil: 5000}

	m.failUpstreamErrorLogState(context.Background(), &state, 9000, errTestUpstream)

	if state.SyncedUntil != 5000 {
		t.Errorf("失败时水位被推进了，那段日志将永久漏掉: got=%d want=5000", state.SyncedUntil)
	}
	if state.Status != upstreamStatusError {
		t.Errorf("状态应为 error，got=%s", state.Status)
	}
	if state.NextSyncAt <= 9000 {
		t.Errorf("应排一个将来的重试时刻，got=%d", state.NextSyncAt)
	}
}

// TestFailUpstreamErrorLogStateBacksOffExponentially 连续失败要指数退避并有上限。
// 不退避会让上游挂掉时每 5 分钟撞一次；无上限则会退到几天后、恢复了也不拉。
func TestFailUpstreamErrorLogStateBacksOffExponentially(t *testing.T) {
	m := newTestMonitor(t)
	state := UpstreamErrorLogSyncState{Domain: "d.example"}
	var prev int64
	for i := 1; i <= 12; i++ {
		m.failUpstreamErrorLogState(context.Background(), &state, 0, errTestUpstream)
		gap := state.NextSyncAt
		if i > 1 && gap < prev {
			t.Errorf("第 %d 次退避比上次短了: %d < %d", i, gap, prev)
		}
		if gap > int64(upstreamErrorLogBackoffMax.Seconds()) {
			t.Errorf("第 %d 次退避超过上限: %ds > %v", i, gap, upstreamErrorLogBackoffMax)
		}
		prev = gap
	}
	// 末尾应已顶到上限，否则说明上限没生效。
	if prev != int64(upstreamErrorLogBackoffMax.Seconds()) {
		t.Errorf("多次失败后应顶到上限 %v，got=%ds", upstreamErrorLogBackoffMax, prev)
	}
}

// TestSyncOneUpstreamErrorLogHonoursNextSyncAt 未到期就不该发请求。
// 调度器每分钟叫一次，靠这个跳过——没有它就会变成每分钟拉一次上游。
func TestSyncOneUpstreamErrorLogHonoursNextSyncAt(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UpstreamErrorLogSyncEnabled = true
	row := ChannelUpstreamAccount{
		Domain: "n.example", Provider: upstreamProviderNewAPI,
		Enabled: true, UsageSyncEnabled: true,
	}
	// 预置一个未来的下次同步时刻。
	if err := m.storeDB.Create(&UpstreamErrorLogSyncState{
		Domain: row.Domain, NextSyncAt: 100000, Status: upstreamStatusOK, UpdatedAt: 500,
	}).Error; err != nil {
		t.Fatal(err)
	}

	// 若未按期跳过，会走到凭据解密并因缺凭据而落 error 状态。
	m.syncOneUpstreamErrorLog(context.Background(), row, 1000)

	var state UpstreamErrorLogSyncState
	if err := m.storeDB.First(&state, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if state.UpdatedAt != 500 || state.Status != upstreamStatusOK {
		t.Errorf("未到期却动了状态: updated=%d status=%s", state.UpdatedAt, state.Status)
	}
}

// TestEncodeUnresolvedFieldsIsStable 未命中统计的文本必须稳定。
//
// map 遍历顺序随机，直接拼会让这一列每轮都"变化"，运营看不出到底有没有变；
// 也会让 upsert 每轮都真的写一次库。
func TestEncodeUnresolvedFieldsIsStable(t *testing.T) {
	in := map[string]int{"content": 3, "token_name": 1, "group": 2}
	first := encodeUnresolvedFields(in)
	for i := 0; i < 20; i++ {
		if got := encodeUnresolvedFields(in); got != first {
			t.Fatalf("输出不稳定: %q vs %q", got, first)
		}
	}
	// 必须是合法 JSON 且含计数，页面要能直接解析。
	if !strings.Contains(first, `"content":3`) {
		t.Errorf("缺 content 计数: %s", first)
	}
	if encodeUnresolvedFields(nil) != "" {
		t.Error("空统计应返回空串，避免库里存一个无意义的 {}")
	}
}

// TestUpstreamErrorLogSyncStateRegisteredForMigration 调度状态表也要进 AutoMigrate。
// 漏了会在生产上「表不存在」，而这个错误只在第一次调度时才出现。
func TestUpstreamErrorLogSyncStateRegisteredForMigration(t *testing.T) {
	m := newTestMonitor(t)
	// 能建能查即证明已在模型集里（newTestMonitor 走的是同一条 openStore 路径）。
	if err := m.storeDB.Create(&UpstreamErrorLogSyncState{
		Domain: "x.example", Status: upstreamStatusPending, UpdatedAt: 1,
	}).Error; err != nil {
		t.Fatalf("调度状态表不可用，可能未注册进 AutoMigrate: %v", err)
	}
	var got UpstreamErrorLogSyncState
	if err := m.storeDB.First(&got, "domain = ?", "x.example").Error; err != nil {
		t.Fatalf("回读失败: %v", err)
	}
}

// TestUpstreamErrorLogSyncWiredIntoScheduler 调度器真的调了它。
//
// 函数写对但没挂进 worker 等于没做，而两者各自的用例都会绿。
// 读源码断言：构造一次真实的 worker 启动需要起 goroutine 与计时器，代价远大于此。
func TestUpstreamErrorLogSyncWiredIntoScheduler(t *testing.T) {
	src, err := readMonitorSource("channel_upstream.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(src, "m.syncDueUpstreamErrorLogs(ctx)") < 2 {
		t.Error("调度器应在首轮与定时轮各调一次错误日志采集")
	}
	// 三个开关任一打开都得起 worker：漏掉这个条件会导致「只开错误日志」时
	// worker 根本不启动。
	if !strings.Contains(src, "!m.cfg.UpstreamErrorLogSyncEnabled {") {
		t.Error("worker 启动条件未包含 UpstreamErrorLogSyncEnabled")
	}
}

// TestUpstreamErrorLogSettingHasOwnEnv 采集开关必须独立于用量同步。
//
// 两者拉同一端点但目的与频率不同（type=2 量大为账单、type=5 量小为排障），
// 复用一个开关会互相牵制。
func TestUpstreamErrorLogSettingHasOwnEnv(t *testing.T) {
	src, err := readMonitorSource("settings.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(src, "MONITOR_UPSTREAM_ERRORLOG_SYNC_ENABLED") {
		t.Error("缺独立环境变量 MONITOR_UPSTREAM_ERRORLOG_SYNC_ENABLED")
	}
	// 默认必须是关：新功能一律灰度，与 UpstreamUsageSyncEnabled 同规矩。
	if !strings.Contains(src, `env("MONITOR_UPSTREAM_ERRORLOG_SYNC_ENABLED", "false")`) {
		t.Error("新采集开关默认值必须为 false")
	}
}

// readMonitorSource 读同包源文件。判断「某处有没有调用某函数」只能看源码：
// 构造真实的 worker 启动要起 goroutine 与计时器，代价远大于读一次文件。
// 与 logchain_radius_wiring_test.go 的 logChainGoSource 同一做法。
func readMonitorSource(name string) (string, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

var errTestUpstream = &testUpstreamError{}

type testUpstreamError struct{}

func (e *testUpstreamError) Error() string { return "测试用上游错误" }

// 不需要「确保 upstreamErrorLogBackoffMax 是 time.Duration」的守卫：
// 上面第 131/137 行直接调了 .Seconds()，常量若被改成整数，那两行会编译失败。
// 原先那行 var _ = time.Duration(...) 既守不住（整数也能转换通过），
// 又被 golangci-lint 的 unconvert 判为多余转换。
