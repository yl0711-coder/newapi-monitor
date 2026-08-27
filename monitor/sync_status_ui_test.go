package monitor

import (
	"strings"
	"testing"
)

func TestSyncStatusPageUsesLocalStatusEndpoints(t *testing.T) {
	for _, want := range []string{
		`data-tab="sync"`,
		`id="tab-sync"`,
		`/sync/overview`,
		`/sync/workloads?domain=usage`,
		"刷新本页不会发起 NewAPI 来源查询",
		"syncFactMembers",
		"syncDeactivate",
		"syncAbort",
		"syncWorkloadAbort",
		"syncWorkloadSeq",
		"syncMaintenanceAbort",
		"syncMaintenanceSeq",
		"syncFactMaintenance",
		"kind=maintenance",
		"maintenance_attention",
		"维护、审计与修复任务",
		"成员任务${timedOut?'读取超时'",
		"维护任务${timedOut?'读取超时'",
		"setTimeout(()=>{timedOut=true;controller.abort()},7000)",
		"setTimeout(()=>{timedOut=true;controller.abort()},6000)",
		"分页事实导入",
		"分页水位",
		"raw_page_source_rows",
		"hourly_migration",
		"migration.percent",
		"小时稳定性重签",
		"问题历史重签",
	} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("同步状态页缺少 %q", want)
		}
	}
	for _, forbidden := range []string{
		`syncFetchJSON('/ready'`,
		`syncFetchJSON('/usage/facts-status'`,
		`syncFetchJSON('/usage/facts-history'`,
		`syncFetchJSON('/channels/report?hours=24'`,
		`const [ready,facts,history,health,channels]=await Promise.all([`,
	} {
		if strings.Contains(pageHTML, forbidden) {
			t.Fatalf("同步状态页仍使用分散或过重读取 %q", forbidden)
		}
	}
	if strings.Contains(portalHTML, `data-tab="sync"`) || strings.Contains(portalHTML, "syncFactMembers") {
		t.Fatal("同步状态管理页不得进入客户 Portal")
	}
}

func TestSyncStatusPageRendersBothStabilityMigrationsWithCorrectFields(t *testing.T) {
	for _, want := range []string{
		`const h=health||{},migration=h.problem_migration||{},hourly=h.hourly_migration||null;`,
		`hourly.progress_percent`,
		`hourly.completed_hours`,
		`hourly.total_hours`,
		`hourly.failed_hours`,
		`migration.percent`,
		`migration.last_success_at`,
		`'无迁移任务'`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("稳定性双迁移进度缺少 %q", want)
		}
	}
	if strings.Contains(pageHTML, `migration.coverage_percent`) {
		t.Fatal("问题迁移前端仍读取不存在的 coverage_percent 字段")
	}
}

func TestSyncStatusPageAllowsHashNavigation(t *testing.T) {
	// 白名单是完整字面量断言：新增 tab 时必须同步改这里。
	// 这样既保证 #tab=sync 仍可直达，也能挡住"误删某个 tab 名"的回归。
	if !strings.Contains(pageHTML, `/^(sync|model|server|capacity|usage|stability|channels|logchain)$/`) {
		t.Fatal("同步状态页必须能由 #tab=sync 直接打开")
	}
	if !strings.Contains(string(stabilityJS), `sync:{title:'数据同步状态'`) {
		t.Fatal("同步状态页必须更新统一页面标题")
	}
}

func TestSyncStatusPageSeparatesUpstreamBalanceTailAndHistory(t *testing.T) {
	for _, marker := range []string{
		`sync-upstream-summary`,
		`sync-upstream-account`,
		`余额快照`,
		`上游消费账单`,
		`当天数据至`,
		`历史补数`,
		`usage_adapter_name`,
		`usage_backfill_cursor`,
		`后续可按账户逐个开启验证`,
		`usage_effective_status`,
		`usage_worker_enabled`,
		`全局灰度闸门已关闭`,
		`数据陈旧`,
	} {
		if !strings.Contains(pageHTML, marker) {
			t.Fatalf("上游账户同步状态缺少 %q", marker)
		}
	}
	if strings.Contains(pageHTML, `<th>余额同步</th><th>消费同步</th>`) {
		t.Fatal("上游同步不应继续使用将状态混在一行的旧表格")
	}
	if !strings.Contains(pageHTML, `u.usage_sync_enabled&&u.usage_worker_enabled&&(usage.level==='warn'||!u.usage_backfill_done)`) {
		t.Fatal("全局灰度关闭时，历史未补完不得把同步状态提升为告警")
	}
	managementJS := string(channelManagementJS)
	if !strings.Contains(managementJS, `upstream.usage_sync_enabled&&upstream.usage_worker_enabled&&!upstream.usage_backfill_done`) {
		t.Fatal("全局灰度关闭时，渠道摘要不得继续显示历史补全中")
	}
}

func TestSyncStatusPageShowsIndependentPricingLedgerProgress(t *testing.T) {
	for _, marker := range []string{
		`计价证据账本`,
		`pricing_ledger_worker_enabled`,
		`pricing_ledger_eligible`,
		`pricing_verified_hours`,
		`pricing_mismatch_hours`,
		`不影响现有消费账单`,
		`分页续传`,
		`等待复核`,
		`对账差异`,
	} {
		if !strings.Contains(pageHTML, marker) {
			t.Fatalf("上游计价证据状态缺少 %q", marker)
		}
	}
}

func TestUsagePageKeepsOperationalProgressInSyncStatus(t *testing.T) {
	for _, forbidden := range []string{
		`id="usageSyncStatus"`,
		`<th>全历史</th>`,
		`usageLoadFactsStatus`,
		`usageMemberHistoryHTML`,
		`usageHistoryByUser`,
		"全部成员近期用量已可用",
		"逐成员进度与维护任务",
		"⏳ 历史数据补全中",
	} {
		if strings.Contains(pageHTML, forbidden) {
			t.Fatalf("用量业务页仍渲染运维进度 %q", forbidden)
		}
	}
	for _, required := range []string{
		`id="tab-sync"`,
		`syncKV('全历史归档'`,
		`syncKV('来源状态'`,
		`当前包含已签收成员`,
	} {
		if !strings.Contains(pageHTML, required) {
			t.Fatalf("数据同步状态页或业务范围提示缺少 %q", required)
		}
	}
}
