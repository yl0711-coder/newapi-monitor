package monitor

import (
	"strings"
	"testing"
)

func TestSyncStatusPageUsesLocalStatusEndpoints(t *testing.T) {
	for _, want := range []string{
		`data-tab="sync"`,
		`id="tab-sync"`,
		`/ready`,
		`/usage/facts-status`,
		`/usage/facts-history`,
		`/channels/report?hours=24`,
		`/stability/health`,
		"刷新本页不会发起 NewAPI 来源查询",
		"syncFactMembers",
		"syncDeactivate",
		"分页事实导入",
		"分页水位",
		"raw_page_source_rows",
	} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("同步状态页缺少 %q", want)
		}
	}
	if strings.Contains(portalHTML, `data-tab="sync"`) || strings.Contains(portalHTML, "syncFactMembers") {
		t.Fatal("同步状态管理页不得进入客户 Portal")
	}
}

func TestSyncStatusPageAllowsHashNavigation(t *testing.T) {
	if !strings.Contains(pageHTML, `/^(sync|model|server|usage|stability|channels)$/`) {
		t.Fatal("同步状态页必须能由 #tab=sync 直接打开")
	}
	if !strings.Contains(string(stabilityJS), `sync:{title:'数据同步状态'`) {
		t.Fatal("同步状态页必须更新统一页面标题")
	}
}

func TestUsagePageKeepsOperationalProgressInSyncStatus(t *testing.T) {
	for _, forbidden := range []string{
		`id="usageSyncStatus"`,
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
