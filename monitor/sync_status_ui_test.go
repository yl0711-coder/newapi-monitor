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
