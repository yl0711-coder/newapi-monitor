package monitor

import (
	"strings"
	"testing"
)

func TestCapacityUIIsIsolatedAndRegistered(t *testing.T) {
	for _, want := range []string{
		`data-tab="capacity"`, `id="tab-capacity"`, `/capacity.css?v=2`, `/capacity.js?v=2`,
		`window.capacityActivate`, `MONITOR_CAPACITY_ENABLED=true`, `id="capUser"`, `id="capSearch"`,
		`id="capFrom"`, `id="capTo"`, `id="capApplyRange"`, `真实分钟峰值 TPM`,
	} {
		if !strings.Contains(pageHTML, want) && !strings.Contains(string(capacityJS), want) {
			t.Fatalf("容量规划 UI 缺少 %q", want)
		}
	}
	if !strings.Contains(string(stabilityJS), "capacity:{title:'容量规划'") {
		t.Fatal("容量规划必须注册独立页头，不能静默回退成用户用量")
	}
	for _, forbidden := range []string{"NEWAPI_LOG_DSN", "/data", "/usage/"} {
		if strings.Contains(string(capacityJS), forbidden) {
			t.Fatalf("容量规划前端不得触发现有来源/用量路径: %s", forbidden)
		}
	}
}
