package monitor

import (
	"strings"
	"testing"
)

func TestUsageMemberUIUsesRetryStableIdempotencyKeys(t *testing.T) {
	for _, required := range []string{
		"const usageMutationKeys=new Map()",
		"'Idempotency-Key':requestId",
		"request_id:requestId",
		"member-add:${v}:${gid}",
		"member-remove:${id}",
		"member-company:${uid}:${newGid}",
		"group-delete:${id}",
		"const data=await res.json()",
		"if(res.status<500)usageMutationKeys.delete(slot)",
	} {
		if !strings.Contains(pageHTML, required) {
			t.Fatalf("成员生命周期 UI 缺少幂等契约 %q", required)
		}
	}
	for _, forbidden := range []string{
		"fetch('/usage/users/delete'",
		"fetch('/usage/users/group'",
		"fetch('/usage/groups/delete'",
		"fetch('/usage/users',{method:'POST'",
	} {
		if strings.Contains(pageHTML, forbidden) {
			t.Fatalf("成员生命周期 UI 仍绕过幂等请求包装: %q", forbidden)
		}
	}
	parsedAt := strings.Index(pageHTML, "const data=await res.json()")
	releasedAt := strings.Index(pageHTML, "if(res.status<500)usageMutationKeys.delete(slot)")
	if parsedAt < 0 || releasedAt < parsedAt {
		t.Fatal("必须在完整读取响应体后才释放幂等键")
	}
}

func TestUsageMemberUIConfirmsCompanyCorrectionAndGuardsGroupDelete(t *testing.T) {
	for _, required := range []string{
		"该用户的全部历史用量将立即从原公司撤出",
		"此操作只应用于纠正最初分错的公司",
		"系统不会把成员批量改成未分组",
		"客户 Portal 仍已开通",
		"确认删除空公司",
		"历史 facts 保留且不修改主站",
	} {
		if !strings.Contains(pageHTML, required) {
			t.Fatalf("成员/公司高风险操作缺少决策文案 %q", required)
		}
	}
	if strings.Contains(pageHTML, "个用户回到未分组") {
		t.Fatal("删除公司仍在暗示后端会批量改组")
	}
}

func TestSyncStatusUIOwnsFullHistoryStagesAndRootActions(t *testing.T) {
	for _, required := range []string{
		"/sync/workloads?domain=usage",
		"syncRenderFactMembers",
		"verification_status",
		"usageFactsStageLabel",
		"full_history_source_audit",
		"usageRetryHistory",
		"/usage/facts-history/retry",
		"usageRepairHistoryDay",
		"/usage/facts-history/repair",
		"REPAIR_FULL_HISTORY_DAY",
		"usageHistoryRepairKeys",
		"estimated_seconds",
		"disk_blocked",
		"bulk_circuit_state",
		"if(!IS_ROOT||status!=='paused')return ''",
	} {
		if !strings.Contains(pageHTML, required) {
			t.Fatalf("同步状态的全历史管理员 UI 缺少契约 %q", required)
		}
	}
	for _, forbidden := range []string{"usageLoadFactsStatus", "usageMemberHistoryHTML", "usageHistoryByUser", `<th>全历史</th>`} {
		if strings.Contains(pageHTML, forbidden) {
			t.Fatalf("用户用量业务页仍保留全历史运维入口 %q", forbidden)
		}
	}
}
