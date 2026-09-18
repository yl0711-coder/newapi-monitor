package monitor

import (
	"strings"
	"testing"
)

// “加载更多”按钮的解锁顺序。
//
// render() 会按 lc.loading 设置 disabled；因此最终收尾必须先解锁再渲染。
func TestMoreButtonUnlocksBeforeRender(t *testing.T) {
	js := string(logChainJS)
	finallyAt := strings.Index(js, "}finally{")
	if finallyAt < 0 {
		t.Fatal("找不到 load 的 finally")
	}
	seg := js[finallyAt:]
	u := strings.Index(seg, "lc.loading=false")
	r := strings.Index(seg, "render();")
	if u < 0 || r < 0 {
		t.Fatal("finally 必须同时解锁并重绘")
	}
	if u > r {
		t.Error("解锁必须在 render() 之前：否则按钮会渲染成 disabled")
	}
}

// 新筛选开始后，上一页留下的 hasMore/游标必须立即失效；否则旧按钮在新响应
// 回来前仍可点击，并把新筛选中止成一次 append 查询。
func TestNewQueryInvalidatesOldPaginationBeforeFetch(t *testing.T) {
	js := string(logChainJS)
	start := strings.Index(js, "async function load(more){")
	if start < 0 {
		t.Fatal("找不到 load")
	}
	seg := js[start:]
	fetchAt := strings.Index(seg, "fetch('/logchain/requests?")
	resetAt := strings.Index(seg, "lc.hasMore=false;lc.nextBeforeTs=0;lc.nextBeforeID=0")
	disableAt := strings.Index(seg, "moreBtn.disabled=true")
	if fetchAt < 0 || resetAt < 0 || disableAt < 0 {
		t.Fatal("新查询必须在 fetch 前清游标并同步禁用分页按钮")
	}
	if resetAt > fetchAt || disableAt > fetchAt {
		t.Error("旧分页状态必须在发起新请求前失效")
	}
}

// 只拦重复分页，不得拦新的首屏筛选：后者需要中止旧请求并立即生效。
func TestDuplicateLoadMoreIsIgnored(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	if !strings.Contains(js, "if(more&&(lc.loading||!lc.hasMore||!lc.nextBeforeTs||!lc.nextBeforeID))return;") {
		t.Error("加载更多应在已有请求、无下一页或无完整游标时直接返回")
	}
	if strings.Contains(js, "if(lc.loading)return") {
		t.Error("不得拦截所有 load：新筛选必须能抢占正在运行的旧请求")
	}
}

// 离开排障页必须主动 abort，避免隐藏页面继续占用与客户 Portal 共用的查询槽位。
func TestLeavingLogChainCancelsRequest(t *testing.T) {
	js := string(logChainJS)
	start := strings.Index(js, "window.logChainDeactivate=function(){")
	if start < 0 {
		t.Fatal("缺少 logChainDeactivate")
	}
	seg := js[start:]
	if end := strings.Index(seg, "\n};"); end >= 0 {
		seg = seg[:end]
	}
	for _, want := range []string{"++lc.generation", "lc.abort?.abort()", "lc.abort=null", "lc.loading=false"} {
		if !strings.Contains(seg, want) {
			t.Errorf("离页清理缺少 %q", want)
		}
	}
	if !strings.Contains(pageHTML, "else if(window.logChainDeactivate)window.logChainDeactivate();") {
		t.Error("switchTab 离开客户排障时没有调用 logChainDeactivate")
	}
}
