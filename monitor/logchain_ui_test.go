package monitor

// logchain_ui_test.go：客户排障前端的结构约束。
//
// 前端是零构建的 go:embed 字符串，没有 JS 单测框架；这些断言用字面量把
// "读代码才发现、跑起来也不报错" 的那几类缺陷钉住。每条都对应一个真实修过的 bug，
// 注释写清为什么，避免后来者当成无意义的字符串检查而顺手删掉。

import (
	"strings"
	"testing"
)

func TestLogChainPageWiring(t *testing.T) {
	for _, want := range []string{
		`data-tab="logchain"`,     // 侧边栏入口
		`id="tab-logchain"`,       // tab 容器
		`/logchain.js`,            // 脚本已挂载
		`id="lcTableBody"`,        // 表体锚点
		`id="lcBlind"`,            // 盲区提示容器
		`window.logChainActivate`, // switchTab 激活入口
	} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("客户排障页缺少 %q", want)
		}
	}
	// 排障含渠道/上游域名等经营内部信息，绝不可进客户 Portal。
	for _, forbidden := range []string{`data-tab="logchain"`, `id="tab-logchain"`, "logchain/requests", "logChainActivate"} {
		if strings.Contains(portalHTML, forbidden) {
			t.Fatalf("客户排障不得进入客户 Portal：%q", forbidden)
		}
	}
}

// TestLogChainNavPlacedAfterSync 用户明确要求排障入口放在"数据同步状态"下面。
func TestLogChainNavPlacedAfterSync(t *testing.T) {
	sync := strings.Index(pageHTML, `data-tab="sync" title="数据同步状态"`)
	logchain := strings.Index(pageHTML, `data-tab="logchain" title="客户排障"`)
	if sync < 0 || logchain < 0 {
		t.Fatal("侧边栏缺少 sync 或 logchain 入口")
	}
	if logchain < sync {
		t.Fatal("客户排障入口必须排在数据同步状态之后")
	}
}

// TestLogChainJSAvoidsChangeOnTextInputs 文本框绑 change 会在失焦时触发：
// 用户输入后点"查询"＝blur + click，发两次相同查询。
// detail 泳道容量只有 1 且与客户 Portal 的日志分页共用，白发一次就是让客户多排一次队。
func TestLogChainJSAvoidsChangeOnTextInputs(t *testing.T) {
	js := string(logChainJS)
	// 必须扫描**每一处** change 绑定，不能只看第一处：
	// 文件里 lcDate / lcErrorOnly 也绑 change（它们是 date / checkbox，绑 change 是对的），
	// 只取第一个匹配会停在 lcDate 上，永远看不到真正违规的那一行。
	//
	// 也不能整串字面量乱搜：syncControls() 里回填控件值的列表本来就该含 lcModel
	// （重置要清空模型框），搜整串会把它误判成违规。
	textInputs := []string{"lcModel", "lcUser", "lcKeyword"}
	found := 0
	for _, ln := range strings.Split(stripJSLineComments(js), "\n") {
		if !strings.Contains(ln, `addEventListener('change'`) {
			continue
		}
		found++
		for _, id := range textInputs {
			if strings.Contains(ln, id) {
				t.Errorf("%s 是文本框，不得绑 change：失焦时会多发一次查询"+
					"（点查询＝blur+click 两次）。违规行：%s", id, strings.TrimSpace(ln))
			}
		}
	}
	if found == 0 {
		t.Fatal("找不到任何 change 绑定")
	}
	if !strings.Contains(js, `['lcModel','lcUser','lcKeyword'].forEach`) {
		t.Error("lcModel 应与其它文本框一起走回车/查询按钮")
	}
}

// TestLogChainJSUsesGenerationGuard 早期版本用 if(loading)return 做互斥，
// 导致请求进行中改筛选条件被静默丢弃，表格停在旧结果上，用户以为筛选没生效。
func TestLogChainJSUsesGenerationGuard(t *testing.T) {
	js := string(logChainJS)
	// 必须剔掉注释再搜：解释"为什么不用 loading 互斥"的注释里本就含这段字面量，
	// 直接搜整个文件会命中自己写的说明，制造假警报。
	code := stripJSLineComments(js)
	if strings.Contains(code, "if(lc.loading)return") {
		t.Error("不得用 loading 标记做互斥：会静默丢弃请求进行中的筛选变更")
	}
	if !strings.Contains(code, "++lc.generation") || !strings.Contains(code, "gen!==lc.generation") {
		t.Error("应使用世代计数：新请求中止旧请求，且只有最新世代能写状态")
	}
}

// stripJSLineComments 去掉 // 行注释，只保留可执行代码。
// 用于"某写法不得出现"这类断言——注释里为解释而引用该写法是正常的，不该判为违规。
// 只处理行注释即可：本文件不用块注释，且不需要处理字符串里的 // （无此用法）。
func stripJSLineComments(js string) string {
	lines := strings.Split(js, "\n")
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		if at := strings.Index(ln, "//"); at >= 0 {
			ln = ln[:at]
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}

// TestLogChainJSChecksButtonsBeforeRow 复制/跳转按钮都在展开行(tr.lc-detail)里，
// 而展开行没有 data-lc-id。先取行会让 closest 返回 null 直接 return，按钮永远点不动。
func TestLogChainJSChecksButtonsBeforeRow(t *testing.T) {
	js := string(logChainJS)
	copyAt := strings.Index(js, "data-lc-copy]")
	jumpAt := strings.Index(js, "data-lc-jump]")
	rowAt := strings.Index(js, `closest('tr[data-lc-id]')`)
	if copyAt < 0 || jumpAt < 0 || rowAt < 0 {
		t.Fatal("行点击委托缺少复制/跳转/取行三者之一")
	}
	if copyAt > rowAt || jumpAt > rowAt {
		t.Error("按钮判断必须排在取行之前，否则展开行内的按钮点不动")
	}
	// 内联 onclick 要把域名插进 HTML 属性里的 JS 字符串字面量，多一层转义面。
	if strings.Contains(js, "onclick=") {
		t.Error("不得用内联 onclick，统一走 data 属性 + 事件委托")
	}
}

// TestLogChainJSUsesAbsoluteAPIPaths 页面同时挂在 / 和 /monitor 下，
// 相对路径在 /monitor 会拼成 /monitor/logchain/... 而 404。
func TestLogChainJSUsesAbsoluteAPIPaths(t *testing.T) {
	js := string(logChainJS)
	for _, want := range []string{`fetch('/logchain/requests?`, `fetch('/logchain/filters'`} {
		if !strings.Contains(js, want) {
			t.Errorf("接口路径必须是绝对路径：缺少 %q", want)
		}
	}
}

// TestLogChainJSKeepsBlindSpotsVisible 盲区不得折叠。这个功能最可能造成的实际损害是：
// 客户说"我请求根本发不出去"，管理员在此查不到，于是判断客户在瞎说。
// 而前置拒绝(限流/无可用渠道)根本不写 logs，查不到属预期。
func TestLogChainJSKeepsBlindSpotsVisible(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "renderBlindSpots") {
		t.Fatal("缺少盲区渲染")
	}
	if !strings.Contains(js, "lc.blindSpots=data.blind_spots") {
		t.Error("必须消费后端返回的 blind_spots，不得在前端自行硬编码")
	}
	if strings.Contains(pageHTML, `id="lcBlind" class="lc-blind" hidden><details`) ||
		strings.Contains(js, "<details") {
		t.Error("盲区不得放进折叠面板")
	}
}

// TestLogChainJSNeverScrubsContent 排障的全部意义在于看到上游错误原文。
// 若前端对 content 做二次清洗/截断，等于把 scrubContent 的坑搬到前端。
func TestLogChainJSNeverScrubsContent(t *testing.T) {
	js := string(logChainJS)
	if strings.Contains(js, "scrubContent") {
		t.Error("前端不得引入 scrubContent 语义")
	}
	// 原文用 <pre> 原样呈现：要能直接拿去问上游客服，不折行不美化。
	if !strings.Contains(js, `<pre class="lc-raw">`) {
		t.Error("错误原文必须用 <pre> 原样呈现")
	}
	if !strings.Contains(pageHTML, "white-space:pre") {
		t.Error("lc-raw 必须保留原始空白与换行")
	}
}
