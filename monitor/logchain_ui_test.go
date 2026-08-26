package monitor

// logchain_ui_test.go：客户排障前端的结构约束。
//
// 前端是零构建的 go:embed 字符串，没有 JS 单测框架；这些断言用字面量把
// "读代码才发现、跑起来也不报错" 的那几类缺陷钉住。每条都对应一个真实修过的 bug，
// 注释写清为什么，避免后来者当成无意义的字符串检查而顺手删掉。

import (
	"strconv"
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

// TestLogChainJSKeepsBlindSpotsPresent 盲区必须存在且标题始终可见。
//
// 早先这条断言写的是"不得折叠"。用户后来要求默认收起（自用系统，展开占地方），
// 折叠本身已不再是回归——**真正要守的是"标题在收起态仍然可见"**：
// 这个功能最可能造成的实际损害是客户说"我请求根本发不出去"、管理员查不到、
// 于是判断客户在瞎说，而前置拒绝根本不写 logs。整块隐藏才是回归。
// 折叠细节由 TestLogChainBlindSpotsCollapsible 覆盖。
func TestLogChainJSKeepsBlindSpotsPresent(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "renderBlindSpots") {
		t.Fatal("缺少盲区渲染")
	}
	if !strings.Contains(js, "lc.blindSpots=data.blind_spots") {
		t.Error("必须消费后端返回的 blind_spots，不得在前端自行硬编码")
	}
	// 标题必须在收起态可见：<summary> 保证这一点。整块 hidden 才是回归。
	if !strings.Contains(js, "<summary") {
		t.Error("收起态必须仍显示标题，否则盲区等于消失")
	}
	if !strings.Contains(pageHTML, `id="lcBlind"`) {
		t.Error("页面缺少盲区容器")
	}
}

// TestLogChainPageTitleRegistered 顶部标题来自 stability.js 的 ST_HEADERS 映射表，
// 缺条目会走 ||ST_HEADERS.usage 兜底，静默显示成"用户用量"——页面能用但标题串台。
// 新增 tab 时必须同步加，这条测试就是防止漏加。
func TestLogChainPageTitleRegistered(t *testing.T) {
	js := string(stabilityJS)
	if !strings.Contains(js, "logchain:{title:'客户排障'") {
		t.Error("ST_HEADERS 缺 logchain 条目，顶部标题会错显成「用户用量」")
	}
	// 图标也要注册，否则 ST_ICONS[h.icon] 取到 undefined、图标位置空白。
	if !strings.Contains(js, "search:'<svg") {
		t.Error("ST_ICONS 缺 search 图标")
	}
}

// TestLogChainScopeBarHasNoAllRequests 本页定位是问题清单，不提供"全部请求"档。
// 用户明确要求：只统计错误和异常，正常请求不看。要看全量流水去「用户用量」。
func TestLogChainScopeBarHasNoAllRequests(t *testing.T) {
	for _, want := range []string{
		`data-lc-scope="error"`,
		`data-lc-scope="stream"`,
		`data-lc-scope="billing"`,
		`data-lc-scope="anomaly_all"`,
		`data-lc-scope="err_anom"`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("范围按钮缺少 %q", want)
		}
	}
	// 精确匹配整个属性，避免把 anomaly_all / err_anom 当成 all 误判。
	if strings.Contains(pageHTML, `data-lc-scope="all"`) {
		t.Error(`不得有"全部请求"档：本页只看错误与异常`)
	}
	js := string(logChainJS)
	if strings.Contains(js, `'err_anom','all'`) || strings.Contains(js, `,'all']`) {
		t.Error("SCOPES 里不得残留 all")
	}
	// 默认必须落在 error，与本页定位一致（当天故障清单）。
	if !strings.Contains(js, "scope:'error'") {
		t.Error("默认范围应为 error")
	}
}

// TestLogChainClientGoneIsSeparateScope 客户端断连必须是独立的查看范围。
//
// 2026-08-24 生产实测：当天 1594 条 client_gone 里 92% 已真交付内容，
// 而真正的流故障只有 25 条。混在同一档时后者会被彻底淹掉。
func TestLogChainClientGoneIsSeparateScope(t *testing.T) {
	js := string(logChainJS)
	page := pageHTML

	// 前端必须有这一档，且传给后端而非自己滤（前端滤会让分页与计数失准）。
	if !strings.Contains(js, "'client_gone'") {
		t.Error("前端 SCOPES 缺 client_gone 档")
	}
	if !strings.Contains(js, `q.set('anomaly','client_gone')`) {
		t.Error("client_gone 必须作为 anomaly 参数传给后端，不能在前端过滤")
	}
	// 必须有对应的按钮，否则这一档用户点不到。
	if !strings.Contains(page, `data-lc-scope="client_gone"`) {
		t.Error("page.html 缺客户端断连的范围按钮")
	}
	// 「流故障」按钮的说明必须写明不含客户端断连——否则用户以为流故障档已覆盖它。
	streamBtnIdx := strings.Index(page, `data-lc-scope="stream"`)
	if streamBtnIdx < 0 {
		t.Fatal("找不到流故障按钮")
	}
	streamBtn := page[streamBtnIdx:]
	if end := strings.Index(streamBtn, "</button>"); end > 0 {
		streamBtn = streamBtn[:end]
	}
	if !strings.Contains(streamBtn, "不含客户端断连") {
		t.Error("流故障按钮的 title 应说明不含客户端断连，否则用户以为它已覆盖")
	}

	// 只带 client_gone 的行不得标黄底：黄底是"要核查"的信号，
	// 而客户断连多数是客户自己的正常行为，全标黄会淹掉真需要核查的行。
	if !strings.Contains(js, "onlyClientGone") {
		t.Error("只带 client_gone 标签的行不应标异常黄底，缺少该判断")
	}
	// 标签用中性色而非告警色。
	if !strings.Contains(js, "lc-tag-gone") || !strings.Contains(page, ".lc-tag-gone") {
		t.Error("client_gone 标签应有独立的中性色样式（lc-tag-gone），不与告警色混用")
	}
}

// TestLogChainTableColumnCountsAgree 表头列数、初始占位 colspan、每行 td 数必须一致（P3-01）。
//
// 报告实测：表格 8 列而初始空状态写 colspan=7，加载中那行占位宽度对不上。
// 这类缺陷编译能过、测试也不报错，只在肉眼看页面时才发现，
// 所以用测试把三处数字钉在一起。
func TestLogChainTableColumnCountsAgree(t *testing.T) {
	page := pageHTML
	js := string(logChainJS)

	// 表头列数：截取 lcTable 到 lcTableBody 之间的片段再数 <th。
	// 用 lcSortTh（排障表格独有的排序表头）反向定位到它所在的 thead，
	// 再数到 lcTableBody 为止。页面里有多个表格，不能全局数 <th。
	end := strings.Index(page, `id="lcTableBody"`)
	sortTh := strings.Index(page, `id="lcSortTh"`)
	if end < 0 || sortTh < 0 {
		t.Fatal("找不到 lcTableBody / lcSortTh 锚点")
	}
	start := strings.LastIndex(page[:sortTh], "<thead>")
	if start < 0 {
		t.Fatal("找不到排障表格的 thead")
	}
	// 数 "<th " 与 "<th>" 两种，不能只数 "<th"——那会把 <thead> 也算进来。
	head := page[start:end]
	thCount := strings.Count(head, "<th ") + strings.Count(head, "<th>")

	// 初始占位 colspan 必须等于表头列数。
	want := `colspan="` + strconv.Itoa(thCount) + `" class="lc-empty"`
	if !strings.Contains(page, want) {
		t.Errorf("初始占位 colspan 与表头 %d 列不一致", thCount)
	}

	// rowHTML 里每行渲染的单元格数也必须一致。只数带 lc- 类名的 td，
	// 避开展开行（lc-detail）里的表格。
	rowStart := strings.Index(js, "function rowHTML")
	if rowStart < 0 {
		t.Fatal("找不到 rowHTML")
	}
	rowEnd := strings.Index(js[rowStart:], "\n}")
	if rowEnd < 0 {
		t.Fatal("rowHTML 边界不清")
	}
	rowFn := js[rowStart : rowStart+rowEnd]
	// 同理数 "<td " 与 "<td>" 两种：单元格有的带 class 有的不带。
	tdCount := strings.Count(rowFn, "<td ") + strings.Count(rowFn, "<td>")
	if tdCount != thCount {
		t.Errorf("每行 td 数=%d，表头=%d 列", tdCount, thCount)
	}
}

// TestLogChainErrAnomFiltersInSQL err_anom（错误+异常）必须在 SQL 层筛。
// 若改成"后端返全部、前端滤掉正常的"，limit / has_more / 计数三者会全部失准：
// 后端按 limit 返 100 行，前端滤掉其中正常的消费请求，页面显示 40 行却说还有更多。
func TestLogChainErrAnomFiltersInSQL(t *testing.T) {
	sql := logChainAnomalySQL(anomalyErrAnom)
	if !strings.Contains(sql, "type = 5") {
		t.Errorf("err_anom 必须含错误日志: %s", sql)
	}
	// 不绑 NOT IN 的具体字面量：正常取值名单加成员时字面量会变而行为不变
	// （2026-08-21 加入 done 即属此情形）。只断言排除法在场。
	for _, want := range []string{"quota > 0", "quota = 0", "NOT IN ("} {
		if !strings.Contains(sql, want) {
			t.Errorf("err_anom 缺少异常判据 %q: %s", want, sql)
		}
	}
	js := string(logChainJS)
	if !strings.Contains(js, `q.set('anomaly','err_anom')`) {
		t.Error("前端必须把 err_anom 传给后端，不能自己滤")
	}
	// 前端不得按 anomaly_tags 过滤行——那正是会让分页失准的写法。
	if strings.Contains(js, "rows.filter(r=>(r.anomaly_tags") &&
		!strings.Contains(js, "const anoms=rows.filter") {
		t.Error("前端不得用 anomaly_tags 过滤行（只可用于计数）")
	}
}

// TestLogChainBlindSpotsCollapsible 盲区默认收起（自用系统，展开占地方），
// 但**标题必须始终可见** —— 它的价值在于"你没主动去看时也知道它存在"。
// 若整块隐藏或删掉，"查不到"就会被读成"没发生过"。
func TestLogChainBlindSpotsCollapsible(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "<details class=\"lc-blind-details\"") {
		t.Error("盲区应用 <details> 折叠（原生元素自带键盘可达性与 aria 语义）")
	}
	if !strings.Contains(js, "<summary class=\"lc-blind-head\"") {
		t.Error("标题必须是 <summary>，收起时仍可见")
	}
	// 默认收起：不得无条件写死 open。
	if strings.Contains(js, "<details class=\"lc-blind-details\" open>") {
		t.Error("不得默认展开")
	}
	// 仍必须消费后端返回的 blind_spots，不得在前端硬编码或省略。
	if !strings.Contains(js, "lc.blindSpots=data.blind_spots") {
		t.Error("必须消费后端的 blind_spots")
	}
	if !strings.Contains(pageHTML, ".lc-blind-details[open]") {
		t.Error("缺少展开态样式")
	}
}

// TestLogChainBlindSpotsDropsBodyCapture 第三条"从不采集请求/响应正文"已删。
// 加入 end_reason / end_error 后，"回答只出一半就断了"已能回答；
// 剩下真正答不了的是"内容写得不对"，那属内容审查、不是排障范畴。
func TestLogChainBlindSpotsDropsBodyCapture(t *testing.T) {
	spots := logChainBlindSpots()
	if len(spots) != 2 {
		t.Fatalf("盲区应为 2 条，实际 %d 条: %v", len(spots), spots)
	}
	for _, s := range spots {
		if strings.Contains(s, "请求/响应正文") {
			t.Error("第三条应已删除")
		}
	}
	// 前两条必须留：前置拒绝不写 logs、重试链无法归并，都仍然成立。
	joined := strings.Join(spots, "\n")
	// 按实际措辞断言，不按我脑子里的叫法："未打到渠道即被拒"才是文案原文。
	for _, want := range []string{"未打到渠道即被拒", "user_id", "归并"} {
		if !strings.Contains(joined, want) {
			t.Errorf("盲区缺少关键说明 %q: %v", want, spots)
		}
	}
}

// TestLogChainDetailColumnSwitchesByScope 明细列必须随范围切换表头与内容。
//
// logs.content 在不同类型里装的是完全不同的东西：type=5 是上游错误原文，
// type=2 是计费摘要（"模型倍率 3.00, 分组倍率 1.00"）。异常行全是 type=2，
// 表头固定写"上游返回原文"就会在最显眼的位置摆一句无用的计费摘要。
func TestLogChainDetailColumnSwitchesByScope(t *testing.T) {
	if !strings.Contains(pageHTML, `id="lcDetailTh"`) {
		t.Fatal("明细列表头缺少 id，无法随范围切换")
	}
	js := string(logChainJS)
	if !strings.Contains(js, "function syncDetailHeader(") {
		t.Fatal("缺少 syncDetailHeader")
	}
	if !strings.Contains(js, "异常详情") {
		t.Error("异常范围下表头应改为「异常详情」")
	}
	// contentCell 必须按行的性质判断，不能只按当前筛选——
	// err_anom 混排时同一列里两种行并存。
	if !strings.Contains(js, "if(r.type===2&&isAnom)") {
		t.Error("contentCell 应按行判断（type=2 且有异常标签）而非按当前 scope")
	}
	// 异常行不得把计费摘要当主内容。
	if !strings.Contains(js, "计费与交付不一致") {
		t.Error("纯消费异常应说清事实，而不是摆计费摘要")
	}
	// 展开区的 content 标题必须如实说明它是计费摘要。
	if !strings.Contains(js, "非上游返回") {
		t.Error("展开区应说明 type=2 的 content 是计费摘要、不是上游返回")
	}
}

// TestLogChainSortControls 排序有两个入口（按钮组 + 点表头），必须共用同一处状态，
// 否则按钮高亮、表头箭头、实际查询三者会脱节。
func TestLogChainSortControls(t *testing.T) {
	for _, want := range []string{
		`data-lc-order="desc"`,
		`data-lc-order="asc"`,
		`id="lcSortTh"`,
		`id="lcSortArrow"`,
		`id="lcSortHint"`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("排序控件缺少 %q", want)
		}
	}
	js := string(logChainJS)
	if !strings.Contains(js, "function setOrder(") {
		t.Fatal("缺少 setOrder：两个入口必须共用同一处状态")
	}
	// 切方向后必须回到第一页。游标沿排序方向前进，方向反了还沿用旧游标会往回翻。
	if !strings.Contains(js, "if(lc.asc===asc)return") {
		t.Error("点当前方向应直接返回，不重复查库（该泳道与客户 Portal 共用）")
	}
	// 表头是 role=button，键盘可达性要跟上。
	if !strings.Contains(js, "e.key==='Enter'||e.key===' '") {
		t.Error("可点击表头须支持回车/空格")
	}
	// 「加载更多」文案必须跟随方向：正序取的是更晚的记录。
	if !strings.Contains(js, "lc.asc?'加载更晚的记录':'加载更早的记录'") {
		t.Error("加载更多的文案未跟随排序方向，正序下会与实际行为相反")
	}
}

// TestLogChainTableHeaderHasSubLabels 表头用主名称 + 灰色副说明两行，
// 省掉一堆要悬停才看得到的解释。这里只校验关键几列，不逐字锁死文案。
func TestLogChainTableHeaderHasSubLabels(t *testing.T) {
	if !strings.Contains(pageHTML, `class="lc-th-sub"`) {
		t.Fatal("表头缺少副说明样式")
	}
	// 明细列表头带 id（随范围切换文案），故按 id 断言而非按文案全等——
	// 文案是动态的，钉死字面量会与 syncDetailHeader 冲突。
	for _, want := range []string{
		`<span class="lc-th">渠道 → 上游主域名</span>`,
		`<span class="lc-th" id="lcDetailTh">`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("表头缺少 %q", want)
		}
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
