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
	textInputs := []string{"lcModel", "lcUserID", "lcUsername", "lcTokenName", "lcTokenID", "lcEndpoint", "lcRequestID"}
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
	if !strings.Contains(js, `['lcModel','lcUserID','lcUsername','lcTokenName','lcTokenID','lcEndpoint','lcRequestID'].forEach`) {
		t.Error("排障文本框应统一走回车或查询按钮")
	}
}

func TestLogChainKeywordFilterIsNotExposed(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, forbidden := range []string{`id="lcKeyword"`, `placeholder="错误原文关键词"`} {
		if strings.Contains(pageHTML, forbidden) {
			t.Errorf("客户排障页面仍暴露关键词筛选 %q", forbidden)
		}
	}
	for _, forbidden := range []string{"filters.keyword", "$('lcKeyword')", "q.set('keyword'"} {
		if strings.Contains(js, forbidden) {
			t.Errorf("客户排障前端仍保留关键词筛选接线 %q", forbidden)
		}
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

func TestLogChainMinuteRangeUIWiring(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		`id="lcTimeRange"`,
		`id="lcFromTime" type="time" step="60" value="00:00"`,
		`id="lcToTime" type="time" step="60" value="23:59"`,
		`aria-label="开始时间"`, `aria-label="结束时间"`,
		`id="lcTimeHint" class="lc-time-hint" hidden`,
		`.lc-daybar{display:flex;align-items:center;gap:6px;flex-wrap:wrap}`,
		`background:#f8fafc`, `border:2px solid #60a5fa`, `color-scheme:light`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("分钟范围控件或高对比样式缺少 %q", want)
		}
	}
	for _, want := range []string{
		"fromTime:'00:00'", "toTime:'23:59'", "timeDirty:false",
		"q.set('from_time',lc.fromTime)", "q.set('to_time',lc.toTime)",
		"$('lcFromTime').value=lc.fromTime", "$('lcToTime').value=lc.toTime",
		"$('lcFromTime')?.addEventListener('input',markTimeDirty)",
		"$('lcToTime')?.addEventListener('input',markTimeDirty)",
		"范围已修改，点查询生效",
		"lc.fromTime='00:00';lc.toTime='23:59';lc.timeDirty=false",
		"const fromTime=(c.from_time||'').trim(),toTime=(c.to_time||'').trim()",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("分钟范围前端接线缺少 %q", want)
		}
	}

	// 起止日期必须都能选，且查询范围两端由各自日期决定，不再固定成同一天。
	for _, want := range []string{`id="lcDate"`, `id="lcToDate"`, `aria-label="开始日期"`, `aria-label="结束日期"`} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("起止日期控件缺少 %q", want)
		}
	}
	for _, want := range []string{
		"toDate:''", "function rangeEnd()", "q.set('to',rangeEnd())",
		"$('lcDate')?.addEventListener('input',markTimeDirty)",
		"$('lcToDate')?.addEventListener('input',markTimeDirty)",
		"$('lcToDate').value=rangeEnd()",
		"function shiftRangeDraft(delta)",
		"const toDate=(c.to_date||'').trim()",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("起止日期前端接线缺少 %q", want)
		}
	}
	if strings.Contains(js, "q.set('to',lc.date)") {
		t.Error("结束日期不得再固定为开始日期，跨日范围会被截成单日")
	}

	// 选择时间只更新草稿标记，不得直接查询，也不得改写已应用的查询范围。
	start := strings.Index(js, "const markTimeDirty=()=>{")
	if start < 0 {
		t.Fatal("找不到 markTimeDirty")
	}
	draft := js[start:]
	if end := strings.Index(draft, "\n  };"); end >= 0 {
		draft = draft[:end]
	}
	if strings.Contains(draft, "load(") || strings.Contains(draft, "lc.fromTime=from") ||
		strings.Contains(draft, "lc.toTime=to") {
		t.Error("选择时间只能标记待查询，不得立即查询或覆盖已应用范围")
	}
	// 草稿判断必须覆盖起止日期与起止时间四者，缺一项就会出现“改了但没提示待查询”。
	if !strings.Contains(draft, "lc.timeDirty=from!==lc.date||to!==rangeEnd()||fromTime!==lc.fromTime||toTime!==lc.toTime") ||
		!strings.Contains(draft, "syncTimeDraftUI()") {
		t.Error("范围草稿应覆盖起止日期与时间，并同步待查询提示、按钮和高亮状态")
	}

	// 前后翻页按钮先应用当前草稿，再整段平移起止日期；不能用旧范围覆盖用户刚选的日期。
	if !strings.Contains(js, "if(!shiftRangeDraft(-1))return;") || !strings.Contains(js, "if(!shiftRangeDraft(1))return;") {
		t.Error("前后翻页应从当前草稿整段平移起止日期，而不是从旧范围平移")
	}
	if !strings.Contains(js, "function readTimeDraft()") || !strings.Contains(js, "function applyTimeDraft(") {
		t.Error("日期查询和平移应共用同一套草稿读取与应用逻辑")
	}
	if !strings.Contains(js, "const visibleEnd=lc.timeDirty?($('lcToDate')?.value||rangeEnd()):rangeEnd()") {
		t.Error("后移按钮可用性必须跟随草稿结束日期，不能停留在旧查询范围")
	}
	for _, forbidden := range []string{
		"lc.date=shiftDate(lc.date,-1);lc.fromTime=",
		"lc.date=shiftDate(lc.date,1);lc.fromTime=",
	} {
		if strings.Contains(js, forbidden) {
			t.Errorf("日期切换不应重置分钟范围：仍含 %q", forbidden)
		}
	}
	if !strings.Contains(js, "lc.date=cstToday();lc.toDate=cstToday();lc.timeDirty=false;syncControls();load()") {
		t.Error("点击今天应把范围收回今天、保留已应用分钟范围并清除旧草稿")
	}
	if !strings.Contains(js, "lc.date=cstToday();lc.toDate=cstToday();lc.fromTime='00:00';lc.toTime='23:59';lc.timeDirty=false") {
		t.Error("只有重置按钮应同时恢复今天全天")
	}

	// 查询与前后移必须共用同一套范围校验；非法值不得发请求，合法值先同步 UI 再加载第一页。
	validateStart := strings.Index(js, "function validateTimeDraft(draft){")
	if validateStart < 0 {
		t.Fatal("找不到 validateTimeDraft")
	}
	validate := js[validateStart:]
	if end := strings.Index(validate, "\n}"); end >= 0 {
		validate = validate[:end]
	}
	for _, want := range []string{
		"if(!fromDate||!toDate)", "if(!fromTime||!toTime)", "if(fromDate>toDate)",
		"if(fromDate===toDate&&fromTime>toTime)", "if(fromDate>today||toDate>today)",
	} {
		if !strings.Contains(validate, want) {
			t.Errorf("统一范围校验缺少 %q", want)
		}
	}
	applyStart := strings.Index(js, "const applyText=()=>{")
	if applyStart < 0 {
		t.Fatal("找不到 applyText")
	}
	apply := js[applyStart:]
	if end := strings.Index(apply, "\n  };"); end >= 0 {
		apply = apply[:end]
	}
	if !strings.Contains(apply, "const invalid=validateTimeDraft(draft)") {
		t.Error("查询必须复用统一范围校验")
	}
	applyRange := strings.Index(apply, "applyTimeDraft(draft);")
	applyFilters := strings.Index(apply, "lc.filters.request_id=")
	applySync := strings.Index(apply, "syncControls();")
	applyLoad := strings.Index(apply, "load();")
	if applyRange < 0 || applyFilters <= applyRange || applySync <= applyFilters || applyLoad <= applySync {
		t.Error("点击查询后必须先应用日期和全部筛选，再同步控件，最后重新加载第一页")
	}
	if strings.Contains(apply, "load(true)") {
		t.Error("查询按钮不得按追加分页执行，必须清空旧游标")
	}
}

func TestLogChainDateSummaryKeepsToolbarControlsStable(t *testing.T) {
	const want = `#lcDateLabel{display:inline-block;width:310px;font-variant-numeric:tabular-nums;white-space:nowrap}`
	if !strings.Contains(pageHTML, want) {
		t.Errorf("客户排障日期摘要缺少固定占位样式 %q", want)
	}
}

func TestLogChainDatePickerMatchesTimeStyleWithoutChangingBehavior(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		`.lc-daybar input[type=date]{width:142px;color:#0f172a;background:#f8fafc;border:2px solid #60a5fa;font-weight:700;color-scheme:light}`,
		`.lc-daybar input[type=date]:focus{outline:2px solid #93c5fd;outline-offset:2px;border-color:#2563eb}`,
		`.lc-daybar input[type=date]::-webkit-calendar-picker-indicator{opacity:1;cursor:pointer}`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("日期选择器高对比样式缺少 %q", want)
		}
	}
	// 支持跨日范围后，改日期只能记草稿：只改一端时另一端还没确定，
	// 立即查询会发出中间态的错范围，还白占一次共享查询通道。
	if strings.Contains(js, "$('lcDate')?.addEventListener('change'") {
		t.Error("日期不得再绑 change 立即查询：跨日范围需两端确定后再查")
	}
	if !strings.Contains(js, "$('lcDate')?.addEventListener('input',markTimeDirty)") ||
		!strings.Contains(js, "$('lcToDate')?.addEventListener('input',markTimeDirty)") {
		t.Error("起止日期都应只标记待查询")
	}
	// 未来日期必须在应用范围时拒绝，不能靠控件属性单独兜底。
	if !strings.Contains(js, "不能选择未来日期") {
		t.Error("应用范围时必须拒绝未来日期")
	}
	if !strings.Contains(js, "$('lcDate').max=cstToday()") || !strings.Contains(js, "$('lcToDate').max=cstToday()") {
		t.Error("起止日期控件都应限制不可选未来")
	}
}

func TestLogChainRequestIDFilterWiring(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		`id="lcRequestID"`,
		`placeholder="Request ID（精确）"`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("Request ID 查询入口缺少 %q", want)
		}
	}
	for _, want := range []string{
		"request_id:''",
		"lc.filters.request_id=($('lcRequestID')?.value||'').trim()",
		"q.set('request_id',lc.filters.request_id)",
		"$('lcRequestID').value=lc.filters.request_id",
		"const requestID=(c.request_id||'').trim()",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("Request ID 前端接线缺少 %q", want)
		}
	}
	// Request ID 是独立索引上的精确条件；前端不得把它塞进 keyword 或 token_name。
	if strings.Contains(js, "q.set('keyword',lc.filters.request_id)") ||
		strings.Contains(js, "q.set('token_name',lc.filters.request_id)") {
		t.Error("Request ID 必须只传 request_id 精确参数")
	}
}

func TestLogChainUserAndTokenFiltersAreSeparate(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		`id="lcUserID"`, `placeholder="客户 ID（精确）"`,
		`id="lcTokenName"`, `placeholder="令牌名（模糊）"`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("客户/令牌筛选拆分缺少 %q", want)
		}
	}
	for _, want := range []string{
		"user_id:''", "token_name:''",
		"lc.filters.user_id=($('lcUserID')?.value||'').trim()",
		"lc.filters.token_name=($('lcTokenName')?.value||'').trim()",
		"q.set('user_id',lc.filters.user_id)",
		"q.set('token_name',lc.filters.token_name)",
		"$('lcUserID').value=lc.filters.user_id",
		"$('lcTokenName').value=lc.filters.token_name",
		"const tokenName=(c.token_name||'').trim()",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("客户/令牌筛选接线缺少 %q", want)
		}
	}
	// 不得恢复“纯数字猜客户 ID、其它猜令牌名”的旧行为：它让纯数字令牌名永远查不到。
	for _, forbidden := range []string{
		"if(/^\\d+$/.test(", "const u=lc.filters.user", "lc.filters.user=",
	} {
		if strings.Contains(js, forbidden) {
			t.Errorf("客户 ID 与令牌名必须使用独立状态，不得按字符形状猜测：仍含 %q", forbidden)
		}
	}
}

func TestLogChainUndeliveredUnbilledUIIsExplicit(t *testing.T) {
	js := string(logChainJS)
	for _, want := range []string{
		"undelivered_unbilled:{t:'未交付·未扣费'", "文本请求未交付且未扣费",
		"消费异常记录（logs.content）", "r.content||'文本请求未交付且未扣费'",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("未交付·未扣费展示缺少 %q", want)
		}
	}
	if !strings.Contains(pageHTML, ".lc-tag-unbilled") {
		t.Error("未交付·未扣费标签缺少独立样式")
	}
}

// TestLogChainRequestViewGroupsByRequestIDOnly 请求视图的归并约束。
//
// 最要紧的三条：只按 Request ID 归并、尝试次数不等于日志条数、最终结果取最晚记录。
// 任一条写错都会让页面上的数字比原来更容易误读。
// TestLogChainEdgeTimingNamesObservationDirection 入口计时必须点明观察方向。
//
// Nginx 视角的 "upstream" 是 NewAPI 进程本身，不是模型供应商。写成“上游耗时”
// 会被读成“我方访问供应商用了多久”，据此判断供应商慢是错的。
func TestLogChainEdgeTimingNamesObservationDirection(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"'入口观察总耗时'", "'Nginx 等待 NewAPI 耗时'", "'Nginx→NewAPI 状态序列'",
		"'NewAPI→供应商网络分段','未采集'", "'客户端 DNS/TLS/上传','未采集'",
		"不是模型供应商",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("入口计时说明缺少 %q", want)
		}
	}
	if strings.Contains(js, "['上游耗时'") {
		t.Error("不得再把 Nginx→NewAPI 的等待时间标成“上游耗时”")
	}
}

// TestLogChainConsumeAnomalyOutcomeAvoidsBilledClaim “已记账”会被读成一定扣了钱，
// 但这些异常里包含未交付且未扣费（quota=0）。摘要不能替日志下这个结论。
func TestLogChainConsumeAnomalyOutcomeAvoidsBilledClaim(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	if strings.Contains(js, "最终已记账但有异常") {
		t.Error("不得使用“已记账”：未交付·未扣费并没有扣费")
	}
	if !strings.Contains(js, "最终写入消费日志，但有交付/计费异常") {
		t.Error("消费异常摘要应如实描述为写入消费日志但有交付/计费异常")
	}
}

// TestLogChainEmptyAndFailureStatesAreDistinguished 查不到、被收窄、失败必须分开说。
//
// 三者混同时，最危险的读法是把一次超时或一次被收窄的查询当成“这段时间很正常”。
func TestLogChainEmptyAndFailureStatesAreDistinguished(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"实际查询范围：",
		"未覆盖到的时间段没有被查询",
		"请求可能尚未写入日志",
		"渠道信息补全失败，按渠道或上游域名筛选的结果可能不完整",
		"上游错误证据关联暂不可用，这不代表上游没有报错",
		"这不代表没有客户遇到问题",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("空结果分档缺少 %q", want)
		}
	}
	// 查询失败必须明确“无法判断有没有问题”，不能与空结果同义。
	for _, want := range []string{
		"无法判断这段时间有没有问题",
		"这不等于该范围内没有问题",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("查询失败提示缺少 %q", want)
		}
	}
}

func TestLogChainRequestViewGroupsByRequestIDOnly(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"function groupRequests(rows)",
		"function requestAttempts(g)",
		"function requestOutcome(g)",
		"function requestGroupHTML(g,isLastGroup)",
		"个用户请求 /",
		"次渠道尝试 ·",
		"该请求可能不完整",
		"无 Request ID · 不可关联",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("请求视图缺少 %q", want)
		}
	}
	// 无 Request ID 必须独立成组，绝不按时间/客户/模型猜同一请求。
	if !strings.Contains(js, "unlinkable:true") {
		t.Error("无 Request ID 的记录必须标为不可关联并独立成组")
	}
	for _, forbidden := range []string{"created_at+'@'+r.member", "r.member+'@'", "byTime.set("} {
		if strings.Contains(js, forbidden) {
			t.Errorf("不得按时间或客户猜测归并：仍含 %q", forbidden)
		}
	}
	// 尝试次数按渠道去重，不能直接用日志条数——同一次尝试可能写两条日志。
	if !strings.Contains(js, "seen.add(ch+'@'+(r.created_at||0))") {
		t.Error("尝试次数必须按渠道+时间去重，不能等于日志条数")
	}
	// 最终结果取 (created_at,id) 最大那条：同秒重试只比时间会把旧结果当最终结果。
	for _, want := range []string{"function compareRequestRows(a,b)", "if(at!==bt)return at-bt", "(+a?.id||0)-(+b?.id||0)", "compareRequestRows(b,a)>0"} {
		if !strings.Contains(js, want) {
			t.Errorf("同秒最终结果复合排序缺少 %q", want)
		}
	}
	// 组内每条日志仍独立渲染，折叠的是归属而不是原因。
	if !strings.Contains(js, "g.rows.map(rowHTML).join('')") {
		t.Error("组内必须逐条渲染原始记录，不得把多个原因合成一条")
	}
	// 归属行不再展示 Request ID 明文与“用户请求”前缀，只从客户名开始。
	for _, forbidden := range []string{"lc-req-id", "lc-req-label", "'用户请求'", "esc(g.requestID)"} {
		if strings.Contains(js, forbidden) {
			t.Errorf("请求归属行不得展示 Request ID 明文或“用户请求”前缀：仍含 %q", forbidden)
		}
	}
	// 但归并依据必须仍然只有 Request ID，展示上的精简不能动判读逻辑。
	if !strings.Contains(js, "requestID:rid") || !strings.Contains(js, "requestID:''") {
		t.Error("归并仍必须以 Request ID 为唯一依据")
	}
	for _, want := range []string{".lc-reqhead td", ".lc-req-outcome", ".lc-req-partial"} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("请求组样式缺少 %q", want)
		}
	}
}

func TestLogChainFilterOptionsUseShortRefreshableCache(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"opts:null,optsLoadedAt:0,optsLoading:false",
		"loadFilterOptions();",
		"Date.now()-lc.optsLoadedAt<60000",
		"if(lc.optsLoading)return",
		"lc.optsLoadedAt=Date.now()",
		"cache:'no-store'",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("排障筛选项短缓存契约缺少 %q", want)
		}
	}
}

func TestLogChainTokenIDEndpointStreamFilterWiring(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		`id="lcTokenID"`, `placeholder="令牌 ID（精确）"`,
		`id="lcEndpoint"`, `placeholder="请求端点（精确）"`, `id="lcEndpointList"`,
		`id="lcStream"`, `<option value="true">仅流式</option>`, `<option value="false">仅非流式</option>`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("新增筛选入口缺少 %q", want)
		}
	}
	for _, want := range []string{
		"token_id:''", "endpoint:''", "stream:''",
		"lc.filters.token_id=($('lcTokenID')?.value||'').trim()",
		"lc.filters.endpoint=($('lcEndpoint')?.value||'').trim()",
		"q.set('token_id',lc.filters.token_id)",
		"q.set('endpoint',lc.filters.endpoint)",
		"q.set('stream',lc.filters.stream)",
		"$('lcTokenID').value=lc.filters.token_id",
		"$('lcEndpoint').value=lc.filters.endpoint",
		"$('lcStream').value=lc.filters.stream",
		"lcStream:'stream'",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("新增筛选接线缺少 %q", want)
		}
	}
	// 令牌 ID 与端点是文本框，绑 change 会在失焦时多发一次查询（占用共享通道）。
	for _, ln := range strings.Split(js, "\n") {
		if !strings.Contains(ln, "addEventListener('change'") {
			continue
		}
		for _, id := range []string{"lcTokenID", "lcEndpoint"} {
			if strings.Contains(ln, id) {
				t.Errorf("%s 是文本框，不得绑 change：%s", id, strings.TrimSpace(ln))
			}
		}
	}
}

func TestLogChainAmbiguousUsernameOffersChoice(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"data.username_ambiguous",
		"function showUsernameChoices(",
		"data-lc-pick-user",
		"lc.filters.user_id=btn.dataset.lcPickUser",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("同名客户选择入口缺少 %q", want)
		}
	}
	// 同名冲突时必须清空旧结果：留着上一次的行会让人以为那就是该客户的请求。
	if !strings.Contains(js, "lc.rows=[];lc.hasMore=false;lc.nextBeforeTs=0;lc.nextBeforeID=0;lc.radius=null;lc.radiusStale=false;\n      showUsernameChoices(") {
		t.Error("同名冲突时应清空旧结果与影响面后再要求选择")
	}
	// 不得由前端自动挑一个客户，那等于把“选错客户”的风险藏起来。
	for _, forbidden := range []string{"candidates[0].user_id", "candidates.find("} {
		if strings.Contains(js, forbidden) {
			t.Errorf("前端不得自动选择同名客户：仍含 %q", forbidden)
		}
	}
}

func TestLogChainUsernameFilterWiring(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		`id="lcUsername"`, `placeholder="客户名（精确）"`, `aria-label="客户名"`,
	} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("客户名筛选入口缺少 %q", want)
		}
	}
	for _, want := range []string{
		"username:''", "lc.filters.username=($('lcUsername')?.value||'').trim()",
		"q.set('username',lc.filters.username)", "$('lcUsername').value=lc.filters.username",
		"const username=(c.username||'').trim()", "lc.filters.username!==username",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("客户名筛选接线缺少 %q", want)
		}
	}
	if strings.Contains(js, "q.set('username','%") || strings.Contains(js, "lc.filters.username.includes") {
		t.Error("客户名必须走后端精确等值，不得在前端改成模糊匹配")
	}
}

func TestLogChainFocusedReasonUIWiring(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"br?.mode==='focused'", "REASON_SHAPE_LABEL", "br.reason_shape", "br.reason_why",
		"筛选结果原因", "筛选结果主要原因", "筛选结果涉及范围",
		"这些维度仅说明筛选结果涉及谁，不参与主要原因判定",
		"br.page_has_more?'仅当前页':'已返回全部'",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("筛选原因模式前端接线缺少 %q", want)
		}
	}
	if !strings.Contains(pageHTML, `<script src="/logchain.js?v=15"></script>`) {
		t.Error("logchain.js 行为已变化但缓存版本未提升到 v14")
	}
}

func TestLogChainDiagnosisContextIsFlatValidatedAndCleared(t *testing.T) {
	stJS := stripJSLineComments(string(stabilityJS))
	lcJS := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"stability_from_ts:from", "stability_to_ts:to", "stability_requests:",
		"stability_problems:", "stability_anomaly:", "stability_failed:", "stability_rate:",
	} {
		if !strings.Contains(stJS, want) {
			t.Errorf("稳定性跳转缺扁平原范围指标 %q", want)
		}
	}
	if strings.Contains(stJS, "stability_context:") {
		t.Error("monitorNavigate 只支持一级标量，嵌套 stability_context 会被写成 [object Object]")
	}
	for _, want := range []string{
		"const finite=v=>v!==''", "const count=v=>", "fromTs>0&&toTs>fromTs",
		"problems===anomaly+failed", "problems<=requests", "stability>=0&&stability<=100",
		"lc.diagnosisContext=validContext?", "function clearDiagnosisContext()",
		"clearDiagnosisContext();lc.date=cstToday()", "clearDiagnosisContext();lc.scope=",
		"const markTimeDirty=()=>{\n    clearDiagnosisContext();",
		"if(more){lc.radiusStale=true}", "稳定性原范围", "下方原因分析只覆盖客户排障本次返回结果",
	} {
		if !strings.Contains(lcJS, want) {
			t.Errorf("渠道诊断上下文校验/失效/展示缺少 %q", want)
		}
	}
}

func TestLogChainChannelDiagnosisPresetClearsStaleFilters(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"c.preset==='channel_diagnosis'",
		"channel_id:diagnosisChannel",
		"model:''", "user_id:''", "username:''", "token_name:''", "request_id:''",
		"if(lc.scope!=='err_anom')",
		"if(lc.asc)",
		"if(lc.timeDirty)",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("渠道诊断预设缺少 %q", want)
		}
	}
	if !strings.Contains(js, "^\\d+$") || !strings.Contains(js, "+diagnosisChannel>0") {
		t.Error("渠道诊断预设必须拒绝非法/非正整数渠道 ID")
	}
}

func TestLogChainCustomerDiagnosisPresetClearsStaleFiltersAndForcesReload(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	for _, want := range []string{
		"c.preset==='customer_diagnosis'",
		"user_id:diagnosisUser",
		"group:''", "domain:''", "channel_id:''", "model:''", "username:''",
		"token_name:''", "token_id:''", "endpoint:''", "stream:''", "request_id:''",
		"lc.scope='err_anom'", "lc.date=today", "lc.toDate=today",
		"lc.fromTime='00:00'", "lc.toTime='23:59'", "lc.timeDirty=false", "lc.asc=false",
		"syncControls();\n    return true;",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("客户维护去排障预设缺少 %q", want)
		}
	}
	if !strings.Contains(js, "^\\d+$") || !strings.Contains(js, "+diagnosisUser>0") {
		t.Error("客户维护去排障预设必须拒绝非法/非正整数用户 ID")
	}
}

func TestAlertsUseServerFilteringCursorAndGeneration(t *testing.T) {
	js := stripJSLineComments(string(alertsJS))
	for _, want := range []string{
		"++generation", "AbortController", "gen!==generation", `q.set("reason",fReason)`,
		`q.set("user_id",fUser)`, `q.set("cursor",nextCursor)`, "d.row_total", "d.has_more",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("问题预警缺服务端分页/竞态保护 %q", want)
		}
	}
	if strings.Contains(js, "lastRows.filter(") {
		t.Error("问题预警仍在截断结果上做前端筛选，会漏掉第 2000 行后的匹配项")
	}
}

func TestLogChainRenderDoesNotClearQueryError(t *testing.T) {
	js := string(logChainJS)
	start := strings.Index(js, "function render(){")
	if start < 0 {
		t.Fatal("找不到 render")
	}
	end := strings.Index(js[start:], "\n}")
	if end < 0 {
		t.Fatal("找不到 render 结尾")
	}
	if strings.Contains(js[start:start+end], "clearError()") {
		t.Error("纯重绘不得清除查询失败提示")
	}
	loadStart := strings.Index(js, "async function load(more){")
	fetchAt := strings.Index(js[loadStart:], "await fetch('/logchain/requests?")
	if loadStart < 0 || fetchAt < 0 {
		t.Fatal("找不到 load/fetch")
	}
	if strings.Contains(js[loadStart:loadStart+fetchAt], "clearError()") {
		t.Error("新查询开始时不得清除旧错误并折叠提示框；只能在最新成功响应后清除")
	}
	notOK := strings.Index(js, "if(!r.ok)throw")
	if notOK < 0 {
		t.Fatal("找不到 HTTP 错误分支")
	}
	success := js[notOK:]
	clear := strings.Index(success, "clearError()")
	assign := strings.Index(success, "lc.rows=")
	catch := strings.Index(success, "}catch(e){")
	if clear < 0 || assign < 0 || catch < 0 || clear > assign || clear > catch {
		t.Error("查询错误只能在最新成功响应分支、写入新 rows 之前清除")
	}
}

// stripJSLineComments 去掉 // 行注释，只保留可执行代码。
// 用于"某写法不得出现"这类断言——注释里为解释而引用该写法是正常的，不该判为违规。
// 只处理行注释即可：本文件不用块注释，且不需要处理字符串里的 // （无此用法）。
func stripJSLineComments(js string) string {
	// Windows 工作区以 CRLF 签出，go:embed 会原样保留；先统一行尾，
	// 否则多行行为断言只在 Linux checkout 通过，Windows 交叉编译会误报缺线。
	js = strings.ReplaceAll(js, "\r\n", "\n")
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
	// 默认必须落在 err_anom，让错误日志与消费异常都不会因默认口径而漏掉。
	if !strings.Contains(js, "scope:'err_anom'") {
		t.Error("默认范围应为 err_anom")
	}
	if !strings.Contains(js, "lc.scope='err_anom'") {
		t.Error("重置后应恢复为 err_anom")
	}
	if !strings.Contains(pageHTML, `data-lc-scope="err_anom" class="active"`) {
		t.Error("页面初始高亮应为错误+异常")
	}
	if strings.Contains(pageHTML, `data-lc-scope="error" class="active"`) {
		t.Error("错误按钮不应保留默认高亮")
	}
}

// TestLogChainClientGoneIsSeparateScope 零输出慢断连必须是独立的查看范围。
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
	if !strings.Contains(page, "等待超时后断连") || !strings.Contains(page, "3 秒内取消") {
		t.Error("断连范围必须向用户说明只保留零输出超过 3 秒的异常")
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

	// 现在能拿到 client_gone 标签的只剩零输出慢断连，必须按异常高亮；正常
	// 断连已由后端排除，前端不应再特殊降级。
	if strings.Contains(js, "onlyClientGone") {
		t.Error("异常断连不应继续被前端降级为正常样式")
	}
	// 仍保留独立标签样式，便于与普通流故障区分。
	if !strings.Contains(js, "lc-tag-gone") || !strings.Contains(page, ".lc-tag-gone") {
		t.Error("client_gone 标签应有独立样式（lc-tag-gone）")
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

// CloudWatch 按需证据的前端契约。
//
// 核心是"不点不查"：一旦前端在渲染或加载时自动请求该接口，每次翻页都会
// 产生 AWS 调用与费用，而这一点在页面上看不出来，只会出现在账单里。
func TestLogChainCloudWatchIsOnDemandOnly(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "'/logchain/investigations'") {
		t.Fatal("前端缺少异步排障任务创建接口调用")
	}
	// 必须是 POST：Request ID 不能进 URL、浏览器历史与 access log。
	if !strings.Contains(js, "method:'POST'") {
		t.Error("按需证据必须用 POST，避免 Request ID 进入 URL 与日志")
	}
	if strings.Contains(js, "/logchain/investigations?") {
		t.Error("不得把参数拼进 query")
	}
	for _, want := range []string{"pollCloudWatchInvestigation", "/cancel", "pending_delivery", "source_status", "timeline", "bytes_scanned", "audit_recorded", "candidate_truncated"} {
		if !strings.Contains(js, want) {
			t.Errorf("完整阶段三前端接线缺少 %q", want)
		}
	}
	// 只能由点击触发。load()/render() 里出现该调用即视为自动预取。
	idx := strings.Index(js, "loadCloudWatchEvidence")
	if idx < 0 {
		t.Fatal("缺少按需查询函数")
	}
	if !strings.Contains(js, "data-lc-cw") {
		t.Error("按需查询必须走既有 data-lc-* 事件委托约定")
	}
	// 结果必须按行独立保存，否则展开多行会互相覆盖。
	if !strings.Contains(js, "cwState") {
		t.Error("按需证据结果必须按行 id 独立保存")
	}
	// 换筛选条件后必须清空：行集整体替换时若沿用旧结果，相同行 id 会把
	// 上一次查询的证据显示在新一次查询的行下面，页面上完全看不出来。
	if !strings.Contains(js, "lc.cwState.clear()") {
		t.Error("重新查询时必须清空按需证据结果，避免跨查询错配")
	}
	// 竞态与防连点：迟到响应不得覆盖更新的一次查询。
	if !strings.Contains(js, "cur.seq!==seq") {
		t.Error("缺少按需查询的响应竞态防护")
	}
	// empty 与 unavailable 必须给出不同文案，不能都写成"无数据"。
	for _, want := range []string{"不代表请求没有发生", "数据源不可用", "无权限读取", "被 AWS 限流"} {
		if !strings.Contains(js, want) {
			t.Errorf("缺少来源状态文案 %q", want)
		}
	}
	// 候选关联不得被写成确定结论。
	if !strings.Contains(js, "不能据此定责") {
		t.Error("候选关联必须显式声明不能据此定责")
	}
	// 状态色必须让 empty 与故障可分。
	if !strings.Contains(pageHTML, ".lc-cw b.cw-empty") || !strings.Contains(pageHTML, ".lc-cw b.cw-unavailable") {
		t.Error("缺少区分 empty 与不可用的状态样式")
	}
}

// TestLogChainCloudWatchRequestUsesRequestIDOnly 单条日志已有精确 Request ID 时，
// 不应把历史业务分组/模型再次当作 CloudWatch 过滤条件发送。生产 logs 允许的分组名
// 比 CloudWatch 业务标签闭集更宽，原样转发会让点击排障无故得到“group 不合法”；
// Request ID 查询会在服务端重新取回候选并补齐这些字段。
func TestLogChainCloudWatchRequestUsesRequestIDOnly(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "newapi_request_id:row.request_id") {
		t.Fatal("按单条记录排障必须携带精确 Request ID")
	}
	for _, forbidden := range []string{"group:row.group", "model:row.model_name", "user_id:+row.user_id"} {
		if strings.Contains(js, forbidden) {
			t.Fatalf("精确 Request ID 排障不应转发冗余业务筛选 %q", forbidden)
		}
	}
	if !strings.Contains(js, "safePath") {
		t.Fatal("入口查询路径必须经过边界校验后再发送")
	}
	if !strings.Contains(js, "lc-cw-diagnosis") || !strings.Contains(js, "查看证据与查询详情") {
		t.Fatal("全链路结果必须默认展示原因/位置，并将原始证据收进折叠详情")
	}
}

// TestLogChainCloudWatchDetailReceivesRowID 阶段三状态按日志行 ID 隔离。
// detailHTML 最初直接读取 rowHTML 的局部变量 id，浏览器只有在点击展开行时才会
// 抛 ReferenceError，静态编译和后端测试都发现不了；结果是整个详情区都无法打开。
// 必须显式把 id 作为参数传入，既用于 cwState，也用于 data-lc-cw 事件委托。
func TestLogChainCloudWatchDetailReceivesRowID(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	if !strings.Contains(js, "if(open)html+=detailHTML(r,id)") {
		t.Error("rowHTML 必须把当前行 ID 显式传给 detailHTML")
	}
	if !strings.Contains(js, "function detailHTML(r,id){") {
		t.Error("detailHTML 必须显式接收行 ID，不能读取调用方局部变量")
	}
	if strings.Contains(js, "if(open)html+=detailHTML(r);") || strings.Contains(js, "function detailHTML(r){") {
		t.Error("detailHTML 仍保留会在浏览器展开时报 ReferenceError 的旧签名")
	}
}

// TestLogChainBlindSpotsDropsBodyCapture 第三条"从不采集请求/响应正文"已删。
// 加入 end_reason / end_error 后，"回答只出一半就断了"已能回答；
// 剩下真正答不了的是"内容写得不对"，那属内容审查、不是排障范畴。
func TestLogChainBlindSpotsDropsBodyCapture(t *testing.T) {
	// 关闭 CloudWatch 按需证据时的口径：前置拒绝仍只在问题预警可见。
	spots := logChainBlindSpots(false)
	if len(spots) != 2 {
		t.Fatalf("盲区应为 2 条，实际 %d 条: %v", len(spots), spots)
	}
	for _, s := range spots {
		if strings.Contains(s, "请求/响应正文") {
			t.Error("第三条应已删除")
		}
	}
	// 两条能力边界必须留：前置拒绝不写 logs、重试链无法归并，都仍然成立。
	joined := strings.Join(spots, "\n")
	for _, want := range []string{"未到达渠道即被拒", "问题预警", "客户 ID", "归并"} {
		if !strings.Contains(joined, want) {
			t.Errorf("能力边界缺少关键说明 %q: %v", want, spots)
		}
	}
	// 问题预警已有分钟明细与 user_id，旧事实不得回归。
	for _, stale := range []string{"stability_reject_hours", "该表无 user_id", "无法定位到具体客户"} {
		if strings.Contains(joined, stale) {
			t.Errorf("能力边界仍有过时事实 %q: %v", stale, spots)
		}
	}
	js := string(logChainJS)
	for _, want := range []string{"本页能力边界", "前往问题预警", "monitorNavigate('alerts')"} {
		if !strings.Contains(js, want) {
			t.Errorf("能力边界前端缺少 %q", want)
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
