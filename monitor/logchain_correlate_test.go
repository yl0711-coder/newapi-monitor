package monitor

// 上下游关联的约束。
//
// 最要紧的是**置信度不能串档**：把 probable 当 exact 给出去，人会拿推断当证据，
// 照着别的请求的上游日志去解释眼前的故障。所以这里逐档钉死。

import (
	"context"
	"strings"
	"testing"
)

// upRow 造一条上游错误日志。
func upRow(domain string, id int64, joinKey, model string, sc int64, ts int64) ChannelUpstreamErrorLog {
	return ChannelUpstreamErrorLog{
		Domain: domain, UpstreamID: id, JoinKey: joinKey,
		ModelName: model, StatusCode: sc, CreatedAt: ts,
		UpstreamChannelName: "up_ch_" + model,
		ErrorCode:           "unknown_error",
		ErrorType:           "openai_error",
		Content:             "status_code=" + itoa(sc) + ", upstream said something",
		UseTime:             7,
	}
}

// ourRow 造一条我方错误行。
func ourRow(id int64, content, model, domain string, sc int, ts int64) LogChainRow {
	return LogChainRow{
		ID: id, Type: 5, Content: content, ModelName: model,
		UpstreamDomain: domain, UpstreamStatusCode: sc, CreatedAt: ts,
	}
}

// TestCorrelateExactByJoinKey ★ 最要紧的一条 ★
// 双方 content 嵌同一个模型商 id → exact。
func TestCorrelateExactByJoinKey(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UpstreamErrorLogSyncEnabled = true
	key := "202608280208089288070118268d9d6WhLijC4B"
	if err := m.storeDB.Create(&[]ChannelUpstreamErrorLog{
		upRow("kpzhu.com", 1, key, "gpt-5.5", 503, 1000),
	}).Error; err != nil {
		t.Fatal(err)
	}

	rows := []LogChainRow{
		ourRow(9, "status_code=503, no available upstream (request id: "+key+")",
			"gpt-5.5", "kpzhu.com", 503, 1000),
	}
	got := m.correlateUpstreamErrors(context.Background(), rows)
	mt, ok := got[9]
	if !ok {
		t.Fatal("串联键相等却没匹配上")
	}
	if mt.Confidence != correlateExact {
		t.Errorf("应为 exact: got=%s why=%s", mt.Confidence, mt.Why)
	}
	if mt.UpstreamChannelName != "up_ch_gpt-5.5" {
		t.Errorf("未带回上游渠道名: %+v", mt)
	}
	if mt.UpstreamErrorCode != "unknown_error" {
		t.Errorf("未带回上游 error_code: %+v", mt)
	}
}

// TestCorrelateJoinKeyIgnoresDomainAndTime 精确键不受域名与时间影响。
//
// 串联键是模型商全局唯一的 id，加域名或时间条件只会在渠道快照对不上、
// 或两侧时钟偏差大时误杀。键相等本身就是足够强的证据。
func TestCorrelateJoinKeyIgnoresDomainAndTime(t *testing.T) {
	m := newTestMonitor(t)
	key := "req_1787652519221_1a03fa99"
	if err := m.storeDB.Create(&[]ChannelUpstreamErrorLog{
		// 域名不同、时间差一小时
		upRow("other.example", 2, key, "gpt-5.5", 503, 1000),
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []LogChainRow{
		ourRow(11, "status_code=503, x (request_id: "+key+")", "gpt-5.5", "kpzhu.com", 503, 1000+3600),
	}
	got := m.correlateUpstreamErrors(context.Background(), rows)
	if got[11].Confidence != correlateExact {
		t.Errorf("键相等就该 exact，不该被域名/时间否掉: %+v", got[11])
	}
}

// TestCorrelateFallbackUniqueIsProbableNotExact 回退唯一命中只能给 probable。
//
// ★ 这条防的是最危险的错误 ★
// 回退键是「同模型 + 同状态码 + 时间相近」，实测 10 秒窗内 82% 唯一——
// 那 18% 就是会认错的部分。标成 exact 会让人拿别的请求的上游日志下结论。
func TestCorrelateFallbackUniqueIsProbableNotExact(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.storeDB.Create(&[]ChannelUpstreamErrorLog{
		upRow("kpzhu.com", 3, "", "gpt-5.5", 503, 1000),
	}).Error; err != nil {
		t.Fatal(err)
	}
	// 我方这条没有串联键（超时类错误就是这样）
	rows := []LogChainRow{
		ourRow(12, "status_code=503, no request id here", "gpt-5.5", "kpzhu.com", 503, 1003),
	}
	got := m.correlateUpstreamErrors(context.Background(), rows)
	mt := got[12]
	if mt.Confidence == correlateExact {
		t.Fatal("回退匹配绝不能标 exact——那是把推断当证据")
	}
	if mt.Confidence != correlateProbable {
		t.Errorf("唯一命中应为 probable: got=%s why=%s", mt.Confidence, mt.Why)
	}
	if !strings.Contains(mt.Why, "非精确") {
		t.Errorf("依据必须说明这不是精确匹配: %s", mt.Why)
	}
}

// TestCorrelateFallbackMultipleIsAmbiguous 多候选必须标 ambiguous 且不给具体那条。
func TestCorrelateFallbackMultipleIsAmbiguous(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.storeDB.Create(&[]ChannelUpstreamErrorLog{
		upRow("kpzhu.com", 4, "", "gpt-5.5", 503, 1000),
		upRow("kpzhu.com", 5, "", "gpt-5.5", 503, 1002),
		upRow("kpzhu.com", 6, "", "gpt-5.5", 503, 1004),
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []LogChainRow{ourRow(13, "status_code=503, x", "gpt-5.5", "kpzhu.com", 503, 1002)}
	got := m.correlateUpstreamErrors(context.Background(), rows)
	mt := got[13]
	if mt.Confidence != correlateAmbiguous {
		t.Fatalf("多候选应为 ambiguous: got=%s", mt.Confidence)
	}
	if mt.CandidateCount != 3 {
		t.Errorf("应报候选条数 3，让人知道模糊到什么程度: got=%d", mt.CandidateCount)
	}
	// 绝不能带上某一条的内容——带了就是在猜。
	if mt.UpstreamContent != "" || mt.UpstreamChannelName != "" {
		t.Errorf("ambiguous 不得给出具体某条: %+v", mt)
	}
}

// TestCorrelateSkipsNonErrorAndMissingDomain 正常行与无域名行不参与关联。
func TestCorrelateSkipsNonErrorAndMissingDomain(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.storeDB.Create(&[]ChannelUpstreamErrorLog{
		upRow("kpzhu.com", 7, "k7", "gpt-5.5", 503, 1000),
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []LogChainRow{
		// type=2 正常消费行，即使 content 里有 key 也不该关联
		{ID: 21, Type: 2, Content: "x (request id: k7)", ModelName: "gpt-5.5",
			UpstreamDomain: "kpzhu.com", CreatedAt: 1000},
		// 错误行但渠道快照缺失（无上游域名）
		{ID: 22, Type: 5, Content: "x (request id: k7)", ModelName: "gpt-5.5",
			UpstreamDomain: "", CreatedAt: 1000},
	}
	got := m.correlateUpstreamErrors(context.Background(), rows)
	if _, ok := got[21]; ok {
		t.Error("正常消费行不该参与关联")
	}
	if _, ok := got[22]; ok {
		t.Error("无上游域名的行无从查起，不该参与关联")
	}
}

// TestCorrelateUIDistinguishesConfidence ★ 前端最要紧的一条 ★
//
// exact 与 probable 在页面上必须视觉可分。混在一起会让人拿推断当证据，
// 照着别的请求的上游日志解释眼前的故障——比没有关联更糟。
func TestCorrelateUIDistinguishesConfidence(t *testing.T) {
	js := string(logChainJS)
	for _, want := range []string{
		"CORRELATE_LABEL", "lc-corr-exact", "lc-corr-probable", "lc-corr-ambiguous",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("前端缺 %q", want)
		}
	}
	// probable 的提示必须写明可能认错，否则会被当铁证用。
	if !strings.Contains(js, "可能认错") {
		t.Error("probable 的提示未说明可能认错：人会把推断当证据")
	}
	// ambiguous 只报候选数，绝不展示具体某条——展示了就是在猜哪一条。
	ambIdx := strings.Index(js, "um.confidence==='ambiguous'")
	if ambIdx < 0 {
		t.Fatal("前端未对 ambiguous 单独分支处理")
	}
	tail := js[ambIdx:]
	if end := strings.Index(tail, "}else{"); end > 0 {
		if strings.Contains(tail[:end], "upstream_content") {
			t.Error("ambiguous 分支展示了上游原文：那是在猜哪一条")
		}
	} else {
		t.Error("ambiguous 分支没有 else 兜住其余档位")
	}
	// 表格里要有标记，不展开也能看出有上游数据。
	if !strings.Contains(js, "lc-corr-dot") {
		t.Error("表格缺关联标记")
	}
	// CSS 缺了色块会退化成无样式文本，四档看起来一模一样。
	css := pageHTML
	for _, want := range []string{
		".lc-corr{", ".lc-corr-exact{", ".lc-corr-probable{",
		".lc-corr-ambiguous{", ".lc-corr-dot{",
	} {
		if !strings.Contains(css, want) {
			t.Errorf("page.html 缺样式 %q", want)
		}
	}
}

// TestAddHTMLOnlyUsedForFixedFragments addHTML 不得用于接口返回的字段值。
//
// 它不转义 value，而上游错误原文里可能带 < > 之类字符——传进去就是 XSS 面。
func TestAddHTMLOnlyUsedForFixedFragments(t *testing.T) {
	js := string(logChainJS)
	for idx := 0; ; {
		i := strings.Index(js[idx:], "addHTML(")
		if i < 0 {
			break
		}
		start := idx + i
		end := strings.Index(js[start:], "\n")
		if end < 0 {
			end = len(js) - start
		}
		line := js[start : start+end]
		for _, bad := range []string{"upstream_content", "r.content", "um.why", "upstream_channel_name"} {
			if strings.Contains(line, bad) {
				t.Errorf("addHTML 收了未转义的接口字段 %q:\n  %s", bad, line)
			}
		}
		idx = start + 8
	}
}

// TestCorrelateUIHidesRedundantFields 与我方相同的字段不该无条件显示。
//
// ★ 这条修的是我一个实测证伪的设计 ★
// 2026-08-28 实测 33 条 exact 匹配：状态码/错误码/错误类型/原文两侧**全部逐字相同**。
// 机制是上游把返回给我方的响应体原样记进自己日志，我方也原样记进 content。
// 无条件显示等于让人把同一句话读两遍，还会误以为拿到了上游的内部诊断。
//
// 但不一致时必须显示——那说明上游记的与它告诉我方的不是一回事。
func TestCorrelateUIHidesRedundantFields(t *testing.T) {
	js := string(logChainJS)
	// 三处差异判断都必须在：无条件显示就是让人读两遍同一句话。
	for _, want := range []string{
		"um.upstream_status_code!==r.upstream_status_code",
		"um.upstream_error_code!==(r.upstream_error_code",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("缺差异判断 %q：该字段会被无条件显示", want)
		}
	}
	// 标题里要写明"与我方不一致"，否则人不知道为什么这次显示了。
	if !strings.Contains(js, "与我方不一致") {
		t.Error("差异字段的标题未标明不一致")
	}

	// ★ 上游原文用折叠块，不能"相同就隐藏" ★
	// 一行凭空消失与「压根没拿到上游日志」无法区分，那正是
	// docs/aimustkonw.md「缺失绝不显示为零」要防的。所以：
	//   相同 → 折叠，标题写「已逐字核对，与我方一致」
	//   不同 → 折叠但标警告色，标题写「与我方不一致」
	if !strings.Contains(js, "lc-upraw") {
		t.Error("上游侧原文缺独立折叠块")
	}
	if !strings.Contains(js, "已逐字核对") {
		t.Error("两侧一致时未说明「已核对过」：那会让人以为没拿到上游日志")
	}
	if !strings.Contains(js, "lc-upraw-diff") {
		t.Error("不一致时缺警告样式")
	}
	for _, want := range []string{".lc-upraw{", ".lc-upraw-head{", ".lc-upraw-diff"} {
		if !strings.Contains(pageHTML, want) {
			t.Errorf("page.html 缺样式 %q", want)
		}
	}
	if !strings.Contains(js, "候选涉及渠道") {
		t.Error("ambiguous 档未给出候选涉及的上游渠道：那是该档唯一有信息量的东西")
	}
}

// TestCorrelateCDNStatusIsNotApplicable ★ 本组新增最要紧的一条 ★
//
// CDN 系状态码（520~526）必须判 not_applicable，不能判 none。
//
// 实测依据：我方 08-26 23:52 那条 524（渠道 #66 kpzhu）在上游 507 条日志里
// 找不到对应，而上游的状态码分布是 503×542 / 404×230 / 429×74 / 500×48 /
// 502×44 / 403×20 / 504×10 / 400×4 —— **524 一条都没有**。
// 524 由 Cloudflare 在上游应用之前返回，上游从未看到这次请求。
//
// 判成 none 会让人反复去核对采集，而那趟必然白跑。
func TestCorrelateCDNStatusIsNotApplicable(t *testing.T) {
	m := newTestMonitor(t)
	// 上游日志里有同模型的 503，但没有 524——与实测一致。
	if err := m.storeDB.Create(&[]ChannelUpstreamErrorLog{
		upRow("kpzhu.com", 30, "", "gpt-5.5", 503, 1000),
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []LogChainRow{
		ourRow(40, "status_code=524, The origin web server did not respond",
			"gpt-5.5", "kpzhu.com", 524, 1000),
		ourRow(41, "status_code=502, bad gateway", "gpt-5.5", "kpzhu.com", 502, 5000),
	}
	got := m.correlateUpstreamErrors(context.Background(), rows)

	cdn := got[40]
	if cdn.Confidence != correlateNotApplicable {
		t.Errorf("524 应判 not_applicable: got=%s why=%s", cdn.Confidence, cdn.Why)
	}
	if !strings.Contains(cdn.Why, "CDN") || !strings.Contains(cdn.Why, "不是采集缺失") {
		t.Errorf("依据须说明是 CDN 产生且非采集缺失: %s", cdn.Why)
	}
	// 502 是标准 HTTP，上游自己也会记（实测记了 44 条），找不到时只能是 none。
	if standard := got[41]; standard.Confidence != correlateNone {
		t.Errorf("502 找不到时应为 none: got=%s", standard.Confidence)
	}
}

// TestCorrelateGatedByCollectionSwitch 采集关闭时接线处不该调关联。
//
// 表必然是空的，白查两次只会加到用户等待上。读源码验证守卫存在——
// 构造真实 HTTP 请求要连生产库，代价远大于读一次文件。
func TestCorrelateGatedByCollectionSwitch(t *testing.T) {
	src, err := readMonitorSource("logchain.go")
	if err != nil {
		t.Fatal(err)
	}
	idx := strings.Index(src, "m.correlateUpstreamErrors(ctx, rows)")
	if idx < 0 {
		t.Fatal("关联层未接进请求处理")
	}
	start := idx - 400
	if start < 0 {
		start = 0
	}
	if !strings.Contains(src[start:idx], "m.cfg.UpstreamErrorLogSyncEnabled") {
		t.Error("关联调用缺采集开关守卫：采集关闭时会白查两次空表")
	}
	// 必须在 attachChannelSnaps 之后：关联要用 UpstreamDomain。
	snapIdx := strings.Index(src, "m.attachChannelSnaps(ctx, rows)")
	if snapIdx < 0 || snapIdx > idx {
		t.Error("关联必须放在 attachChannelSnaps 之后，否则 UpstreamDomain 是空的")
	}
}
