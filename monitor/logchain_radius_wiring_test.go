package monitor

// 影响面接线测试：把「后端算出来了」与「接口返回了、前端认得出」钉在一起。
//
// 为什么单独一个文件：logchain_radius_test.go 测的是纯计算（给定 rows → 形状），
// 本文件测的是接线（handler 响应里有没有、前端 JS 与 Go 常量是否同步）。
// 两者失效方式不同——计算对但没挂进响应，或挂了但前端不认得那个形状取值，
// 编译和纯计算测试都不会报。

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// logChainGoSource 读 logchain.go 源码。判断「某分支不得带某字段」只能看源码：
// 那条分支要在渠道补全真失败时才走到，构造它需要打断一个内部调用，
// 代价远大于读一次源文件。与 logchain_nostore_test.go 读 server.go 同一做法。
func logChainGoSource(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("logchain.go")
	if err != nil {
		t.Fatalf("读 logchain.go 失败: %v", err)
	}
	return string(b)
}

// TestLogChainRadiusShapeLabelsCoverAllGoConstants 前端 SHAPE_LABEL 必须覆盖
// logchain_radius.go 的全部形状取值。
//
// 漏一个的后果：页面显示原始英文枚举（如 "single_domain"），
// 而运营看不懂那是什么意思。这类缺陷只在恰好命中那个形状时才出现，
// 平时测不到，所以用测试把两边钉住。
func TestLogChainRadiusShapeLabelsCoverAllGoConstants(t *testing.T) {
	shapes := []string{
		radiusSingleChannel, radiusSingleCustomer, radiusSingleDomain,
		radiusSingleModel, radiusWidespread, radiusInsufficient,
	}
	js := string(logChainJS)
	for _, s := range shapes {
		// SHAPE_LABEL 的键是裸标识符（single_channel:'…'），故搜 "取值:" 形式。
		if !strings.Contains(js, s+":'") {
			t.Errorf("前端 SHAPE_LABEL 缺形状 %q，页面会显示原始英文枚举", s)
		}
	}
}

// TestLogChainRadiusDimKeysMatchJSONTags 前端 RADIUS_DIM 用的键必须与
// logChainBlastRadius 的 JSON 标签一致，否则那一维永远读到 undefined、静默不显示。
func TestLogChainRadiusDimKeysMatchJSONTags(t *testing.T) {
	// 从真实结构体序列化取标签，而不是手写字符串——手写的话结构体改了这里不会红。
	dim := logChainRadiusDim{
		Items:      []logChainRadiusItem{{Key: "k", Count: 1, Spread: 1}},
		OtherItems: 2, OtherCount: 3,
	}
	data, err := json.Marshal(logChainBlastRadius{
		Rows:       1,
		ByChannel:  dim,
		ByCustomer: dim,
		ByDomain:   dim,
		ByModel:    dim,
		Shape:      radiusWidespread,
		ShapeWhy:   "why",
	})
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	js := string(logChainJS)
	for _, key := range []string{"by_channel", "by_customer", "by_domain", "by_model"} {
		if _, ok := got[key]; !ok {
			t.Errorf("结构体没有 JSON 标签 %q", key)
		}
		if !strings.Contains(js, "'"+key+"'") {
			t.Errorf("前端 RADIUS_DIM 未引用 %q，该维度不会显示", key)
		}
	}
	// shape 与 shape_why 前端都要读：只给结论不给依据，人无法复核。
	for _, key := range []string{"shape", "shape_why", "rows"} {
		if _, ok := got[key]; !ok {
			t.Errorf("结构体缺 JSON 标签 %q", key)
		}
	}
	if !strings.Contains(js, "shape_why") {
		t.Error("前端未读 shape_why：只显示结论不显示依据，人无法判断该不该相信它")
	}

	// 维度内层三个键：items 是明细，other_items / other_count 是被截断部分。
	// 前端漏读后两个，那张表就会看起来像"问题只涉及这 5 项"。
	inner, ok := got["by_channel"].(map[string]any)
	if !ok {
		t.Fatalf("by_channel 应是对象，got=%T", got["by_channel"])
	}
	for _, key := range []string{"items", "other_items", "other_count"} {
		if _, ok := inner[key]; !ok {
			t.Errorf("维度结构体缺 JSON 标签 %q", key)
		}
		if !strings.Contains(js, key) {
			t.Errorf("前端未读维度字段 %q", key)
		}
	}
}

// TestLogChainRadiusTruncationShownInUI 前端必须画出「其余 N 项」那一行。
//
// 后端算出 OtherItems 但前端不画，等于没修：页面上仍然只有 5 行，
// 而形状结论会说"分散在 12 个渠道"，两者对不上。
func TestLogChainRadiusTruncationShownInUI(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "其余 ") {
		t.Error("前端未画「其余 N 项」行：被截断的维度项在页面上不可见")
	}
	// 必须有 >0 守卫，否则不足上限时会画出一行「其余 0 项」。
	if !strings.Contains(js, "other_items>0") {
		t.Error("前端缺 other_items>0 守卫：未截断时会画出「其余 0 项」噪声行")
	}
	// Spread 一列不得给数字：去重计数跨项相加会重复计数。
	if !strings.Contains(js, "去重计数不可跨项相加") {
		t.Error("「其余」行的 Spread 列应给「—」并说明原因，不能给一个相加得来的假数")
	}
}

// TestLogChainRadiusHiddenAfterPaging 翻页后必须隐藏影响面。
//
// 原因：radius 只覆盖单次请求那一页，而 lc.rows 在「加载更早的记录」时累积。
// 第三页时表格 150 行、影响面只描述 50 行，两个数字摆在同一屏上自相矛盾。
// 这里断言前端确实按 more 分支置 radiusStale，而不是用新页覆盖旧值。
func TestLogChainRadiusHiddenAfterPaging(t *testing.T) {
	js := stripJSLineComments(string(logChainJS))
	if !strings.Contains(js, "radiusStale=true") {
		t.Error("翻页分支未置 radiusStale：影响面会与累积表格对不上")
	}
	// 非翻页分支必须同时复位，否则翻过一次页后永久不再显示影响面。
	if !strings.Contains(js, "radiusStale=false") {
		t.Error("首页分支未复位 radiusStale：翻过一次页后影响面永久消失")
	}
}

// TestLogChainRadiusStatesScopeIsSinglePage 「仅本页」字样必须在收起状态可见。
//
// 统计范围看不见时，单页结论会被当成整个筛选范围的结论——
// 那会让人以为"这个渠道占了全部问题的 80%"，而实际只是本页 50 条里的 80%。
func TestLogChainRadiusStatesScopeIsSinglePage(t *testing.T) {
	js := string(logChainJS)
	if !strings.Contains(js, "仅本页") {
		t.Error("影响面未标注统计范围仅本页")
	}
	// 必须在 summary（收起时可见）里，不能只写在展开区。
	head := js
	if i := strings.Index(js, "lc-radius-head"); i >= 0 {
		end := i + 400
		if end > len(js) {
			end = len(js)
		}
		head = js[i:end]
	}
	if !strings.Contains(head, "仅本页") {
		t.Error("「仅本页」不在 summary 内：收起状态看不到统计范围")
	}
}

// TestLogChainRadiusNotOnEnrichFailurePath 渠道补全失败的响应分支不得带影响面。
//
// 那条分支的 rows 缺渠道名与上游域名，按这两维分组会得出假形状。
// 宁可不给结论也不给错结论。
func TestLogChainRadiusNotOnEnrichFailurePath(t *testing.T) {
	src := logChainGoSource(t)
	idx := strings.Index(src, "channel_enrich_error")
	if idx < 0 {
		t.Fatal("找不到渠道补全失败分支")
	}
	// 取该分支所在的 gin.H 块（前后各一小段），确认其中没有 blast_radius。
	start := idx - 400
	if start < 0 {
		start = 0
	}
	block := src[start : idx+200]
	if strings.Contains(block, "blast_radius") {
		t.Error("渠道补全失败分支带了 blast_radius：渠道/域名维度为空会算出假形状")
	}
}
