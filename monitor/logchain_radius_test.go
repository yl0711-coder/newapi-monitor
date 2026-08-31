package monitor

// logchain_radius_test.go：影响面判别的行为约束。
//
// 最要紧的一条是「长尾渠道混淆」：某渠道只有一个客户在用时，它的错误天然
// 全部来自那一个客户，形状上**无法**区分「渠道坏了」与「那个客户在做异常请求」。
// 这不是假想——2026-08-24 生产实测里渠道 16 的 26 条错误全部来自客户 130，
// 正是这种情形。硬判成「渠道问题」会让人去找上游，而问题可能在客户侧。

import (
	"strings"
	"testing"
)

// radiusRow 造一行问题记录。type=5 即错误行，会计入影响面统计。
func radiusRow(chanID int64, chanName, member, domain, model string) LogChainRow {
	return LogChainRow{
		Type: 5, ChannelID: chanID, ChannelName: chanName,
		Member: member, UpstreamDomain: domain, ModelName: model,
	}
}

// repeatRows 复制同一行 n 次，便于构造集中度。
func repeatRows(n int, r LogChainRow) []LogChainRow {
	out := make([]LogChainRow, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, r)
	}
	return out
}

// TestLogChainRadiusLongTailChannelIsNotJudged ★ 本文件最要紧的一条 ★
//
// 长尾渠道：错误全集中在一个渠道，但该渠道只有一个客户在用。
// 生产实测原型：2026-08-24 渠道 16 的 26 条错误全部来自客户 130。
// 此时形状**没有判别力**，必须如实说"判不了"，不能判成渠道问题——
// 那会让人去找上游客服，而问题可能在这个客户自己的请求上。
func TestLogChainRadiusLongTailChannelIsNotJudged(t *testing.T) {
	rows := repeatRows(10, radiusRow(16, "solo-chan", "客户130", "solo.example", "gpt-4o"))

	br := computeLogChainBlastRadius(rows)

	if br.Shape != radiusInsufficient {
		t.Errorf("长尾渠道必须判为 insufficient，got=%s（依据: %s）", br.Shape, br.ShapeWhy)
	}
	if !strings.Contains(br.ShapeWhy, "1 个客户") {
		t.Errorf("依据必须点明只有 1 个客户在用: %s", br.ShapeWhy)
	}
	// 依据里要指路，不能只说"判不了"。
	if !strings.Contains(br.ShapeWhy, "逐行") {
		t.Errorf("判不了时应引导去看逐行责任方与原文: %s", br.ShapeWhy)
	}
}

// TestLogChainRadiusSingleChannelMultiCustomer 单渠道集中且跨多客户 → 渠道/上游问题。
// 这是最有行动价值的结论：多客户同时中招，不可能都是客户自己的问题。
// 生产实测原型：渠道 52 的 15 条错误影响 3 个客户，全是 502。
func TestLogChainRadiusSingleChannelMultiCustomer(t *testing.T) {
	var rows []LogChainRow
	rows = append(rows, repeatRows(4, radiusRow(52, "codeyu", "客户A", "codeyu.shop", "gpt-5.4"))...)
	rows = append(rows, repeatRows(4, radiusRow(52, "codeyu", "客户B", "codeyu.shop", "gpt-5.4"))...)
	rows = append(rows, repeatRows(3, radiusRow(52, "codeyu", "客户C", "codeyu.shop", "gpt-5.4"))...)

	br := computeLogChainBlastRadius(rows)

	if br.Shape != radiusSingleChannel {
		t.Errorf("单渠道多客户应判 single_channel，got=%s（依据: %s）", br.Shape, br.ShapeWhy)
	}
	if !strings.Contains(br.ShapeWhy, "3 个客户") {
		t.Errorf("依据须给出受影响客户数（复核的关键）: %s", br.ShapeWhy)
	}
	if br.ByChannel.Items[0].Spread != 3 {
		t.Errorf("渠道维度的 Spread 应是客户数 3，got=%d", br.ByChannel.Items[0].Spread)
	}
}

// TestLogChainRadiusSingleCustomerAcrossChannels 单客户集中且跨多渠道 → 客户侧问题。
// 换渠道仍失败，说明不是某个上游的问题。
// 生产实测原型：客户 1 的 17 条错误跨 8 个渠道，其中 8 条是 429。
func TestLogChainRadiusSingleCustomerAcrossChannels(t *testing.T) {
	var rows []LogChainRow
	for i, ch := range []int64{31, 32, 33, 46, 52} {
		name := "chan" + lcNum(i)
		rows = append(rows, repeatRows(2, radiusRow(ch, name, "客户X", "d"+lcNum(i)+".example", "gpt-4o"))...)
	}

	br := computeLogChainBlastRadius(rows)

	if br.Shape != radiusSingleCustomer {
		t.Errorf("单客户跨多渠道应判 single_customer，got=%s（依据: %s）", br.Shape, br.ShapeWhy)
	}
	if !strings.Contains(br.ShapeWhy, "5 个渠道") {
		t.Errorf("依据须给出涉及渠道数: %s", br.ShapeWhy)
	}
}

// TestLogChainRadiusInsufficientSample 样本太少不判。
// 两三条上的"集中度"是偶然，硬给结论会把偶发当规律。
func TestLogChainRadiusInsufficientSample(t *testing.T) {
	rows := repeatRows(3, radiusRow(52, "c", "客户A", "d.example", "m"))

	br := computeLogChainBlastRadius(rows)

	if br.Shape != radiusInsufficient {
		t.Errorf("少于 %d 条不应判读形状，got=%s", logChainRadiusMinRows, br.Shape)
	}
	if !strings.Contains(br.ShapeWhy, "3 条") {
		t.Errorf("依据须给出实际条数: %s", br.ShapeWhy)
	}
}

// TestLogChainRadiusWidespread 分散时如实说分散，并提示可能混了多个故障。
func TestLogChainRadiusWidespread(t *testing.T) {
	var rows []LogChainRow
	for i := 0; i < 8; i++ {
		s := lcNum(i)
		rows = append(rows, radiusRow(int64(30+i), "chan"+s, "客户"+s, "d"+s+".example", "model"+s))
	}

	br := computeLogChainBlastRadius(rows)

	if br.Shape != radiusWidespread {
		t.Errorf("无集中应判 widespread，got=%s（依据: %s）", br.Shape, br.ShapeWhy)
	}
	// 必须提示"可能混了多个互不相关的故障"——否则人会以为是一个全局故障。
	if !strings.Contains(br.ShapeWhy, "互不相关") {
		t.Errorf("分散时须提示可能混了多个故障: %s", br.ShapeWhy)
	}
}

// TestLogChainRadiusIgnoresNormalRows 正常消费行不参与统计。
// 把它们算进来会稀释集中度，让所有形状都变成 widespread。
func TestLogChainRadiusIgnoresNormalRows(t *testing.T) {
	var rows []LogChainRow
	// 6 条真错误，集中在一个渠道的两个客户上。
	rows = append(rows, repeatRows(3, radiusRow(52, "c", "客户A", "d.example", "m"))...)
	rows = append(rows, repeatRows(3, radiusRow(52, "c", "客户B", "d.example", "m"))...)
	// 90 条正常消费行（type=2 且无异常标签），分散在别的渠道上。
	for i := 0; i < 90; i++ {
		rows = append(rows, LogChainRow{
			Type: 2, ChannelID: int64(100 + i%9), ChannelName: "normal",
			Member: "客户" + lcNum(i%9), UpstreamDomain: "n.example", ModelName: "m",
		})
	}

	br := computeLogChainBlastRadius(rows)

	if br.Rows != 6 {
		t.Errorf("只应统计 6 条问题行，got=%d（正常行被算进来了）", br.Rows)
	}
	if br.Shape != radiusSingleChannel {
		t.Errorf("正常行不该稀释集中度，应仍判 single_channel，got=%s", br.Shape)
	}
}

// TestLogChainRadiusTruncationIsReported 超过上限的项必须以「其余」汇总告知。
//
// 为什么这条重要：形状判读用全量 map（chanCount = len(m)，不受截断影响），
// 而明细表只有 5 行。若不报截断，就会出现「结论说分散在 8 个渠道，
// 表里只有 5 行」——人会以为表坏了，或者以为问题只涉及这 5 个渠道。
//
// 造 8 个渠道、条数 8/7/6/5/4/3/2/1（共 36 条），故 Top5 覆盖 30 条，
// 其余 3 项共 6 条。每个渠道给多个客户，避免命中长尾渠道保护。
func TestLogChainRadiusTruncationIsReported(t *testing.T) {
	var rows []LogChainRow
	counts := []int{8, 7, 6, 5, 4, 3, 2, 1}
	for i, n := range counts {
		s := lcNum(i)
		for j := 0; j < n; j++ {
			// 客户在渠道内轮转，保证每个渠道跨多客户；模型与域名随渠道走，
			// 否则单模型 100% 集中会把形状判成 single_model。
			rows = append(rows, radiusRow(int64(30+i), "chan"+s,
				"客户"+lcNum(j%3), "d"+s+".example", "model"+s))
		}
	}

	br := computeLogChainBlastRadius(rows)

	if br.Rows != 36 {
		t.Fatalf("问题行数应为 36，got=%d", br.Rows)
	}
	if len(br.ByChannel.Items) != logChainRadiusMaxItems {
		t.Errorf("渠道维度应截到 %d 项，got=%d", logChainRadiusMaxItems, len(br.ByChannel.Items))
	}
	if br.ByChannel.OtherItems != 3 {
		t.Errorf("应报其余 3 项，got=%d", br.ByChannel.OtherItems)
	}
	if br.ByChannel.OtherCount != 6 {
		t.Errorf("其余项共 6 条，got=%d", br.ByChannel.OtherCount)
	}

	// ★ 不丢行不变量 ★：每个维度的 Top-N 条数 + 其余条数 必须等于问题总行数。
	// 这条不成立就说明有行被静默吞掉了，而那正是本次要修的问题。
	for _, d := range []struct {
		name string
		dim  logChainRadiusDim
	}{
		{"渠道", br.ByChannel}, {"客户", br.ByCustomer},
		{"上游域名", br.ByDomain}, {"模型", br.ByModel},
	} {
		sum := d.dim.OtherCount
		for _, it := range d.dim.Items {
			sum += it.Count
		}
		if sum != br.Rows {
			t.Errorf("%s维度条数合计 %d ≠ 问题总行数 %d：有行被静默丢弃",
				d.name, sum, br.Rows)
		}
	}
}

// TestLogChainRadiusNoTruncationLeavesZero 不足上限时不得留下残值。
// OtherItems 非 0 会让前端画出一行「其余 0 项」，是纯噪声。
//
// 恰好取 5 个渠道（等于上限）而不是更少：截断条件写成 >= 还是 > 只在边界上
// 才看得出差别，5 是唯一能钉住这一点的取值。每渠道 2 条以满足最小样本数。
func TestLogChainRadiusNoTruncationLeavesZero(t *testing.T) {
	var rows []LogChainRow
	for i := 0; i < logChainRadiusMaxItems; i++ {
		s := lcNum(i)
		rows = append(rows, repeatRows(2, radiusRow(int64(30+i), "chan"+s,
			"客户"+s, "d"+s+".example", "model"+s))...)
	}

	br := computeLogChainBlastRadius(rows)

	if len(br.ByChannel.Items) != logChainRadiusMaxItems {
		t.Fatalf("应保留全部 %d 项，got=%d", logChainRadiusMaxItems, len(br.ByChannel.Items))
	}
	if br.ByChannel.OtherItems != 0 || br.ByChannel.OtherCount != 0 {
		t.Errorf("渠道数恰好等于上限 %d，不应有其余项：items=%d count=%d",
			logChainRadiusMaxItems, br.ByChannel.OtherItems, br.ByChannel.OtherCount)
	}
}

// TestLogChainRadiusOutputIsStable 输出必须稳定可测：
// 同数量的项按 key 升序，否则 map 遍历顺序会让结果每次不同。
func TestLogChainRadiusOutputIsStable(t *testing.T) {
	var rows []LogChainRow
	for _, ch := range []int64{31, 32, 33, 34, 35, 36} {
		rows = append(rows, repeatRows(2, radiusRow(ch, "chan", "客户"+lcNum64(ch), "d.example", "m"))...)
	}

	first := computeLogChainBlastRadius(rows)
	for i := 0; i < 5; i++ {
		again := computeLogChainBlastRadius(rows)
		if again.ByChannel.Items[0].Key != first.ByChannel.Items[0].Key {
			t.Fatalf("输出不稳定：第 %d 次 ByChannel[0]=%s，首次=%s",
				i+2, again.ByChannel.Items[0].Key, first.ByChannel.Items[0].Key)
		}
	}
	// 维度项数受上限约束，不能无限长。
	if len(first.ByChannel.Items) > logChainRadiusMaxItems {
		t.Errorf("ByChannel 超过上限 %d，got=%d", logChainRadiusMaxItems, len(first.ByChannel.Items))
	}
	if first.Rows != 12 {
		t.Errorf("Rows 应是问题行数 12，got=%d", first.Rows)
	}
}
