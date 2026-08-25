package monitor

// logchain_modality_test.go：多模态「扣费未交付」判定矩阵（验收报告 RB-02 复测用例）。
//
// 背景：原实现只按模型名关键词排除天然无输出的模型，漏掉 dall-e / sora / veo /
// kling / wan / vidu / flux / stable-diffusion 及语音转录等一整批模态，
// 把成功请求误判成"客户付了钱没拿到内容"，运营可能据此错误赔付或投诉上游。
//
// 修复后以 other.request_path 端点白名单为主判据。

import "testing"

type modalityCase struct {
	name     string
	path     string
	model    string
	wantTag  bool
	why      string
}

// modalityCases 七类用例，端点路径全部取自生产实测（近 5 天 197371 行 type=2，
// request_path 填充率 100%，实际只有 /v1/responses、/v1/chat/completions、
// /v1/messages、/pg/chat/completions 四种）。
var modalityCases = []modalityCase{
	{"文本真实未交付", "/v1/chat/completions", "gpt-5.4", true,
		"文本端点且真的零输出，仍须命中——这是本规则存在的理由"},
	{"文本端点_responses", "/v1/responses", "gpt-5.6-sol", true,
		"生产上量最大的端点(156547 行)，同样必须能识别真未交付"},
	{"文本端点_anthropic", "/v1/messages", "claude-opus-5", true,
		"Anthropic 端点也是文本，判据须一致"},

	// —— 以下全部是报告已动态复现的误报，修复后必须不命中 ——
	{"图片_dall-e-3", "/v1/images/generations", "dall-e-3", false,
		"报告已复现的误报：图片成功返回但无文本 token"},
	{"图片_sd", "/v1/images/generations", "stable-diffusion-3.5", false,
		"模型名不含任何旧关键词，只有端点能救它"},
	{"视频_sora-2", "/v1/videos/generations", "sora-2", false,
		"报告已复现的误报：视频成功返回"},
	{"视频_veo", "/v1/videos/generations", "veo-3", false,
		"报告点名遗漏的模型之一"},
	{"视频_kling", "/v1/videos/generations", "kling-v2", false,
		"报告点名遗漏的模型之一"},
	{"音频_转录", "/v1/audio/transcriptions", "whisper-1", false,
		"转录不产出 completion token"},
	{"音频_语音合成", "/v1/audio/speech", "tts-1", false,
		"语音合成同理"},
	{"未知非文本端点", "/v1/some-future-modality", "unknown-model", false,
		"★ 关键：未知端点按不在白名单处理，宁可漏报也不误报"},
	{"路径缺失", "", "gpt-5.4", false,
		"取不到端点时不猜——把未知当文本端点就会重新引入 RB-02"},
}

// TestLogChainBillingUnpaidModalityMatrix 标签侧的模态矩阵。
func TestLogChainBillingUnpaidModalityMatrix(t *testing.T) {
	for _, tc := range modalityCases {
		t.Run(tc.name, func(t *testing.T) {
			row := LogChainRow{
				Type: 2, ModelName: tc.model, RequestPath: tc.path,
				CompletionTokens: 0, EndReason: "eof",
			}
			// quota>0 且 completion_tokens=0：只有端点/模型判据能决定是否算异常。
			tags := logChainAnomalyTags(row, 1000)
			got := false
			for _, tg := range tags {
				if tg == "billing_unpaid" {
					got = true
				}
			}
			if got != tc.wantTag {
				t.Errorf("%s: path=%q model=%q 期望命中=%v 实际=%v\n理由: %s",
					tc.name, tc.path, tc.model, tc.wantTag, got, tc.why)
			}
		})
	}
}


