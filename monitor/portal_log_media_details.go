package monitor

// Media fields are evidence only. Their counts may overlap the log's prompt /
// completion totals. Do not sum them, derive missing counts or use current rates.
func portalMediaDetailItems(e portalLogEvidence) []portalLogDetailItem {
	if e.State != portalEvidenceValid {
		return nil
	}
	var items []portalLogDetailItem
	add := func(key, label string, expected bool) {
		f := e.Numbers[key]
		if f.State == portalEvidenceMissing && !expected {
			return
		}
		value := f.Text
		switch f.State {
		case portalEvidenceMissing, portalEvidenceNull:
			value = "未记录"
		case portalEvidenceValid:
		default:
			value = "记录异常"
		}
		items = append(items, portalLogDetailItem{Label: label, Value: value, State: f.State})
	}
	// Both GenerateAudioOtherInfo and GenerateWssOtherInfo emit all four
	// counts, including explicit zero. Missing fields must not become zero.
	audio := e.Flags["audio"].Text == "true" || e.Flags["ws"].Text == "true"
	for _, field := range []struct{ key, label string }{
		{"text_input", "文本输入 Tokens（分项原值）"},
		{"text_output", "文本输出 Tokens（分项原值）"},
		{"audio_input", "音频输入 Tokens（分项原值）"},
		{"audio_output", "音频输出 Tokens（分项原值）"},
		{"audio_ratio", "音频输入倍率（日志原值，非单价）"},
		{"audio_completion_ratio", "音频输出倍率（日志原值，非单价）"},
	} {
		add(field.key, field.label, audio)
	}
	separate := e.Flags["audio_input_seperate_price"].Text == "true"
	add("audio_input_token_count", "独立计价音频输入 Tokens（分项原值）", separate)
	add("audio_input_price", "独立音频输入单价（$/百万 Tokens，未乘分组倍率）", separate)
	image := e.Flags["image"].Text == "true"
	// rc.4 and rc.26 text_quota.go store PromptTokensDetails.ImageTokens
	// under image_output. Do not call it output tokens based on the key name.
	add("image_output", "图片 Tokens（历史字段原值）", image)
	add("image_ratio", "图片倍率（日志原值，非单价）", image)
	if e.Numbers["image_output"].State != portalEvidenceMissing || image {
		items = append(items, portalLogDetailItem{Label: "图片分项说明", Value: "此历史字段用于记录输入图片 Tokens，不作为输出 Tokens 再次累加。", State: portalEvidenceValid})
	}
	if len(items) != 0 {
		items = append(items, portalLogDetailItem{Label: "多模态分项说明", Value: "分项可能已包含在输入/输出总量内，不能再次累加；仅展示日志记录，不据此推算扣费。", State: portalEvidenceValid})
	}
	return items
}
