package main

import (
	"encoding/json"
	"testing"
	"time"
)

// TestJoinKeyFromBothFormats 两种 request id 形态都要认。
//
// 2026-08-28 生产实测两种都存在：
//
//	(request id: 2026...)      ← new-api 系上游，下划线前是空格
//	(request_id: req_...)      ← 另一种上游，下划线连写
//
// 只认一种会漏掉另一批，而漏掉的那批恰是 bad_response_status_code 的主体——
// 那正是最需要串联的一类（状态码层判不出责任方）。
func TestJoinKeyFromBothFormats(t *testing.T) {
	cases := []struct {
		name, content, want string
	}{
		{
			"new-api 形态（空格）",
			"status_code=503, no available upstream in group (request id: 202608280208089288070118268d9d6WhLijC4B)",
			"202608280208089288070118268d9d6WhLijC4B",
		},
		{
			"另一种上游（下划线）",
			"status_code=503, 上游负载已饱和 (request_id: req_1787669295312_86f87189)",
			"req_1787669295312_86f87189",
		},
		{"无 key", "status_code=408, 响应时间 125.03s 超过阈值 120.00s", ""},
		{"空原文", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinKeyFrom(tc.content); got != tc.want {
				t.Errorf("joinKeyFrom() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestParseOtherBothForms other 有对象与转义字符串两种形态，只认一种会静默丢字段。
func TestParseOtherBothForms(t *testing.T) {
	obj := json.RawMessage(`{"channel_name":"kpzhu_gpt_pro","status_code":502}`)
	form, inner := parseOther(obj)
	if form != "对象" || unquote(inner["channel_name"]) != "kpzhu_gpt_pro" {
		t.Errorf("对象形态解析失败: form=%s inner=%v", form, inner)
	}

	// 转义字符串：契约 fixture 里就是这种。
	escaped, _ := json.Marshal(`{"channel_name":"jikesoft","status_code":503}`)
	form, inner = parseOther(escaped)
	if form != "转义字符串" || unquote(inner["channel_name"]) != "jikesoft" {
		t.Errorf("转义字符串形态解析失败: form=%s inner=%v", form, inner)
	}

	// 解不开时必须明确报出来，不能装作空对象——那会让人以为上游没给这些字段。
	if form, _ := parseOther(json.RawMessage(`"not json"`)); form != "解不开" {
		t.Errorf("非 JSON 字符串应报解不开，got=%s", form)
	}
}

// TestCSTWindowIsHalfOpenNaturalDay 窗口口径必须与 monitor 侧一致。
//
// 差一秒的后果：end_timestamp 是闭区间，不减 1 就会把 to 那一秒重复算进两个窗口。
func TestCSTWindowIsHalfOpenNaturalDay(t *testing.T) {
	cst := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 8, 28, 15, 30, 0, 0, cst)

	from, to := cstWindow(1, now)
	wantTo := time.Date(2026, 8, 29, 0, 0, 0, 0, cst).Unix()
	if to != wantTo {
		t.Errorf("to 应是明天 00:00: got=%s", time.Unix(to, 0).In(cst))
	}
	if to-from != 86400 {
		t.Errorf("1 天窗口应恰好 86400 秒: got=%d", to-from)
	}

	from3, to3 := cstWindow(3, now)
	if to3 != wantTo || to3-from3 != 3*86400 {
		t.Errorf("3 天窗口不对: from=%s to=%s", time.Unix(from3, 0).In(cst), time.Unix(to3, 0).In(cst))
	}
}

// TestTokenStatusNeverLeaks 凭据绝不能进输出。
func TestTokenStatusNeverLeaks(t *testing.T) {
	secret := "sk-abcdefghijklmnopqrstuvwxyz0123456789"
	got := tokenStatus(secret)
	if len(got) > 0 && containsSecret(got, secret) {
		t.Errorf("tokenStatus 泄漏了 token: %s", got)
	}
	if tokenStatus("") == "" {
		t.Error("未设置时也要有明确提示")
	}
}

func containsSecret(out, secret string) bool {
	// 只要出现连续 8 个字符的片段就算泄漏。
	for i := 0; i+8 <= len(secret); i++ {
		if len(out) > 0 && contains(out, secret[i:i+8]) {
			return true
		}
	}
	return false
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) &&
		func() bool {
			for i := 0; i+len(needle) <= len(haystack); i++ {
				if haystack[i:i+len(needle)] == needle {
					return true
				}
			}
			return false
		}()
}
