package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type upstreamDiagnosticInput struct {
	Domain  string `json:"domain"`
	BaseURL string `json:"base_url"`
}

func (m *Monitor) diagnoseChannelUpstreamHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	var in upstreamDiagnosticInput
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	if c.ShouldBindJSON(&in) != nil {
		c.JSON(400, gin.H{"error": "检测参数无效"})
		return
	}
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	if in.Domain == "" || len(in.Domain) > 253 {
		c.JSON(400, gin.H{"error": "主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), diagnosticTimeout)
	defer cancel()
	exists, err := m.channelDomainExists(ctx, in.Domain)
	if err != nil {
		c.JSON(500, gin.H{"error": "读取渠道快照失败"})
		return
	}
	if !exists {
		c.JSON(400, gin.H{"error": "该主域名不属于当前渠道快照"})
		return
	}
	// Try-only: diagnostics never queue ahead of ongoing sync/token rotation.
	release, err := m.tryAcquireUpstreamAccountBackground(in.Domain)
	if err != nil {
		c.JSON(409, gin.H{"error": "此账户正在同步或保存，请稍后检测"})
		return
	}
	defer release()
	var row ChannelUpstreamAccount
	err = m.storeDB.WithContext(ctx).First(&row, "domain = ?", in.Domain).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(500, gin.H{"error": "读取上游配置失败"})
		return
	}
	if in.BaseURL == "" {
		in.BaseURL = row.BaseURL
	}
	if in.BaseURL == "" {
		in.BaseURL = "https://" + in.Domain
	}
	base, err := diagnosticBaseURL(in.BaseURL)
	report := upstreamDiagnosticReport{Provider: "unknown", Confidence: "unknown", Checks: []upstreamDiagnosticCheck{}, Scope: "仅检测连接、接口兼容和保存的账号；不写账单、不修改配置、不刷新令牌。单页抽查不证明整个区间账单完整或金额正确。"}
	if err != nil {
		report.Checks = append(report.Checks, diagnosticFailure("连接地址", err))
		report.Instructions = diagnosticGuide("")
		c.JSON(200, report)
		return
	}
	savedBase, _ := diagnosticBaseURL(row.BaseURL)
	report.Configured = row.Credential != "" && savedBase == base
	if report.Configured {
		report.Provider = row.Provider
		report.Confidence = "configured"
	}
	if m.cfg.LocalSnapshotOnly && !m.cfg.UpstreamDiagnosticsLocalEnabled {
		report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "本机隔离", Status: "skipped", Code: "local_snapshot", Message: "当前为离线快照预览，未访问真实上游", Action: "本地自动测试使用模拟上游；真实账号检测须在允许外部访问的环境由管理员主动发起。"})
	} else {
		// Reuse the process host guard for spacing/cooldown, but no login or write APIs.
		shared := m.channelUpstreamHTTPClient().Transport.(*upstreamGuardTransport)
		client, closeIdle := newGuardedDiagnosticHTTPClient(shared.guard)
		defer closeIdle()
		if report.Configured {
			identified := upstreamDiagnosticReport{Provider: "unknown"}
			diagnosePublicUpstream(ctx, client, base, &identified)
			if identified.Confidence == "compatible" && identified.Provider != row.Provider {
				report.Provider, report.Confidence = identified.Provider, identified.Confidence
				report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "配置类型", Status: "error", Code: "provider_mismatch", Message: "接口特征与已保存的平台类型不一致，未发送凭据", Action: "按识别出的兼容类型核对上游，手动修改类型和对应凭据后保存。"})
			} else {
				m.diagnoseSavedUpstream(ctx, client, row, &report)
			}
		} else {
			diagnosePublicUpstream(ctx, client, base, &report)
			if row.Credential != "" {
				report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "账号范围", Status: "skipped", Code: "address_changed", Message: "地址与已保存配置不同，未发送已有凭据", Action: "核对控制台地址后保存并验证，再检测账号。"})
			}
		}
	}
	report.Instructions = diagnosticGuide(report.Provider)
	c.JSON(200, report)
}

// Structured API signatures are stronger than branding in a white-label SPA.
// 401/404/HTML alone never identifies a provider.
func diagnosticSignature(path string, body []byte) string {
	var v struct {
		Success *bool `json:"success"`
		Code    *int  `json:"code"`
		Data    struct {
			QuotaPerUnit        *float64 `json:"quota_per_unit"`
			Version             string   `json:"version"`
			RegistrationEnabled *bool    `json:"registration_enabled"`
			TurnstileEnabled    *bool    `json:"turnstile_enabled"`
			EmailVerifyEnabled  *bool    `json:"email_verify_enabled"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &v) != nil {
		return ""
	}
	if path == "/api/status" && v.Success != nil && *v.Success && v.Data.QuotaPerUnit != nil && *v.Data.QuotaPerUnit > 0 && strings.TrimSpace(v.Data.Version) != "" {
		return upstreamProviderNewAPI
	}
	if path == "/api/v1/settings/public" && v.Code != nil && *v.Code == 0 && v.Data.RegistrationEnabled != nil && (v.Data.TurnstileEnabled != nil || v.Data.EmailVerifyEnabled != nil) {
		return upstreamProviderSub2API
	}
	return ""
}

func diagnosePublicUpstream(ctx context.Context, client *http.Client, base string, report *upstreamDiagnosticReport) {
	for _, path := range []string{"/api/status", "/api/v1/settings/public", "/"} {
		body, err := diagnosticGET(ctx, client, upstreamEndpoint(base, path), nil)
		if err != nil {
			check := diagnosticFailure("平台探测 "+path, err)
			// Missing paths are expected while identifying a platform, not an account failure.
			if check.Code == "route_missing" || check.Code == "unauthorized" {
				continue
			}
			report.Checks = append(report.Checks, check)
			return
		}
		if provider := diagnosticSignature(path, body); provider != "" {
			report.Provider = provider
			report.Confidence = "compatible"
			report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "平台类型", Status: "ok", Code: "signature", Message: "发现 " + upstreamProviderName(provider) + " 兼容接口特征", Action: "按下方指引配置；仍需保存后检测实际账户权限。"})
			return
		}
		if path == "/" {
			text := strings.ToLower(string(body))
			if strings.Contains(text, "cf-chl-") || strings.Contains(text, "challenges.cloudflare.com") {
				report.Checks = append(report.Checks, diagnosticFailure("访问验证", errors.New("browser challenge")))
				return
			}
			candidates := []string{}
			for _, item := range []struct {
				provider string
				markers  []string
			}{
				{upstreamProviderAICodeWith, []string{"aicodewith"}},
				{upstreamProviderTokenForce, []string{"tokenforce", "fastmodels", "海南海纳"}},
				{upstreamProviderSub2API, []string{"sub2api", "sub2-api"}},
				{upstreamProviderNewAPI, []string{"new-api", "new api", "one-api"}},
			} {
				for _, marker := range item.markers {
					if strings.Contains(text, marker) {
						candidates = append(candidates, item.provider)
						break
					}
				}
			}
			if len(candidates) == 1 {
				report.Provider = candidates[0]
				report.Confidence = "suspected"
				report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "平台类型", Status: "warning", Code: "branding_only", Message: "页面疑似 " + upstreamProviderName(report.Provider) + "，尚未验证接口", Action: "品牌特征不能证明兼容；请核对类型后再配置账号。"})
				return
			}
		}
	}
	report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "平台类型", Status: "warning", Code: "unknown", Message: "站点可访问，但无法可靠识别平台", Action: "确认管理控制台地址；自研或修改过接口的站点需要上游提供只读 API 说明。"})
}

func (m *Monitor) diagnoseSavedUpstream(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, report *upstreamDiagnosticReport) {
	if !row.Enabled || !row.UsageSyncEnabled || !m.cfg.UpstreamUsageSyncEnabled {
		report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "同步开关", Status: "warning", Code: "sync_disabled", Message: "账号或消费自动同步尚未全部开启", Action: "确认余额和金额单位后，在账户配置开启同步；全局消费任务关闭时需由运维开启。"})
	}
	// Stored errors are explicitly historical and classified without echoing secrets.
	for _, item := range []struct{ name, text string }{{"上次余额同步", row.LastError}, {"上次消费同步", row.UsageLastError}} {
		if item.text != "" {
			check := diagnosticFailure(item.name, errors.New(item.text))
			check.Status = "warning"
			check.Message = "历史记录（不是本次结果）：" + check.Message
			report.Checks = append(report.Checks, check)
		}
	}
	err := m.probeSavedUpstream(ctx, client, row, report)
	if err != nil {
		report.Checks = append(report.Checks, diagnosticFailure("账户检测", err))
	}
}
