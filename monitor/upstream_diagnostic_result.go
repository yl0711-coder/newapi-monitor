package monitor

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"strings"
)

type upstreamDiagnosticCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action,omitempty"`
}

type upstreamDiagnosticReport struct {
	Provider     string                    `json:"provider"`
	Confidence   string                    `json:"confidence"`
	Configured   bool                      `json:"configured"`
	Checks       []upstreamDiagnosticCheck `json:"checks"`
	Instructions []string                  `json:"instructions"`
	Scope        string                    `json:"scope"`
}

func diagnosticGuide(provider string) []string {
	switch provider {
	case upstreamProviderNewAPI:
		return []string{"类型选择 NewAPI；站点地址填上游控制台实际根地址。", "用户 ID 和访问令牌：登录上游控制台，查看个人设置中的用户 ID、访问令牌；这是管理账户令牌，不是调用模型的 sk- Key。", "余额验证后再开启消费日志同步；额度换算单位与充值折扣需要分别核对，不能从渠道名称猜倍率。"}
	case upstreamProviderSub2API:
		return []string{"类型选择 Sub2API；填写上游控制台地址、登录邮箱。", "普通登录可填写密码；若提示 Turnstile，先在上游浏览器完成人机验证，再使用 Refresh Token 连接方式。", "Chrome 开发者工具 → 网络（Network）→ Fetch/XHR，登录时找到 /api/v1/auth/login，响应（Response）的 data.refresh_token；不要复制 access_token 或模型 Key，不要公开令牌。", "余额可用不代表历史账单已补齐；消费接口可能仅支持自然日汇总。"}
	case upstreamProviderTokenForce:
		return []string{"类型选择 海南海纳（TokenForce MaaS）；海南海纳控制台地址为 https://maas.hainahn.com，不要填 https://hainahn.com。", "登录上游后打开开发者工具 → 网络（Network）→ Fetch/XHR，进入余额页；筛选 balance，查看请求 URL /api/orgs/数字/balance，其中数字是 orgId，不是 tenantId。", "在登录或正常刷新时，查找 /api/sys/login/password 或 /api/sys/login/refresh，展开响应（Response），复制 refreshToken。不要复制 accessToken 或模型 Key，也不要为了取令牌反复重放刷新请求。", "填写每 1 USD 对应的结算 CNY；保存和正常同步负责令牌轮换，本次检测不刷新令牌。若 Refresh Token 被撤销或过期，需重新登录导入。"}
	case upstreamProviderAICodeWith:
		return []string{"类型选择 AICodeWith（春秋），使用其控制台 API 地址（当前适配地址 https://aicodewith.ai）。", "填写上游 API Key，可按业务实际加入多把 Key；每把 Key 的消费范围需要分别核对。", "默认同步自然日账单，不能将日合计强拆为小时。运维显式开启记录模式后，可在记录补齐并核对日总额后生成小时账单；本次实际抽查范围见检测结果。倍率、充值支付/到账比例仍需按真实合同配置。"}
	default:
		return []string{"未取得足够平台特征，不自动猜测类型或修改配置。", "填写真正的管理控制台根地址，不是模型调用地址、主域名归并名称或 /v1 接口地址。", "请在上游控制台确认平台类型及只读余额/用量 API；无法识别的自研平台需要新增适配器。不要尝试四种类型逐个提交账号密码。"}
	}
}

func diagnosticFailure(name string, err error) upstreamDiagnosticCheck {
	c := upstreamDiagnosticCheck{Name: name, Status: "error", Code: "response_invalid", Message: "接口响应格式或业务字段不兼容", Action: "核对平台类型、控制台地址及上游接口版本；保留已有账单，不要改金额绕过校验。"}
	var status *upstreamHTTPError
	var dns *net.DNSError
	var hostname x509.HostnameError
	var authority x509.UnknownAuthorityError
	var auth *upstreamAuthError
	var api *tokenForceAPIError
	var circuit *upstreamCircuitOpenError
	// Error text is classified internally, never returned verbatim (may echo credentials).
	text := strings.ToLower(err.Error())
	switch {
	case errors.As(err, &circuit):
		c.Code = "rate_limited"
		c.Message = "上游主机仍在限流或故障冷却期，本次未发出请求"
		c.Action = "查看数据同步状态中的下次重试时间，等待冷却结束后再检测；不要反复点击。"
	case errors.Is(err, errDiagnosticAddress):
		c.Code = "unsafe_address"
		c.Message = "检测地址不是允许的公网 HTTPS 地址"
		c.Action = "填写上游公网控制台地址；不允许访问内网、云元数据或非标准端口。"
	case errors.As(err, &hostname) || errors.As(err, &authority) || strings.Contains(text, "x509:"):
		c.Code = "tls"
		c.Message = "HTTPS 证书校验失败"
		c.Action = "确认控制台域名与证书匹配；海南海纳应填 https://maas.hainahn.com。请上游修复证书，不要关闭校验。"
	case strings.Contains(text, "turnstile") || strings.Contains(text, "captcha") || strings.Contains(text, "challenge"):
		c.Code = "challenge"
		c.Message = "上游要求人机验证"
		c.Action = "先在上游网站完成验证；Sub2API 可改用 Refresh Token 连接，或申请正式只读服务账号，不能自动绕过验证码。"
	case strings.Contains(text, "refresh token") || strings.Contains(text, "refresh_token"):
		c.Code = "refresh_expired"
		c.Message = "刷新会话失效"
		c.Action = "在上游重新登录，取得新的 Refresh Token 后保存并验证；不要继续重复使用失效令牌。"
	case errors.As(err, &auth) || (errors.As(err, &api) && api.authenticationFailure()):
		c.Code = "unauthorized"
		c.Message = "账户认证未通过"
		c.Action = "先点同步余额让正常流程刷新会话；仍失败则重新登录并保存正确的管理凭据。"
	case strings.Contains(text, "凭据"):
		c.Code = "credential_unavailable"
		c.Message = "保存的凭据不可用或无法解密"
		c.Action = "核对用户/组织 ID；重新保存管理凭据。若多个账户同时失败，请运维检查凭据加密密钥，不要清空历史账单。"
	case strings.Contains(text, "cny/usd") || strings.Contains(text, "换算"):
		c.Code = "currency_invalid"
		c.Message = "金额换算单位未配置或无效"
		c.Action = "按实际结算币种填写大于零的换算比例；不要凭猜测填 1。"
	case errors.As(err, &dns):
		c.Code = "dns"
		c.Message = "域名解析失败"
		c.Action = "核对域名拼写、DNS 记录及 Monitor 主机网络。"
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(text, "timeout"):
		c.Code = "timeout"
		c.Message = "检测超时"
		c.Action = "可能是上游响应慢或共享限频队列繁忙；查看同步状态，稍后再试，不要连续点击加压。"
	case errors.As(err, &status):
		switch {
		case status.Status == 401:
			c.Code = "unauthorized"
			c.Message = "账户认证未通过"
			c.Action = "核对用户 ID 与管理访问令牌；若是短期会话，可先点同步余额让正常流程刷新，仍失败再重新登录保存。"
		case status.Status == 403:
			c.Code = "forbidden"
			c.Message = "上游拒绝访问"
			c.Action = "检查账号读取权限、IP 白名单及 WAF 限制；不要扩大到全站管理员权限。"
		case status.Status == 429:
			c.Code = "rate_limited"
			c.Message = "上游限流"
			c.Action = "等待上游 Retry-After 或冷却期结束后再试，不要重复提交检测。"
		case status.Status == 404 || status.Status == 405:
			c.Code = "route_missing"
			c.Message = "检测接口不存在或方法不支持"
			c.Action = "核对控制台地址与类型；上游可能关闭了该接口，需向上游确认兼容路径。"
		case status.Status >= 300 && status.Status < 400:
			c.Code = "redirect"
			c.Message = "上游接口发生跳转，已停止跟随"
			c.Action = "在浏览器确认最终控制台地址后手动修改；不会携带凭据跨站跳转。"
		case status.Status >= 500:
			c.Code = "upstream_error"
			c.Message = "上游服务暂时异常"
			c.Action = "稍后重试或联系上游；保留原有账号配置和已同步账单。"
		}
	}
	return c
}
