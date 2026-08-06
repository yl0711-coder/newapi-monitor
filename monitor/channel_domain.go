package monitor

import (
	"net"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// normalizeChannelBaseHost 只提取 NewAPI channels.base_url 的主机名。
// 返回值故意不包含 scheme、端口、路径、query 和 userinfo，既可核对归并依据，
// 也不会把完整上游地址或可能附带的凭据带入本地报表。
func normalizeChannelBaseHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	} else if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(u.Hostname()), "."))
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
}

// normalizeChannelBaseDomain 把 NewAPI channels.base_url 归并为可注册主域名（eTLD+1）。
//
// 例如 temp.last-api.ai 和 www.last-api.ai 都归入 last-api.ai；
// api.example.co.uk 归入 example.co.uk。本地/IP 地址无可注册域名，保留主机名本身。
func normalizeChannelBaseDomain(raw string) string {
	host := normalizeChannelBaseHost(raw)
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	base, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return host
	}
	return strings.ToLower(base)
}
