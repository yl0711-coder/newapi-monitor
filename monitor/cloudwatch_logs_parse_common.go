package monitor

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var cwVersionPattern = regexp.MustCompile(`(?i)(?:^|[/ ])([0-9]+(?:\.[0-9]+){0,3})`)

func cwMethod(raw string) string {
	switch value := strings.ToUpper(strings.TrimSpace(raw)); value {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return value
	default:
		return "OTHER"
	}
}

func cwRoute(raw string) string {
	path := strings.TrimSpace(strings.SplitN(raw, "?", 2)[0])
	switch path {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/models", "/api/status":
		return path
	}
	if strings.HasPrefix(path, "/v1/") {
		return "/v1/*"
	}
	if strings.HasPrefix(path, "/api/") {
		return "/api/*"
	}
	return "/other"
}

func cwHost(raw string) string {
	host := strings.ToLower(strings.TrimSpace(raw))
	if host == "" || host == "-" {
		return ""
	}
	if index := strings.IndexByte(host, ':'); index >= 0 {
		host = host[:index]
	}
	switch host {
	case "nexusapi.link", "us.nexusapi.link":
		return host
	default:
		return "other"
	}
}

func cwSafeToken(raw string, maximum int) string {
	value := strings.TrimSpace(raw)
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return ""
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._:/+-", r)) {
			return ""
		}
	}
	return value
}

func cwOpaqueID(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" || value == "-" || len(value) > 256 || !utf8.ValidString(value) {
		return "", false
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f {
			return "", false
		}
	}
	return value, true
}

func (p *cloudWatchEvidenceParser) optionalOpaqueHMAC(domain, raw string) (string, bool) {
	if strings.TrimSpace(raw) == "" || strings.TrimSpace(raw) == "-" {
		return "", true
	}
	value, ok := cwOpaqueID(raw)
	if !ok {
		return "", false
	}
	return p.hmac(domain, value), true
}

func cwBusinessLabel(raw string, maximum int) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", true
	}
	if len(value) > maximum || !utf8.ValidString(value) {
		return "", false
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f || strings.ContainsRune("|?&=#'\"`\\<>[]{}()", r) {
			return "", false
		}
	}
	lower := strings.ToLower(value)
	for _, fragment := range []string{"authorization", "api_key", "apikey", "access_token", "refresh_token", "bearer", "secret"} {
		if strings.Contains(lower, fragment) {
			return "", false
		}
	}
	for _, prefix := range []string{"sk-", "rk-", "pk-", "sess-", "token-"} {
		if strings.HasPrefix(lower, prefix) {
			return "", false
		}
	}
	return value, true
}

func cwBoundedLabel(raw string, maximum int) string {
	value := strings.TrimSpace(raw)
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return ""
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f || r == '|' {
			return ""
		}
	}
	return value
}

func cwCompletion(raw string) string {
	switch strings.TrimSpace(raw) {
	case "OK":
		return "complete_at_edge"
	case "", "-":
		return "incomplete_at_edge"
	default:
		return "unknown"
	}
}

func cwHTTPFault(status int, detail string) string {
	lower := strings.ToLower(detail)
	switch {
	case status == 0 || status == 499:
		return "client_gone"
	case strings.Contains(lower, "connecterror") || strings.Contains(lower, "connectionfailed"):
		return "transport_connect"
	case status == 408 || status == 504 || strings.Contains(lower, "timeout"):
		return "transport_timeout"
	case status == 429:
		return "rate_limit_capacity"
	case status >= 400:
		return "unknown"
	default:
		return ""
	}
}

func cwHTTPSummary(layer string, status int) string {
	switch {
	case status == 0 || status == 499:
		return layer + "记录到响应完成前断开"
	case status >= 500:
		return layer + "记录到 5xx 响应"
	case status >= 400:
		return layer + "记录到 4xx 响应"
	case status >= 300:
		return layer + "记录到 3xx 响应"
	default:
		return layer + "记录到 2xx 响应"
	}
}

func cwUserAgent(raw string) (string, string) {
	value := strings.TrimSpace(raw)
	lower := strings.ToLower(value)
	families := []struct{ needle, name string }{
		{"edg/", "Edge"}, {"chrome/", "Chrome"}, {"firefox/", "Firefox"},
		{"postmanruntime/", "Postman"}, {"python-requests/", "python-requests"},
		{"okhttp/", "okhttp"}, {"curl/", "curl"}, {"go-http-client/", "Go-http-client"},
		{"safari/", "Safari"},
	}
	for _, item := range families {
		index := strings.Index(lower, item.needle)
		if index < 0 {
			continue
		}
		version := ""
		if match := cwVersionPattern.FindStringSubmatch(value[index+len(item.needle)-1:]); len(match) == 2 {
			version = match[1]
		}
		return item.name, version
	}
	if value == "" || value == "-" {
		return "", ""
	}
	return "other", ""
}
