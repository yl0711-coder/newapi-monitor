package monitor

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

// Covers public identification and account reads, including the shared host
// queue. Five Spring requests can require 5 * 8.25s of spacing (including an
// initial wait) plus the final network read. Keep below the usual 60s proxy
// timeout, and keep the browser cancellation budget longer than this deadline.
const diagnosticTimeout = 55 * time.Second
const diagnosticNetworkTimeout = 6 * time.Second
const diagnosticBodyLimit = 512 << 10

var errDiagnosticAddress = errors.New("检测只允许公网 HTTPS 地址")
var errDiagnosticShape = errors.New("上游响应与已支持的接口格式不符")

// Reject internal, metadata, multicast and translation ranges before dialing.
// Dial the validated IP, not the hostname again, to prevent DNS rebinding.
func diagnosticPublicIP(ip net.IP) bool {
	a, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	a = a.Unmap()
	if !a.IsGlobalUnicast() || a.IsPrivate() || a.IsLoopback() || a.IsLinkLocalUnicast() {
		return false
	}
	for _, raw := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "64:ff9b::/96", "64:ff9b:1::/48", "2001::/32", "2001:db8::/32", "2002::/16"} {
		if netip.MustParsePrefix(raw).Contains(a) {
			return false
		}
	}
	return true
}

func diagnosticBaseURL(raw string) (string, error) {
	base, err := normalizeUpstreamBaseURL(raw)
	if err != nil {
		return "", errDiagnosticAddress
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || (u.Port() != "" && u.Port() != "443") {
		return "", errDiagnosticAddress
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !diagnosticPublicIP(ip) {
		return "", errDiagnosticAddress
	}
	return base, nil
}

func newDiagnosticHTTPClient() *http.Client {
	client := newUpstreamHTTPClient(diagnosticNetworkTimeout)
	// A short Client.Timeout also counts time waiting in upstreamGuardTransport.
	// Bound network I/O separately, after the host guard admits the request.
	client.Timeout = diagnosticTimeout
	transport := client.Transport.(*http.Transport)
	transport.Proxy = nil // Do not delegate destination validation to an environment proxy.
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errDiagnosticAddress
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errDiagnosticAddress
		}
		for _, ip := range ips {
			if !diagnosticPublicIP(ip.IP) {
				return nil, errDiagnosticAddress
			}
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
	return client
}

func newGuardedDiagnosticHTTPClient(guard *upstreamHostGuard) (*http.Client, func()) {
	client := newDiagnosticHTTPClient()
	transport := client.Transport.(*http.Transport)
	client.Transport = &upstreamGuardTransport{
		base:  &diagnosticNetworkTransport{base: transport, timeout: diagnosticNetworkTimeout},
		guard: guard,
	}
	return client, transport.CloseIdleConnections
}

// Installed INSIDE the host guard: queue/spacing time must not consume the
// network budget. Keep the deadline alive through body reads, not just headers.
type diagnosticNetworkTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *diagnosticNetworkTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), t.timeout)
	res, err := t.base.RoundTrip(req.Clone(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	res.Body = &diagnosticResponseBody{ReadCloser: res.Body, cancel: cancel}
	return res, nil
}

type diagnosticResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *diagnosticResponseBody) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// No cookies, redirects, login POSTs or token rotation. Bodies stay in memory
// and are never included in diagnostics responses or logs.
func diagnosticGET(ctx context.Context, client *http.Client, endpoint string, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "NexusAPI-Monitor-Diagnostics/1.0")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, diagnosticBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > diagnosticBodyLimit {
		return nil, errDiagnosticShape
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, &upstreamHTTPError{Status: res.StatusCode, Message: upstreamResponseMessage(data)}
	}
	return data, nil
}
