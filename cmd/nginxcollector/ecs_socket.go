package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Optional private IPC only. HTTPS remains the logical destination and is
// enforced on the agent's outbound connection. No TCP/plaintext fallback exists.
type ecsSocketTransport struct{ socket string }

func (t ecsSocketTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" || !filepath.IsAbs(t.socket) {
		return nil, errors.New("invalid private ECS transport")
	}
	info, err := os.Lstat(t.socket)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("private ECS agent socket unavailable")
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("X-Monitor-Archive-Ack", "1")
	urlCopy := *req.URL
	urlCopy.Scheme = "http"
	clone.URL = &urlCopy
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", t.socket)
	}}
	return transport.RoundTrip(clone)
}

func collectorTransport(c config) http.RoundTripper {
	if c.ecsSocket != "" {
		return ecsSocketTransport{c.ecsSocket}
	}
	return nil
}
