package main

import (
	"net/http"
	"testing"
	"time"
)

func TestMonitoredHTTPServerTimeouts(t *testing.T) {
	s := monitoredHTTPServer(":0", http.NewServeMux())
	if s.ReadHeaderTimeout != 10*time.Second || s.ReadTimeout != 30*time.Second || s.WriteTimeout != 2*time.Minute || s.IdleTimeout != 60*time.Second {
		t.Fatalf("HTTP 超时配置错误: %#v", s)
	}
}
