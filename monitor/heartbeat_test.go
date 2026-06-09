package monitor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeartbeat(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()

	// 配了 URL → 打一次心跳
	m := &Monitor{}
	m.cfg.HeartbeatURL = srv.URL
	m.heartbeat()
	if hits != 1 {
		t.Errorf("配置 URL 后应 ping 一次,实际 %d", hits)
	}

	// 未配 URL → 空操作,不打
	m2 := &Monitor{}
	m2.heartbeat()
	if hits != 1 {
		t.Errorf("未配 URL 不应 ping,实际 hits=%d", hits)
	}

	// URL 不可达 → fire-and-forget,静默返回(不 panic、不影响)
	m3 := &Monitor{}
	m3.cfg.HeartbeatURL = "http://127.0.0.1:0/unreachable"
	m3.heartbeat() // 不应 panic
}
