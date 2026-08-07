package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestParseMeminfo(t *testing.T) {
	// MemTotal 2097152 kB=2048MB, MemAvailable 1048576 kB=1024MB, Swap 已用=(2097152-1048576)kB=1024MB
	b := []byte("MemTotal:       2097152 kB\nMemFree:         100000 kB\nMemAvailable:   1048576 kB\nSwapTotal:      2097152 kB\nSwapFree:       1048576 kB\n")
	mi := parseMeminfo(b)
	if mi.totalMB != 2048 {
		t.Fatalf("totalMB 应 2048,得 %v", mi.totalMB)
	}
	if mi.availMB != 1024 {
		t.Fatalf("availMB 应 1024,得 %v", mi.availMB)
	}
	if mi.swapUsedMB != 1024 {
		t.Fatalf("swapUsedMB 应 1024,得 %v", mi.swapUsedMB)
	}
}

func TestParseLoadavg(t *testing.T) {
	l1, l5, l15 := parseLoadavg("0.52 0.31 0.20 1/234 5678\n")
	if l1 != 0.52 || l5 != 0.31 || l15 != 0.20 {
		t.Fatalf("load 解析错: %v %v %v", l1, l5, l15)
	}
}

func TestParseLoadavgShort(t *testing.T) {
	// 字段不足时不应 panic,缺的归 0
	l1, l5, l15 := parseLoadavg("0.10")
	if l1 != 0.10 || l5 != 0 || l15 != 0 {
		t.Fatalf("短输入应只取到 l1: %v %v %v", l1, l5, l15)
	}
}

func TestDiskUsedPctSmoke(t *testing.T) {
	pct, err := diskUsedPct("/")
	if err != nil {
		t.Fatalf("statfs / 失败: %v", err)
	}
	if pct <= 0 || pct >= 100 {
		t.Fatalf("磁盘用量应在 0~100 之间,得 %v", pct)
	}
}

func TestLoadConfigCanDisablePrivilegedMetrics(t *testing.T) {
	t.Setenv("HOSTAGENT_ROOTFS", "")
	t.Setenv("HOSTAGENT_DOCKER_SOCK", "")
	c := loadConfig()
	if c.rootfs != "" || c.dockSock != "" {
		t.Fatalf("显式空配置应关闭高权限指标: rootfs=%q socket=%q", c.rootfs, c.dockSock)
	}
}

func TestEnvCSVNormalizesContainerAllowlist(t *testing.T) {
	t.Setenv("HOSTAGENT_CONTAINER_ALLOWLIST", " /new-api, caddy,new-api, ,monitor ")
	got := envCSV("HOSTAGENT_CONTAINER_ALLOWLIST")
	if fmt.Sprint(got) != "[new-api caddy monitor]" {
		t.Fatalf("白名单归一化错误: %v", got)
	}
}

func TestDockerSnapshotOnlyInspectsAllowlistAndMarksMissing(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "hostagent-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "docker.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var inspectCalls atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/containers/json":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `[{"Id":"id-api","Names":["/new-api"],"State":"running"},{"Id":"id-unreadable","Names":["/inspect-fails"],"State":"running"},{"Id":"id-secret","Names":["/do-not-inspect"],"State":"exited"}]`)
		case "/containers/id-api/json":
			inspectCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"RestartCount":3,"State":{"Status":"running","Health":{"Status":"healthy"}},"Config":{"Env":["SECRET=must-not-leave-host"]}}`)
		default:
			http.NotFound(w, r)
		}
	})}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("关闭测试 Docker API: %v", err)
		}
		if err := <-serveDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("测试 Docker API 异常退出: %v", err)
		}
	})

	up, total, details, err := dockerSnapshot(sock, []string{"new-api", "inspect-fails", "missing-one"})
	if err != nil {
		t.Fatal(err)
	}
	if up != 2 || total != 3 || inspectCalls.Load() != 1 {
		t.Fatalf("总数/inspect 范围错误: up=%d total=%d inspect=%d", up, total, inspectCalls.Load())
	}
	if len(details) != 3 || details[0].Name != "new-api" || details[0].Health != "healthy" || details[0].RestartCount != 3 || details[1].Health != "unknown" || details[2].State != "missing" {
		t.Fatalf("安全明细错误: %+v", details)
	}
	_, _, empty, err := dockerSnapshot(sock, nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("空白名单必须返回可编码的明确空数组: %#v err=%v", empty, err)
	}
	payload, err := json.Marshal(sample{Containers: empty})
	if err != nil || !strings.Contains(string(payload), `"containers":[]`) {
		t.Fatalf("空白名单必须序列化为 containers:[]，得 %s err=%v", payload, err)
	}
}
