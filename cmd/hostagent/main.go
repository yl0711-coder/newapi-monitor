// hostagent:在每个节点上周期性采集 OS 指标(内存/Swap/磁盘/load/容器存活),
// POST 到 monitor 的 /internal/host(Bearer = MONITOR_INGEST_TOKEN)。
// AWS 看不到这些 OS 内部指标,故由本 agent 补齐——它们会按 node 名并入 monitor 的同名实例行。
//
// 只读采集、不改动主机;失败 fail-open(记日志、跳过本轮、不退出)。仅 stdlib,无第三方依赖。
//
// 关键:HOSTAGENT_NODE 必须等于 monitor 里该实例名(如 Ubuntu-NexusAPI-Master),
//
//	否则 host 行无法并入对应实例行。
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	sinkURL  string
	token    string
	node     string
	interval time.Duration
	procPath string // 宿主 /proc 路径(容器内挂载,默认 /proc)
	rootfs   string // 统计磁盘用量的挂载点(默认 /)
	dockSock string // docker.sock 路径;空=不采容器
	insecure bool   // 跳过 TLS 校验(私网自签场景)
}

func loadConfig() config {
	c := config{
		sinkURL:  os.Getenv("HOSTAGENT_SINK_URL"),
		token:    os.Getenv("HOSTAGENT_TOKEN"),
		node:     os.Getenv("HOSTAGENT_NODE"),
		interval: time.Duration(envInt("HOSTAGENT_INTERVAL_SECONDS", 60)) * time.Second,
		procPath: envStr("HOSTAGENT_PROC", "/proc"),
		rootfs:   envStr("HOSTAGENT_ROOTFS", "/"),
		dockSock: envStr("HOSTAGENT_DOCKER_SOCK", "/var/run/docker.sock"),
		insecure: os.Getenv("HOSTAGENT_INSECURE") == "true",
	}
	if c.interval < 10*time.Second {
		c.interval = 10 * time.Second
	}
	return c
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// sample 是一轮采集结果(字段名与 monitor /internal/host 契约一致)。
type sample struct {
	Node            string  `json:"node"`
	MemTotalMB      float64 `json:"mem_total_mb"`
	MemAvailMB      float64 `json:"mem_avail_mb"`
	SwapUsedMB      float64 `json:"swap_used_mb"`
	DiskUsedPct     float64 `json:"disk_used_pct"`
	Load1           float64 `json:"load1"`
	Load5           float64 `json:"load5"`
	Load15          float64 `json:"load15"`
	ContainersUp    float64 `json:"containers_up"`
	ContainersTotal float64 `json:"containers_total"`
	Ts              int64   `json:"ts"`
}

func main() {
	c := loadConfig()
	if c.sinkURL == "" || c.token == "" || c.node == "" {
		log.Fatal("hostagent: 必须设置 HOSTAGENT_SINK_URL / HOSTAGENT_TOKEN / HOSTAGENT_NODE")
	}
	log.Printf("hostagent 启动: node=%s interval=%s proc=%s rootfs=%s", c.node, c.interval, c.procPath, c.rootfs)
	cl := newClient(c)
	runOnce(c, cl) // 启动即采一轮
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for range t.C {
		runOnce(c, cl)
	}
}

func runOnce(c config, cl *http.Client) {
	s := collect(c)
	if err := push(c, cl, s); err != nil {
		log.Printf("hostagent: 推送失败(忽略本轮): %v", err)
		return
	}
	log.Printf("hostagent: 已推送 node=%s mem_avail=%.0fMB swap=%.0fMB disk=%.1f%% load1=%.2f 容器=%.0f/%.0f",
		s.Node, s.MemAvailMB, s.SwapUsedMB, s.DiskUsedPct, s.Load1, s.ContainersUp, s.ContainersTotal)
}

// collect 采一轮;任一项失败只记日志、该项留零,不影响其它项(fail-open)。
func collect(c config) sample {
	s := sample{Node: c.node, Ts: time.Now().Unix()}
	if b, err := os.ReadFile(c.procPath + "/meminfo"); err == nil {
		mi := parseMeminfo(b)
		s.MemTotalMB, s.MemAvailMB, s.SwapUsedMB = mi.totalMB, mi.availMB, mi.swapUsedMB
	} else {
		log.Printf("hostagent: 读 meminfo 失败: %v", err)
	}
	if b, err := os.ReadFile(c.procPath + "/loadavg"); err == nil {
		l1, l5, l15 := parseLoadavg(string(b))
		s.Load1, s.Load5, s.Load15 = l1, l5, l15
	} else {
		log.Printf("hostagent: 读 loadavg 失败: %v", err)
	}
	if pct, err := diskUsedPct(c.rootfs); err == nil {
		s.DiskUsedPct = pct
	} else {
		log.Printf("hostagent: 统计磁盘失败: %v", err)
	}
	if c.dockSock != "" {
		if up, total, err := dockerCounts(c.dockSock); err == nil {
			s.ContainersUp, s.ContainersTotal = float64(up), float64(total)
		} else {
			log.Printf("hostagent: 采集容器数失败(忽略): %v", err)
		}
	}
	return s
}

type meminfo struct{ totalMB, availMB, swapUsedMB float64 }

// parseMeminfo 解析 /proc/meminfo(单位 kB),算出总/可用内存与已用 Swap(均转 MB)。
func parseMeminfo(b []byte) meminfo {
	kv := map[string]float64{}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		key := strings.TrimSuffix(f[0], ":")
		if v, err := strconv.ParseFloat(f[1], 64); err == nil {
			kv[key] = v // kB
		}
	}
	toMB := func(kb float64) float64 { return kb / 1024 }
	return meminfo{
		totalMB:    toMB(kv["MemTotal"]),
		availMB:    toMB(kv["MemAvailable"]),
		swapUsedMB: toMB(kv["SwapTotal"] - kv["SwapFree"]),
	}
}

// parseLoadavg 解析 /proc/loadavg 的前三个数(1/5/15 分钟)。
func parseLoadavg(s string) (l1, l5, l15 float64) {
	f := strings.Fields(s)
	get := func(i int) float64 {
		if i < len(f) {
			if v, err := strconv.ParseFloat(f[i], 64); err == nil {
				return v
			}
		}
		return 0
	}
	return get(0), get(1), get(2)
}

// diskUsedPct 用 statfs 取挂载点用量百分比(df 口径:used/(used+avail))。
func diskUsedPct(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	bs := float64(st.Bsize)
	total := float64(st.Blocks) * bs
	free := float64(st.Bfree) * bs
	avail := float64(st.Bavail) * bs
	used := total - free
	denom := used + avail
	if denom <= 0 {
		return 0, nil
	}
	return used / denom * 100, nil
}

// dockerCounts 经 docker.sock 取运行中 / 全部容器数。
func dockerCounts(sock string) (up, total int, err error) {
	cl := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}},
	}
	count := func(all bool) (int, error) {
		url := "http://unix/containers/json"
		if all {
			url += "?all=1"
		}
		resp, e := cl.Get(url)
		if e != nil {
			return 0, e
		}
		defer resp.Body.Close()
		var arr []struct{}
		if e := json.NewDecoder(resp.Body).Decode(&arr); e != nil {
			return 0, e
		}
		return len(arr), nil
	}
	if up, err = count(false); err != nil {
		return 0, 0, err
	}
	if total, err = count(true); err != nil {
		return 0, 0, err
	}
	return up, total, nil
}

func newClient(c config) *http.Client {
	tr := &http.Transport{}
	if c.insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // 私网自签场景显式开启
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: tr}
}

func push(c config, cl *http.Client, s sample) error {
	body, _ := json.Marshal(s)
	req, err := http.NewRequest(http.MethodPost, c.sinkURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &httpErr{resp.StatusCode}
	}
	return nil
}

type httpErr struct{ code int }

func (e *httpErr) Error() string { return "sink 返回 HTTP " + strconv.Itoa(e.code) }
