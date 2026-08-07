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
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	sinkURL        string
	token          string
	node           string
	interval       time.Duration
	procPath       string   // 宿主 /proc 路径(容器内挂载,默认 /proc)
	rootfs         string   // 统计磁盘用量的挂载点(默认 /)
	dockSock       string   // docker.sock 路径;空=不采容器
	containerAllow []string // 仅这些容器上报明细；总数仍统计全部容器
	insecure       bool     // 跳过 TLS 校验(私网自签场景)
}

func loadConfig() config {
	// 原生运行时维持原有默认值；容器编排可以显式传空值关闭高权限指标。
	rootfs := "/"
	if v, ok := os.LookupEnv("HOSTAGENT_ROOTFS"); ok {
		rootfs = v
	}
	dockSock := "/var/run/docker.sock"
	if v, ok := os.LookupEnv("HOSTAGENT_DOCKER_SOCK"); ok {
		dockSock = v
	}
	c := config{
		sinkURL:        os.Getenv("HOSTAGENT_SINK_URL"),
		token:          os.Getenv("HOSTAGENT_TOKEN"),
		node:           os.Getenv("HOSTAGENT_NODE"),
		interval:       time.Duration(envInt("HOSTAGENT_INTERVAL_SECONDS", 60)) * time.Second,
		procPath:       envStr("HOSTAGENT_PROC", "/proc"),
		rootfs:         rootfs,
		dockSock:       dockSock,
		containerAllow: envCSV("HOSTAGENT_CONTAINER_ALLOWLIST"),
		insecure:       os.Getenv("HOSTAGENT_INSECURE") == "true",
	}
	if c.interval < 10*time.Second {
		c.interval = 10 * time.Second
	}
	return c
}

func envCSV(k string) []string {
	var out []string
	seen := map[string]bool{}
	for _, item := range strings.Split(os.Getenv(k), ",") {
		item = strings.TrimPrefix(strings.TrimSpace(item), "/")
		if item != "" && !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
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
// 指标用指针 + omitempty:某项采集失败就不带该字段,接收端也就不写——
// 避免「读失败=0」被下游算成「可用 0 = 已用 100%」这种误报。
type sample struct {
	Node            string   `json:"node"`
	MemTotalMB      *float64 `json:"mem_total_mb,omitempty"`
	MemAvailMB      *float64 `json:"mem_avail_mb,omitempty"`
	SwapUsedMB      *float64 `json:"swap_used_mb,omitempty"`
	DiskUsedPct     *float64 `json:"disk_used_pct,omitempty"`
	Load1           *float64 `json:"load1,omitempty"`
	Load5           *float64 `json:"load5,omitempty"`
	Load15          *float64 `json:"load15,omitempty"`
	ContainersUp    *float64 `json:"containers_up,omitempty"`
	ContainersTotal *float64 `json:"containers_total,omitempty"`
	// 不使用 omitempty：docker.sock 可用且白名单为空时必须发送 []，让接收端
	// 明确清除旧快照；未启用/采集失败时 nil 编码为 null，接收端继续保留旧快照。
	Containers []containerStatus `json:"containers"`
	Ts         int64             `json:"ts"`
}

// containerStatus 只包含运维判断所需的安全字段；不采集镜像环境变量、命令、
// 挂载、标签、网络或日志，避免 docker.sock 的高权限信息被带出主机。
type containerStatus struct {
	Name         string `json:"name"`
	State        string `json:"state"`
	Health       string `json:"health,omitempty"`
	RestartCount int    `json:"restart_count"`
}

func fp(v float64) *float64 { return &v }
func dv(p *float64) float64 {
	if p != nil {
		return *p
	}
	return -1
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
	log.Printf("hostagent: 已推送 node=%s mem_avail=%.0fMB swap=%.0fMB disk=%.1f%% load1=%.2f 容器=%.0f/%.0f (-1=该项未采到)",
		s.Node, dv(s.MemAvailMB), dv(s.SwapUsedMB), dv(s.DiskUsedPct), dv(s.Load1), dv(s.ContainersUp), dv(s.ContainersTotal))
}

// collect 采一轮;任一项失败只记日志、该项【不带】(留 nil),不影响其它项(fail-open)。
func collect(c config) sample {
	s := sample{Node: c.node, Ts: time.Now().Unix()}
	if b, err := os.ReadFile(c.procPath + "/meminfo"); err == nil {
		mi := parseMeminfo(b)
		s.MemTotalMB, s.MemAvailMB, s.SwapUsedMB = fp(mi.totalMB), fp(mi.availMB), fp(mi.swapUsedMB)
	} else {
		log.Printf("hostagent: 读 meminfo 失败(本项不上报): %v", err)
	}
	if b, err := os.ReadFile(c.procPath + "/loadavg"); err == nil {
		l1, l5, l15 := parseLoadavg(string(b))
		s.Load1, s.Load5, s.Load15 = fp(l1), fp(l5), fp(l15)
	} else {
		log.Printf("hostagent: 读 loadavg 失败(本项不上报): %v", err)
	}
	if c.rootfs != "" {
		if pct, err := diskUsedPct(c.rootfs); err == nil {
			s.DiskUsedPct = fp(pct)
		} else {
			log.Printf("hostagent: 统计磁盘失败(本项不上报): %v", err)
		}
	}
	if c.dockSock != "" {
		if up, total, details, err := dockerSnapshot(c.dockSock, c.containerAllow); err == nil {
			s.ContainersUp, s.ContainersTotal = fp(float64(up)), fp(float64(total))
			s.Containers = details
		} else {
			log.Printf("hostagent: 采集容器数失败(本项不上报): %v", err)
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

// dockerSnapshot 经 docker.sock 一次读取全部容器以统计总数，并且只对显式白名单
// 做 inspect。白名单里已不存在的容器也会上报 missing，避免“消失=看起来正常”。
func dockerSnapshot(sock string, allow []string) (up, total int, details []containerStatus, err error) {
	cl := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}},
	}
	resp, err := cl.Get("http://unix/containers/json?all=1")
	if err != nil {
		return 0, 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, nil, &httpErr{resp.StatusCode}
	}
	var rows []struct {
		ID    string   `json:"Id"`
		Names []string `json:"Names"`
		State string   `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return 0, 0, nil, err
	}
	details = make([]containerStatus, 0, len(allow))
	byName := map[string]struct{ ID, State string }{}
	for _, row := range rows {
		total++
		if row.State == "running" {
			up++
		}
		for _, name := range row.Names {
			byName[strings.TrimPrefix(name, "/")] = struct{ ID, State string }{row.ID, row.State}
		}
	}
	for _, name := range allow {
		item := containerStatus{Name: name, State: "missing"}
		row, ok := byName[name]
		if !ok {
			details = append(details, item)
			continue
		}
		item.State = row.State
		inspect, inspectErr := cl.Get("http://unix/containers/" + url.PathEscape(row.ID) + "/json")
		if inspectErr == nil {
			var body struct {
				RestartCount int `json:"RestartCount"`
				State        struct {
					Status string `json:"Status"`
					Health *struct {
						Status string `json:"Status"`
					} `json:"Health"`
				} `json:"State"`
			}
			if inspect.StatusCode == http.StatusOK && json.NewDecoder(inspect.Body).Decode(&body) == nil {
				item.State, item.RestartCount = body.State.Status, body.RestartCount
				if body.State.Health != nil {
					item.Health = body.State.Health.Status
				}
			} else {
				item.Health = "unknown"
			}
			inspect.Body.Close()
		} else {
			item.Health = "unknown"
		}
		details = append(details, item)
	}
	return up, total, details, nil
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
