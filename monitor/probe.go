package monitor

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// probe.go:端到端可用性探活。周期性对每个前端域名做 HTTPS 探活(GET <path>),
// 记录 HTTP 状态码 + 响应延时(ms) + 是否可达;并 tls.Dial 读 leaf 证书剩余天数。
// 结果以 InfraSample(rtype="probe")写本地库,computeInfraSnapshot 时带出。
// fail-open:探活失败只记日志,不影响主流程;受 MONITOR_INFRA_ENABLED 同一开关控制。
//
// 存储到 infra_samples 的指标(resource=域名,rtype="probe"):
//   reachable  = 1 可达 / 0 不可达
//   status_code= HTTP 状态码(不可达=0)
//   latency_ms = 请求往返延时(毫秒)
//   cert_days  = TLS 证书剩余天数(取不到=不写该指标)

// probeHTTPTimeout 单次 HTTPS 探活的总超时。
const probeHTTPTimeout = 10 * time.Second

// probeResult 是一次探活的原始结果。
type probeResult struct {
	domain    string
	reachable bool
	status    int
	latencyMs float64
	certDays  float64
	hasCert   bool
	viaCDN    bool // 响应是否经 CloudFront(Via/X-Amz-Cf-Id 头);用于检测域名脱离 CDN 的漂移
}

// lockResult 是一次源站锁检查的原始结果(直连源站不带头,期望 403)。
type lockResult struct {
	target    string
	reachable bool // 是否拿到 HTTP 响应(连不上=false,不写样本,显示无数据而非误报)
	status    int  // 实际返回码(期望 403)
	locked    bool // status==403 即认为锁生效
}

// startProbe 启动端到端探活的独立循环(独立于 infra/日志采样)。
func (m *Monitor) startProbe(ctx context.Context) {
	if !m.cfg.InfraEnabled {
		return
	}
	domains := probeDomainList(m.cfg.ProbeDomains)
	if len(domains) == 0 {
		slog.Warn("端到端探活:未配置域名(MONITOR_PROBE_DOMAINS 为空),不启动")
		return
	}
	iv := time.Duration(m.cfg.ProbeSeconds) * time.Second
	if iv < 10*time.Second {
		iv = 10 * time.Second
	}
	locks := probeDomainList(m.cfg.OriginLockTargets) // 复用解析器(去空白/去重);源站端点 host:port
	m.runProbe(ctx, domains, locks)                   // 启动即探一轮
	go func() {
		t := time.NewTicker(iv)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.runProbe(ctx, domains, locks)
			}
		}
	}()
	slog.Info("端到端探活已启动", "interval", iv.String(), "domains", strings.Join(domains, ","), "path", probePath(m.cfg.ProbePath), "lock_targets", strings.Join(locks, ","))
}

// runProbe 对所有域名各探一次 + 对各源站做锁检查,并入库(同一分钟桶,幂等覆盖)。
func (m *Monitor) runProbe(ctx context.Context, domains, locks []string) {
	bucket := time.Now().Unix() / 60 * 60
	path := probePath(m.cfg.ProbePath)
	var rows []InfraSample
	var ok int
	for _, d := range domains {
		r := m.probeOne(ctx, d, path)
		add := func(metric string, v float64) {
			rows = append(rows, InfraSample{BucketTs: bucket, Resource: d, RType: "probe", Metric: metric, Value: v})
		}
		if r.reachable {
			add("reachable", 1)
			ok++
		} else {
			add("reachable", 0)
		}
		add("status_code", float64(r.status))
		add("latency_ms", r.latencyMs)
		add("via_cdn", b2f(r.viaCDN))
		if r.hasCert {
			add("cert_days", r.certDays)
		}
	}
	// 源站锁检查:连不上的目标不写样本(显示无数据,不误报);拿到响应才写 locked/status_code。
	var lockOK int
	host := strings.TrimSpace(m.cfg.OriginLockHost)
	if host == "" {
		host = "nexusapi.link"
	}
	lpath := probePath(m.cfg.OriginLockPath)
	for _, t := range locks {
		r := m.probeOriginLock(ctx, t, host, lpath)
		if !r.reachable {
			continue
		}
		rows = append(rows, InfraSample{BucketTs: bucket, Resource: t, RType: "lock", Metric: "locked", Value: b2f(r.locked)})
		rows = append(rows, InfraSample{BucketTs: bucket, Resource: t, RType: "lock", Metric: "status_code", Value: float64(r.status)})
		if r.locked {
			lockOK++
		}
	}
	if err := m.upsertInfra(rows); err != nil {
		slog.Warn("端到端探活入库失败(忽略)", "err", err)
		return
	}
	slog.Info("端到端探活完成", "domains", len(domains), "reachable", ok, "lock_targets", len(locks), "lock_ok", lockOK)
}

// b2f 把 bool 映成 0/1(入库为 float)。
func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// probeOne 探测单个域名:HTTPS GET <path> 取状态码/延时,再 tls.Dial 取证书剩余天数。
func (m *Monitor) probeOne(ctx context.Context, domain, path string) probeResult {
	res := probeResult{domain: domain}

	// 1) HTTPS 探活
	cctx, cancel := context.WithTimeout(ctx, probeHTTPTimeout)
	defer cancel()
	url := "https://" + domain + path
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Warn("端到端探活:构造请求失败", "domain", domain, "err", err)
	} else {
		client := &http.Client{Timeout: probeHTTPTimeout}
		start := time.Now()
		resp, err := client.Do(req)
		res.latencyMs = float64(time.Since(start).Microseconds()) / 1000.0
		if err != nil {
			slog.Warn("端到端探活:请求失败", "domain", domain, "err", err)
		} else {
			res.reachable = true
			res.status = resp.StatusCode
			res.viaCDN = isCloudFront(resp.Header)
			resp.Body.Close()
		}
	}

	// 2) TLS 证书剩余天数(独立于上面的 HTTP,即便 HTTP 非 200 也能拿到证书)
	if days, ok := certDaysLeft(ctx, domain); ok {
		res.certDays = days
		res.hasCert = true
	}
	return res
}

// isCloudFront 判断响应是否经 CloudFront(看 Via 含 cloudfront 或存在 X-Amz-Cf-Id 头)。
func isCloudFront(h http.Header) bool {
	if strings.Contains(strings.ToLower(h.Get("Via")), "cloudfront") {
		return true
	}
	return h.Get("X-Amz-Cf-Id") != ""
}

// probeOriginLock 直连源站端点(http://<target><path>),带 Host 头但【不带】X-Origin-Verify,
// 期望被 nginx 拦 403(F-5 锁生效)。不跟随重定向(锁失效时 301/200 都算未锁)。
// 连不上(私网不可达/实例宕)时 reachable=false,调用方不写样本——避免本地预览或宕机误报"锁失效"。
func (m *Monitor) probeOriginLock(ctx context.Context, target, host, path string) lockResult {
	res := lockResult{target: target}
	cctx, cancel := context.WithTimeout(ctx, probeHTTPTimeout)
	defer cancel()
	url := "http://" + target + path
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Warn("源站锁检查:构造请求失败", "target", target, "err", err)
		return res
	}
	req.Host = host // 源站靠 Host 选 vhost;不设任何 X-Origin-Verify
	client := &http.Client{
		Timeout: probeHTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse // 不跟随:锁失效时可能 301→https,需如实记为非 403
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		// 连不上不视为"锁失效",仅记日志、不写样本(reachable 保持 false)。
		slog.Warn("源站锁检查:连不上目标(忽略,不计为锁失效)", "target", target, "err", err)
		return res
	}
	defer resp.Body.Close()
	res.reachable = true
	res.status = resp.StatusCode
	res.locked = resp.StatusCode == 403
	return res
}

// certDaysLeft 用 tls.Dial 连 <domain>:443,取 leaf 证书 NotAfter 算剩余天数。
func certDaysLeft(ctx context.Context, domain string) (float64, bool) {
	dctx, cancel := context.WithTimeout(ctx, probeHTTPTimeout)
	defer cancel()
	d := &net.Dialer{Timeout: probeHTTPTimeout}
	conn, err := tls.DialWithDialer(d, "tcp", net.JoinHostPort(domain, "443"), &tls.Config{ServerName: domain})
	if err != nil {
		// 用 ctx 取消防止泄漏(DialWithDialer 不接 ctx;此处保持与超时一致即可)
		_ = dctx
		slog.Warn("端到端探活:TLS 握手失败", "domain", domain, "err", err)
		return 0, false
	}
	defer conn.Close()
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return 0, false
	}
	leaf := certs[0]
	return time.Until(leaf.NotAfter).Hours() / 24, true
}

// probeDomainList 解析逗号分隔域名(去空白、去空项、去重),容忍误带 https:// 前缀与路径。
func probeDomainList(s string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "https://")
		p = strings.TrimPrefix(p, "http://")
		if i := strings.IndexByte(p, '/'); i >= 0 {
			p = p[:i]
		}
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

func probePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/api/status"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}
