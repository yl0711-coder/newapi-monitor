// Command local-facts-loadtest drives the portal and admin read-only APIs of an
// isolated Monitor acceptance container. It never discovers or calls an online
// URL: both bases must be explicit loopback HTTP addresses.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type options struct {
	portalBase  string
	adminBase   string
	email       string
	password    string
	secret      string
	duration    time.Duration
	report      string
	backupAfter time.Duration
}

type sample struct {
	duration time.Duration
	status   int
	bytes    int64
	err      string
}

type collector struct {
	mu      sync.Mutex
	samples map[string][]sample
}

type endpointReport struct {
	Requests     int         `json:"requests"`
	Errors       int         `json:"errors"`
	StatusCounts map[int]int `json:"status_counts"`
	P50MS        float64     `json:"p50_ms"`
	P95MS        float64     `json:"p95_ms"`
	MaxMS        float64     `json:"max_ms"`
	AverageBytes int64       `json:"average_bytes"`
	TotalBytes   int64       `json:"total_bytes"`
	LastError    string      `json:"last_error,omitempty"`
}

type report struct {
	StartedAt        string                    `json:"started_at"`
	CompletedAt      string                    `json:"completed_at"`
	DurationSeconds  float64                   `json:"duration_seconds"`
	PortalBase       string                    `json:"portal_base"`
	AdminBase        string                    `json:"admin_base"`
	Endpoints        map[string]endpointReport `json:"endpoints"`
	PreflightOK      bool                      `json:"preflight_ok"`
	BackupTriggered  bool                      `json:"backup_triggered"`
	SyntheticOnly    bool                      `json:"synthetic_only"`
	FactsStatusStart factsStatusSnapshot       `json:"facts_status_start"`
	FactsStatusEnd   factsStatusSnapshot       `json:"facts_status_end"`
	CacheStatsStart  json.RawMessage           `json:"cache_stats_start"`
	CacheStatsEnd    json.RawMessage           `json:"cache_stats_end"`
}

type factsStatusSnapshot struct {
	ReadActive               bool   `json:"read_active"`
	CoverageBasis            string `json:"coverage_basis"`
	ExpectedMemberHours      int64  `json:"expected_member_hours"`
	CompleteMemberHours      int64  `json:"complete_member_hours"`
	ProofMigrationRequired   bool   `json:"proof_migration_required"`
	StoreIntegrityOK         bool   `json:"store_integrity_ok"`
	FactsStoreIntegrityOK    bool   `json:"facts_store_integrity_ok"`
	BackupRunning            bool   `json:"backup_running"`
	LastBackupFailureAt      int64  `json:"last_backup_failure_at"`
	FactsLastBackupFailureAt int64  `json:"facts_last_backup_failure_at"`
	LastBackupSuccessAt      int64  `json:"last_backup_success_at"`
	FactsLastBackupSuccessAt int64  `json:"facts_last_backup_success_at"`
}

func main() {
	var opt options
	flag.StringVar(&opt.portalBase, "portal-base", "http://127.0.0.1:28101", "loopback portal base")
	flag.StringVar(&opt.adminBase, "admin-base", "http://127.0.0.1:28100", "loopback admin base")
	flag.StringVar(&opt.email, "email", "director@local.test", "synthetic portal account")
	flag.StringVar(&opt.password, "password", "local-director-pass", "synthetic portal password")
	flag.StringVar(&opt.secret, "session-secret", "local-acceptance-session-only", "local session secret")
	flag.DurationVar(&opt.duration, "duration", 60*time.Minute, "steady mixed-load duration")
	flag.DurationVar(&opt.backupAfter, "backup-after", 2*time.Minute, "trigger one local backup after this duration")
	flag.StringVar(&opt.report, "report", "", "write JSON report")
	flag.Parse()
	if err := run(opt); err != nil {
		fmt.Fprintln(os.Stderr, "local-facts-loadtest:", err)
		os.Exit(1)
	}
}

func run(opt options) error {
	if err := validateLoopbackBase(opt.portalBase); err != nil {
		return fmt.Errorf("portal base: %w", err)
	}
	if err := validateLoopbackBase(opt.adminBase); err != nil {
		return fmt.Errorf("admin base: %w", err)
	}
	if opt.duration < 10*time.Second || opt.duration > 2*time.Hour {
		return errors.New("duration must be between 10s and 2h")
	}
	jar, _ := cookiejar.New(nil)
	transport := &http.Transport{
		Proxy: nil, MaxIdleConns: 32, MaxIdleConnsPerHost: 16, MaxConnsPerHost: 16,
		IdleConnTimeout: 30 * time.Second, DisableCompression: false,
	}
	portalClient := &http.Client{Timeout: 30 * time.Second, Jar: jar, Transport: transport}
	loginBody, _ := json.Marshal(map[string]string{"email": opt.email, "password": opt.password})
	resp, err := portalClient.Post(opt.portalBase+"/login", "application/json", bytes.NewReader(loginBody))
	if err != nil {
		return err
	}
	loginResponse, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(loginResponse, []byte(`"ok":true`)) {
		return fmt.Errorf("portal login status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(loginResponse)))
	}
	adminJar, _ := cookiejar.New(nil)
	adminURL, _ := url.Parse(opt.adminBase)
	adminJar.SetCookies(adminURL, []*http.Cookie{{
		Name: "newapi_monitor_session", Value: signAdminSession(opt.secret, "local-director", 100, time.Now().Unix()), Path: "/",
	}})
	adminClient := &http.Client{Timeout: 30 * time.Second, Jar: adminJar, Transport: transport}

	loc := time.FixedZone("CST", 8*3600)
	today := time.Now().In(loc)
	date := func(daysAgo int) string { return today.AddDate(0, 0, -daysAgo).Format("2006-01-02") }
	overview100 := opt.portalBase + "/api/overview?from=" + date(99) + "&to=" + date(0)
	overview101 := opt.portalBase + "/api/overview?from=" + date(100) + "&to=" + date(0)
	breakdown366 := opt.portalBase + "/api/breakdown?from=" + date(365) + "&to=" + date(0)
	userURL := func(id int) string {
		return fmt.Sprintf("%s/api/user?uid=%d&from=%s&to=%s", opt.portalBase, id, date(365), date(0))
	}
	logs7 := opt.portalBase + "/api/logs?from=" + date(6) + "&to=" + date(0) + "&token=token"
	statusURL := opt.adminBase + "/usage/facts-status"

	c := &collector{samples: make(map[string][]sample)}
	// 黑盒前置断言：20,000 格必须真正返回矩阵，20,200 格必须在查询前拒绝。
	if err := checkedRequest(context.Background(), portalClient, overview100, `"matrix_available":true`); err != nil {
		return fmt.Errorf("20k matrix preflight: %w", err)
	}
	if err := checkedRequest(context.Background(), portalClient, overview101, `"matrix_available":false`); err != nil {
		return fmt.Errorf("over-limit matrix preflight: %w", err)
	}
	if err := checkedRequest(context.Background(), adminClient, statusURL, `"read_active":true`); err != nil {
		return fmt.Errorf("facts read-active preflight: %w", err)
	}
	factsSnapshotStart, err := readFactsStatus(context.Background(), adminClient, statusURL)
	if err != nil {
		return fmt.Errorf("facts status snapshot: %w", err)
	}
	cacheStatsURL := opt.adminBase + "/usage/cache-stats"
	cacheStatsStart, err := readJSONSnapshot(context.Background(), adminClient, cacheStatsURL)
	if err != nil {
		return fmt.Errorf("cache stats snapshot: %w", err)
	}

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), opt.duration)
	defer cancel()
	var wg sync.WaitGroup
	startTicker := func(name string, every time.Duration, client *http.Client, makeURL func(int) string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					seq++
					c.do(ctx, name, client, http.MethodGet, makeURL(seq), nil)
				}
			}
		}()
	}
	startTicker("matrix_20k", 20*time.Second, portalClient, func(int) string { return overview100 })
	startTicker("matrix_guard", 10*time.Second, portalClient, func(int) string { return overview101 })
	startTicker("breakdown_366d", 10*time.Second, portalClient, func(int) string { return breakdown366 })
	startTicker("member_366d", 500*time.Millisecond, portalClient, func(seq int) string { return userURL(seq%8 + 1) })
	startTicker("raw_fuzzy_7d", 30*time.Second, portalClient, func(int) string { return logs7 })
	startTicker("facts_status", 15*time.Second, adminClient, func(int) string { return statusURL })

	backupTriggered := false
	if opt.backupAfter > 0 {
		backupTimer := time.NewTimer(min(opt.backupAfter, opt.duration/2))
		select {
		case <-ctx.Done():
		case <-backupTimer.C:
			s := c.do(ctx, "manual_backup", adminClient, http.MethodPost, opt.adminBase+"/admin/store/backup", nil)
			backupTriggered = s.status == http.StatusAccepted
			<-ctx.Done()
		}
		if !backupTimer.Stop() {
			select {
			case <-backupTimer.C:
			default:
			}
		}
	} else {
		<-ctx.Done()
	}
	wg.Wait()
	completed := time.Now()
	// The load context has expired by design. Use fresh bounded requests so the
	// report records terminal health/counters instead of only the preflight state.
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer snapshotCancel()
	factsSnapshotEnd, err := readFactsStatus(snapshotCtx, adminClient, statusURL)
	if err != nil {
		return fmt.Errorf("terminal facts status snapshot: %w", err)
	}
	cacheStatsEnd, err := readJSONSnapshot(snapshotCtx, adminClient, cacheStatsURL)
	if err != nil {
		return fmt.Errorf("terminal cache stats snapshot: %w", err)
	}
	out := report{
		StartedAt: started.Format(time.RFC3339), CompletedAt: completed.Format(time.RFC3339),
		DurationSeconds: completed.Sub(started).Seconds(), PortalBase: opt.portalBase, AdminBase: opt.adminBase,
		Endpoints: c.report(), PreflightOK: true, BackupTriggered: backupTriggered, SyntheticOnly: true,
		FactsStatusStart: factsSnapshotStart, FactsStatusEnd: factsSnapshotEnd,
		CacheStatsStart: cacheStatsStart, CacheStatsEnd: cacheStatsEnd,
	}
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if opt.report != "" {
		if err := os.WriteFile(opt.report, append(encoded, '\n'), 0o644); err != nil {
			return err
		}
	}
	fmt.Println(string(encoded))
	return nil
}

func readJSONSnapshot(ctx context.Context, client *http.Client, endpoint string) (json.RawMessage, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, errors.New("response is not valid JSON")
	}
	return json.RawMessage(body), nil
}

func readFactsStatus(ctx context.Context, client *http.Client, endpoint string) (factsStatusSnapshot, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	resp, err := client.Do(req)
	if err != nil {
		return factsStatusSnapshot{}, err
	}
	defer resp.Body.Close()
	var body struct {
		Facts struct {
			ReadActive             bool   `json:"read_active"`
			CoverageBasis          string `json:"coverage_basis"`
			ExpectedMemberHours    int64  `json:"expected_member_hours"`
			CompleteMemberHours    int64  `json:"complete_member_hours"`
			ProofMigrationRequired bool   `json:"proof_migration_required"`
		} `json:"facts"`
		Store struct {
			IntegrityOK         bool  `json:"integrity_ok"`
			BackupRunning       bool  `json:"backup_running"`
			LastBackupFailureAt int64 `json:"last_backup_failure_at"`
			LastBackupSuccessAt int64 `json:"last_backup_success_at"`
		} `json:"store"`
		FactsStore struct {
			IntegrityOK         bool  `json:"integrity_ok"`
			BackupRunning       bool  `json:"backup_running"`
			LastBackupFailureAt int64 `json:"last_backup_failure_at"`
			LastBackupSuccessAt int64 `json:"last_backup_success_at"`
		} `json:"facts_store"`
	}
	if resp.StatusCode != http.StatusOK {
		return factsStatusSnapshot{}, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return factsStatusSnapshot{}, err
	}
	return factsStatusSnapshot{
		ReadActive: body.Facts.ReadActive, CoverageBasis: body.Facts.CoverageBasis,
		ExpectedMemberHours: body.Facts.ExpectedMemberHours, CompleteMemberHours: body.Facts.CompleteMemberHours,
		ProofMigrationRequired: body.Facts.ProofMigrationRequired,
		StoreIntegrityOK:       body.Store.IntegrityOK, FactsStoreIntegrityOK: body.FactsStore.IntegrityOK,
		BackupRunning:            body.Store.BackupRunning || body.FactsStore.BackupRunning,
		LastBackupFailureAt:      body.Store.LastBackupFailureAt,
		FactsLastBackupFailureAt: body.FactsStore.LastBackupFailureAt,
		LastBackupSuccessAt:      body.Store.LastBackupSuccessAt,
		FactsLastBackupSuccessAt: body.FactsStore.LastBackupSuccessAt,
	}, nil
}

func validateLoopbackBase(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("must be a plain loopback http origin")
	}
	host := u.Hostname()
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing non-loopback host %q", host)
	}
	return nil
}

func signAdminSession(secret, name string, role int, now int64) string {
	payload := fmt.Sprintf("%s|%d|%d", strings.ReplaceAll(name, "|", "/"), role, now)
	encoded := base64.RawURLEncoding.EncodeToString([]byte(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(encoded))
	return encoded + "." + hex.EncodeToString(mac.Sum(nil))
}

func checkedRequest(ctx context.Context, client *http.Client, endpoint, marker string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(marker)) {
		return fmt.Errorf("status=%d bytes=%d missing=%s body_prefix=%q", resp.StatusCode, len(body), marker, body[:min(len(body), 256)])
	}
	return nil
}

func (c *collector) do(ctx context.Context, name string, client *http.Client, method, endpoint string, body io.Reader) sample {
	started := time.Now()
	s := sample{}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err == nil {
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			err = requestErr
		} else {
			s.status = resp.StatusCode
			s.bytes, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if err == nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
				err = fmt.Errorf("HTTP %d", resp.StatusCode)
			}
		}
	}
	s.duration = time.Since(started)
	// 压测截止瞬间的在途请求是调度器正常取消，不是 HTTP=0 的业务样本。
	if err != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return s
	}
	if err != nil {
		s.err = err.Error()
	}
	c.mu.Lock()
	c.samples[name] = append(c.samples[name], s)
	c.mu.Unlock()
	return s
}

func (c *collector) report() map[string]endpointReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]endpointReport, len(c.samples))
	for name, samples := range c.samples {
		latencies := make([]time.Duration, 0, len(samples))
		r := endpointReport{Requests: len(samples), StatusCounts: map[int]int{}}
		for _, s := range samples {
			latencies = append(latencies, s.duration)
			r.StatusCounts[s.status]++
			r.TotalBytes += s.bytes
			if s.err != "" {
				r.Errors++
				r.LastError = s.err
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		if len(latencies) > 0 {
			toMS := func(v time.Duration) float64 { return float64(v.Microseconds()) / 1000 }
			p95 := (len(latencies)*95 + 99) / 100
			r.P50MS = toMS(latencies[len(latencies)/2])
			r.P95MS = toMS(latencies[p95-1])
			r.MaxMS = toMS(latencies[len(latencies)-1])
			r.AverageBytes = r.TotalBytes / int64(len(latencies))
		}
		out[name] = r
	}
	return out
}
