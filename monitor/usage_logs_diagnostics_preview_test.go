package monitor

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Manual, opt-in preview of the real embedded Monitor page and log handlers.
// No SettingsFromEnv, Monitor.Start, source DSN, external credentials or jobs.
// Both databases are generated locally and removed by the test on exit.
func TestUsageLogsDiagnosticPreview(t *testing.T) {
	if os.Getenv("MONITOR_USAGE_LOGS_DIAGNOSTIC_PREVIEW") != "1" {
		t.Skip("manual synthetic loopback preview only")
	}
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.LocalAuthBypass = true
	m.cfg.LocalSnapshotOnly = true
	m.cfg.AlertsDisabled = true
	m.cfg.SessionSecret = "synthetic-log-preview-only"
	if err := validateLocalAuthBypassSettings(m.cfg); err != nil {
		t.Fatal(err)
	}
	m.prodDB = newUsageLogsFakeProd(t)
	if local, err := m.storeDB.DB(); err == nil {
		t.Cleanup(func() { _ = local.Close() })
	}
	now := time.Now().Unix()
	var rows []usageLogRow
	for i := 1; i <= 30; i++ {
		rows = append(rows, usageLogRow{int64(i), 31, now - 600 - int64(i*20), 2,
			"demo-user", "demo-token", "claude-demo", 50000, 1200, 240, 7,
			"演示分组", fmt.Sprintf("demo-normal-%02d", i), `{"frt":400}`})
	}
	rows = append(rows,
		usageLogRow{101, 31, now - 10, 2, "demo-user", "demo-token", "claude-demo", 15000, 800, 30, 7, "演示分组", "demo-refusal-001", `{"frt":600,"admin_info":{"reject_reason":"claude_stop_reason=refusal"}}`},
		usageLogRow{102, 31, now - 20, 2, "demo-user", "demo-token", "claude-demo", 12000, 500, 25, 7, "演示分组", "demo-refusal-002", `{"frt":700,"admin_info":{"reject_reason":"claude_stop_reason=refusal; 演示：模型拒绝回答"}}`},
		usageLogRow{103, 31, now - 30, 5, "demo-user", "demo-token", "gpt-demo", 0, 0, 0, 9, "演示分组", "demo-error-ultra", `{}`},
		usageLogRow{104, 31, now - 40, 5, "demo-user", "demo-token", "claude-demo", 0, 0, 0, 7, "演示分组", "demo-error-rate-limit", `{}`},
		usageLogRow{105, 32, now - 50, 2, "demo-other-user", "demo-other-token", "claude-demo", 20000, 750, 30, 9, "演示分组", "demo-legacy-refusal", `{"reject_reason":"claude_stop_reason=refusal; 演示：历史格式","admin_info":null}`},
		usageLogRow{106, 32, now - 60, 2, "demo-other-user", "demo-other-token", "claude-demo", 20000, 750, 30, 9, "演示分组", "demo-other-rejection", `{"admin_info":{"reject_reason":"演示：safety policy blocked"}}`},
	)
	insertUsageLogs(t, m.prodDB, rows...)
	// A refusal does not replace the billing summary. Populate both in this
	// synthetic preview instead of falling back to the fixture word "content".
	expression := base64.StdEncoding.EncodeToString([]byte(`tier("base", p * 4 + c * 20 + cr * 0.4)`))
	if _, err := m.prodDB.Exec(`UPDATE logs SET other=json_set(other,'$.billing_mode','tiered_expr','$.matched_tier','base','$.expr_b64',?,'$.cache_tokens',30),content='' WHERE type=2`, expression); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE logs SET content='status_code=400, Invalid value: ultra. 演示参数错误' WHERE id=103`,
		`UPDATE logs SET content='status_code=429, rate_limit_exceeded. 演示上游限流' WHERE id=104`,
		`UPDATE users SET username='demo-user',display_name='演示账号A' WHERE id=31`,
		`INSERT INTO users(id,username,display_name) VALUES(32,'demo-other-user','演示账号B')`,
		`UPDATE channels SET name=CASE id WHEN 7 THEN '演示-Claude渠道' ELSE '演示-备用渠道' END`,
		`INSERT INTO options VALUES('general_setting.quota_display_type','CNY'),('USDExchangeRate','1'),('GroupRatio','{"演示分组":1}')`,
	} {
		if _, err := m.prodDB.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	r := gin.New()
	// Only static assets, local identity and usage-log reads are reachable.
	// Even accidental clicks elsewhere cannot start a job or save settings.
	r.Use(func(c *gin.Context) {
		p := c.Request.URL.Path
		allowed := p == "/" || p == "/me" || strings.HasSuffix(p, ".css") || strings.HasSuffix(p, ".js") ||
			p == "/usage-logs/meta" || p == "/usage-logs/list" || p == "/usage-logs/count" || p == "/usage-logs/stat" || p == "/usage-logs/shortcuts" || strings.HasPrefix(p, "/usage-logs/user/")
		if c.Request.Method != http.MethodGet || !allowed {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"success": false, "message": "本机合成预览：仅开放使用日志读取，其他功能未接入"})
			return
		}
		c.Header("Cache-Control", "no-store")
	})
	m.RegisterRoutes(r)
	addr := "127.0.0.1:0"
	if value := os.Getenv("MONITOR_USAGE_LOGS_DIAGNOSTIC_PREVIEW_PORT"); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1024 || port > 65535 {
			t.Fatal("preview port must be an unprivileged TCP port")
		}
		addr = fmt.Sprintf("127.0.0.1:%d", port)
	}
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	t.Cleanup(func() { signal.Stop(stop) })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	t.Logf("PREVIEW_URL=http://%s/#tab=usage-logs", listener.Addr())
	t.Log("Synthetic data only: 30 normal consumes, 4 consumes with rejection reasons, 2 API errors. No workers/notifications/external sources; expires after 8 hours.")
	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Hour):
	case <-stop:
	}
}
