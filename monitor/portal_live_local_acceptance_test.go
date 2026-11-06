package monitor

// Opt-in, loopback-only manual acceptance. Never part of the product binary.
// Real MySQL is SELECT-only; the Monitor database is freshly created locally.
// No collectors, automatic generation, fake bill approval or production login
// credentials are loaded. Missing real monthly evidence stays unavailable.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

func TestUsageRealReadonlyLocalAcceptanceServer(t *testing.T) {
	root := os.Getenv("USAGE_REAL_READONLY_ACCEPTANCE")
	if root == "" {
		t.Skip("explicit private live-readonly acceptance workspace required")
	}
	info, err := os.Lstat(root)
	canonical, canonicalErr := filepath.EvalSymlinks(root)
	if err != nil || canonicalErr != nil || !info.IsDir() || info.Mode().Perm() != 0700 || root != canonical || !filepath.IsAbs(root) {
		t.Fatal("private canonical workspace required")
	}
	read := func(name string) []byte {
		t.Helper()
		p := filepath.Join(root, name)
		st, err := os.Lstat(p)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > 262144 {
			t.Fatal("private input unavailable")
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal("private input read failed")
		}
		return b
	}
	var meta struct {
		Groups []struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"groups"`
		Users []TrackedUser `json:"users"`
	}
	groupID, groupErr := strconv.ParseInt(os.Getenv("USAGE_REAL_READONLY_GROUP_ID"), 10, 64)
	userID, userErr := strconv.ParseInt(os.Getenv("USAGE_REAL_READONLY_USER_ID"), 10, 64)
	if groupErr != nil || userErr != nil || groupID <= 0 || userID <= 0 || json.Unmarshal(read("organization.json"), &meta) != nil || len(meta.Groups) != 1 || meta.Groups[0].ID != groupID || len(meta.Users) != 1 || meta.Users[0].GroupID != groupID || meta.Users[0].UserID != userID {
		t.Fatal("explicit real acceptance scope differs")
	}
	var dsn string
	for _, line := range strings.Split(string(read("readonly.env")), "\n") {
		if strings.HasPrefix(line, "NEWAPI_LOG_DSN=") {
			if dsn != "" {
				t.Fatal("duplicate configuration")
			}
			dsn = strings.TrimSpace(strings.TrimPrefix(line, "NEWAPI_LOG_DSN="))
		}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.User != "monitor_ro" || cfg.DBName != "nexusapi" || cfg.Net != "tcp" || cfg.Addr != "host.docker.internal:13356" || cfg.MultiStatements {
		t.Fatal("unexpected readonly configuration")
	}
	cfg.Addr = "127.0.0.1:13356"
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 2*time.Second, 4*time.Second, 2*time.Second
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["max_execution_time"] = "2000"
	store := filepath.Join(root, "usage-acceptance.db")
	if _, err := os.Lstat(store); !os.IsNotExist(err) {
		t.Fatal("fresh local receiver required; never overwrite")
	}
	m, err := New(Settings{ProdDSN: cfg.FormatDSN(), StorePath: store, AlertsDisabled: true, SessionSecret: randomSecret(), PortalAddr: "127.0.0.1:8196", PortalMonthlyStatementEnabled: true, sourceLifecycleConfigured: true})
	if err != nil {
		t.Fatal("local Monitor initialization failed; private diagnostic withheld")
	}
	defer m.Close()
	m.prodDB.SetMaxOpenConns(1)
	m.prodDB.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var uuid, database, account string
	err = m.prodDB.QueryRowContext(ctx, "SELECT @@server_uuid,DATABASE(),CURRENT_USER()").Scan(&uuid, &database, &account)
	cancel()
	pin := sha256.Sum256([]byte("mysql-wallet-v1\x00" + uuid + "\x00" + database))
	if err != nil || database != "nexusapi" || !strings.HasPrefix(account, "monitor_ro@") || hex.EncodeToString(pin[:]) != "f3cd458aac20374cf8b52dcdf0c4baf8d3e90c966ae42229365cc30399968937" {
		t.Fatal("readonly source identity differs")
	}
	password := "LocalUsage2026!"
	pw, err := hashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	g := CustomerGroup{ID: groupID, Name: meta.Groups[0].Name, PortalEmail: "usage-local@example.invalid", PortalPwAdmin: pw}
	if m.storeDB.Create(&g).Error != nil || m.storeDB.Create(&meta.Users).Error != nil {
		t.Fatal("local metadata creation failed")
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	_ = r.SetTrustedProxies(nil)
	r.Use(func(c *gin.Context) {
		if c.Request.Host != "localhost:8196" && c.Request.Host != "127.0.0.1:8196" {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		c.Header("Cache-Control", "private, no-store")
		c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; form-action 'self'")
		if c.Request.Method != http.MethodGet && c.Request.URL.Path != "/login" && !strings.HasPrefix(c.Request.URL.Path, "/api/logs/") {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		if c.Request.Method == http.MethodGet && c.Request.URL.Path == "/" {
			original := c.Writer
			buffer := newPortalBufferedResponseWriter(original)
			c.Writer = buffer
			c.Next()
			c.Writer = original
			if buffer.Status() == http.StatusOK {
				page := strings.Replace(buffer.body.String(), "<body>", `<body><p style="padding:12px;color:#9a6700">本机真实只读验收 · 单组织隔离 · 使用日志经 SSH 读取线上；月度账单仅显示本机已核验资料，不自动生成。无资料不代表无消费。</p>`, 1)
				buffer.body.Reset()
				buffer.body.WriteString(page)
			}
			buffer.commitTo(original)
			return
		}
		c.Next()
	})
	m.RegisterPortalRoutes(r)
	ln, err := net.Listen("tcp4", "127.0.0.1:8196")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	defer srv.Close()
	stop, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()
	lifetime := os.Getenv("USAGE_REAL_READONLY_LIFETIME")
	if lifetime != "" && lifetime != "until-user-stop" {
		t.Fatal("invalid explicit acceptance lifetime")
	}
	go func() {
		if lifetime == "until-user-stop" {
			<-stop.Done()
		} else {
			select {
			case <-stop.Done():
			case <-time.After(45 * time.Minute):
			}
		}
		_ = srv.Close()
	}()
	// Deliberately NEVER call m.Start: no source supervisor or collectors.
	t.Logf("REAL_READONLY_USAGE http://localhost:8196/login · local login usage-local@example.invalid / LocalUsage2026! · group %d / user %d only · no monthly publication", groupID, userID)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}
