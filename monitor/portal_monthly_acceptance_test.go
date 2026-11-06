package monitor

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Explicit manual acceptance server, excluded from the production binary.
// No settings/environment loader, production connections or collectors are run.
func TestMonthlyLocalAcceptanceServer(t *testing.T) {
	if os.Getenv("MONTHLY_ACCEPTANCE_SERVER") != "1" {
		t.Skip("manual loopback-only acceptance server")
	}
	raw, err := os.ReadFile(os.Getenv("MONTHLY_LOCAL_EVIDENCE"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		SessionSecret: randomSecret(), PortalMonthlyStatementEnabled: true,
		LocalSnapshotOnly: true, AlertsDisabled: true,
		StorePath: filepath.Join(dir, "acceptance.db"),
	}, chNames: map[string]string{}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	pw, err := hashPassword("BillLocal2026!")
	if err != nil {
		t.Fatal(err)
	}
	g := CustomerGroup{ID: e.GroupID, Name: "italarm", PortalEmail: "bill@local.test", PortalPwAdmin: pw}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, u := range e.Users {
		if err := m.storeDB.Create(&TrackedUser{UserID: u.UserID, GroupID: g.ID, Username: "italarm"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, raw, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	_ = r.SetTrustedProxies(nil)
	// Only the monthly-bill feature is in scope; don't fabricate usage details.
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/api/") && !strings.HasPrefix(c.Request.URL.Path, "/api/monthly-statement") {
			c.AbortWithStatusJSON(503, gin.H{"error": "本机验收仅加载月度账单，未连接线上使用日志"})
			return
		}
		if c.Request.Method == "GET" && c.Request.URL.Path == "/" {
			original := c.Writer
			buffer := newPortalBufferedResponseWriter(original)
			c.Writer = buffer
			c.Next()
			c.Writer = original
			// Same UI renderer, with only the initial navigation changed for this fixture.
			if buffer.Status() == http.StatusOK {
				page := strings.Replace(buffer.body.String(), "\nload();\n", "\nshowView('monthly');\n", 1)
				buffer.body.Reset()
				buffer.body.WriteString(page)
			}
			buffer.commitTo(original)
		}
	})
	m.RegisterPortalRoutes(r)
	ln, err := net.Listen("tcp4", "127.0.0.1:28111")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	defer srv.Close()
	defer close(m.shutdownSignal())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); _ = srv.Close() }()
	t.Log("LOCAL_MONTHLY_ACCEPTANCE http://127.0.0.1:28111/login")
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}
