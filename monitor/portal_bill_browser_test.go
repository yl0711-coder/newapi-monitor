package monitor

import (
	"context"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Manual local acceptance ONLY. Not compiled into the delivered application.
// Run inside the isolated synthetic MySQL container, publish HTTP to host
// loopback only, and remove the container afterwards. No arbitrary DSN allowed.
func TestPortalBillBrowserLocalPreview(t *testing.T) {
	if os.Getenv("BILL_BROWSER_LOCAL_PREVIEW") != "1" {
		t.Skip("manual isolated local browser acceptance only")
	}
	source, _ := billSnapshotMySQLFixture(t, 2100)
	m, _, ck, _ := billHTTPFixture(t)
	m.prodDB = source.prodDB
	m.cfg.PortalBillTasksEnabled = true
	r := gin.New()
	r.Use(gin.Recovery())
	// Synthetic test session is injected only in this test binary. Avoid
	// overwriting the user's real localhost Usage cookies, which span ports.
	r.Use(func(c *gin.Context) {
		c.Request.Header.Del("Cookie")
		c.Request.AddCookie(ck)
		c.Next()
	})
	m.RegisterPortalRoutes(r)
	defer m.portalBillService.Close()
	css := regexp.MustCompile(`(?s)<style>(.*?)</style>`).FindStringSubmatch(portalHTML)
	start, end := strings.Index(portalHTML, "let exporting=false"), strings.Index(portalHTML, "// 超 5 万条确认框")
	modalStart, modalEnd := strings.Index(portalHTML, `<div class="modal-mask" id="expMask">`), strings.Index(portalHTML, "<!-- 导出确认")
	contract := regexp.MustCompile(`(?ms)^function logContractError\(.*?^}`).FindString(portalHTML)
	if len(css) != 2 || start < 0 || end <= start || modalStart < 0 || modalEnd <= modalStart || contract == "" {
		t.Fatal("production export UI extraction failed")
	}
	page := `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Usage 真实本机账单验收</title><style>` + css[1] + `body{padding:24px}</style><h2>Usage 账单 · 真实本机接口验收</h2><p>仅本机 MySQL 合成数据，不连接线上。固定范围：2023-11-15；共 2,100 条消费记录。</p><p>实际 Portal 预检、任务、快照与下载接口；仅页面范围选择固定为测试日期。</p><button class="btn" id="billBtn">导出账单</button><button class="btn" id="expBtn">导出使用日志</button>` + portalHTML[modalStart:modalEnd] + `<script>const $=id=>document.getElementById(id);function logQuery(){return 'from=2023-11-15&to=2023-11-15&type=2'}
` + contract + "\n" + portalHTML[start:end] + `</script></html>`
	page = strings.Replace(page, `<script>const $=`, `<button class="exp-btn" id="billResumeBtn" hidden title="恢复上次范围，不应用当前筛选">恢复上次账单</button><script>const $=`, 1)
	r.GET("/acceptance", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Data(200, "text/html; charset=utf-8", []byte(page))
	})
	r.GET("/acceptance/health", func(c *gin.Context) { c.String(200, "local synthetic MySQL ready") })
	srv := &http.Server{Addr: ":8125", Handler: r, ReadHeaderTimeout: 5 * time.Second}
	errC := make(chan error, 1)
	go func() { errC <- srv.ListenAndServe() }()
	t.Log("Local synthetic browser preview ready on container :8125/acceptance; 30 minute lifetime")
	select {
	case err := <-errC:
		t.Fatal(err)
	case <-time.After(30 * time.Minute):
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}
