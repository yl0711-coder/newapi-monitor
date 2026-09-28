//go:build unix

package monitor

import (
	"context"
	_ "embed"
	"github.com/gin-gonic/gin"
	"net/http"
)

//go:embed finance_gift_local.html
var financeGiftLocalHTML string

func (m *Monitor) giftTaskAvailable(c *gin.Context) bool {
	if !m.cfg.FinanceGiftHandoffLocalExecutionEnabled {
		c.JSON(404, gin.H{"error": "本地交接执行未启用"})
		return false
	}
	if m.shuttingDown.Load() || m.validateGiftLocalExecution() != nil {
		c.JSON(503, gin.H{"error": "本地隔离条件不满足或正在退出，未启动任务"})
		return false
	}
	if reader := m.financeFactsReadStore(); reader == nil || reader == m.usageFactsStore() {
		c.JSON(503, gin.H{"error": "独立只读事实连接未就绪，未启动任务"})
		return false
	}
	return m.giftAuthorizationAvailable(c)
}

func (m *Monitor) serveFinanceGiftLocalPage(c *gin.Context) {
	if !m.giftTaskAvailable(c) {
		return
	}
	c.Header("X-Frame-Options", "DENY")
	c.Header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	c.Data(200, "text/html; charset=utf-8", []byte(financeGiftLocalHTML))
}

func (m *Monitor) serveFinanceGiftLocalControl(c *gin.Context) {
	if !m.giftTaskAvailable(c) {
		return
	}
	view, err := m.financeGiftTask.snapshot()
	if err != nil {
		c.JSON(503, gin.H{"error": "任务控制暂不可用"})
		return
	}
	c.JSON(200, gin.H{"control": view, "confirmation": m.cfg.FinanceGiftHandoffPreviewSHA256, "execution_enabled": true, "mode": "local_snapshot_only", "preview_retry_after_seconds": m.financeGiftPreviewRetrySeconds()})
}

type financeGiftTaskRequest struct {
	Instance  string `json:"instance"`
	Revision  uint64 `json:"revision"`
	RequestID string `json:"request_id"`
}

func (m *Monitor) serveFinanceGiftLocalTaskAction(c *gin.Context, stop bool) {
	if !m.giftTaskAvailable(c) {
		return
	}
	var request financeGiftTaskRequest
	if !giftAuthorizationJSON(c, &request) {
		return
	}
	if _, err := financeGiftAuthorizationID(request.RequestID); err != nil {
		c.JSON(400, gin.H{"error": "操作请求编号无效"})
		return
	}
	row, _, ok := m.giftAuthorizationReadRequest(c)
	if !ok {
		return
	}
	expected := financeGiftTaskView{Instance: request.Instance, Revision: request.Revision, TaskID: row.ID, RequestID: request.RequestID}
	var view financeGiftTaskView
	var err error
	if stop {
		view, err = m.financeGiftTask.stop(expected)
	} else {
		if _, _, err := m.loadGiftExecutionAuthorization(c.Request.Context(), row.ID); err != nil {
			c.JSON(409, gin.H{"error": "授权已撤销、过期或配置变化，请重新核验"})
			return
		}
		// Accepted tasks belong to the controller, not the HTTP request lifetime.
		view, err = m.financeGiftTask.start(expected, func(ctx context.Context) (string, error) {
			progress, err := m.runFinanceGiftAuthorizedLocal(ctx, row.ID, waitFinanceGiftScopeBatch)
			return progress.Status, err
		})
	}
	if err != nil {
		c.JSON(409, gin.H{"error": "已有任务执行中、页面状态已变化或服务已重启，请刷新后再操作", "control": view})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"control": view})
}

func (m *Monitor) serveFinanceGiftLocalStart(c *gin.Context) {
	m.serveFinanceGiftLocalTaskAction(c, false)
}
func (m *Monitor) serveFinanceGiftLocalStop(c *gin.Context) {
	m.serveFinanceGiftLocalTaskAction(c, true)
}
