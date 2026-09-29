//go:build unix

package monitor

import "github.com/gin-gonic/gin"

func (m *Monitor) giftLiveTaskAvailable(c *gin.Context) bool {
	if !m.cfg.FinanceGiftHandoffLiveExecutionEnabled {
		c.JSON(404, gin.H{"error": "在线有限交接未启用"})
		return false
	}
	if m.validateGiftLiveExecution() != nil {
		c.JSON(503, gin.H{"error": "在线交接配置或存储未就绪，或服务正在退出；未启动任务"})
		return false
	}
	return m.giftAuthorizationAvailable(c)
}

func (m *Monitor) serveFinanceGiftLivePage(c *gin.Context) {
	if m.giftLiveTaskAvailable(c) {
		serveFinanceGiftTaskPage(c, true)
	}
}

func (m *Monitor) serveFinanceGiftLiveControl(c *gin.Context) {
	if m.giftLiveTaskAvailable(c) {
		m.serveFinanceGiftTaskControl(c, "live_finite_handoff")
	}
}
