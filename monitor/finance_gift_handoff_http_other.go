//go:build !unix

package monitor

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (m *Monitor) serveFinanceGiftHandoffPreview(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": "此平台不支持交接只读预检"})
}

func (m *Monitor) serveFinanceGiftHandoffAuthorize(c *gin.Context) {
	m.serveFinanceGiftHandoffPreview(c)
}
func (m *Monitor) serveFinanceGiftHandoffAuthorizationStatus(c *gin.Context) {
	m.serveFinanceGiftHandoffPreview(c)
}
func (m *Monitor) serveFinanceGiftHandoffRevoke(c *gin.Context) { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftLocalPage(c *gin.Context)     { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftLocalControl(c *gin.Context)  { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftLivePage(c *gin.Context)      { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftLiveControl(c *gin.Context)   { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftLocalStart(c *gin.Context)    { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftLocalStop(c *gin.Context)     { m.serveFinanceGiftHandoffPreview(c) }
func (m *Monitor) serveFinanceGiftHandoffProgress(c *gin.Context) {
	m.serveFinanceGiftHandoffPreview(c)
}
