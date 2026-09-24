package monitor

// 客户维护的 HTTP 边界。
//
// 报表和名单都是客户维护自己的域；名单写接口位于
// /customer-health 下，与用户用量的 /usage 写接口完全分开。

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// serveCustomerHealthReport GET /customer-health/report
//
// 读本地分钟事实，不查生产库。参数为空：本页固定看"今天"，不提供时间范围
// ——要看历史趋势去稳定性报表，要看某条请求去客户排障。
func (m *Monitor) serveCustomerHealthReport(c *gin.Context) {
	// storeDB 为空说明本地库还没初始化完。明确 503，不返回空列表：
	// 空列表会被读成"一个客户都没加"，而实际是读不到。
	if m.storeDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地事实库未就绪，无法出报"})
		return
	}
	report, err := m.buildCustomerHealthReport(c.Request.Context(), time.Now())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "客户维护取数失败：" + err.Error()})
		return
	}
	c.JSON(http.StatusOK, report)
}
