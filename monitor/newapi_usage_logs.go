package monitor

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/usagelogs"
)

// Only the admin HTTP adapter knows Gin. Local shortcut configuration and
// customer Usage routes are deliberately outside the replica query service.
func (m *Monitor) usageLogReads() *usagelogs.Service {
	m.usageLogReadsOnce.Do(func() {
		m.usageLogReader = usagelogs.New(m.prodDB, m.cfg.ProdDSN, m.cfg.sourceOpen)
	})
	return m.usageLogReader
}

// parseUsageLogFilter reads the admin filter. Unparseable numbers silently turn
// the filter off (0), as in new-api.
func parseUsageLogFilter(c *gin.Context) usagelogs.Filter {
	atoi := func(k string) int { v, _ := strconv.Atoi(strings.TrimSpace(c.Query(k))); return v }
	atoi64 := func(k string) int64 { v, _ := strconv.ParseInt(strings.TrimSpace(c.Query(k)), 10, 64); return v }
	return usagelogs.Filter{
		Type:              atoi("type"),
		Start:             atoi64("start_timestamp"),
		End:               atoi64("end_timestamp"),
		ModelName:         c.Query("model_name"),
		Username:          c.Query("username"),
		TokenName:         c.Query("token_name"),
		Group:             c.Query("group"),
		RequestID:         c.Query("request_id"),
		UpstreamRequestID: c.Query("upstream_request_id"),
		Channel:           atoi("channel"),
	}
}

func usageLogsPage(c *gin.Context) (page, size int) {
	page, _ = strconv.Atoi(c.Query("p"))
	size = 0
	for _, k := range []string{"page_size", "ps", "size"} {
		if v, err := strconv.Atoi(c.Query(k)); err == nil && v > 0 {
			size = v
			break
		}
	}
	return page, size
}

func usageLogsFail(c *gin.Context, err error) {
	if errors.Is(err, context.Canceled) {
		c.Status(statusClientClosedRequest)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": false, "message": err.Error()})
}

func usageLogsReply(c *gin.Context, data any, err error) {
	if err != nil {
		usageLogsFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": data})
}

func (m *Monitor) serveUsageLogsList(c *gin.Context) {
	f := parseUsageLogFilter(c)
	page, size := usageLogsPage(c)
	r := m.usageLogReads().Request(c.Request.Context())
	defer r.Finish()
	data, err := r.List(usagelogs.ListInput{Filter: f, Page: page, PageSize: size,
		BeforeCreatedAt: c.Query("before_created_at"), BeforeID: c.Query("before_id"),
		DeferTotal: c.Query("defer_total") == "1"})
	usageLogsReply(c, data, err)
}

func (m *Monitor) serveUsageLogsCount(c *gin.Context) {
	r := m.usageLogReads().Request(c.Request.Context())
	defer r.Finish()
	data, err := r.Count(parseUsageLogFilter(c))
	usageLogsReply(c, data, err)
}

func (m *Monitor) serveUsageLogsStat(c *gin.Context) {
	r := m.usageLogReads().Request(c.Request.Context())
	defer r.Finish()
	data, err := r.Stat(parseUsageLogFilter(c), c.Query("skip_quota") == "1")
	usageLogsReply(c, data, err)
}

func (m *Monitor) serveUsageLogsMeta(c *gin.Context) {
	r := m.usageLogReads().Request(c.Request.Context())
	defer r.Finish()
	data, err := r.Meta()
	usageLogsReply(c, data, err)
}

func (m *Monitor) serveUsageLogsUser(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		id = 0
	}
	r := m.usageLogReads().Request(c.Request.Context())
	defer r.Finish()
	data, err := r.User(id)
	usageLogsReply(c, data, err)
}
