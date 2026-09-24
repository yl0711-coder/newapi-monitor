package monitor

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const logChainInvestigationBodyLimit = 16 << 10

// serveCreateLogChainInvestigation POST /logchain/investigations
func (m *Monitor) serveCreateLogChainInvestigation(c *gin.Context) {
	if !m.cloudWatchEvidenceAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "cloudwatch logs disabled", "enabled": false})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, logChainInvestigationBodyLimit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var payload logChainInvestigationCreateRequest
	if err := decoder.Decode(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "排障任务参数不是合法 JSON 或包含未知字段"})
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求体只能包含一个 JSON 对象"})
		return
	}
	input, err := parseLogChainInvestigationInput(payload, time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	owner := c.GetString("uname")
	if owner == "" {
		owner, _, _ = m.currentUser(c)
	}
	result, err := m.createLogChainInvestigation(owner, input)
	if err != nil {
		var active *logChainInvestigationActiveError
		if errors.As(err, &active) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "investigation_id": active.ID, "status": active.Status})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"ok": true, "investigation_id": result.InvestigationID, "status": result.Status,
		"poll_after_ms": 800, "cache_hit": result.Cost.CacheHit,
	})
}

// serveGetLogChainInvestigation GET /logchain/investigations/:id
func (m *Monitor) serveGetLogChainInvestigation(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if !validLogChainInvestigationID(id) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "investigation_id 不合法"})
		return
	}
	owner := c.GetString("uname")
	if owner == "" {
		owner, _, _ = m.currentUser(c)
	}
	result, ok := m.getLogChainInvestigation(owner, id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "排障任务不存在或不属于当前操作者"})
		return
	}
	c.JSON(http.StatusOK, result)
}

// serveCancelLogChainInvestigation POST /logchain/investigations/:id/cancel
func (m *Monitor) serveCancelLogChainInvestigation(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if !validLogChainInvestigationID(id) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "investigation_id 不合法"})
		return
	}
	owner := c.GetString("uname")
	if owner == "" {
		owner, _, _ = m.currentUser(c)
	}
	result, ok := m.cancelLogChainInvestigation(owner, id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "排障任务不存在或不属于当前操作者"})
		return
	}
	c.JSON(http.StatusOK, result)
}

func validLogChainInvestigationID(value string) bool {
	if len(value) != 36 || !strings.HasPrefix(value, "inv_") {
		return false
	}
	for _, r := range value[4:] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}
