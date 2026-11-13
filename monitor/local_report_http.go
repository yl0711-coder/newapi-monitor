package monitor

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Compression is negotiated per response, after the bounded local read. No
// global middleware or proxy changes affect other APIs/downloads.
func serveLocalReportJSON(c *gin.Context, report any) {
	c.Header("Cache-Control", "private, no-store")
	c.Header("X-Accel-Buffering", "no")
	c.Header("Vary", "Accept-Encoding")
	if !groupGovernanceAcceptsGzip(c.GetHeader("Accept-Encoding")) {
		c.JSON(http.StatusOK, report)
		return
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "生成本地报告失败"})
		return
	}
	var compressed bytes.Buffer
	w, _ := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if _, err = w.Write(encoded); err == nil {
		err = w.Close()
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "生成本地报告失败"})
		return
	}
	c.Header("Content-Encoding", "gzip")
	c.Data(http.StatusOK, "application/json; charset=utf-8", compressed.Bytes())
}
