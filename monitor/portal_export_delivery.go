package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// Check the request separately: a disconnected client cannot receive a useful
// error, whereas an exhausted generation budget can still receive a JSON 504.
func portalExportGenerationStopped(ctx context.Context, c *gin.Context) bool {
	if c.Request.Context().Err() != nil {
		return true
	}
	if ctx.Err() != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"code":  "export_generation_timeout",
			"error": "导出生成超时，请缩小时间范围后重试；未生成部分文件",
		})
		return true
	}
	return false
}

type portalExportContextReader struct {
	ctx context.Context
	src io.Reader
}

func (r portalExportContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.src.Read(p)
}

// The complete file is generated and authorized before this function is called.
// Content-Length makes an interrupted body detectable, rather than a successful
// shorter CSV. Never append JSON to a CSV after its headers have been sent.
func portalSendExport(c *gin.Context, src io.Reader, size int64, name string, ttl time.Duration) error {
	if err := c.Request.Context().Err(); err != nil {
		return err
	}
	// Bypass Gin's void Flush method so net/http's FlushError reaches us.
	wire := http.ResponseWriter(c.Writer)
	if wrapped, ok := wire.(interface{ Unwrap() http.ResponseWriter }); ok {
		wire = wrapped.Unwrap()
	}
	controller := http.NewResponseController(wire)
	err := controller.SetWriteDeadline(time.Now().Add(ttl))
	// In-memory test recorders have no connection deadlines. Real net/http
	// HTTP/1 and HTTP/2 writers support them; other errors fail before headers.
	deadlineSupported := err == nil
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "准备下载失败，请稍后重试"})
		return err
	}
	finished := false
	defer func() {
		// Do not clear a failed transfer's deadline and permit buffered bytes to
		// keep blocking. A successful keep-alive response must clear it.
		if deadlineSupported {
			deadline := time.Now()
			if finished {
				deadline = time.Time{}
			}
			_ = controller.SetWriteDeadline(deadline)
		}
	}()
	c.Header("Content-Disposition", "attachment; filename=\""+name+"\"")
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Length", strconv.FormatInt(size, 10))
	c.Header("Cache-Control", "no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	c.Status(http.StatusOK)
	if _, err := io.CopyN(c.Writer, portalExportContextReader{c.Request.Context(), src}, size); err != nil {
		return err
	}
	if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if err := c.Request.Context().Err(); err != nil {
		return err
	}
	finished = true
	return nil
}
