package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	modelStatisticsQueryTimeout = 5 * time.Second
	modelReportQueueWait        = 350 * time.Millisecond
)

var errModelReportBusy = errors.New("model report read slot busy")

type modelReportReadGate struct {
	once    sync.Once
	active  chan struct{}
	waiting chan struct{}
}

func (g *modelReportReadGate) acquire(ctx context.Context, wait time.Duration) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.once.Do(func() {
		g.active = make(chan struct{}, 1)
		g.waiting = make(chan struct{}, 1)
	})
	select {
	case g.active <- struct{}{}:
	default:
		select {
		case g.waiting <- struct{}{}:
			defer func() { <-g.waiting }()
		default:
			return nil, errModelReportBusy
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case g.active <- struct{}{}:
		case <-timer.C:
			return nil, errModelReportBusy
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	release := func() { <-g.active }
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func serveModelReport[T any](c *gin.Context, gate *modelReportReadGate, failure string, build func(context.Context) (T, error)) {
	c.Header("Cache-Control", "private, no-store")
	ctx, cancel := context.WithTimeout(c.Request.Context(), modelStatisticsQueryTimeout)
	defer cancel()
	started := time.Now()
	payload, compressed, err := prepareModelReport(ctx, gate, groupGovernanceAcceptsGzip(c.GetHeader("Accept-Encoding")), build)
	if err == nil {
		err = ctx.Err()
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		slog.Info("模型报表读取及编码耗时", "route", c.FullPath(), "elapsed_ms", elapsed.Milliseconds(), "deadline", errors.Is(err, context.DeadlineExceeded), "canceled", isCanceledUsageRequest(c, err))
	}
	if err != nil {
		writeModelReportError(c, err, failure)
		return
	}
	// The slot protects local reads and encoding, not network backpressure
	// from a slow browser. net/http bounds the write separately.
	c.Header("X-Accel-Buffering", "no")
	c.Header("Vary", "Accept-Encoding")
	if compressed {
		c.Header("Content-Encoding", "gzip")
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", payload)
}

var errModelReportEncode = errors.New("model report encoding failed")

func prepareModelReport[T any](ctx context.Context, gate *modelReportReadGate, compress bool, build func(context.Context) (T, error)) ([]byte, bool, error) {
	release, err := gate.acquire(ctx, modelReportQueueWait)
	if err != nil {
		return nil, false, err
	}
	defer release()
	report, err := build(ctx)
	if err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return nil, false, errModelReportEncode
	}
	if err := ctx.Err(); err != nil || !compress {
		return encoded, false, err
	}
	var compressed bytes.Buffer
	w, _ := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if _, err = w.Write(encoded); err == nil {
		err = w.Close()
	}
	if err != nil {
		return nil, false, errModelReportEncode
	}
	return compressed.Bytes(), true, ctx.Err()
}

func writeModelReportError(c *gin.Context, err error, failure string) {
	if abortCanceledUsageRequest(c, err) {
		return
	}
	var input *modelFRTInputError
	switch {
	case errors.As(err, &input):
		c.JSON(http.StatusBadRequest, gin.H{"error": input.Error()})
	case errors.Is(err, errModelReportBusy):
		c.Header("Retry-After", "1")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "正在处理另一份模型报表，请稍后刷新"})
	case errors.Is(err, errModelFRTLimit):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "所选范围的模型维度过多，请按渠道、分组或模型筛选后重试"})
	case errors.Is(err, errModelReportEncode):
		c.JSON(http.StatusInternalServerError, gin.H{"error": "生成本地报告失败"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": failure})
	}
}
