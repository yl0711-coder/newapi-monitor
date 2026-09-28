//go:build unix

package monitor

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const financeGiftPreviewCooldown = 10 * time.Second
const financeGiftPreviewTimeout = 10 * time.Second

// Read-only UI hint; admission is still enforced under the preview mutex.
func (m *Monitor) financeGiftPreviewRetrySeconds() int {
	if !m.financeGiftPreviewMu.TryLock() {
		return int(financeGiftPreviewCooldown / time.Second)
	}
	defer m.financeGiftPreviewMu.Unlock()
	remaining := time.Until(m.financeGiftPreviewNextAt)
	if remaining <= 0 {
		return 0
	}
	return int((remaining + time.Second - 1) / time.Second)
}

// Root-only route registered by server.go. No execution, uploads, caller paths,
// source connections, schema creation or persistent writes are exposed here.
func (m *Monitor) serveFinanceGiftHandoffPreview(c *gin.Context) {
	if m.cfg.FinanceGiftHandoffPreviewDir == "" || m.cfg.FinanceGiftHandoffPreviewSHA256 == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "交接只读预检未启用"})
		return
	}
	if err := validateFinanceGiftPreviewSettings(m.cfg); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "交接预检配置不可用"})
		return
	}
	media, _, _ := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if media != "application/json" || c.Request.URL.RawQuery != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "仅接受 JSON 确认请求，不接受查询参数"})
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1024))
	var request struct {
		Confirmation string `json:"confirmation"`
	}
	if err != nil || giftLocalDecodeJSON(data, &request) != nil || request.Confirmation != m.cfg.FinanceGiftHandoffPreviewSHA256 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "确认摘要不匹配或请求格式无效"})
		return
	}
	if !m.financeGiftPreviewMu.TryLock() {
		c.Header("Retry-After", "10")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "预检正在运行，请稍后重试"})
		return
	}
	defer m.financeGiftPreviewMu.Unlock()
	if time.Now().Before(m.financeGiftPreviewNextAt) {
		c.Header("Retry-After", "10")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "预检冷却中，请稍后重试"})
		return
	}
	defer func() { m.financeGiftPreviewNextAt = time.Now().Add(financeGiftPreviewCooldown) }()
	c.Header("X-Finance-Gift-Cooldown-Seconds", strconv.Itoa(int(financeGiftPreviewCooldown/time.Second)))
	reader := m.financeFactsReadStore()
	if reader == nil || reader == m.usageFactsStore() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "独立只读事实连接未就绪"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), financeGiftPreviewTimeout)
	defer cancel()
	report, err := previewFinanceGiftConfiguredJob(ctx, reader, m.cfg)
	if err != nil {
		code := http.StatusConflict
		if ctx.Err() != nil || usageFactLocalStoreBusy(err) {
			code = http.StatusServiceUnavailable
		}
		c.JSON(code, gin.H{"error": "交接证据未通过核验或读取暂不可用；未执行任何修复"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"mode": "current_facts_preview_only", "execution_enabled": false, "preview": report})
}

func previewFinanceGiftConfiguredJob(ctx context.Context, receiver *gorm.DB, cfg Settings) (FinanceGiftHandoffPreview, error) {
	input, err := loadFinanceGiftConfiguredEvidence(ctx, cfg)
	if err != nil {
		return FinanceGiftHandoffPreview{}, err
	}
	report, err := previewFinanceGiftHandoffTargets(ctx, receiver, input.plan.Targets, input.verified)
	if err != nil {
		return FinanceGiftHandoffPreview{}, err
	}
	report.Mode = "current_facts_preview_only"
	report.PlanSHA256, report.SourceSHA256 = cfg.FinanceGiftHandoffPreviewSHA256, input.sourceHash
	// Never claim a stable file SHA for the current, possibly updating receiver.
	return report, nil
}

type financeGiftConfiguredEvidence struct {
	plan       FinanceGiftLocalPlan
	verified   [][]FinanceGiftBoundaryEvent
	sourceHash string
}

// Load and pin all evidence under the source job lock. Returned values are an
// owned in-memory snapshot; no file reads occur during receiver writes.
func loadFinanceGiftConfiguredEvidence(ctx context.Context, cfg Settings) (financeGiftConfiguredEvidence, error) {
	var empty financeGiftConfiguredEvidence
	job := cfg.FinanceGiftHandoffPreviewDir
	path := filepath.Join(job, "usage-facts.db")
	if sameStorePath(path, cfg.UsageFactsStorePath) || sameStorePath(path, cfg.StorePath) {
		return empty, errors.New("handoff source must be separate from active stores")
	}
	lock, err := giftLocalLock(job)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	plan, evidence, err := giftLocalLoadConfirmedInputs(job, cfg.FinanceGiftHandoffPreviewSHA256)
	if err != nil {
		return empty, err
	}
	for _, target := range plan.Targets {
		if target.SourceEpoch != cfg.UsageFactsHistorySourceEpoch {
			return empty, errors.New("handoff source epoch does not match receiver configuration")
		}
	}
	if err := giftLocalValidateAudit(filepath.Join(job, "audit.jsonl"), plan); err != nil {
		return empty, err
	}
	digest, err := giftSeriesFileHash(ctx, path)
	if err != nil {
		return empty, err
	}
	source, closeDB, err := giftLocalReadonlyDatabase(path)
	if err != nil {
		return empty, err
	}
	defer closeDB()
	verified, err := loadFinanceGiftHandoffCompletedEvidence(ctx, source, plan, evidence)
	if err != nil {
		return empty, err
	}
	after, err := giftSeriesFileHash(ctx, path)
	if err != nil || after != digest {
		return empty, errors.New("handoff source changed during preview")
	}
	return financeGiftConfiguredEvidence{plan: plan, verified: verified, sourceHash: digest}, nil
}
