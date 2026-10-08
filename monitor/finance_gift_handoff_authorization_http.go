//go:build unix

package monitor

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (m *Monitor) giftAuthorizationAvailable(c *gin.Context) bool {
	if !m.cfg.FinanceGiftHandoffApprovalEnabled {
		c.JSON(http.StatusNotFound, gin.H{"error": "交接授权未启用"})
		return false
	}
	if validateFinanceGiftPreviewSettings(m.cfg) != nil || m.storeDB == nil || m.storeDB == m.usageFactsStore() || sameStorePath(m.cfg.StorePath, m.cfg.UsageFactsStorePath) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "交接授权配置或独立存储未就绪"})
		return false
	}
	if c.Request.URL.RawQuery != "" {
		c.JSON(400, gin.H{"error": "不接受查询参数"})
		return false
	}
	return true
}

func giftAuthorizationJSON(c *gin.Context, out any) bool {
	media, _, _ := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if media != "application/json" || c.GetHeader("Sec-Fetch-Site") == "cross-site" {
		c.JSON(400, gin.H{"error": "需要同源 JSON 确认请求"})
		return false
	}
	if origin := c.GetHeader("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u.Host != c.Request.Host || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			c.JSON(403, gin.H{"error": "拒绝跨站确认请求"})
			return false
		}
	}
	b, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1024))
	if err != nil || giftLocalDecodeJSON(b, out) != nil {
		c.JSON(400, gin.H{"error": "确认格式无效"})
		return false
	}
	return true
}

func giftAuthorizationState(row financeGiftHandoffAuthorization, p financeGiftAuthorizationPayload, cfg Settings, now int64) string {
	state := "awaiting_execution"
	if p.ExpiresAt <= now {
		state = "expired"
	}
	if p.SourcePlan != cfg.FinanceGiftHandoffPreviewSHA256 || p.ReceiverBinding != financeGiftReceiverBinding(cfg) {
		state = "configuration_changed"
	}
	if row.RevokedAt > 0 {
		state = "revoked"
	}
	return state
}

func giftAuthorizationReply(c *gin.Context, row financeGiftHandoffAuthorization, p financeGiftAuthorizationPayload, cfg Settings, now int64) {
	state := giftAuthorizationState(row, p, cfg, now)
	c.JSON(200, gin.H{"task_id": row.ID, "status": state, "execution_enabled": false, "requires_revalidation": true,
		"authorization": p, "revoked_at": row.RevokedAt, "revoked_by": row.RevokedBy})
}

func giftAuthorizationError(c *gin.Context, err error) {
	code := http.StatusServiceUnavailable
	if errors.Is(err, gorm.ErrRecordNotFound) {
		code = http.StatusNotFound
	}
	if errors.Is(err, errGiftAuthorizationConflict) {
		code = http.StatusConflict
	}
	c.JSON(code, gin.H{"error": "授权记录不存在、冲突或暂不可用；本次请求未执行事实修复"})
}

func (m *Monitor) serveFinanceGiftHandoffAuthorize(c *gin.Context) {
	if !m.giftAuthorizationAvailable(c) {
		return
	}
	request := financeGiftAuthorizationRequest{MaxHours: 2}
	if !giftAuthorizationJSON(c, &request) {
		return
	}
	id, err := financeGiftAuthorizationID(request.RequestID)
	actor := c.GetString("uname")
	if err != nil || validateFinanceGiftHandoffLimit(request.MaxHours) != nil || request.Confirmation != m.cfg.FinanceGiftHandoffPreviewSHA256 || actor == "" || len(actor) > 256 {
		c.JSON(400, gin.H{"error": "确认摘要、请求编号或修复限额无效"})
		return
	}
	// A lost HTTP response must not cause a new scope or extend authorization.
	row, p, err := readFinanceGiftAuthorization(c.Request.Context(), m.storeDB, id)
	if err == nil {
		if p.Actor != actor || p.SourcePlan != request.Confirmation || p.MaxHours != request.MaxHours || p.ReceiverBinding != financeGiftReceiverBinding(m.cfg) {
			giftAuthorizationError(c, errGiftAuthorizationConflict)
			return
		}
		giftAuthorizationReply(c, row, p, m.cfg, time.Now().Unix())
		return
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		giftAuthorizationError(c, err)
		return
	}
	// Share the existing evidence-IO gate with preview, without queuing work.
	if !m.financeGiftPreviewMu.TryLock() {
		c.Header("Retry-After", "10")
		c.JSON(429, gin.H{"error": "预检或授权核验进行中"})
		return
	}
	defer m.financeGiftPreviewMu.Unlock()
	if time.Now().Before(m.financeGiftPreviewNextAt) {
		c.Header("Retry-After", "10")
		c.JSON(429, gin.H{"error": "交接核验冷却中"})
		return
	}
	defer func() { m.financeGiftPreviewNextAt = time.Now().Add(financeGiftPreviewCooldown) }()
	c.Header("X-Finance-Gift-Cooldown-Seconds", strconv.Itoa(int(financeGiftPreviewCooldown/time.Second)))
	reader := m.financeFactsReadStore()
	if reader == nil || reader == m.usageFactsStore() {
		c.JSON(503, gin.H{"error": "独立只读事实连接未就绪"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), financeGiftPreviewTimeout)
	defer cancel()
	preview, err := previewFinanceGiftConfiguredJob(ctx, reader, m.cfg)
	if err != nil {
		c.JSON(409, gin.H{"error": "证据重新核验未通过，未记录授权"})
		return
	}
	if preview.BlockedTargets != 0 || preview.ReadyTargets == 0 {
		c.JSON(409, gin.H{"error": "存在冲突或没有待修复目标，未记录授权"})
		return
	}
	now := time.Now().Unix()
	p = financeGiftAuthorizationPayload{Version: 1, Actor: actor, SourcePlan: preview.PlanSHA256, SourceSnapshot: preview.SourceSHA256,
		ReceiverBinding: financeGiftReceiverBinding(m.cfg), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, MaxHours: request.MaxHours,
		ApprovedAt: now, ExpiresAt: now + int64(financeGiftAuthorizationTTL/time.Second)}
	for _, target := range preview.Entries {
		if target.Status == "ready" && len(p.Targets) < request.MaxHours {
			p.Targets = append(p.Targets, target)
		}
	}
	row, p, err = saveFinanceGiftAuthorization(ctx, m.storeDB, id, p)
	if err != nil {
		giftAuthorizationError(c, err)
		return
	}
	giftAuthorizationReply(c, row, p, m.cfg, time.Now().Unix())
}

func (m *Monitor) giftAuthorizationReadRequest(c *gin.Context) (financeGiftHandoffAuthorization, financeGiftAuthorizationPayload, bool) {
	var row financeGiftHandoffAuthorization
	var p financeGiftAuthorizationPayload
	if !m.giftAuthorizationAvailable(c) {
		return row, p, false
	}
	id := c.Param("id")
	if !giftSeriesDigestValid(id) {
		c.JSON(400, gin.H{"error": "授权编号无效"})
		return row, p, false
	}
	row, p, err := readFinanceGiftAuthorization(c.Request.Context(), m.storeDB, id)
	if err != nil {
		giftAuthorizationError(c, err)
		return row, p, false
	}
	return row, p, true
}

func (m *Monitor) serveFinanceGiftHandoffAuthorizationStatus(c *gin.Context) {
	row, p, ok := m.giftAuthorizationReadRequest(c)
	if ok {
		giftAuthorizationReply(c, row, p, m.cfg, time.Now().Unix())
	}
}

func (m *Monitor) serveFinanceGiftHandoffRevoke(c *gin.Context) {
	if !m.giftAuthorizationAvailable(c) {
		return
	}
	var request struct{}
	if !giftAuthorizationJSON(c, &request) {
		return
	}
	row, _, ok := m.giftAuthorizationReadRequest(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), financeGiftAuthorizationDBTimeout)
	defer cancel()
	if err := m.storeDB.WithContext(ctx).Model(&financeGiftHandoffAuthorization{}).Where("id=? AND revoked_at=0", row.ID).
		Updates(map[string]any{"revoked_at": time.Now().Unix(), "revoked_by": c.GetString("uname")}).Error; err != nil {
		giftAuthorizationError(c, err)
		return
	}
	row, p, err := readFinanceGiftAuthorization(ctx, m.storeDB, row.ID)
	if err != nil {
		giftAuthorizationError(c, err)
		return
	}
	giftAuthorizationReply(c, row, p, m.cfg, time.Now().Unix())
}
