package monitor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var errTokenDeliveryCapacity = errors.New("token delivery capacity")
var errTokenExportScope = errors.New("export scope not authorized")
var errTokenExportConflict = errors.New("export version conflict")

func (m *Monitor) tokenDeliveryCSRF(c *gin.Context) string {
	cookie, _ := c.Cookie(sessionCookie)
	if m.cfg.LocalAuthBypass {
		cookie = "local-offline-snapshot"
	}
	h := hmac.New(sha256.New, []byte(m.cfg.SessionSecret))
	_, _ = h.Write([]byte("token-export-v1\x00" + cookie))
	return hex.EncodeToString(h.Sum(nil))
}
func (m *Monitor) tokenDeliveryWrite(c *gin.Context) bool {
	media, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || media != "application/json" || c.GetHeader("Sec-Fetch-Site") == "cross-site" || !hmac.Equal([]byte(c.GetHeader("X-Monitor-Token-CSRF")), []byte(m.tokenDeliveryCSRF(c))) {
		c.JSON(http.StatusForbidden, gin.H{"error": "需要已授权的同源操作"})
		return false
	}
	if origin := c.GetHeader("Origin"); origin != "" {
		u, e := url.Parse(origin)
		if e != nil || u.Host != c.Request.Host || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			c.JSON(403, gin.H{"error": "拒绝跨站操作"})
			return false
		}
	}
	return true
}
func tokenDeliveryBody(c *gin.Context, v any, limit int64) bool {
	d := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, limit))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		c.JSON(400, gin.H{"error": "请求格式或范围无效"})
		return false
	}
	return true
}
func tokenDeliveryError(c *gin.Context, err error) {
	code := 503
	reason := "Token 本地处理暂不可用"
	switch {
	case errors.Is(err, errTokenDeliveryCapacity):
		code = 429
		reason = "本地容量或操作频率已达上限，请稍后重试"
	case errors.Is(err, errTokenExportScope):
		code = 403
		reason = "导出目标、时间或价格范围未授权"
	case errors.Is(err, errTokenExportConflict):
		code = 409
		reason = "证据修订已变化或导出已过期，请重新核对"
	case errors.Is(err, gorm.ErrRecordNotFound):
		code = 404
		reason = "本地记录不存在或已过期"
	}
	c.JSON(code, gin.H{"error": reason})
}
func tokenDeliveryAudit(tx *gorm.DB, actor, action, id, reason string, now int64) error {
	var count int64
	if err := tx.Model(&TokenDeliveryAudit{}).Count(&count).Error; err != nil {
		return err
	}
	if count >= 10000 {
		return errTokenDeliveryCapacity
	}
	return tx.Create(&TokenDeliveryAudit{At: now, Actor: actor, Action: action, ObjectID: id, Reason: reason}).Error
}
func (m *Monitor) serveTokenDeliveryAccess(c *gin.Context) {
	_, role, _ := m.currentUser(c)
	p, err := parseTokenExportPolicy(m.cfg.TokenExportPolicy)
	targets := []tokenExportTarget{}
	if err == nil && role >= roleRoot {
		targets = p.Targets
	}
	c.JSON(200, gin.H{"schema_version": tokenDeliveryVersion, "is_root": role >= roleRoot, "csrf_token": m.tokenDeliveryCSRF(c), "export_enabled": m.cfg.TokenExportsEnabled, "export_configured": err == nil, "export_targets": targets, "max_requests": tokenExportMaxRequests, "max_bytes": tokenExportMaxBytes, "ttl_seconds": tokenExportTTL, "notification_evaluation_enabled": m.cfg.TokenNotificationEvaluationEnabled && !m.cfg.LocalSnapshotOnly, "notifications_enabled": m.cfg.TokenNotificationsEnabled && !m.cfg.LocalSnapshotOnly})
}
func (m *Monitor) serveTokenDeliveryHealth(c *gin.Context) {
	unavailable := func(reason string) {
		c.JSON(http.StatusOK, gin.H{"version": tokenDeliveryVersion, "evaluation_enabled": m.cfg.TokenNotificationEvaluationEnabled && !m.cfg.LocalSnapshotOnly, "delivery_enabled": m.cfg.TokenNotificationsEnabled && !m.cfg.LocalSnapshotOnly && !m.cfg.AlertsDisabled, "policy_configured": false, "available": false, "counts": nil, "recent": []TokenNotifyOutbox{}, "recent_limit": 20, "generated_at": time.Now().Unix(), "delivery_semantics": "bounded_at_least_once_uncertain_may_duplicate", "gap_reasons": []string{reason}})
	}
	if m.tokenStore() == nil {
		unavailable("local_store_unavailable")
		return
	}
	ctx, cancel := tokenDeliveryContext(c.Request.Context())
	defer cancel()
	db := m.tokenStore().WithContext(ctx)
	var state tokenDeliveryState
	if err := db.Where("id = ?", "notify").Limit(1).Find(&state).Error; err != nil {
		unavailable("local_notification_state_unavailable")
		return
	}
	var counts []struct {
		State string
		Count int64
	}
	if err := db.Model(&TokenNotifyOutbox{}).Select("state, COUNT(*) AS count").Group("state").Scan(&counts).Error; err != nil {
		unavailable("local_notification_counts_unavailable")
		return
	}
	var rows []TokenNotifyOutbox
	if err := db.Select("id,target_key,severity,evidence,count,state,reason,created_at,expires_at,next_at,attempts,sent_at").Order("created_at DESC,id").Limit(20).Find(&rows).Error; err != nil {
		unavailable("local_notification_outbox_unavailable")
		return
	}
	_, policyErr := parseTokenNotifyPolicy(m.cfg.TokenNotificationPolicy)
	c.JSON(http.StatusOK, gin.H{"version": tokenDeliveryVersion, "evaluation_enabled": m.cfg.TokenNotificationEvaluationEnabled && !m.cfg.LocalSnapshotOnly, "delivery_enabled": m.cfg.TokenNotificationsEnabled && !m.cfg.LocalSnapshotOnly && !m.cfg.AlertsDisabled, "policy_configured": policyErr == nil, "available": true, "state": state, "counts": counts, "recent": rows, "recent_limit": 20, "generated_at": time.Now().Unix(), "delivery_semantics": "bounded_at_least_once_uncertain_may_duplicate"})
}
