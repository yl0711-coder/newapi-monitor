package monitor

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

const ecsLogVerifiedContext = "monitor.internal.ecs.verified-source.v1"
const ecsLogMaxBody = 2 << 20
const ecsLogMaxInflight = int32(16)

func decodeECSBoundedJSON(body io.Reader, limit int, out any) error {
	b, err := io.ReadAll(io.LimitReader(body, int64(limit)+1))
	if err != nil || len(b) > limit {
		return errors.New("request exceeds read budget")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var tail any
	if d.Decode(out) != nil || !errors.Is(d.Decode(&tail), io.EOF) {
		return errors.New("invalid JSON envelope")
	}
	return nil
}

func (m *Monitor) registerECSLogHTTP(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if !m.cfg.ECSLogEnabled || m.cfg.ECSLogScope != "isolated" || len(m.cfg.ECSLogBridgeToken) < 32 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ECS registration disabled"})
		return
	}
	// This credential is ONLY held by the AWS_IAM registration bridge. It
	// must never be distributed to tasks, or trusted caller context is lost.
	if subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+m.cfg.ECSLogBridgeToken)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "trusted registration bridge required"})
		return
	}
	var in ecsLogRegistration
	if err := decodeECSBoundedJSON(c.Request.Body, 16<<10, &in); err != nil {
		c.JSON(400, gin.H{"error": "invalid registration"})
		return
	}
	policy, ok := m.ecsLogPolicy(in.ServiceARN, in.Container, in.Lane)
	key, err := base64.StdEncoding.DecodeString(in.PublicKey)
	if !ok || err != nil || len(key) != ed25519.PublicKeySize || !validateECSCaller(in, policy) {
		c.JSON(403, gin.H{"error": "service, task or lane not authorized"})
		return
	}
	n := m.ecsLogVerifications.Add(1)
	defer m.ecsLogVerifications.Add(-1)
	if n > 4 {
		c.JSON(429, gin.H{"error": "registration verification busy"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
	defer cancel()
	source, err := m.verifyECSRegistration(ctx, in, policy)
	if err != nil {
		c.JSON(503, gin.H{"error": "AWS task verification unavailable or rejected"})
		return
	}
	source, err = m.registerECSLogLease(ctx, source, in.PublicKey, time.Now().Unix())
	if errors.Is(err, errECSLogOwnerConflict) || errors.Is(err, errECSLogResponsibilityConflict) {
		c.JSON(409, gin.H{"error": "source owner conflict or retired source"})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "source registry unavailable"})
		return
	}
	finalEnabled := m.cfg.ECSArchiveEnabled && m.cfg.ECSLogScope == "isolated"
	c.JSON(200, gin.H{"ok": true, "version": 1, "node": source.Node, "lane": source.Lane, "audience": m.cfg.ECSLogAudience, "lease_until": source.LeaseUntil, "archive_closure_v2": finalEnabled, "final_boundary_v1": finalEnabled, "final_newapi_files_v1": finalEnabled})
}

// Only signature headers change on a retry. The body, its batch ID and the
// original protocol hash are never rewritten at this adapter boundary.
func ecsLogSigningMessage(audience, node, lane, path, timestamp string, body []byte) []byte {
	h := sha256.Sum256(body)
	return []byte("monitor-ecs-log-v1\n" + audience + "\n" + node + "\n" + lane + "\n" + path + "\n" + timestamp + "\n" + hex.EncodeToString(h[:]))
}

func (m *Monitor) ingestECSLogHTTP(c *gin.Context) {
	m.acceptECSLogHTTP(c, false)
}

func (m *Monitor) heartbeatECSLogHTTP(c *gin.Context) {
	m.acceptECSLogHTTP(c, true)
}

func (m *Monitor) acceptECSLogHTTP(c *gin.Context, heartbeat bool) {
	if !m.cfg.ECSLogEnabled || m.cfg.ECSLogScope != "isolated" {
		c.JSON(503, gin.H{"error": "ECS log ingest disabled"})
		return
	}
	n := m.ecsLogRequests.Add(1)
	defer m.ecsLogRequests.Add(-1)
	if n > ecsLogMaxInflight {
		c.JSON(429, gin.H{"error": "ECS ingestion busy; retry unchanged batch"})
		return
	}
	lane, node := c.Param("lane"), c.GetHeader("X-Monitor-ECS-Node")
	timestamp := c.GetHeader("X-Monitor-ECS-Time")
	at, err := strconv.ParseInt(timestamp, 10, 64)
	now := time.Now().Unix()
	if !ecsLogLane(lane) || !ecsLogNodePattern.MatchString(node) || err != nil || at < now-ecsLogSignatureSkew || at > now+ecsLogSignatureSkew || c.Request.URL.RawQuery != "" {
		c.JSON(401, gin.H{"error": "invalid ECS request identity or time"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	var source ECSLogSource
	if err := m.storeDB.WithContext(ctx).First(&source, "node = ? AND lane = ?", node, lane).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(401, gin.H{"error": "ECS source not registered"})
		} else {
			c.JSON(503, gin.H{"error": "ECS source registry unavailable"})
		}
		return
	}
	policy, allowed := m.ecsLogPolicy(source.ServiceARN, source.Container, lane)
	if !allowed || source.TaskRoleARN != policy.TaskRoleARN || source.Revoked || source.LeaseUntil <= now || source.StoppedAt > 0 && now > source.StoppedAt+ecsLogReplaySeconds {
		c.JSON(401, gin.H{"error": "ECS source lease expired or revoked"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, ecsLogMaxBody+1))
	if err != nil || len(body) > ecsLogMaxBody {
		c.JSON(413, gin.H{"error": "ECS payload too large"})
		return
	}
	key, keyErr := base64.StdEncoding.DecodeString(source.PublicKey)
	signature, sigErr := base64.StdEncoding.DecodeString(c.GetHeader("X-Monitor-ECS-Signature"))
	if keyErr != nil || sigErr != nil || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, ecsLogSigningMessage(m.cfg.ECSLogAudience, node, lane, c.Request.URL.EscapedPath(), timestamp, body), signature) {
		c.JSON(401, gin.H{"error": "invalid ECS signature"})
		return
	}
	var envelope struct {
		Node string `json:"node"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.Node != node {
		c.JSON(400, gin.H{"error": "ECS payload source mismatch"})
		return
	}
	if err := m.checkECSLogOwnership(ctx, source); err != nil {
		c.JSON(503, gin.H{"error": "ECS collection responsibility unavailable; retain batch"})
		return
	}
	c.Set(ecsLogVerifiedContext, source)
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	if heartbeat {
		m.recordECSLogHeartbeat(c, source, body, min(at, now))
		return
	}
	if m.cfg.ECSArchiveEnabled {
		hash := sha256.Sum256(body)
		status := (&ecsArchiveReplayer{m: m}).checkArchiveClosure(ctx, ecsarchive.Envelope{Node: node, Lane: lane, Hash: hex.EncodeToString(hash[:])})
		if status != 200 {
			c.JSON(status, gin.H{"error": "archive lane closed or closure state unavailable; preserve batch"})
			return
		}
	}
	m.dispatchECSLog(c, source)
	// Receipt/fact transactions above are authoritative. This is only a
	// reconstructable freshness projection; failure must NOT regenerate a batch.
	if c.Writer.Status() >= 200 && c.Writer.Status() < 300 {
		if err := m.storeDB.WithContext(ctx).Model(&ECSLogSource{}).Where("node = ? AND lane = ? AND last_report < ?", node, lane, now).Update("last_report", now).Error; err != nil {
			slog.Warn("ECS accepted batch freshness projection failed", "node", node, "lane", lane, "err", err)
		}
	}
}

// Both transports use the same authoritative protocol validation and receipts.
// Archive replay deliberately does not execute the live freshness update above.
func (m *Monitor) dispatchECSLog(c *gin.Context, source ECSLogSource) {
	switch source.Lane {
	case "access":
		m.ingestNginx(c)
	case "error":
		m.ingestNginxErrors(c)
	case "evidence":
		m.ingestNginxEvidence(c)
	case "reject":
		m.ingestRejectionsV2(c)
	default:
		c.JSON(400, gin.H{"error": "invalid archive lane"})
	}
}

func (m *Monitor) recordECSLogHeartbeat(c *gin.Context, source ECSLogSource, body []byte, at int64) {
	var in struct {
		Node string `json:"node"`
	}
	if decodeECSBoundedJSON(bytes.NewReader(body), 1024, &in) != nil || in.Node != source.Node {
		c.JSON(400, gin.H{"error": "invalid ECS heartbeat"})
		return
	}
	err := m.infraAssetWrite(c.Request.Context(), 3*time.Second, func(tx *gorm.DB) error {
		return tx.Model(&ECSLogSource{}).Where("node = ? AND lane = ? AND last_heartbeat < ?", source.Node, source.Lane, at).Update("last_heartbeat", at).Error
	})
	if err != nil {
		c.JSON(503, gin.H{"error": "ECS heartbeat registry unavailable"})
		return
	}
	// A heartbeat confirms only the collector's signed liveness. It commits
	// no log receipt/cursor and cannot claim that any request interval is complete.
	c.JSON(200, gin.H{"ok": true, "node": source.Node, "lane": source.Lane, "heartbeat_at": at})
}

func verifiedECSLog(c *gin.Context) (ECSLogSource, bool) {
	value, exists := c.Get(ecsLogVerifiedContext)
	source, ok := value.(ECSLogSource)
	return source, exists && ok
}

func (m *Monitor) nginxRequestNodeAllowed(c *gin.Context, node, lane string) bool {
	if source, ok := verifiedECSLog(c); ok {
		return source.Node == node && source.Lane == lane
	}
	return m.nginxNodeAllowed(node)
}
