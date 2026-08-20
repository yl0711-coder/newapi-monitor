package monitor

// 客户端交付证据全部在 Monitor 内闭环，不依赖、不修改 NewAPI。
// NewAPI 既有日志仍只用于“历史日志推断”，绝不用来伪造客户端成功。

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	clientEvidenceContractVersion  = 1
	clientEvidenceMaxEvents        = 500
	clientEvidenceRawRetention     = 90 * 24 * time.Hour
	clientEvidenceReceiptRetention = 100 * 24 * time.Hour
)

var errClientEvidenceConflict = errors.New("client evidence conflict")

func validateDeliveryEvidenceSettings(s Settings) error {
	enabled := strings.TrimSpace(s.ClientEvidenceToken) != "" || strings.TrimSpace(s.ClientEvidenceHMACSecret) != "" || len(s.ClientEvidenceAllowedClients) > 0
	if !enabled {
		return nil
	}
	if len(strings.TrimSpace(s.ClientEvidenceToken)) < 32 {
		return errors.New("MONITOR_CLIENT_EVIDENCE_TOKEN长度必须至少32")
	}
	if len(strings.TrimSpace(s.ClientEvidenceHMACSecret)) < 32 {
		return errors.New("MONITOR_CLIENT_EVIDENCE_HMAC_SECRET长度必须至少32")
	}
	if subtle.ConstantTimeCompare([]byte(s.ClientEvidenceToken), []byte(s.ClientEvidenceHMACSecret)) == 1 || (s.IngestToken != "" && subtle.ConstantTimeCompare([]byte(s.ClientEvidenceToken), []byte(s.IngestToken)) == 1) {
		return errors.New("客户端证据token、HMAC密钥与节点采集token必须彼此独立")
	}
	if len(s.ClientEvidenceAllowedClients) == 0 || len(s.ClientEvidenceAllowedClients) > 64 {
		return errors.New("MONITOR_CLIENT_EVIDENCE_ALLOWED_CLIENTS必须包含1到64个family@version")
	}
	seen := make(map[string]struct{}, len(s.ClientEvidenceAllowedClients))
	for _, raw := range s.ClientEvidenceAllowedClients {
		value := strings.TrimSpace(raw)
		parts := strings.Split(value, "@")
		if len(parts) != 2 || !validEvidenceToken(parts[0], 64) || !validEvidenceToken(parts[1], 64) {
			return errors.New("MONITOR_CLIENT_EVIDENCE_ALLOWED_CLIENTS必须使用精确family@version")
		}
		if _, ok := seen[value]; ok {
			return errors.New("MONITOR_CLIENT_EVIDENCE_ALLOWED_CLIENTS包含重复项")
		}
		seen[value] = struct{}{}
	}
	return nil
}

func (m *Monitor) clientEvidenceEnabled() bool {
	return len(strings.TrimSpace(m.cfg.ClientEvidenceToken)) >= 32 && len(strings.TrimSpace(m.cfg.ClientEvidenceHMACSecret)) >= 32 && len(m.cfg.ClientEvidenceAllowedClients) > 0
}

func (m *Monitor) clientEvidenceAllowed(family, version string) bool {
	want := family + "@" + version
	for _, allowed := range m.cfg.ClientEvidenceAllowedClients {
		if strings.TrimSpace(allowed) == want {
			return true
		}
	}
	return false
}

func (m *Monitor) deliveryEvidenceStore() *gorm.DB {
	if m.usageFactsDB != nil {
		return m.usageFactsDB
	}
	return m.storeDB
}

func decodeStrictDeliveryJSON(c *gin.Context, dst any, limit int64) error {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return errors.New("missing JSON body")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("multiple JSON values")
	}
	return nil
}

func validEvidenceToken(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._:-", r) {
			continue
		}
		return false
	}
	return true
}

func validEvidenceHMAC(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func clientEvidenceHMAC(secret, value string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func validClientOutcome(value string) bool {
	switch value {
	case "succeeded", "client_timeout", "transport_failure", "protocol_failure", "semantic_failure", "user_cancelled":
		return true
	default:
		return false
	}
}

func validClientEventType(value string) bool {
	return value == "request_started" || value == "request_outcome"
}

type ClientEvidenceInput struct {
	Version           int    `json:"version"`
	EventID           string `json:"event_id"`
	EventType         string `json:"event_type"`
	OccurredAtMS      int64  `json:"occurred_at_ms"`
	RequestID         string `json:"request_id"`
	LogicalRequestKey string `json:"logical_request_key,omitempty"`
	ClientVersion     string `json:"client_version"`
	Outcome           string `json:"outcome,omitempty"`
	ErrorSignature    string `json:"error_signature,omitempty"`
	RetryIndex        int    `json:"retry_index,omitempty"`
	Protocol          string `json:"protocol,omitempty"`
	Model             string `json:"model,omitempty"`
}

// 只持久化派生标识与有界枚举；不保存原始 Request ID、逻辑键、内容、密钥、IP 或自由文本错误。
type ClientDeliveryEvidence struct {
	EventID            string `gorm:"primaryKey;size:128;column:event_id" json:"event_id"`
	PayloadHash        string `gorm:"size:64;not null;column:payload_hash" json:"-"`
	OccurredAtMS       int64  `gorm:"index;column:occurred_at_ms" json:"occurred_at_ms"`
	ReceivedAt         int64  `gorm:"index;column:received_at" json:"received_at"`
	RequestTraceHMAC   string `gorm:"size:64;index;column:request_trace_hmac" json:"request_trace_hmac"`
	LogicalRequestHMAC string `gorm:"size:64;index;column:logical_request_hmac" json:"logical_request_hmac,omitempty"`
	ClientFamily       string `gorm:"size:64;index;column:client_family" json:"client_family"`
	ClientVersion      string `gorm:"size:64;column:client_version" json:"client_version"`
	EventType          string `gorm:"size:32;index;column:event_type" json:"event_type"`
	Outcome            string `gorm:"size:48;index" json:"outcome,omitempty"`
	ErrorSignature     string `gorm:"size:96;index;column:error_signature" json:"error_signature,omitempty"`
	RetryIndex         int    `gorm:"column:retry_index" json:"retry_index"`
	Protocol           string `gorm:"size:48;index" json:"protocol,omitempty"`
	Model              string `gorm:"size:128;index" json:"model,omitempty"`
}

type ClientEvidenceIngestBatch struct {
	ClientFamily string `gorm:"primaryKey;size:64;column:client_family"`
	BatchID      string `gorm:"primaryKey;size:128;column:batch_id"`
	PayloadHash  string `gorm:"size:64;not null;column:payload_hash"`
	Events       int
	ReceivedAt   int64 `gorm:"index;column:received_at"`
}

func normalizeClientEvidence(in ClientEvidenceInput, family, secret string, receivedAt int64) (ClientDeliveryEvidence, error) {
	if in.Version != clientEvidenceContractVersion || !validEvidenceToken(in.EventID, 128) || !validClientEventType(in.EventType) || !validEvidenceToken(in.ClientVersion, 64) || in.RetryIndex < 0 || in.RetryIndex > 100 {
		return ClientDeliveryEvidence{}, errors.New("invalid client evidence envelope")
	}
	if in.OccurredAtMS < time.Now().Add(-370*24*time.Hour).UnixMilli() || in.OccurredAtMS > time.Now().Add(10*time.Minute).UnixMilli() {
		return ClientDeliveryEvidence{}, errors.New("client evidence timestamp outside accepted range")
	}
	rawRequestID := strings.TrimSpace(in.RequestID)
	if rawRequestID == "" || len(rawRequestID) > 256 || strings.ContainsAny(rawRequestID, "\r\n\x00") {
		return ClientDeliveryEvidence{}, errors.New("invalid request id")
	}
	if in.EventType == "request_started" && (in.Outcome != "" || in.ErrorSignature != "") {
		return ClientDeliveryEvidence{}, errors.New("request_started cannot contain outcome")
	}
	if in.EventType == "request_outcome" && !validClientOutcome(in.Outcome) {
		return ClientDeliveryEvidence{}, errors.New("invalid client outcome")
	}
	if len(in.ErrorSignature) > 96 || in.ErrorSignature != "" && !validEvidenceToken(in.ErrorSignature, 96) || len(in.Protocol) > 48 || len(in.Model) > 128 || strings.ContainsAny(in.Protocol+in.Model, "\r\n\x00") {
		return ClientDeliveryEvidence{}, errors.New("invalid bounded client metadata")
	}
	requestHMAC := clientEvidenceHMAC(secret, rawRequestID)
	logicalHMAC := ""
	if raw := strings.TrimSpace(in.LogicalRequestKey); raw != "" {
		if len(raw) > 256 || strings.ContainsAny(raw, "\r\n\x00") {
			return ClientDeliveryEvidence{}, errors.New("invalid logical request key")
		}
		logicalHMAC = clientEvidenceHMAC(secret, family+"\x00"+raw)
	}
	canonical := []any{in.Version, in.EventID, in.EventType, in.OccurredAtMS, requestHMAC, logicalHMAC, family, in.ClientVersion, in.Outcome, in.ErrorSignature, in.RetryIndex, in.Protocol, in.Model}
	payload, _ := json.Marshal(canonical)
	hash := sha256.Sum256(payload)
	return ClientDeliveryEvidence{EventID: in.EventID, PayloadHash: hex.EncodeToString(hash[:]), OccurredAtMS: in.OccurredAtMS, ReceivedAt: receivedAt, RequestTraceHMAC: requestHMAC, LogicalRequestHMAC: logicalHMAC, ClientFamily: family, ClientVersion: in.ClientVersion, EventType: in.EventType, Outcome: in.Outcome, ErrorSignature: in.ErrorSignature, RetryIndex: in.RetryIndex, Protocol: in.Protocol, Model: in.Model}, nil
}

func clientBatchHash(rows []ClientDeliveryEvidence) string {
	copyRows := append([]ClientDeliveryEvidence(nil), rows...)
	sort.Slice(copyRows, func(i, j int) bool { return copyRows[i].EventID < copyRows[j].EventID })
	payload, _ := json.Marshal(copyRows)
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}

func (m *Monitor) ingestClientDeliveryEvidence(c *gin.Context) {
	if !m.clientEvidenceEnabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence disabled"})
		return
	}
	provided := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
	if subtle.ConstantTimeCompare([]byte(provided), []byte(m.cfg.ClientEvidenceToken)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var in struct {
		ClientFamily string                `json:"client_family"`
		BatchID      string                `json:"batch_id"`
		Events       []ClientEvidenceInput `json:"events"`
	}
	if err := decodeStrictDeliveryJSON(c, &in, 4<<20); err != nil || !validEvidenceToken(in.ClientFamily, 64) || !validEvidenceToken(in.BatchID, 128) || len(in.Events) == 0 || len(in.Events) > clientEvidenceMaxEvents {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid client evidence batch"})
		return
	}
	receivedAt := time.Now().Unix()
	rows := make([]ClientDeliveryEvidence, 0, len(in.Events))
	for i := range in.Events {
		event := in.Events[i]
		if !m.clientEvidenceAllowed(in.ClientFamily, event.ClientVersion) {
			c.JSON(http.StatusForbidden, gin.H{"error": "client family/version not allowed"})
			return
		}
		row, err := normalizeClientEvidence(event, in.ClientFamily, m.cfg.ClientEvidenceHMACSecret, receivedAt)
		in.Events[i].RequestID, in.Events[i].LogicalRequestKey = "", ""
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		rows = append(rows, row)
	}
	hash := clientBatchHash(rows)
	db := m.deliveryEvidenceStore()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence store unavailable"})
		return
	}
	duplicate := false
	err := db.Transaction(func(tx *gorm.DB) error {
		var receipt ClientEvidenceIngestBatch
		err := tx.First(&receipt, "client_family = ? AND batch_id = ?", in.ClientFamily, in.BatchID).Error
		if err == nil {
			if receipt.PayloadHash != hash {
				return errClientEvidenceConflict
			}
			duplicate = true
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		for _, row := range rows {
			var existing ClientDeliveryEvidence
			err := tx.First(&existing, "event_id = ?", row.EventID).Error
			if err == nil {
				if existing.PayloadHash != row.PayloadHash {
					return errClientEvidenceConflict
				}
				continue
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		return tx.Create(&ClientEvidenceIngestBatch{ClientFamily: in.ClientFamily, BatchID: in.BatchID, PayloadHash: hash, Events: len(rows), ReceivedAt: receivedAt}).Error
	})
	if errors.Is(err, errClientEvidenceConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": "client batch or event payload conflict"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence store unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "accepted": len(rows), "duplicate": duplicate, "batch_id": in.BatchID})
}

func deliveryRange(c *gin.Context) (int64, int64, error) {
	now := time.Now()
	if strings.TrimSpace(c.Query("from")) == "" && strings.TrimSpace(c.Query("to")) == "" {
		if hours := strings.TrimSpace(c.Query("hours")); hours != "" {
			if duration, err := time.ParseDuration(hours + "h"); err == nil && duration > 0 && duration <= 90*24*time.Hour {
				return now.Add(-duration).UnixMilli(), now.UnixMilli(), nil
			}
		}
	}
	scope, err := stabilityRange(c, now, 90)
	if err != nil {
		return 0, 0, err
	}
	return scope.FromTs * 1000, scope.ToTs * 1000, nil
}

func percentage(numerator, denominator int64) *float64 {
	if denominator <= 0 {
		return nil
	}
	value := float64(numerator) * 100 / float64(denominator)
	return &value
}

type clientEvidenceSummary struct {
	Started            int64    `json:"started"`
	Outcomes           int64    `json:"outcomes"`
	OrphanOutcomes     int64    `json:"orphan_outcomes"`
	TechnicalOutcomes  int64    `json:"technical_outcomes"`
	Successes          int64    `json:"successes"`
	Cancelled          int64    `json:"cancelled"`
	Failures           int64    `json:"failures"`
	Conflicts          int64    `json:"conflicts"`
	LogicalRequests    int64    `json:"logical_requests"`
	RetriedRequests    int64    `json:"retried_requests"`
	FirstSuccesses     int64    `json:"first_successes"`
	EventualSuccesses  int64    `json:"eventual_successes"`
	SuccessRate        *float64 `json:"success_rate"`
	Coverage           *float64 `json:"coverage"`
	EvidenceSufficient bool     `json:"evidence_sufficient"`
	LastReceivedAt     int64    `json:"last_received_at"`
}

// clientEvidenceStartedRows freezes the selected cohort by request start time.
// Outcomes may arrive after midnight or after the selected range closes; they
// still belong to the request's start cohort rather than to their receipt day.
func clientEvidenceStartedRows(db *gorm.DB, c *gin.Context, from, to int64) *gorm.DB {
	query := db.Model(&ClientDeliveryEvidence{}).
		Where("event_type = ? AND occurred_at_ms >= ? AND occurred_at_ms < ?", "request_started", from, to)
	if protocol := strings.TrimSpace(c.Query("protocol")); protocol != "" {
		query = query.Where("protocol = ?", protocol)
	}
	if model := strings.TrimSpace(c.Query("model")); model != "" {
		query = query.Where("model = ?", model)
	}
	if family := strings.TrimSpace(c.Query("client_family")); family != "" {
		query = query.Where("client_family = ?", family)
	}
	return query.Select(`request_trace_hmac,
		MAX(logical_request_hmac) AS logical_request_hmac,
		MAX(retry_index) AS retry_index,
		MIN(occurred_at_ms) AS started_at_ms`).Group("request_trace_hmac")
}

func (m *Monitor) serveDeliveryEvidenceSummary(c *gin.Context) {
	from, to, err := deliveryRange(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	summary := clientEvidenceSummary{}
	if !m.clientEvidenceEnabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "source": "controlled_client", "from_ms": from, "to_ms": to, "summary": summary, "client_metric_available": false, "status": "not_configured"})
		return
	}
	db := m.deliveryEvidenceStore()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence store unavailable"})
		return
	}
	starts := clientEvidenceStartedRows(db, c, from, to)
	read := func(result *gorm.DB) bool {
		if result.Error != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence query unavailable"})
			return false
		}
		return true
	}
	if !read(db.Table("(?) AS starts", starts).Count(&summary.Started)) {
		return
	}
	outcomeRows := db.Table("client_delivery_evidences AS evidence").
		Joins("JOIN (?) AS starts ON starts.request_trace_hmac = evidence.request_trace_hmac", starts).
		Where("evidence.event_type = ?", "request_outcome").Select(`evidence.request_trace_hmac,
		MAX(starts.logical_request_hmac) AS logical_request_hmac,
		MAX(starts.retry_index) AS retry_index,
		COUNT(DISTINCT outcome) AS outcome_kinds,
		MAX(CASE WHEN outcome = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
		MAX(CASE WHEN outcome = 'user_cancelled' THEN 1 ELSE 0 END) AS cancelled,
		MAX(CASE WHEN outcome NOT IN ('succeeded','user_cancelled') THEN 1 ELSE 0 END) AS failed,
		MIN(evidence.occurred_at_ms) AS outcome_at_ms`).Group("evidence.request_trace_hmac")
	queries := []struct {
		where string
		args  []any
		dest  *int64
	}{
		{"", nil, &summary.Outcomes},
		{"NOT (cancelled = 1 AND succeeded = 0 AND failed = 0)", nil, &summary.TechnicalOutcomes},
		{"succeeded = 1 AND failed = 0 AND cancelled = 0 AND outcome_kinds = 1", nil, &summary.Successes},
		{"cancelled = 1 AND succeeded = 0 AND failed = 0 AND outcome_kinds = 1", nil, &summary.Cancelled},
		{"failed = 1 AND succeeded = 0 AND outcome_kinds = 1", nil, &summary.Failures},
		{"outcome_kinds > 1", nil, &summary.Conflicts},
	}
	for _, query := range queries {
		q := db.Table("(?) AS outcomes", outcomeRows)
		if query.where != "" {
			q = q.Where(query.where, query.args...)
		}
		if !read(q.Count(query.dest)) {
			return
		}
	}
	orphanBase := db.Table("client_delivery_evidences AS evidence").
		Where("evidence.event_type = ? AND evidence.occurred_at_ms >= ? AND evidence.occurred_at_ms < ?", "request_outcome", from, to).
		Where("NOT EXISTS (?)", db.Model(&ClientDeliveryEvidence{}).Select("1").Where("event_type = ? AND request_trace_hmac = evidence.request_trace_hmac", "request_started"))
	if protocol := strings.TrimSpace(c.Query("protocol")); protocol != "" {
		orphanBase = orphanBase.Where("evidence.protocol = ?", protocol)
	}
	if model := strings.TrimSpace(c.Query("model")); model != "" {
		orphanBase = orphanBase.Where("evidence.model = ?", model)
	}
	if family := strings.TrimSpace(c.Query("client_family")); family != "" {
		orphanBase = orphanBase.Where("evidence.client_family = ?", family)
	}
	if !read(orphanBase.Distinct("evidence.request_trace_hmac").Count(&summary.OrphanOutcomes)) {
		return
	}
	logical := db.Table("(?) AS starts", starts).
		Joins("LEFT JOIN (?) AS outcomes ON outcomes.request_trace_hmac = starts.request_trace_hmac", outcomeRows).
		Where("starts.logical_request_hmac <> ''").Select(`starts.logical_request_hmac,
		COUNT(*) AS attempts,
		MAX(COALESCE(outcomes.succeeded,0)) AS eventual_success`).Group("starts.logical_request_hmac")
	if !read(db.Table("(?) AS logical", logical).Count(&summary.LogicalRequests)) || !read(db.Table("(?) AS logical", logical).Where("attempts > 1").Count(&summary.RetriedRequests)) || !read(db.Table("(?) AS logical", logical).Where("eventual_success = 1").Count(&summary.EventualSuccesses)) {
		return
	}
	first := db.Table("client_delivery_evidences AS evidence").
		Joins("JOIN (?) AS starts ON starts.request_trace_hmac = evidence.request_trace_hmac", starts).
		Where("starts.logical_request_hmac <> '' AND evidence.event_type = 'request_outcome' AND evidence.outcome <> ?", "user_cancelled").Select(`starts.logical_request_hmac, evidence.outcome,
		ROW_NUMBER() OVER (PARTITION BY starts.logical_request_hmac ORDER BY starts.retry_index ASC, evidence.occurred_at_ms ASC, evidence.event_id ASC) AS rank`)
	associatedEvents := db.Table("client_delivery_evidences AS evidence").
		Joins("JOIN (?) AS starts ON starts.request_trace_hmac = evidence.request_trace_hmac", starts)
	if !read(db.Table("(?) AS first", first).Where("rank = 1 AND outcome = ?", "succeeded").Count(&summary.FirstSuccesses)) || !read(associatedEvents.Select("COALESCE(MAX(evidence.received_at),0)").Scan(&summary.LastReceivedAt)) {
		return
	}
	summary.SuccessRate = percentage(summary.Successes, summary.TechnicalOutcomes)
	summary.Coverage = percentage(summary.Outcomes, summary.Started)
	summary.EvidenceSufficient = summary.Started >= minSample && summary.TechnicalOutcomes >= minSample && summary.Coverage != nil && *summary.Coverage >= 95 && summary.Conflicts == 0 && summary.OrphanOutcomes == 0
	status := "insufficient_coverage"
	if summary.EvidenceSufficient {
		status = "available"
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "source": "controlled_client", "from_ms": from, "to_ms": to, "summary": summary, "client_metric_available": summary.Outcomes > 0, "status": status})
}

func (m *Monitor) serveDeliveryEvidenceTimeline(c *gin.Context) {
	requestHMAC := strings.TrimSpace(c.Query("request"))
	if !validEvidenceHMAC(requestHMAC) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid request trace required"})
		return
	}
	m.serveClientTimeline(c, requestHMAC)
}

func (m *Monitor) serveDeliveryEvidenceTimelineLookup(c *gin.Context) {
	var in struct {
		RequestID string `json:"request_id"`
	}
	if err := decodeStrictDeliveryJSON(c, &in, 4<<10); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request id lookup"})
		return
	}
	raw := strings.TrimSpace(in.RequestID)
	if !m.clientEvidenceEnabled() || raw == "" || len(raw) > 256 || strings.ContainsAny(raw, "\r\n\x00") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request id lookup unavailable"})
		return
	}
	hash := clientEvidenceHMAC(m.cfg.ClientEvidenceHMACSecret, raw)
	in.RequestID = ""
	m.serveClientTimeline(c, hash)
}

func (m *Monitor) serveClientTimeline(c *gin.Context, requestHMAC string) {
	db := m.deliveryEvidenceStore()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence store unavailable"})
		return
	}
	var events []ClientDeliveryEvidence
	if err := db.Where("request_trace_hmac = ?", requestHMAC).Order("occurred_at_ms ASC, event_id ASC").Find(&events).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence query unavailable"})
		return
	}
	if len(events) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "request evidence not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"request": gin.H{"request_trace_hmac": requestHMAC}, "client_evidence": events})
}

func (m *Monitor) serveDeliveryEvidenceIssues(c *gin.Context) {
	from, to, err := deliveryRange(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	db := m.deliveryEvidenceStore()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence store unavailable"})
		return
	}
	starts := clientEvidenceStartedRows(db, c, from, to)
	query := db.Table("client_delivery_evidences AS evidence").
		Joins("JOIN (?) AS starts ON starts.request_trace_hmac = evidence.request_trace_hmac", starts).
		Where("evidence.event_type = ? AND evidence.outcome NOT IN ?", "request_outcome", []string{"succeeded", "user_cancelled"}).
		Select("evidence.*")
	var rows []ClientDeliveryEvidence
	if err := query.Order("occurred_at_ms DESC").Limit(100).Find(&rows).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "client evidence query unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": m.clientEvidenceEnabled(), "source": "controlled_client", "from_ms": from, "to_ms": to, "issues": rows})
}

func (m *Monitor) startDeliveryEvidenceMaintenance(ctx context.Context) {
	if !m.clientEvidenceEnabled() {
		return
	}
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			m.pruneClientEvidence(time.Now())
			select {
			case <-ctx.Done():
				return
			case <-m.shutdown:
				return
			case <-ticker.C:
			}
		}
	}()
}

func (m *Monitor) pruneClientEvidence(now time.Time) {
	db := m.deliveryEvidenceStore()
	if db == nil {
		return
	}
	_ = db.Where("occurred_at_ms < ?", now.Add(-clientEvidenceRawRetention).UnixMilli()).Delete(&ClientDeliveryEvidence{}).Error
	_ = db.Where("received_at < ?", now.Add(-clientEvidenceReceiptRetention).Unix()).Delete(&ClientEvidenceIngestBatch{}).Error
}
