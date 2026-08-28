package monitor

// nginx_evidence.go 是入口请求级证据的独立、短期存储层。它不保存原始
// Request ID、IP、query、Header、请求体或响应体；两个 Request ID 都必须
// 已在节点侧使用共享密钥做 HMAC-SHA256。

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const nginxEvidenceSchemaVersion = 1

const nginxEvidenceWriteReserveBytes int64 = 4 << 20

var (
	nginxEvidenceHex64Pattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	nginxEvidenceKeyIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$`)
	nginxEvidenceFileIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)
	errNginxEvidenceBatchConflict  = errors.New("nginx evidence batch id reused with different payload")
	errNginxEvidenceSourceConflict = errors.New("nginx evidence source cursor is not continuous")
)

type NginxRequestEvidence struct {
	EventID          string `gorm:"primaryKey;size:64;column:event_id" json:"event_id"`
	EventMS          int64  `gorm:"index:idx_nginx_evidence_time;column:event_ms" json:"event_ms"`
	Node             string `gorm:"size:64;not null" json:"node,omitempty"`
	Route            string `gorm:"size:48;not null" json:"route"`
	Method           string `gorm:"size:8;not null" json:"method"`
	Status           int    `json:"status"`
	UpstreamStatus   int    `gorm:"column:upstream_status" json:"upstream_status"`
	UpstreamAttempts int    `gorm:"column:upstream_attempts" json:"upstream_attempts"`
	UpstreamStatuses string `gorm:"size:64;column:upstream_statuses" json:"-"`
	RequestMS        int64  `gorm:"column:request_ms" json:"request_ms"`
	UpstreamMS       int64  `gorm:"column:upstream_ms" json:"upstream_ms"`
	UpstreamPresent  bool   `gorm:"column:upstream_present" json:"upstream_present"`
	ConnectMS        int64  `gorm:"column:connect_ms" json:"connect_ms"`
	HeaderMS         int64  `gorm:"column:header_ms" json:"header_ms"`
	BytesSent        int64  `gorm:"column:bytes_sent" json:"bytes_sent"`
	Completion       string `gorm:"size:24" json:"completion"`
	NginxIDHMAC      string `gorm:"size:64;index:idx_nginx_evidence_nginx_id,priority:2;column:nginx_id_hmac" json:"-"`
	OneAPIIDHMAC     string `gorm:"size:64;index:idx_nginx_evidence_oneapi_id,priority:2;column:oneapi_id_hmac" json:"-"`
	HMACKeyID        string `gorm:"size:32;index:idx_nginx_evidence_nginx_id,priority:1;index:idx_nginx_evidence_oneapi_id,priority:1;column:hmac_key_id" json:"-"`
	BatchID          string `gorm:"size:64;column:batch_id" json:"-"`
	ReceivedAt       int64  `gorm:"column:received_at" json:"-"`
}

type NginxEvidenceIngestBatch struct {
	Node         string `gorm:"primaryKey;size:64"`
	SourceKind   string `gorm:"primaryKey;size:16;column:source_kind"`
	BatchID      string `gorm:"primaryKey;size:64;column:batch_id"`
	PayloadHash  string `gorm:"size:64;not null;column:payload_hash"`
	FirstEventMS int64  `gorm:"column:first_event_ms"`
	LastEventMS  int64  `gorm:"column:last_event_ms"`
	EventCount   int    `gorm:"column:event_count"`
	Accepted     int
	Rejected     int
	ReceivedAt   int64  `gorm:"index;column:received_at"`
	SourceFileID string `gorm:"size:64;column:source_file_id"`
	StartOffset  int64  `gorm:"column:start_offset"`
	EndOffset    int64  `gorm:"column:end_offset"`
}

type NginxEvidenceSourceState struct {
	Node                         string `gorm:"primaryKey;size:64"`
	SourceKind                   string `gorm:"primaryKey;size:16;column:source_kind"`
	Mode                         string `gorm:"size:16"`
	LogSchema                    int
	HMACKeyID                    string `gorm:"size:32;column:hmac_key_id"`
	LastEventMS                  int64
	LastIngestAt                 int64
	Accepted                     int64
	Rejected                     int64
	MissingOneAPIID              int64
	OutboxBytes                  int64
	OutboxBatches                int64
	DroppedEvents                int64
	GapCount                     int64
	AppliedGapCount              int64 `gorm:"column:applied_gap_count"`
	LastGapFromMS                int64
	LastGapToMS                  int64
	LastFileID                   string `gorm:"size:64;column:last_file_id"`
	LastEndOffset                int64  `gorm:"column:last_end_offset"`
	CursorDiscontinuities        int64  `gorm:"column:cursor_discontinuities"`
	DiscardedLines               int64  `gorm:"column:discarded_lines"`
	RejectedBytes                int64  `gorm:"column:rejected_bytes"`
	RejectedBatches              int64  `gorm:"column:rejected_batches"`
	UnknownDroppedBatches        int64  `gorm:"column:unknown_dropped_batches"`
	EvidenceEligible             int64  `gorm:"column:evidence_eligible"`
	EvidenceParseRejected        int64  `gorm:"column:evidence_parse_rejected"`
	LastEvidenceParseRejectedAt  int64  `gorm:"column:last_evidence_parse_rejected_at"`
	EvidencePersistFailures      int64  `gorm:"column:evidence_persist_failures"`
	EvidenceDroppedEvents        int64  `gorm:"column:evidence_dropped_events"`
	LastEvidencePersistFailureAt int64  `gorm:"column:last_evidence_persist_failure_at"`
	AppliedPersistFailures       int64  `gorm:"column:applied_persist_failures"`
}

type nginxEvidenceSourceRange struct {
	Kind                         string `json:"kind"`
	FileID                       string `json:"file_id"`
	StartOffset                  int64  `json:"start_offset"`
	EndOffset                    int64  `json:"end_offset"`
	FirstEventMS                 int64  `json:"first_event_ms"`
	LastEventMS                  int64  `json:"last_event_ms"`
	CursorDiscontinuities        int64  `json:"cursor_discontinuities"`
	LastCursorDiscontinuity      int64  `json:"last_cursor_discontinuity_at"`
	DiscardedLines               int64  `json:"discarded_lines"`
	LastDiscardedAt              int64  `json:"last_discarded_at"`
	EvidenceEligible             int64  `json:"evidence_eligible"`
	EvidenceParseRejected        int64  `json:"evidence_parse_rejected"`
	LastEvidenceParseRejectedAt  int64  `json:"last_evidence_parse_rejected_at"`
	EvidencePersistFailures      int64  `json:"evidence_persist_failures"`
	EvidenceDroppedEvents        int64  `json:"evidence_dropped_events"`
	LastEvidencePersistFailureAt int64  `json:"last_evidence_persist_failure_at"`
}

type nginxEvidenceTelemetry struct {
	OutboxBytes           int64 `json:"outbox_bytes"`
	OutboxBatches         int64 `json:"outbox_batches"`
	DroppedEvents         int64 `json:"dropped_events"`
	GapCount              int64 `json:"gap_count"`
	LastGapFromMS         int64 `json:"last_gap_from_ms"`
	LastGapToMS           int64 `json:"last_gap_to_ms"`
	RejectedBytes         int64 `json:"rejected_bytes"`
	RejectedBatches       int64 `json:"rejected_batches"`
	UnknownDroppedBatches int64 `json:"unknown_dropped_batches"`
}

type nginxEvidenceEvent struct {
	EventID          string `json:"event_id"`
	EventMS          int64  `json:"event_ms"`
	Route            string `json:"route"`
	Method           string `json:"method"`
	Status           int    `json:"status"`
	UpstreamStatus   int    `json:"upstream_status"`
	UpstreamAttempts int    `json:"upstream_attempts"`
	UpstreamStatuses []int  `json:"upstream_statuses,omitempty"`
	RequestMS        int64  `json:"request_ms"`
	UpstreamMS       int64  `json:"upstream_ms"`
	UpstreamPresent  bool   `json:"upstream_present"`
	ConnectMS        int64  `json:"connect_ms"`
	HeaderMS         int64  `json:"header_ms"`
	BytesSent        int64  `json:"bytes_sent"`
	Completion       string `json:"completion"`
	NginxIDHMAC      string `json:"nginx_id_hmac,omitempty"`
	OneAPIIDHMAC     string `json:"oneapi_id_hmac,omitempty"`
}

type nginxEvidenceBatch struct {
	SchemaVersion int                      `json:"schema_version"`
	Node          string                   `json:"node"`
	BatchID       string                   `json:"batch_id"`
	PayloadHash   string                   `json:"payload_hash"`
	LogSchema     int                      `json:"log_schema"`
	HMACKeyID     string                   `json:"hmac_key_id"`
	Source        nginxEvidenceSourceRange `json:"source"`
	Events        []nginxEvidenceEvent     `json:"events"`
	Telemetry     nginxEvidenceTelemetry   `json:"telemetry"`
}

type nginxEvidenceHealth struct {
	Mode                         string `json:"mode"`
	Enabled                      bool   `json:"enabled"`
	StoreReachable               bool   `json:"store_reachable"`
	StoreBytes                   int64  `json:"store_bytes"`
	LinkageVerified              bool   `json:"linkage_verified"`
	SourceCount                  int    `json:"source_count"`
	UnhealthySources             int    `json:"unhealthy_sources"`
	LastIngestAt                 int64  `json:"last_ingest_at"`
	LastEventMS                  int64  `json:"last_event_ms"`
	OutboxBytes                  int64  `json:"outbox_bytes"`
	OutboxBatches                int64  `json:"outbox_batches"`
	DroppedEvents                int64  `json:"dropped_events"`
	GapCount                     int64  `json:"gap_count"`
	MissingOneAPIID              int64  `json:"missing_oneapi_id"`
	Accepted                     int64  `json:"accepted"`
	Rejected                     int64  `json:"rejected"`
	RejectedBytes                int64  `json:"rejected_bytes"`
	RejectedBatches              int64  `json:"rejected_batches"`
	UnknownDroppedBatches        int64  `json:"unknown_dropped_batches"`
	EvidenceEligible             int64  `json:"evidence_eligible"`
	EvidenceParseRejected        int64  `json:"evidence_parse_rejected"`
	EvidencePersistFailures      int64  `json:"evidence_persist_failures"`
	EvidenceDroppedEvents        int64  `json:"evidence_dropped_events"`
	LastEvidencePersistFailureAt int64  `json:"last_evidence_persist_failure_at"`
}

type nginxEvidenceLookupResponse struct {
	Mode            string                `json:"mode"`
	LinkageVerified bool                  `json:"linkage_verified"`
	Found           bool                  `json:"found"`
	Evidence        *NginxRequestEvidence `json:"evidence,omitempty"`
}

func nginxEvidenceMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "pilot", "verified":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return "off"
	}
}

func (m *Monitor) openNginxEvidenceStore() error {
	mode := nginxEvidenceMode(m.cfg.NginxEvidenceMode)
	if mode == "off" {
		return nil
	}
	if !m.cfg.NginxEnabled {
		return errors.New("evidence mode requires MONITOR_NGINX_ENABLED=true")
	}
	path := strings.TrimSpace(m.cfg.NginxEvidenceStorePath)
	if path == "" {
		return errors.New("evidence store path is required")
	}
	if sameStorePath(path, m.cfg.StorePath) || sameStorePath(path, m.cfg.UsageFactsStorePath) {
		return errors.New("evidence store must be separate from main and usage facts stores")
	}
	if len(m.cfg.NginxEvidenceHMACKey) < 32 || !nginxEvidenceKeyIDPattern.MatchString(m.cfg.NginxEvidenceHMACKeyID) {
		return errors.New("evidence HMAC key must be at least 32 bytes and key id must be valid")
	}
	dsn := path + "?_pragma=busy_timeout(3000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return fmt.Errorf("open evidence store: %w", err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &NginxEvidenceSourceState{}); err != nil {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		return fmt.Errorf("migrate evidence store: %w", err)
	}
	if err := configureNginxEvidencePageLimit(db, m.cfg.NginxEvidenceMaxMiB); err != nil {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		return fmt.Errorf("secure evidence store permissions: %w", err)
	}
	m.nginxEvidenceDB = db
	if sqlDB, dbErr := db.DB(); dbErr == nil {
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
	}
	return nil
}

// configureNginxEvidencePageLimit uses SQLite's reusable page accounting instead
// of the physical file size. Expired rows add pages to freelist, so retention can
// recover write capacity without VACUUM shrinking the main file.
func configureNginxEvidencePageLimit(db *gorm.DB, maxMiB int) error {
	var pageSize int64
	if err := db.Raw("PRAGMA page_size").Scan(&pageSize).Error; err != nil {
		return fmt.Errorf("read evidence page size: %w", err)
	}
	if pageSize <= 0 {
		return errors.New("invalid evidence page size")
	}
	maxPages := (int64(maxMiB) << 20) / pageSize
	if maxPages <= 0 {
		return errors.New("evidence page limit is too small")
	}
	var applied int64
	if err := db.Raw(fmt.Sprintf("PRAGMA max_page_count = %d", maxPages)).Scan(&applied).Error; err != nil {
		return fmt.Errorf("set evidence page limit: %w", err)
	}
	if applied > maxPages {
		return fmt.Errorf("evidence store already exceeds configured page limit: pages=%d limit=%d", applied, maxPages)
	}
	return nil
}

func nginxEvidenceReusableBytes(db *gorm.DB) (int64, error) {
	var pageSize, pageCount, freePages, maxPages int64
	for query, target := range map[string]*int64{
		"PRAGMA page_size":      &pageSize,
		"PRAGMA page_count":     &pageCount,
		"PRAGMA freelist_count": &freePages,
		"PRAGMA max_page_count": &maxPages,
	} {
		if err := db.Raw(query).Scan(target).Error; err != nil {
			return 0, err
		}
	}
	if pageSize <= 0 || pageCount < 0 || freePages < 0 || maxPages < pageCount {
		return 0, errors.New("invalid evidence page accounting")
	}
	return (maxPages - pageCount + freePages) * pageSize, nil
}

func nginxEvidenceHash(in nginxEvidenceBatch) string {
	copyIn := in
	copyIn.PayloadHash = ""
	copyIn.Telemetry = nginxEvidenceTelemetry{}
	copyIn.Events = append([]nginxEvidenceEvent(nil), in.Events...)
	sort.Slice(copyIn.Events, func(i, j int) bool { return copyIn.Events[i].EventID < copyIn.Events[j].EventID })
	payload, _ := json.Marshal(copyIn)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func nginxEvidenceIDHMAC(key, domain, raw string) string {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(mac.Sum(nil))
}

func (m *Monitor) nginxEvidenceKeyAllowed(keyID string) bool {
	return keyID == m.cfg.NginxEvidenceHMACKeyID || m.cfg.NginxEvidencePreviousHMACKey != "" && keyID == m.cfg.NginxEvidencePreviousHMACKeyID
}

func validNginxEvidenceLookupID(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "-" || len(raw) > 256 {
		return false
	}
	for _, r := range raw {
		if r < 0x21 || r == 0x7f {
			return false
		}
	}
	return true
}

func (m *Monitor) nginxEvidenceHealth(ctx context.Context, now time.Time) nginxEvidenceHealth {
	mode := nginxEvidenceMode(m.cfg.NginxEvidenceMode)
	result := nginxEvidenceHealth{Mode: mode, Enabled: mode != "off", LinkageVerified: mode == "verified"}
	if mode == "off" || m.nginxEvidenceDB == nil {
		return result
	}
	probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sqlDB, err := m.nginxEvidenceDB.DB()
	if err != nil || sqlDB.PingContext(probeCtx) != nil {
		return result
	}
	result.StoreReachable = true
	for _, path := range []string{m.cfg.NginxEvidenceStorePath, m.cfg.NginxEvidenceStorePath + "-wal", m.cfg.NginxEvidenceStorePath + "-shm"} {
		if info, statErr := os.Stat(path); statErr == nil {
			result.StoreBytes += info.Size()
		}
	}
	var states []NginxEvidenceSourceState
	if err := m.nginxEvidenceDB.WithContext(probeCtx).Limit(64).Find(&states).Error; err != nil {
		result.StoreReachable = false
		return result
	}
	result.SourceCount = len(states)
	nowUnix := now.Unix()
	for _, state := range states {
		result.LastIngestAt = max(result.LastIngestAt, state.LastIngestAt)
		result.LastEventMS = max(result.LastEventMS, state.LastEventMS)
		result.OutboxBytes += state.OutboxBytes
		result.OutboxBatches += state.OutboxBatches
		result.DroppedEvents += state.DroppedEvents
		result.GapCount += state.GapCount
		result.MissingOneAPIID += state.MissingOneAPIID
		result.Accepted += state.Accepted
		result.Rejected += state.Rejected
		result.RejectedBytes += state.RejectedBytes
		result.RejectedBatches += state.RejectedBatches
		result.UnknownDroppedBatches += state.UnknownDroppedBatches
		result.EvidenceEligible += state.EvidenceEligible
		result.EvidenceParseRejected += state.EvidenceParseRejected
		result.EvidencePersistFailures += state.EvidencePersistFailures
		result.EvidenceDroppedEvents += state.EvidenceDroppedEvents
		result.LastEvidencePersistFailureAt = max(result.LastEvidencePersistFailureAt, state.LastEvidencePersistFailureAt)
		if state.LastIngestAt == 0 || nowUnix-state.LastIngestAt > 180 || state.LogSchema != 2 || state.OutboxBytes >= 16<<20 || state.OutboxBatches >= 1000 ||
			(state.LastEvidenceParseRejectedAt > 0 && nowUnix-state.LastEvidenceParseRejectedAt <= 900) ||
			(state.LastEvidencePersistFailureAt > 0 && nowUnix-state.LastEvidencePersistFailureAt <= 900) ||
			(state.LastGapToMS > 0 && now.UnixMilli()-state.LastGapToMS < int64(m.cfg.NginxEvidenceRetentionHours)*3_600_000) {
			result.UnhealthySources++
		}
	}
	// Missing an allowed access source is observable even when a stale or foreign
	// source row exists; compare names, not just row counts.
	present := make(map[string]bool, len(states))
	for _, state := range states {
		if state.SourceKind == "access" {
			present[state.Node] = true
		}
	}
	for _, node := range m.cfg.NginxAllowedNodes {
		if !present[node] {
			result.UnhealthySources++
		}
	}
	return result
}

func (m *Monitor) serveNginxEvidenceLookup(c *gin.Context) {
	mode := nginxEvidenceMode(m.cfg.NginxEvidenceMode)
	if mode == "off" || m.nginxEvidenceDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx evidence disabled"})
		return
	}
	var in struct {
		RequestID string `json:"request_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || !validNginxEvidenceLookupID(in.RequestID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request_id"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 500*time.Millisecond)
	defer cancel()
	var row NginxRequestEvidence
	query := m.nginxEvidenceDB.WithContext(ctx).Where("hmac_key_id = ? AND oneapi_id_hmac = ?", m.cfg.NginxEvidenceHMACKeyID,
		nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", in.RequestID))
	if m.cfg.NginxEvidencePreviousHMACKey != "" {
		query = query.Or("hmac_key_id = ? AND oneapi_id_hmac = ?", m.cfg.NginxEvidencePreviousHMACKeyID,
			nginxEvidenceIDHMAC(m.cfg.NginxEvidencePreviousHMACKey, "oneapi-request-id", in.RequestID))
	}
	err := query.
		Order("event_ms DESC").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusOK, nginxEvidenceLookupResponse{Mode: mode, LinkageVerified: mode == "verified", Found: false})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx evidence lookup unavailable"})
		return
	}
	c.JSON(http.StatusOK, nginxEvidenceLookupResponse{Mode: mode, LinkageVerified: mode == "verified", Found: true, Evidence: &row})
}

func (m *Monitor) pruneNginxEvidenceOnce(ctx context.Context, now time.Time) error {
	if m.nginxEvidenceDB == nil {
		return nil
	}
	retentionHours := m.cfg.NginxEvidenceRetentionHours
	if retentionHours < 24 || retentionHours > 24*31 {
		retentionHours = 168
	}
	eventCutoffMS := now.Add(-time.Duration(retentionHours) * time.Hour).UnixMilli()
	batchCutoff := now.Add(-time.Duration(retentionHours+24) * time.Hour).Unix()
	for i := 0; i < 5; i++ {
		result := m.nginxEvidenceDB.WithContext(ctx).Exec(`DELETE FROM nginx_request_evidences WHERE event_id IN (
			SELECT event_id FROM nginx_request_evidences WHERE event_ms < ? ORDER BY event_ms LIMIT 10000
		)`, eventCutoffMS)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected < 10000 {
			break
		}
	}
	return m.nginxEvidenceDB.WithContext(ctx).Exec(`DELETE FROM nginx_evidence_ingest_batches WHERE rowid IN (
		SELECT rowid FROM nginx_evidence_ingest_batches WHERE received_at < ? ORDER BY received_at LIMIT 1000
	)`, batchCutoff).Error
}

func (m *Monitor) startNginxEvidenceMaintenance(ctx context.Context) {
	if m.nginxEvidenceDB == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			pruneCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			_ = m.pruneNginxEvidenceOnce(pruneCtx, time.Now())
			cancel()
			select {
			case <-ctx.Done():
				return
			case <-m.shutdownSignal():
				return
			case <-ticker.C:
			}
		}
	}()
}

func validNginxEvidenceEvent(e nginxEvidenceEvent, nowMS int64, retentionHours int) bool {
	if !nginxEvidenceHex64Pattern.MatchString(e.EventID) || e.EventMS <= 0 || e.EventMS > nowMS+300_000 {
		return false
	}
	if retentionHours < 1 || retentionHours > 24*31 {
		retentionHours = 168
	}
	if e.EventMS < nowMS-int64(retentionHours+24)*3_600_000 {
		return false
	}
	if e.Method != "POST" || !isNginxInferenceRoute(e.Route) || e.Status < 100 || e.Status > 599 || e.UpstreamStatus < 0 || e.UpstreamStatus > 599 {
		return false
	}
	if e.UpstreamAttempts < 0 || e.UpstreamAttempts > 8 || len(e.UpstreamStatuses) != e.UpstreamAttempts {
		return false
	}
	for _, status := range e.UpstreamStatuses {
		if status < 100 || status > 599 {
			return false
		}
	}
	if e.UpstreamAttempts == 0 && e.UpstreamStatus != 0 || e.UpstreamAttempts > 0 && e.UpstreamStatuses[len(e.UpstreamStatuses)-1] != e.UpstreamStatus {
		return false
	}
	if e.RequestMS < 0 || e.RequestMS > 86_400_000 || e.UpstreamMS < 0 || e.UpstreamMS > 86_400_000 ||
		e.ConnectMS < 0 || e.ConnectMS > 86_400_000 || e.HeaderMS < 0 || e.HeaderMS > 86_400_000 || e.BytesSent < 0 || e.BytesSent > 16<<30 {
		return false
	}
	if e.Completion != "complete_at_edge" && e.Completion != "incomplete_at_edge" && e.Completion != "unknown" {
		return false
	}
	for _, h := range []string{e.NginxIDHMAC, e.OneAPIIDHMAC} {
		if h != "" && !nginxEvidenceHex64Pattern.MatchString(h) {
			return false
		}
	}
	return true
}

func validNginxEvidenceSource(source nginxEvidenceSourceRange, eventCount int) bool {
	if source.Kind != "access" || !nginxEvidenceFileIDPattern.MatchString(source.FileID) || source.StartOffset < 0 || source.EndOffset < source.StartOffset || source.EndOffset > 1<<50 ||
		source.FirstEventMS < 0 || source.LastEventMS < source.FirstEventMS {
		return false
	}
	if source.CursorDiscontinuities < 0 || source.LastCursorDiscontinuity < 0 || source.DiscardedLines < 0 || source.LastDiscardedAt < 0 {
		return false
	}
	if source.EvidenceEligible < 0 || source.EvidenceParseRejected < 0 || source.EvidenceParseRejected > source.EvidenceEligible || source.LastEvidenceParseRejectedAt < 0 ||
		source.EvidencePersistFailures < 0 || source.EvidenceDroppedEvents < 0 || source.LastEvidencePersistFailureAt < 0 ||
		(source.EvidencePersistFailures == 0) != (source.LastEvidencePersistFailureAt == 0) ||
		(source.EvidenceParseRejected == 0) != (source.LastEvidenceParseRejectedAt == 0) {
		return false
	}
	if (source.CursorDiscontinuities == 0) != (source.LastCursorDiscontinuity == 0) || (source.DiscardedLines == 0) != (source.LastDiscardedAt == 0) {
		return false
	}
	if eventCount == 0 {
		return source.FirstEventMS == 0 && source.LastEventMS == 0
	}
	return source.FirstEventMS > 0 && source.LastEventMS > 0
}

func continuousNginxEvidenceSource(previous NginxEvidenceSourceState, source nginxEvidenceSourceRange, telemetry nginxEvidenceTelemetry) bool {
	if previous.LastFileID == "" {
		// Enabling pilot on an already-running collector legitimately starts at
		// its current durable cursor. This establishes the explicit pilot baseline;
		// continuity is enforced from the next batch onward.
		return true
	}
	if source.CursorDiscontinuities < previous.CursorDiscontinuities || source.DiscardedLines < previous.DiscardedLines {
		return false
	}
	if source.FileID == previous.LastFileID {
		return source.StartOffset == previous.LastEndOffset || source.CursorDiscontinuities > previous.CursorDiscontinuities || telemetry.GapCount > previous.AppliedGapCount || source.EvidencePersistFailures > previous.AppliedPersistFailures
	}
	return source.StartOffset == 0 || source.CursorDiscontinuities > previous.CursorDiscontinuities || telemetry.GapCount > previous.AppliedGapCount || source.EvidencePersistFailures > previous.AppliedPersistFailures
}

func isNginxInferenceRoute(route string) bool {
	switch route {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/*":
		return true
	default:
		return false
	}
}

func (m *Monitor) ingestNginxEvidence(c *gin.Context) {
	if nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" || m.nginxEvidenceDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx evidence disabled"})
		return
	}
	if !m.checkIngest(c) {
		return
	}
	if reusable, err := nginxEvidenceReusableBytes(m.nginxEvidenceDB); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "evidence store capacity unavailable"})
		return
	} else if reusable < nginxEvidenceWriteReserveBytes {
		c.JSON(http.StatusInsufficientStorage, gin.H{"error": "evidence store size limit reached"})
		return
	}
	var in nginxEvidenceBatch
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid evidence payload"})
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid evidence payload"})
		return
	}
	if in.SchemaVersion != nginxEvidenceSchemaVersion || in.LogSchema < 0 || in.LogSchema > 2 || len(in.Events) > 0 && in.LogSchema != 2 || !nginxNodeNamePattern.MatchString(in.Node) || !m.nginxNodeAllowed(in.Node) ||
		!validIngestBatchID(in.BatchID) || len(in.Events) > 1000 || !m.nginxEvidenceKeyAllowed(in.HMACKeyID) ||
		!nginxEvidenceHex64Pattern.MatchString(in.PayloadHash) || !validNginxEvidenceSource(in.Source, len(in.Events)) ||
		in.Telemetry.OutboxBytes < 0 || in.Telemetry.OutboxBytes > 1<<40 || in.Telemetry.OutboxBatches < 0 || in.Telemetry.OutboxBatches > 1_000_000 ||
		in.Telemetry.RejectedBytes < 0 || in.Telemetry.RejectedBytes > 1<<40 || in.Telemetry.RejectedBatches < 0 || in.Telemetry.RejectedBatches > 1_000_000 ||
		in.Telemetry.DroppedEvents < 0 || in.Telemetry.UnknownDroppedBatches < 0 || in.Telemetry.GapCount < 0 || in.Telemetry.LastGapFromMS < 0 || in.Telemetry.LastGapToMS < in.Telemetry.LastGapFromMS {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid evidence envelope"})
		return
	}
	computed := nginxEvidenceHash(in)
	if computed != in.PayloadHash {
		c.JSON(http.StatusBadRequest, gin.H{"error": "payload hash mismatch"})
		return
	}
	if len(in.Events) > 0 {
		firstMS, lastMS := in.Events[0].EventMS, in.Events[0].EventMS
		for _, event := range in.Events[1:] {
			firstMS = min(firstMS, event.EventMS)
			lastMS = max(lastMS, event.EventMS)
		}
		if in.Source.FirstEventMS != firstMS || in.Source.LastEventMS != lastMS {
			c.JSON(http.StatusBadRequest, gin.H{"error": "evidence source range mismatch"})
			return
		}
	}
	now, nowMS := time.Now().Unix(), time.Now().UnixMilli()
	advancesRange := in.Source.EndOffset > in.Source.StartOffset
	retention := m.cfg.NginxEvidenceRetentionHours
	acceptedRows := make([]NginxRequestEvidence, 0, len(in.Events))
	rejected, missingID := 0, 0
	for _, event := range in.Events {
		if !validNginxEvidenceEvent(event, nowMS, retention) {
			rejected++
			continue
		}
		statuses, _ := json.Marshal(event.UpstreamStatuses)
		if event.OneAPIIDHMAC == "" {
			missingID++
		}
		acceptedRows = append(acceptedRows, NginxRequestEvidence{EventID: event.EventID, EventMS: event.EventMS, Node: in.Node, Route: event.Route, Method: event.Method,
			Status: event.Status, UpstreamStatus: event.UpstreamStatus, UpstreamAttempts: event.UpstreamAttempts, UpstreamStatuses: string(statuses), RequestMS: event.RequestMS,
			UpstreamMS: event.UpstreamMS, UpstreamPresent: event.UpstreamPresent, ConnectMS: event.ConnectMS, HeaderMS: event.HeaderMS,
			BytesSent: event.BytesSent, Completion: event.Completion,
			NginxIDHMAC: event.NginxIDHMAC, OneAPIIDHMAC: event.OneAPIIDHMAC, HMACKeyID: in.HMACKeyID, BatchID: in.BatchID, ReceivedAt: now})
	}
	duplicate := false
	err := m.nginxEvidenceDB.Transaction(func(tx *gorm.DB) error {
		batch := NginxEvidenceIngestBatch{Node: in.Node, SourceKind: in.Source.Kind, BatchID: in.BatchID, PayloadHash: computed,
			FirstEventMS: in.Source.FirstEventMS, LastEventMS: in.Source.LastEventMS, SourceFileID: in.Source.FileID,
			StartOffset: in.Source.StartOffset, EndOffset: in.Source.EndOffset,
			EventCount: len(in.Events), Accepted: len(acceptedRows), Rejected: rejected, ReceivedAt: now}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			var existing NginxEvidenceIngestBatch
			if err := tx.First(&existing, "node = ? AND source_kind = ? AND batch_id = ?", in.Node, in.Source.Kind, in.BatchID).Error; err != nil {
				return err
			}
			if existing.PayloadHash != computed {
				return errNginxEvidenceBatchConflict
			}
			duplicate = true
			acceptedRows = nil
			return nil
		}
		var previous NginxEvidenceSourceState
		stateErr := tx.First(&previous, "node = ? AND source_kind = ?", in.Node, in.Source.Kind).Error
		if stateErr != nil && !errors.Is(stateErr, gorm.ErrRecordNotFound) {
			return stateErr
		}
		if advancesRange && stateErr == nil && !continuousNginxEvidenceSource(previous, in.Source, in.Telemetry) {
			return errNginxEvidenceSourceConflict
		}
		if len(acceptedRows) > 0 {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(acceptedRows, 200).Error; err != nil {
				return err
			}
		}
		state := NginxEvidenceSourceState{Node: in.Node, SourceKind: in.Source.Kind, Mode: nginxEvidenceMode(m.cfg.NginxEvidenceMode), LogSchema: in.LogSchema, HMACKeyID: in.HMACKeyID,
			LastEventMS: in.Source.LastEventMS, LastIngestAt: now, Accepted: int64(len(acceptedRows)), Rejected: int64(rejected), MissingOneAPIID: int64(missingID),
			OutboxBytes: in.Telemetry.OutboxBytes, OutboxBatches: in.Telemetry.OutboxBatches, DroppedEvents: in.Telemetry.DroppedEvents,
			GapCount: in.Telemetry.GapCount, LastGapFromMS: in.Telemetry.LastGapFromMS, LastGapToMS: in.Telemetry.LastGapToMS,
			RejectedBytes: in.Telemetry.RejectedBytes, RejectedBatches: in.Telemetry.RejectedBatches, UnknownDroppedBatches: in.Telemetry.UnknownDroppedBatches}
		state.EvidenceEligible = in.Source.EvidenceEligible
		state.EvidenceParseRejected = in.Source.EvidenceParseRejected
		state.LastEvidenceParseRejectedAt = in.Source.LastEvidenceParseRejectedAt
		state.EvidencePersistFailures = in.Source.EvidencePersistFailures
		state.EvidenceDroppedEvents = in.Source.EvidenceDroppedEvents
		state.LastEvidencePersistFailureAt = in.Source.LastEvidencePersistFailureAt
		if advancesRange {
			state.LastFileID, state.LastEndOffset = in.Source.FileID, in.Source.EndOffset
			state.CursorDiscontinuities, state.DiscardedLines = in.Source.CursorDiscontinuities, in.Source.DiscardedLines
			state.AppliedGapCount = in.Telemetry.GapCount
			state.AppliedPersistFailures = in.Source.EvidencePersistFailures
		} else if stateErr == nil {
			state.LastFileID, state.LastEndOffset = previous.LastFileID, previous.LastEndOffset
			state.CursorDiscontinuities, state.DiscardedLines = previous.CursorDiscontinuities, previous.DiscardedLines
			state.AppliedGapCount = previous.AppliedGapCount
			state.AppliedPersistFailures = previous.AppliedPersistFailures
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}, {Name: "source_kind"}}, DoUpdates: clause.Assignments(map[string]any{
			"mode": state.Mode, "log_schema": state.LogSchema, "hmac_key_id": state.HMACKeyID, "last_event_ms": gorm.Expr("MAX(last_event_ms, excluded.last_event_ms)"),
			"last_ingest_at": now, "accepted": gorm.Expr("accepted + excluded.accepted"), "rejected": gorm.Expr("rejected + excluded.rejected"),
			"missing_one_api_id": gorm.Expr("missing_one_api_id + excluded.missing_one_api_id"), "outbox_bytes": state.OutboxBytes, "outbox_batches": state.OutboxBatches,
			"dropped_events": gorm.Expr("MAX(dropped_events, excluded.dropped_events)"), "gap_count": gorm.Expr("MAX(gap_count, excluded.gap_count)"),
			"last_gap_from_ms": gorm.Expr("MAX(last_gap_from_ms, excluded.last_gap_from_ms)"), "last_gap_to_ms": gorm.Expr("MAX(last_gap_to_ms, excluded.last_gap_to_ms)"),
			"last_file_id": state.LastFileID, "last_end_offset": state.LastEndOffset, "cursor_discontinuities": state.CursorDiscontinuities,
			"discarded_lines": state.DiscardedLines, "rejected_bytes": state.RejectedBytes, "rejected_batches": state.RejectedBatches,
			"unknown_dropped_batches":          gorm.Expr("MAX(unknown_dropped_batches, excluded.unknown_dropped_batches)"),
			"applied_gap_count":                state.AppliedGapCount,
			"evidence_persist_failures":        gorm.Expr("MAX(evidence_persist_failures, excluded.evidence_persist_failures)"),
			"evidence_dropped_events":          gorm.Expr("MAX(evidence_dropped_events, excluded.evidence_dropped_events)"),
			"last_evidence_persist_failure_at": gorm.Expr("MAX(last_evidence_persist_failure_at, excluded.last_evidence_persist_failure_at)"),
			"applied_persist_failures":         state.AppliedPersistFailures,
			"evidence_eligible":                gorm.Expr("MAX(evidence_eligible, excluded.evidence_eligible)"),
			"evidence_parse_rejected":          gorm.Expr("MAX(evidence_parse_rejected, excluded.evidence_parse_rejected)"),
			"last_evidence_parse_rejected_at":  gorm.Expr("MAX(last_evidence_parse_rejected_at, excluded.last_evidence_parse_rejected_at)"),
		})}).Create(&state).Error
	})
	if errors.Is(err, errNginxEvidenceBatchConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": "batch id conflict"})
		return
	}
	if errors.Is(err, errNginxEvidenceSourceConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": "source cursor discontinuity"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "evidence store unavailable"})
		return
	}
	accepted := len(acceptedRows)
	if duplicate {
		var existing NginxEvidenceIngestBatch
		if err := m.nginxEvidenceDB.First(&existing, "node = ? AND source_kind = ? AND batch_id = ?", in.Node, in.Source.Kind, in.BatchID).Error; err == nil {
			accepted, rejected = existing.Accepted, existing.Rejected
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "schema_version": nginxEvidenceSchemaVersion, "batch_id": in.BatchID, "payload_hash": computed, "accepted": accepted, "rejected": rejected, "duplicate": duplicate})
}
