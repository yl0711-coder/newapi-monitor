package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var nginxErrorCategories = map[string]bool{
	"upstream_timeout": true, "upstream_connect_failed": true, "upstream_closed": true,
	"upstream_tls": true, "client_closed": true, "worker_capacity": true, "resolver": true,
	"rate_limited": true, "request_body": true, "other_error": true,
}

var nginxErrorSeverities = map[string]bool{
	"emerg": true, "alert": true, "crit": true, "error": true, "warn": true, "notice": true,
}

type nginxErrorIngestSample struct {
	BucketTs int64  `json:"bucket_ts"`
	Category string `json:"category"`
	Severity string `json:"severity"`
	Count    int64  `json:"count"`
}

type nginxErrorIngestRequest struct {
	Node                      string                   `json:"node"`
	BatchID                   string                   `json:"batch_id"`
	Samples                   []nginxErrorIngestSample `json:"samples"`
	BacklogBytes              int64                    `json:"backlog_bytes"`
	BacklogKnown              bool                     `json:"backlog_known"`
	CursorDiscontinuities     int64                    `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt int64                    `json:"last_cursor_discontinuity_at"`
	DiscardedLines            int64                    `json:"discarded_lines"`
	LastDiscardedAt           int64                    `json:"last_discarded_at"`
	SourceBoundary            *nginxSourceBoundaryV1   `json:"source_boundary,omitempty"`
	SourceRangeV2             *nginxSourceRangeV2      `json:"source_range_v2,omitempty"`
}

type NginxErrorSummary struct {
	Category string `json:"category"`
	Severity string `json:"severity"`
	Count    int64  `json:"count"`
}

type NginxErrorSource struct {
	Node                      string   `json:"node"`
	LastEventTs               int64    `json:"last_event_ts"`
	LastIngestTs              int64    `json:"last_ingest_ts"`
	AgeSec                    int64    `json:"age_sec"`
	Status                    string   `json:"status"`
	HealthReasons             []string `json:"health_reasons,omitempty"`
	BacklogBytes              int64    `json:"backlog_bytes"`
	BacklogKnown              bool     `json:"backlog_known"`
	CursorDiscontinuities     int64    `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt int64    `json:"last_cursor_discontinuity_at"`
	DiscardedLines            int64    `json:"discarded_lines"`
	LastDiscardedAt           int64    `json:"last_discarded_at"`
}

func nginxErrorPayloadHash(rows []NginxErrorMinuteSample) string {
	canonical := append([]NginxErrorMinuteSample(nil), rows...)
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].BucketTs != canonical[j].BucketTs {
			return canonical[i].BucketTs < canonical[j].BucketTs
		}
		if canonical[i].Category != canonical[j].Category {
			return canonical[i].Category < canonical[j].Category
		}
		return canonical[i].Severity < canonical[j].Severity
	})
	payload, _ := json.Marshal(canonical)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func validateNginxErrorSample(raw nginxErrorIngestSample, now int64, retentionDays int) (NginxErrorMinuteSample, error) {
	bucket := raw.BucketTs / 60 * 60
	if bucket <= 0 || bucket < now-int64(retentionDays+1)*86400 || bucket > now+300 {
		return NginxErrorMinuteSample{}, errors.New("bucket_ts outside retention window")
	}
	category, severity := strings.TrimSpace(raw.Category), strings.TrimSpace(raw.Severity)
	if !nginxErrorCategories[category] || !nginxErrorSeverities[severity] || raw.Count <= 0 || raw.Count > 10_000_000 {
		return NginxErrorMinuteSample{}, errors.New("invalid finite error aggregate")
	}
	return NginxErrorMinuteSample{BucketTs: bucket, Category: category, Severity: severity, Count: raw.Count}, nil
}

func (m *Monitor) ingestNginxErrors(c *gin.Context) {
	if !m.cfg.NginxEnabled || !m.cfg.NginxErrorEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx error ingest disabled"})
		return
	}
	if !m.checkIngest(c) {
		return
	}
	var in nginxErrorIngestRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	in.Node = strings.TrimSpace(in.Node)
	if !nginxNodeNamePattern.MatchString(in.Node) || !m.nginxNodeAllowed(in.Node) || !safeBatchID(in.BatchID) || len(in.Samples) > 2000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source envelope"})
		return
	}
	if err := validateNginxSourceBoundaryV1(in.SourceBoundary); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if in.SourceBoundary != nil && in.SourceRangeV2 != nil || in.SourceRangeV2 != nil && !validNginxSourceRangeV2(*in.SourceRangeV2, "error") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid or ambiguous source range"})
		return
	}
	if in.SourceRangeV2 != nil && !m.nginxSourceV2LaneAllowed(in.Node, "error") {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx source v2 is not enabled for this node"})
		return
	}
	if in.SourceBoundary != nil && in.SourceBoundary.Checkpoint && len(in.Samples) != 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source checkpoint must not contain samples"})
		return
	}
	if in.SourceBoundary != nil && !m.nginxSourceV2SchemaReady.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx source continuity preparation is not enabled"})
		return
	}
	now := time.Now().Unix()
	if in.BacklogBytes < 0 || in.BacklogBytes > 1<<50 || (!in.BacklogKnown && in.BacklogBytes != 0) ||
		in.CursorDiscontinuities < 0 || in.LastCursorDiscontinuityAt < 0 || in.LastCursorDiscontinuityAt > now+300 ||
		(in.CursorDiscontinuities == 0) != (in.LastCursorDiscontinuityAt == 0) ||
		in.DiscardedLines < 0 || in.LastDiscardedAt < 0 || in.LastDiscardedAt > now+300 ||
		(in.DiscardedLines == 0) != (in.LastDiscardedAt == 0) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid collector telemetry"})
		return
	}
	merged := make(map[string]NginxErrorMinuteSample, len(in.Samples))
	var firstTs, lastTs, accepted int64
	for i, raw := range in.Samples {
		row, err := validateNginxErrorSample(raw, now, m.nginxRetentionDays())
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid sample %d: %v", i, err)})
			return
		}
		row.Node = in.Node
		key := fmt.Sprintf("%d\x00%s\x00%s", row.BucketTs, row.Category, row.Severity)
		if current, ok := merged[key]; ok {
			current.Count += row.Count
			merged[key] = current
		} else {
			merged[key] = row
		}
		if firstTs == 0 || row.BucketTs < firstTs {
			firstTs = row.BucketTs
		}
		if row.BucketTs > lastTs {
			lastTs = row.BucketTs
		}
		accepted += row.Count
	}
	rows := make([]NginxErrorMinuteSample, 0, len(merged))
	for _, row := range merged {
		rows = append(rows, row)
	}
	hash, duplicate := nginxErrorPayloadHash(rows), false
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		sourceDuplicate := false
		if in.SourceRangeV2 != nil {
			var err error
			sourceDuplicate, err = applyNginxSourceRangeV2(tx, in.Node, "error", in.BatchID, hash, *in.SourceRangeV2, now)
			if err != nil {
				return err
			}
		} else if m.cfg.NginxSourceV2Enabled || m.nginxSourceV2SchemaReady.Load() {
			allowed, err := legacyNginxSourceAllowed(tx, in.Node, "error")
			if err != nil {
				return err
			}
			if !allowed {
				return errNginxLegacyAfterV2
			}
		}
		batch := NginxErrorIngestBatch{Node: in.Node, BatchID: in.BatchID, PayloadHash: hash, FirstTs: firstTs, LastTs: lastTs, Rows: len(rows), ReceivedAt: now}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			var existing NginxErrorIngestBatch
			if err := tx.First(&existing, "node = ? AND batch_id = ?", in.Node, in.BatchID).Error; err != nil {
				return err
			}
			if existing.PayloadHash != "" && existing.PayloadHash != hash {
				return errNginxBatchConflict
			}
			if in.SourceRangeV2 != nil && !sourceDuplicate {
				return errNginxSourceConflict
			}
			if in.SourceBoundary != nil {
				if existing.PayloadHash == "" {
					return errNginxBatchConflict
				}
				if err := applyNginxSourceBoundaryV1(tx, in.Node, "error", in.BatchID, hash, *in.SourceBoundary, true, now); err != nil {
					return err
				}
			}
			duplicate = true
			return nil
		}
		if sourceDuplicate {
			return errNginxSourceConflict
		}
		if in.SourceBoundary != nil {
			if err := applyNginxSourceBoundaryV1(tx, in.Node, "error", in.BatchID, hash, *in.SourceBoundary, false, now); err != nil {
				return err
			}
		}
		if len(rows) > 0 {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "bucket_ts"}, {Name: "node"}, {Name: "category"}, {Name: "severity"}}, DoUpdates: clause.Assignments(map[string]any{"count": gorm.Expr("count + excluded.count")})}).CreateInBatches(rows, 200).Error; err != nil {
				return err
			}
		}
		state := NginxErrorSourceState{Node: in.Node, LastEventTs: lastTs, LastIngestTs: now, LastBatchID: in.BatchID, AcceptedRows: int64(len(rows)), AcceptedCount: accepted, BacklogBytes: in.BacklogBytes, BacklogKnown: in.BacklogKnown, CursorDiscontinuities: in.CursorDiscontinuities, LastCursorDiscontinuityAt: in.LastCursorDiscontinuityAt, DiscardedLines: in.DiscardedLines, LastDiscardedAt: in.LastDiscardedAt}
		updates := map[string]any{
			"last_event_ts": gorm.Expr("MAX(last_event_ts, excluded.last_event_ts)"), "last_ingest_ts": now, "last_batch_id": in.BatchID,
			"accepted_rows": gorm.Expr("accepted_rows + excluded.accepted_rows"), "accepted_count": gorm.Expr("accepted_count + excluded.accepted_count"),
			"backlog_bytes": in.BacklogBytes, "backlog_known": in.BacklogKnown,
			"cursor_discontinuities": gorm.Expr("MAX(cursor_discontinuities, excluded.cursor_discontinuities)"), "last_cursor_discontinuity_at": gorm.Expr("MAX(last_cursor_discontinuity_at, excluded.last_cursor_discontinuity_at)"),
			"discarded_lines": gorm.Expr("MAX(discarded_lines, excluded.discarded_lines)"), "last_discarded_at": gorm.Expr("MAX(last_discarded_at, excluded.last_discarded_at)"),
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}}, DoUpdates: clause.Assignments(updates)}).Create(&state).Error
	})
	if errors.Is(err, errNginxLegacyAfterV2) {
		c.JSON(http.StatusConflict, gin.H{"error": "collector protocol v2 is active; legacy ingest is closed"})
		return
	}
	if errors.Is(err, errNginxBatchConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": "batch id conflict"})
		return
	}
	if errors.Is(err, errNginxSourceConflict) || errors.Is(err, errNginxSourceGap) || errors.Is(err, errNginxSourceOverlap) || errors.Is(err, errNginxSourceEpoch) || errors.Is(err, errNginxSourceUnregistered) {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
		return
	}
	response := gin.H{"ok": true, "duplicate": duplicate, "stored": len(rows)}
	if ack := nginxSourceBoundaryAckForV1(in.SourceBoundary, in.BatchID); ack != nil {
		response["source_boundary_ack"] = ack
	}
	if ack := nginxSourceCommitAckForV2(in.SourceRangeV2, in.BatchID); ack != nil {
		response["source_ack_v2"] = ack
	}
	c.JSON(http.StatusOK, response)
}

func (m *Monitor) nginxErrorSummary(ctx context.Context, from, to int64) []NginxErrorSummary {
	var rows []NginxErrorSummary
	warnReadErr("nginx error summary", m.storeDB.WithContext(ctx).Raw(`SELECT category, severity, COALESCE(SUM(count),0) count FROM nginx_error_minute_samples WHERE bucket_ts >= ? AND bucket_ts < ? GROUP BY category, severity ORDER BY count DESC`, from, to).Scan(&rows))
	return rows
}

func (m *Monitor) nginxErrorSources(ctx context.Context, now int64) []NginxErrorSource {
	var states []NginxErrorSourceState
	warnReadErr("nginx error sources", m.storeDB.WithContext(ctx).Where("node IN ?", m.cfg.NginxAllowedNodes).Find(&states))
	byNode := make(map[string]NginxErrorSourceState, len(states))
	for _, state := range states {
		byNode[state.Node] = state
	}
	out := make([]NginxErrorSource, 0, len(m.cfg.NginxAllowedNodes))
	for _, node := range m.cfg.NginxAllowedNodes {
		state, ok := byNode[node]
		if !ok {
			out = append(out, NginxErrorSource{Node: node, AgeSec: -1, Status: "bad", HealthReasons: []string{"source_missing"}})
			continue
		}
		age, status := now-state.LastIngestTs, "ok"
		if age < 0 {
			age = 0
		}
		reasons := []string{}
		if !state.BacklogKnown {
			status = "warn"
			reasons = append(reasons, "log_or_backlog_unreadable")
		}
		if age > 180 {
			status = "warn"
			reasons = append(reasons, "heartbeat_stale")
		}
		if age > 900 {
			status = "bad"
		}
		if state.BacklogKnown && state.BacklogBytes >= nginxBacklogWarnBytes {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "backlog_large")
		}
		if state.LastCursorDiscontinuityAt > 0 && now-state.LastCursorDiscontinuityAt <= nginxRecentDataLossWindowSec {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "recent_cursor_discontinuity")
		}
		if state.LastDiscardedAt > 0 && now-state.LastDiscardedAt <= nginxRecentDataLossWindowSec {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "recent_discarded_lines")
		}
		out = append(out, NginxErrorSource{Node: node, LastEventTs: state.LastEventTs, LastIngestTs: state.LastIngestTs, AgeSec: age, Status: status, HealthReasons: reasons, BacklogBytes: state.BacklogBytes, BacklogKnown: state.BacklogKnown, CursorDiscontinuities: state.CursorDiscontinuities, LastCursorDiscontinuityAt: state.LastCursorDiscontinuityAt, DiscardedLines: state.DiscardedLines, LastDiscardedAt: state.LastDiscardedAt})
	}
	return out
}
