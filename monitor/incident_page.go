package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// A cursor over mutable incidents is only safe while its ordered set is unchanged.
// The page opts into this fence; legacy B3 consumers retain their existing contract.
// Hash every selected row in a short local read transaction, with bounded memory.
func incidentListFingerprint(q *gorm.DB) (string, error) {
	h := sha256.New()
	encoder := json.NewEncoder(h)
	if err := incidentEncodeRows[Incident](q.Order("last_seen_ts DESC, id ASC"), encoder); err != nil {
		return "", err
	}
	// Pending status can change independently of Incident.Revision. Hash its
	// inputs in two bounded-memory scans instead of issuing a query per incident.
	db := q.Session(&gorm.Session{NewDB: true})
	ids := q.Session(&gorm.Session{}).Select("id")
	if err := incidentEncodeRows[IncidentPendingSummary](db.Model(&IncidentPendingSummary{}).
		Where("incident_id IN (?)", ids).Order("incident_id ASC"), encoder); err != nil {
		return "", err
	}
	if err := incidentEncodeRows[IncidentIngestWindowState](db.Model(&IncidentIngestWindowState{}).
		Where("incident_id IN (?) AND scenario_pending = ?", ids, true).
		Order("incident_id, window_to_ts, dedup_key"), encoder); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func incidentEncodeRows[T any](q *gorm.DB, encoder *json.Encoder) error {
	rows, err := q.Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var row T
		if err := q.ScanRows(rows, &row); err != nil {
			return err
		}
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Pending state is independent of the incident revision. Fence the displayed
// list item as well, so opening detail cannot silently change its conclusion.
func (m *Monitor) incidentPageItem(ctx context.Context, inc Incident) (gin.H, error) {
	item := m.incidentListItem(ctx, inc)
	data, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	item["row_snapshot"] = fmt.Sprintf("%x", sha256.Sum256(data))
	return item, nil
}

func (m *Monitor) incidentEvidenceFingerprint(ctx context.Context, inc Incident) (string, error) {
	h := sha256.New()
	e := json.NewEncoder(h)
	if err := e.Encode(inc); err != nil {
		return "", err
	}
	db := m.storeDB.WithContext(ctx)
	queries := []func() error{
		func() error {
			return incidentEncodeRows[IncidentSegment](db.Model(&IncidentSegment{}).Where("incident_id = ?", inc.ID).Order("id"), e)
		},
		func() error {
			return incidentEncodeRows[IncidentObservation](db.Model(&IncidentObservation{}).Where("incident_id = ?", inc.ID).Order("id"), e)
		},
		func() error {
			return incidentEncodeRows[IncidentImpact](db.Model(&IncidentImpact{}).Where("incident_id = ?", inc.ID).Order("id"), e)
		},
		func() error {
			return incidentEncodeRows[IncidentImpactWindow](db.Model(&IncidentImpactWindow{}).Where("incident_id = ? AND row_kind = ?", inc.ID, incidentImpactRowKindArchive).Order("id"), e)
		},
		func() error {
			return incidentEncodeRows[IncidentIngestRevision](db.Model(&IncidentIngestRevision{}).Where("incident_id = ?", inc.ID).Order("id"), e)
		},
	}
	for _, query := range queries {
		if err := query(); err != nil {
			return "", err
		}
	}
	pending, reason, err := m.incidentScenarioPendingByID(ctx, inc.ID)
	if err != nil {
		return "", err
	}
	if err := e.Encode([]any{pending, reason}); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Window processing proof is independent from scan progress and response time.
// Empty periods have explicit no_sample rows in B3. Missing retained windows,
// unfinished keys and archived gaps therefore never become an empty healthy report.
func (m *Monitor) incidentPageCoverage(ctx context.Context, from, to int64, cfg IncidentConfig, now int64) (gin.H, error) {
	_, ingest, active := incidentIngestActive()
	cursor, err := m.loadIncidentIngestCursor(ctx)
	if err != nil {
		return nil, err
	}
	result := gin.H{"complete": false, "status": "unknown", "ingest_running": active,
		"actual_through_ts": int64(0), "scan_through_ts": cursor.ScanThroughTs,
		"global_complete_through_ts": cursor.CompleteThroughTs,
		"finalize_lag_seconds":       ingest.FinalizeLagSeconds, "poll_seconds": ingest.PollSeconds,
		"lag_seconds": nil, "from_ts": from, "to_ts": to}
	target := AlignIncidentWindow(incidentFinalizeFrontier(ingest, now), int64(cfg.WindowSeconds)).FromTs
	result["target_ts"] = target
	if !active {
		result["status"] = "disabled"
		return result, nil
	}
	if from <= 0 || to <= from {
		return result, nil
	}
	rangeTarget := max(from, min(to, target))
	result["range_target_ts"] = rangeTarget
	var windows []struct {
		FromTs int64
		ToTs   int64
		Bad    int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Select("window_from_ts AS from_ts, window_to_ts AS to_ts, SUM(CASE WHEN data_complete = 0 OR process_state <> ? THEN 1 ELSE 0 END) AS bad", incidentProcessDone).
		Where("window_to_ts > ? AND window_from_ts < ? AND window_to_ts <= ?", from, to, target).
		Group("window_from_ts, window_to_ts").Order("window_from_ts, window_to_ts").Scan(&windows).Error; err != nil {
		return nil, err
	}
	through := from
	for _, w := range windows {
		if w.FromTs > through || w.Bad > 0 {
			break
		}
		through = max(through, w.ToTs)
	}
	var gaps struct {
		Count int64
		First int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestGapSummary{}).
		Select("COUNT(*) AS count, COALESCE(MIN(gap_from_ts),0) AS first").
		Where("gap_through_ts > ? AND gap_from_ts < ?", from, to).Scan(&gaps).Error; err != nil {
		return nil, err
	}
	if gaps.Count > 0 {
		through = min(through, max(from, gaps.First))
	}
	through = min(through, rangeTarget)
	// The left edge is a query boundary, not evidence of processing. Report
	// no actual cutoff until at least one intersecting interval is proved.
	actual := int64(0)
	if through > from {
		actual = through
	}
	result["actual_through_ts"] = actual
	result["range_complete_through_ts"] = actual
	result["lag_seconds"] = rangeTarget - through
	complete := to <= target && through >= to && gaps.Count == 0
	result["complete"] = complete
	result["status"] = "incomplete"
	if complete {
		result["status"] = "complete"
	} else if to > target && through >= rangeTarget && gaps.Count == 0 {
		result["status"] = "finalizing"
	}
	return result, nil
}

func (m *Monitor) serveIncidentPageList(ctx context.Context, c *gin.Context, cfg IncidentConfig, from, to int64, limit int) {
	var response gin.H
	changed := false
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		reader := &Monitor{storeDB: tx}
		q := tx.Model(&Incident{}).Where("last_seen_ts >= ? AND first_seen_ts < ?", from, to)
		fingerprint, err := incidentListFingerprint(q.Session(&gorm.Session{}))
		if err != nil {
			return err
		}
		if expected := c.Query("snapshot"); expected != "" && expected != fingerprint {
			changed = true
			return nil
		}
		ts, id, err := incidentParseListCursor(c.Query("cursor"))
		if err != nil {
			return err
		}
		if ts > 0 {
			q = q.Where("last_seen_ts < ? OR (last_seen_ts = ? AND id > ?)", ts, ts, id)
		}
		var rows []Incident
		if err := q.Order("last_seen_ts DESC, id ASC").Limit(limit + 1).Find(&rows).Error; err != nil {
			return err
		}
		more := len(rows) > limit
		if more {
			rows = rows[:limit]
		}
		next := ""
		if more {
			r := rows[len(rows)-1]
			next = fmt.Sprintf("%d:%s", r.LastSeenTs, r.ID)
		}
		out := make([]gin.H, 0, len(rows))
		for _, row := range rows {
			item, err := reader.incidentPageItem(ctx, row)
			if err != nil {
				return err
			}
			out = append(out, item)
		}
		now := time.Now().Unix()
		coverage, err := reader.incidentPageCoverage(ctx, from, to, cfg, now)
		if err != nil {
			return err
		}
		response = gin.H{"enabled": true, "incidents": out, "has_more": more, "next_cursor": next,
			"snapshot": fingerprint, "limit": limit, "from_ts": from, "to_ts": to, "generated_at": now,
			"coverage": coverage, "anomaly_rate_caveat": incidentAnomalyRateCaveat}
		return nil
	})
	if err != nil {
		incidentRespondQueryFailure(c, "list_snapshot", err)
		return
	}
	if changed {
		c.JSON(409, gin.H{"error": "事故状态已更新，分页快照已变化；请重新查询后继续，当前结果保留。", "data_status": "snapshot_changed"})
		return
	}
	c.JSON(200, response)
}
