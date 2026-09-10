package monitor

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

type ecsArchiveHealth struct {
	Status          string `json:"status"`
	LastScan        int64  `json:"last_scan"`
	LastProgress    int64  `json:"last_progress"`
	LastFailure     int64  `json:"last_failure"`
	LastError       string `json:"last_error,omitempty"`
	Accepted        int64  `json:"accepted_batches"`
	Closures        int64  `json:"collector_closures"`
	FinalBoundaries int64  `json:"verified_final_boundaries"`
	Pending         int64  `json:"pending_batches"`
	Conflicts       int64  `json:"conflict_batches"`
	Rejected        int64  `json:"rejected_batches"`
	CoverageStatus  string `json:"coverage_status"`
}

// Recovered archive objects are NOT a final producer file manifest. Neither
// successful scans nor stopped tasks can publish complete stability coverage.
func (m *Monitor) ecsArchiveHealth(ctx context.Context, now int64) *ecsArchiveHealth {
	if !m.cfg.ECSArchiveEnabled {
		return nil
	}
	h := &ecsArchiveHealth{Status: "unavailable", CoverageStatus: "awaiting_final_boundary"}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var scan ECSLogArchiveScan
	err := m.storeDB.WithContext(ctx).First(&scan, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		h.Status = "awaiting_first_scan"
		return h
	}
	if err != nil {
		return h
	}
	h.LastScan = scan.LastSuccess
	h.LastProgress, h.LastFailure, h.LastError = scan.LastProgress, scan.LastFailure, scan.LastError
	var counts []struct {
		Status                int
		Kind                  string
		FinalBoundaryVerified bool
		Count                 int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&ECSLogArchiveReceipt{}).Select("status,kind,final_boundary_verified,COUNT(*) AS count").Group("status,kind,final_boundary_verified").Scan(&counts).Error; err != nil {
		return h
	}
	for _, c := range counts {
		switch c.Status {
		case 200:
			if c.Kind == ecsarchive.CollectorClosure {
				h.Closures += c.Count
				if c.FinalBoundaryVerified {
					h.FinalBoundaries += c.Count
				}
			} else {
				h.Accepted += c.Count
			}
		case 409:
			h.Conflicts += c.Count
		case 400, 403, 410, 422:
			h.Rejected += c.Count
		default:
			h.Pending += c.Count
		}
	}
	h.Status = "scanned"
	if scan.PageJSON != "" || scan.NextToken != "" {
		h.Status = "scan_progressing"
	}
	if max(scan.LastSuccess, scan.LastProgress) == 0 || now-max(scan.LastSuccess, scan.LastProgress) > int64(3*ecsArchivePollInterval/time.Second) {
		h.Status = "scan_stale"
	}
	if h.Pending > 0 || h.Conflicts > 0 || h.Rejected > 0 {
		h.Status = "recovery_pending"
	}
	if scan.LastError != "" {
		h.Status = "scan_failed" // Pending batches must not mask the scanner outage.
	}
	return h
}

func (m *Monitor) attachECSArchiveHealth(ctx context.Context, h *ecsLogHealth, now int64) {
	h.Archive = m.ecsArchiveHealth(ctx, now)
	if h.Archive != nil && h.Archive.Status != "scanned" && h.Archive.Status != "scan_progressing" {
		h.Status = "degraded"
	}
}
