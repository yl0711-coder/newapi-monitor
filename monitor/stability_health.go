package monitor

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type stabilityHealthResponse struct {
	Status                     string                            `json:"status"`
	CheckedAt                  int64                             `json:"checked_at"`
	MainSamplerLastSuccess     int64                             `json:"main_sampler_last_success"`
	MainSamplerAgeSec          int64                             `json:"main_sampler_age_sec"`
	ProblemSamplerLastSuccess  int64                             `json:"problem_sampler_last_success"`
	ProblemSamplerAgeSec       int64                             `json:"problem_sampler_age_sec"`
	ProblemSamplerLastFailure  int64                             `json:"problem_sampler_last_failure"`
	ProblemCoverageTo          int64                             `json:"problem_coverage_to"`
	ProblemLiveTargetTo        int64                             `json:"problem_live_target_to"`
	ProblemLiveLagSec          int64                             `json:"problem_live_lag_sec"`
	ProblemLiveStatus          string                            `json:"problem_live_status"`
	ProblemPendingMinutes      int64                             `json:"problem_pending_minutes"`
	ProblemMigration           stabilityProblemMigrationProgress `json:"problem_migration"`
	StoreReachable             bool                              `json:"store_reachable"`
	StoreBytes                 int64                             `json:"store_bytes"`
	NginxEnabled               bool                              `json:"nginx_enabled"`
	NginxSourceCount           int                               `json:"nginx_source_count"`
	NginxUnhealthySources      int                               `json:"nginx_unhealthy_sources"`
	NginxBacklogBytes          int64                             `json:"nginx_backlog_bytes"`
	NginxBacklogUnknown        int                               `json:"nginx_backlog_unknown"`
	NginxLaggingSources        int                               `json:"nginx_lagging_sources"`
	NginxLargeBacklogSources   int                               `json:"nginx_large_backlog_sources"`
	NginxCursorDiscontinuities int64                             `json:"nginx_cursor_discontinuities"`
	NginxDiscardedLines        int64                             `json:"nginx_discarded_lines"`
	NginxRecentDataLossSources int                               `json:"nginx_recent_data_loss_sources"`
}

// serveStabilityHealth 只检查 Monitor 自身和本地采集状态，不主动查询 NewAPI 生产库，
// 因而可以被运维页面频繁查看而不给主站增加负担。
func (m *Monitor) serveStabilityHealth(c *gin.Context) {
	now := time.Now().Unix()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	result := stabilityHealthResponse{
		Status: "ok", CheckedAt: now, MainSamplerLastSuccess: m.lastRun.Load(),
		ProblemSamplerLastSuccess: m.problemLastSuccess.Load(), ProblemSamplerLastFailure: m.problemLastFailure.Load(),
		ProblemMigration: m.stabilityProblemMigrationProgress(), NginxEnabled: m.cfg.NginxEnabled,
	}
	var liveCursor StabilityProblemLiveCursor
	if err := m.storeDB.WithContext(ctx).First(&liveCursor, "id = ? AND traffic_class_version = ?", 1, userTrafficClassificationVersion).Error; err == nil {
		result.ProblemCoverageTo = liveCursor.NextTs
		result.ProblemLiveTargetTo = liveCursor.TargetThroughTs
		result.ProblemLiveStatus = liveCursor.Status
		result.ProblemLiveLagSec = max(liveCursor.TargetThroughTs-liveCursor.NextTs, int64(0))
		if result.ProblemSamplerLastSuccess == 0 && liveCursor.LastSuccessAt > 0 {
			result.ProblemSamplerLastSuccess = liveCursor.LastSuccessAt
		}
		if result.ProblemSamplerLastFailure == 0 && liveCursor.LastFailureAt > 0 {
			result.ProblemSamplerLastFailure = liveCursor.LastFailureAt
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		result.Status = "degraded"
	}
	if result.MainSamplerLastSuccess > 0 {
		result.MainSamplerAgeSec = now - result.MainSamplerLastSuccess
		if result.MainSamplerAgeSec < 0 {
			result.MainSamplerAgeSec = 0
		}
	}
	if result.ProblemSamplerLastSuccess > 0 {
		result.ProblemSamplerAgeSec = now - result.ProblemSamplerLastSuccess
		if result.ProblemSamplerAgeSec < 0 {
			result.ProblemSamplerAgeSec = 0
		}
	}
	if sqlDB, err := m.storeDB.DB(); err == nil && sqlDB.PingContext(ctx) == nil {
		result.StoreReachable = true
	} else {
		result.Status = "degraded"
	}
	problemTargetTo := (now - stabilityProblemFinalizeDelaySec) / 60 * 60
	problemEvery := stabilityProblemIntervalSeconds(m.cfg.StabilityProblemSampleSec)
	problemLiveFrom := problemTargetTo - 2*problemEvery - 120
	if result.ProblemCoverageTo == 0 {
		result.ProblemCoverageTo = m.problemLiveThrough.Load()
	}
	var pending int64
	if tx := m.storeDB.WithContext(ctx).Model(&StabilityProblemIngestState{}).
		Where("bucket_ts >= ? AND bucket_ts < ? AND complete = ? AND traffic_class_version = ?",
			problemLiveFrom, problemTargetTo, false, userTrafficClassificationVersion).Count(&pending); tx.Error == nil {
		result.ProblemPendingMinutes = pending
	} else {
		result.Status = "degraded"
	}
	// SQLite WAL 模式的实际占用包含主文件、-wal 与 -shm，运维状态不能只报主文件。
	for _, path := range []string{m.cfg.StorePath, m.cfg.StorePath + "-wal", m.cfg.StorePath + "-shm"} {
		if info, err := os.Stat(path); err == nil {
			result.StoreBytes += info.Size()
		}
	}
	maxSamplerAge := int64(m.cfg.SampleSeconds * 3)
	if maxSamplerAge < 180 {
		maxSamplerAge = 180
	}
	if result.MainSamplerLastSuccess == 0 || result.MainSamplerAgeSec > maxSamplerAge ||
		result.ProblemPendingMinutes > 0 || result.ProblemSamplerLastFailure > result.ProblemSamplerLastSuccess {
		result.Status = "degraded"
	}
	if m.cfg.StabilityEnabled {
		maxProblemAge := stabilityProblemIntervalSeconds(m.cfg.StabilityProblemSampleSec)*3 + 120
		if result.ProblemSamplerLastSuccess == 0 || result.ProblemCoverageTo == 0 || result.ProblemSamplerAgeSec > maxProblemAge ||
			result.ProblemLiveLagSec > maxProblemAge {
			result.Status = "degraded"
		}
	}
	if result.ProblemMigration.Status != "complete" && result.ProblemMigration.Status != "not_required" &&
		result.ProblemMigration.Status != "disabled" {
		// Historical migration health is visible without overwriting the live
		// watermark/age fields. Overall status remains honest until both domains
		// are signed, while operators can see that recent Tail is still fresh.
		result.Status = "degraded"
	}
	if m.cfg.NginxEnabled {
		sources := m.nginxSources(ctx, now)
		result.NginxSourceCount = len(sources)
		for _, source := range sources {
			if source.Status != "ok" {
				result.NginxUnhealthySources++
			}
			if source.BacklogKnown {
				result.NginxBacklogBytes += source.BacklogBytes
			} else {
				result.NginxBacklogUnknown++
			}
			result.NginxCursorDiscontinuities += source.CursorDiscontinuities
			result.NginxDiscardedLines += source.DiscardedLines
			recentDataLoss := false
			for _, reason := range source.HealthReasons {
				switch reason {
				case "event_lag_with_backlog":
					result.NginxLaggingSources++
				case "backlog_large":
					result.NginxLargeBacklogSources++
				case "recent_cursor_discontinuity", "recent_discarded_lines":
					recentDataLoss = true
				}
			}
			if recentDataLoss {
				result.NginxRecentDataLossSources++
			}
		}
		if len(sources) != len(m.cfg.NginxAllowedNodes) || result.NginxUnhealthySources > 0 {
			result.Status = "degraded"
		}
	}
	c.JSON(200, result)
}
