package monitor

import (
	"context"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

type stabilityHealthResponse struct {
	Status                    string `json:"status"`
	CheckedAt                 int64  `json:"checked_at"`
	MainSamplerLastSuccess    int64  `json:"main_sampler_last_success"`
	MainSamplerAgeSec         int64  `json:"main_sampler_age_sec"`
	ProblemSamplerLastSuccess int64  `json:"problem_sampler_last_success"`
	ProblemSamplerAgeSec      int64  `json:"problem_sampler_age_sec"`
	ProblemSamplerLastFailure int64  `json:"problem_sampler_last_failure"`
	ProblemCoverageTo         int64  `json:"problem_coverage_to"`
	ProblemPendingMinutes     int64  `json:"problem_pending_minutes"`
	StoreReachable            bool   `json:"store_reachable"`
	StoreBytes                int64  `json:"store_bytes"`
}

// serveStabilityHealth 只检查 Monitor 自身和本地采集状态，不主动查询 NewAPI 生产库，
// 因而可以被运维页面频繁查看而不给主站增加负担。
func (m *Monitor) serveStabilityHealth(c *gin.Context) {
	now := time.Now().Unix()
	result := stabilityHealthResponse{
		Status: "ok", CheckedAt: now, MainSamplerLastSuccess: m.lastRun.Load(),
		ProblemSamplerLastSuccess: m.problemLastSuccess.Load(), ProblemSamplerLastFailure: m.problemLastFailure.Load(),
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
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if sqlDB, err := m.storeDB.DB(); err == nil && sqlDB.PingContext(ctx) == nil {
		result.StoreReachable = true
	} else {
		result.Status = "degraded"
	}
	var coverage struct {
		MaxComplete int64
		Pending     int64
	}
	if tx := m.storeDB.WithContext(ctx).Raw(`SELECT
		COALESCE(MAX(CASE WHEN complete THEN bucket_ts END),0) max_complete,
		COALESCE(SUM(CASE WHEN complete THEN 0 ELSE 1 END),0) pending
		FROM stability_problem_ingest_states`).Scan(&coverage); tx.Error == nil {
		if coverage.MaxComplete > 0 {
			result.ProblemCoverageTo = coverage.MaxComplete + 60
		}
		result.ProblemPendingMinutes = coverage.Pending
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
		if result.ProblemSamplerLastSuccess == 0 || result.ProblemCoverageTo == 0 || result.ProblemSamplerAgeSec > maxProblemAge {
			result.Status = "degraded"
		}
	}
	c.JSON(200, result)
}
