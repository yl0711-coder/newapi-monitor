package monitor

import (
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type infraAssetView struct {
	InfraAsset
	Status     string `json:"status"`
	CanRestore bool   `json:"can_restore"`
	CanRemove  bool   `json:"can_remove"`
}

func (m *Monitor) assetView(a InfraAsset, now int64) infraAssetView {
	v := infraAssetView{InfraAsset: a}
	v.CanRestore = m.assetRecovery(a, now)
	v.CanRemove = a.State == "archived" && !v.CanRestore
	fresh := m.infraMetricFreshnessSec()
	switch {
	case a.CloudState == "stopped" && now-a.LastDiscovered <= fresh:
		v.Status = "stopped"
	case a.LastReport > 0 && a.LastReport <= now && now-a.LastReport <= fresh:
		v.Status = "sample_unconfirmed"
		if a.LastSampleAt > 0 && a.LastSampleAt <= now && now-a.LastSampleAt <= fresh {
			v.Status = "reporting"
		}
	case a.LastChecked > 0 && now-a.LastChecked > fresh && a.Kind != "host":
		v.Status = "discovery_stale"
	case a.CloudState == "missing":
		v.Status = "missing"
	case a.Platform == "ECS/Fargate" && a.CloudState == "running" && now-a.LastLive <= fresh:
		v.Status = "cloud_running"
	case a.Platform == "ECS/Fargate" && a.CloudState == "idle" && now-a.LastDiscovered <= fresh:
		v.Status = "cloud_idle"
	case a.LastReport > 0:
		v.Status = "report_lost"
	case a.LastDiscovered > 0 && now-a.LastDiscovered > fresh:
		v.Status = "discovery_stale"
	default:
		v.Status = "awaiting_report"
	}
	return v
}

func (m *Monitor) serveInfraAssets(c *gin.Context) {
	activeAfter, archivedAfter := c.Query("active_after"), c.Query("archived_after")
	for _, cursor := range []string{activeAfter, archivedAfter} {
		if cursor == "" {
			continue
		}
		if _, err := hex.DecodeString(cursor); err != nil || len(cursor) != 64 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "资源分页游标无效"})
			return
		}
	}
	page, err := m.infraAssetPage(c.Request.Context(), activeAfter, archivedAfter)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "资源目录暂不可用，不能据此判断资源已下线"})
		return
	}
	active, archived := []infraAssetView{}, []infraAssetView{}
	for _, a := range append(page.Active, page.Archived...) {
		if m.infraExcluded(a.Resource) || a.State == "removed" || a.State == "linked" {
			continue
		}
		v := m.assetView(a, time.Now().Unix())
		if a.State == "archived" {
			archived = append(archived, v)
		} else {
			active = append(active, v)
		}
	}
	c.JSON(http.StatusOK, gin.H{"active": active, "archived": archived, "active_next": page.ActiveNext, "archived_next": page.ArchivedNext, "page_size": infraAssetPageSize, "freshness_seconds": m.infraMetricFreshnessSec()})
}

func (m *Monitor) serveInfraAssetAction(c *gin.Context) {
	// Only JSON same-origin requests; Lax cookies alone do not protect against
	// same-site sibling origins. Never trust client-provided forwarded hosts.
	u, err := url.Parse(c.GetHeader("Origin"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || !strings.EqualFold(u.Host, c.Request.Host) || u.User != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "需要同源页面操作"})
		return
	}
	if c.ContentType() != "application/json" {
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"error": "需要 JSON 请求"})
		return
	}
	var in struct {
		ID       string `json:"id"`
		Action   string `json:"action"`
		Revision int64  `json:"revision"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2048)
	if c.ShouldBindJSON(&in) != nil || len(in.ID) != 64 || in.Revision < 1 || (in.Action != "archive" && in.Action != "restore" && in.Action != "remove") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "资源操作参数无效"})
		return
	}
	err = m.changeInfraAsset(c.Request.Context(), in.ID, in.Action, c.GetString("uname"), in.Revision, time.Now().Unix())
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "资源不存在"})
	case errors.Is(err, errInfraAssetConflict):
		c.JSON(http.StatusConflict, gin.H{"error": "资源状态已变化，或尚未收到归档后的有效实时更新，请刷新后再操作"})
	case err != nil:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "资源操作未完成，请刷新核对后重试"})
	default:
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}
