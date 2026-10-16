package monitor

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/usagebill/exportjobs"
)

// Registration is startup-only. Failure disables bills, not the whole Portal,
// and never falls back to a weaker synchronous generation mode.
func (m *Monitor) registerPortalBillTasks(r *gin.Engine) {
	if !m.cfg.PortalBillTasksEnabled {
		return
	}
	s, err := m.initPortalBillTasks()
	if err == nil {
		s.mount(r)
		return
	}
	slog.Warn("客户后台账单初始化失败，仅暂停账单导出", "err", err)
	api := r.Group("/api/logs/bill/tasks", m.requirePortal(true))
	unavailable := func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("X-Content-Type-Options", "nosniff")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "账单任务服务暂不可用，请稍后重试"})
	}
	api.POST("", unavailable)
	api.GET("/:id", unavailable)
	api.POST("/:id/cancel", unavailable)
	api.GET("/:id/download", unavailable)
}

// Explicit opt-in at Portal registration. Monitor owns shutdown ordering once
// this returns successfully. Repeated initialization reuses one worker.
func (m *Monitor) initPortalBillTasks() (*portalBillTasks, error) {
	m.portalBillMu.Lock()
	defer m.portalBillMu.Unlock()
	if m.shuttingDown.Load() {
		return nil, exportjobs.ErrClosed
	}
	if m.portalBillService != nil {
		select {
		case <-m.portalBillService.stop:
			return nil, exportjobs.ErrClosed
		default:
		}
		return m.portalBillService, nil
	}
	parent, err := m.portalExportTempDir()
	if err != nil {
		return nil, err
	}
	// Never follow a pre-existing symlink or chmod someone else's directory.
	for _, dir := range []string{parent, filepath.Join(parent, "bill-tasks-v1")} {
		if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
			return nil, err
		}
		info, err := os.Lstat(dir)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return nil, exportjobs.ErrInvalid
		}
	}
	s, err := newPortalBillTasks(m, filepath.Join(parent, "bill-tasks-v1"))
	if err != nil {
		return nil, err
	}
	m.portalBillService = s
	return s, nil
}

func (s *portalBillTasks) maintain(period time.Duration) {
	defer close(s.maintenanceDone)
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			// Current sessions and open downloads retain a lifetime lock. This
			// sweep only removes recognized sessions whose owner has exited.
			if _, err := exportjobs.Recover(s.base); err != nil && !errors.Is(err, exportjobs.ErrBusy) {
				slog.Warn("客户账单旧任务目录回收暂不可用", "err", err)
			}
		}
	}
}

func (s *portalBillTasks) beginClose() {
	s.closeOnce.Do(func() {
		close(s.stop)
		go func() {
			// Cancel generation immediately, rather than waiting for recovery.
			s.closeErr = s.jobs.Close()
			<-s.maintenanceDone
			close(s.closeDone)
		}()
	})
}

func (s *portalBillTasks) Close() error {
	s.beginClose()
	<-s.closeDone
	if errors.Is(s.closeErr, exportjobs.ErrBusy) {
		return s.jobs.Close()
	}
	return s.closeErr
}

func (m *Monitor) stopPortalBillTasks(ctx context.Context) bool {
	m.portalBillMu.Lock()
	s := m.portalBillService
	m.portalBillMu.Unlock()
	if s == nil {
		return true
	}
	s.beginClose()
	select {
	case <-ctx.Done():
		return false
	case <-s.closeDone:
		// Existing file readers require no database connection. Their last Close
		// releases the directory; do not delete an in-progress download here.
		return s.closeErr == nil || errors.Is(s.closeErr, exportjobs.ErrBusy)
	}
}
