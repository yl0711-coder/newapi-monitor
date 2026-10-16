package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/usagebill/exportjobs"
	"gorm.io/gorm"
)

var errPortalBillAuthUnavailable = errors.New("bill authorization temporarily unavailable")

func portalBillAuthorizationError(err error) error {
	// The legacy snapshot helper also wraps database failures with the invalid
	// sentinel. Only explicit closure or a proven missing group is revocation.
	if err == errPortalAuthorizationInvalid || (errors.Is(err, errPortalAuthorizationInvalid) && errors.Is(err, gorm.ErrRecordNotFound)) { //nolint:errorlint // Exact sentinel intentionally excludes wrapped database outages; covered by classification tests.
		return exportjobs.ErrUnauthorized
	}
	return errPortalBillAuthUnavailable
}

// Opt-in service; ordinary Portal startup does not create a worker.
type portalBillTasks struct {
	m               *Monitor
	jobs            *exportjobs.Manager
	build           func(context.Context, io.Writer, portalBillScope, int64, func(context.Context) error, func(int64) error) (int64, error)
	generation      exportLimiter
	base            string
	stop            chan struct{}
	maintenanceDone chan struct{}
	closeOnce       sync.Once
	closeDone       chan struct{}
	closeErr        error // published by closeDone
	mounted         atomic.Bool
}

func portalBillOwner(s portalUsageAuthorizationSnapshot) exportjobs.Owner {
	// Raw log exports read RDS, not a published facts generation. Bind durable
	// membership and serving members, not unrelated hourly facts publications.
	b, _ := json.Marshal([]string{s.MemberManifest, s.ServingFingerprint})
	h := sha256.Sum256(b)
	return exportjobs.Owner{GroupID: s.GroupID, AuthVersion: s.AuthVersion, MemberFingerprint: hex.EncodeToString(h[:])}
}

func newPortalBillTasks(m *Monitor, base string) (*portalBillTasks, error) {
	if m == nil || m.exportLim == nil {
		return nil, exportjobs.ErrInvalid
	}
	s := &portalBillTasks{m: m, build: m.buildPortalBillSnapshot, generation: exportLimiter{last: map[int64]int64{}}, base: base, stop: make(chan struct{}), maintenanceDone: make(chan struct{}), closeDone: make(chan struct{})}
	jobs, err := exportjobs.New(base, exportjobs.Limits{MaxJobs: 2, MaxRows: portalExportCap, MaxBytes: portalExportMaxBytes, GenerateTimeout: 20 * time.Second, Retention: 10 * time.Minute, RetryDelay: 5 * time.Second}, s.authorize)
	if err != nil {
		return nil, err
	}
	s.jobs = jobs
	go s.maintain(time.Minute)
	return s, nil
}

func (s *portalBillTasks) authorize(ctx context.Context, owner exportjobs.Owner) error {
	snap, err := s.m.loadPortalUsageAuthorizationSnapshot(ctx, owner.GroupID)
	if err != nil {
		return portalBillAuthorizationError(err)
	}
	if portalBillOwner(snap) != owner {
		return exportjobs.ErrUnauthorized
	}
	return nil
}

func (s *portalBillTasks) mount(r *gin.Engine) {
	api := r.Group("/api/logs/bill/tasks")
	api.Use(func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("X-Content-Type-Options", "nosniff")
		select {
		case <-s.stop:
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "账单服务正在关闭，请稍后重试"})
			return
		default:
		}
		c.Next()
	})
	api.Use(s.m.requirePortal(true))
	api.POST("", s.create)
	api.GET("/:id", s.status)
	api.POST("/:id/cancel", s.cancel)
	api.GET("/:id/download", s.download)
	s.mounted.Store(true)
}

// Advertise only a mounted, live service. Merely allocating a worker is not a
// capability promise to customers. This does not initialize/enable the service.
func (m *Monitor) portalBillTaskContract() int {
	m.portalBillMu.Lock()
	defer m.portalBillMu.Unlock()
	s := m.portalBillService
	if s == nil || !s.mounted.Load() || m.shuttingDown.Load() {
		return 0
	}
	select {
	case <-s.stop:
		return 0
	default:
		return 1
	}
}

func portalBillMutationAllowed(c *gin.Context) bool {
	u, err := url.Parse(c.GetHeader("Origin"))
	local := c.Request.Host == "localhost" || strings.HasPrefix(c.Request.Host, "localhost:") || c.Request.Host == "127.0.0.1" || strings.HasPrefix(c.Request.Host, "127.0.0.1:")
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" ||
		!strings.EqualFold(u.Host, c.Request.Host) || (u.Scheme != "https" && !(local && u.Scheme == "http")) ||
		c.GetHeader("X-Usage-Bill-Request") != "1" || (c.GetHeader("Sec-Fetch-Site") != "" && c.GetHeader("Sec-Fetch-Site") != "same-origin") {
		c.JSON(http.StatusForbidden, gin.H{"error": "请求来源验证失败，请刷新页面后重试"})
		return false
	}
	return true
}

func (s *portalBillTasks) owner(c *gin.Context) (exportjobs.Owner, portalUsageAuthorizationSnapshot, error) {
	snap, err := s.m.loadPortalUsageAuthorizationSnapshot(c.Request.Context(), c.GetInt64("portalGID"))
	if err != nil {
		return exportjobs.Owner{}, snap, portalBillAuthorizationError(err)
	}
	if snap.AuthVersion != c.GetInt64("portalAuthVer") {
		return exportjobs.Owner{}, snap, exportjobs.ErrUnauthorized
	}
	return portalBillOwner(snap), snap, nil
}

func portalBillTaskError(c *gin.Context, err error) {
	code, msg := http.StatusServiceUnavailable, "账单服务暂不可用，请稍后重试"
	switch {
	case errors.Is(err, exportjobs.ErrNotFound):
		code, msg = http.StatusNotFound, "任务不存在或已过期，请重新生成"
	case errors.Is(err, exportjobs.ErrUnauthorized), errors.Is(err, errPortalAuthorizationInvalid):
		code, msg = http.StatusUnauthorized, "权限已变化，请重新登录"
	case errors.Is(err, exportjobs.ErrBusy):
		code, msg = http.StatusTooManyRequests, "导出任务繁忙，请稍后重试"
	case errors.Is(err, exportjobs.ErrNotReady):
		code, msg = http.StatusConflict, "账单尚未生成完成"
	case errors.Is(err, exportjobs.ErrInvalid):
		code, msg = http.StatusBadRequest, "账单请求无效"
	}
	c.JSON(code, gin.H{"error": msg})
}

func portalBillTaskJSON(v exportjobs.View) gin.H {
	return gin.H{"id": v.ID, "state": v.State, "rows": v.Rows, "total": v.ExpectedRows, "bytes": v.Bytes, "sha256": v.SHA256, "failure_code": v.FailureCode, "expires_at": v.ExpiresAt.Unix()}
}

func (s *portalBillTasks) create(c *gin.Context) {
	if !portalBillMutationAllowed(c) {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var in struct {
		Ticket string `json:"ticket"`
	}
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&in); err != nil {
		portalBillTaskError(c, exportjobs.ErrInvalid)
		return
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		portalBillTaskError(c, exportjobs.ErrInvalid)
		return
	}
	claim, ok := s.m.verifyPortalExportClaim(in.Ticket, time.Now().Unix())
	if !ok || claim.Format != portalBillFormat || claim.LogType != 2 || claim.GID != c.GetInt64("portalGID") || claim.Total < 0 || claim.Total > portalExportCap || claim.ToTs <= claim.FromTs || claim.ToTs-claim.FromTs > 31*86400 {
		portalBillTaskError(c, exportjobs.ErrInvalid)
		return
	}
	owner, snap, err := s.owner(c)
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	tracked, err := s.m.portalTrackedMembersForUsageRead(c.Request.Context(), owner.GroupID)
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	ids := idsOf(tracked)
	if len(ids) == 0 || portalMemberFingerprintFromIDs(ids) != claim.MemberFP || claim.MemberFP != snap.ServingFingerprint {
		portalBillTaskError(c, exportjobs.ErrUnauthorized)
		return
	}
	claim.ExpiresAt = 0 // Re-issued equivalent tickets deduplicate within retention.
	keyBytes, _ := json.Marshal(struct {
		Claim portalExportClaim
		Owner exportjobs.Owner
	}{claim, owner})
	h := sha256.Sum256(keyBytes)
	scope := portalBillScope{IDs: append([]int64(nil), ids...), From: claim.FromTs, To: claim.ToTs, Member: claim.MemberUID, Model: claim.Model, Group: claim.Group, Token: claim.TokenName, Detail: claim.DetailKw, RequestID: claim.RequestID}
	v, err := s.jobs.Submit(c.Request.Context(), exportjobs.Request{Owner: owner, Key: hex.EncodeToString(h[:]), ExpectedRows: claim.Total, Build: func(ctx context.Context, w io.Writer, p func(int64) error) (int64, error) {
		// Keep cancellation/recreation from bypassing generation throttling.
		now := time.Now().Unix()
		s.generation.prune(now, 5)
		if _, ok := s.generation.reserve(owner.GroupID, now, 5); !ok {
			return 0, exportjobs.ErrBusy
		}
		_, lease, err := s.m.acquirePortalExportStorage(portalExportMaxBytes)
		if err != nil {
			return 0, err
		}
		defer lease.release()
		return s.build(ctx, w, scope, claim.Total, func(ctx context.Context) error { return s.authorize(ctx, owner) }, p)
	}})
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"ok": true, "task": portalBillTaskJSON(v)})
}

func (s *portalBillTasks) status(c *gin.Context) {
	owner, _, err := s.owner(c)
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	v, err := s.jobs.View(c.Request.Context(), owner, c.Param("id"))
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "task": portalBillTaskJSON(v)})
}

func (s *portalBillTasks) cancel(c *gin.Context) {
	if !portalBillMutationAllowed(c) {
		return
	}
	owner, _, err := s.owner(c)
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	if err := s.jobs.Cancel(c.Request.Context(), owner, c.Param("id")); err != nil {
		portalBillTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *portalBillTasks) download(c *gin.Context) {
	owner, _, err := s.owner(c)
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	a, err := s.jobs.Open(c.Request.Context(), owner, c.Param("id"))
	if err != nil {
		portalBillTaskError(c, err)
		return
	}
	defer a.Close()
	now := time.Now().Unix()
	prev, ok := s.m.exportLim.reserve(owner.GroupID, now, int64(portalExportWindow.Seconds()))
	if !ok {
		portalBillTaskError(c, exportjobs.ErrBusy)
		return
	}
	if err := portalSendExport(c, a, a.View().Bytes, "NexusAPI-bill-"+a.View().ID+".csv", portalExportTotalTTL); err != nil {
		s.m.exportLim.rollback(owner.GroupID, prev, now)
	}
}
