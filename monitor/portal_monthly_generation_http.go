package monitor

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

//go:embed portal_monthly_generation_local.js
var portalMonthlyGenerationLocalJS string

// MonthlyGenerationView deliberately excludes money, source/task identifiers,
// raw diagnostics and publication identifiers. A ready draft is NOT a saved bill;
// only the HTTP adapter can derive "available" from a validated publication.
type MonthlyGenerationView struct {
	Month string
	State string
}

// MonthlyGenerationFailure carries an allowlisted code, never a raw diagnostic.
type MonthlyGenerationFailure string

func (e MonthlyGenerationFailure) Error() string { return string(e) }

// MonthlyGenerationService is injected by the composition root. Monitor
// must not import the draft engine (which already uses Monitor's bill validator).
type MonthlyGenerationService interface {
	SubmitMonthly(context.Context, int64, int64, string) (MonthlyGenerationView, error)
	ReadMonthly(context.Context, int64, int64, string) (MonthlyGenerationView, error)
}

// Registration-derived, customer-safe metadata. No account IDs, source paths,
// balances or registration timestamps are exposed. Explicitly optional so old
// pre-prepared/local services do not silently gain a new source dependency.
type MonthlyGenerationCalendar struct {
	FirstMonth       string   `json:"first_month"`
	LastMonth        string   `json:"last_month"`
	FirstAvailableAt string   `json:"first_available_at"`
	Months           []string `json:"months"`
}

type MonthlyGenerationCalendarService interface {
	MonthlyCalendarEnabled() bool
	ReadMonthlyCalendar(context.Context, int64, int64) (MonthlyGenerationCalendar, error)
}

// RegisterLocalPortalMonthlyGenerationRoutes is explicit local-only wiring.
// Normal RegisterPortalRoutes does NOT call it. It installs no executor, source
// collector or publication flow, and never changes existing statement routes.
func (m *Monitor) RegisterLocalPortalMonthlyGenerationRoutes(r *gin.Engine, service MonthlyGenerationService) error {
	if r == nil || service == nil || !m.cfg.LocalSnapshotOnly || !m.cfg.PortalMonthlyStatementEnabled {
		return errors.New("monthly generation routes require explicit local setup")
	}
	const previewPath = "/monthly-generation-local"
	for _, route := range r.Routes() {
		if route.Path == previewPath {
			return errors.New("monthly generation routes already registered")
		}
	}
	if err := m.registerMonthlyGenerationAPI(r, service); err != nil {
		return err
	}
	r.GET(previewPath, m.requirePortal(false), func(c *gin.Context) {
		c.Header("Cache-Control", "private, no-store")
		c.Header("Content-Type", "text/html; charset=utf-8")
		html := strings.Replace(portalHTML, "<!--MONTHLY_STATEMENT_LINK-->", `<a data-v="monthly">月度账单</a>`, 1)
		script := portalMonthlyGenerationLocalJS
		if calendar, ok := service.(MonthlyGenerationCalendarService); ok && calendar.MonthlyCalendarEnabled() {
			script = strings.Replace(script, "/*MONTHLY_CALENDAR_ENABLED*/ false", "/*MONTHLY_CALENDAR_ENABLED*/ true", 1)
		}
		html = strings.Replace(html, "</body>", "<script>"+script+"</script></body>", 1)
		c.String(http.StatusOK, html)
	})
	return nil
}

// RegisterPortalRoutesWithGeneration explicitly adds controlled generation to
// the customer engine only. The caller owns validated resources and lifetime.
// This never starts a worker or registers any approval/publication endpoint.
func (m *Monitor) RegisterPortalRoutesWithGeneration(r *gin.Engine, service MonthlyGenerationService) error {
	if r == nil || service == nil || !m.cfg.PortalMonthlyStatementEnabled {
		return errors.New("monthly generation requires enabled statements and service")
	}
	for _, route := range r.Routes() {
		if route.Path == "/" || route.Path == "/login" {
			return errors.New("portal already registered")
		}
	}
	if err := m.registerMonthlyGenerationAPI(r, service); err != nil {
		return err
	}
	calendar, ok := service.(MonthlyGenerationCalendarService)
	m.registerPortalRoutes(r, true, ok && calendar.MonthlyCalendarEnabled())
	return nil
}

func (m *Monitor) registerMonthlyGenerationAPI(r *gin.Engine, service MonthlyGenerationService) error {
	const path = "/api/monthly-statement/generation"
	for _, route := range r.Routes() {
		if route.Path == path {
			return errors.New("monthly generation routes already registered")
		}
	}
	// Bounded in-flight metadata/admission operations; no waiting queue or timer
	// goroutine. Long-running calculation is never executed inside an HTTP call.
	slots := make(chan struct{}, 4)
	gate := func(c *gin.Context) {
		c.Header("Cache-Control", "private, no-store")
		c.Header("X-Content-Type-Options", "nosniff")
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			monthlyGenerationHTTPError(c, MonthlyGenerationFailure("temporarily_busy"))
			c.Abort()
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
	api := r.Group("", gate, m.requirePortal(true))
	api.GET(path, m.monthlyGenerationHTTP(service, false))
	api.POST(path, m.monthlyGenerationHTTP(service, true))
	if calendar, ok := service.(MonthlyGenerationCalendarService); ok && calendar.MonthlyCalendarEnabled() {
		api.GET("/api/monthly-statement/calendar", func(c *gin.Context) {
			if c.Request.URL.RawQuery != "" {
				monthlyGenerationHTTPError(c, MonthlyGenerationFailure("invalid_request"))
				return
			}
			scope, err := LoadMonthlyGenerationScope(c.Request.Context(), m.storeDB, c.GetInt64("portalGID"), c.GetInt64("portalAuthVer"))
			if err != nil {
				monthlyGenerationHTTPError(c, err)
				return
			}
			value, err := calendar.ReadMonthlyCalendar(c.Request.Context(), scope.GroupID, scope.AuthVersion)
			if err != nil {
				monthlyGenerationHTTPError(c, err)
				return
			}
			after, err := LoadMonthlyGenerationScope(c.Request.Context(), m.storeDB, scope.GroupID, scope.AuthVersion)
			if err != nil {
				monthlyGenerationHTTPError(c, err)
				return
			}
			if after.GroupID != scope.GroupID || after.AuthVersion != scope.AuthVersion || after.MemberFingerprint != scope.MemberFingerprint || after.MemberManifest != scope.MemberManifest {
				monthlyGenerationHTTPError(c, MonthlyGenerationFailure("state_changed"))
				return
			}
			c.JSON(http.StatusOK, value)
		})
	}
	return nil
}

func monthlyGenerationMonth(c *gin.Context, submit bool) (string, error) {
	var month string
	if submit {
		media, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
		if err != nil || media != "application/json" || c.Request.URL.RawQuery != "" {
			return "", MonthlyGenerationFailure("invalid_request")
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 512)
		dec := json.NewDecoder(c.Request.Body)
		open, err := dec.Token()
		if err != nil || open != json.Delim('{') {
			return "", MonthlyGenerationFailure("invalid_request")
		}
		key, err := dec.Token()
		if err != nil || key != "month" || dec.Decode(&month) != nil || dec.More() {
			return "", MonthlyGenerationFailure("invalid_request")
		}
		closeToken, err := dec.Token()
		var trailing any
		if err != nil || closeToken != json.Delim('}') || dec.Decode(&trailing) != io.EOF {
			return "", MonthlyGenerationFailure("invalid_request")
		}
	} else {
		query, err := url.ParseQuery(c.Request.URL.RawQuery)
		if err != nil || len(query) != 1 || len(query["month"]) != 1 {
			return "", MonthlyGenerationFailure("invalid_request")
		}
		month = query.Get("month")
	}
	start, err := time.ParseInLocation("2006-01", month, usageCST)
	if err != nil || len(month) != 7 || start.Year() < 2020 || start.AddDate(0, 1, 0).After(time.Now()) {
		return "", MonthlyGenerationFailure("invalid_request")
	}
	return month, nil
}

func (m *Monitor) monthlyGenerationHTTP(service MonthlyGenerationService, submit bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if submit && !portalBillMutationAllowed(c) {
			return
		}
		month, err := monthlyGenerationMonth(c, submit)
		if err != nil {
			monthlyGenerationHTTPError(c, err)
			return
		}
		ctx := c.Request.Context()
		gid, version := c.GetInt64("portalGID"), c.GetInt64("portalAuthVer")
		before, err := LoadMonthlyGenerationScope(ctx, m.storeDB, gid, version)
		if err != nil {
			monthlyGenerationHTTPError(c, err)
			return
		}
		// Frozen bills outlive their preparation inputs and worker. Never enqueue
		// another calculation merely because an old task/source was retired.
		record, err := m.monthlyGenerationSavedBill(ctx, before, month)
		if err != nil {
			monthlyGenerationHTTPError(c, err)
			return
		}
		view := MonthlyGenerationView{Month: month, State: "available"}
		if record == nil {
			if submit {
				view, err = service.SubmitMonthly(ctx, gid, version, month)
			} else {
				view, err = service.ReadMonthly(ctx, gid, version, month)
			}
			// The service cannot assert publication, even by returning a known
			// state string. A source-ready draft alone is not customer money data.
			if err == nil && (view.Month != month || view.State == "available" || monthlyGenerationStateMessage(view.State) == "") {
				err = MonthlyGenerationFailure("temporarily_unavailable")
			}
			if err != nil {
				monthlyGenerationHTTPError(c, err)
				return
			}
			// Completion may race this request. Re-read only, never resubmit.
			record, err = m.monthlyGenerationSavedBill(ctx, before, month)
			if err != nil {
				monthlyGenerationHTTPError(c, err)
				return
			}
			if record != nil {
				view.State = "available"
			}
		}
		// Do not reuse the read-only statement guard: it may rerun its handler.
		// A successful admission with a lost response is recovered by idempotency,
		// not by replaying submission in this request.
		after, err := LoadMonthlyGenerationScope(ctx, m.storeDB, gid, version)
		if err != nil {
			monthlyGenerationHTTPError(c, err)
			return
		}
		if before.MemberFingerprint != after.MemberFingerprint || before.MemberManifest != after.MemberManifest {
			monthlyGenerationHTTPError(c, MonthlyGenerationFailure("state_changed"))
			return
		}
		if record != nil && !m.portalMonthlyPublicationStillCurrent(ctx, c, *record) {
			return
		}
		message := monthlyGenerationStateMessage(view.State)
		if view.Month != month || message == "" || ctx.Err() != nil {
			monthlyGenerationHTTPError(c, MonthlyGenerationFailure("temporarily_unavailable"))
			return
		}
		code := http.StatusOK
		if submit && record == nil {
			code = http.StatusAccepted
		}
		c.JSON(code, gin.H{"month": month, "state": view.State, "message": message})
	}
}

// A failed integrity/storage check is not "no bill": fail closed rather than
// recalculate over a damaged publication. This is bounded local IO only and
// deliberately excludes the retained private evidence column.
// Current membership still gates new generation, but a verified saved bill is
// organization-owned and must not be regenerated after a roster change.
func (m *Monitor) monthlyGenerationSavedBill(ctx context.Context, scope MonthlyGenerationScope, month string) (*PortalMonthlyBill, error) {
	var record PortalMonthlyBill
	err := m.storeDB.WithContext(ctx).
		Select("group_id, month, payload, payload_hash, member_fingerprint, member_manifest, published_at, publication_id").
		Where("group_id = ? AND month = ? AND member_fingerprint <> '' AND member_manifest <> ''",
			scope.GroupID, month).First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, MonthlyGenerationFailure("temporarily_unavailable")
	}
	p, err := readPublishedMonthlyBill(record)
	if err != nil || !monthlyStatementAmountsComplete(p) {
		return nil, MonthlyGenerationFailure("temporarily_unavailable")
	}
	return &record, nil
}

func monthlyGenerationStateMessage(state string) string {
	switch state {
	case "available":
		return "账单已生成并保存，可在列表中查看和下载。"
	case "not_prepared":
		return "该月账单暂未准备好，请稍后查看。"
	case "not_requested":
		return "该月尚未提交生成申请。"
	case "queued":
		return "生成申请已排队。"
	case "processing":
		return "账单正在生成，请稍后查看。"
	case "finalizing":
		return "账单正在核对和保存，完成后可查看和下载。"
	case "pending_verification":
		return "请在账单列表查看生成结果；如需帮助请联系客服。"
	case "needs_preparation":
		return "该月账单暂未生成，请稍后查看或联系客服。"
	case "needs_review":
		return "该月账单暂未生成，请联系客服协助处理。"
	case "temporarily_unavailable":
		return "账单暂时未能生成，请稍后查看。"
	default:
		return ""
	}
}

func monthlyGenerationHTTPError(c *gin.Context, err error) {
	code, message, status := "temporarily_unavailable", "账单服务暂不可用，请稍后查看。", http.StatusServiceUnavailable
	var failure MonthlyGenerationFailure
	if errors.Is(err, ErrMonthlyGenerationUnauthorized) {
		code, message, status = "unauthorized", "登录状态已失效，请重新登录。", http.StatusUnauthorized
	} else if errors.As(err, &failure) {
		switch failure {
		case "invalid_request":
			code, message, status = string(failure), "请求无效，请选择已结束的月份后重试。", http.StatusBadRequest
		case "state_changed":
			code, message, status = string(failure), "登录权限或账单状态已变化，请刷新后重试。", http.StatusConflict
		case "not_prepared":
			code, message, status = string(failure), monthlyGenerationStateMessage("not_prepared"), http.StatusConflict
		case "needs_review":
			code, message, status = string(failure), monthlyGenerationStateMessage("needs_review"), http.StatusConflict
		case "temporarily_busy":
			code, message, status = string(failure), "账单服务暂时繁忙，请稍后重试。", http.StatusTooManyRequests
			c.Header("Retry-After", "5")
		case "request_cancelled":
			code, message, status = string(failure), "请求已取消。", http.StatusRequestTimeout
		}
	}
	c.JSON(status, gin.H{"code": code, "error": message})
}
