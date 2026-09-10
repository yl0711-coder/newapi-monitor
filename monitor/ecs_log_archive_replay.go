package monitor

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

// This is an in-process, read-only archive consumer, NOT a public replay route.
// Only objects read from the configured owner-checked S3 bucket may reach it.
type ecsArchiveReplayer struct {
	m      *Monitor
	prefix string
	router http.Handler
}
type ecsArchiveContextKey struct{}

func newECSArchiveReplayer(m *Monitor, prefix string) *ecsArchiveReplayer {
	r := gin.New()
	r.POST("/archive-replay", func(c *gin.Context) {
		source, ok := c.Request.Context().Value(ecsArchiveContextKey{}).(ECSLogSource)
		if !ok {
			c.Status(403)
			return
		}
		c.Set(ecsLogVerifiedContext, source)
		m.dispatchECSLog(c, source)
	})
	return &ecsArchiveReplayer{m: m, prefix: prefix, router: r}
}

// validate uses S3's timestamp, not a timestamp signed by the former task.
// Uploads must have occurred during a verified lease. Recovery may occur after
// lease expiry, but cannot resurrect, renew, or declare a source complete.
func (r *ecsArchiveReplayer) validate(ctx context.Context, o ecsarchive.Object, now time.Time) (ECSLogSource, ecsarchive.Envelope, int) {
	var source ECSLogSource
	if !r.m.cfg.ECSLogEnabled || r.m.cfg.ECSLogScope != "isolated" {
		return source, ecsarchive.Envelope{}, 503
	}
	owner, node, lane, hash, err := ecsarchive.ParseKey(r.prefix, o.Key)
	e, decodeErr := ecsarchive.Decode(o.Body)
	if err != nil || decodeErr != nil || o.ETag == "" || o.Modified.IsZero() || e.Node != node || e.Lane != lane || e.Hash != hash || e.Audience != r.m.cfg.ECSLogAudience {
		return source, e, 403
	}
	err = r.m.storeDB.WithContext(ctx).First(&source, "node = ? AND lane = ?", node, lane).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return source, e, 403
	}
	if err != nil {
		return source, e, 503
	}
	policy, allowed := r.m.ecsLogPolicy(source.ServiceARN, source.Container, lane)
	key, keyErr := base64.StdEncoding.DecodeString(source.PublicKey)
	task := source.TaskARN[strings.LastIndex(source.TaskARN, "/")+1:]
	if !allowed || source.TaskRoleARN != policy.TaskRoleARN || source.Revoked || ecsarchive.OwnerTask(owner) != task || keyErr != nil || !e.Verify(ed25519.PublicKey(key)) {
		return source, e, 403
	}
	if err := r.m.checkECSLogOwnership(ctx, source); err != nil {
		return source, e, 503
	}
	at := o.Modified.Unix()
	if at < source.FirstSeen || at > source.LeaseUntil || at > now.Unix()+ecsLogSignatureSkew || at < now.Unix()-ecsLogReplaySeconds || source.StoppedAt > 0 && at > source.StoppedAt+ecsLogLeaseSeconds {
		return source, e, 410
	}
	var window ECSLogLeaseWindow
	err = r.m.storeDB.WithContext(ctx).Where("node = ? AND lane = ? AND started_at <= ? AND until >= ?", node, lane, at, at).First(&window).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return source, e, 403
	}
	if err != nil {
		return source, e, 503
	}
	return source, e, 200
}

type ecsArchiveReplayResult struct {
	Status                int
	Duplicate             bool
	Accepted              bool
	PreviousHash          *string
	Kind                  string
	FinalBoundaryVerified bool
}

func (r *ecsArchiveReplayer) replay(ctx context.Context, o ecsarchive.Object, now time.Time) ecsArchiveReplayResult {
	source, e, status := r.validate(ctx, o, now)
	if status != 200 {
		return ecsArchiveReplayResult{Status: status}
	}
	if status := r.checkArchiveClosure(ctx, e); status != 200 {
		return ecsArchiveReplayResult{Status: status, PreviousHash: &e.Previous, Kind: e.Kind}
	}
	if status := r.dependency(ctx, e.Node, e.Lane, e.Previous); status != 200 {
		return ecsArchiveReplayResult{Status: status, PreviousHash: &e.Previous, Kind: e.Kind}
	}
	if e.Kind == ecsarchive.CollectorClosure {
		result := ecsArchiveReplayResult{Status: 200, Accepted: true, PreviousHash: &e.Previous, Kind: e.Kind}
		boundary, err := ecsarchive.ReadClosure(e.Body, e.Node, e.Lane)
		if err != nil {
			result.Status, result.Accepted = 422, false
			return result
		}
		if boundary != nil {
			if source.StoppedAt == 0 || source.ProducerExitCode == nil {
				result.Status, result.Accepted = 425, false
				return result
			}
			if *source.ProducerExitCode != 0 || (source.StopCode != "ServiceSchedulerInitiated" && source.StopCode != "UserInitiated") || boundary.ObservedAt < source.FirstSeen || boundary.ObservedAt > o.Modified.Unix()+ecsLogSignatureSkew || boundary.ObservedAt > source.StoppedAt+ecsLogSignatureSkew {
				result.Status, result.Accepted = 422, false
				return result
			}
			result.FinalBoundaryVerified = true
		}
		var count int64
		if err := r.m.storeDB.WithContext(ctx).Model(&ECSLogArchiveReceipt{}).Where("object_key = ? AND status = 200", o.Key).Count(&count).Error; err != nil {
			return ecsArchiveReplayResult{Status: 503}
		}
		result.Duplicate = count > 0
		if err := r.record(ctx, o, result); err != nil {
			return ecsArchiveReplayResult{Status: 503, PreviousHash: &e.Previous, Kind: e.Kind}
		}
		return result // Control proof only. Never dispatch as a request/sample.
	}
	ctx = context.WithValue(ctx, ecsArchiveContextKey{}, source)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://archive.internal/archive-replay", bytes.NewReader(e.Body))
	if err != nil {
		return ecsArchiveReplayResult{Status: 500}
	}
	req.Header.Set("Content-Type", "application/json")
	w := &ecsArchiveResponse{header: make(http.Header)}
	r.router.ServeHTTP(w, req)
	result := ecsArchiveReplayResult{Status: w.code, PreviousHash: &e.Previous}
	if w.code == 409 {
		var detail struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(w.body.Bytes(), &detail) == nil && detail.Error == "source cursor discontinuity" {
			result.Status = 425
		}
	}
	if w.code != 200 {
		return result
	}
	var ack struct {
		OK        bool `json:"ok"`
		Rejected  int  `json:"rejected"`
		Duplicate bool `json:"duplicate"`
	}
	if json.Unmarshal(w.body.Bytes(), &ack) != nil || !ack.OK || ack.Rejected != 0 {
		result.Status = 422
		return result
	}
	result.Accepted, result.Duplicate = true, ack.Duplicate
	if err := r.record(ctx, o, result); err != nil {
		return ecsArchiveReplayResult{Status: 503}
	}
	return result
}

// Bounded internal acknowledgement sink. Gin supplies its own response wrapper.
type ecsArchiveResponse struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *ecsArchiveResponse) Header() http.Header { return w.header }
func (w *ecsArchiveResponse) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *ecsArchiveResponse) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = 200
	}
	if w.body.Len()+len(b) > 64<<10 {
		return 0, errors.New("archive acknowledgement exceeds budget")
	}
	return w.body.Write(b)
}
