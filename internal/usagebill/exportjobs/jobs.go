// Package exportjobs owns bounded, ephemeral bill-generation tasks. It is not
// an HTTP authorization layer: callers must derive Owner from a server-verified
// session and Key from canonical server-validated filters, never client identity.
// It is not yet wired into Monitor's routes or source database.
package exportjobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	ErrBusy         = errors.New("bill task capacity unavailable")
	ErrNotFound     = errors.New("bill task not found or expired")
	ErrUnauthorized = errors.New("bill authorization changed")
	ErrNotReady     = errors.New("bill task is not ready")
	ErrClosed       = errors.New("bill task manager closed")
	ErrInvalid      = errors.New("invalid bill task parameters")
	ErrTooLarge     = errors.New("bill artifact exceeds byte limit")
	ErrRowCount     = errors.New("bill row count mismatch")
)

type Owner struct {
	GroupID           int64
	AuthVersion       int64
	MemberFingerprint string
}

func (o Owner) valid() bool {
	return o.GroupID > 0 && o.AuthVersion >= 0 && o.MemberFingerprint != "" && len(o.MemberFingerprint) <= 128
}

type Limits struct {
	MaxJobs         int
	MaxRows         int64
	MaxBytes        int64
	GenerateTimeout time.Duration
	Retention       time.Duration
	RetryDelay      time.Duration
}

// Authorize must use ctx for bounded local authorization reads. Return
// ErrUnauthorized for a proven revocation; other errors are transient failures.
type Authorize func(context.Context, Owner) error

// Build must honor ctx, remain synchronous, not retain writer/progress, and
// return the independently counted number of rows. No database transaction or
// connection may remain held after it returns. It must not modify source facts.
type Build func(context.Context, io.Writer, func(int64) error) (int64, error)

type Request struct {
	Owner        Owner
	Key          string // SHA-256 of canonical filters, source snapshot and format/rule version
	ExpectedRows int64
	Build        Build
}

type View struct {
	ID           string
	State        string
	Rows         int64
	ExpectedRows int64
	Bytes        int64
	SHA256       string
	FailureCode  string // fixed code only, never upstream/raw error text
	ExpiresAt    time.Time
}

type job struct {
	request  Request
	view     View
	dir      string
	cancel   context.CancelFunc
	readers  int
	retired  bool
	finished time.Time
}

type Manager struct {
	mu          sync.Mutex
	root        string // only the uniquely created child directory is ever removed
	limits      Limits
	authorize   Authorize
	jobs        map[string]*job
	queue       chan *job
	ctx         context.Context
	stop        context.CancelFunc
	done        chan struct{}
	closed      bool
	sessionLock *os.File // held through generation, retained artifacts and open downloads
}

func New(base string, limits Limits, authorize Authorize) (*Manager, error) {
	if limits.MaxJobs < 1 || limits.MaxJobs > 8 || limits.MaxRows < 1 || limits.MaxRows > 1000000 ||
		limits.MaxBytes < 1 || limits.MaxBytes > 128<<20 || limits.GenerateTimeout <= 0 || limits.GenerateTimeout > 10*time.Minute ||
		limits.Retention < limits.GenerateTimeout || limits.Retention > time.Hour || limits.RetryDelay < 0 || limits.RetryDelay > limits.Retention ||
		int64(limits.MaxJobs)*limits.MaxBytes > 256<<20 || authorize == nil {
		return nil, ErrInvalid
	}
	root, lease, err := newSession(base)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{root: root, sessionLock: lease, limits: limits, authorize: authorize, jobs: make(map[string]*job), queue: make(chan *job, limits.MaxJobs), ctx: ctx, stop: cancel, done: make(chan struct{})}
	go m.worker()
	return m, nil
}

func (m *Manager) authorized(ctx context.Context, owner Owner) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.authorize(ctx, owner); err != nil {
		return err
	}
	return ctx.Err()
}

func (m *Manager) Submit(ctx context.Context, r Request) (View, error) {
	key, err := hex.DecodeString(r.Key)
	if err != nil || len(key) != sha256.Size || !r.Owner.valid() || r.ExpectedRows < 0 || r.ExpectedRows > m.limits.MaxRows || r.Build == nil {
		return View{}, ErrInvalid
	}
	r.Key = hex.EncodeToString(key)
	if err := m.authorized(ctx, r.Owner); err != nil {
		return View{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return View{}, ErrClosed
	}
	m.sweepLocked(time.Now())
	for _, existing := range m.jobs {
		if existing.request.Owner.GroupID != r.Owner.GroupID {
			continue
		}
		if existing.retired {
			return View{}, ErrBusy
		}
		// One retained task per group. Repeated clicks reuse the exact task;
		// changing filters cannot start parallel work or overwrite a download.
		if existing.request.Owner == r.Owner && existing.request.Key == r.Key && existing.request.ExpectedRows == r.ExpectedRows &&
			(existing.view.State == "queued" || existing.view.State == "running" || existing.view.State == "ready") {
			return existing.view, nil
		}
		if existing.view.State == "queued" || existing.view.State == "running" || existing.readers != 0 || time.Since(existing.finished) < m.limits.RetryDelay {
			return View{}, ErrBusy
		}
		if err := m.removeLocked(existing); err != nil {
			return View{}, err
		}
	}
	if len(m.jobs) >= m.limits.MaxJobs {
		return View{}, ErrBusy
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return View{}, err
	}
	j := &job{request: r, view: View{ID: hex.EncodeToString(id[:]), State: "queued", ExpectedRows: r.ExpectedRows, ExpiresAt: time.Now().Add(m.limits.Retention)}}
	m.jobs[j.view.ID] = j
	select {
	case m.queue <- j:
		return j.view, nil
	default:
		delete(m.jobs, j.view.ID)
		return View{}, ErrBusy
	}
}

func (m *Manager) lookupLocked(owner Owner, id string) (*job, error) {
	if m.closed {
		return nil, ErrClosed
	}
	j := m.jobs[id]
	if j == nil || j.retired || j.request.Owner != owner || !time.Now().Before(j.view.ExpiresAt) {
		return nil, ErrNotFound
	}
	return j, nil
}

func (m *Manager) View(ctx context.Context, owner Owner, id string) (View, error) {
	if err := m.access(ctx, owner, id); err != nil {
		return View{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.lookupLocked(owner, id)
	if err != nil {
		return View{}, err
	}
	return j.view, nil
}

func (m *Manager) access(ctx context.Context, owner Owner, id string) error {
	m.mu.Lock()
	_, err := m.lookupLocked(owner, id)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return m.authorized(ctx, owner)
}

// Artifact holds a lease until Close. Expiry/cancellation cannot delete a file
// being streamed, but no new download may open an expired or unauthorized task.
type Artifact struct {
	view    View
	file    *os.File
	manager *Manager
	job     *job
	once    sync.Once
	err     error
}

func (a *Artifact) View() View { return a.view }

func (a *Artifact) Read(p []byte) (int, error) { return a.file.Read(p) }

func (a *Artifact) Close() error {
	a.once.Do(func() {
		a.err = a.file.Close()
		a.manager.mu.Lock()
		defer a.manager.mu.Unlock()
		a.job.readers--
		if a.job.retired {
			_ = a.manager.removeLocked(a.job)
		}
		if a.manager.closed && len(a.manager.jobs) == 0 {
			if err := a.manager.removeRootLocked(); a.err == nil {
				a.err = err
			}
		}
	})
	return a.err
}

func (m *Manager) Open(ctx context.Context, owner Owner, id string) (*Artifact, error) {
	if err := m.access(ctx, owner, id); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.lookupLocked(owner, id)
	if err != nil {
		return nil, err
	}
	if j.view.State != "ready" {
		return nil, ErrNotReady
	}
	if j.readers != 0 {
		return nil, ErrBusy
	}
	path := filepath.Join(j.dir, "bill.csv")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != j.view.Bytes {
		return nil, ErrNotReady
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	j.readers++
	return &Artifact{view: j.view, file: f, manager: m, job: j}, nil
}

func (m *Manager) Cancel(ctx context.Context, owner Owner, id string) error {
	if err := m.access(ctx, owner, id); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.lookupLocked(owner, id)
	if err != nil {
		return err
	}
	if j.cancel != nil {
		j.cancel()
	}
	j.view.State = "canceled"
	j.finished = time.Now()
	j.retired = true
	// The worker owns partial files while running. A reader owns ready files.
	if j.cancel == nil && j.readers == 0 {
		return m.removeLocked(j)
	}
	return nil
}

func (m *Manager) removeLocked(j *job) error {
	if j.readers != 0 || j.cancel != nil {
		return ErrBusy
	}
	if j.dir != "" {
		if err := os.RemoveAll(j.dir); err != nil {
			return err
		}
	}
	j.request.Build = nil // A canceled queued item may still be in the bounded channel.
	delete(m.jobs, j.view.ID)
	return nil
}

func (m *Manager) sweepLocked(now time.Time) {
	for _, j := range m.jobs {
		if now.Before(j.view.ExpiresAt) && !j.retired {
			continue
		}
		j.retired = true
		if j.cancel != nil {
			j.cancel()
		} else if j.readers == 0 {
			_ = m.removeLocked(j)
		}
	}
}

func (m *Manager) worker() {
	defer close(m.done)
	ticker := time.NewTicker(min(m.limits.Retention, 30*time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			m.sweepLocked(time.Now())
			m.mu.Unlock()
		case j := <-m.queue:
			m.run(j)
		}
	}
}

type boundedWriter struct {
	ctx        context.Context
	w          io.Writer
	max, bytes int64
	err        error
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if w.err == nil {
		w.err = w.ctx.Err()
	}
	if w.err == nil && int64(len(p)) > w.max-w.bytes {
		w.err = ErrTooLarge
	}
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.w.Write(p)
	w.bytes += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

func (m *Manager) run(j *job) {
	m.mu.Lock()
	if m.closed || j.retired || m.jobs[j.view.ID] != j || !time.Now().Before(j.view.ExpiresAt) {
		m.mu.Unlock()
		return
	}
	deadline := time.Now().Add(m.limits.GenerateTimeout)
	if j.view.ExpiresAt.Before(deadline) {
		deadline = j.view.ExpiresAt
	}
	ctx, cancel := context.WithDeadline(m.ctx, deadline)
	j.cancel = cancel
	j.view.State = "running"
	m.mu.Unlock()
	code := "generation_failed"
	defer func() {
		if recover() != nil {
			code = "generation_failed"
		}
		cancel()
		m.mu.Lock()
		defer m.mu.Unlock()
		j.cancel = nil
		j.finished = time.Now()
		j.request.Build = nil // Do not retain source snapshots captured in closures.
		if code != "" {
			j.view.State, j.view.FailureCode = "failed", code
			j.view.Bytes, j.view.SHA256 = 0, ""
			if j.dir != "" {
				_ = os.RemoveAll(j.dir)
			}
		}
		j.view.ExpiresAt = time.Now().Add(m.limits.Retention)
		if j.retired || m.closed {
			_ = m.removeLocked(j)
		}
	}()
	if err := m.authorized(ctx, j.request.Owner); err != nil {
		code = failureCode(ctx, err)
		return
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(m.root, &disk); err != nil {
		return
	}
	if disk.Bsize <= 0 || disk.Bavail < (uint64(m.limits.MaxBytes)+(256<<20)+uint64(disk.Bsize)-1)/uint64(disk.Bsize) {
		code = "insufficient_storage"
		return
	}
	dir, err := os.MkdirTemp(m.root, "artifact-")
	if err != nil {
		return
	}
	m.mu.Lock()
	j.dir = dir
	m.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(dir, "partial"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	hash := sha256.New()
	w := &boundedWriter{ctx: ctx, w: io.MultiWriter(f, hash), max: m.limits.MaxBytes}
	progressInvalid := false
	rows, err := j.request.Build(ctx, w, func(rows int64) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		if rows < j.view.Rows || rows > j.view.ExpectedRows || rows < 0 {
			progressInvalid = true
			return ErrRowCount
		}
		j.view.Rows = rows
		return ctx.Err()
	})
	if err == nil {
		err = w.err
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		code = failureCode(ctx, err)
		return
	}
	if progressInvalid || rows != j.view.ExpectedRows {
		code = "row_count_mismatch"
		return
	}
	if err := m.authorized(ctx, j.request.Owner); err != nil {
		code = failureCode(ctx, err)
		return
	}
	if err := f.Close(); err != nil {
		return
	}
	if err := os.Chmod(filepath.Join(dir, "partial"), 0400); err != nil {
		return
	}
	if err := os.Rename(filepath.Join(dir, "partial"), filepath.Join(dir, "bill.csv")); err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil || j.retired || m.closed {
		code = failureCode(ctx, ctx.Err())
		return
	}
	j.view.Rows, j.view.Bytes, j.view.SHA256 = rows, w.bytes, hex.EncodeToString(hash.Sum(nil))
	j.view.State = "ready"
	code = ""
}

func failureCode(ctx context.Context, err error) string {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return "authorization_changed"
	case errors.Is(err, ErrTooLarge):
		return "file_too_large"
	case errors.Is(err, ErrRowCount):
		return "row_count_mismatch"
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return "generation_timeout"
	default:
		return "generation_failed"
	}
}

// Close requires builders to honor their context. Open downloads retain their
// leases until callers Close them; only this manager's generated directory is
// cleaned, never the supplied base directory or another manager's files.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.closed = true
	m.stop()
	m.mu.Unlock()
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		j.retired = true
		if j.readers == 0 {
			_ = m.removeLocked(j)
		}
	}
	if len(m.jobs) != 0 {
		return ErrBusy
	}
	return m.removeRootLocked()
}
