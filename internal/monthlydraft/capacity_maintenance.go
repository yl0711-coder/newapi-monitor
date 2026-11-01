package monthlydraft

// Opt-in orchestration of the existing verified publication archiver.
// No raw-source deletion, customer endpoint, amount change or quota increase.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type publishedCapacityMaintenance struct {
	mu                  sync.Mutex
	paused              atomic.Bool
	g                   *Generation
	main                *gorm.DB // borrowed read-only authority handle, not owned
	root                *os.Root
	lock                *os.File
	directory           string
	identity            os.FileInfo
	scope               *generationScope
	hotCount            int
	hotBytes, coldBytes int64
	transientFailures   int
	retryAt             time.Time
}

func newPublishedCapacityMaintenance(g *Generation, main *gorm.DB, directory string) (*publishedCapacityMaintenance, error) {
	if g == nil || g.readOnly || main == nil {
		return nil, ErrGenerationInput
	}
	root, lock, err := openCapturedOutput(directory)
	if err != nil {
		return nil, err
	}
	m := &publishedCapacityMaintenance{g: g, main: main, root: root, lock: lock, directory: directory,
		hotCount: 28, hotBytes: generationReceiverLimit - generationPlanLimit - monitor.MonthlyPreparationInputLimit - generationArtifactLimit, coldBytes: 256 << 20}
	m.identity, err = root.Stat(".")
	if err == nil {
		_, _, err = m.inventory(context.Background())
	}
	if err != nil {
		_ = m.close()
		return nil, err
	}
	return m, nil
}

func (m *publishedCapacityMaintenance) close() error {
	return errors.Join(m.root.Close(), m.lock.Close())
}

// Only the disabled, already installed preparation owner may opt in. Normal
// constructors/default startup do not call it; product startup requires an
// explicit validated cold-directory configuration after capture installation.
func (d *Deployment) EnablePublishedCapacityMaintenance(ctx context.Context, directory string) error {
	if d == nil || ctx == nil {
		return ErrGenerationInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, err := d.preparationResumer()
	if err != nil {
		return err
	}
	if p.capacity != nil || d.capacity != nil {
		return ErrGenerationBusy
	}
	// A still-empty raw spool must not be mistaken for an empty cold root.
	// Cold child directories would invalidate its independent source inventory.
	for _, unsafe := range []string{d.capturedPreparation.spoolDirectory(), d.capturedPreparation.captureDirectory()} {
		if unsafe == "" {
			continue
		}
		rel, e := filepath.Rel(unsafe, directory)
		if e != nil || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return ErrGenerationInput
		}
	}
	if d.g.root != nil && directory == d.g.root.Name() {
		return ErrGenerationInput
	}
	m, err := newPublishedCapacityMaintenance(d.g, p.auth, directory)
	if err != nil {
		return err
	}
	if err := InitPublishedGenerationRetention(ctx, d.g.receiver); err != nil {
		_ = m.close()
		return err
	}
	p.capacity, d.capacity = m, m
	m.scope = p.scope
	return nil
}

func (r *capturedPreparationResources) spoolDirectory() string {
	if r.spool != nil {
		return r.spool.directory
	}
	return ""
}
func (r *capturedPreparationResources) captureDirectory() string {
	if r.root != nil {
		return r.root.Name()
	}
	return ""
}

// One iteration, before reserving a preparation lease. Idle polls never call
// this method. Auditing/copying performs no source SQL and holds no SQL writer.
// A partial copy is retained, never overwritten or allocated a fresh retry dir.
func (m *publishedCapacityMaintenance) step(parent context.Context) (worked bool, err error) {
	if !m.mu.TryLock() {
		return false, ErrGenerationBusy
	}
	defer m.mu.Unlock()
	if m.paused.Load() {
		return false, ErrGenerationCapacity
	}
	if time.Now().Before(m.retryAt) {
		return false, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	defer func() {
		if err != nil && parent.Err() == nil {
			if errors.Is(err, ErrGenerationBusy) || errors.Is(err, monthlytasks.ErrBusy) || errors.Is(err, context.DeadlineExceeded) {
				m.transientFailures++
				m.retryAt = time.Now().Add(30 * time.Second)
				if m.transientFailures < 3 {
					return
				}
			}
			m.paused.Store(true)
			log.Print("monthly_capacity_maintenance stopped: local verification or capacity requires review")
		}
	}()
	var count int
	var size int64
	readCtx, done := context.WithTimeout(ctx, 2*time.Second)
	err = m.g.receiver.QueryRowContext(readCtx, `SELECT count(*),COALESCE(sum(length(plan)+length(preparation)+COALESCE(length(artifact),0)),0) FROM portal_monthly_generation_inputs`).Scan(&count, &size)
	done()
	if err != nil {
		return false, err
	}
	if count > generationRecordLimit || size > generationReceiverLimit {
		return false, ErrGenerationIntegrity
	}
	if count < m.hotCount && size <= m.hotBytes {
		return false, nil
	}
	// Metadata hint only. The original archiver independently validates the
	// task, current selection, source evidence, saved journal and customer bill.
	id, req, err := m.candidate(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	directories, bytes, err := m.inventory(ctx)
	if err != nil {
		return false, err
	}
	name := "input-" + req.InputSHA256
	if _, e := m.root.Lstat(name); os.IsNotExist(e) {
		var payload int64
		if err := m.g.receiver.QueryRowContext(ctx, `SELECT length(plan)+length(preparation)+COALESCE(length(artifact),0) FROM portal_monthly_generation_inputs WHERE id=?`, req.InputSHA256).Scan(&payload); err != nil {
			return false, err
		}
		if directories >= 256 || payload <= 0 || payload+2*archiveManifestLimit > m.coldBytes-bytes {
			return false, ErrGenerationCapacity
		}
		if err := m.root.Mkdir(name, 0700); err != nil {
			return false, err
		}
	} else if e != nil {
		return false, e
	}
	if err := automaticPreparationRootCurrent(m.root, m.directory); err != nil {
		return false, err
	}
	receipt, err := m.g.CopyPublishedGenerationArchive(ctx, m.main, req.Owner, id, filepath.Join(m.directory, name))
	if err != nil {
		return false, err
	}
	if err := syncArchiveDirectory(m.root); err != nil {
		return false, err
	}
	if _, _, err := m.inventory(ctx); err != nil {
		return false, err
	}
	if err := m.g.ChangePublishedGenerationPayload(ctx, m.main, receipt, req.Owner, id, false); err != nil {
		return false, err
	}
	m.transientFailures = 0
	m.retryAt = time.Time{}
	return true, nil
}

// Skip only proven stale authority/selection or scopes outside this owner's
// rollout. Corruption and unavailable authority are NOT silently skipped.
func (m *publishedCapacityMaintenance) candidate(parent context.Context) (string, monthlytasks.Request, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	rows, err := m.g.receiver.QueryContext(ctx, `SELECT t.id,CASE WHEN length(t.descriptor)<=4096 THEN t.descriptor END
FROM portal_monthly_draft_tasks t JOIN portal_monthly_generation_inputs i ON i.id=json_extract(CAST(t.descriptor AS TEXT),'$.input_sha256')
JOIN portal_monthly_publication_jobs j ON j.task_id=t.id
WHERE t.state='draft_ready' AND j.state='saved' AND NOT EXISTS(SELECT 1 FROM portal_monthly_generation_archives a WHERE a.id=i.id)
ORDER BY j.updated_at,t.id LIMIT 33`)
	if err != nil {
		cancel()
		return "", monthlytasks.Request{}, err
	}
	type item struct {
		id  string
		req monthlytasks.Request
	}
	items := []item{}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			_ = rows.Close()
			cancel()
			return "", monthlytasks.Request{}, err
		}
		var req monthlytasks.Request
		if len(items) >= 32 || json.Unmarshal(raw, &req) != nil || !generationHex(id, 16) || !generationHex(req.InputSHA256, 32) {
			_ = rows.Close()
			cancel()
			return "", monthlytasks.Request{}, ErrGenerationIntegrity
		}
		items = append(items, item{id, req})
	}
	err = errors.Join(rows.Err(), rows.Close())
	cancel()
	if err != nil {
		return "", monthlytasks.Request{}, err
	}
	for _, i := range items {
		if !m.scope.allows(i.req.Owner.GroupID, i.req.Month) {
			continue
		}
		if err := m.g.current(parent, i.req); errors.Is(err, ErrGenerationChanged) {
			continue
		} else if err != nil {
			return "", monthlytasks.Request{}, err
		}
		return i.id, i.req, nil
	}
	return "", monthlytasks.Request{}, sql.ErrNoRows
}

// Inventory counts every retained byte, including failed copies and BOTH
// manifest names (conservative for their hard link). No payload is loaded.
func (m *publishedCapacityMaintenance) inventory(ctx context.Context) (int, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	info, err := m.root.Stat(".")
	if err != nil || !os.SameFile(info, m.identity) || automaticPreparationRootCurrent(m.root, m.directory) != nil {
		return 0, 0, ErrGenerationIntegrity
	}
	f, err := m.root.Open(".")
	if err != nil {
		return 0, 0, err
	}
	names, err := f.ReadDir(257)
	closeErr := f.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, 0, err
	}
	if closeErr != nil || len(names) > 256 {
		return 0, 0, ErrGenerationCapacity
	}
	var used int64
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if !strings.HasPrefix(name.Name(), "input-") || !generationHex(strings.TrimPrefix(name.Name(), "input-"), 32) {
			return 0, 0, ErrGenerationIntegrity
		}
		st, err := m.root.Lstat(name.Name())
		if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
			return 0, 0, ErrGenerationIntegrity
		}
		child, err := m.root.OpenRoot(name.Name())
		if err != nil {
			return 0, 0, err
		}
		files, err := child.Open(".")
		if err != nil {
			_ = child.Close()
			return 0, 0, err
		}
		entries, readErr := files.ReadDir(6)
		if e := files.Close(); e != nil {
			_ = child.Close()
			return 0, 0, e
		}
		if (readErr != nil && !errors.Is(readErr, io.EOF)) || len(entries) > 5 {
			_ = child.Close()
			return 0, 0, ErrGenerationIntegrity
		}
		for _, e := range entries {
			n := e.Name()
			base := strings.TrimPrefix(name.Name(), "input-")
			if n != "manifest.json" && n != "manifest.pending" && n != base+".plan" && n != base+".preparation" && n != base+".artifact" {
				_ = child.Close()
				return 0, 0, ErrGenerationIntegrity
			}
			st, err := child.Lstat(n)
			if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() < 0 || st.Size() > m.coldBytes-used {
				_ = child.Close()
				return 0, 0, ErrGenerationCapacity
			}
			used += st.Size()
		}
		if err := child.Close(); err != nil {
			return 0, 0, err
		}
	}
	return len(names), used, ctx.Err()
}
