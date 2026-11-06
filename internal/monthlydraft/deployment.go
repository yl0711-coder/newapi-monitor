package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DeploymentConfig struct {
	Rollout                                                GenerationRollout
	ReceiverPath, FundingPath, AuthorityPath, EvidenceRoot string
	// Optional private index basename under EvidenceRoot; retained automatic
	// mode only. Empty preserves the existing behavior (no preparation loop).
	PreparationInbox string
}

// Deployment owns only generation resources, never the live Monitor connection.
// Open does not migrate, prepare inputs, start workers, or approve/publish bills.
type Deployment struct {
	mu                           sync.Mutex
	runtime                      *LocalRuntime
	g                            *Generation
	receiver, funding, authority *sql.DB
	lock                         *os.File
	closed                       bool
	readOnly                     bool
	inbox                        *preparationInbox
	capturedPreparation          *capturedPreparationResources
	preparationInstallFailed     bool
	capacity                     *publishedCapacityMaintenance
	automaticScopeID             string
}

func OpenDeployment(parent context.Context, cfg DeploymentConfig) (_ *Deployment, err error) {
	return openDeployment(parent, cfg, false)
}

type automaticDeploymentDependencies struct {
	load          AutomaticScopeLoader
	publish       AutomaticPublisher
	sourceID      string
	seriesIndex   string
	publishSeries AutomaticDisplaySeriesPublisher
}

// OpenRetainedAutomaticDeployment uses a pinned, private source manifest instead
// of an in-memory historical scope callback. It does not create that evidence,
// migrate, collect or start work. The service must explicitly select this mode.
func OpenRetainedAutomaticDeployment(parent context.Context, cfg DeploymentConfig, sourceID string, publish AutomaticPublisher) (*Deployment, error) {
	if cfg.Rollout.Enabled && (!validScopeSourceID(sourceID) || publish == nil) {
		return nil, ErrGenerationInput
	}
	return openDeploymentWithAutomatic(parent, cfg, false, &automaticDeploymentDependencies{sourceID: sourceID, publish: publish})
}

// OpenAutomaticDeployment explicitly composes the existing production resource
// owner with audited automatic issuance. The source loader and publisher must
// come from a trusted composition root, never customer input. Construction
// starts nothing, initializes no schemas, and does not collect or prepare data.
// OpenDeployment remains draft-only; automatic mode is explicitly opt-in.
func OpenAutomaticDeployment(parent context.Context, cfg DeploymentConfig, load AutomaticScopeLoader, publish AutomaticPublisher) (*Deployment, error) {
	if cfg.Rollout.Enabled && (load == nil || publish == nil) {
		return nil, ErrGenerationInput
	}
	return openDeploymentWithAutomatic(parent, cfg, false, &automaticDeploymentDependencies{load: load, publish: publish})
}

// readOnly is used only by the private preparation inspector. It cannot start
// workers and does not acquire/create the writer's sibling lock file.
func openDeployment(parent context.Context, cfg DeploymentConfig, readOnly bool) (_ *Deployment, err error) {
	return openDeploymentWithAutomatic(parent, cfg, readOnly, nil)
}

func openDeploymentWithAutomatic(parent context.Context, cfg DeploymentConfig, readOnly bool, automatic *automaticDeploymentDependencies) (_ *Deployment, err error) {
	if _, err := rolloutScope(cfg.Rollout, time.Now()); err != nil {
		return nil, err
	}
	if !cfg.Rollout.Enabled {
		return nil, nil
	}
	if cfg.Rollout.Policy == GenerationPolicyUsageRegistered && (readOnly || cfg.PreparationInbox != "" || automatic == nil || automatic.seriesIndex == "" || automatic.sourceID == "" || automatic.publishSeries == nil) {
		return nil, ErrGenerationInput
	}
	if parent == nil {
		return nil, ErrGenerationInput
	}
	if cfg.PreparationInbox != "" && (!preparationInboxName(cfg.PreparationInbox) || (!readOnly && (automatic == nil || !validScopeSourceID(automatic.sourceID)))) {
		return nil, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	paths := []string{cfg.ReceiverPath, cfg.FundingPath, cfg.AuthorityPath, cfg.EvidenceRoot}
	infos := make([]os.FileInfo, len(paths))
	for i, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return nil, ErrGenerationInput
		}
		st, e := os.Lstat(path)
		if e != nil || st.Mode()&os.ModeSymlink != 0 || (i < 3 && !st.Mode().IsRegular()) || (i == 3 && !st.IsDir()) {
			return nil, ErrGenerationInput
		}
		infos[i] = st
	}
	if infos[0].Mode().Perm()&0077 != 0 || infos[3].Mode().Perm()&0077 != 0 {
		return nil, ErrGenerationInput
	}
	// A receiver hardlink would allow another filename to acquire another lock
	// for the same SQLite file. Canonicalize directory aliases as well.
	receiverStat, ok := infos[0].Sys().(*syscall.Stat_t)
	if !ok || receiverStat.Nlink != 1 {
		return nil, ErrGenerationInput
	}
	receiverPath, e := filepath.EvalSymlinks(cfg.ReceiverPath)
	if e != nil {
		return nil, ErrGenerationInput
	}
	parentInfo, e := os.Stat(filepath.Dir(cfg.ReceiverPath))
	if e != nil || parentInfo.Mode().Perm()&0077 != 0 {
		return nil, ErrGenerationInput
	}
	for i := 0; i < 3; i++ {
		for j := 0; j < i; j++ {
			if os.SameFile(infos[i], infos[j]) {
				return nil, ErrGenerationInput
			}
		}
	}
	d := &Deployment{readOnly: readOnly}
	if automatic != nil {
		d.automaticScopeID = automatic.sourceID
	}
	defer func() {
		if err != nil {
			_ = d.closeResources()
		}
	}()
	// Private sibling lock remains in place after shutdown; never unlink a lock
	// inode while another process could already be waiting on it.
	if !readOnly {
		lockPath := receiverPath + ".runtime.lock"
		fd, e := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
		if e != nil {
			return nil, ErrGenerationInput
		}
		d.lock = os.NewFile(uintptr(fd), lockPath)
		st, e := d.lock.Stat()
		if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return nil, ErrGenerationInput
		}
		lockStat, ok := st.Sys().(*syscall.Stat_t)
		if !ok || lockStat.Nlink != 1 {
			return nil, ErrGenerationInput
		}
		for _, info := range infos[:3] {
			if os.SameFile(st, info) {
				return nil, ErrGenerationInput
			}
		}
		if e := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return nil, ErrGenerationBusy
		}
	}
	open := func(path, mode string) (*sql.DB, error) {
		u := url.URL{Scheme: "file", Path: path}
		q := url.Values{"mode": {mode}, "_pragma": {"busy_timeout(500)"}}
		if mode == "ro" {
			q.Add("_pragma", "query_only(1)")
		}
		u.RawQuery = q.Encode()
		db, e := sql.Open("sqlite", u.String())
		if e != nil {
			return nil, e
		}
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(2)
		if e = db.PingContext(ctx); e != nil {
			_ = db.Close()
			return nil, e
		}
		return db, nil
	}
	receiverMode := "rw"
	if readOnly {
		receiverMode = "ro"
	}
	if d.receiver, err = open(cfg.ReceiverPath, receiverMode); err != nil {
		return nil, err
	}
	if d.funding, err = open(cfg.FundingPath, "ro"); err != nil {
		return nil, err
	}
	if d.authority, err = open(cfg.AuthorityPath, "ro"); err != nil {
		return nil, err
	}
	// Detect alias/replacement during opening; all files are required to live in
	// operator-controlled directories. No automatic creation or migrations.
	for i, db := range []*sql.DB{d.receiver, d.funding, d.authority} {
		actual, e := generationDatabaseFile(db)
		if e != nil || !os.SameFile(infos[i], actual) {
			return nil, ErrGenerationInput
		}
	}
	var version int
	if err = d.receiver.QueryRowContext(ctx, `SELECT version FROM portal_monthly_draft_task_control WHERE id=1`).Scan(&version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, ErrGenerationIntegrity
	}
	for _, check := range []struct {
		db     *sql.DB
		tables []string
	}{
		{d.receiver, []string{"portal_monthly_draft_tasks", "portal_monthly_generation_inputs", "portal_monthly_generation_selections"}},
		{d.funding, []string{"portal_monthly_funding_pages", "portal_monthly_funding_records"}},
		{d.authority, []string{"customer_groups", "tracked_users", "usage_member_controls", "usage_member_control_migration_state"}},
	} {
		for _, name := range check.tables {
			var count int
			if err = check.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count); err != nil {
				return nil, err
			}
			if count != 1 {
				return nil, ErrGenerationIntegrity
			}
		}
	}
	auth, e := gorm.Open(&sqlite.Dialector{Conn: d.authority}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if e != nil {
		return nil, e
	}
	selector, e := GenerationSelector(d.receiver)
	if e != nil {
		return nil, e
	}
	guard, e := NewUsageGenerationGuard(auth, selector)
	if e != nil {
		return nil, e
	}
	if d.g, err = NewGeneration(d.receiver, d.funding, cfg.EvidenceRoot, guard); err != nil {
		return nil, err
	}
	d.g.readOnly = readOnly
	if automatic == nil {
		d.runtime, err = NewControlledLocalRuntime(d.g, auth, cfg.Rollout)
	} else {
		var metadata AutomaticContext
		load := automatic.load
		if automatic.sourceID != "" {
			load, err = NewRetainedAutomaticScopeLoader(d.g, automatic.sourceID)
		}
		if err == nil {
			metadata, err = NewObservedAutomaticContext(d.g, load)
		}
		if err == nil {
			if automatic.seriesIndex != "" {
				var ranges []AutomaticDisplaySeriesRange
				ranges, err = readAutomaticDisplaySeriesIndex(ctx, d.g, automatic.seriesIndex, cfg.Rollout)
				if err == nil {
					d.runtime, err = newAutomaticDisplaySeriesRuntime(d.g, auth, cfg.Rollout, metadata, ranges, automatic.publishSeries)
				}
			} else {
				d.runtime, err = NewAutomaticLocalRuntime(d.g, auth, cfg.Rollout, metadata, automatic.publish)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if cfg.PreparationInbox != "" && !readOnly {
		d.inbox = &preparationInbox{name: cfg.PreparationInbox, sourceID: automatic.sourceID}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Deployment) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.readOnly || d.preparationInstallFailed {
		return ErrGenerationInput
	}
	var background func(context.Context)
	if d.inbox != nil {
		background = func(ctx context.Context) { d.inbox.serve(ctx, d) }
	}
	return d.runtime.start(ctx, background)
}
func (d *Deployment) SubmitMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	if d.readOnly {
		return monitor.MonthlyGenerationView{}, ErrGenerationInput
	}
	return d.runtime.SubmitMonthly(ctx, gid, version, month)
}
func (d *Deployment) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	return d.runtime.ReadMonthly(ctx, gid, version, month)
}

// A timeout retains ALL handles and the process lock. Retry Close; never close
// the shared app/source handles out from under active work.
func (d *Deployment) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	if err := d.runtime.Stop(ctx); err != nil {
		return err
	}
	err := d.closeResources()
	d.closed = true
	return err
}
func (d *Deployment) closeResources() error {
	var errs []error
	if d.capacity != nil {
		errs = append(errs, d.capacity.close())
	}
	if d.capturedPreparation != nil {
		errs = append(errs, d.capturedPreparation.close())
	}
	if d.g != nil {
		errs = append(errs, d.g.Close())
	}
	for _, db := range []*sql.DB{d.receiver, d.funding, d.authority} {
		if db != nil {
			errs = append(errs, db.Close())
		}
	}
	if d.lock != nil {
		errs = append(errs, d.lock.Close())
	}
	return errors.Join(errs...)
}
