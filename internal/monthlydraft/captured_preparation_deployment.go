package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// CapturedPreparationConfig is PRIVATE composition configuration. All stores
// and evidence already exist. No customer, DSN or approval input is accepted.
type CapturedPreparationConfig struct {
	Version                    int                                  `json:"version"`
	SourceIdentity             string                               `json:"source_identity"`
	Connection                 monthlysource.MonthlyObservationFile `json:"connection"`
	CapturePath                string                               `json:"capture_path"`
	SpoolDirectory             string                               `json:"spool_directory"`
	History                    monthlysource.MonthlyObservationFile `json:"history"`
	ScopeCatalogue             monthlysource.MonthlyObservationFile `json:"scope_catalogue"`
	RecoverTokenWindows        bool                                 `json:"recover_token_windows,omitempty"`
	AccountTokenWork           bool                                 `json:"account_token_work,omitempty"`
	PartitionUsage             bool                                 `json:"partition_usage,omitempty"`
	RetainCompletedSeals       bool                                 `json:"retain_completed_seals,omitempty"`
	AssembleRetainedHistory    bool                                 `json:"assemble_retained_history,omitempty"`
	CaptureRegisteredOrigins   bool                                 `json:"capture_registered_origins,omitempty"`
	CaptureWalletTails         bool                                 `json:"capture_wallet_tails,omitempty"`
	DeriveHistoryFromOriginals bool                                 `json:"derive_history_from_originals,omitempty"`
}

type capturedPreparationConnection struct {
	Network  string `json:"network"`
	Address  string `json:"address"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	TLS      bool   `json:"tls"`
}

type capturedPreparationResources struct {
	source, capture *sql.DB
	funding         *FundingSourcePreparation
	spool           *CapturedSourceSpool // borrowed metadata handle; owns no connection
	root            *os.Root
	lock            *os.File
}

func (r *capturedPreparationResources) close() error {
	var errs []error
	for _, db := range []*sql.DB{r.source, r.capture} {
		if db != nil {
			errs = append(errs, db.Close())
		}
	}
	if r.root != nil {
		errs = append(errs, r.root.Close())
	}
	if r.lock != nil {
		errs = append(errs, r.lock.Close())
	}
	return errors.Join(errs...)
}

// EnableCapturedSourcePreparation explicitly wires the tested on-demand chain
// before Start. Source connection is LAZY; no source queries or collectors run
// at construction. Credentials must be a dedicated SELECT-only account. The
// collectors additionally enforce bounded READ ONLY transactions themselves.
// The deployment owns these separate handles/lock and closes them only AFTER
// every source operation, calendar request and publisher has drained.
func (d *Deployment) EnableCapturedSourcePreparation(ctx context.Context, sourceID string, config monthlysource.MonthlyObservationFile) error {
	return d.enableCapturedSourcePreparation(ctx, sourceID, config, func(cfg *mysql.Config) (*sql.DB, error) {
		connector, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, ErrGenerationInput
		}
		db := sql.OpenDB(connector)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(5 * time.Minute)
		return db, nil
	})
}

func (d *Deployment) enableCapturedSourcePreparation(parent context.Context, sourceID string, config monthlysource.MonthlyObservationFile, openSource func(*mysql.Config) (*sql.DB, error)) (err error) {
	if parent == nil || d == nil || d.runtime == nil || d.g == nil || openSource == nil || !validScopeSourceID(sourceID) {
		return ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.readOnly || d.capturedPreparation != nil || d.preparationInstallFailed || d.inbox != nil || d.automaticScopeID != sourceID {
		return ErrGenerationInput
	}
	r := d.runtime
	r.mu.Lock()
	base, ok := r.service.(*usageHTTPService)
	ready := ok && r.phase == "disabled" && r.preparation == nil && r.worker != nil && (len(r.worker.seriesScopes) != 0 || r.worker.dynamicSeries)
	r.mu.Unlock()
	if !ready {
		return ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	stage := "configuration"
	defer func() {
		if err != nil {
			err = fmt.Errorf("monthly source preparation (%s): %w", stage, err)
		}
	}()
	raw, err := d.g.readPrivatePreparationFile(ctx, config, 64<<10)
	if err != nil {
		return err
	}
	var cfg CapturedPreparationConfig
	if json.Unmarshal(raw, &cfg) != nil {
		return ErrGenerationInput
	}
	canonical, err := json.Marshal(cfg)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || cfg.Version != 1 || !generationHex(cfg.SourceIdentity, 32) {
		return ErrGenerationInput
	}
	if cfg.AssembleRetainedHistory && (r.scope == nil || !r.scope.registered || !r.worker.dynamicSeries) {
		return ErrGenerationInput
	}
	if cfg.DeriveHistoryFromOriginals && (cfg.AssembleRetainedHistory || cfg.History != (monthlysource.MonthlyObservationFile{}) || cfg.ScopeCatalogue != (monthlysource.MonthlyObservationFile{}) ||
		!cfg.CaptureRegisteredOrigins || !cfg.CaptureWalletTails || !cfg.PartitionUsage || !cfg.RetainCompletedSeals || !cfg.AccountTokenWork ||
		r.scope == nil || !r.scope.registered || !r.worker.dynamicSeries) {
		return ErrGenerationInput
	}
	if cfg.CaptureRegisteredOrigins && !cfg.AssembleRetainedHistory && !cfg.DeriveHistoryFromOriginals {
		return ErrGenerationInput
	}
	if cfg.CaptureWalletTails && !cfg.CaptureRegisteredOrigins {
		return ErrGenerationInput
	}
	for _, scope := range r.worker.seriesScopes {
		if scope.SourceIdentity != cfg.SourceIdentity {
			return ErrGenerationEvidence
		}
	}
	stage = "connection_configuration"
	connection, err := d.g.readPrivatePreparationFile(ctx, cfg.Connection, 16<<10)
	if err != nil {
		return err
	}
	mysqlConfig, err := capturedConnectionConfig(connection)
	if err != nil {
		return err
	}
	// Validate pinned original metadata before opening ANY new writer. Missing
	// history must not be mistaken for a request to initialize replacement data.
	stage = "history_sources"
	historyRefs := []monthlysource.MonthlyObservationFile{}
	if !cfg.DeriveHistoryFromOriginals {
		historyRefs = append(historyRefs, cfg.History)
	}
	if !cfg.DeriveHistoryFromOriginals && (!cfg.AssembleRetainedHistory || cfg.ScopeCatalogue.Path != "" || cfg.ScopeCatalogue.SHA256 != "") {
		historyRefs = append(historyRefs, cfg.ScopeCatalogue)
	}
	for _, ref := range historyRefs {
		if _, err := d.g.readPrivatePreparationFile(ctx, ref, monitor.MonthlyPreparationInputLimit); err != nil {
			return err
		}
	}
	res := &capturedPreparationResources{}
	defer func() {
		if err != nil {
			err = errors.Join(err, res.close())
		}
	}()
	stage = "capture_store"
	res.root, res.lock, res.capture, err = d.openCapturedPreparationStore(ctx, cfg.CapturePath)
	if err != nil {
		return err
	}
	if cfg.CaptureRegisteredOrigins {
		stage = "registered_origin_store"
		if err := monthlysource.ValidateRegisteredWalletMonthCaptureStore(ctx, res.capture); err != nil {
			return err
		}
	}
	if cfg.CaptureWalletTails {
		stage = "wallet_tail_store"
		if err := monthlysource.ValidateWalletTailCaptureStore(ctx, res.capture); err != nil {
			return err
		}
	}
	if cfg.RecoverTokenWindows {
		stage = "token_recovery_store"
		if err := monthlysource.ValidateRecoveringTokenMonthStore(ctx, res.capture); err != nil {
			return err
		}
	}
	stage = "source_connector"
	res.source, err = openSource(mysqlConfig)
	if err != nil || res.source == nil {
		return ErrGenerationInput // Never log a driver/credential-bearing error.
	}
	stage = "source_adapters"
	funding, err := NewFundingSourcePreparation(res.source, res.capture, d.g, base.authorization, cfg.SourceIdentity)
	if err != nil {
		return err
	}
	funding.tokenRecovery = cfg.RecoverTokenWindows
	funding.accountTokenWork = cfg.AccountTokenWork
	funding.partitionUsage = cfg.PartitionUsage
	funding.registeredOrigins = cfg.CaptureRegisteredOrigins
	funding.walletTails = cfg.CaptureWalletTails
	res.funding = funding
	wallet, err := NewWalletSourcePreparation(funding)
	if err != nil {
		return err
	}
	stage = "sealed_directory"
	spool, err := newCapturedSourceSpool(wallet, cfg.SpoolDirectory, cfg.RetainCompletedSeals)
	if err != nil {
		return err
	}
	res.spool = spool
	stage = "history_assembly"
	var history *automaticHistoryPreparation
	var step SourcePreparationStep
	if cfg.DeriveHistoryFromOriginals {
		history, err = newAutomaticHistoryPreparation(wallet, spool)
		if err == nil {
			step, err = newAutomaticHistorySelectedPreparation(d, history, sourceID)
		}
	} else {
		var next func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error)
		next, err = newCatalogueCapturedHistoryPreparation(d, spool, sourceID, cfg.History, cfg.ScopeCatalogue, cfg.AssembleRetainedHistory)
		if err == nil {
			step, err = ComposeCapturedSourcePreparation(wallet, spool, next)
		}
	}
	if err != nil {
		return err
	}
	// No source I/O or history scan in admission. Current complete authority,
	// explicit rollout and registered closed-month checks remain authoritative.
	d.preparationInstallFailed = true
	stage = "preparation_queue"
	if err := r.EnableOnDemandPreparation(ctx, base.authorization, func(ctx context.Context, _ SourcePreparationRequest) error { return ctx.Err() }, step); err != nil {
		return err
	}
	stage = "registration_calendar"
	r.preparation.accountTokenWork = cfg.AccountTokenWork
	if history != nil {
		if err := history.bindWorkAccounting(r); err != nil {
			return err
		}
	}
	if err := d.EnableRegistrationCalendar(funding); err != nil {
		return err
	}
	stage = "registered_prefix"
	if err := d.EnableRegisteredSeriesPreparation(); err != nil {
		return err
	}
	if history != nil {
		stage = "original_history_pipeline"
		if err := history.installRuntime(r); err != nil {
			return err
		}
	}
	d.capturedPreparation, d.preparationInstallFailed = res, false
	return nil
}

func capturedConnectionConfig(raw []byte) (*mysql.Config, error) {
	var c capturedPreparationConnection
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if len(raw) > 16<<10 || dec.Decode(&c) != nil || dec.Decode(new(any)) != io.EOF || c.User == "" || strings.EqualFold(c.User, "root") || len(c.User) > 96 || c.Database == "" || len(c.Database) > 64 || len(c.Password) > 8192 {
		return nil, ErrGenerationInput
	}
	switch c.Network {
	case "unix":
		if !filepath.IsAbs(c.Address) || filepath.Clean(c.Address) != c.Address || c.TLS {
			return nil, ErrGenerationInput
		}
	case "tcp":
		host, port, err := net.SplitHostPort(c.Address)
		n, portErr := strconv.Atoi(port)
		ip := net.ParseIP(host)
		if err != nil || portErr != nil || host == "" || n < 1 || n > 65535 || (!c.TLS && (ip == nil || !ip.IsLoopback())) {
			return nil, ErrGenerationInput
		}
	default:
		return nil, ErrGenerationInput
	}
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName = c.Network, c.Address, c.User, c.Password, c.Database
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 2*time.Second, 4*time.Second, 2*time.Second
	cfg.MaxAllowedPacket = 2 << 20
	cfg.Logger = log.New(io.Discard, "", 0)
	if c.TLS {
		cfg.TLSConfig = "true" // verified TLS only, never skip-verify/preferred
	}
	return cfg, nil
}

func (d *Deployment) openCapturedPreparationStore(ctx context.Context, path string) (root *os.Root, lock *os.File, db *sql.DB, err error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, nil, nil, ErrGenerationInput
	}
	root, lock, err = openCapturedOutput(filepath.Dir(path))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("private capture directory: %w", err)
	}
	defer func() {
		if err != nil {
			if db != nil {
				_ = db.Close()
			}
			_ = root.Close()
			_ = lock.Close()
			root, lock, db = nil, nil, nil
		}
	}()
	directoryInfo, statErr := root.Stat(".")
	if statErr != nil {
		return root, lock, db, statErr
	}
	if err := capturedSpoolPrivate(directoryInfo, true); err != nil {
		return root, lock, db, err
	}
	info, err := root.Lstat(filepath.Base(path))
	if err != nil {
		return root, lock, db, fmt.Errorf("existing capture file required: %w", ErrGenerationInput)
	}
	if err := capturedSpoolPrivate(info, false); err != nil {
		return root, lock, db, err
	}
	for _, other := range []*sql.DB{d.receiver, d.funding, d.authority} {
		st, err := generationDatabaseFile(other)
		if err != nil {
			return root, lock, db, fmt.Errorf("capture isolation metadata: %w", err)
		}
		if os.SameFile(st, info) {
			return root, lock, db, fmt.Errorf("capture aliases a runtime database: %w", ErrGenerationInput)
		}
	}
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(500)"}}.Encode()
	db, err = sql.Open("sqlite", u.String())
	if err != nil {
		return root, lock, db, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, table := range []string{"portal_monthly_funding_pages", "portal_monthly_funding_records", "portal_monthly_funding_capture_plans", "portal_monthly_token_capture_slots", "portal_monthly_usage_observations", "portal_monthly_wallet_observations", "portal_monthly_wallet_capture_slots"} {
		var found int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&found); err != nil || found != 1 {
			return root, lock, db, ErrGenerationIntegrity
		}
	}
	actual, err := generationDatabaseFile(db)
	if err != nil || !os.SameFile(actual, info) {
		return root, lock, db, ErrGenerationIntegrity
	}
	if err := automaticPreparationRootCurrent(root, filepath.Dir(path)); err != nil {
		return root, lock, db, err
	}
	return root, lock, db, ctx.Err()
}
