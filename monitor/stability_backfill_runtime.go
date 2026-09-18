package monitor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// NewStabilityFactBackfill constructs a least-privilege maintenance runtime.
// It opens the existing main SQLite store without running migrations or
// touching encrypted upstream-account credentials, then connects only to the
// configured read-only NewAPI source. HTTP routes and background workers are
// deliberately outside this command's dependency graph.
func NewStabilityFactBackfill(s Settings) (*Monitor, error) {
	if s.LocalSnapshotOnly || !s.StabilityBackfillEnabled {
		return nil, errors.New("stability backfill requires explicit online read-only sync settings")
	}
	storePath := strings.TrimSpace(s.StorePath)
	if storePath == "" {
		return nil, errors.New("stability backfill requires an existing main store")
	}
	db, err := openExistingStabilityBackfillStore(storePath)
	if err != nil {
		return nil, err
	}
	m := &Monitor{
		cfg:                 s,
		storeDB:             db,
		chNames:             map[string]string{},
		snapCache:           map[snapshotCacheKey]cachedSnap{},
		sourceFailureNotify: make(chan struct{}, 1),
	}
	m.storeIntegrityCheckedAt.Store(time.Now().Unix())
	m.storeIntegrityOK.Store(true)
	m.sourceLifecycleInitialized.Store(true)
	initialized := false
	defer func() {
		if !initialized {
			m.Close()
		}
	}()
	if err := m.initializeSource(); err != nil {
		return nil, err
	}
	initialized = true
	return m, nil
}

func openExistingStabilityBackfillStore(path string) (*gorm.DB, error) {
	exists, err := preflightStoreIntegrity(path)
	if err != nil {
		return nil, fmt.Errorf("stability store integrity preflight: %w", err)
	}
	if !exists {
		return nil, errors.New("stability backfill refuses to create a new main store")
	}
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, fmt.Errorf("open stability store: %w", err)
	}
	closeOnError := func(err error) (*gorm.DB, error) {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		return nil, err
	}
	for _, model := range []any{
		&MetricSample{},
		&StabilityHourSample{},
		&ChannelTestHourSample{},
		&StabilityHourIngestState{},
		&StabilityBackfillJob{},
	} {
		if !db.Migrator().HasTable(model) {
			return closeOnError(fmt.Errorf("stability backfill required table is missing: %T", model))
		}
	}
	sqlDB, err := db.DB()
	if err != nil {
		return closeOnError(err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	return db, nil
}
