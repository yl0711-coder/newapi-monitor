package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type connectionConfig struct {
	Network  string `json:"network"`
	Address  string `json:"address"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

func readConnection(path string) (*mysql.Config, error) {
	raw, err := privateRead(path, 16<<10)
	if err != nil {
		return nil, err
	}
	var c connectionConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil || dec.Decode(new(any)) != io.EOF || c.User == "" || c.User == "root" || len(c.User) > 96 || c.Database == "" || len(c.Database) > 64 || len(c.Password) > 8192 {
		return nil, errInput
	}
	switch c.Network {
	case "unix":
		if !filepath.IsAbs(c.Address) || filepath.Clean(c.Address) != c.Address {
			return nil, errInput
		}
	case "tcp":
		host, port, err := net.SplitHostPort(c.Address)
		ip := net.ParseIP(host)
		n, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || n < 1 || n > 65535 || ip == nil || !ip.IsLoopback() {
			return nil, errInput
		}
	default:
		return nil, errInput
	}
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName = c.Network, c.Address, c.User, c.Password, c.Database
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 2*time.Second, 4*time.Second, 2*time.Second
	cfg.MaxAllowedPacket = 2 << 20
	cfg.Logger = log.New(io.Discard, "", 0)
	return cfg, nil
}

func sameScan(a, b monthlysource.FundingScan) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func checkpoint(ctx context.Context, db *sql.DB, scan monthlysource.FundingScan) (*monthlysource.FundingPage, error) {
	p, err := monthlysource.LoadLatestFundingPage(ctx, db, scan.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // Absent, not an observed empty result.
	}
	if err != nil || !sameScan(p.Scan, scan) {
		return nil, errInput
	}
	return &p, nil
}

func execute(ctx context.Context, workspace, job, expected, phase, connection string, limits monthlysource.FundingCaptureLimits, capture bool) (result receipt, err error) {
	root, _, err := jobPath(workspace, job)
	if err != nil {
		return result, err
	}
	lock, err := workspaceLock(root, capture, false)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	return executeLocked(ctx, root, job, expected, phase, connection, limits, capture)
}

// Caller holds the workspace lock from selection through capture and receipt.
func executeLocked(ctx context.Context, workspace, job, expected, phase, connection string, limits monthlysource.FundingCaptureLimits, capture bool) (result receipt, err error) {
	_, path, err := jobPath(workspace, job)
	if err != nil {
		return result, err
	}
	m, raw, err := readManifest(path, expected)
	if err != nil {
		return result, err
	}
	database := filepath.Join(path, "funding.sqlite")
	before, err := privateInfo(database, false)
	if err != nil {
		return result, err
	}
	db, err := openReceiver(ctx, database, capture)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	scan := m.Older
	if phase == "newer" {
		scan = m.Newer
	}
	last, err := checkpoint(ctx, db, scan)
	if err != nil {
		return result, err
	}
	result = manifestReceipt(m, raw)
	result.Mode, result.Phase = "checked_local_checkpoint_no_source_connection", phase
	result.Started, result.EndReached = last != nil, last != nil && last.EndReached
	if last != nil {
		encoded, err := json.Marshal(last)
		if err != nil {
			return result, err
		}
		result.CheckpointHead = hash(encoded)
	}
	if result.EndReached {
		return result, nil
	}
	if phase == "newer" {
		older, err := checkpoint(ctx, db, m.Older)
		if err != nil {
			return result, err
		}
		if older == nil || !older.EndReached || time.Now().UnixMilli() < older.CompletedAtMS {
			result.BlockedReason = "older_scan_not_finished_or_clock_regressed"
			if capture {
				return result, errInput
			}
			return result, nil
		}
		// The runner waits >=1 second before its first query. Observations
		// remain independent; equality still requires the separate audit path.
	}
	result.LocalReady = true // Not a successful connection or coverage assertion.
	if !capture {
		return result, nil
	}
	cfg, err := readConnection(connection)
	if err != nil {
		return result, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return result, err
	}
	source := sql.OpenDB(connector) // No connection until the bounded runner.
	defer func() { err = errors.Join(err, source.Close()) }()
	source.SetMaxOpenConns(1)
	source.SetMaxIdleConns(1)
	// Reserve the source transaction budget for source reads, not a cold
	// SSH/MySQL handshake. Transport setup is separately
	// bounded, single-attempt, and still under the CLI's outer deadline.
	if connectErr := prepareObservationTransport(ctx, source.PingContext); connectErr != nil {
		return result, connectErr
	}
	result.Mode = "bounded_capture_local_observations_not_bill"
	progress, runErr := monthlysource.RunFundingCapture(ctx, source, db, scan, limits)
	result.Capture = &progress
	result.NeedsCheckpointCheck = runErr != nil
	if progress.HeadID != "" {
		result.CheckpointHead = progress.HeadID
	}
	result.Started = result.Started || progress.PagesSaved > 0
	result.EndReached = progress.EndReached
	result.LocalReady = !progress.EndReached
	// A changed file cannot be accepted as the job we opened. Preserve the
	// receipt on failure; an operator must investigate, never silently re-init.
	after, statErr := privateInfo(database, false)
	_, _, manifestErr := readManifest(path, expected)
	if statErr != nil || !os.SameFile(before, after) || manifestErr != nil {
		return result, errors.Join(runErr, errInput)
	}
	return result, runErr
}
