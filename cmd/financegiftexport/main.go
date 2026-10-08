// financegiftexport is an explicit readonly evidence tool, never a service.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Getenv)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "readonly large-hour export:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	f := flag.NewFlagSet("financegiftexport", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	planPath := f.String("plan", "", "private large-hour read plan")
	confirm := f.String("confirm-read-plan", "", "exact reviewed plan SHA256")
	epoch := f.String("source-epoch", "", "separately verified Monitor source epoch")
	database := f.String("expected-database", "", "separately verified source database name")
	uuid := f.String("expected-server-uuid", "", "separately verified MySQL server UUID")
	output := f.String("output", "", "absolute new private evidence file")
	allow := f.Bool("allow-source-read", false, "explicit approval to read this confirmed target")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !*allow || *planPath == "" || *confirm == "" || *epoch == "" || *database == "" || *uuid == "" || !filepath.IsAbs(*output) {
		return errors.New("require plan, confirmation, source-epoch, expected-database, expected-server-uuid, absolute output and allow-source-read")
	}
	plan, digest, err := financegiftexport.ReadLargeHourPlan(*planPath)
	if err != nil {
		return err
	}
	if digest != *confirm || plan.SourceEpoch != *epoch {
		return errors.New("plan confirmation or source epoch mismatch")
	}
	if _, err := os.Lstat(*output); !os.IsNotExist(err) {
		return errors.New("output already exists or cannot be checked")
	}
	cfg, err := readonlyConfig(getenv("MONITOR_GIFT_EXPORT_DSN"), *database)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return errors.New("cannot open readonly source")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	identity := financegiftexport.SourceIdentity{Database: *database, ServerUUID: *uuid}
	identityCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = financegiftexport.VerifySourceIdentity(identityCtx, db, identity)
	cancel()
	if err != nil {
		return err
	}
	if err := financegiftexport.ExportVerifiedLargeHour(ctx, db, plan, digest, *output, identity); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]any{"status": "complete", "mode": "readonly_evidence_only_no_repair", "file": *output, "rows": len(plan.Rows), "confirm_read_plan_sha256": digest})
}

func readonlyConfig(dsn, database string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.User != "monitor_ro" || cfg.Net != "tcp" || cfg.DBName != database || cfg.MultiStatements || cfg.AllowAllFiles || len(cfg.Params) != 0 {
		return nil, errors.New("require monitor_ro loopback TCP DSN for the expected database, without multi-statements, file access or session overrides")
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("source DSN must use a literal loopback SSH tunnel or isolated test address")
	}
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 3*time.Second, 8*time.Second, 3*time.Second
	return cfg, nil
}
