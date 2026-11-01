// monthlybillwithdraw archives and withdraws one reviewed local publication.
// Inspection/dry-run are read-only; changes require -apply and an exact version.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, time.Now()); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer, now time.Time) error {
	fs := flag.NewFlagSet("monthlybillwithdraw", flag.ContinueOnError)
	fs.SetOutput(out)
	path := fs.String("db", "", "absolute path of an existing local Monitor database")
	gid := fs.Int64("group-id", 0, "Monitor customer group ID, not a NewAPI user ID")
	month := fs.String("month", "", "one closed Beijing calendar month, YYYY-MM")
	expected := fs.String("expected-publication", "", "exact inspected publication version")
	actor := fs.String("actor", "", "operator identifier for the retained audit record")
	reason := fs.String("reason", "", "reason for withdrawal/review")
	apply := fs.Bool("apply", false, "archive and withdraw (default: inspect or validate only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || !filepath.IsAbs(*path) || *gid <= 0 || *month == "" {
		return fmt.Errorf("明确指定绝对路径 -db、-group-id 和 -month，不接受额外位置参数")
	}
	if *expected == "" && (*apply || *actor != "" || *reason != "") {
		return fmt.Errorf("撤回核验必须提供 -expected-publication；不提供时只允许查看")
	}
	if *expected != "" && (strings.TrimSpace(*actor) == "" || strings.TrimSpace(*reason) == "") {
		return fmt.Errorf("撤回核验必须提供 -actor 和 -reason")
	}
	st, err := os.Stat(*path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("database path is not a regular file")
	}
	mode := "ro"
	if *apply {
		mode = "rw"
	}
	u := url.URL{Scheme: "file", Path: *path}
	u.RawQuery = url.Values{"mode": {mode}, "_pragma": {"busy_timeout(3000)"}}.Encode()
	db, err := gorm.Open(sqlite.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if *expected == "" {
		version, err := monitor.MonthlyBillPublicationVersion(ctx, db, *gid, *month, now)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "仅查看，未写入\ngroup_id=%d month=%s\npublication=%s\n", *gid, *month, version)
		return err
	}
	if err := monitor.WithdrawMonthlyBill(ctx, db, *gid, *month, *expected, *actor, *reason, *apply, now); err != nil {
		return err
	}
	status := "撤回核验通过，未写入"
	if *apply {
		status = "指定版本已撤回，原账单和原因保留在审计表中"
	}
	_, err = fmt.Fprintf(out, "%s\ngroup_id=%d month=%s\npublication=%s\n", status, *gid, *month, *expected)
	return err
}
