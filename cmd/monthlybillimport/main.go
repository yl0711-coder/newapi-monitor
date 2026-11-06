// monthlybillimport is a local, operator-only publication tool. Dry-run default.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	dbPath := flag.String("db", "", "absolute path of an existing local Monitor database")
	input := flag.String("evidence", "", "absolute path of reviewed monthly evidence JSON")
	apply := flag.Bool("apply", false, "publish to the local database (default: validate only)")
	flag.Parse()
	fail := func(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	if !filepath.IsAbs(*dbPath) || !filepath.IsAbs(*input) {
		fail(fmt.Errorf("-db and -evidence must be absolute local paths"))
	}
	for _, p := range []string{*dbPath, *input} {
		st, err := os.Stat(p)
		if err != nil {
			fail(err)
		}
		if !st.Mode().IsRegular() {
			fail(fmt.Errorf("path is not a regular file"))
		}
	}
	f, err := os.Open(*input)
	if err != nil {
		fail(err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (2<<20)+1))
	if err != nil {
		fail(err)
	}
	mode := "ro"
	if *apply {
		mode = "rw"
	}
	u := url.URL{Scheme: "file", Path: *dbPath}
	q := url.Values{"mode": {mode}, "_pragma": {"busy_timeout(3000)"}}
	u.RawQuery = q.Encode()
	db, err := gorm.Open(sqlite.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		fail(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		fail(err)
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	digest, err := monitor.ImportMonthlyBill(ctx, db, raw, *apply, time.Now())
	if err != nil {
		fail(err)
	}
	status := "核验通过，未写入"
	if *apply {
		status = "已发布（同一证据重复导入为幂等操作）"
	}
	fmt.Printf("%s\nevidence_sha256=%s\n", status, digest)
}
