// usage-sql-bench replays bounded, synthetic scalar queries over ONE real Go
// MySQL connection. Run only inside the task-owned network-none container.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type step struct {
	SQL  string
	Args []any
	Want *string
}
type variant struct {
	Name  string
	Steps []step
}
type benchCase struct {
	Name     string
	Variants []variant
}
type input struct{ Cases []benchCase }
type result struct {
	Case, Variant string
	Interpolate   bool
	Statements    int
	US            []int64
}

func execute(ctx context.Context, c *sql.Conn, v variant) error {
	for _, s := range v.Steps {
		rows, e := c.QueryContext(ctx, s.SQL, s.Args...)
		if e != nil {
			return e
		}
		if !rows.Next() {
			e = rows.Err()
			rows.Close()
			if e != nil {
				return e
			}
			return errors.New("missing scalar")
		}
		var got sql.NullString
		if e = rows.Scan(&got); e != nil {
			rows.Close()
			return e
		}
		if rows.Next() {
			rows.Close()
			return errors.New("multiple scalars")
		}
		if e = rows.Err(); e != nil {
			rows.Close()
			return e
		}
		if e = rows.Close(); e != nil {
			return e
		}
		if got.Valid != (s.Want != nil) || got.Valid && got.String != *s.Want {
			return errors.New("captured scalar differs")
		}
	}
	return nil
}

func run() error {
	if runtime.GOOS != "linux" {
		return errors.New("Linux task container required")
	}
	st, e := os.Stat("/var/run/mysqld/mysqld.sock")
	if e != nil || st.Mode()&os.ModeSocket == 0 {
		return errors.New("task MySQL socket required")
	}
	d := json.NewDecoder(io.LimitReader(os.Stdin, 2<<20))
	d.UseNumber()
	d.DisallowUnknownFields()
	var in input
	if e = d.Decode(&in); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing input")
	}
	if len(in.Cases) == 0 || len(in.Cases) > 32 {
		return errors.New("case budget")
	}
	for _, c := range in.Cases {
		if len(c.Variants) == 0 || len(c.Variants) > 4 {
			return errors.New("variant budget")
		}
		for _, v := range c.Variants {
			if len(v.Steps) == 0 || len(v.Steps) > 4 {
				return errors.New("step budget")
			}
			for _, s := range v.Steps {
				if len(s.SQL) > 20000 || len(s.Args) > 64 || !strings.HasPrefix(s.SQL, "SELECT ") || strings.Contains(s.SQL, ";") {
					return errors.New("only bounded scalar SELECT replay allowed")
				}
				for i, a := range s.Args {
					if n, ok := a.(json.Number); ok {
						x, e := n.Int64()
						if e != nil {
							return e
						}
						s.Args[i] = x
					}
				}
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var out []result
	for _, interpolate := range []bool{false, true} {
		cfg := mysql.NewConfig()
		cfg.User = "root"
		cfg.Net = "unix"
		cfg.Addr = "/var/run/mysqld/mysqld.sock"
		cfg.DBName = "sql_usage_perf"
		cfg.InterpolateParams = interpolate
		cfg.Timeout = 2 * time.Second
		cfg.ReadTimeout = 4 * time.Second
		cfg.WriteTimeout = 2 * time.Second
		db, e := sql.Open("mysql", cfg.FormatDSN())
		if e != nil {
			return e
		}
		db.SetMaxOpenConns(1)
		c, e := db.Conn(ctx)
		if e != nil {
			db.Close()
			return e
		}
		for _, bc := range in.Cases {
			start := len(out)
			for _, v := range bc.Variants {
				if e = execute(ctx, c, v); e != nil {
					c.Close()
					db.Close()
					return fmt.Errorf("warmup %s/%s: %w", bc.Name, v.Name, e)
				}
				out = append(out, result{Case: bc.Name, Variant: v.Name, Interpolate: interpolate, Statements: len(v.Steps)})
			}
			for round := 0; round < 15; round++ {
				for j := range bc.Variants {
					i := j
					if round%2 != 0 {
						i = len(bc.Variants) - 1 - j
					}
					v := bc.Variants[i]
					at := time.Now()
					e = execute(ctx, c, v)
					us := time.Since(at).Microseconds()
					if e != nil {
						c.Close()
						db.Close()
						return fmt.Errorf("sample %s/%s: %w", bc.Name, v.Name, e)
					}
					out[start+i].US = append(out[start+i].US, us)
				}
			}
		}
		if e = c.Close(); e != nil {
			db.Close()
			return e
		}
		if e = db.Close(); e != nil {
			return e
		}
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Warning string
		Results []result
	}{"One persistent Go MySQL socket connection per mode; captured actual SQL/args replayed against immutable synthetic fixture; 15 alternating warm samples incl prepare/execute/scan/close and value checks, no CLI subprocess between statements. Not HTTP, cold-cache, WAN or concurrent load. No result/plan caching.", out})
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
