// Command local-facts-loader creates a deterministic, synthetic NewAPI source
// database for local acceptance. It has hard safety checks and refuses every
// non-loopback host, database name, or account.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	requiredDatabase = "newapi_local_acceptance"
	requiredUser     = "local_loader"
	requiredConfirm  = "LOAD_SYNTHETIC_LOCAL_DB"
	candidateIndex   = "idx_logs_user_created_type"
)

type options struct {
	dsn               string
	confirm           string
	report            string
	users             int
	tracked           int
	days              int
	trackedLogsPerDay int
	backgroundDays    int
	backgroundPerDay  int
	benchmarkIndex    bool
	keepCandidate     bool
	internalHost      string
	probeRows         int
}

type latencySummary struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	MaxMS   float64 `json:"max_ms"`
	Rows    int     `json:"result_rows"`
	Plan    string  `json:"explain_analyze"`
}

type loaderReport struct {
	GeneratedAt                 string         `json:"generated_at"`
	SafetyHost                  string         `json:"safety_host"`
	Database                    string         `json:"database"`
	Users                       int            `json:"users"`
	TrackedUsers                int            `json:"tracked_users"`
	Logs                        int64          `json:"logs"`
	RangeStart                  int64          `json:"range_start"`
	RangeEnd                    int64          `json:"range_end"`
	BenchmarkHour               int64          `json:"benchmark_hour"`
	LoadSeconds                 float64        `json:"load_seconds"`
	CurrentIndexQuery           latencySummary `json:"current_index_query"`
	CandidateIndexQuery         latencySummary `json:"candidate_index_query"`
	CurrentIndexBytes           int64          `json:"current_index_bytes"`
	CandidateIndexBytes         int64          `json:"candidate_index_bytes"`
	CandidateIndexBuildMS       float64        `json:"candidate_index_build_ms"`
	CurrentProbeInsertMS        float64        `json:"current_probe_insert_ms"`
	CandidateProbeInsertMS      float64        `json:"candidate_probe_insert_ms"`
	CandidateIndexKept          bool           `json:"candidate_index_kept"`
	WriteProbeRows              int            `json:"write_probe_rows"`
	SyntheticOnly               bool           `json:"synthetic_only"`
	SourceAggregationSQLBatches int            `json:"source_aggregation_sql_member_batch"`
}

func main() {
	var opt options
	flag.StringVar(&opt.dsn, "dsn", "local_loader:local-facts-loader-only@tcp(127.0.0.1:13316)/newapi_local_acceptance?charset=utf8mb4&parseTime=true&timeout=5s", "strictly local loader DSN")
	flag.StringVar(&opt.confirm, "confirm-local", "", "must equal "+requiredConfirm)
	flag.StringVar(&opt.report, "report", "", "write JSON result to this local path")
	flag.IntVar(&opt.users, "users", 10000, "synthetic users")
	flag.IntVar(&opt.tracked, "tracked", 200, "tracked users")
	flag.IntVar(&opt.days, "days", 366, "tracked history days")
	flag.IntVar(&opt.trackedLogsPerDay, "tracked-logs-per-day", 4, "logs per tracked user/day")
	flag.IntVar(&opt.backgroundDays, "background-days", 30, "background history days")
	flag.IntVar(&opt.backgroundPerDay, "background-logs-per-day", 4, "logs per background user/day")
	flag.BoolVar(&opt.benchmarkIndex, "benchmark-index", true, "benchmark the local candidate source index")
	flag.BoolVar(&opt.keepCandidate, "keep-candidate-index", false, "leave candidate index in the local synthetic DB")
	flag.StringVar(&opt.internalHost, "internal-container-host", "", "exact nxmon-facts-mysql-* host allowed only inside an isolated Docker network")
	flag.IntVar(&opt.probeRows, "write-probe-rows", 50000, "rows copied into each temporary write-amplification probe")
	flag.Parse()
	if err := run(opt); err != nil {
		fmt.Fprintln(os.Stderr, "local-facts-loader:", err)
		os.Exit(1)
	}
}

func run(opt options) error {
	if opt.confirm != requiredConfirm {
		return fmt.Errorf("refusing destructive local load without --confirm-local=%s", requiredConfirm)
	}
	parsed, host, err := validateLocalDSN(opt.dsn, opt.internalHost)
	if err != nil {
		return err
	}
	if opt.users < 1 || opt.users > 100000 || opt.tracked < 1 || opt.tracked > 500 || opt.tracked > opt.users {
		return fmt.Errorf("invalid user counts users=%d tracked=%d", opt.users, opt.tracked)
	}
	if opt.days < 1 || opt.days > 366 || opt.backgroundDays < 1 || opt.backgroundDays > opt.days ||
		opt.trackedLogsPerDay < 1 || opt.trackedLogsPerDay > 24 || opt.backgroundPerDay < 1 || opt.backgroundPerDay > 24 {
		return errors.New("invalid synthetic date/log cardinality")
	}
	if opt.probeRows < 1000 || opt.probeRows > 100000 {
		return fmt.Errorf("invalid write probe rows=%d", opt.probeRows)
	}
	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect strictly local MySQL: %w", err)
	}
	var database, currentUser string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE(), CURRENT_USER()").Scan(&database, &currentUser); err != nil {
		return err
	}
	if database != requiredDatabase || !strings.HasPrefix(currentUser, requiredUser+"@") {
		return fmt.Errorf("server identity changed: database=%q user=%q", database, currentUser)
	}

	loadStarted := time.Now()
	for _, table := range []string{"logs", "tokens", "users", "channels"} {
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncate local %s: %w", table, err)
		}
	}
	for _, table := range []string{"logs_write_probe_current", "logs_write_probe_candidate"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop stale local probe %s: %w", table, err)
		}
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE logs DROP INDEX "+candidateIndex); err != nil && !mysqlErrorCode(err, 1091) {
		return fmt.Errorf("drop stale local candidate index: %w", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO channels(id,type,status,name,`group`,models,base_url) VALUES "+
		"(1,1,1,'local-channel-1','default','model-1,model-2','http://127.0.0.1'),"+
		"(2,1,1,'local-channel-2','default','model-3,model-4','http://127.0.0.1')"); err != nil {
		return err
	}
	if err := insertUsersAndTokens(ctx, db, opt.users); err != nil {
		return err
	}
	end := time.Now().Add(-10 * time.Minute).Truncate(time.Hour).Unix()
	start := end - int64(opt.days)*86400
	logs, err := insertSyntheticLogs(ctx, db, opt, start, end)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "ANALYZE TABLE logs"); err != nil {
		return err
	}
	report := loaderReport{
		GeneratedAt: time.Now().Format(time.RFC3339), SafetyHost: host, Database: database,
		Users: opt.users, TrackedUsers: opt.tracked, Logs: logs, RangeStart: start, RangeEnd: end,
		LoadSeconds: time.Since(loadStarted).Seconds(), SyntheticOnly: true,
		SourceAggregationSQLBatches: 200, WriteProbeRows: opt.probeRows,
	}
	benchmarkHour, err := busiestTrackedHour(ctx, db, opt.tracked, start, end)
	if err != nil {
		return err
	}
	report.BenchmarkHour = benchmarkHour
	report.CurrentIndexBytes, _ = tableIndexBytes(ctx, db, "logs")
	report.CurrentIndexQuery, err = benchmarkSourceHour(ctx, db, opt.tracked, benchmarkHour, 25)
	if err != nil {
		return err
	}
	if opt.benchmarkIndex {
		report.CurrentProbeInsertMS, err = benchmarkWriteProbe(ctx, db, "logs_write_probe_current", opt.probeRows)
		if err != nil {
			return err
		}
		started := time.Now()
		if _, err := db.ExecContext(ctx, "ALTER TABLE logs ADD INDEX "+candidateIndex+" (user_id,created_at,type)"); err != nil {
			return err
		}
		report.CandidateIndexBuildMS = float64(time.Since(started).Microseconds()) / 1000
		if _, err := db.ExecContext(ctx, "ANALYZE TABLE logs"); err != nil {
			return err
		}
		report.CandidateIndexBytes, _ = tableIndexBytes(ctx, db, "logs")
		report.CandidateIndexQuery, err = benchmarkSourceHour(ctx, db, opt.tracked, benchmarkHour, 25)
		if err != nil {
			return err
		}
		report.CandidateProbeInsertMS, err = benchmarkWriteProbe(ctx, db, "logs_write_probe_candidate", opt.probeRows)
		if err != nil {
			return err
		}
		if !opt.keepCandidate {
			if _, err := db.ExecContext(ctx, "ALTER TABLE logs DROP INDEX "+candidateIndex); err != nil {
				return err
			}
		} else {
			report.CandidateIndexKept = true
		}
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if opt.report != "" {
		if err := os.WriteFile(opt.report, append(encoded, '\n'), 0o644); err != nil {
			return err
		}
	}
	fmt.Println(string(encoded))
	return nil
}

func validateLocalDSN(raw, internalHost string) (*mysql.Config, string, error) {
	cfg, err := mysql.ParseDSN(strings.TrimSpace(raw))
	if err != nil {
		return nil, "", err
	}
	if cfg.User != requiredUser || cfg.DBName != requiredDatabase || cfg.Net != "tcp" {
		return nil, "", fmt.Errorf("refusing DSN user=%q db=%q net=%q", cfg.User, cfg.DBName, cfg.Net)
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, "", fmt.Errorf("DSN must contain an explicit loopback host:port: %w", err)
	}
	trimmed := strings.Trim(host, "[]")
	if trimmed == "localhost" {
		return cfg, trimmed, nil
	}
	ip := net.ParseIP(trimmed)
	if ip != nil && ip.IsLoopback() {
		return cfg, trimmed, nil
	}
	// OrbStack/Docker 的 internal network 可以完全禁止容器出网，但它也
	// 不向宿主机发布回环端口。因此允许加载器与 MySQL 同网运行，
	// 但只接受命令行再次给出的精确临时容器名，且名称必须带有
	// 本工具的专用前缀。任意内网 DNS、IP 或生产主机名仍会拒绝。
	internalHost = strings.TrimSpace(internalHost)
	if internalHost != "" && trimmed == internalHost && strings.HasPrefix(internalHost, "nxmon-facts-mysql-") {
		return cfg, trimmed, nil
	}
	return nil, "", fmt.Errorf("refusing non-loopback MySQL host %q", host)
}

func insertUsersAndTokens(ctx context.Context, db *sql.DB, users int) error {
	for first := 1; first <= users; first += 1000 {
		last := min(first+1000, users+1)
		var userSQL strings.Builder
		var tokenSQL strings.Builder
		userSQL.WriteString("INSERT INTO users(id,username,email,quota,used_quota) VALUES ")
		tokenSQL.WriteString("INSERT INTO tokens(id,user_id,\u0060key\u0060,name,used_quota,\u0060group\u0060) VALUES ")
		userArgs := make([]any, 0, (last-first)*5)
		tokenArgs := make([]any, 0, (last-first)*6)
		for id := first; id < last; id++ {
			if id > first {
				userSQL.WriteByte(',')
				tokenSQL.WriteByte(',')
			}
			userSQL.WriteString("(?,?,?,?,?)")
			tokenSQL.WriteString("(?,?,?,?,?,?)")
			userArgs = append(userArgs, id, fmt.Sprintf("acceptance-user-%04d", id), fmt.Sprintf("user-%04d@local.test", id), 50_000_000, id*1000)
			tokenArgs = append(tokenArgs, id, id, fmt.Sprintf("local-key-%08d", id), fmt.Sprintf("token-%d-1", id), id*100, "group-1")
		}
		if _, err := db.ExecContext(ctx, userSQL.String(), userArgs...); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, tokenSQL.String(), tokenArgs...); err != nil {
			return err
		}
	}
	return nil
}

func insertSyntheticLogs(ctx context.Context, db *sql.DB, opt options, start, end int64) (int64, error) {
	batch := make([][]any, 0, 1000)
	var inserted int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		var q strings.Builder
		q.WriteString("INSERT INTO logs(user_id,created_at,type,content,username,token_name,model_name,quota,prompt_tokens,completion_tokens,channel_id,token_id,\u0060group\u0060,request_id) VALUES ")
		args := make([]any, 0, len(batch)*14)
		for i, row := range batch {
			if i > 0 {
				q.WriteByte(',')
			}
			q.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
			args = append(args, row...)
		}
		if _, err := db.ExecContext(ctx, q.String(), args...); err != nil {
			return err
		}
		inserted += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	appendLog := func(userID int, created int64, seq int64) error {
		logType := 2
		quota := int64(1000 + userID%13)
		if seq%97 == 0 {
			logType = 6
			quota = 100
		}
		dimension := int(seq % 4)
		batch = append(batch, []any{
			userID, created, logType, fmt.Sprintf("synthetic-local-detail-%06d", seq%1000000),
			fmt.Sprintf("acceptance-user-%04d", userID), fmt.Sprintf("token-%d-%d", userID, dimension+1),
			fmt.Sprintf("model-%d", dimension+1), quota, 10 + userID%5 + dimension, 2 + dimension,
			dimension%2 + 1, userID*10 + dimension + 1, fmt.Sprintf("group-%d", dimension%2+1),
			fmt.Sprintf("local-request-%012d", seq),
		})
		if len(batch) == cap(batch) {
			return flush()
		}
		return nil
	}
	var seq int64
	for day := 0; day < opt.days; day++ {
		dayStart := start + int64(day)*86400
		for userID := 1; userID <= opt.tracked; userID++ {
			for n := 0; n < opt.trackedLogsPerDay; n++ {
				seq++
				created := dayStart + int64((userID*37+n*211)%86400)
				if err := appendLog(userID, created, seq); err != nil {
					return inserted, err
				}
			}
		}
	}
	backgroundStart := end - int64(opt.backgroundDays)*86400
	for day := 0; day < opt.backgroundDays; day++ {
		dayStart := backgroundStart + int64(day)*86400
		for userID := opt.tracked + 1; userID <= opt.users; userID++ {
			for n := 0; n < opt.backgroundPerDay; n++ {
				seq++
				created := dayStart + int64((userID*53+n*307)%86400)
				if err := appendLog(userID, created, seq); err != nil {
					return inserted, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return inserted, err
	}
	return inserted, nil
}

func benchmarkSourceHour(ctx context.Context, db *sql.DB, tracked int, hour int64, repetitions int) (latencySummary, error) {
	ids := min(tracked, 200)
	placeholders := strings.TrimSuffix(strings.Repeat("?,", ids), ",")
	q := "SELECT COALESCE(user_id,0),COALESCE(channel_id,0),COALESCE(`group`,''),COALESCE(model_name,'')," +
		"COALESCE(token_id,0),COALESCE(MAX(token_name),'')," +
		"SUM(CASE WHEN type=2 THEN 1 ELSE 0 END),SUM(CASE WHEN type=6 THEN 1 ELSE 0 END)," +
		"SUM(CASE WHEN type=2 THEN COALESCE(prompt_tokens,0) ELSE 0 END)," +
		"SUM(CASE WHEN type=2 THEN COALESCE(completion_tokens,0) ELSE 0 END)," +
		"SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END),SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END) " +
		"FROM logs WHERE type IN (2,6) AND created_at>=? AND created_at<? AND user_id IN (" + placeholders + ") " +
		"GROUP BY user_id,channel_id,`group`,model_name,token_id LIMIT 50001"
	args := make([]any, 0, ids+2)
	args = append(args, hour, hour+3600)
	for id := 1; id <= ids; id++ {
		args = append(args, id)
	}
	latencies := make([]time.Duration, 0, repetitions)
	resultRows := 0
	for i := 0; i < repetitions; i++ {
		started := time.Now()
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return latencySummary{}, err
		}
		count := 0
		for rows.Next() {
			var values [12]any
			var stringsOut [6]sql.NullString
			var intsOut [6]sql.NullInt64
			for j := range stringsOut {
				values[j] = &stringsOut[j]
				values[j+6] = &intsOut[j]
			}
			if err := rows.Scan(values[:]...); err != nil {
				rows.Close()
				return latencySummary{}, err
			}
			count++
		}
		if err := rows.Close(); err != nil {
			return latencySummary{}, err
		}
		resultRows = count
		latencies = append(latencies, time.Since(started))
	}
	plan := ""
	if row := db.QueryRowContext(ctx, "EXPLAIN ANALYZE "+q, args...); row != nil {
		_ = row.Scan(&plan)
	}
	return summarizeLatencies(latencies, resultRows, plan), nil
}

func busiestTrackedHour(ctx context.Context, db *sql.DB, tracked int, start, end int64) (int64, error) {
	var hour int64
	err := db.QueryRowContext(ctx, `SELECT (created_at DIV 3600)*3600 AS hour_ts
 FROM logs WHERE user_id BETWEEN 1 AND ? AND created_at >= ? AND created_at < ?
 GROUP BY hour_ts ORDER BY COUNT(*) DESC, hour_ts DESC LIMIT 1`, tracked, start, end).Scan(&hour)
	if err != nil {
		return 0, fmt.Errorf("find representative tracked hour: %w", err)
	}
	return hour, nil
}

func summarizeLatencies(values []time.Duration, rows int, plan string) latencySummary {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return latencySummary{}
	}
	ms := func(v time.Duration) float64 { return float64(v.Microseconds()) / 1000 }
	p95 := (len(values)*95 + 99) / 100
	if p95 > len(values) {
		p95 = len(values)
	}
	return latencySummary{
		Samples: len(values), P50MS: ms(values[len(values)/2]), P95MS: ms(values[p95-1]),
		MaxMS: ms(values[len(values)-1]), Rows: rows, Plan: plan,
	}
}

func tableIndexBytes(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var bytes sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT index_length FROM information_schema.tables
 WHERE table_schema=? AND table_name=?`, requiredDatabase, table).Scan(&bytes)
	return bytes.Int64, err
}

func benchmarkWriteProbe(ctx context.Context, db *sql.DB, name string, rows int) (float64, error) {
	if name != "logs_write_probe_current" && name != "logs_write_probe_candidate" {
		return 0, errors.New("invalid local probe table")
	}
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+name)
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+name+" LIKE logs"); err != nil {
		return 0, err
	}
	defer func() {
		if _, dropErr := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+name); dropErr != nil {
			fmt.Fprintf(os.Stderr, "warning: drop local probe table %s: %v\n", name, dropErr)
		}
	}()
	started := time.Now()
	_, err := db.ExecContext(ctx, "INSERT INTO "+name+" SELECT * FROM logs ORDER BY id LIMIT ?", rows)
	return float64(time.Since(started).Microseconds()) / 1000, err
}

func mysqlErrorCode(err error, code uint16) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == code
}
