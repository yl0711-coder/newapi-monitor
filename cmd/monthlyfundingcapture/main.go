// monthlyfundingcapture is an explicit operator tool, not application startup.
// It never loads .env or accepts a DSN, and never publishes a customer bill.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

var errInput = errors.New("invalid capture input")
var errBusy = errors.New("capture workspace busy")
var jobName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

type manifest struct {
	Version int                       `json:"version"`
	GroupID int64                     `json:"group_id"`
	Month   string                    `json:"month"`
	Older   monthlysource.FundingScan `json:"older"`
	Newer   monthlysource.FundingScan `json:"newer"`
}

type receipt struct {
	CheckpointHead       string                               `json:"checkpoint_head_sha256,omitempty"`
	NeedsCheckpointCheck bool                                 `json:"needs_checkpoint_check"`
	LocalReady           bool                                 `json:"local_ready_for_capture"`
	BlockedReason        string                               `json:"blocked_reason,omitempty"`
	Mode                 string                               `json:"mode"`
	Created              bool                                 `json:"created"`
	ManifestSHA256       string                               `json:"manifest_sha256"`
	GroupID              int64                                `json:"group_id"`
	Month                string                               `json:"month"`
	Accounts             int                                  `json:"accounts"`
	OlderScanID          string                               `json:"older_scan_id"`
	NewerScanID          string                               `json:"newer_scan_id"`
	Phase                string                               `json:"phase,omitempty"`
	Started              bool                                 `json:"started"`
	EndReached           bool                                 `json:"end_reached"`
	Capture              *monthlysource.FundingCaptureReceipt `json:"capture,omitempty"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, safeError(err))
		os.Exit(1)
	}
}

func safeError(err error) string {
	var transport *observationTransportFailure
	if errors.As(err, &transport) {
		kind := safeSourceFailureClass(transport, "connection_error")
		return "只读采集失败（stage=transport_prepare, class=" + kind + safeSourceFailureDetail(transport, kind) + "）；连接准备未完成、未开始业务查询、未自动重试"
	}
	if stage := monthlysource.FundingFailureStage(err); stage != "" {
		kind := safeSourceFailureClass(err, "source_or_validation_error")
		return "资金只读采集失败（stage=" + stage + ", class=" + kind + safeSourceFailureDetail(err, kind) + "）；未返回部分结果、未放宽限制、未自动重试"
	}
	if stage := monthlysource.TokenUsageFailureStage(err); stage != "" {
		kind := safeSourceFailureClass(err, "source_or_validation_error")
		reason := ""
		if code := monthlysource.TokenUsageFailureReason(err); code != "" {
			reason = ", reason=" + code
		}
		return "令牌只读采集失败（stage=" + stage + ", class=" + kind + safeSourceFailureDetail(err, kind) + reason + "）；未返回部分结果、未放宽限制、未自动重试"
	}
	if errors.Is(err, errBusy) {
		return "采集工作区正在使用；未抢锁、未重试"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "操作取消或超时；请按原清单检查本机断点或装配目录，不覆盖半成品、不另建扫描冒充恢复"
	}
	return "本机采集、核验或装配失败；核对私有文件、清单摘要、范围、断点及输出目录。未自动重试或发布账单"
}

// Diagnose connection failures without printing driver messages, SQL or source
// values. Classification is operator information only: it does not authorize a
// retry, accept partial results or change any query/time/transaction limits.
func safeSourceFailureClass(err error, fallback string) string {
	var sqlError *mysql.MySQLError
	var networkError net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.As(err, &sqlError):
		if sqlError.Number == 3024 {
			return "sql_execution_budget"
		}
		return "mysql_error"
	case errors.Is(err, driver.ErrBadConn), errors.Is(err, mysql.ErrInvalidConn):
		return "invalid_connection"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection_closed"
	case errors.As(err, &networkError):
		if networkError.Timeout() {
			return "network_timeout"
		}
		return "network_error"
	default:
		return fallback
	}
}

func safeSourceFailureDetail(err error, class string) string {
	var sqlError *mysql.MySQLError
	if class != "mysql_error" || !errors.As(err, &sqlError) {
		return ""
	}
	// Keep class a fixed alphabetic code for existing operator parsers. Only
	// the protocol's numeric errno is added, never its free-form message.
	return ", mysql_errno=" + strconv.FormatUint(uint64(sqlError.Number), 10)
}

func run(parent context.Context, args []string, out io.Writer) error {
	ctx, cancel := context.WithTimeout(parent, 40*time.Second)
	defer cancel()
	if len(args) > 0 && args[0] == "assemble-wallet-reuse" {
		return runWalletReuseMaterial(ctx, args[1:], out)
	}
	if len(args) > 0 && args[0] == "inspect-token-wallet" {
		return runTokenWalletReuse(ctx, args[1:], out)
	}
	if len(args) > 0 && args[0] == "assemble-observation-month" {
		return runObservationMonthAssembly(ctx, args[1:], out)
	}
	if len(args) > 0 && (args[0] == "init-observation-month" || args[0] == "inspect-observation-month" || args[0] == "run-observation-month" || args[0] == "plan-observation-month") {
		return runObservationMonth(ctx, args, out)
	}
	if len(args) > 0 && args[0] == "split-usage-window" {
		if err := ctx.Err(); err != nil {
			return err
		}
		return runUsageSplit(args[1:], out)
	}
	if len(args) > 0 && (args[0] == "observe-usage" || args[0] == "observe-wallet" || args[0] == "observe-token-usage") {
		return runObservation(ctx, args, out)
	}
	if len(args) > 0 && (args[0] == "run-month" || args[0] == "plan-month") {
		return runMonthStep(ctx, args, out)
	}
	if len(args) > 0 && (args[0] == "init-month" || args[0] == "inspect-month") {
		return runMonthBatch(ctx, args, out)
	}
	if len(args) > 0 && args[0] == "audit-month" {
		return runMonthAudit(ctx, args[1:], out)
	}
	if len(args) > 0 && args[0] == "assemble-month" {
		return runMonthAssembly(ctx, args[1:], out)
	}
	if len(args) > 0 && args[0] == "assemble-observations" {
		return runObservationAssembly(ctx, args[1:], out)
	}
	if len(args) > 0 && args[0] == "assemble-token-usage" {
		return runTokenMonthAssembly(ctx, args[1:], out)
	}
	if len(args) == 0 || (args[0] != "init" && args[0] != "run") {
		return errInput
	}
	fs := flag.NewFlagSet("monthlyfundingcapture "+args[0], flag.ContinueOnError)
	// Flag parsing errors may echo user-provided secrets/paths. Only main's
	// categorical error reaches stderr; credentials never belong in argv.
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private absolute directory")
	job := fs.String("job", "", "one job name in that workspace")
	if args[0] == "init" {
		group := fs.Int64("group-id", 0, "explicit organization ID")
		month := fs.String("month", "", "one closed Beijing month")
		source := fs.String("source-identity", "", "previously verified source identity SHA256")
		users := fs.String("user-ids", "", "comma-separated exact account IDs")
		from := fs.Int64("from", 0, "inclusive Unix second, within month")
		to := fs.Int64("to", 0, "exclusive Unix second, at most one hour")
		daily := fs.Bool("daily", false, "explicit v2 scan: exactly one UTC+8 day instead of v1 hour")
		create := fs.Bool("create", false, "create a new job; default checks only")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
			return errInput
		}
		ids, err := parseIDs(*users)
		if err != nil {
			return err
		}
		rules := monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}
		newScan := monthlysource.NewFundingScan
		if *daily {
			newScan = monthlysource.NewFundingDayScan
		}
		a, err := newScan(*source, rules, ids, *from, *to)
		if err != nil {
			return errInput
		}
		b, err := newScan(*source, rules, ids, *from, *to)
		if err != nil {
			return errInput
		}
		m := manifest{1, *group, *month, a, b}
		if err := m.validate(time.Now()); err != nil {
			return err
		}
		result, err := initialize(ctx, *workspace, *job, m, *create)
		if err != nil && !result.Created {
			return err
		}
		return errors.Join(err, json.NewEncoder(out).Encode(result))
	}
	expected := fs.String("expected-manifest", "", "exact retained manifest SHA256")
	phase := fs.String("phase", "", "older or newer, explicitly selected")
	connection := fs.String("connection-file", "", "0600 local MySQL connection JSON; read only with -capture")
	capture := fs.Bool("capture", false, "perform bounded SELECTs and append local pages; default inspect only")
	pages := fs.Int("max-pages", 1, "1..10 pages this invocation")
	interval := fs.Duration("interval", 2*time.Second, "1s..10s between pages, including first")
	budget := fs.Duration("budget", 15*time.Second, "1s..30s total runner budget")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || (*phase != "older" && *phase != "newer") || !validSHA(*expected) || *pages < 1 || *pages > 10 || *interval < time.Second || *interval > 10*time.Second || *budget < time.Second || *budget > 30*time.Second || (*capture && *connection == "") {
		return errInput
	}
	limits := monthlysource.FundingCaptureLimits{MaxPages: *pages, Interval: *interval, Timeout: *budget}
	result, err := execute(ctx, *workspace, *job, *expected, *phase, *connection, limits, *capture)
	if result.Mode == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(result))
}

func parseIDs(text string) ([]int64, error) {
	parts := strings.Split(text, ",")
	if len(parts) > 200 {
		return nil, errInput
	}
	ids := make([]int64, 0, len(parts))
	for _, part := range parts {
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil || id <= 0 || strconv.FormatInt(id, 10) != part {
			return nil, errInput
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func validSHA(s string) bool {
	_, err := hex.DecodeString(s)
	return len(s) == 64 && strings.ToLower(s) == s && err == nil
}

func hash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }

func (m manifest) validate(now time.Time) error {
	zone := time.FixedZone("CST", 8*60*60)
	start, err := time.ParseInLocation("2006-01", m.Month, zone)
	end := start.AddDate(0, 1, 0)
	if err != nil || start.Format("2006-01") != m.Month || end.After(now) || m.Version != 1 || m.GroupID <= 0 || monthlysource.ValidateFundingScan(m.Older) != nil || monthlysource.ValidateFundingScan(m.Newer) != nil || m.Older.ID == m.Newer.ID || m.Older.From < start.Unix() || m.Older.To > end.Unix() {
		return errInput
	}
	a, b := m.Older, m.Newer
	b.ID = a.ID
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	if string(x) != string(y) {
		return errInput
	}
	return nil
}

func jobPath(workspace, job string) (string, string, error) {
	if !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace || !jobName.MatchString(job) {
		return "", "", errInput
	}
	if _, err := privateInfo(workspace, true); err != nil {
		return "", "", err
	}
	root, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return "", "", err
	}
	return root, filepath.Join(root, job), nil
}
