package monthlydraft

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// Driver double of the REAL collector's SQL protocol, not a replacement step
// returning invented financial observations. It cannot reach any network.
type sourcePreparationDriver struct {
	queries, pings, writes atomic.Int32
	onQuery                func() error
	registrationAt         map[int64]int64 // optional synthetic originals, immutable before startup
}
type sourcePreparationConn struct{ d *sourcePreparationDriver }
type sourcePreparationTx struct{}
type sourcePreparationRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

var sourcePreparationDriverSequence atomic.Int64

func (d *sourcePreparationDriver) Open(string) (driver.Conn, error) {
	return &sourcePreparationConn{d}, nil
}
func (*sourcePreparationConn) Close() error { return nil }
func (*sourcePreparationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*sourcePreparationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("read-only transaction required")
}
func (*sourcePreparationConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second || !opts.ReadOnly || opts.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) {
		return nil, errors.New("unsafe source transaction")
	}
	return &sourcePreparationTx{}, nil
}
func (c *sourcePreparationConn) Ping(ctx context.Context) error {
	c.d.pings.Add(1)
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 4*time.Second {
		return errors.New("unbounded connection prepare")
	}
	return ctx.Err()
}
func (c *sourcePreparationConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.d.writes.Add(1)
	return nil, errors.New("source write forbidden")
}
func (c *sourcePreparationConn) QueryContext(ctx context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.d.queries.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.d.onQuery != nil {
		if err := c.d.onQuery(); err != nil {
			return nil, err
		}
	}
	if strings.Contains(q, "@@server_uuid") {
		if strings.Contains(q, "MAX(id)") {
			return &sourcePreparationRows{columns: []string{"uuid", "database", "fence"}, values: []driver.Value{"local-source-server", "local-source-db", int64(0)}}, nil
		}
		return &sourcePreparationRows{columns: []string{"uuid", "database"}, values: []driver.Value{"local-source-server", "local-source-db"}}, nil
	}
	if !strings.HasPrefix(q, "SELECT ") || !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") || !strings.Contains(q, "logs") {
		return nil, errors.New("unexpected source query")
	}
	return &sourcePreparationRows{columns: []string{"id", "user_id", "created_at", "type", "content", "other"}}, nil
}
func (*sourcePreparationTx) Commit() error         { return nil }
func (*sourcePreparationTx) Rollback() error       { return nil }
func (r *sourcePreparationRows) Columns() []string { return r.columns }
func (*sourcePreparationRows) Close() error        { return nil }
func (r *sourcePreparationRows) Next(dst []driver.Value) error {
	if r.read || r.values == nil {
		return io.EOF
	}
	copy(dst, r.values)
	r.read = true
	return nil
}

func fundingPreparationFixture(t *testing.T) (generationFixture, *gorm.DB, *LocalRuntime, *FundingSourcePreparation, *sourcePreparationDriver) {
	t.Helper()
	f, auth, ro, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	d := &sourcePreparationDriver{}
	name := fmt.Sprintf("funding-preparation-%d", sourcePreparationDriverSequence.Add(1))
	sql.Register(name, d)
	source, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	source.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = source.Close() })
	capturePath := filepath.Join(t.TempDir(), "private-capture.sqlite")
	capture := generationDB(t, capturePath, false)
	if err := monthlysource.InitFundingMonthCaptureStore(context.Background(), capture); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(capturePath, 0600); err != nil {
		t.Fatal(err)
	}
	pin, err := monthlysource.WalletSourceIdentity("local-source-server", "local-source-db")
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewFundingSourcePreparation(source, capture, f.g, ro, pin)
	if err != nil {
		t.Fatal(err)
	}
	return f, auth, r, p, d
}

func TestFundingSourcePreparationQueueInvokesRealReadOnlyCollector(t *testing.T) {
	f, _, r, p, d := fundingPreparationFixture(t)
	ctx := context.Background()
	d.onQuery = func() error {
		if p.capture.Stats().InUse != 0 || f.receiver.Stats().InUse != 0 {
			return errors.New("SQLite transaction held during source I/O")
		}
		return nil
	}
	nextCalls := 0
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { nextCalls++; return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	if _, err := r.preparation.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if d.queries.Load() != 0 || d.pings.Load() != 0 {
		t.Fatal("HTTP path contacted source")
	}
	if err := r.preparation.runOne(ctx); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 1 || failures != 0 || nextCalls != 0 {
		t.Fatal("partial funding advertised as bill readiness", state, steps, failures, nextCalls)
	}
	if d.queries.Load() != 2 || d.pings.Load() != 1 || d.writes.Load() != 0 {
		t.Fatal("unexpected source work", d.queries.Load(), d.pings.Load(), d.writes.Load())
	}
	var pages int
	if err := p.capture.QueryRow(`SELECT count(*) FROM portal_monthly_funding_pages`).Scan(&pages); err != nil || pages != 1 {
		t.Fatal(pages, err)
	}
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	scope, err := p.scope(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := monthlysource.InspectFundingMonthCapture(ctx, p.capture, req.Key(), scope)
	if err != nil || progress.FinishedScans != 1 || progress.TotalScans != 60 || progress.TraversalsFinished {
		t.Fatal(progress, err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
	if _, err := p.Before(nil); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("missing remaining stage accepted", err)
	}
}

func TestFundingSourcePreparationAuthorityFailureBeforeOrAfterRead(t *testing.T) {
	for _, when := range []string{"before", "during", "members"} {
		t.Run(when, func(t *testing.T) {
			f, auth, _, p, d := fundingPreparationFixture(t)
			req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
			revoke := func() error {
				return auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error
			}
			switch when {
			case "before":
				if err := revoke(); err != nil {
					t.Fatal(err)
				}
			case "during":
				d.onQuery = func() error { d.onQuery = nil; return revoke() }
			case "members":
				if err := auth.Create(&monitor.TrackedUser{UserID: 30001, GroupID: 1}).Error; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := p.Advance(context.Background(), req); !errors.Is(err, ErrGenerationChanged) {
				t.Fatal("changed scope accepted", err)
			}
			if when != "during" && (d.queries.Load() != 0 || d.pings.Load() != 0) {
				t.Fatal("revoked scope reached source")
			}
			if d.writes.Load() != 0 {
				t.Fatal("source written")
			}
		})
	}
}

func TestFundingSourcePreparationFailureUsesQueueBackoff(t *testing.T) {
	f, _, r, p, d := fundingPreparationFixture(t)
	d.onQuery = func() error { return errors.New("private source outage") }
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	if _, err := r.preparation.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 1 || failures != 1 || d.queries.Load() != 1 {
		t.Fatal(state, steps, failures, d.queries.Load())
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.queries.Load() != 1 {
		t.Fatal("retry ignored cooldown")
	}
	var n int
	if err := p.capture.QueryRow(`SELECT count(*) FROM portal_monthly_funding_pages`).Scan(&n); err != nil || n != 0 {
		t.Fatal("failed source advanced capture", n, err)
	}
}

func TestFundingSourcePreparationRejectsDatabaseAliasesAndUnsafePermissions(t *testing.T) {
	f, _, r, p, _ := fundingPreparationFixture(t)
	authDB, err := p.authorization.DB()
	if err != nil {
		t.Fatal(err)
	}
	for _, capture := range []*sql.DB{f.receiver, authDB, p.source, f.funding} {
		if _, err := NewFundingSourcePreparation(p.source, capture, f.g, p.authorization, p.sourceIdentity); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("unsafe capture target", err)
		}
	}
	alias := generationDB(t, f.receiverPath, false)
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFundingSourcePreparation(p.source, alias, f.g, p.authorization, p.sourceIdentity); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("second connection bypassed file isolation", err)
	}
	if _, err := NewFundingSourcePreparation(alias, p.capture, f.g, p.authorization, p.sourceIdentity); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("SQLite handle accepted as a live MySQL source", err)
	}
	unsafePath := filepath.Join(t.TempDir(), "unsafe-permissions.sqlite")
	unsafeCapture := generationDB(t, unsafePath, false)
	if err := monthlysource.InitFundingMonthCaptureStore(context.Background(), unsafeCapture); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafePath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFundingSourcePreparation(p.source, unsafeCapture, f.g, p.authorization, p.sourceIdentity); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("non-private capture accepted", err)
	}
	if _, err := NewFundingSourcePreparation(nil, p.capture, f.g, r.preparation.auth, p.sourceIdentity); !errors.Is(err, ErrGenerationInput) {
		t.Fatal(err)
	}
}
