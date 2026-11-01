package monthlydraft

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type walletPreparationDriver struct{ base *sourcePreparationDriver }
type walletPreparationConn struct{ *sourcePreparationConn }
type walletPreparationRows struct {
	data             [][]driver.Value
	index            int
	registration     bool
	registeredWallet bool
}

func (d *walletPreparationDriver) Open(string) (driver.Conn, error) {
	return &walletPreparationConn{&sourcePreparationConn{d: d.base}}, nil
}
func (c *walletPreparationConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "@@server_uuid") {
		return c.sourcePreparationConn.QueryContext(ctx, q, args)
	}
	c.d.queries.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.d.onQuery != nil {
		if err := c.d.onQuery(); err != nil {
			return nil, err
		}
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 3*time.Second || !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") || !strings.Contains(q, "FROM users WHERE id IN") {
		return nil, errors.New("unsafe wallet query")
	}
	rows := &walletPreparationRows{registration: strings.Contains(q, "id,created_at FROM users"), registeredWallet: strings.Contains(q, "id,created_at,quota,used_quota FROM users")}
	for _, arg := range args {
		if rows.registration || rows.registeredWallet {
			at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
			if c.d.registrationAt != nil {
				at = c.d.registrationAt[arg.Value.(int64)]
			}
			row := []driver.Value{arg.Value, at}
			if rows.registeredWallet {
				row = append(row, int64(0), int64(0))
			}
			rows.data = append(rows.data, row)
		} else {
			rows.data = append(rows.data, []driver.Value{arg.Value, int64(0), int64(0)})
		}
	}
	return rows, nil
}
func (r *walletPreparationRows) Columns() []string {
	if r.registeredWallet {
		return []string{"id", "created_at", "quota", "used_quota"}
	}
	if r.registration {
		return []string{"id", "created_at"}
	}
	return []string{"id", "quota", "used_quota"}
}
func (*walletPreparationRows) Close() error { return nil }
func (r *walletPreparationRows) Next(dst []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	copy(dst, r.data[r.index])
	r.index++
	return nil
}

func walletPreparationFixture(t *testing.T) (generationFixture, *gorm.DB, *LocalRuntime, *WalletSourcePreparation, *sourcePreparationDriver) {
	t.Helper()
	f, auth, r, funding, d := fundingPreparationFixture(t)
	name := fmt.Sprintf("wallet-preparation-%d", sourcePreparationDriverSequence.Add(1))
	sql.Register(name, &walletPreparationDriver{base: d})
	source, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	source.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = source.Close() })
	funding, err = NewFundingSourcePreparation(source, funding.capture, f.g, funding.authorization, funding.sourceIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitWalletMonthCaptureStore(context.Background(), funding.capture); err != nil {
		t.Fatal(err)
	}
	p, err := NewWalletSourcePreparation(funding)
	if err != nil {
		t.Fatal(err)
	}
	return f, auth, r, p, d
}

func TestWalletSourcePreparationQueueWaitNoPrematureReadiness(t *testing.T) {
	f, _, r, p, d := walletPreparationFixture(t)
	calls := 0
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	d.onQuery = func() error {
		if f.receiver.Stats().InUse != 0 || p.funding.capture.Stats().InUse != 0 {
			return errors.New("SQLite held during source query")
		}
		return nil
	}
	if _, err := r.preparation.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if d.queries.Load() != 0 {
		t.Fatal("HTTP read source")
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 1 || failures != 0 || calls != 0 || d.queries.Load() != 2 || d.writes.Load() != 0 {
		t.Fatal(state, steps, failures, calls, d.queries.Load())
	}
	// A pure wait next time must not add source work or failed attempts.
	if _, err := r.preparation.db.Exec(`UPDATE portal_monthly_source_preparation SET next_at=0`); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, steps, failures = preparationState(t, f.receiver)
	if steps != 1 || failures != 0 || d.queries.Load() != 2 || calls != 0 {
		t.Fatal("wait spent work/failure budget", steps, failures, calls)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestWalletSourcePreparationAuthorityAndBackoff(t *testing.T) {
	for _, scenario := range []string{"before", "during", "outage"} {
		t.Run(scenario, func(t *testing.T) {
			f, auth, r, p, d := walletPreparationFixture(t)
			req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
			switch scenario {
			case "before":
				if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
					t.Fatal(err)
				}
			case "during":
				d.onQuery = func() error {
					d.onQuery = nil
					return auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error
				}
			case "outage":
				d.onQuery = func() error { return errors.New("private wallet outage") }
			}
			if scenario != "outage" {
				if _, err := p.Advance(context.Background(), req); !errors.Is(err, ErrGenerationChanged) {
					t.Fatal(err)
				}
				if scenario == "before" && d.queries.Load() != 0 {
					t.Fatal("revoked scope reached source")
				}
				return
			}
			step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
			if err != nil {
				t.Fatal(err)
			}
			r.preparation.step = step
			if _, err := r.preparation.SubmitMonthly(context.Background(), 1, 1, req.Month); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := r.preparation.runOne(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			state, steps, failures := preparationState(t, f.receiver)
			if state != "queued" || steps != 1 || failures != 1 || d.queries.Load() != 1 {
				t.Fatal(state, steps, failures, d.queries.Load())
			}
		})
	}
}

func TestTokenSourcePreparationLastFirstSweepCaptureCountsAsWork(t *testing.T) {
	f, _, r, p, d := tokenPreparationFixture(t)
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	scope, err := p.funding.scope(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(context.Background(), p.funding.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	for ordinal := 0; ordinal < 59; ordinal++ {
		scan := plan.Scans[ordinal/len(scope.Funding.UserIDs)]
		uid := scope.Funding.UserIDs[ordinal%len(scope.Funding.UserIDs)]
		at := time.Now().Add(-time.Hour).UnixMilli() + int64(ordinal)
		o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary",
			Request:     monthlysource.UsageWindowRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: uid, From: scan.From, To: scan.To},
			StartedAtMS: at, CompletedAtMS: at, Wallet: monthlysource.WalletUser{UserID: uid, Exists: true, BalanceQuota: &zero, UsedQuota: &zero}, Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
		u, err := monthlysource.UsageObservationFromToken(o)
		if err != nil {
			t.Fatal(err)
		}
		usageID, err := monthlysource.SaveUsageWindowObservation(context.Background(), p.funding.capture, u)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.funding.capture.Exec(`INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, req.Key(), ordinal, uid, generationHash(raw), usageID, at, at, raw); err != nil {
			t.Fatal(err)
		}
	}
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) {
		t.Error("incomplete token month reached next stage")
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	if _, err := r.preparation.SubmitMonthly(context.Background(), 1, 1, req.Month); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, steps, failures := preparationState(t, f.receiver)
	if steps != 1 || failures != 0 || d.queries.Load() != 4 {
		t.Fatal("real capture counted as pure wait", steps, failures, d.queries.Load())
	}
	if _, err := r.preparation.db.Exec(`UPDATE portal_monthly_source_preparation SET next_at=0`); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, steps, failures = preparationState(t, f.receiver)
	if steps != 1 || failures != 0 || d.queries.Load() != 4 {
		t.Fatal("pure wait spent source budget", steps, failures, d.queries.Load())
	}
}

func TestWalletSourcePreparationWarmFirstAllowsUsefulWorkWhileWaiting(t *testing.T) {
	f, auth, _, p, d := walletPreparationFixture(t)
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	calls := 0
	work := func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return false, nil }
	warm, err := p.WarmFirst(work)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warm(context.Background(), req); err != nil || calls != 0 || d.queries.Load() != 2 {
		t.Fatal("warmup crossed a source stage", calls, err)
	}
	// Reconstructing the adapter cannot lose the original first view. It must
	// advance useful work rather than block another five minutes or recapture.
	restarted, err := NewWalletSourcePreparation(p.funding)
	if err != nil {
		t.Fatal(err)
	}
	warm, err = restarted.WarmFirst(work)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warm(context.Background(), req); err != nil || calls != 1 || d.queries.Load() != 2 {
		t.Fatal("warmup blocked useful work or recaptured", calls, err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := warm(context.Background(), req); !errors.Is(err, ErrGenerationChanged) || calls != 1 {
		t.Fatal("warmup bypassed authority", calls, err)
	}
}

func TestWalletSourcePreparationAssemblyCannotApproveMissingOrRevokedSources(t *testing.T) {
	f, auth, _, p, d := walletPreparationFixture(t)
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	scope, err := p.funding.scope(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := monthlysource.EnsureFundingMonthCapture(context.Background(), p.funding.capture, req.Key(), scope); err != nil {
		t.Fatal(err)
	}
	out, err := p.AssembleCapturedSources(context.Background(), req)
	if !errors.Is(err, ErrGenerationEvidence) || len(out.Selection) != 0 || len(out.Tokens) != 0 || d.queries.Load() != 0 {
		t.Fatal("incomplete sources approved or recaptured", out, err)
	}
	p.funding.capture.SetMaxOpenConns(1)
	held, err := p.funding.capture.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	out, err = p.AssembleCapturedSources(ctx, req)
	cancel()
	if closeErr := held.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrGenerationEvidence) || len(out.Selection) != 0 {
		t.Fatal("temporary contention marked as bad money", err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := p.AssembleCapturedSources(context.Background(), req); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("revoked scope assembled", err)
	}
}
