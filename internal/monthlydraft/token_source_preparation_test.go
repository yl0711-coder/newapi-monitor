package monthlydraft

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type tokenPreparationDriver struct{ base *sourcePreparationDriver }
type tokenPreparationConn struct{ *sourcePreparationConn }

func (d *tokenPreparationDriver) Open(string) (driver.Conn, error) {
	return &tokenPreparationConn{&sourcePreparationConn{d: d.base}}, nil
}
func (c *tokenPreparationConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
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
	if strings.Contains(q, "FROM users WHERE id=?") {
		if len(args) != 1 {
			return nil, errors.New("wrong wallet scope")
		}
		return &sourcePreparationRows{columns: []string{"id", "quota", "used_quota"}, values: []driver.Value{args[0].Value, int64(0), int64(0)}}, nil
	}
	if !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") || !strings.Contains(q, "logs") || len(args) != 3 {
		return nil, errors.New("unexpected token source query")
	}
	columns := []string{"user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}
	if strings.Contains(q, "token_id") {
		columns = append(columns, "token_id", "name")
	}
	return &sourcePreparationRows{columns: columns}, nil
}

func tokenPreparationFixture(t *testing.T) (generationFixture, *gorm.DB, *LocalRuntime, *TokenSourcePreparation, *sourcePreparationDriver) {
	t.Helper()
	f, auth, r, funding, d := fundingPreparationFixture(t)
	name := fmt.Sprintf("token-preparation-%d", sourcePreparationDriverSequence.Add(1))
	sql.Register(name, &tokenPreparationDriver{base: d})
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
	if err := monthlysource.InitTokenMonthCaptureStore(context.Background(), funding.capture); err != nil {
		t.Fatal(err)
	}
	p, err := NewTokenSourcePreparation(funding)
	if err != nil {
		t.Fatal(err)
	}
	return f, auth, r, p, d
}

func TestTokenSourcePreparationQueueOneSnapshotAndNoApproval(t *testing.T) {
	f, _, r, p, d := tokenPreparationFixture(t)
	nextCalls := 0
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { nextCalls++; return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	d.onQuery = func() error {
		if p.funding.capture.Stats().InUse != 0 || f.receiver.Stats().InUse != 0 {
			return errors.New("SQLite transaction held during source I/O")
		}
		return nil
	}
	if _, err := r.preparation.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if d.queries.Load() != 0 {
		t.Fatal("HTTP contacted source")
	}
	if err := r.preparation.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 1 || failures != 0 || nextCalls != 0 || d.queries.Load() != 4 || d.writes.Load() != 0 {
		t.Fatal(state, steps, failures, nextCalls, d.queries.Load())
	}
	var originals, projections int
	if err := p.funding.capture.QueryRow(`SELECT count(*) FROM portal_monthly_token_capture_slots`).Scan(&originals); err != nil {
		t.Fatal(err)
	}
	if err := p.funding.capture.QueryRow(`SELECT count(*) FROM portal_monthly_usage_observations`).Scan(&projections); err != nil {
		t.Fatal(err)
	}
	if originals != 1 || projections != 1 {
		t.Fatal("user/token snapshot not saved together", originals, projections)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
	if _, err := p.Before(nil); !errors.Is(err, ErrGenerationInput) {
		t.Fatal(err)
	}
}

func TestTokenSourcePreparationRevocationAndBackoff(t *testing.T) {
	for _, scenario := range []string{"before", "during", "outage"} {
		t.Run(scenario, func(t *testing.T) {
			f, auth, r, p, d := tokenPreparationFixture(t)
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
				d.onQuery = func() error { return errors.New("private token source outage") }
			}
			if scenario != "outage" {
				if _, err := p.Advance(context.Background(), req); !errors.Is(err, ErrGenerationChanged) {
					t.Fatal("revoked scope accepted", err)
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
				t.Fatal("outage amplified", state, steps, failures, d.queries.Load())
			}
		})
	}
}

func TestOnDemandPreparationIndependentObservationWaitDoesNotConsumeRetries(t *testing.T) {
	for _, scenario := range []string{"future", "expired", "overlong"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			now := time.Now()
			f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) {
				calls++
				if calls > 1 {
					return false, nil
				}
				until := now.Add(5 * time.Minute)
				if scenario == "expired" {
					until = now.Add(-time.Second)
				}
				if scenario == "overlong" {
					until = now.Add(6 * time.Minute)
				}
				return false, &sourcePreparationPause{until: until}
			})
			p := r.preparation
			p.now = func() time.Time { return now }
			if _, err := p.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
				t.Fatal(err)
			}
			if err := p.runOne(context.Background()); err != nil {
				t.Fatal(err)
			}
			state, steps, failures := preparationState(t, f.receiver)
			if state != "queued" {
				t.Fatal(state)
			}
			switch scenario {
			case "future":
				if steps != 0 || failures != 0 {
					t.Fatal("independent wait spent work/failure budget", steps, failures)
				}
				if _, err := p.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
					t.Fatal(err)
				}
				if err := p.runOne(context.Background()); err != nil || calls != 1 {
					t.Fatal("duplicate bypassed wait", calls, err)
				}
				now = now.Add(301 * time.Second)
				if err := p.runOne(context.Background()); err != nil {
					t.Fatal(err)
				}
				_, steps, failures = preparationState(t, f.receiver)
				if steps != 1 || failures != 0 || calls != 2 {
					t.Fatal(steps, failures, calls)
				}
			case "expired":
				if steps != 1 || failures != 0 {
					t.Fatal("expired wait treated as source failure", steps, failures)
				}
			case "overlong":
				if steps != 1 || failures != 1 {
					t.Fatal("unbounded wait accepted", steps, failures)
				}
			}
		})
	}
}

func TestFundingSourcePreparationStageBoundaryDefersNextSourceOperation(t *testing.T) {
	f, auth, _, p, d := fundingPreparationFixture(t)
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	scope, err := p.scope(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(context.Background(), p.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(plan.Scans)-1; i++ {
		if i == len(plan.Scans)/2 {
			time.Sleep(2 * time.Millisecond)
		}
		page, err := monthlysource.CaptureFundingPage(context.Background(), p.source, plan.Scans[i], nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := monthlysource.SaveFundingPage(context.Background(), p.capture, page); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := step(context.Background(), req); err != nil || calls != 0 {
		t.Fatal("two source stages executed in one step", calls, err)
	}
	queries := d.queries.Load()
	// Once the scheduling stage is done, later steps must not reserve or
	// reread the capture database's funding pages. Final audit is separate.
	p.capture.SetMaxOpenConns(1)
	held, err := p.capture.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := step(ctx, req); err != nil || calls != 1 || d.queries.Load() != queries {
		t.Fatal("finished funding source recollected", calls, err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := step(ctx, req); !errors.Is(err, ErrGenerationChanged) || calls != 1 {
		t.Fatal("scheduling hint bypassed authority", err, calls)
	}
}
