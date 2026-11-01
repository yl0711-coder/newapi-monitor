package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Original synthetic observations for an otherwise empty August. With finalGaps
// the last newer funds page, two token views and newer wallet view are absent;
// the real collector must finish them. No original times are subsequently edited.
func seedHistoryEmptyMonth(t *testing.T, f capturedHistoryFixture, req SourcePreparationRequest, finalGaps bool, funding ...monthlysource.FundingLog) {
	t.Helper()
	p := f.wallet.funding
	scope, err := p.scope(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), p.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	days := len(plan.Scans) / 2
	for phase := range 2 {
		for day := range days {
			scan := plan.Scans[phase*days+day]
			if finalGaps && phase == 1 && day == days-1 {
				continue
			}
			at := time.Now().Add(time.Duration(-7+phase*6)*time.Minute).UnixMilli() + int64(day)
			page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 0, EndReached: true, Rows: []monthlysource.FundingLog{}, StartedAtMS: at, CompletedAtMS: at + 1}
			for _, row := range funding {
				if row.ID > page.FenceID {
					page.FenceID = row.ID
				}
				if row.CreatedAt >= scan.From && row.CreatedAt < scan.To {
					page.Rows = append(page.Rows, row)
					page.Next = monthlysource.FundingCursor{At: row.CreatedAt, ID: row.ID}
				}
			}
			if _, err := monthlysource.SaveFundingPage(t.Context(), p.capture, page); err != nil {
				t.Fatal(err)
			}
			for i, id := range scope.Funding.UserIDs {
				zero := int64(0)
				o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary",
					Request:     monthlysource.UsageWindowRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: id, From: scan.From, To: scan.To},
					StartedAtMS: at, CompletedAtMS: at + 1, Wallet: monthlysource.WalletUser{UserID: id, Exists: true, BalanceQuota: &zero, UsedQuota: &zero}, Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
				u, err := monthlysource.UsageObservationFromToken(o)
				if err != nil {
					t.Fatal(err)
				}
				uid, err := monthlysource.SaveUsageWindowObservation(t.Context(), p.capture, u)
				if err != nil {
					t.Fatal(err)
				}
				raw := materialPreparationJSON(t, o)
				ordinal := phase*days*len(scope.Funding.UserIDs) + day*len(scope.Funding.UserIDs) + i
				if _, err := p.capture.Exec(`INSERT INTO portal_monthly_token_capture_slots VALUES(?,?,?,?,?,?,?,?)`, req.Key(), ordinal, id, generationHash(raw), uid, at, at+1, raw); err != nil {
					t.Fatal(err)
				}
			}
		}
		if finalGaps && phase == 1 {
			continue
		}
		at := time.Now().Add(time.Duration(-7+phase*6) * time.Minute).UnixMilli()
		o := monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary", SourceIdentity: scope.Funding.SourceIdentity, StartedAtMS: at, CompletedAtMS: at + 1, Users: []monthlysource.WalletUser{}}
		for _, id := range scope.Funding.UserIDs {
			zero := int64(0)
			o.Users = append(o.Users, monthlysource.WalletUser{UserID: id, Exists: true, BalanceQuota: &zero, UsedQuota: &zero})
		}
		id, err := monthlysource.SaveWalletObservation(t.Context(), p.capture, o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.capture.Exec(`INSERT INTO portal_monthly_wallet_capture_slots VALUES(?,?,?)`, req.Key(), phase, id); err != nil {
			t.Fatal(err)
		}
	}
}

// Route the existing SQL-protocol doubles only; the collector, pacing,
// read-only transactions, checkpoints and original audits are unchanged.
type historyPreparationDriver struct{ base *sourcePreparationDriver }
type historyPreparationConn struct{ *sourcePreparationConn }

func (d *historyPreparationDriver) Open(string) (driver.Conn, error) {
	return &historyPreparationConn{&sourcePreparationConn{d: d.base}}, nil
}

func (c *historyPreparationConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "FROM users WHERE id IN") {
		return (&walletPreparationConn{c.sourcePreparationConn}).QueryContext(ctx, q, args)
	}
	if strings.Contains(q, "FROM users WHERE id=?") || strings.Contains(q, "GROUP BY user_id,type") && strings.Contains(q, "created_at<?") {
		return (&tokenPreparationConn{c.sourcePreparationConn}).QueryContext(ctx, q, args)
	}
	return c.sourcePreparationConn.QueryContext(ctx, q, args)
}

func TestAutomaticHistoryPreparationActualCapturesProgressWithoutHoldingSQLite(t *testing.T) {
	f, _, _ := automaticHistoryOriginalFixture(t, "2026-08")
	older := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, older, true)
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	db := sql.OpenDB(capturedPreparationTestConnector{&historyPreparationDriver{base: state}})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	f.wallet.funding.source = db
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	state.onQuery = func() error {
		if f.wallet.funding.capture.Stats().InUse != 0 || f.f.receiver.Stats().InUse != 0 {
			return errors.New("local connection/transaction held during source reads")
		}
		return nil
	}
	calls := 0
	step, err := p.Before(func(ctx context.Context, req SourcePreparationRequest, ref monthlysource.MonthlyObservationFile) (bool, error) {
		_, err := f.f.g.auditAutomaticHistorySources(ctx, ref, req)
		if err == nil {
			calls++
		}
		return false, err
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 12 {
		pings := state.pings.Load()
		if ready, err := step(t.Context(), f.req); err != nil || ready {
			t.Fatal("raw progress incorrectly approved a bill", ready, err)
		}
		if state.pings.Load()-pings > 1 {
			t.Fatal("more than one original source capture in one dispatch")
		}
		if calls != 0 {
			break
		}
	}
	if calls != 1 || state.writes.Load() != 0 || db.Stats().InUse != 0 {
		t.Fatal("incomplete dependency, source mutation or held connection", calls, state.writes.Load())
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

// The SAME original paired current view is used for another closed-month
// fixture. The original request has no selected closed-month field; its source,
// user, boundary, bytes, hashes and physical times remain completely unchanged.
func seedHistoryBoundaryForMonth(t *testing.T, f capturedHistoryFixture, req SourcePreparationRequest) {
	t.Helper()
	p := f.wallet.funding
	scope, err := p.scope(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), p.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.capture.Exec(`INSERT INTO portal_monthly_wallet_tail_captures SELECT ?,?,?,boundary,user_id,ordinal,id,started_at_ms,completed_at_ms,payload FROM portal_monthly_wallet_tail_captures WHERE job_key=?`, req.Key(), generationHash(materialPreparationJSON(t, plan)), generationHash(materialPreparationJSON(t, scope)), f.req.Key()); err != nil {
		t.Fatal(err)
	}
	registered, err := registeredSourceBefore(p, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registered(t.Context(), req); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticHistoryPreparationEarlierRequestIncludesLaterRawMonthAndRestarts(t *testing.T) {
	f, _, _ := automaticHistoryOriginalFixture(t, "2026-08")
	req := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, req, false)
	seedHistoryBoundaryForMonth(t, f, req)
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	queries := state.queries.Load()
	var first monthlysource.MonthlyObservationFile
	for round := range 2 {
		p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
		if err != nil {
			t.Fatal(err)
		}
		months, err := p.months(t.Context(), req)
		if err != nil || len(months) != 2 || months[0].Month != "2026-08" || months[1].Month != "2026-09" {
			t.Fatal("later raw dependency lost or site-May origin invented", months, err)
		}
		calls := 0
		step, err := p.Before(func(ctx context.Context, actual SourcePreparationRequest, ref monthlysource.MonthlyObservationFile) (bool, error) {
			calls++
			if actual != req {
				t.Fatal("customer month replaced by later data month")
			}
			if round == 0 {
				first = ref
			} else if first != ref {
				t.Fatal("restart changed immutable original-source identity")
			}
			got, err := f.f.g.auditAutomaticHistorySources(ctx, ref, req)
			if err != nil {
				return false, err
			}
			for _, a := range got.history.Report().Accounts {
				if len(a.Months) != 2 || a.Months[0].ConsumeQuota != 0 || a.Months[0].ClosingBalanceQuota != 1000000 || a.Months[1].ConsumeQuota != 1500000 || a.Months[1].ClosingBalanceQuota != 0 || !a.OriginRequiresResolution {
					t.Fatal("source chain changed monthly money or hid origin", a)
				}
			}
			// The SAME originals also support the inner prefix's month scope;
			// this changes source selection only, not permission to publish it.
			later := SourcePreparationRequest{Owner: req.Owner, Month: "2026-09"}
			other, err := f.f.g.auditAutomaticHistorySources(ctx, ref, later)
			if err != nil || other.scope.Funding.Month != later.Month || !reflect.DeepEqual(other.history.Report(), got.history.Report()) {
				t.Fatal("boundary month was confused with requested financial scope", err)
			}
			return false, nil // financial normalization/CAS is still required
		})
		if err != nil {
			t.Fatal(err)
		}
		for range 6 {
			if ready, err := step(t.Context(), req); err != nil || ready {
				t.Fatal("raw material became bill approval", ready, err)
			}
			if calls != 0 {
				break
			}
		}
		if calls != 1 || state.queries.Load() != queries || state.writes.Load() != 0 {
			t.Fatal("sources were recaptured or readiness not reached", calls, state.queries.Load(), queries)
		}
		generationAdmissionCounts(t, f.f.receiver, 0, 0)
	}
	// The registered financial gate itself was NOT widened to September by
	// preparing a raw September dependency for an August customer request.
	var rows int
	if err := f.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&rows); err != nil || rows != 0 {
		t.Fatal("data dependency created another customer job", rows, err)
	}
}

func TestAutomaticHistoryPreparationRunsInExistingQueueWithoutPublishing(t *testing.T) {
	f, _, _ := automaticHistoryOriginalFixture(t)
	d, cfg := registeredPipelineConfig(t, f)
	cfg.PartitionUsage, cfg.CaptureWalletTails = true, true
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	p, err := newAutomaticHistoryPreparation(d.capturedPreparation.spool.wallet, d.capturedPreparation.spool)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	d.runtime.preparation.step, err = p.Before(func(ctx context.Context, req SourcePreparationRequest, ref monthlysource.MonthlyObservationFile) (bool, error) {
		_, err := d.g.auditAutomaticHistorySources(ctx, ref, req)
		if err == nil {
			calls++
		}
		return false, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.runtime.preparation.SubmitMonthly(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion, f.req.Month); err != nil {
		t.Fatal(err)
	}
	if err := d.runtime.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, _, failures := preparationState(t, f.f.receiver)
	if state != "queued" || failures != 0 || calls != 1 {
		t.Fatal("material readiness submitted a customer bill", state, failures, calls)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
	// A mistaken producer returning true is independently rejected by the
	// existing queue, because no real financially validated selection exists.
	d.runtime.preparation.step, err = p.Before(func(context.Context, SourcePreparationRequest, monthlysource.MonthlyObservationFile) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.receiver.Exec(`UPDATE portal_monthly_source_preparation SET next_at=0 WHERE key=?`, f.req.Key()); err != nil {
		t.Fatal(err)
	}
	if err := d.runtime.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, _, failures = preparationState(t, f.f.receiver)
	if state != "failed" || failures != 1 {
		t.Fatal("raw-only true bypassed authoritative financial selection", state, failures)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryPreparationRejectsRevocationAndUnboundWorkMode(t *testing.T) {
	f, _, _ := automaticHistoryOriginalFixture(t)
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	f.wallet.funding.accountTokenWork = true
	unbound, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := unbound.Before(func(context.Context, SourcePreparationRequest, monthlysource.MonthlyObservationFile) (bool, error) {
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := blocked(t.Context(), f.req); ready || !errors.Is(err, ErrGenerationInput) {
		t.Fatal("unbound work bypassed queue accounting", ready, err)
	}
	f.wallet.funding.accountTokenWork = false
	step, err := p.Before(func(context.Context, SourcePreparationRequest, monthlysource.MonthlyObservationFile) (bool, error) {
		t.Fatal("revocation allowed next stage")
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id=?", f.req.Owner.GroupID).Update("portal_auth_ver", f.req.Owner.AuthVersion+1).Error; err != nil {
		t.Fatal(err)
	}
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	queries := state.queries.Load()
	if ready, err := step(t.Context(), f.req); ready || !errors.Is(err, ErrGenerationChanged) || state.queries.Load() != queries {
		t.Fatal("revoked scope did data work", ready, err)
	}
}

func TestAutomaticHistoryPreparationDamagedSealStopsWithoutRecapture(t *testing.T) {
	f, files, _ := automaticHistoryOriginalFixture(t)
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(files.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	var material monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(before, &material) != nil {
		t.Fatal("bad fixture")
	}
	if err := os.WriteFile(material.Tokens[0].File.Path, []byte("changed source"), 0600); err != nil {
		t.Fatal(err)
	}
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	queries := state.queries.Load()
	step, err := p.Before(func(ctx context.Context, req SourcePreparationRequest, ref monthlysource.MonthlyObservationFile) (bool, error) {
		_, err := f.f.g.auditAutomaticHistorySources(ctx, ref, req)
		return false, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := step(t.Context(), f.req); err == nil || ready || state.queries.Load() != queries {
		t.Fatal("damaged source triggered recapture or approval", ready, err)
	}
	after, err := os.ReadFile(files.Material.Path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("damaged seal was overwritten", err)
	}
	if same := reflect.DeepEqual(*p.wallet.funding, *p.raw.funding); same || !p.wallet.funding.walletTails || p.raw.funding.walletTails || !p.wallet.funding.registeredOrigins || p.raw.funding.registeredOrigins {
		t.Fatal("raw dependency configuration mutated shared producer")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryPreparationWaitAllowsOtherMonthThenReleasesConnections(t *testing.T) {
	f, _, _ := automaticHistoryOriginalFixture(t, "2026-08")
	req := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, req, false)
	funding := f.wallet.funding
	scope, err := funding.scope(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), funding.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	registered, err := registeredSourceBefore(funding, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registered(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	zone := time.FixedZone("UTC+8", 8*3600)
	month, _ := time.ParseInLocation("2006-01", "2026-08", zone)
	now := time.Now().In(zone)
	boundary := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, zone)
	// If this test runs after October, prepare the additional empty closed
	// months too. An incomplete old round must NOT freeze a later real view
	// into October or require test clocks/timestamps to be forged.
	for at := month.AddDate(0, 2, 0); at.Before(boundary); at = at.AddDate(0, 1, 0) {
		seedHistoryEmptyMonth(t, f, SourcePreparationRequest{Owner: req.Owner, Month: at.Format("2006-01")}, false)
	}
	f.spool, err = NewRetainedCapturedSourceSpool(f.wallet, f.spool.directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range scope.Funding.UserIDs {
		// New synthetic FIRST originals, not edited copies of the completed
		// September pair. No second observation is fabricated to avoid waiting.
		at := time.Now().Add(-time.Minute).UnixMilli()
		balance, used := int64(0), int64(1000000)
		o := monthlysource.WalletTailObservation{Version: 1, Basis: "same_read_view_wallet_and_visible_tail_not_settled_boundary",
			Request:     monthlysource.WalletTailRequest{SourceIdentity: funding.sourceIdentity, Rules: scope.Funding.Rules, UserID: id, Boundary: boundary.Unix()},
			StartedAtMS: at, CompletedAtMS: at, User: monthlysource.RegisteredWalletUser{UserID: id, RegisteredAt: month.Unix(), BalanceQuota: &balance, UsedQuota: &used}, Usage: []monthlysource.UsageWindowRow{}, Funding: []monthlysource.FundingLog{}}
		if err := monthlysource.ValidateWalletTailObservation(o); err != nil {
			t.Fatal(err)
		}
		raw := materialPreparationJSON(t, o)
		if _, err := funding.capture.Exec(`INSERT INTO portal_monthly_wallet_tail_captures VALUES(?,?,?,?,?,?,?,?,?,?)`, req.Key(), generationHash(materialPreparationJSON(t, plan)), generationHash(materialPreparationJSON(t, scope)), boundary.Unix(), id, 0, generationHash(raw), at, at, raw); err != nil {
			t.Fatal(err)
		}
	}
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	step, err := p.Before(func(context.Context, SourcePreparationRequest, monthlysource.MonthlyObservationFile) (bool, error) {
		t.Fatal("missing second original allowed financial preparation")
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	state := funding.source.Driver().(*walletTailPreparationDriver).base
	queries := state.queries.Load()
	// August must wait, but the already captured September originals can be
	// sealed in this dispatch, rather than wasting the whole wait interval.
	if ready, err := step(t.Context(), req); ready || err != nil {
		t.Fatal("wait prevented useful later-month data work", ready, err)
	}
	if _, found, err := p.core.completedSourceReferences(t.Context(), f.req); err != nil || !found {
		t.Fatal("later raw dependency was not sealed during wait", found, err)
	}
	var pause *sourcePreparationPause
	for range 120 {
		ready, err := step(t.Context(), req)
		if ready {
			t.Fatal("pending originals became financial readiness")
		}
		if errors.As(err, &pause) {
			break
		}
		if err != nil {
			t.Fatal("later closed-month data work failed", err)
		}
	}
	if pause == nil || pause.until.Before(time.Now()) || time.Until(pause.until) > 5*time.Minute {
		t.Fatal("independent-view wait lost its bounded queue hint", pause)
	}
	if state.queries.Load() != queries || funding.capture.Stats().InUse != 0 || funding.source.Stats().InUse != 0 || f.f.receiver.Stats().InUse != 0 {
		t.Fatal("wait reread source or kept a connection/transaction")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}
