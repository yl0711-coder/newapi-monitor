package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Synthetic transport only. The real MySQL collector/isolation test is kept
// separately in monthlysource; no test here can resolve a network source.
type walletTailPreparationDriver struct{ base *sourcePreparationDriver }
type walletTailPreparationConn struct{ *walletPreparationConn }

func (d *walletTailPreparationDriver) Open(string) (driver.Conn, error) {
	return &walletTailPreparationConn{&walletPreparationConn{&sourcePreparationConn{d: d.base}}}, nil
}

func (c *walletTailPreparationConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	tailUser := strings.Contains(q, "id,created_at,quota,used_quota FROM users") && strings.HasSuffix(q, "LIMIT 2")
	tailUsage := strings.Contains(q, "GROUP BY user_id,type,billing_source") && !strings.Contains(q, "created_at<?")
	tailFunds := strings.Contains(q, "AS branch_ids") && !strings.Contains(q, "created_at<?")
	boundedUsage := strings.Contains(q, "WITH usage_keys AS")
	tailUsage = tailUsage || boundedUsage
	tailFunds = tailFunds || strings.Contains(q, "AS funding_candidates")
	if !tailUser && !tailUsage && !tailFunds {
		return c.walletPreparationConn.QueryContext(ctx, q, args)
	}
	c.d.queries.Add(1)
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second || !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") {
		return nil, errors.New("unsafe wallet tail query")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.d.onQuery != nil {
		if err := c.d.onQuery(); err != nil {
			return nil, err
		}
	}
	if tailUser {
		if len(args) != 1 {
			return nil, errors.New("tail capture must select exactly one account")
		}
		id := args[0].Value.(int64)
		at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
		if c.d.registrationAt != nil {
			at = c.d.registrationAt[id]
		}
		return &walletPreparationRows{registeredWallet: true, data: [][]driver.Value{{id, at, int64(0), int64(0)}}}, nil
	}
	if tailUsage {
		if boundedUsage {
			if len(args) != 6 || args[0].Value != args[2].Value {
				return nil, errors.New("empty tail fixture received a resume or mismatched account")
			}
			return &sourcePreparationRows{columns: []string{"page_records", "next_at", "next_type", "next_id", "user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}, values: []driver.Value{int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), "", int64(0), int64(0), "0", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)}}, nil
		}
		return &sourcePreparationRows{columns: []string{"user_id", "type", "billing_source", "records", "known", "quota", "min_quota", "max_quota", "min_id", "max_id", "first_at", "last_at"}}, nil
	}
	return &sourcePreparationRows{columns: []string{"id", "user_id", "created_at", "type", "content", "other"}}, nil
}

func tailPreparationSource(t *testing.T, f capturedHistoryFixture) (*sourcePreparationDriver, driver.Driver) {
	t.Helper()
	base := f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	d := &walletTailPreparationDriver{base: base}
	db := sql.OpenDB(capturedPreparationTestConnector{d})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	f.wallet.funding.source = db
	return base, d
}

func tailPreparationConfig(t *testing.T, f capturedHistoryFixture) (*Deployment, CapturedPreparationConfig, driver.Driver) {
	t.Helper()
	if err := monthlysource.InitRegisteredWalletMonthCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitWalletTailCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
		t.Fatal(err)
	}
	_, source := tailPreparationSource(t, f)
	d, cfg := registeredPipelineConfig(t, f)
	cfg.CaptureWalletTails = true
	return d, cfg, source
}

func tailPreparationOpener(source driver.Driver) func(*mysql.Config) (*sql.DB, error) {
	return func(*mysql.Config) (*sql.DB, error) {
		db := sql.OpenDB(capturedPreparationTestConnector{source})
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		return db, nil
	}
}

func TestWalletTailPipelineWarmFirstWaitAndNoEarlySeal(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	if err := monthlysource.InitWalletTailCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
		t.Fatal(err)
	}
	state, _ := tailPreparationSource(t, f)
	f.wallet.funding.registeredOrigins, f.wallet.funding.walletTails = true, true
	calls := 0
	warm, err := walletTailSourceBefore(f.wallet.funding, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return false, nil }, true)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if ready, err := warm(t.Context(), f.req); err != nil || ready || calls != 0 {
			t.Fatal("source step skipped or reported bill ready", ready, calls, err)
		}
	}
	queries := state.queries.Load()
	if ready, err := warm(t.Context(), f.req); err != nil || ready || calls != 1 || state.queries.Load() != queries {
		t.Fatal("first views did not allow useful following work", ready, calls, err)
	}
	final, err := walletTailSourceBefore(f.wallet.funding, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return true, nil }, false)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := final(t.Context(), f.req)
	var pause *sourcePreparationPause
	if ready || !errors.As(err, &pause) || pause.until.Before(time.Now()) || calls != 1 || state.queries.Load() != queries {
		t.Fatal("wait occupied source or allowed an early seal", ready, calls, err)
	}
	if f.wallet.funding.source.Stats().InUse != 0 || f.wallet.funding.capture.Stats().InUse != 0 || state.writes.Load() != 0 {
		t.Fatal("wait retained connection/transaction or attempted source write")
	}
	if _, err := f.wallet.AssembleCapturedSources(t.Context(), f.req); err == nil {
		t.Fatal("incomplete tail originals allowed a seal")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestWalletTailPipelineRevocationStopsSourceAndNextStage(t *testing.T) {
	for _, when := range []string{"before", "during"} {
		t.Run(when, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			if err := monthlysource.InitWalletTailCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
				t.Fatal(err)
			}
			state, _ := tailPreparationSource(t, f)
			f.wallet.funding.registeredOrigins, f.wallet.funding.walletTails = true, true
			calls := 0
			stage, err := walletTailSourceBefore(f.wallet.funding, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return true, nil }, true)
			if err != nil {
				t.Fatal(err)
			}
			revoke := func() error {
				return f.auth.Model(&monitor.CustomerGroup{}).Where("id=?", f.req.Owner.GroupID).Update("portal_auth_ver", 2).Error
			}
			before := state.queries.Load()
			if when == "before" {
				if err := revoke(); err != nil {
					t.Fatal(err)
				}
			} else {
				state.onQuery = revoke
			}
			if ready, err := stage(t.Context(), f.req); err == nil || ready || calls != 0 {
				t.Fatal("revoked authority continued", ready, calls, err)
			}
			if when == "before" && state.queries.Load() != before {
				t.Fatal("revoked authority contacted source")
			}
			generationAdmissionCounts(t, f.f.receiver, 0, 0)
		})
	}
}

func TestWalletTailPipelineMissingSchemaIsExplicitAndOldModeStillInstalls(t *testing.T) {
	for _, fault := range []string{"schema", "requires_registered"} {
		t.Run(fault, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			if err := monthlysource.InitRegisteredWalletMonthCaptureStore(t.Context(), f.wallet.funding.capture); err != nil {
				t.Fatal(err)
			}
			d, cfg := registeredPipelineConfig(t, f)
			cfg.CaptureWalletTails = true
			if fault == "requires_registered" {
				cfg.CaptureRegisteredOrigins = false
			}
			opened := 0
			opener := func(c *mysql.Config) (*sql.DB, error) { opened++; return capturedPreparationTestOpener(f)(c) }
			if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), opener); err == nil || opened != 0 {
				t.Fatal("invalid opt-in opened source", opened, err)
			}
			var count int
			if err := f.wallet.funding.capture.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='portal_monthly_wallet_tail_captures'").Scan(&count); err != nil || count != 0 {
				t.Fatal("startup implicitly initialized evidence schema", count, err)
			}
			cfg.CaptureWalletTails, cfg.CaptureRegisteredOrigins = false, true
			if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), opener); err != nil {
				t.Fatal("failed opt-in damaged old path", err)
			}
			if d.capturedPreparation.funding.walletTails || d.capturedPreparation.source.Stats().OpenConnections != 0 {
				t.Fatal("old startup queried financial source or enabled new mode")
			}
		})
	}
}

// A retained synthetic first view models a prior worker iteration. The actual
// current second view comes from the real collector protocol in the worker.
// Existing synthetic historical cutoffs remain necessary for this test bill;
// this is deliberately NOT an empty-history self-service acceptance claim.
func seedTailPipelineFirsts(t *testing.T, f capturedHistoryFixture) {
	t.Helper()
	scope, err := f.wallet.funding.scope(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), f.wallet.funding.capture, f.req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	planRaw, _ := json.Marshal(plan)
	scopeRaw, _ := json.Marshal(scope)
	at := time.Now().Add(-6 * time.Minute)
	zone := time.FixedZone("UTC+8", 8*3600)
	local := at.In(zone)
	boundary := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, zone).Unix()
	for _, id := range scope.Funding.UserIDs {
		zero := int64(0)
		o := monthlysource.WalletTailObservation{Version: 1, Basis: "same_read_view_wallet_and_visible_tail_not_settled_boundary", Request: monthlysource.WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: id, Boundary: boundary}, StartedAtMS: at.UnixMilli(), CompletedAtMS: at.UnixMilli(), User: monthlysource.RegisteredWalletUser{UserID: id, RegisteredAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix(), BalanceQuota: &zero, UsedQuota: &zero}, Usage: []monthlysource.UsageWindowRow{}, Funding: []monthlysource.FundingLog{}}
		if err := monthlysource.ValidateWalletTailObservation(o); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(o)
		if _, err := f.wallet.funding.capture.Exec(`INSERT INTO portal_monthly_wallet_tail_captures VALUES(?,?,?,?,?,?,?,?,?,?)`, f.req.Key(), generationHash(planRaw), generationHash(scopeRaw), boundary, id, 0, generationHash(raw), at.UnixMilli(), at.UnixMilli(), raw); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWalletTailPipelineStartupClickSecondCaptureSealBillAndRestartUsingExistingHistory(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d, cfg, source := tailPreparationConfig(t, f)
	seedTailPipelineFirsts(t, f)
	history, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), tailPreparationOpener(source)); err != nil {
		t.Fatal(err)
	}
	if d.capturedPreparation.source.Stats().OpenConnections != 0 {
		t.Fatal("installation contacted source")
	}
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = 5*time.Millisecond, 20*time.Millisecond
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar := request("GET", "/api/monthly-statement/calendar", "", own)
	initial := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	if calendar.Code != 200 || initial.Code != 200 {
		t.Fatal("calendar/generation unavailable", calendar.Code, initial.Code)
	}
	method, path, body := capturedHistoryFirstButtonRequest(t, initial.Body.Bytes(), calendar.Body.Bytes())
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	bill := waitHistoryAssemblyBill(t, f, d)
	assertHistoryAssemblyMoney(t, bill)
	scope, err := d.capturedPreparation.funding.scope(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	p, err := monthlysource.InspectWalletTailMonthCapture(t.Context(), d.capturedPreparation.capture, f.req.Key(), scope)
	if err != nil || p.Completed != 4 || p.Captured {
		t.Fatal("worker omitted paired tail originals", p, err)
	}
	files, completed, err := d.capturedPreparation.spool.Advance(t.Context(), f.req)
	if err != nil || completed {
		t.Fatal("finished seal not reused", err)
	}
	var material monthlysource.MonthlyObservationMaterial
	raw, err := os.ReadFile(files.Material.Path)
	if err != nil || json.Unmarshal(raw, &material) != nil {
		t.Fatal("sealed material unavailable", err)
	}
	selection, err := os.ReadFile(files.Selection.Path)
	if err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+material.Database.Path+"?mode=ro&immutable=1&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	tails, err := monthlysource.ReadSelectedWalletTails(t.Context(), ro, selection)
	if closeErr := ro.Close(); err != nil || closeErr != nil || len(tails.Pairs) != 2 {
		t.Fatal("sealed original/reference missing", err, closeErr)
	}
	for _, pair := range tails.Pairs {
		if pair.Newer.StartedAtMS-pair.Older.CompletedAtMS < 300000 || pair.Newer.User.BalanceQuota == nil || *pair.Newer.User.BalanceQuota != 0 {
			t.Fatal("worker retimed/altered original")
		}
	}
	closeDeploymentTest(t, d)
	page, book := assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	reopened := f
	reopened.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	d2 := reopened.open(t)
	if err := d2.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), tailPreparationOpener(source)); err != nil {
		t.Fatal(err)
	}
	read, session, other := capturedHistoryHTTP(t, f, d2)
	if err := d2.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, d2)
	after, afterBook := assertCapturedHistoryCustomerDownload(t, read, session, other)
	if !bytes.Equal(page, after) || !bytes.Equal(book, afterBook) {
		t.Fatal("restart changed saved customer money or workbook")
	}
	if got, err := os.ReadFile(f.sources.History.Path); err != nil || !bytes.Equal(history, got) {
		t.Fatal("tail capture changed existing historical money", err)
	}
}
