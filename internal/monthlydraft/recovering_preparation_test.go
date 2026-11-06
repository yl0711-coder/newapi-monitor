package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Complete synthetic child observations ONLY, no real files or online reads.
// Actual deadline/handoff/pacing/restart are exercised by monthlysource tests;
// here exercise the real queue/startup/sealer/history/customer publication.
func syntheticCapturedRecovery(t *testing.T, f *capturedHistoryFixture) {
	syntheticCapturedRecoveryWithRecentRoot(t, f, false)
}

func syntheticCapturedRecoveryWithRecentRoot(t *testing.T, f *capturedHistoryFixture, recent bool) {
	t.Helper()
	db := f.wallet.funding.capture
	if err := monthlysource.InitRecoveringTokenMonthStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	const first, second = 20, 80 // empty day, first account of two, September
	var a, b monthlysource.TokenUsageWindowObservation
	for i, dst := range map[int]*monthlysource.TokenUsageWindowObservation{first: &a, second: &b} {
		var raw []byte
		if err := db.QueryRow(`SELECT payload FROM portal_monthly_token_capture_slots WHERE job_key=? AND ordinal=?`, f.req.Key(), i).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if json.Unmarshal(raw, dst) != nil || len(dst.Rows) != 0 {
			t.Fatal("day must actually be empty")
		}
	}
	if recent {
		a.StartedAtMS = time.Now().Add(-time.Hour).UnixMilli()
		a.CompletedAtMS = a.StartedAtMS + 1
		b.StartedAtMS = a.CompletedAtMS + 600000
		b.CompletedAtMS = b.StartedAtMS + 1
	}
	root := struct {
		Version int                              `json:"version"`
		JobKey  string                           `json:"job_key"`
		Request monthlysource.UsageWindowRequest `json:"request"`
	}{1, f.req.Key(), a.Request}
	raw := materialPreparationJSON(t, root)
	id := generationHash(raw)
	at := a.StartedAtMS - 1
	if _, err := db.Exec(`INSERT INTO monthly_adaptive_token_roots(id,payload,created_at_ms) VALUES(?,?,?)`, id, raw, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO monthly_adaptive_token_splits(root_id,window_from,window_to,phase) VALUES(?,?,?,0)`, id, a.Request.From, a.Request.To); err != nil {
		t.Fatal(err)
	}
	for _, from := range []int64{a.Request.From, a.Request.From + 43200} {
		for phase, o := range []monthlysource.TokenUsageWindowObservation{a, b} {
			o.Request.From, o.Request.To = from, from+43200
			u, err := monthlysource.UsageObservationFromToken(o)
			if err != nil {
				t.Fatal(err)
			}
			uid, err := monthlysource.SaveUsageWindowObservation(t.Context(), db, u)
			if err != nil {
				t.Fatal(err)
			}
			raw := materialPreparationJSON(t, o)
			projection := materialPreparationJSON(t, u)
			if generationHash(projection) != uid {
				t.Fatal("projection not canonical")
			}
			if _, err := db.Exec(`INSERT INTO monthly_adaptive_token_observations(root_id,window_from,window_to,phase,sha,usage_id,started_at_ms,completed_at_ms,payload,projection) VALUES(?,?,?,?,?,?,?,?,?,?)`, id, from, from+43200, phase, generationHash(raw), uid, o.StartedAtMS, o.CompletedAtMS, raw, projection); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO monthly_token_recovery_bindings(job_key,ordinal,root_id,created_at_ms) VALUES(?,?,?,?)`, f.req.Key(), first, id, at); err != nil {
		t.Fatal(err)
	}
	// SETUP ONLY: model missing source originals. Restore the immutable trigger
	// immediately; no production recovery API deletes/rewrites old records.
	if _, err := db.Exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE ordinal IN (20,80); CREATE TRIGGER monthly_token_capture_no_delete BEFORE DELETE ON portal_monthly_token_capture_slots BEGIN SELECT RAISE(ABORT,'token captures are immutable'); END`); err != nil {
		t.Fatal(err)
	}
	f.wallet.funding.tokenRecovery = true
}

func TestRecoveringPreparationStartupClickPublishesAndRetainsExactMoney(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	syntheticCapturedRecovery(t, &f)
	addCapturedHistoryReconstruction(t, &f)
	testCapturedHistoryPublicationAdapter(t, f, true, true, true, true)
}

func TestRecoveringPreparationOptInRequiresStoreAndIsLazy(t *testing.T) {
	for _, schema := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_store", true: "initialized_store"}[schema], func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			if schema {
				if err := monthlysource.InitRecoveringTokenMonthStore(t.Context(), f.wallet.funding.capture); err != nil {
					t.Fatal(err)
				}
			}
			d := f.open(t)
			cfg, ref := capturedPreparationConfigFixture(t, f)
			cfg.RecoverTokenWindows = true
			raw := materialPreparationJSON(t, cfg)
			if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ref.SHA256 = generationHash(raw)
			opened := 0
			opener := capturedPreparationTestOpener(f)
			// A real lazy connector is used in the initialized case; an unreachable
			// synthetic address must not be contacted by startup.
			if !schema {
				err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, func(c *mysql.Config) (*sql.DB, error) { opened++; return opener(c) })
				if err == nil || opened != 0 || d.capturedPreparation != nil {
					t.Fatal("startup initialized schema or contacted source", err, opened)
				}
				var count int
				if err := f.wallet.funding.capture.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='monthly_token_recovery_bindings'`).Scan(&count); err != nil || count != 0 {
					t.Fatal("implicit schema creation", count, err)
				}
				return
			}
			if err := d.EnableCapturedSourcePreparation(t.Context(), "captured-scope", ref); err != nil {
				t.Fatal(err)
			}
			res := d.capturedPreparation
			if res == nil || res.source.Stats().OpenConnections != 0 || d.runtime.Phase() != "disabled" {
				t.Fatal("recovery started source work during installation")
			}
			closeDeploymentTest(t, d)
			assertCapturedPreparationDBClosed(t, res.source)
			assertCapturedPreparationDBClosed(t, res.capture)
			if err := f.wallet.funding.capture.Ping(); err != nil {
				t.Fatal("closed borrowed capture", err)
			}
		})
	}
}

func TestRecoveringPreparationAuthorizationChangeStopsBeforeSource(t *testing.T) {
	f, auth, _, p, driver := tokenPreparationFixture(t)
	if err := monthlysource.InitRecoveringTokenMonthStore(t.Context(), p.funding.capture); err != nil {
		t.Fatal(err)
	}
	p.funding.tokenRecovery = true
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	before := driver.queries.Load()
	if _, err := p.Advance(t.Context(), req); err == nil || before != driver.queries.Load() {
		t.Fatal("revoked recovery queried source", err)
	}
}

func TestRecoveringPreparationSourceOutageUsesExistingQueueBudget(t *testing.T) {
	f, _, r, p, driver := tokenPreparationFixture(t)
	if err := monthlysource.InitRecoveringTokenMonthStore(t.Context(), p.funding.capture); err != nil {
		t.Fatal(err)
	}
	p.funding.tokenRecovery = true
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	driver.onQuery = func() error { return errors.New("synthetic source outage") }
	if _, err := r.preparation.SubmitMonthly(t.Context(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 1 || failures != 1 || driver.queries.Load() != 1 {
		t.Fatal("recovery bypassed queue failure/cooldown budget", state, steps, failures, driver.queries.Load())
	}
	var roots int
	if err := p.funding.capture.QueryRow(`SELECT count(*) FROM monthly_adaptive_token_roots`).Scan(&roots); err != nil || roots != 0 {
		t.Fatal("transport outage caused a split", roots, err)
	}
}

func TestRecoveringHistorySelectionRefusesMismatchedPinnedMaterial(t *testing.T) {
	for _, scenario := range []string{"usage_order", "wallet_ids", "unknown_version", "missing_parent", "database_pin"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			syntheticCapturedRecovery(t, &f)
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(files.Material.Path)
			if err != nil {
				t.Fatal(err)
			}
			var material monthlysource.MonthlyObservationMaterial
			if json.Unmarshal(raw, &material) != nil {
				t.Fatal("invalid fixture material")
			}
			raw, err = os.ReadFile(files.Selection.Path)
			if err != nil {
				t.Fatal(err)
			}
			var selected monthlysource.CapturedMonthSelection
			if json.Unmarshal(raw, &selected) != nil || selected.Version != 2 {
				t.Fatal("missing v2 fixture")
			}
			switch scenario {
			case "usage_order":
				material.UsageWindows[0], material.UsageWindows[1] = material.UsageWindows[1], material.UsageWindows[0]
			case "wallet_ids":
				material.WalletIDs[0], material.WalletIDs[1] = material.WalletIDs[1], material.WalletIDs[0]
			case "unknown_version":
				selected.Version = 3
			case "missing_parent":
				selected.Tokens[0].Parents = selected.Tokens[0].Parents[1:]
			case "database_pin":
				material.Database.SHA256 = generationHash([]byte("wrong sealed digest"))
			}
			d := f.open(t)
			if err := d.g.verifyRecoveredHistorySelection(t.Context(), materialPreparationJSON(t, selected), selected, material); err == nil {
				t.Fatal("mismatched v2 source metadata accepted")
			}
			preparationCounts(t, f.f, 0, 0)
		})
	}
}
