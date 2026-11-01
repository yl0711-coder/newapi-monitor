package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Synthetic originals, not reconstructed history supplied as input. Current
// balances/counters independently say each account has consumed 1,000,000 net
// quota. The historical origin residual must remain visible, never charged.
func automaticHistoryOriginalFixture(t *testing.T, firstRegistration ...string) (capturedHistoryFixture, CapturedSourceFiles, monthlysource.MonthlyObservationFile) {
	t.Helper()
	return automaticHistoryOriginalFixtureWithTail(t, nil, firstRegistration...)
}

func automaticHistoryOriginalFixtureWithTail(t *testing.T, edit func(*monthlysource.WalletTailObservation, int), firstRegistration ...string) (capturedHistoryFixture, CapturedSourceFiles, monthlysource.MonthlyObservationFile) {
	return automaticHistoryOriginalFixtureOptions(t, edit, nil, nil, firstRegistration...)
}

func automaticHistoryOriginalFixtureOptions(t *testing.T, edit func(*monthlysource.WalletTailObservation, int), registrations map[int64]int64, tokenEdits []func(*monthlysource.TokenUsageWindowObservation), firstRegistration ...string) (capturedHistoryFixture, CapturedSourceFiles, monthlysource.MonthlyObservationFile) {
	t.Helper()
	f := newCapturedHistoryFixture(t, tokenEdits...)
	// Seal paths are canonical absolute paths, as in real deployment startup.
	root, err := os.OpenRoot(f.cfg.EvidenceRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	g := *f.f.g
	g.root = root
	f.f.g, f.f.root = &g, f.cfg.EvidenceRoot
	state, _ := tailPreparationSource(t, f)
	zone := time.FixedZone("UTC+8", 8*3600)
	month, _ := time.ParseInLocation("2006-01", f.req.Month, zone)
	registration := month.Unix()
	if len(firstRegistration) != 0 {
		at, err := time.ParseInLocation("2006-01", firstRegistration[0], zone)
		if err != nil {
			t.Fatal(err)
		}
		registration = at.Unix()
	}
	state.registrationAt = map[int64]int64{201: registration, 202: registration}
	for id, at := range registrations {
		state.registrationAt[id] = at
	}
	p := f.wallet.funding
	p.partitionUsage, p.registeredOrigins, p.walletTails = true, true, true
	if err := monthlysource.InitRegisteredWalletMonthCaptureStore(t.Context(), p.capture); err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitWalletTailCaptureStore(t.Context(), p.capture); err != nil {
		t.Fatal(err)
	}
	registered, err := registeredSourceBefore(p, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := registered(t.Context(), f.req); err != nil || ready {
		t.Fatal("failed to retain synthetic registration original", ready, err)
	}
	scope, err := p.scope(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), p.capture, f.req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	planRaw, scopeRaw := materialPreparationJSON(t, plan), materialPreparationJSON(t, scope)
	boundary := month.AddDate(0, 1, 0).Unix()
	for _, user := range scope.Funding.UserIDs {
		for phase := range 2 {
			// Retained October originals, not a fake "current October" view
			// created in a later calendar month. Replays must work after rollover.
			at := time.Unix(boundary, 0).Add(36*time.Hour + time.Duration(phase)*6*time.Minute).UnixMilli()
			balance, used := int64(0), int64(1000000)
			o := monthlysource.WalletTailObservation{Version: 1, Basis: "same_read_view_wallet_and_visible_tail_not_settled_boundary",
				Request:     monthlysource.WalletTailRequest{SourceIdentity: p.sourceIdentity, Rules: scope.Funding.Rules, UserID: user, Boundary: boundary},
				StartedAtMS: at, CompletedAtMS: at, User: monthlysource.RegisteredWalletUser{UserID: user, RegisteredAt: state.registrationAt[user], BalanceQuota: &balance, UsedQuota: &used},
				Usage: []monthlysource.UsageWindowRow{}, Funding: []monthlysource.FundingLog{}}
			if edit != nil {
				edit(&o, phase) // Construct synthetic originals before their first save.
			}
			if err := monthlysource.ValidateWalletTailObservation(o); err != nil {
				t.Fatal(err)
			}
			raw := materialPreparationJSON(t, o)
			if _, err := p.capture.Exec(`INSERT INTO portal_monthly_wallet_tail_captures VALUES(?,?,?,?,?,?,?,?,?,?)`, f.req.Key(), generationHash(planRaw), generationHash(scopeRaw), boundary, user, phase, generationHash(raw), o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	// No manual History/Scope is passed to collection or original-chain audit.
	files, sealed, err := collectCapturedSources(t.Context(), f.wallet, f.spool, f.req)
	if err != nil || sealed || files != (CapturedSourceFiles{}) {
		t.Fatal("a newly written seal was mistaken for selected financial input", sealed, err)
	}
	files, sealed, err = collectCapturedSources(t.Context(), f.wallet, f.spool, f.req)
	if err != nil || !sealed {
		t.Fatal("sealed data-only dependency unavailable", sealed, err)
	}
	m := automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: f.req.Owner, SourceIdentity: p.sourceIdentity, Boundary: files, Months: []CapturedSourceFiles{files}}
	return f, files, writeAutomaticHistoryManifest(t, f.f.root, m)
}

func writeAutomaticHistoryManifest(t *testing.T, root string, m automaticHistorySources) monthlysource.MonthlyObservationFile {
	t.Helper()
	raw := materialPreparationJSON(t, m)
	path := filepath.Join(root, "history-originals-"+generationHash(raw)+".json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)}
}

func TestAutomaticHistorySourcesOriginalFilesWithoutManualHistory(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	historyBefore, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	// Even removing the existing fixture's manual-history file does not make
	// the data-only reader use/fabricate it. All originals are under a seal.
	if err := os.Rename(f.sources.History.Path, f.sources.History.Path+".unused"); err != nil {
		t.Fatal(err)
	}
	got, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	r := got.history.Report()
	if r.FromMonth != f.req.Month || r.ThroughMonth != f.req.Month || len(r.Accounts) != 2 || len(got.tails.Pairs) != 2 || !got.history.MatchesCollectionScope(got.scope) {
		t.Fatal("full original identity lost", r)
	}
	for _, a := range r.Accounts {
		m := a.Months[0]
		if a.OriginBalanceQuota != 1000000 || a.OriginUsedQuota != 0 || !a.OriginRequiresResolution || m.ConsumeQuota != 1500000 || m.RefundQuota != 500000 || m.ClosingBalanceQuota != 0 || m.ClosingUsedQuota != 1000000 {
			t.Fatal("origin residual hid/changed logged charges", a)
		}
	}
	// Restart/replay must re-audit originals, not deserialize the report.
	again, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req)
	if err != nil || !reflect.DeepEqual(again.history.Report(), r) {
		t.Fatal("original replay changed financial arithmetic", err)
	}
	if _, err := json.Marshal(got); err == nil {
		t.Fatal("proof became an editable JSON report")
	}
	if err := json.Unmarshal([]byte(`{}`), &got); err == nil || got.history.MatchesCollectionScope(again.scope) {
		t.Fatal("unmarshal retained a stale proof")
	}
	if b, err := os.ReadFile(f.sources.History.Path + ".unused"); err != nil || !bytes.Equal(b, historyBefore) {
		t.Fatal("existing history was rewritten", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
	if f.wallet.funding.capture.Stats().InUse != 0 || f.f.receiver.Stats().InUse != 0 {
		t.Fatal("source-only calculation retained SQLite handles")
	}
}

func TestAutomaticHistorySourcesMaterialBindingAndTotalBudget(t *testing.T) {
	f, files, ref := automaticHistoryOriginalFixture(t)
	raw, err := os.ReadFile(ref.Path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest automaticHistorySources
	if json.Unmarshal(raw, &manifest) != nil {
		t.Fatal("bad manifest fixture")
	}
	raw, err = os.ReadFile(files.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	var original monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(raw, &original) != nil {
		t.Fatal("bad material fixture")
	}
	for _, name := range []string{"funding_rules", "funding_window", "wallet_id", "token_owner", "token_alias", "selection_alias", "database_alias", "unknown_field", "aggregate_budget"} {
		t.Run(name, func(t *testing.T) {
			var m monthlysource.MonthlyObservationMaterial
			if json.Unmarshal(raw, &m) != nil {
				t.Fatal("bad material fixture")
			}
			switch name {
			case "funding_rules":
				m.Funding.Rules.QuotaPerUnit++
			case "funding_window":
				m.FundingWindows[0].To--
			case "wallet_id":
				m.WalletIDs[0] = strings.Repeat("f", 64)
			case "token_owner":
				m.Tokens[0].UserID++
			case "token_alias":
				m.Tokens[1].File = m.Tokens[0].File
			case "selection_alias":
				m.UsageSelection = &files.Material
			case "database_alias":
				m.Database = files.Selection
			case "aggregate_budget":
				// Sparse PRIVATE test files: each fits the individual limit,
				// together they exceed the unchanged total limit. Reject before
				// hashing/decoding either large file or opening SQLite.
				for i := range 2 {
					p := filepath.Join(f.f.root, "budget-"+stringID(int64(i)))
					file, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if err != nil {
						t.Fatal(err)
					}
					if err := errors.Join(file.Truncate(generationEvidenceLimit/2+1), file.Close()); err != nil {
						t.Fatal(err)
					}
					v := monthlysource.MonthlyObservationFile{Path: p, SHA256: strings.Repeat("f", 64)}
					if i == 0 {
						m.Database = v
					} else {
						m.Tokens[0].File = v
					}
				}
			}
			body := materialPreparationJSON(t, m)
			if name == "unknown_field" {
				body = append(bytes.Clone(body[:len(body)-1]), []byte(`,"approved":true}`)...)
			}
			path := filepath.Join(f.f.root, "bad-material-"+generationHash(body)+".json")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			changed := files
			changed.Material = monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(body)}
			next := manifest
			next.Boundary, next.Months = changed, []CapturedSourceFiles{changed}
			_, err = f.f.g.auditAutomaticHistorySources(t.Context(), writeAutomaticHistoryManifest(t, f.f.root, next), f.req)
			if err == nil || name == "aggregate_budget" && !errors.Is(err, ErrGenerationCapacity) {
				t.Fatal("changed/oversized source admitted", err)
			}
		})
	}
	// Invalid alternatives never modify/select the valid originals.
	if got, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req); err != nil || len(got.history.Report().Accounts) != 2 {
		t.Fatal("failed alternatives changed reusable originals", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestCapturedCollectionSealDoesNotBypassRevocation(t *testing.T) {
	f, _, _ := automaticHistoryOriginalFixture(t)
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	queries := state.queries.Load()
	if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id=?", f.req.Owner.GroupID).Update("portal_auth_ver", f.req.Owner.AuthVersion+1).Error; err != nil {
		t.Fatal(err)
	}
	files, sealed, err := collectCapturedSources(t.Context(), f.wallet, f.spool, f.req)
	if !errors.Is(err, ErrGenerationChanged) || sealed || files != (CapturedSourceFiles{}) || state.queries.Load() != queries {
		t.Fatal("revoked owner reused a seal or contacted source", files, sealed, err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistorySourcesRefusesLiveOrAliasedDatabase(t *testing.T) {
	for _, kind := range []string{"wal", "hardlink", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			f, files, ref := automaticHistoryOriginalFixture(t)
			body, err := os.ReadFile(files.Material.Path)
			if err != nil {
				t.Fatal(err)
			}
			var m monthlysource.MonthlyObservationMaterial
			if json.Unmarshal(body, &m) != nil {
				t.Fatal("bad fixture")
			}
			switch kind {
			case "wal":
				err = os.WriteFile(m.Database.Path+"-wal", []byte("not a sealed database"), 0600)
			case "hardlink":
				err = os.Link(m.Database.Path, filepath.Join(f.f.root, "database-hardlink"))
			case "symlink":
				original := m.Database.Path + ".original"
				if err = os.Rename(m.Database.Path, original); err == nil {
					err = os.Symlink(original, m.Database.Path)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req); err == nil {
				t.Fatal("live/aliased database treated as an immutable original")
			}
			generationAdmissionCounts(t, f.f.receiver, 0, 0)
		})
	}
}

func TestAutomaticHistorySourcesRejectsIncompleteAndCrossOwnerManifests(t *testing.T) {
	f, files, ref := automaticHistoryOriginalFixture(t)
	raw, err := os.ReadFile(ref.Path)
	if err != nil {
		t.Fatal(err)
	}
	var original automaticHistorySources
	if json.Unmarshal(raw, &original) != nil {
		t.Fatal("bad fixture")
	}
	for _, name := range []string{"empty", "duplicate_month", "wrong_owner", "wrong_source", "wrong_boundary", "wrong_mode", "unrequested_month"} {
		t.Run(name, func(t *testing.T) {
			m := original
			m.Months = append([]CapturedSourceFiles(nil), original.Months...)
			req := f.req
			switch name {
			case "empty":
				m.Months = nil
			case "duplicate_month":
				m.Months = append(m.Months, files)
			case "wrong_owner":
				m.Owner.AuthVersion++
			case "wrong_source":
				m.SourceIdentity = strings.Repeat("f", 64)
			case "wrong_boundary":
				m.Boundary.Selection.SHA256 = strings.Repeat("f", 64)
			case "wrong_mode":
				m.Mode = "approved"
			case "unrequested_month":
				req.Month = "2026-08"
			}
			if _, err := f.f.g.auditAutomaticHistorySources(t.Context(), writeAutomaticHistoryManifest(t, f.f.root, m), req); err == nil {
				t.Fatal("invalid original chain accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.f.g.auditAutomaticHistorySources(ctx, ref, f.req); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled original audit continued", err)
	}
	if _, err := f.f.g.auditAutomaticHistorySources(nil, ref, f.req); !errors.Is(err, ErrGenerationInput) { //nolint:staticcheck // Deliberate invalid-call rejection test.
		t.Fatal(err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistorySourcesRejectsModifiedRawTokenEvenAfterSuccessfulAudit(t *testing.T) {
	f, files, ref := automaticHistoryOriginalFixture(t)
	if _, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(files.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	var m monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(body, &m) != nil {
		t.Fatal("bad material")
	}
	if err := os.WriteFile(m.Tokens[0].File.Path, []byte(`{"approved":true,"consume":0}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req); err == nil {
		t.Fatal("old proof/cache hid changed source money")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}
