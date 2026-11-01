package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func useOrganizationHistoryFixture(t *testing.T, f generationFixture, cfg DeploymentConfig, r *ObservationMaterialPreparationRequest) monitor.MonthlyPreparationInput {
	t.Helper()
	catalogue, err := monitor.DecodeMonthlyPreparationInput(r.Template)
	if err != nil {
		t.Fatal(err)
	}
	for i := range catalogue.Accounts {
		catalogue.Accounts[i].Months = []string{"2026-08", f.plan.Funding.Month}
	}
	// An unrelated account's unavailable file must not become this customer's
	// history dependency. Its mere presence must not drop a required member.
	other := catalogue.Accounts[0]
	other.UserID, other.GroupID, other.Username = 919191, 81717, "unrelated synthetic account"
	other.Baseline.UserID, other.Baseline.Source = other.UserID, "unrelated#baseline"
	other.BasisSource = "unrelated#basis"
	other.Spans = []monitor.MonthlyPreparationSpan{}
	other.Movements = []monitor.MonthlyPreparationMovement{}
	other.MovementCoverage = []monitor.MonthlyPreparationCoverage{}
	other.Snapshots = []monitor.MonthlyPreparationBalance{}
	catalogue.Accounts = append(catalogue.Accounts, other)
	slices.Reverse(catalogue.Accounts)
	catalogue.Sources = append(catalogue.Sources, monitor.MonthlyPreparationSource{ID: "unrelated", Path: filepath.Join(f.root, "never-created.private"), SHA256: strings.Repeat("a", 64)})
	path := filepath.Join(f.root, "history-catalogue.json")
	raw := draftJSON(t, catalogue)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	r.History = &monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)}
	r.Template = nil
	root := filepath.Join(f.root, "current-roster")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := filepath.EvalSymlinks(cfg.AuthorityPath)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := PrepareOrganizationCollection(t.Context(), OrganizationCollectionRequest{AuthorityPath: authority, OutputRoot: canonicalRoot,
		GroupID: f.plan.Owner.GroupID, Month: f.plan.Funding.Month, SourceIdentity: f.plan.Funding.SourceIdentity, Write: true})
	if err != nil || !receipt.Complete {
		t.Fatal("cannot produce real roster fixture", err)
	}
	r.Organization = &monthlysource.MonthlyObservationFile{Path: filepath.Join(root, "organization.json"), SHA256: receipt.ScopeSHA256}
	return catalogue
}

func TestOrganizationHistorySelectionPreservesMoneyAndCompleteScope(t *testing.T) {
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	catalogue := useOrganizationHistoryFixture(t, f, cfg, &r)
	before := map[string]string{}
	for _, path := range []string{r.History.Path, r.Organization.Path, r.Material.Path, cfg.AuthorityPath, cfg.ReceiverPath, f.path} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	preview, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || preview.Complete || preview.Directory != "" {
		t.Fatal("history preview failed", err)
	}
	again, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || !reflect.DeepEqual(preview, again) {
		t.Fatal("history selection not stable", err)
	}
	entries, err := os.ReadDir(r.OutputRoot)
	if err != nil || len(entries) != 0 {
		t.Fatal("default inspection wrote files", err)
	}
	preparationCounts(t, f, 0, 0)
	r.Write = true
	receipt, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || !receipt.Complete || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal("history cannot enter existing prepared format", err)
	}
	raw, err := os.ReadFile(filepath.Join(r.OutputRoot, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil || len(input.Accounts) != len(f.plan.Funding.UserIDs) {
		t.Fatal("incomplete or foreign members", err)
	}
	byID := map[int64]monitor.MonthlyPreparationAccount{}
	for _, a := range catalogue.Accounts {
		byID[a.UserID] = a
	}
	for i, a := range input.Accounts {
		old := byID[a.UserID]
		if a.UserID != f.plan.Funding.UserIDs[i] {
			t.Fatal("accounts selected by row position, not ID")
		}
		old.Months = []string{f.plan.Funding.Month}
		// These additions must come only from the existing observed normalizers.
		a.Spans, a.Movements, a.TokenMonths = old.Spans, old.Movements, old.TokenMonths
		a.Snapshots = a.Snapshots[:len(old.Snapshots)]
		if !reflect.DeepEqual(a, old) {
			t.Fatal("history balance, counter or source was rewritten", a.UserID)
		}
	}
	found := map[string]bool{}
	for _, s := range input.Sources {
		if s.ID == "unrelated" {
			t.Fatal("unrelated missing file still selected")
		}
		found[s.ID] = true
	}
	if !found[organizationHistorySourceID] || !found[organizationRosterSourceID] {
		t.Fatal("original catalogue or roster digest not bound to input")
	}
	plan, err := os.ReadFile(filepath.Join(r.OutputRoot, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	if checked, err := PrepareGenerationInput(t.Context(), cfg, plan, raw, "", false); err != nil || checked.Applied {
		t.Fatal("history result rejected by existing staging inspector", err)
	}
	for path, sha := range before {
		body, err := os.ReadFile(path)
		if err != nil || generationHash(body) != sha {
			t.Fatal("source changed", path, err)
		}
	}
	preparationCounts(t, f, 0, 0)
	// Explicit test-only activation, then the existing audit and exact per-user
	// assertions. Selection itself never activates or creates a customer task.
	if _, err := PrepareGenerationInput(t.Context(), cfg, plan, raw, "", true); err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: plan, Preparation: raw})
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact, 1)
}

func TestOrganizationHistorySelectionRejectsMissingOrConflictingEvidence(t *testing.T) {
	for _, kind := range []string{"mixed_template", "no_roster", "no_history", "bad_digest", "roster_digest", "missing_member", "wrong_group", "missing_month", "duplicate_member", "missing_reference", "reserved_id", "changed_balance", "missing_history_bridge", "collection_cutoff", "bad_wallet", "source_hash", "roster_owner", "roster_users", "roster_month", "roster_source", "scope_members", "roster_as_history", "catalogue_as_history", "self_history", "public_history", "public_roster", "symlink", "parent_symlink", "hardlink", "unknown", "duplicate_field", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r, _ := observationMaterialPreparationFixture(t)
			catalogue := useOrganizationHistoryFixture(t, f, cfg, &r)
			var roster monthlysource.OrganizationMonthScope
			body, err := os.ReadFile(r.Organization.Path)
			if err != nil || json.Unmarshal(body, &roster) != nil {
				t.Fatal("bad fixture", err)
			}
			var scope RetainedAutomaticScope
			if json.Unmarshal(r.Scope, &scope) != nil {
				t.Fatal("bad scope fixture")
			}
			// In the reversed fixture, account 1 is a required original member.
			a := &catalogue.Accounts[1]
			ctx := context.Background()
			switch kind {
			case "mixed_template":
				r.Template = []byte("{}")
			case "no_roster":
				r.Organization = nil
			case "no_history":
				r.History = nil
			case "missing_member":
				catalogue.Accounts = catalogue.Accounts[:2]
			case "wrong_group":
				a.GroupID++
			case "missing_month":
				a.Months = []string{"2026-08"}
			case "duplicate_member":
				catalogue.Accounts = append(catalogue.Accounts, *a)
			case "missing_reference":
				a.Baseline.Source = "missing#baseline"
			case "reserved_id":
				catalogue.Sources = append(catalogue.Sources, monitor.MonthlyPreparationSource{ID: organizationRosterSourceID, Path: filepath.Join(f.root, "reserved"), SHA256: strings.Repeat("b", 64)})
			case "changed_balance":
				a.Baseline.BalanceQuota++
			case "missing_history_bridge":
				a.Baseline.At -= 3600
			case "collection_cutoff":
				a.Baseline.TimeBasis = "collection_observation"
			case "bad_wallet":
				a.WalletMonths = []monitor.MonthlyPreparationWalletMonth{{UserID: a.UserID, Month: f.plan.Funding.Month, RefundQuota: 1, Source: a.BasisSource}}
			case "source_hash":
				catalogue.Sources[0].SHA256 = strings.Repeat("e", 64)
			case "roster_owner":
				roster.AuthVersion++
			case "roster_users":
				roster.Funding.UserIDs = []int64{201}
				roster.MemberFingerprint = generationMemberHash(roster.Funding.UserIDs)
			case "roster_month":
				roster.Funding.Month = "2026-08"
			case "roster_source":
				roster.Funding.SourceIdentity = strings.Repeat("c", 64)
			case "scope_members":
				scope.Scope.Group.Members = scope.Scope.Group.Members[:1]
			case "roster_as_history":
				scope.Scope.Group.Source = organizationRosterSourceID + "#not-historical-membership"
			case "catalogue_as_history":
				a.Baseline.Source = organizationHistorySourceID + "#not-independent-balance"
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw := draftJSON(t, catalogue)
			if kind == "unknown" {
				raw = append(bytes.TrimSuffix(raw, []byte("}")), []byte(`,"unknown":true}`)...)
			}
			if kind == "duplicate_field" {
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			}
			if r.History != nil {
				if err := os.WriteFile(r.History.Path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				r.History.SHA256 = generationHash(raw)
			}
			if r.Organization != nil {
				raw := materialPreparationJSON(t, roster)
				if err := os.WriteFile(r.Organization.Path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				r.Organization.SHA256 = generationHash(raw)
			}
			r.Scope = materialPreparationJSON(t, scope)
			switch kind {
			case "bad_digest":
				r.History.SHA256 = strings.Repeat("f", 64)
			case "roster_digest":
				r.Organization.SHA256 = strings.Repeat("f", 64)
			case "self_history":
				r.History = &monthlysource.MonthlyObservationFile{Path: r.Organization.Path, SHA256: r.Organization.SHA256}
			case "public_history":
				if err := os.Chmod(r.History.Path, 0644); err != nil {
					t.Fatal(err)
				}
			case "public_roster":
				if err := os.Chmod(r.Organization.Path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink", "hardlink":
				path := filepath.Join(f.root, "history-alias")
				var err error
				if kind == "symlink" {
					err = os.Symlink(r.History.Path, path)
				} else {
					err = os.Link(r.History.Path, path)
				}
				if err != nil {
					t.Fatal(err)
				}
				r.History.Path = path
			case "parent_symlink":
				path := filepath.Join(f.root, "roster-alias")
				if err := os.Symlink(filepath.Dir(r.Organization.Path), path); err != nil {
					t.Fatal(err)
				}
				r.Organization.Path = filepath.Join(path, "organization.json")
			}
			r.Write = true
			receipt, err := PrepareObservationMaterialFiles(ctx, cfg, r)
			if err == nil || receipt.Complete || receipt.Directory != "" {
				t.Fatal("invalid evidence reached output", kind, receipt, err)
			}
			entries, readErr := os.ReadDir(r.OutputRoot)
			if readErr != nil || len(entries) != 0 {
				t.Fatal("invalid history wrote partial output", readErr)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestOrganizationHistoryLateChangeNeverCompletes(t *testing.T) {
	for _, kind := range []string{"history", "roster", "member", "disk"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r, _ := observationMaterialPreparationFixture(t)
			useOrganizationHistoryFixture(t, f, cfg, &r)
			r.Write = true
			write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				switch kind {
				case "history":
					return os.WriteFile(r.History.Path, []byte("changed history"), 0600)
				case "roster":
					return os.WriteFile(r.Organization.Path, []byte("changed roster"), 0600)
				case "member":
					_, err := generationDB(t, cfg.AuthorityPath, false).Exec("UPDATE usage_member_controls SET tracked_revision=tracked_revision+1 WHERE user_id=201")
					return err
				default:
					return errors.New("synthetic disk full")
				}
			}
			receipt, err := prepareObservationMaterialFiles(t.Context(), cfg, r, write)
			if err == nil || receipt.Complete || receipt.Directory != r.OutputRoot {
				t.Fatal("late change completed", receipt, err)
			}
			if _, err := os.Stat(filepath.Join(r.OutputRoot, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("late change left final marker", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func testOrganizationHistoryCLI(t *testing.T, binary string, populated ...bool) {
	t.Helper()
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	useOrganizationHistoryFixture(t, f, cfg, &r)
	if len(populated) != 0 && populated[0] {
		fillRetainedHistoryMonth(t, f, &r, true)
	}
	scopePath := filepath.Join(f.root, "history-scope.json")
	if err := os.WriteFile(scopePath, r.Scope, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"prepare", "-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id=" + strconv.FormatInt(f.plan.Owner.GroupID, 10), "-month=" + f.plan.Funding.Month,
		"-material", r.Material.Path, "-material-sha256", r.Material.SHA256, "-history", r.History.Path, "-history-sha256", r.History.SHA256,
		"-organization-scope", r.Organization.Path, "-expected-organization", r.Organization.SHA256, "-scope", scopePath,
		"-scope-source", r.ScopeSourceID, "-output-root", r.OutputRoot}
	for _, extra := range [][]string{{"-template=not-used"}, {"-history-sha256="}, {"-expected-organization="}, {"-organization-scope="}, {"-group-id=2"}, {"-month=2026-08"}, {"-apply"}} {
		raw, err := exec.CommandContext(t.Context(), binary, append(slices.Clone(args), extra...)...).CombinedOutput()
		if err == nil || bytes.Contains(raw, []byte(f.root)) {
			t.Fatal("invalid CLI accepted or private path leaked", err)
		}
	}
	var preview AutomaticPreparationReceipt
	for _, write := range []bool{false, true} {
		current := slices.Clone(args)
		if write {
			current = append(current, "-write")
		}
		raw, err := exec.CommandContext(t.Context(), binary, current...).CombinedOutput()
		if err != nil {
			t.Fatalf("history CLI: %v %s", err, raw)
		}
		var receipt AutomaticPreparationReceipt
		if json.Unmarshal(raw, &receipt) != nil || receipt.Complete != write {
			t.Fatal("history CLI bad receipt")
		}
		if !write {
			preview = receipt
		} else if preview.ManifestSHA256 != receipt.ManifestSHA256 {
			t.Fatal("write differs from inspected history")
		}
		preparationCounts(t, f, 0, 0)
	}
}
