package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const organizationHistorySourceID = "organization-history-catalogue"
const organizationRosterSourceID = "organization-current-roster"
const organizationScopeCatalogueID = "organization-scope-catalogue"

func validOrganizationHistoryForm(r ObservationMaterialPreparationRequest) bool {
	if r.History == nil && r.Organization == nil {
		return len(r.Template) != 0
	}
	return r.History != nil && r.Organization != nil && len(r.Template) == 0 &&
		generationHex(r.History.SHA256, 32) && generationHex(r.Organization.SHA256, 32)
}

// Shared private, pinned, bounded reader. All returned files also become normal
// preparation sources and are rehashed by the existing final-write checks.
func (g *Generation) readPrivatePreparationFile(ctx context.Context, source monthlysource.MonthlyObservationFile, limit int64) ([]byte, error) {
	if !generationHex(source.SHA256, 32) {
		return nil, ErrGenerationInput
	}
	rel, err := g.relativeSource(source.Path)
	if err != nil {
		return nil, err
	}
	for name := filepath.Dir(rel); name != "."; name = filepath.Dir(name) {
		info, err := g.root.Lstat(name)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return nil, ErrGenerationIntegrity
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(os.Geteuid()) {
			return nil, ErrGenerationIntegrity
		}
	}
	before, err := g.root.Lstat(rel)
	if err != nil {
		return nil, ErrGenerationIntegrity
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return nil, ErrGenerationIntegrity
	}
	raw, err := readArchiveFile(ctx, g.root, rel, limit)
	if err != nil {
		return nil, err
	}
	after, err := g.root.Lstat(rel)
	if err != nil || generationHash(raw) != source.SHA256 || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, ErrGenerationIntegrity
	}
	return raw, ctx.Err()
}

// Select only previously retained history for the full CURRENT roster. Keep
// the independently supplied historical scope intact. Never derive an opening
// balance, a wallet-only assertion or a historical member from current rights.
func (g *Generation) organizationHistoryTemplate(ctx context.Context, r ObservationMaterialPreparationRequest,
	material monthlysource.MonthlyObservationMaterial, scope RetainedAutomaticScope) ([]byte, error) {
	historyRaw, err := g.readPrivatePreparationFile(ctx, *r.History, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return nil, err
	}
	rosterLimit := int64(monthlysource.OrganizationMonthScopeLimit)
	if r.capturedRosterKey != "" {
		rosterLimit = 2 << 20
	}
	rosterRaw, err := g.readPrivatePreparationFile(ctx, *r.Organization, rosterLimit)
	if err != nil {
		return nil, err
	}
	if r.capturedRosterKey != "" {
		var selection monthlysource.CapturedMonthSelection
		if json.Unmarshal(rosterRaw, &selection) != nil {
			return nil, ErrGenerationIntegrity
		}
		canonical, err := json.Marshal(selection)
		legacy := selection.Version == 1 && selection.Mode == monthlysource.CapturedMonthSelectionMode
		recovered := selection.Version == 2 && selection.Mode == monthlysource.RecoveredCapturedMonthSelectionMode
		if err != nil || !bytes.Equal(rosterRaw, canonical) || (!legacy && !recovered) || selection.Key != r.capturedRosterKey || !generationHex(selection.PlanSHA256, 32) {
			return nil, ErrGenerationIntegrity
		}
		if recovered || material.UsageSelection != nil {
			if err := g.verifyRecoveredHistorySelection(ctx, rosterRaw, selection, material); err != nil {
				return nil, err
			}
		}
		rosterRaw, err = json.Marshal(selection.Scope)
		if err != nil {
			return nil, err
		}
	}
	roster, err := monthlysource.DecodeOrganizationMonthScope(rosterRaw, time.Now())
	if err != nil {
		return nil, ErrGenerationInput
	}
	owner := monthlytasks.Owner{GroupID: roster.GroupID, AuthVersion: roster.AuthVersion,
		MemberFingerprint: roster.MemberFingerprint, MemberManifest: roster.MemberManifest}
	if owner != scope.Owner || roster.Funding.Month != material.Funding.Month || roster.GroupID != material.GroupID ||
		roster.Funding.SourceIdentity != material.Funding.SourceIdentity || roster.Funding.Rules != material.Funding.Rules ||
		!slices.Equal(roster.Funding.UserIDs, material.Funding.UserIDs) {
		return nil, ErrGenerationEvidence
	}
	assembled := r.historyAssembly != nil
	if assembled && (r.scopeCatalogue != nil || r.capturedRosterKey == "" || !r.historyAssembly.matches(owner, scope.Month, material.Funding.SourceIdentity, *r.History, r.Material, *r.Organization, r.Scope)) {
		return nil, ErrGenerationEvidence
	}
	if r.scopeCatalogue != nil {
		original, err := g.readPrivatePreparationFile(ctx, *r.scopeCatalogue, monitor.MonthlyPreparationInputLimit)
		if err != nil {
			return nil, err
		}
		selectedScope, err := selectCapturedHistoryScope(original, material.Funding.SourceIdentity, owner, roster.Funding.Month, roster.Funding.UserIDs)
		if err != nil {
			return nil, err
		}
		want, err := json.Marshal(selectedScope)
		if err != nil || !bytes.Equal(want, bytes.TrimSpace(r.Scope)) {
			return nil, ErrGenerationEvidence
		}
	}
	catalogue, err := monitor.DecodeMonthlyPreparationInput(historyRaw)
	if err != nil {
		return nil, err
	}
	byID := map[int64]monitor.MonthlyPreparationAccount{}
	for _, account := range catalogue.Accounts {
		byID[account.UserID] = account
	}
	selected := monitor.MonthlyPreparationInput{Version: 1, Accounts: []monitor.MonthlyPreparationAccount{}}
	month, err := time.ParseInLocation("2006-01", roster.Funding.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return nil, ErrGenerationInput
	}
	end := month.AddDate(0, 1, 0).Unix()
	for _, id := range roster.Funding.UserIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		account, exists := byID[id]
		if !exists || account.GroupID != owner.GroupID {
			return nil, ErrGenerationEvidence
		}
		if !assembled && !slices.Contains(account.Months, roster.Funding.Month) {
			beforeRegistration := false
			for _, meta := range scope.Scope.Accounts {
				r := meta.Registration
				beforeRegistration = beforeRegistration || (meta.UserID == id && r != nil && r.At >= end && r.At >= account.Baseline.At &&
					futureZeroOrigin(account, end) && r.Source != "" && r.NoEarlierActivitySource != "")
			}
			if !beforeRegistration {
				return nil, ErrGenerationEvidence
			}
		}
		// Only month selection changes. No monetary record, counter, cutoff or
		// history bridge is rebuilt or edited. Other-month optional metadata is
		// outside this single-month preparation, not merged into this month.
		account.Months = []string{roster.Funding.Month}
		account.WalletMonths = slices.DeleteFunc(slices.Clone(account.WalletMonths), func(m monitor.MonthlyPreparationWalletMonth) bool { return m.Month != roster.Funding.Month })
		account.TokenMonths = slices.DeleteFunc(slices.Clone(account.TokenMonths), func(m monitor.MonthlyBillTokenUsage) bool { return m.Month != roster.Funding.Month })
		account.MonthStatistics = slices.DeleteFunc(slices.Clone(account.MonthStatistics), func(m monitor.MonthlyPreparationStatistics) bool { return m.Month != roster.Funding.Month })
		selected.Accounts = append(selected.Accounts, account)
	}
	refs, digests := organizationHistoryRefs(selected.Accounts, scope.Scope)
	if assembled {
		// Only the producer's directory/classification references may use the
		// two sources added by the common captured-material path below. Existing
		// financial history cannot borrow those new sources as balance evidence.
		originalRefs, _ := organizationHistoryRefs(selected.Accounts, AutomaticScope{})
		if originalRefs[organizationRosterSourceID] || originalRefs[observationMaterialSourceID] {
			return nil, ErrGenerationEvidence
		}
		delete(refs, organizationRosterSourceID)
		delete(refs, observationMaterialSourceID)
	}
	for _, source := range catalogue.Sources {
		// Reserved IDs cannot be smuggled into unrelated catalogue entries.
		if source.ID == organizationHistorySourceID || source.ID == organizationRosterSourceID || (r.scopeCatalogue != nil && source.ID == organizationScopeCatalogueID) {
			return nil, ErrGenerationInput
		}
		if refs[source.ID] || digests[source.SHA256] {
			selected.Sources = append(selected.Sources, source)
			delete(refs, source.ID)
			delete(digests, source.SHA256)
		}
	}
	if len(refs) != 0 || len(digests) != 0 {
		return nil, ErrGenerationEvidence
	}
	selected.Sources = append(selected.Sources,
		monitor.MonthlyPreparationSource{ID: organizationHistorySourceID, Path: r.History.Path, SHA256: r.History.SHA256},
		monitor.MonthlyPreparationSource{ID: organizationRosterSourceID, Path: r.Organization.Path, SHA256: r.Organization.SHA256})
	if r.scopeCatalogue != nil {
		selected.Sources = append(selected.Sources, monitor.MonthlyPreparationSource{ID: organizationScopeCatalogueID, Path: r.scopeCatalogue.Path, SHA256: r.scopeCatalogue.SHA256})
	}
	raw, err := json.Marshal(selected)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return nil, ErrGenerationCapacity
	}
	return raw, ctx.Err()
}

// V2 is accepted only through this private captured-roster path. Re-audit the
// hash-pinned sealed DB and exact material selection, not just its scope field.
func (g *Generation) verifyRecoveredHistorySelection(ctx context.Context, raw []byte, s monthlysource.CapturedMonthSelection, m monthlysource.MonthlyObservationMaterial) error {
	versions := []monthlysource.UsageWindowVersion{}
	if len(s.Tokens) != len(m.Tokens) {
		return ErrGenerationEvidence
	}
	for i, a := range s.Tokens {
		if a.UserID != m.Tokens[i].UserID {
			return ErrGenerationEvidence
		}
		versions = append(versions, a.Usage...)
	}
	partitioned := m.Version == 2 && m.Mode == monthlysource.PartitionedObservationMaterialMode && m.UsageSelection != nil
	if !partitioned && !reflect.DeepEqual(versions, m.UsageWindows) || !reflect.DeepEqual(s.Funding, m.FundingWindows) || !reflect.DeepEqual(s.WalletIDs, m.WalletIDs) {
		return ErrGenerationEvidence
	}
	if partitioned && (len(m.UsageWindows) != 0 || m.UsageSelection.SHA256 != generationHash(raw)) {
		return ErrGenerationEvidence
	}
	source := monitor.MonthlyPreparationSource{ID: observationMaterialDatabaseID, Path: m.Database.Path, SHA256: m.Database.SHA256}
	input := monitor.MonthlyPreparationInput{Sources: []monitor.MonthlyPreparationSource{source}}
	bound, release, err := g.bindObservationStore(ctx, GenerationPlan{ObservationStoreSourceID: source.ID}, input)
	if err != nil {
		return err
	}
	defer release()
	verify := func() error { return monthlysource.VerifyCapturedObservationSelection(ctx, bound.funding, raw) }
	if partitioned {
		verify = func() error {
			parts, err := monthlysource.PartitionCapturedUsageIndex(ctx, raw, time.Now())
			if err != nil {
				return err
			}
			return monthlysource.VerifyPartitionedCapturedObservationSelection(ctx, bound.funding, generationHash(raw), s.Scope, parts)
		}
	}
	if err := verify(); err != nil {
		return err
	}
	return bound.verifyBoundObservationStore(ctx, input.Sources)
}

// Closed list of existing evidence references; do not treat arbitrary names,
// file paths or CURRENT roster metadata as historical source references.
func organizationHistoryRefs(accounts []monitor.MonthlyPreparationAccount, scope AutomaticScope) (map[string]bool, map[string]bool) {
	refs := map[string]bool{}
	digests := map[string]bool{}
	add := func(ref string) {
		if ref != "" {
			id, _, _ := strings.Cut(ref, "#")
			refs[id] = true
		}
	}
	balance := func(b monitor.MonthlyPreparationBalance) { add(b.Source); add(b.CutoffSource) }
	for _, a := range accounts {
		add(a.BasisSource)
		balance(a.Baseline)
		for _, b := range a.Snapshots {
			balance(b)
		}
		for _, s := range a.Spans {
			add(s.Source)
		}
		for _, m := range a.Movements {
			add(m.Source)
		}
		for _, c := range a.MovementCoverage {
			add(c.Source)
		}
		for _, m := range a.WalletMonths {
			add(m.Source)
		}
		for _, m := range a.TokenMonths {
			add(m.Source)
		}
		for _, s := range a.MonthStatistics {
			add(s.Source)
		}
	}
	add(scope.Group.Source)
	for _, a := range scope.Accounts {
		add(a.WalletSource)
		if a.Registration != nil {
			add(a.Registration.Source)
			add(a.Registration.NoEarlierActivitySource)
		}
		for _, r := range a.Reconstructions {
			if r.HistoricalBalance != nil {
				add(r.HistoricalBalance.DecisionSource)
				digests[r.HistoricalBalance.DecisionSHA256] = true
			}
			for _, ref := range []string{r.SnapshotSource, r.ScopeSource, r.MonthSource, r.BridgeSource, r.FundingSource, r.OpeningBasisSource} {
				add(ref)
			}
		}
	}
	for _, c := range scope.Corrections {
		for _, o := range c.Evidence.Observations {
			digests[o.SourceSHA256] = true
		}
	}
	return refs, digests
}
