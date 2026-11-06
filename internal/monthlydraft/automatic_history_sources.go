package monthlydraft

// LOCAL, read-only bridge between capture/sealing and historical calculation.
// Neither a JSON manifest nor this result selects/publishes a customer bill.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const automaticHistorySourcesMode = "original_month_chain_not_bill_approval"
const automaticHistorySourceID = "automatic-history-originals"

// Server-owned references only; no supplied opening/closing values or approval
// flags. Boundary is one selected month's seal with its paired tail originals;
// another month in this same chain may be the requested bill/dependency month.
// Months must include that exact seal, not an alternative capture of that month.
type automaticHistorySources struct {
	Version        int                   `json:"version"`
	Mode           string                `json:"mode"`
	Owner          monthlytasks.Owner    `json:"owner"`
	SourceIdentity string                `json:"source_identity"`
	Boundary       CapturedSourceFiles   `json:"boundary"`
	Months         []CapturedSourceFiles `json:"months"`
}

// Only the original-file auditor can produce a nonempty result. Reports are
// diagnostic copies, not a replacement for re-auditing the manifest on reuse.
type auditedAutomaticHistory struct {
	policy  *automaticOriginPolicyProof
	history monthlysource.WalletHistory
	tails   monthlysource.SelectedWalletTails
	scope   monthlysource.OrganizationMonthScope
	sources []monitor.MonthlyPreparationSource
	// Retain the already audited target's exact references. A projector must not
	// add aliases for the same files under another namespace.
	months []automaticHistoryFiles
	target int
}

func (auditedAutomaticHistory) MarshalJSON() ([]byte, error) {
	return nil, errors.New("automatic history must be rebuilt from pinned original files")
}

func (a *auditedAutomaticHistory) UnmarshalJSON([]byte) error {
	if a != nil {
		*a = auditedAutomaticHistory{}
	}
	return errors.New("automatic history reports are not admission evidence")
}

type automaticHistoryFiles struct {
	material  monthlysource.MonthlyObservationMaterial
	selection monthlysource.CapturedMonthSelection
	raw       []byte
	sources   []monitor.MonthlyPreparationSource
}

type automaticHistoryReadBudget struct {
	bytes int64
	paths map[string]bool
}

// Reserve by size BEFORE reading token bodies or retaining more month indexes.
// The existing streaming verifier still checks every actual byte/hash later.
func (b *automaticHistoryReadBudget) reserve(g *Generation, refs ...monthlysource.MonthlyObservationFile) error {
	for _, ref := range refs {
		if !generationHex(ref.SHA256, 32) || b.paths[ref.Path] {
			return ErrGenerationIntegrity
		}
		rel, err := g.relativeSource(ref.Path)
		if err != nil {
			return err
		}
		info, err := g.root.Lstat(rel)
		if err != nil || !info.Mode().IsRegular() {
			return ErrGenerationIntegrity
		}
		if info.Size() <= 0 || info.Size() > generationEvidenceLimit-b.bytes {
			return ErrGenerationCapacity
		}
		b.bytes += info.Size()
		b.paths[ref.Path] = true
	}
	return nil
}

// Reopen every sealed database read-only, pin every raw token archive, and
// rebuild the chain. No source connector, editable history, receiver write,
// schema initialization, or financial admission is performed here.
func (g *Generation) auditAutomaticHistorySources(parent context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest) (auditedAutomaticHistory, error) {
	return g.auditAutomaticHistorySourcesMode(parent, ref, req, false)
}

// Separate diagnostic entry. Normalization/runtime callers keep using the
// strict entry above; a signed arithmetic origin is not financial admission.
func (g *Generation) auditAutomaticHistoryResidualSources(parent context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest) (auditedAutomaticHistory, error) {
	return g.auditAutomaticHistorySourcesMode(parent, ref, req, true)
}

func (g *Generation) auditAutomaticHistorySourcesMode(parent context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest, residualReview bool) (auditedAutomaticHistory, error) {
	var empty auditedAutomaticHistory
	if parent == nil || g == nil || g.root == nil || g.observationSource != nil {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	budget := automaticHistoryReadBudget{paths: map[string]bool{}}
	if err := budget.reserve(g, ref); err != nil {
		return empty, err
	}
	raw, err := g.readPrivatePreparationFile(ctx, ref, generationPlanLimit)
	if err != nil {
		return empty, err
	}
	var manifest automaticHistorySources
	if json.Unmarshal(raw, &manifest) != nil {
		return empty, ErrGenerationIntegrity
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(raw, canonical) || manifest.Version != 1 || manifest.Mode != automaticHistorySourcesMode ||
		manifest.Owner != req.Owner || !generationHex(manifest.SourceIdentity, 32) || len(manifest.Months) == 0 || len(manifest.Months) > 120 {
		return empty, ErrGenerationIntegrity
	}
	all := []monitor.MonthlyPreparationSource{{ID: automaticHistorySourceID, Path: ref.Path, SHA256: ref.SHA256}}
	files := make([]automaticHistoryFiles, len(manifest.Months))
	boundaryIndex, targetIndex := -1, -1
	for i, m := range manifest.Months {
		files[i], err = g.automaticHistoryMonthFiles(ctx, m, req.Owner, manifest.SourceIdentity, i, &budget)
		if err != nil {
			return empty, err
		}
		if i > 0 && files[i-1].selection.Scope.Funding.Month >= files[i].selection.Scope.Funding.Month {
			return empty, ErrGenerationIntegrity
		}
		if files[i].selection.Scope.Funding.Month == req.Month {
			targetIndex = i
		}
		if m == manifest.Boundary {
			boundaryIndex = i
		}
		all = append(all, files[i].sources...)
	}
	if boundaryIndex < 0 || targetIndex < 0 {
		return empty, ErrGenerationIntegrity
	}
	// Recheck the cumulative size while hashing. Hard-link aliases are also
	// refused by the private-file/database readers before source use.
	if err := g.verifyFiles(ctx, all); err != nil {
		return empty, err
	}
	out := auditedAutomaticHistory{sources: all, scope: files[targetIndex].selection.Scope, months: files, target: targetIndex}
	months := make([]monthlysource.WalletHistoryMonth, 0, len(files))
	var closing monthlysource.WalletHistoryBoundary
	for i, file := range files {
		month, boundary, tails, err := g.reviewAutomaticHistoryMonth(ctx, file, i == boundaryIndex)
		if err != nil {
			return empty, err
		}
		months = append(months, month)
		if i == boundaryIndex {
			closing, out.tails = boundary, tails
		}
	}
	if residualReview {
		out.history, err = monthlysource.ReviewWalletHistoryOriginResidual(ctx, closing, months)
	} else {
		out.history, err = monthlysource.DeriveWalletHistory(ctx, closing, months)
	}
	if err != nil || !out.history.MatchesCollectionScope(out.scope) {
		return empty, errors.Join(ErrGenerationEvidence, err)
	}
	// Detect changes while composing another month as well as at each read.
	if err := g.verifyFiles(ctx, all); err != nil {
		return empty, err
	}
	return out, ctx.Err()
}

func (g *Generation) automaticHistoryMonthFiles(ctx context.Context, files CapturedSourceFiles, owner monthlytasks.Owner, source string, ordinal int, budget *automaticHistoryReadBudget) (automaticHistoryFiles, error) {
	var empty automaticHistoryFiles
	if files.Mode != capturedSourceFilesMode {
		return empty, ErrGenerationIntegrity
	}
	if err := budget.reserve(g, files.Material, files.Selection); err != nil {
		return empty, err
	}
	raw, err := g.readPrivatePreparationFile(ctx, files.Material, generationPlanLimit)
	if err != nil {
		return empty, err
	}
	var m monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(raw, &m) != nil {
		return empty, ErrGenerationIntegrity
	}
	if err := budget.reserve(g, m.Database); err != nil {
		return empty, err
	}
	for _, token := range m.Tokens {
		if err := budget.reserve(g, token.File); err != nil {
			return empty, err
		}
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(raw, canonical) || m.Version != 2 || m.Mode != monthlysource.PartitionedObservationMaterialMode ||
		m.UsageSelection == nil || *m.UsageSelection != files.Selection || m.WalletReuse != nil || len(m.UsageWindows) != 0 ||
		m.FundingVersion != 2 || m.GroupID != owner.GroupID || m.Funding.SourceIdentity != source || len(m.Funding.UserIDs) == 0 ||
		len(m.Funding.UserIDs) > 200 || len(m.Tokens) != len(m.Funding.UserIDs) {
		return empty, ErrGenerationIntegrity
	}
	selectionRaw, err := g.readPrivatePreparationFile(ctx, files.Selection, generationPlanLimit)
	if err != nil {
		return empty, err
	}
	var s monthlysource.CapturedMonthSelection
	if json.Unmarshal(selectionRaw, &s) != nil {
		return empty, ErrGenerationIntegrity
	}
	canonical, err = json.Marshal(s)
	scopeRaw, scopeErr := json.Marshal(s.Scope)
	_, decodeErr := monthlysource.DecodeOrganizationMonthScope(scopeRaw, time.Now())
	gotOwner := monthlytasks.Owner{GroupID: s.Scope.GroupID, AuthVersion: s.Scope.AuthVersion, MemberFingerprint: s.Scope.MemberFingerprint, MemberManifest: s.Scope.MemberManifest}
	if err != nil || scopeErr != nil || decodeErr != nil || !bytes.Equal(selectionRaw, canonical) || gotOwner != owner ||
		s.Key != (SourcePreparationRequest{Owner: owner, Month: s.Scope.Funding.Month}).Key() ||
		!reflect.DeepEqual(m.Funding, s.Scope.Funding) || !slices.Equal(m.FundingWindows, s.Funding) ||
		!slices.Equal(m.WalletIDs, s.WalletIDs) || len(s.Tokens) != len(m.Tokens) {
		return empty, ErrGenerationIntegrity
	}
	prefix := fmt.Sprintf("automatic-history-%03d-", ordinal)
	out := automaticHistoryFiles{material: m, selection: s, raw: selectionRaw, sources: []monitor.MonthlyPreparationSource{
		{ID: prefix + "material", Path: files.Material.Path, SHA256: files.Material.SHA256},
		{ID: prefix + "selection", Path: files.Selection.Path, SHA256: files.Selection.SHA256},
		{ID: prefix + "database", Path: m.Database.Path, SHA256: m.Database.SHA256},
	}}
	for i, token := range m.Tokens {
		if token.UserID != m.Funding.UserIDs[i] || token.UserID != s.Tokens[i].UserID {
			return empty, ErrGenerationIntegrity
		}
		out.sources = append(out.sources, monitor.MonthlyPreparationSource{ID: fmt.Sprintf("%stoken-%d", prefix, token.UserID), Path: token.File.Path, SHA256: token.File.SHA256})
	}
	return out, nil
}

func (g *Generation) reviewAutomaticHistoryMonth(ctx context.Context, file automaticHistoryFiles, boundary bool) (monthlysource.WalletHistoryMonth, monthlysource.WalletHistoryBoundary, monthlysource.SelectedWalletTails, error) {
	var month monthlysource.WalletHistoryMonth
	var closing monthlysource.WalletHistoryBoundary
	var tails monthlysource.SelectedWalletTails
	dbSource := file.sources[2]
	scoped, release, err := g.bindObservationStore(ctx, GenerationPlan{ObservationStoreSourceID: dbSource.ID}, monitor.MonthlyPreparationInput{Sources: file.sources})
	if err != nil {
		return month, closing, tails, err
	}
	defer release()
	tokens := make([]monthlysource.CapturedTokenArchive, 0, len(file.material.Tokens))
	for _, token := range file.material.Tokens {
		raw, err := g.readPrivatePreparationFile(ctx, token.File, monthlysource.TokenUsageArchiveLimit)
		if err != nil {
			return month, closing, tails, err
		}
		tokens = append(tokens, monthlysource.CapturedTokenArchive{UserID: token.UserID, Raw: raw})
	}
	month, err = monthlysource.ReviewCapturedWalletHistoryMonth(ctx, scoped.funding, file.raw, tokens)
	if err == nil && boundary {
		closing, err = monthlysource.ReviewCapturedWalletHistoryBoundary(ctx, scoped.funding, file.raw)
		if err == nil {
			tails, err = monthlysource.ReadSelectedWalletTails(ctx, scoped.funding, file.raw)
		}
		if err == nil {
			_, registration, readErr := monthlysource.ReadSelectedRegisteredWallet(ctx, scoped.funding, file.raw)
			if readErr != nil {
				err = readErr
			} else if len(registration.Users) != len(tails.Pairs) {
				err = ErrGenerationIntegrity
			} else {
				for i, pair := range tails.Pairs {
					if registration.Users[i].UserID != pair.Newer.User.UserID || registration.Users[i].RegisteredAt != pair.Newer.User.RegisteredAt {
						err = ErrGenerationIntegrity
						break
					}
				}
			}
		}
	}
	if err == nil {
		err = scoped.verifyBoundObservationStore(ctx, file.sources)
	}
	return month, closing, tails, err
}
