package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// CapturedHistoryCatalogue contains independently established ORIGINAL scope
// metadata. It is a private server source, not customer-editable approval.
// Entries have exactly one month: historical corrections and boundaries are
// never copied across months. Current rights cannot establish wallet history.
type CapturedHistoryCatalogue struct {
	Version        int              `json:"version"`
	SourceIdentity string           `json:"source_identity"`
	Scopes         []AutomaticScope `json:"scopes"`
}

// NewCatalogueCapturedHistoryPreparation removes per-request scope-file
// construction. It selects original metadata for the full authorized roster,
// retains a hash-bound envelope, then uses the SAME audited preparation/CAS.
// No I/O, source collection, task, schema or publication occurs at construction.
func NewCatalogueCapturedHistoryPreparation(d *Deployment, spool *CapturedSourceSpool, scopeID string,
	history, catalogue monthlysource.MonthlyObservationFile) (func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error), error) {
	return newCatalogueCapturedHistoryPreparation(d, spool, scopeID, history, catalogue, false)
}

func newCatalogueCapturedHistoryPreparation(d *Deployment, spool *CapturedSourceSpool, scopeID string,
	history, catalogue monthlysource.MonthlyObservationFile, assemble bool) (func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error), error) {
	if d == nil || d.g == nil || spool == nil || spool.wallet == nil || spool.wallet.funding == nil || spool.identity == nil {
		return nil, ErrGenerationInput
	}
	if assemble && (d.runtime == nil || d.runtime.scope == nil || !d.runtime.scope.registered || d.runtime.worker == nil || !d.runtime.worker.dynamicSeries) {
		return nil, ErrGenerationInput
	}
	refs := []monthlysource.MonthlyObservationFile{history}
	hasCatalogue := catalogue.Path != "" || catalogue.SHA256 != ""
	if !assemble || hasCatalogue {
		refs = append(refs, catalogue)
	}
	for _, ref := range refs {
		if !generationHex(ref.SHA256, 32) {
			return nil, ErrGenerationInput
		}
		if _, err := d.g.relativeSource(ref.Path); err != nil {
			return nil, err
		}
	}
	if history.Path == catalogue.Path {
		return nil, ErrGenerationInput
	}
	if _, err := d.g.relativeSource(filepath.Join(spool.directory, "boundary-check")); err != nil {
		return nil, err
	}
	return newCapturedHistoryPreparation(d, scopeID, func(ctx context.Context, req SourcePreparationRequest, files CapturedSourceFiles) (CapturedHistorySources, error) {
		current, err := spool.wallet.funding.scope(ctx, req)
		if err != nil {
			return CapturedHistorySources{}, err
		}
		var document RetainedAutomaticScope
		var originalCatalogue *monthlysource.MonthlyObservationFile
		if hasCatalogue {
			raw, err := d.g.readPrivatePreparationFile(ctx, catalogue, monitor.MonthlyPreparationInputLimit)
			if err != nil {
				return CapturedHistorySources{}, err
			}
			document, err = selectCapturedHistoryScope(raw, current.Funding.SourceIdentity, req.Owner, req.Month, current.Funding.UserIDs)
			if err != nil && (!assemble || !errors.Is(err, ErrGenerationSelectionMissing)) {
				return CapturedHistorySources{}, err
			}
			if err == nil {
				originalCatalogue = &catalogue
			}
		}
		var assembly *retainedHistoryAssembly
		if originalCatalogue == nil {
			if !assemble {
				return CapturedHistorySources{}, ErrGenerationSelectionMissing
			}
			document, assembly, err = d.assembleRetainedHistoryScope(ctx, spool, req, files, history)
			if err != nil {
				return CapturedHistorySources{}, err
			}
		}
		body, err := json.Marshal(document)
		if err != nil || len(body) > monitor.MonthlyPreparationInputLimit {
			return CapturedHistorySources{}, ErrGenerationCapacity
		}
		// Verify the other pinned source before allocating derived bytes. All
		// original financial/ref checks still happen in the common normalizer.
		if _, err := d.g.readPrivatePreparationFile(ctx, history, monitor.MonthlyPreparationInputLimit); err != nil {
			return CapturedHistorySources{}, err
		}
		if _, err := d.g.readPrivatePreparationFile(ctx, files.Material, 2<<20); err != nil {
			return CapturedHistorySources{}, err
		}
		ref, err := spool.retainHistoryScope(ctx, req, files, body)
		if err != nil {
			return CapturedHistorySources{}, err
		}
		return CapturedHistorySources{History: history, Scope: ref, catalogue: originalCatalogue, assembly: assembly}, nil
	})
}

func selectCapturedHistoryScope(raw []byte, identity string, owner monthlytasks.Owner, month string, ids []int64) (RetainedAutomaticScope, error) {
	var empty RetainedAutomaticScope
	if len(raw) > monitor.MonthlyPreparationInputLimit || !validSelectionScope(owner.GroupID, month) || len(ids) == 0 || len(ids) > 200 {
		return empty, ErrGenerationInput
	}
	var catalogue CapturedHistoryCatalogue
	if json.Unmarshal(raw, &catalogue) != nil {
		return empty, ErrGenerationInput
	}
	canonical, err := json.Marshal(catalogue)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || catalogue.Version != 1 || !generationHex(identity, 32) || catalogue.SourceIdentity != identity || len(catalogue.Scopes) == 0 || len(catalogue.Scopes) > 256 {
		return empty, ErrGenerationInput
	}
	seen := map[GenerationTarget]bool{}
	var selected *AutomaticScope
	for i := range catalogue.Scopes {
		scope := &catalogue.Scopes[i]
		if len(scope.Group.Months) != 1 {
			return empty, ErrGenerationInput
		}
		target := GenerationTarget{GroupID: scope.Group.GroupID, Month: scope.Group.Months[0]}
		if !validSelectionScope(target.GroupID, target.Month) || seen[target] {
			return empty, ErrGenerationInput
		}
		seen[target] = true
		if target.GroupID == owner.GroupID && target.Month == month {
			selected = scope
		}
	}
	if selected == nil {
		return empty, ErrGenerationSelectionMissing
	}
	members, accounts := make([]int64, len(selected.Group.Members)), make([]int64, len(selected.Accounts))
	for i, member := range selected.Group.Members {
		members[i] = member.UserID
	}
	for i, account := range selected.Accounts {
		accounts[i] = account.UserID
	}
	slices.Sort(members)
	slices.Sort(accounts)
	want := slices.Sorted(slices.Values(ids))
	if want[0] <= 0 || len(slices.Compact(slices.Clone(want))) != len(want) || !slices.Equal(members, want) || !slices.Equal(accounts, want) {
		return empty, ErrGenerationEvidence
	}
	return RetainedAutomaticScope{Version: 1, Owner: owner, Month: month, SourceIdentity: identity, Scope: *selected}, nil
}

const capturedHistoryScopeVersions = 8

func capturedHistoryScopeDigest(name string) string {
	digest := strings.TrimSuffix(strings.TrimPrefix(name, "history-scope-"), ".json")
	if name != "history-scope-"+digest+".json" || !generationHex(digest, 32) {
		return ""
	}
	return digest
}

// Only adds a content-addressed envelope beside an already completed seal;
// original marker, raw database and token archives are NEVER overwritten.
// Shares the spool lock, allocated-byte budget and bounded file inventory.
func (s *CapturedSourceSpool) retainHistoryScope(ctx context.Context, req SourcePreparationRequest, files CapturedSourceFiles, body []byte) (monthlysource.MonthlyObservationFile, error) {
	var empty monthlysource.MonthlyObservationFile
	if ctx == nil || s == nil || s.wallet == nil || s.identity == nil || s.limit <= 0 || s.limit > generationEvidenceLimit || files.Mode != capturedSourceFilesMode || len(body) == 0 || len(body) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationInput
	}
	root, lock, err := openCapturedOutput(s.directory)
	if err != nil {
		return empty, err
	}
	defer root.Close()
	defer lock.Close()
	info, err := root.Stat(".")
	if err != nil || !os.SameFile(s.identity, info) {
		return empty, ErrGenerationIntegrity
	}
	inventory, err := inspectCapturedSpoolPolicy(ctx, root, s.retainCompleted)
	if err != nil {
		return empty, err
	}
	job := s.jobName(req)
	jobRoot, err := root.OpenRoot(job)
	if err != nil {
		return empty, err
	}
	defer jobRoot.Close()
	_, complete, err := inspectCapturedJob(jobRoot)
	if err != nil || complete == "" {
		return empty, ErrGenerationIntegrity
	}
	directory := filepath.Join(s.directory, job, complete)
	if files.Material.Path != filepath.Join(directory, "materials.json") || files.Selection.Path != filepath.Join(directory, "selection.json") {
		return empty, ErrGenerationEvidence
	}
	seal, err := jobRoot.OpenRoot(complete)
	if err != nil {
		return empty, err
	}
	defer seal.Close()
	for name, ref := range map[string]monthlysource.MonthlyObservationFile{"materials.json": files.Material, "selection.json": files.Selection} {
		digest, _, err := capturedFileDigest(ctx, seal, name, 2<<20)
		if err != nil || digest != ref.SHA256 {
			return empty, ErrGenerationIntegrity
		}
	}
	name := "history-scope-" + generationHash(body) + ".json"
	ref := monthlysource.MonthlyObservationFile{Path: filepath.Join(directory, name), SHA256: generationHash(body)}
	if _, err := seal.Lstat(name); err == nil {
		raw, err := readArchiveFile(ctx, seal, name, monitor.MonthlyPreparationInputLimit)
		if err != nil || !bytes.Equal(raw, body) {
			return empty, ErrGenerationIntegrity
		}
	} else if !os.IsNotExist(err) {
		return empty, err
	} else {
		f, err := seal.Open(".")
		if err != nil {
			return empty, err
		}
		entries, readErr := f.ReadDir(205 + capturedHistoryScopeVersions)
		closeErr := f.Close()
		if readErr != nil {
			return empty, readErr
		}
		if closeErr != nil {
			return empty, closeErr
		}
		versions := 0
		for _, entry := range entries {
			if capturedHistoryScopeDigest(entry.Name()) != "" {
				versions++
			}
		}
		if versions >= capturedHistoryScopeVersions || int64(len(body))+(64<<10) > s.limit-inventory.bytes {
			return empty, ErrGenerationCapacity
		}
		if err := writeArchiveFile(ctx, seal, name, body); err != nil {
			return empty, err
		}
		if err := syncArchiveDirectory(seal); err != nil {
			return empty, err
		}
	}
	if err := s.current(root, jobRoot, job); err != nil {
		return empty, err
	}
	if err := automaticPreparationRootCurrent(seal, directory); err != nil {
		return empty, err
	}
	return ref, ctx.Err()
}
