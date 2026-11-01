package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const observationMaterialSourceID = "observed-material"
const observationMaterialDatabaseID = "observed-database"

// ObservationMaterialPreparationRequest selects an immutable PRIVATE material
// handoff. Template and Scope must contain independent historical evidence;
// current wallet observations cannot manufacture month boundaries or approval.
// Like PrepareAutomaticGenerationFiles, this never selects or publishes a bill.
type ObservationMaterialPreparationRequest struct {
	Material        monthlysource.MonthlyObservationFile
	Template, Scope []byte
	// Optional pinned history catalogue and CURRENT roster instead of Template.
	// Neither file establishes historical membership or new balance evidence.
	History, Organization *monthlysource.MonthlyObservationFile
	ScopeSourceID         string
	OutputRoot            string
	Write                 bool
	// Private on-demand adapter only: Organization then names the original
	// captured-selection file, whose CURRENT scope is projected, not invented.
	capturedRosterKey string
	// Original independently established scope metadata, not a customer field.
	scopeCatalogue *monthlysource.MonthlyObservationFile
	// Only the registered background producer may select a new month from
	// existing original history; this cannot be decoded from an HTTP/CLI file.
	historyAssembly *retainedHistoryAssembly
}

var observationMaterialPreparationSlots = make(chan struct{}, 1)

// PrepareObservationMaterialFiles adapts a pinned materials.json to the SAME
// existing normalizers, current authorization checks and last-marker writer.
// Default inspection writes nothing. Explicit output is a new prepared bundle,
// not a task, input selection or customer bill. No source network is accessed.
func PrepareObservationMaterialFiles(parent context.Context, cfg DeploymentConfig, request ObservationMaterialPreparationRequest) (AutomaticPreparationReceipt, error) {
	return prepareObservationMaterialFiles(parent, cfg, request, writeArchiveFile)
}

func prepareObservationMaterialFiles(parent context.Context, cfg DeploymentConfig, request ObservationMaterialPreparationRequest,
	writeFile func(context.Context, *os.Root, string, []byte) error) (AutomaticPreparationReceipt, error) {
	var empty AutomaticPreparationReceipt
	if parent == nil || !cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 1 ||
		!validOrganizationHistoryForm(request) || len(request.Template) > monitor.MonthlyPreparationInputLimit ||
		len(request.Scope) == 0 || len(request.Scope) > monitor.MonthlyPreparationInputLimit ||
		!generationHex(request.Material.SHA256, 32) || !validScopeSourceID(request.ScopeSourceID) {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	select {
	case observationMaterialPreparationSlots <- struct{}{}:
		defer func() { <-observationMaterialPreparationSlots }()
	default:
		return empty, ErrGenerationBusy
	}
	request.Template, request.Scope = bytes.Clone(request.Template), bytes.Clone(request.Scope)
	if request.History != nil {
		history, organization := *request.History, *request.Organization
		request.History, request.Organization = &history, &organization
	}
	d, err := openDeployment(ctx, cfg, true)
	if err != nil {
		return empty, err
	}
	prepared, composeErr := d.g.observationMaterialInput(ctx, request)
	err = errors.Join(composeErr, d.closeResources())
	if err != nil {
		return empty, err
	}
	// Do not keep temporary DB/root handles alive across another deployment.
	// Existing preparation rechecks CURRENT permission and all pinned sources
	// before writing and again before its completion marker becomes visible.
	return prepareAutomaticGenerationFiles(ctx, cfg, prepared, writeFile)
}

func (g *Generation) observationMaterialInput(ctx context.Context, request ObservationMaterialPreparationRequest) (AutomaticPreparationRequest, error) {
	var empty AutomaticPreparationRequest
	rel, err := g.relativeSource(request.Material.Path)
	if err != nil {
		return empty, err
	}
	before, err := g.root.Lstat(rel)
	if err != nil {
		return empty, err
	}
	raw, err := g.readPrivatePreparationFile(ctx, request.Material, generationPlanLimit)
	if err != nil {
		return empty, err
	}
	var material monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(raw, &material) != nil {
		return empty, ErrGenerationInput
	}
	canonical, err := json.Marshal(material)
	legacy := material.Version == 1 && material.Mode == monthlysource.MonthlyObservationMaterialMode && material.UsageSelection == nil && len(material.UsageWindows) > 0 && len(material.UsageWindows) <= 4096
	partitioned := material.Version == 2 && material.Mode == monthlysource.PartitionedObservationMaterialMode && material.UsageSelection != nil && len(material.UsageWindows) == 0 && material.WalletReuse == nil
	if err != nil || !bytes.Equal(raw, canonical) || (!legacy && !partitioned) ||
		len(material.Funding.UserIDs) == 0 || len(material.Funding.UserIDs) > 200 || len(material.Tokens) != len(material.Funding.UserIDs) ||
		(material.WalletReuse == nil && len(material.WalletIDs) != 2) || (material.WalletReuse != nil && len(material.WalletIDs) != 0) {
		return empty, ErrGenerationInput
	}
	var scope RetainedAutomaticScope
	if json.Unmarshal(request.Scope, &scope) != nil {
		return empty, ErrGenerationInput
	}
	canonical, err = json.Marshal(scope)
	if err != nil || !bytes.Equal(bytes.TrimSpace(request.Scope), canonical) || scope.Version != 1 || scope.Owner.GroupID != material.GroupID ||
		scope.Month != material.Funding.Month || scope.SourceIdentity != material.Funding.SourceIdentity {
		return empty, ErrGenerationEvidence
	}
	if request.History != nil {
		request.Template, err = g.organizationHistoryTemplate(ctx, request, material, scope)
		if err != nil {
			return empty, err
		}
	}
	input, err := monitor.DecodeMonthlyPreparationInput(request.Template)
	if err != nil {
		return empty, err
	}
	// Stable generated IDs remove all manual per-window/per-token editing.
	// Never replace a template source, even when its path or digest is equal.
	newSources := []monitor.MonthlyPreparationSource{
		{ID: observationMaterialSourceID, Path: request.Material.Path, SHA256: request.Material.SHA256},
		{ID: observationMaterialDatabaseID, Path: material.Database.Path, SHA256: material.Database.SHA256},
	}
	if material.WalletReuse != nil {
		reuseSources, err := g.walletReuseMaterialSources(ctx, material)
		if err != nil {
			return empty, err
		}
		newSources = append(newSources, reuseSources...)
	}
	p := GenerationPlan{Version: material.FundingVersion, Owner: scope.Owner, Funding: material.Funding,
		Windows: material.FundingWindows, UsageWindows: material.UsageWindows, ObservationStoreSourceID: observationMaterialDatabaseID,
		WalletObservations: &WalletObservationSelection{SourceID: observationMaterialDatabaseID, IDs: material.WalletIDs}}
	if partitioned {
		p.CapturedUsageSourceID = "observed-captured-selection"
		if request.History != nil {
			if request.Organization == nil || *request.Organization != *material.UsageSelection {
				return empty, ErrGenerationEvidence
			}
			// The original roster reference already pins this exact file; do not
			// add a second path alias or copy the same evidence into a new file.
			p.CapturedUsageSourceID = organizationRosterSourceID
		} else {
			newSources = append(newSources, monitor.MonthlyPreparationSource{ID: p.CapturedUsageSourceID, Path: material.UsageSelection.Path, SHA256: material.UsageSelection.SHA256})
		}
	}
	if material.WalletReuse != nil {
		p.WalletObservations.ReuseSourceID = walletReuseSourceID
	}
	for _, token := range material.Tokens {
		id := "observed-token-" + strconv.FormatInt(token.UserID, 10)
		newSources = append(newSources, monitor.MonthlyPreparationSource{ID: id, Path: token.File.Path, SHA256: token.File.SHA256})
		p.TokenUsageSources = append(p.TokenUsageSources, TokenUsageSourceSelection{UserID: token.UserID, SourceID: id})
	}
	ids, paths := map[string]bool{request.ScopeSourceID: true}, map[string]bool{}
	for _, source := range append(newSources, input.Sources...) {
		if ids[source.ID] || paths[source.Path] {
			return empty, ErrGenerationInput
		}
		ids[source.ID], paths[source.Path] = true, true
	}
	input.Sources = append(input.Sources, newSources...)
	template, err := json.Marshal(input)
	if err != nil || len(template) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.PreparationSHA256 = generationHash(template)
	_, plan, _, err := canonicalGeneration(p, template, time.Now())
	if err != nil {
		return empty, err
	}
	// This is not a financial approval. The existing normalization pipeline must
	// audit the actual database/token archives and independent history next.
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	after, err := g.root.Lstat(rel)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return empty, ErrGenerationIntegrity
	}
	return AutomaticPreparationRequest{Plan: plan, Template: template, Scope: request.Scope, ObservationSourceID: observationMaterialDatabaseID,
		ScopeSourceID: request.ScopeSourceID, OutputRoot: request.OutputRoot, Write: request.Write, compareRetainedMonth: request.History != nil}, ctx.Err()
}
