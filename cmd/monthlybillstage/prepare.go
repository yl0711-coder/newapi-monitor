package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func runPrepare(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("monthlybillstage prepare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg monthlydraft.DeploymentConfig
	var request monthlydraft.AutomaticPreparationRequest
	fs.StringVar(&cfg.ReceiverPath, "receiver", "", "existing task receiver, read-only")
	fs.StringVar(&cfg.FundingPath, "funding", "", "sealed observation database, read-only")
	fs.StringVar(&cfg.AuthorityPath, "authority", "", "current Usage permission database, read-only")
	fs.StringVar(&cfg.EvidenceRoot, "evidence-root", "", "existing private evidence directory")
	group := fs.Int64("group-id", 0, "one exact Monitor organization ID")
	month := fs.String("month", "", "one closed Beijing calendar month")
	planPath := fs.String("plan", "", "canonical source plan JSON")
	materialPath := fs.String("material", "", "optional sealed materials.json instead of a manual plan; requires -material-sha256")
	materialSHA := fs.String("material-sha256", "", "exact pinned digest of materials.json; never recomputed automatically")
	templatePath := fs.String("template", "", "independent preparation template; not previously normalized")
	historyPath := fs.String("history", "", "optional pinned independent history catalogue instead of -template; material mode only")
	historySHA := fs.String("history-sha256", "", "exact digest of independent history catalogue")
	organizationPath := fs.String("organization-scope", "", "pinned complete current organization roster; not history evidence")
	organizationSHA := fs.String("expected-organization", "", "exact digest of current roster")
	scopePath := fs.String("scope", "", "canonical independently established RetainedAutomaticScope JSON")
	fs.StringVar(&request.ObservationSourceID, "observation-source", "", "template source ID of the sealed observation database")
	fs.StringVar(&request.ScopeSourceID, "scope-source", "", "NEW source ID for the fixed scope")
	fs.StringVar(&request.OutputRoot, "output-root", "", "existing EMPTY private directory inside evidence-root")
	fs.BoolVar(&request.Write, "write", false, "write new private inputs only; default is read-only inspection")
	if err := fs.Parse(args); err != nil {
		return err
	}
	materialMode := *materialPath != "" || *materialSHA != ""
	historyMode := *historyPath != "" || *historySHA != "" || *organizationPath != "" || *organizationSHA != ""
	if fs.NArg() != 0 || *group <= 0 || *month == "" || request.ScopeSourceID == "" || request.OutputRoot == "" ||
		(historyMode && (!materialMode || *historyPath == "" || *historySHA == "" || *organizationPath == "" || *organizationSHA == "" || *templatePath != "")) ||
		(materialMode && (*materialPath == "" || *materialSHA == "" || *planPath != "" || request.ObservationSourceID != "")) ||
		(!materialMode && (*planPath == "" || request.ObservationSourceID == "")) {
		return monthlydraft.ErrGenerationInput
	}
	var err error
	if !historyMode {
		if request.Template, err = readInput(*templatePath, monitor.MonthlyPreparationInputLimit); err != nil {
			return err
		}
	}
	if request.Scope, err = readInput(*scopePath, monitor.MonthlyPreparationInputLimit); err != nil {
		return err
	}
	cfg.Rollout = monthlydraft.GenerationRollout{Enabled: true, Targets: []monthlydraft.GenerationTarget{{GroupID: *group, Month: *month}}}
	var receipt monthlydraft.AutomaticPreparationReceipt
	if materialMode {
		materialRequest := monthlydraft.ObservationMaterialPreparationRequest{
			Material: monthlysource.MonthlyObservationFile{Path: *materialPath, SHA256: *materialSHA}, Template: request.Template, Scope: request.Scope,
			ScopeSourceID: request.ScopeSourceID, OutputRoot: request.OutputRoot, Write: request.Write}
		if historyMode {
			materialRequest.History = &monthlysource.MonthlyObservationFile{Path: *historyPath, SHA256: *historySHA}
			materialRequest.Organization = &monthlysource.MonthlyObservationFile{Path: *organizationPath, SHA256: *organizationSHA}
		}
		receipt, err = monthlydraft.PrepareObservationMaterialFiles(ctx, cfg, materialRequest)
	} else {
		if request.Plan, err = readInput(*planPath, 2<<20); err != nil {
			return err
		}
		receipt, err = monthlydraft.PrepareAutomaticGenerationFiles(ctx, cfg, request)
	}
	if err != nil && receipt.Directory == "" {
		return err
	}
	// A partial or completed local copy must still be reported on write/stdout
	// errors. The caller must inspect it, never retry by overwriting that path.
	return errors.Join(err, json.NewEncoder(out).Encode(receipt))
}
