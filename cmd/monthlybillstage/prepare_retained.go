package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Explicit compatibility command, never an automatic fallback from malformed
// raw material. It only writes a new private package, not a customer statement.
func runPrepareRetained(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("monthlybillstage prepare-retained", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg monthlydraft.DeploymentConfig
	var r monthlydraft.RetainedPreparationRequest
	fs.StringVar(&cfg.ReceiverPath, "receiver", "", "existing task receiver, read-only")
	fs.StringVar(&cfg.FundingPath, "funding", "", "existing sealed funding source, read-only")
	fs.StringVar(&cfg.AuthorityPath, "authority", "", "current Usage permission database, read-only")
	fs.StringVar(&cfg.EvidenceRoot, "evidence-root", "", "existing private evidence directory")
	group := fs.Int64("group-id", 0, "one exact Monitor organization ID")
	month := fs.String("month", "", "one closed Beijing calendar month")
	plan := fs.String("plan", "", "canonical retained plan with an independently pinned automatic scope")
	input := fs.String("preparation", "", "existing normalized input; not a rendered bill")
	fs.StringVar(&r.ExpectedHead, "expected-head", "", "explicit current selection head, empty only for first version")
	fs.StringVar(&r.OutputRoot, "output-root", "", "existing EMPTY private directory inside evidence-root")
	fs.BoolVar(&r.Write, "write", false, "write a new private package only; default read-only inspection")
	if err := fs.Parse(args); err != nil {
		return err
	}
	headSet := false
	fs.Visit(func(f *flag.Flag) { headSet = headSet || f.Name == "expected-head" })
	if fs.NArg() != 0 || !headSet || *group <= 0 || *month == "" || r.OutputRoot == "" {
		return monthlydraft.ErrGenerationInput
	}
	var err error
	if r.Plan, err = readInput(*plan, 2<<20); err != nil {
		return err
	}
	if r.Preparation, err = readInput(*input, monitor.MonthlyPreparationInputLimit); err != nil {
		return err
	}
	cfg.Rollout = monthlydraft.GenerationRollout{Enabled: true, Targets: []monthlydraft.GenerationTarget{{GroupID: *group, Month: *month}}}
	receipt, err := monthlydraft.PrepareRetainedGenerationFiles(ctx, cfg, r)
	if err != nil && receipt.Directory == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(receipt))
}
