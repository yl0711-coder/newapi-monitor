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

func runHandoff(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("monthlybillstage handoff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg monthlydraft.DeploymentConfig
	var request monthlydraft.HandoffExportRequest
	fs.StringVar(&cfg.ReceiverPath, "receiver", "", "existing task receiver, read-only")
	fs.StringVar(&cfg.FundingPath, "funding", "", "retained funding database, read-only")
	fs.StringVar(&cfg.AuthorityPath, "authority", "", "current Usage permission database, read-only")
	fs.StringVar(&cfg.EvidenceRoot, "evidence-root", "", "private evidence directory")
	group := fs.Int64("group-id", 0, "one exact Monitor organization ID")
	month := fs.String("month", "", "one closed Beijing calendar month")
	fs.StringVar(&request.TaskID, "task-id", "", "exact completed task ID")
	fs.StringVar(&request.ExpectedHead, "expected-head", "", "exact inspected current selection head")
	fs.StringVar(&request.ExpectedResult, "expected-result", "", "exact inspected completed draft hash")
	fs.StringVar(&request.OutputRoot, "output-root", "", "existing private directory; exports use fresh subdirectories")
	contextPath := fs.String("context", "", "explicit scope/event metadata; no approval inferred")
	fs.BoolVar(&request.Export, "export", false, "write private review materials only; default checks without writing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *group <= 0 || *month == "" {
		return monthlydraft.ErrGenerationInput
	}
	raw, err := readInput(*contextPath, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return err
	}
	request.Context = raw
	cfg.Rollout = monthlydraft.GenerationRollout{Enabled: true, Targets: []monthlydraft.GenerationTarget{{GroupID: *group, Month: *month}}}
	receipt, err := monthlydraft.ExportReviewHandoff(ctx, cfg, request)
	if err != nil && receipt.Directory == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(receipt))
}
