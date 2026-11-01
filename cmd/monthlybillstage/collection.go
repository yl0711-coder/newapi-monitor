package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
)

func runCollectionPlan(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("monthlybillstage plan-collection", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var request monthlydraft.OrganizationCollectionRequest
	fs.StringVar(&request.AuthorityPath, "authority", "", "existing local Usage authority, read-only")
	fs.Int64Var(&request.GroupID, "group-id", 0, "exact current organization; full members read from authority")
	fs.StringVar(&request.Month, "month", "", "one closed UTC+8 month")
	fs.StringVar(&request.SourceIdentity, "source-identity", "", "independently pinned MySQL source digest")
	fs.StringVar(&request.OutputRoot, "output-root", "", "existing empty private directory")
	fs.BoolVar(&request.Write, "write", false, "write new private current roster only; no source capture or bill")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return monthlydraft.ErrGenerationInput
	}
	receipt, err := monthlydraft.PrepareOrganizationCollection(ctx, request)
	if err != nil && receipt.Directory == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(receipt))
}
