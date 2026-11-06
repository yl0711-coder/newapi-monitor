package main

import (
	"encoding/json"
	"flag"
	"io"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func runUsageSplit(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("split-usage-window", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	source := fs.String("source-identity", "", "exact source digest")
	user := fs.Int64("user-id", 0, "one exact account")
	from := fs.Int64("from", 0, "inclusive source second")
	to := fs.Int64("to", 0, "exclusive source second")
	if fs.Parse(args) != nil || fs.NArg() != 0 || *to > time.Now().Unix()-60 {
		return errInput
	}
	r := monthlysource.UsageWindowRequest{SourceIdentity: *source, UserID: *user, From: *from, To: *to,
		Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}}
	children, err := monthlysource.SplitUsageWindow(r)
	if err != nil {
		return errInput
	}
	return json.NewEncoder(out).Encode(struct {
		Version  int                                 `json:"version"`
		Mode     string                              `json:"mode"`
		Children [2]monthlysource.UsageWindowRequest `json:"children"`
	}{1, "query_budget_child_plan_not_source_data", children})
}
