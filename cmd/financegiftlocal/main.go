//go:build unix

// financegiftlocal is an offline acceptance tool, not a production repair job.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type paths []string

func (p *paths) String() string     { return fmt.Sprint([]string(*p)) }
func (p *paths) Set(s string) error { *p = append(*p, s); return nil }

func main() {
	action := flag.String("action", "plan", "plan (isolated copy), run (also resume), status, candidates, read-plan, or advance (verified local handoff)")
	backup := flag.String("backup", "", "closed local usage-facts.db backup; plan only")
	dir := flag.String("job-dir", "", "new private directory for plan; existing directory for all other actions")
	confirm := flag.String("confirm-plan", "", "exact existing job plan SHA256 required except for plan")
	exportDir := flag.String("export-dir", "", "completed private source export directory; advance only")
	nextDir := flag.String("next-job-dir", "", "new private offline job directory; advance only")
	confirmRead := flag.String("confirm-read-plan", "", "confirmed source read plan SHA256; advance only")
	var evidence paths
	flag.Var(&evidence, "evidence", "local filtered original evidence JSON; repeat at most 10 times; plan only")
	flag.Parse()
	if *dir == "" || flag.NArg() != 0 {
		fail(fmt.Errorf("-job-dir is required; positional arguments are not supported"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var output any
	var err error
	switch *action {
	case "plan":
		if *backup == "" || len(evidence) == 0 || *confirm != "" || *exportDir != "" || *nextDir != "" || *confirmRead != "" {
			fail(fmt.Errorf("plan requires -backup and -evidence; do not pass -confirm-plan"))
		}
		plan, digest, planErr := monitor.PrepareFinanceGiftLocalJob(ctx, *backup, *dir, evidence)
		err = planErr
		output = struct {
			Mode   string                       `json:"mode"`
			Plan   monitor.FinanceGiftLocalPlan `json:"plan"`
			SHA256 string                       `json:"confirm_plan_sha256"`
		}{"offline_preview_no_repair", plan, digest}
	case "run", "status", "candidates", "read-plan", "advance":
		if *backup != "" || len(evidence) != 0 || *confirm == "" {
			fail(fmt.Errorf("this action requires -job-dir and -confirm-plan; original backup/evidence inputs cannot be replaced"))
		}
		if *action == "advance" {
			if *exportDir == "" || *nextDir == "" || *confirmRead == "" {
				fail(fmt.Errorf("advance requires -export-dir, -next-job-dir and -confirm-read-plan"))
			}
			plan, digest, advanceErr := monitor.PrepareFinanceGiftLocalContinuation(ctx, *dir, *confirm, *exportDir, *confirmRead, *nextDir)
			err = advanceErr
			if err == nil {
				output = map[string]any{"mode": "offline_next_plan_no_repair", "plan": plan, "confirm_plan_sha256": digest}
			}
		} else if *exportDir != "" || *nextDir != "" || *confirmRead != "" {
			fail(fmt.Errorf("export and continuation flags are accepted only by advance"))
		} else if *action == "read-plan" {
			plan, digest, planErr := monitor.PrepareFinanceGiftReadPlan(ctx, *dir, *confirm)
			err = planErr
			if err == nil {
				output = map[string]any{"mode": "offline_read_plan_no_source_access", "plan": plan, "confirm_read_plan_sha256": digest}
			}
		} else if *action == "candidates" {
			output, err = monitor.SuggestFinanceGiftLocalCandidates(ctx, *dir, *confirm)
		} else if *action == "status" {
			output, err = monitor.InspectFinanceGiftLocalJob(ctx, *dir, *confirm)
		} else {
			output, err = monitor.RunFinanceGiftLocalJob(ctx, *dir, *confirm)
		}
	default:
		fail(fmt.Errorf("unknown action; use plan, run, status, candidates or read-plan"))
	}
	if output != nil {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if encodeErr := encoder.Encode(output); encodeErr != nil {
			fail(encodeErr)
		}
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "offline gift-scope acceptance:", err)
	os.Exit(1)
}
