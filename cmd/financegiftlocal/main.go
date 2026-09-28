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
	action := flag.String("action", "plan", "inspect-backup, plan, run, status, candidates, read-plan, advance; large-read-plan, large-plan, large-run, large-status; series-check, series-run; handoff-check, handoff-plan, handoff-run")
	receiver := flag.String("receiver-backup", "", "closed local receiver snapshot; read-only input for handoff-check or handoff-plan")
	handoffMaxHours := flag.Int("handoff-max-hours", 2, "maximum repaired user-hours per handoff-run invocation (1-10); paused_limit requires explicit resume")
	seriesPath := flag.String("series-plan", "", "private finite offline series manifest; series-check/run only")
	readPlan := flag.String("read-plan-file", "", "confirmed large-hour read plan JSON; large-plan only")
	outputPath := flag.String("output", "", "new private read plan file; large-read-plan only")
	userID := flag.Int64("user-id", 0, "explicit user; large-read-plan only")
	hourTs := flag.Int64("hour-ts", 0, "explicit closed hour Unix timestamp; large-read-plan only")
	backup := flag.String("backup", "", "closed local usage-facts.db backup; plan or inspect-backup only")
	epoch := flag.String("source-epoch", "", "explicit original source epoch; inspect-backup only")
	dir := flag.String("job-dir", "", "new private directory for plan; existing directory for all other actions")
	confirm := flag.String("confirm-plan", "", "exact existing job plan SHA256 required except for plan")
	exportDir := flag.String("export-dir", "", "completed private source export directory; advance only")
	nextDir := flag.String("next-job-dir", "", "new private offline job directory; advance only")
	confirmRead := flag.String("confirm-read-plan", "", "confirmed source read plan SHA256; advance or large-plan")
	var evidence paths
	flag.Var(&evidence, "evidence", "local filtered original evidence JSON; repeat at most 10 times; plan only")
	flag.Parse()
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "handoff-max-hours" && *action != "handoff-run" {
			fail(fmt.Errorf("-handoff-max-hours requires handoff-run"))
		}
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *action == "handoff-check" || *action == "handoff-plan" || *action == "handoff-run" {
		used := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { used[f.Name] = true })
		if flag.NArg() != 0 {
			fail(fmt.Errorf("positional arguments are not supported"))
		}
		output, err := runHandoffAction(ctx, *action, *dir, *confirm, *receiver, *nextDir, *handoffMaxHours, used)
		printResult(output, err)
		return
	}
	if *receiver != "" {
		fail(fmt.Errorf("-receiver-backup requires handoff-check or handoff-plan"))
	}
	if *action == "series-check" || *action == "series-run" {
		used := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { used[f.Name] = true })
		if flag.NArg() != 0 {
			fail(fmt.Errorf("positional arguments are not supported"))
		}
		output, err := runSeriesAction(ctx, *action, *seriesPath, *confirm, used)
		printResult(output, err)
		return
	}
	if *seriesPath != "" {
		fail(fmt.Errorf("-series-plan requires series-check or series-run"))
	}
	if isLargeAction(*action) {
		used := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { used[f.Name] = true })
		if flag.NArg() != 0 {
			fail(fmt.Errorf("positional arguments are not supported"))
		}
		output, err := runLargeAction(ctx, *action, largeOptions{backup: *backup, epoch: *epoch, dir: *dir, confirm: *confirm, confirmRead: *confirmRead, readPlan: *readPlan, output: *outputPath, user: *userID, hour: *hourTs, evidence: evidence}, used)
		printResult(output, err)
		return
	}
	if *readPlan != "" || *outputPath != "" || *userID != 0 || *hourTs != 0 {
		fail(fmt.Errorf("large-hour flags require a large-hour action"))
	}
	if (*dir == "" && *action != "inspect-backup") || flag.NArg() != 0 {
		fail(fmt.Errorf("-job-dir is required; positional arguments are not supported"))
	}
	var output any
	var err error
	if *epoch != "" && *action != "inspect-backup" {
		fail(fmt.Errorf("-source-epoch is accepted only by inspect-backup"))
	}
	switch *action {
	case "inspect-backup":
		if *backup == "" || *epoch == "" || *dir != "" || len(evidence) != 0 || *confirm != "" || *exportDir != "" || *nextDir != "" || *confirmRead != "" {
			fail(fmt.Errorf("inspect-backup requires only -backup and -source-epoch; no job or source connection"))
		}
		inspection, inspectErr := monitor.InspectFinanceGiftScopeBackup(ctx, *backup, *epoch)
		err = inspectErr
		if err == nil {
			output = inspection
		}
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
		fail(fmt.Errorf("unknown action; use inspect-backup, plan, run, status, candidates, read-plan or advance"))
	}
	printResult(output, err)
}

func printResult(output any, err error) {
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
