//go:build unix

package main

import (
	"context"
	"fmt"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type largeOptions struct {
	backup, epoch, dir, confirm, confirmRead, readPlan, output string
	user, hour                                                 int64
	evidence                                                   []string
}

func isLargeAction(action string) bool {
	return action == "large-read-plan" || action == "large-plan" || action == "large-run" || action == "large-status"
}

func runLargeAction(ctx context.Context, action string, o largeOptions, used map[string]bool) (any, error) {
	allowed := map[string]bool{"action": true}
	var flags []string
	switch action {
	case "large-read-plan":
		flags = []string{"backup", "source-epoch", "user-id", "hour-ts", "output"}
	case "large-plan":
		flags = []string{"backup", "read-plan-file", "confirm-read-plan", "evidence", "job-dir"}
	case "large-run", "large-status":
		flags = []string{"job-dir", "confirm-plan"}
	default:
		return nil, fmt.Errorf("unknown large-hour action")
	}
	for _, name := range flags {
		allowed[name] = true
	}
	for name := range used {
		if !allowed[name] {
			return nil, fmt.Errorf("-%s is not accepted by %s", name, action)
		}
	}
	switch action {
	case "large-read-plan":
		if o.backup == "" || o.epoch == "" || o.user <= 0 || o.hour <= 0 || o.output == "" {
			return nil, fmt.Errorf("large-read-plan requires -backup -source-epoch -user-id -hour-ts -output")
		}
		plan, _, err := monitor.PrepareFinanceGiftLargeHourPlan(ctx, o.backup, o.epoch, o.hour, o.user)
		if err != nil {
			return nil, err
		}
		digest, err := financegiftexport.WriteLargeHourPlan(o.output, plan)
		if err != nil {
			return nil, err
		}
		return map[string]any{"mode": "offline_read_plan_no_source_access", "file": o.output, "rows": len(plan.Rows), "confirm_read_plan_sha256": digest}, nil
	case "large-plan":
		if o.backup == "" || o.readPlan == "" || o.confirmRead == "" || len(o.evidence) != 1 || o.dir == "" {
			return nil, fmt.Errorf("large-plan requires backup, read-plan-file, confirm-read-plan, exactly one evidence file and job-dir")
		}
		_, digest, err := monitor.PrepareFinanceGiftLargeLocalJob(ctx, o.backup, o.readPlan, o.evidence[0], o.dir, o.confirmRead)
		if err != nil {
			return nil, err
		}
		return map[string]any{"mode": "offline_large_copy_no_repair", "job_dir": o.dir, "confirm_plan_sha256": digest}, nil
	default:
		if o.dir == "" || o.confirm == "" {
			return nil, fmt.Errorf("large-run/status requires -job-dir and -confirm-plan")
		}
		if action == "large-status" {
			return monitor.InspectFinanceGiftLargeLocalJob(ctx, o.dir, o.confirm)
		}
		return monitor.RunFinanceGiftLargeLocalJob(ctx, o.dir, o.confirm)
	}
}
