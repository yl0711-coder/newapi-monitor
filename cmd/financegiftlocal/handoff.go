//go:build unix

package main

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func runHandoffAction(ctx context.Context, action, job, confirmation, receiver, next string, maxHours int, used map[string]bool) (any, error) {
	if action == "handoff-check" {
		return runHandoffCheck(ctx, job, confirmation, receiver, used)
	}
	if action != "handoff-plan" && action != "handoff-run" {
		return nil, errors.New("unknown handoff action")
	}
	for name := range used {
		if name == "action" || name == "job-dir" || name == "confirm-plan" {
			continue
		}
		if action == "handoff-plan" && (name == "receiver-backup" || name == "next-job-dir") {
			continue
		}
		if action == "handoff-run" && name == "handoff-max-hours" {
			continue
		}
		return nil, errors.New("unrelated handoff flag")
	}
	if job == "" || confirmation == "" {
		return nil, errors.New("handoff requires job-dir and confirmation")
	}
	if action == "handoff-run" {
		if maxHours < 1 || maxHours > 10 {
			return nil, errors.New("handoff max-hours must be between 1 and 10")
		}
		if receiver != "" || next != "" {
			return nil, errors.New("handoff-run cannot select a receiver database")
		}
		return monitor.RunFinanceGiftLocalHandoffLimited(ctx, job, confirmation, maxHours)
	}
	if receiver == "" || next == "" {
		return nil, errors.New("handoff-plan requires receiver-backup and new next-job-dir")
	}
	bounded, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	receipt, digest, err := monitor.PrepareFinanceGiftLocalHandoff(bounded, job, confirmation, receiver, next)
	if err != nil {
		return nil, err
	}
	return map[string]any{"mode": "offline_handoff_prepared_no_repair", "receipt": receipt, "confirm_handoff_sha256": digest}, nil
}

func runHandoffCheck(parent context.Context, job, confirmation, receiver string, used map[string]bool) (any, error) {
	for name := range used {
		if name != "action" && name != "job-dir" && name != "confirm-plan" && name != "receiver-backup" {
			return nil, errors.New("unrelated handoff-check flag")
		}
	}
	if job == "" || confirmation == "" || receiver == "" {
		return nil, errors.New("handoff-check requires job-dir, confirm-plan and receiver-backup")
	}
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	report, err := monitor.PreviewFinanceGiftLocalHandoff(ctx, job, confirmation, receiver)
	if err != nil {
		return nil, err
	}
	if report.BlockedTargets > 0 {
		return report, errors.New("handoff preflight blocked; no data written")
	}
	return report, nil
}
