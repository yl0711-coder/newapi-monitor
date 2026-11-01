package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// This data-only handoff is NOT an observationAssemblyPlan or a GenerationPlan:
// funding captures and independent historical balance/scope remain required.
type observationMonthSelection struct {
	Version    int                               `json:"version"`
	PlanSHA256 string                            `json:"observation_plan_sha256"`
	GroupID    int64                             `json:"group_id"`
	Funding    monthlysource.FundingMonthRequest `json:"funding"`
	TokenUsage []assemblyUsagePair               `json:"token_usage"`
	Wallet     []assemblyCapture                 `json:"wallet"`
}

func observationMonthIndex(state observationMonthState) ([]byte, error) {
	if state.Next != nil || len(state.Jobs) == 0 || len(state.Refs) != len(state.Jobs) || state.Progress.Completed != len(state.Jobs) {
		return nil, errInput
	}
	selection := observationMonthSelection{Version: 1, PlanSHA256: state.Progress.PlanSHA256, GroupID: state.Plan.GroupID, Funding: state.Plan.Funding}
	for _, job := range state.Jobs {
		if job.Newer {
			continue
		}
		older, a := state.Refs[job.Name]
		newer, b := state.Refs[job.Name[:len(job.Name)-len("older")]+"newer"]
		if !a || !b || !validSHA(older.ManifestSHA256) || !validSHA(newer.ManifestSHA256) {
			return nil, errInput
		}
		if job.Request.Kind == "observe-wallet" {
			selection.Wallet = []assemblyCapture{older, newer}
		} else {
			selection.TokenUsage = append(selection.TokenUsage, assemblyUsagePair{Older: older, Newer: newer})
		}
	}
	raw, err := json.Marshal(selection)
	if err != nil || len(raw) > 2<<20 || len(selection.Wallet) != 2 || len(selection.TokenUsage)*2+2 != len(state.Jobs) {
		return nil, errInput
	}
	return raw, nil
}

func sealObservationMonthIndex(parent context.Context, workspace, name, expected string, create bool) (observationMonthProgress, error) {
	var empty observationMonthProgress
	if parent == nil || !validSHA(expected) {
		return empty, errInput
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	workspacePath, rootPath, err := jobPath(workspace, name)
	if err != nil || workspacePath != workspace {
		return empty, errInput
	}
	lock, err := workspaceLock(workspace, create, false)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	state, err := inspectObservationMonth(ctx, rootPath, expected, time.Now())
	if err != nil {
		return empty, err
	}
	raw, err := observationMonthIndex(state)
	if err != nil {
		return empty, err
	}
	result := state.Progress
	result.Mode, result.IndexSHA256 = "checked_observation_index_no_files_written", hash(raw)
	if !create {
		return result, nil
	}
	if state.Progress.IndexSHA256 != "" {
		result.Mode = "existing_private_observation_index_rechecked"
		return result, nil
	}
	inner, err := workspaceLock(rootPath, false, false)
	if err != nil {
		return empty, err
	}
	defer inner.Close()
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return empty, err
	}
	defer root.Close()
	if err := observationRootCurrent(root, rootPath); err != nil {
		return empty, err
	}
	pending := "observation-index.pending.json"
	if err := writeObservationFile(root, pending, raw); err != nil {
		return empty, err
	}
	if err := verifyObservationFile(root, pending, raw); err != nil {
		return empty, err
	}
	// Recheck the exact source manifests already selected above, not fresh
	// digests from mutable paths. Failed output retains a pending marker.
	var total int
	for _, job := range state.Jobs {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		body, err := readAssemblyCapture(state.Refs[job.Name], job.Request.Kind)
		if err != nil {
			return empty, err
		}
		total += len(body)
		if total > 32<<20 {
			return empty, errInput
		}
	}
	plan, err := privateRead(filepath.Join(rootPath, "observation-month.json"), 65536)
	canonical, marshalErr := json.Marshal(state.Plan)
	if err != nil || marshalErr != nil || !bytes.Equal(plan, canonical) || hash(plan) != expected {
		return empty, errInput
	}
	if err := observationRootCurrent(root, rootPath); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := root.Link(pending, "observation-index.json"); err != nil {
		return empty, err
	}
	if err := root.Remove(pending); err != nil {
		return empty, err
	}
	result.Mode = "created_private_observation_index_not_bill_approval"
	return result, errors.Join(syncObservationRoot(root), observationRootCurrent(root, rootPath), ctx.Err())
}
