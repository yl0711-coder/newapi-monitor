package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func prepareResolutionPlan(inputPath, outputPath string, now time.Time) error {
	raw, err := readPreparationLocal(inputPath, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return err
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return err
	}
	for _, source := range input.Sources {
		if err := verifySource(source); err != nil {
			return fmt.Errorf("来源 %s: %w", source.ID, err)
		}
	}
	plan, err := monitor.PlanMonthlyResolution(raw, now)
	if err != nil {
		return err
	}
	plan.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	body, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return writeBundleExclusive(outputPath, append(body, '\n'))
}
