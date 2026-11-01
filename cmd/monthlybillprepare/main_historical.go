package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func prepareHistoricalPreview(inputPath, outputPath string, request monitor.MonthlyHistoricalSettlementRequest, now time.Time) error {
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
	preview, err := monitor.PrepareMonthlyHistoricalSettlementPreview(raw, request, now)
	if err != nil {
		return err
	}
	preview.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	body, err := json.MarshalIndent(preview, "", "  ")
	if err != nil {
		return err
	}
	return writeBundleExclusive(outputPath, append(body, '\n'))
}
