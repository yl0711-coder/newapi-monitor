package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const financeBackfillMaxHoursLimit = 168

func runFinanceFactBackfillCommand(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("finance-backfill", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	maxHours := flags.Int("max-hours", 1, "maximum chronological hours to publish")
	timeout := flags.Duration("timeout", 30*time.Minute, "overall maintenance timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("finance-backfill does not accept positional arguments")
	}
	if *maxHours < 1 || *maxHours > financeBackfillMaxHoursLimit {
		return fmt.Errorf("max-hours must be between 1 and %d", financeBackfillMaxHoursLimit)
	}
	if *timeout <= 0 || *timeout > 6*time.Hour {
		return fmt.Errorf("timeout must be greater than zero and at most 6h")
	}

	instance, err := monitor.NewFinanceFactBackfill(monitor.LoadSettings())
	if err != nil {
		return fmt.Errorf("initialize finance backfill: %w", err)
	}
	defer instance.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := instance.RunFinanceFactBackfill(ctx, *maxHours)
	if encodeErr := json.NewEncoder(output).Encode(result); encodeErr != nil && err == nil {
		err = encodeErr
	}
	return err
}
