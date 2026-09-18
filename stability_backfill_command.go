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

const stabilityBackfillMaxHoursLimit = 72

func runStabilityFactBackfillCommand(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("stability-backfill", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	from := flags.Int64("from", 0, "inclusive aligned UTC hour")
	to := flags.Int64("to", 0, "exclusive aligned UTC hour")
	timeout := flags.Duration("timeout", 30*time.Minute, "overall maintenance timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("stability-backfill does not accept positional arguments")
	}
	if *from < 0 || *to <= *from || *from%3600 != 0 || *to%3600 != 0 || (*to-*from)/3600 > stabilityBackfillMaxHoursLimit {
		return fmt.Errorf("from/to must be aligned UTC hours with a range of 1 to %d hours", stabilityBackfillMaxHoursLimit)
	}
	if *timeout <= 0 || *timeout > 2*time.Hour {
		return fmt.Errorf("timeout must be greater than zero and at most 2h")
	}

	instance, err := monitor.NewStabilityFactBackfill(monitor.LoadSettings())
	if err != nil {
		return fmt.Errorf("initialize stability backfill: %w", err)
	}
	defer instance.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := instance.RunBoundedStabilityBackfill(ctx, *from, *to)
	if encodeErr := json.NewEncoder(output).Encode(result); encodeErr != nil && err == nil {
		err = encodeErr
	}
	return err
}
