//go:build unix

package main

import (
	"context"
	"errors"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func runSeriesAction(ctx context.Context, action, path, confirmation string, used map[string]bool) (any, error) {
	if action != "series-check" && action != "series-run" {
		return nil, errors.New("unknown series action")
	}
	for name := range used {
		if name != "action" && name != "series-plan" && !(action == "series-run" && name == "confirm-plan") {
			return nil, errors.New("unrelated series flag")
		}
	}
	if path == "" || (action == "series-run" && confirmation == "") || (action == "series-check" && confirmation != "") {
		return nil, errors.New("series requires manifest and an exact confirmation for run only")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if action == "series-check" {
		return monitor.InspectFinanceGiftLocalSeries(path)
	}
	return monitor.RunFinanceGiftLocalSeries(ctx, path, confirmation)
}
