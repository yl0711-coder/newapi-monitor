//go:build unix

package main

import (
	"context"
	"errors"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func runRelevantInspection(ctx context.Context, main, facts, epoch, from, to string, used map[string]bool) (any, error) {
	for name := range used {
		switch name {
		case "action", "main-backup", "backup", "source-epoch", "from", "to":
		default:
			return nil, errors.New("inspect-relevant cannot accept repair, source connection or unrelated flags")
		}
	}
	if main == "" || facts == "" || epoch == "" || from == "" || to == "" {
		return nil, errors.New("inspect-relevant requires closed main-backup, backup, source-epoch, from and exclusive to dates")
	}
	result, err := monitor.InspectFinanceGiftRelevantScopeBackups(ctx, main, facts, epoch, from, to)
	if err != nil {
		return nil, err
	}
	return result, nil
}
