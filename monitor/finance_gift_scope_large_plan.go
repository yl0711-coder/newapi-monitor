//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// PrepareFinanceGiftLargeHourPlan only reads a CLOSED offline backup and one
// explicit target. It neither connects to a source nor authorizes a repair.
// Normal candidates, batch budgets and production workers remain unchanged.
func PrepareFinanceGiftLargeHourPlan(parent context.Context, path, epoch string, hour, user int64) (financegiftexport.LargeHourPlan, string, error) {
	var empty financegiftexport.LargeHourPlan
	if epoch == "" || strings.TrimSpace(epoch) != epoch || len(epoch) > 64 || hour <= 0 || hour%3600 != 0 || hour >= time.Now().Unix()-3600 || user <= 0 {
		return empty, "", errors.New("explicit epoch, user and closed hour required")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return empty, "", err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return empty, "", err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return empty, "", err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(path)
	if err != nil {
		return empty, "", err
	}
	defer closeDB()
	// Check size before loading details; never promote the 50000-row internal
	// collector limit into an operator-authorized source-read budget.
	var state FinanceGiftBoundaryState
	if err := db.WithContext(ctx).First(&state, "source_epoch=? AND hour_ts=? AND user_id=?", epoch, hour, user).Error; err != nil {
		return empty, "", err
	}
	if state.Rows <= financeGiftLocalMaxRows || state.Rows > financegiftexport.LargeHourMaxRows {
		return empty, "", errors.New("large-hour plan requires 3001 to 5000 existing records")
	}
	snapshot, err := loadFinanceGiftScopeSnapshot(ctx, db, epoch, hour, user)
	if err != nil {
		return empty, "", err
	}
	plan := financegiftexport.LargeHourPlan{Version: 1, SourceEpoch: epoch, UserID: user, HourTs: hour, LocalContentHash: snapshot.State.ContentHash}
	missing := false
	for _, event := range snapshot.Events {
		row := financegiftexport.LargeHourRecord{ID: event.SourceLogID, CreatedAt: event.EventAt, Quota: event.Quota}
		switch event.Kind {
		case "usage":
			row.Type = 2
		case "refund":
			row.Type = 6
		default:
			return empty, "", errors.New("unknown ledger event kind")
		}
		if event.GroupKnown {
			group := event.Group
			row.Group = &group
		} else {
			missing = true
		}
		plan.Rows = append(plan.Rows, row)
	}
	if !missing {
		return empty, "", errors.New("large hour already has verified groups; no source read required")
	}
	sort.Slice(plan.Rows, func(i, j int) bool { return plan.Rows[i].ID < plan.Rows[j].ID })
	digest, err := financegiftexport.LargeHourConfirmation(plan)
	if err != nil {
		return empty, "", err
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return empty, "", errors.New("backup changed during large-hour planning")
	}
	if err := giftLocalClosedBackup(path); err != nil {
		return empty, "", err
	}
	if err := ctx.Err(); err != nil {
		return empty, "", err
	}
	return plan, digest, nil
}
