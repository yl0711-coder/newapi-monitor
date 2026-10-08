//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// FinanceGiftBackupInspection measures raw scope gaps, NOT business-filtered
// report coverage. It never connects to a source or modifies the supplied backup.
type FinanceGiftBackupInspection struct {
	Mode          string                       `json:"mode"`
	SourceEpoch   string                       `json:"source_epoch"`
	UnknownRows   int64                        `json:"unknown_rows"`
	MonetaryRows  int64                        `json:"unknown_nonzero_quota_rows"`
	UserHours     int64                        `json:"unknown_user_hours"`
	OversizeHours int64                        `json:"user_hours_over_row_limit"`
	MissingStates int64                        `json:"user_hours_missing_state"`
	Months        []FinanceGiftScopeMonthGap   `json:"months"`
	Candidates    FinanceGiftLocalCandidates   `json:"candidates" gorm:"-"`
	ReadPlan      *financegiftexport.BatchPlan `json:"read_plan,omitempty" gorm:"-"`
	Confirmation  string                       `json:"confirm_read_plan_sha256,omitempty"`
}

// InspectFinanceGiftScopeBackup bootstraps the first read plan without requiring
// a prior repair job or its temporary files. Supply a CLOSED offline backup and
// an explicit epoch; the caller must separately authorize any source export.
func InspectFinanceGiftScopeBackup(parent context.Context, path, epoch string) (FinanceGiftBackupInspection, error) {
	var empty FinanceGiftBackupInspection
	if epoch == "" || strings.TrimSpace(epoch) != epoch || len(epoch) > 64 {
		return empty, errors.New("explicit source epoch required")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return empty, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return empty, err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(path)
	if err != nil {
		return empty, err
	}
	defer closeDB()
	var result FinanceGiftBackupInspection
	var epochs int64
	if err := db.WithContext(ctx).Model(&FinanceGiftBoundaryState{}).Where("source_epoch=?", epoch).Count(&epochs).Error; err != nil {
		return empty, err
	}
	if epochs == 0 {
		return empty, errors.New("source epoch not found in backup; do not assume no gaps")
	}
	result.Months, err = inspectFinanceGiftScopeMonths(ctx, db, epoch)
	if err != nil {
		return empty, err
	}
	for _, month := range result.Months {
		result.UnknownRows += month.UnknownRows
		result.MonetaryRows += month.MonetaryRows
		result.UserHours += month.UserHours
		result.OversizeHours += month.OversizeHours
		result.MissingStates += month.MissingStates
	}
	result.Mode, result.SourceEpoch = "offline_backup_inspection_no_source_access", epoch
	if result.MissingStates > 0 {
		result.Candidates = FinanceGiftLocalCandidates{Mode: "offline_suggestion_only", Status: "blocked", StopReason: "missing_boundary_state", SourceEpoch: epoch}
	} else {
		result.Candidates, err = giftLocalSelectCandidates(ctx, db, epoch, time.Now().Unix())
		if err != nil {
			return empty, err
		}
		if result.Candidates.Status == "ready" {
			plan, digest, err := giftScopeReadPlanFromCandidates(result.Candidates)
			if err != nil {
				return empty, err
			}
			result.ReadPlan, result.Confirmation = &plan, digest
		}
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return empty, errors.New("backup changed during inspection; discard the plan")
	}
	if err := giftLocalClosedBackup(path); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}
