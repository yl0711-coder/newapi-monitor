//go:build unix

package monitor

import (
	"context"
	"net/url"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// FinanceGiftLocalProgress describes only the confirmed offline plan, never
// overall financial coverage, profit readiness, or production job progress.
type FinanceGiftLocalProgress struct {
	Mode             string                          `json:"mode"`
	Status           string                          `json:"status"`
	Targets          int                             `json:"targets"`
	CompletedTargets int                             `json:"completed_targets"`
	RemainingTargets int                             `json:"remaining_targets"`
	Rows             int                             `json:"rows"`
	VerifiedRows     int                             `json:"verified_rows"`
	RemainingRows    int                             `json:"remaining_rows"`
	Entries          []financeGiftLocalProgressEntry `json:"entries,omitempty"`
}

type financeGiftLocalProgressEntry struct {
	financeGiftScopeTarget
	Status        string `json:"status"`
	Rows          int    `json:"rows"`
	VerifiedRows  int    `json:"verified_rows"`
	RemainingRows int    `json:"remaining_rows"`
}

// InspectFinanceGiftLocalJob never repairs facts or appends audit. It takes
// the same nonblocking lock as run so an active writer cannot produce a mixed
// snapshot. A crash with pending WAL/journal requires recovery via run first;
// status must not silently ignore or recover a journal while claiming readonly.
func InspectFinanceGiftLocalJob(ctx context.Context, dir, confirmation string) (FinanceGiftLocalProgress, error) {
	rejected := FinanceGiftLocalProgress{Mode: "offline_plan_only", Status: "unverified"}
	if err := ctx.Err(); err != nil {
		return rejected, err
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return rejected, err
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		return rejected, err
	}
	defer lock.Close()
	plan, evidence, err := giftLocalLoadConfirmedInputs(dir, confirmation)
	if err != nil {
		return rejected, err
	}
	if err = giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), plan); err != nil {
		return rejected, err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return rejected, err
	}
	defer closeDB()
	if err = giftLocalCheckEvidence(ctx, db, plan, evidence); err != nil {
		return rejected, err
	}
	result := FinanceGiftLocalProgress{Mode: "offline_plan_only", Status: "pending", Targets: len(plan.Targets)}
	for _, target := range plan.Targets {
		prior, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil {
			return rejected, err // Never publish partly checked progress.
		}
		entry := financeGiftLocalProgressEntry{financeGiftScopeTarget: target, Status: "pending", Rows: len(prior.Events)}
		for _, event := range prior.Events {
			if event.GroupKnown {
				entry.VerifiedRows++
			}
		}
		entry.RemainingRows = entry.Rows - entry.VerifiedRows
		if entry.RemainingRows == 0 {
			entry.Status = "complete"
			result.CompletedTargets++
		} else if entry.VerifiedRows > 0 {
			entry.Status = "partial"
		}
		result.Rows += entry.Rows
		result.VerifiedRows += entry.VerifiedRows
		result.Entries = append(result.Entries, entry)
	}
	result.RemainingRows = result.Rows - result.VerifiedRows
	result.RemainingTargets = result.Targets - result.CompletedTargets
	if result.RemainingTargets == 0 {
		result.Status = "complete"
	} else if result.VerifiedRows > 0 {
		result.Status = "partial"
	}
	return result, nil
}

// Immutable is safe only for a closed, exclusively locked offline job with
// no pending journal. It avoids creating WAL/SHM files during inspection.
func giftLocalReadonlyDatabase(path string) (*gorm.DB, func(), error) {
	if err := giftLocalClosedBackup(path); err != nil {
		return nil, nil, err
	}
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return nil, nil, err
	}
	f.Close()
	uri := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&immutable=1"}).String()
	db, err := gorm.Open(sqlite.Open(uri), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	conn.SetMaxOpenConns(1)
	return db, func() { _ = conn.Close() }, nil
}
