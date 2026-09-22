//go:build unix

package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Offline acceptance budget only. It does not change the source export limit
// or enable production reads. Bound the entire plan, not just each input file.
const financeGiftLocalMaxRows = 3_000

// FinanceGiftLocalPlan describes ONLY an offline acceptance copy. No source
// DSN, credential, production endpoint or automatic scheduling is supported.
type FinanceGiftLocalPlan struct {
	Version        int                      `json:"version"`
	BackupSHA256   string                   `json:"backup_sha256"`
	Targets        []financeGiftScopeTarget `json:"targets"`
	EvidenceSHA256 []string                 `json:"evidence_sha256"`
	Rows           []int                    `json:"rows"`
}

type financeGiftLocalEvidence struct {
	UserID int64 `json:"user_id"`
	Hour   int64 `json:"hour_ts"`
	Rows   []struct {
		ID        int64 `json:"id"`
		UserID    int64 `json:"user_id"`
		CreatedAt int64 `json:"created_at"`
		Type      int   `json:"type"`
		// Null/omitted is not the same evidence as explicit zero/empty.
		Quota *int64  `json:"quota"`
		Group *string `json:"group"`
	} `json:"rows"`
}

func (e financeGiftLocalEvidence) validate() error {
	if e.UserID <= 0 || e.Hour <= 0 || e.Hour%3600 != 0 || len(e.Rows) == 0 || len(e.Rows) > financeGiftLocalMaxRows {
		return fmt.Errorf("offline evidence requires 1 to %d original records in one explicit user-hour", financeGiftLocalMaxRows)
	}
	ids := map[int64]bool{}
	for _, r := range e.Rows {
		if r.ID <= 0 || ids[r.ID] || r.UserID != e.UserID || r.CreatedAt < e.Hour || r.CreatedAt >= e.Hour+3600 ||
			(r.Type != 2 && r.Type != 6) || r.Quota == nil || *r.Quota < 0 || r.Group == nil {
			return errors.New("offline evidence identity or monetary fields invalid")
		}
		ids[r.ID] = true
	}
	return nil
}

func validateFinanceGiftLocalRowBudget(rows []int) error {
	total := 0
	for _, count := range rows {
		if count <= 0 || count > financeGiftLocalMaxRows-total {
			return fmt.Errorf("offline plan must contain at most %d total original records", financeGiftLocalMaxRows)
		}
		total += count
	}
	if total == 0 {
		return errors.New("offline plan requires original records")
	}
	return nil
}

func giftLocalDatabase(path string) (*gorm.DB, func(), error) {
	f, err := giftLocalOpenRegular(path, os.O_RDWR)
	if err != nil {
		return nil, nil, err
	}
	f.Close()
	uri := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}).String()
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

// giftLocalSource reuses the collector's validation on an in-memory projection.
// Input must be the already traffic-filtered readonly-inspect export. Empty
// classifier columns are scaffolding, NOT reconstructed user request metadata.
func giftLocalSource(ctx context.Context, evidence []financeGiftLocalEvidence) (*sql.DB, error) {
	for _, e := range evidence {
		if err := e.validate(); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, "CREATE TABLE logs(id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,quota INTEGER,`group` TEXT,token_id INTEGER,token_name TEXT,request_id TEXT,content TEXT,other TEXT)"); err == nil {
		for _, e := range evidence {
			for _, r := range e.Rows {
				_, err = db.ExecContext(ctx, "INSERT INTO logs VALUES(?,?,?,?,?,?,1,'','','','')", r.ID, r.UserID, r.CreatedAt, r.Type, *r.Quota, *r.Group)
				if err != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func giftLocalEvidenceName(index int) string { return fmt.Sprintf("evidence-%02d.json", index) }

// PrepareFinanceGiftLocalJob creates a NEW directory. It never opens the
// original backup for writing. A failed preparation is kept for inspection;
// without the final plan.json it cannot be executed.
func PrepareFinanceGiftLocalJob(ctx context.Context, backup, dir string, evidencePaths []string) (FinanceGiftLocalPlan, string, error) {
	plan := FinanceGiftLocalPlan{Version: 1}
	if len(evidencePaths) == 0 || len(evidencePaths) > financeGiftScopeBatchLimit {
		return plan, "", errors.New("supply 1 to 10 offline evidence files")
	}
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return plan, "", err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return plan, "", err
	}
	if err = giftLocalWriteNew(filepath.Join(dir, "job.lock"), nil); err != nil {
		return plan, "", err
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		return plan, "", err
	}
	defer lock.Close()
	plan.BackupSHA256, err = giftLocalCopyBackup(backup, filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return plan, "", err
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return plan, "", err
	}
	defer closeDB()
	// Only the private copy receives the existing scope-column migration.
	if err = db.WithContext(ctx).AutoMigrate(&FinanceGiftBoundaryEvent{}); err != nil {
		return plan, "", err
	}
	var evidence []financeGiftLocalEvidence
	for i, path := range evidencePaths {
		var e financeGiftLocalEvidence
		data, err := giftLocalReadJSON(path, &e)
		if err != nil {
			return plan, "", err
		}
		if err = e.validate(); err != nil {
			return plan, "", err
		}
		var states []FinanceGiftBoundaryState
		if err = db.WithContext(ctx).Where("hour_ts=? AND user_id=?", e.Hour, e.UserID).Limit(2).Find(&states).Error; err != nil || len(states) != 1 {
			return plan, "", errors.New("offline target must have exactly one source epoch")
		}
		plan.Targets = append(plan.Targets, financeGiftScopeTarget{states[0].SourceEpoch, e.Hour, e.UserID})
		plan.EvidenceSHA256 = append(plan.EvidenceSHA256, giftLocalDigest(data))
		plan.Rows = append(plan.Rows, len(e.Rows))
		if err = validateFinanceGiftLocalRowBudget(plan.Rows); err != nil {
			return plan, "", err
		}
		evidence = append(evidence, e)
		if err = giftLocalWriteNew(filepath.Join(dir, giftLocalEvidenceName(i)), data); err != nil {
			return plan, "", err
		}
	}
	if err = validateFinanceGiftScopeBatch(plan.Targets, time.Now().Unix()); err != nil {
		return plan, "", err
	}
	if err = giftLocalCheckEvidence(ctx, db, plan, evidence); err != nil {
		return plan, "", err
	}
	if err = giftLocalWriteNew(filepath.Join(dir, "audit.jsonl"), nil); err != nil {
		return plan, "", err
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return plan, "", err
	}
	if err = giftLocalWriteNew(filepath.Join(dir, "plan.json"), data); err != nil {
		return plan, "", err
	}
	return plan, giftLocalDigest(data), nil
}

func giftLocalCheckEvidence(ctx context.Context, db *gorm.DB, plan FinanceGiftLocalPlan, evidence []financeGiftLocalEvidence) error {
	source, err := giftLocalSource(ctx, evidence)
	if err != nil {
		return err
	}
	defer source.Close()
	for i, target := range plan.Targets {
		e := evidence[i]
		if e.UserID != target.UserID || e.Hour != target.HourTs || len(e.Rows) != plan.Rows[i] {
			return errors.New("offline evidence does not match fixed plan")
		}
		prior, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil {
			return err
		}
		fetched, err := fetchFinanceGiftBoundaryEvents(ctx, source, target.HourTs, []int64{target.UserID})
		if err != nil {
			return err
		}
		if _, _, err = mergeFinanceGiftScopeEvidence(prior.Events, fetched); err != nil {
			return err
		}
	}
	return nil
}

// RunFinanceGiftLocalJob requires the SHA256 printed by prepare. Each process
// waits a full interval after acquiring the OS lock: even a crash after commit
// cannot bypass the restart cooldown. Only the private job DB can be changed.
func RunFinanceGiftLocalJob(ctx context.Context, dir, confirmation string) (financeGiftScopeBatchResult, error) {
	return runFinanceGiftLocalJob(ctx, dir, confirmation, waitFinanceGiftScopeBatch)
}

func runFinanceGiftLocalJob(ctx context.Context, dir, confirmation string, wait func(context.Context, time.Duration) error) (financeGiftScopeBatchResult, error) {
	rejected := financeGiftScopeBatchResult{Status: "rejected"}
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
	journal, err := giftLocalOpenRegular(filepath.Join(dir, "audit.jsonl"), os.O_WRONLY|os.O_APPEND)
	if err != nil {
		return rejected, err
	}
	defer journal.Close()
	// Ensure no previous partial write will be hidden by a newly appended entry.
	if err = giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), plan); err != nil {
		return rejected, err
	}
	if err = wait(ctx, financeGiftScopeBatchInterval); err != nil {
		return financeGiftScopeBatchResult{Status: "paused", Remaining: len(plan.Targets)}, err
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return rejected, err
	}
	defer closeDB()
	if err = giftLocalCheckEvidence(ctx, db, plan, evidence); err != nil {
		return rejected, err
	}
	source, err := giftLocalSource(ctx, evidence)
	if err != nil {
		return rejected, err
	}
	defer source.Close()
	encoder := json.NewEncoder(journal)
	runner := newFinanceGiftScopeBatchRunner(db, source)
	// Share the cancellable wait with startup. The public entry always uses
	// the real timer; offline tests can exercise inter-target interruption
	// deterministically without adding a user-facing rate-limit bypass.
	runner.wait = func(ctx context.Context, delay time.Duration) error {
		if delay <= 0 {
			return ctx.Err()
		}
		return wait(ctx, delay)
	}
	return runner.run(ctx, plan.Targets, func(entry financeGiftScopeBatchEntry) error {
		if err := encoder.Encode(entry); err != nil {
			return err
		}
		return journal.Sync()
	})
}

// Caller holds the job lock. Both run and status must accept exactly the
// same fixed, bounded inputs; status cannot silently trust a changed plan.
func giftLocalLoadConfirmedInputs(dir, confirmation string) (FinanceGiftLocalPlan, []financeGiftLocalEvidence, error) {
	var plan FinanceGiftLocalPlan
	data, err := giftLocalReadJSON(filepath.Join(dir, "plan.json"), &plan)
	if err != nil {
		return plan, nil, err
	}
	if confirmation != giftLocalDigest(data) || plan.Version != 1 || len(plan.Targets) != len(plan.EvidenceSHA256) || len(plan.Targets) != len(plan.Rows) {
		return plan, nil, errors.New("plan confirmation or shape mismatch; prepare and review a new job")
	}
	if err = validateFinanceGiftLocalRowBudget(plan.Rows); err != nil {
		return plan, nil, err
	}
	if err = validateFinanceGiftScopeBatch(plan.Targets, time.Now().Unix()); err != nil {
		return plan, nil, err
	}
	var evidence []financeGiftLocalEvidence
	for i, hash := range plan.EvidenceSHA256 {
		var e financeGiftLocalEvidence
		data, err := giftLocalReadJSON(filepath.Join(dir, giftLocalEvidenceName(i)), &e)
		if err != nil || giftLocalDigest(data) != hash {
			return plan, nil, errors.New("offline evidence changed or unreadable")
		}
		if err = e.validate(); err != nil {
			return plan, nil, err
		}
		if len(e.Rows) != plan.Rows[i] {
			return plan, nil, errors.New("offline evidence row count differs from fixed plan")
		}
		evidence = append(evidence, e)
	}
	return plan, evidence, nil
}
