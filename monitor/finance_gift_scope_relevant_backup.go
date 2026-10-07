//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"gorm.io/gorm"
)

// InspectFinanceGiftRelevantScopeBackups uses ONLY two caller-supplied closed
// local backups. It does not construct a running Monitor, migrate a table,
// establish a source connection, or write any database or plan file.
func InspectFinanceGiftRelevantScopeBackups(parent context.Context, mainPath, factsPath, epoch, startDate, throughDate string) (FinanceGiftRelevantScopeInspection, error) {
	var empty FinanceGiftRelevantScopeInspection
	seed, err := financeStartHour(startDate)
	if err != nil {
		return empty, err
	}
	to, err := financeStartHour(throughDate)
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	main, err := openGiftRelevantBackup(mainPath)
	if err != nil {
		return empty, err
	}
	defer main.close()
	facts, err := openGiftRelevantBackup(factsPath)
	if err != nil {
		return empty, err
	}
	defer facts.close()
	m := &Monitor{storeDB: main.db, usageFactsDB: facts.db}
	defer m.financeGiftEvidenceGuard.close()
	result, err := inspectFinanceGiftRelevantScope(ctx, m, epoch, seed, to)
	if err != nil {
		return empty, err
	}
	for _, backup := range []*giftRelevantBackup{main, facts} {
		if err := backup.unchanged(); err != nil {
			return empty, err
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}

type giftRelevantBackup struct {
	path  string
	info  os.FileInfo
	db    *gorm.DB
	close func()
}

func openGiftRelevantBackup(path string) (*giftRelevantBackup, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &giftRelevantBackup{path: path, info: info, db: db, close: func() { closeDB(); _ = f.Close() }}, nil
}

func (b *giftRelevantBackup) unchanged() error {
	after, err := os.Lstat(b.path)
	if err != nil || !os.SameFile(b.info, after) || b.info.Size() != after.Size() || !b.info.ModTime().Equal(after.ModTime()) {
		return errors.New("closed backup changed during relevant scope inspection; discard the plan")
	}
	return giftLocalClosedBackup(b.path)
}
