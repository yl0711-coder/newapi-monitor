package ecslogagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

const finalReadBudget = ecsarchive.FinalMaxBytes

// ConfigureProducerStopCheck must be called before RunChild. The CLI supplies
// the strict link-local metadata reader; isolated process tests inject a fake.
func (a *Agent) ConfigureProducerStopCheck(check func(context.Context) error) {
	a.producerStopped = check
}

func snapshotFinalFile(ctx context.Context, path string) (ecsarchive.FinalFile, error) {
	return snapshotFinalFileWithin(ctx, path, finalReadBudget)
}

func snapshotFinalFileWithin(ctx context.Context, path string, budget int64) (ecsarchive.FinalFile, error) {
	var result ecsarchive.FinalFile
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return result, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return result, err
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || !before.Mode().IsRegular() || before.Size() > budget {
		return result, errors.New("final source outside bounded regular-file contract")
	}
	h := sha256.New()
	buf := make([]byte, 64<<10)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		n, err := f.Read(buf)
		size += int64(n)
		if size > budget {
			return result, errors.New("final source exceeds hash budget")
		}
		_, _ = h.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, err
		}
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) || after.Size() != size || before.Size() != size || !before.ModTime().Equal(after.ModTime()) {
		return result, errors.New("source changed during final snapshot")
	}
	return ecsarchive.FinalFile{Name: filepath.Base(path), Device: uint64(stat.Dev), Inode: stat.Ino, Size: size, Offset: size, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func (a *Agent) finishBoundaries(ctx context.Context, before map[string]ecsarchive.FinalFile) (map[string]*ecsarchive.FinalBoundary, error) {
	after, err := a.finalInventory(ctx)
	if err != nil {
		return nil, err
	}
	if len(before) != len(after) {
		return nil, errors.New("final inventory changed")
	}
	for name, f := range after {
		if f != before[name] {
			return nil, errors.New("producer wrote after stop verification")
		}
		if a.cfg.Kind == "nginx" {
			if err := a.checkFinalCursor(f); err != nil {
				return nil, err
			}
		}
	}
	if a.cfg.Kind == "reject" {
		if err := a.checkFinalRejectCursors(after); err != nil {
			return nil, err
		}
	}
	result := map[string]*ecsarchive.FinalBoundary{}
	for _, lane := range a.cfg.lanes() {
		var files []ecsarchive.FinalFile
		for name, f := range after {
			if ecsarchive.FinalFileNameAllowed(a.cfg.FinalFileContract, lane, name) {
				files = append(files, f)
			}
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
		if err := ecsarchive.ValidateFinalFiles(a.cfg.FinalFileContract, lane, files); err != nil {
			return nil, err
		}
		result[lane] = &ecsarchive.FinalBoundary{ObservedAt: a.now().Unix(), Files: files, Contract: a.cfg.FinalFileContract}
	}
	return result, nil
}
