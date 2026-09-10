package ecslogagent

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func (c Config) finalSourcePatterns() (access, reject string) {
	if c.FinalFileContract == ecsarchive.NewAPIFileContract {
		return "nexusapi_access.jsonl", "oneapi-*.log"
	}
	return "access.jsonl", "new-api.log"
}

// Only retained files can be proved. Unknown rotations, missing historical
// cursors, symlinks and over-budget inventories never become green coverage.
func (a *Agent) finalInventory(ctx context.Context) (map[string]ecsarchive.FinalFile, error) {
	dir, err := os.OpenFile(a.cfg.FinalLogRoot, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !info.IsDir() {
		return nil, errors.New("final log root unavailable or symlink")
	}
	entries, err := dir.ReadDir(ecsarchive.FinalMaxFiles + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	names, err := a.cfg.finalInventoryNames(entries)
	if err != nil {
		return nil, err
	}
	files := map[string]ecsarchive.FinalFile{}
	seen := map[[2]uint64]bool{}
	budget := int64(finalReadBudget)
	for _, name := range names {
		f, err := snapshotFinalFileWithin(ctx, filepath.Join(a.cfg.FinalLogRoot, name), budget)
		if err != nil {
			return nil, err
		}
		id := [2]uint64{f.Device, f.Inode}
		if seen[id] {
			return nil, errors.New("duplicate source file identity")
		}
		seen[id] = true
		budget -= f.Size
		files[name] = f
	}
	after, err := os.Lstat(a.cfg.FinalLogRoot)
	if err != nil || !os.SameFile(info, after) || !info.ModTime().Equal(after.ModTime()) {
		return nil, errors.New("source directory changed during final inventory")
	}
	return files, nil
}

func (c Config) finalInventoryNames(entries []os.DirEntry) ([]string, error) {
	if len(entries) == 0 || len(entries) > ecsarchive.FinalMaxFiles {
		return nil, errors.New("final inventory missing or exceeds file budget")
	}
	var names []string
	for _, e := range entries {
		allowed, required := false, false
		for _, lane := range c.lanes() {
			if ecsarchive.FinalFileNameAllowed(c.FinalFileContract, lane, e.Name()) {
				allowed, required = true, true
			}
		}
		// Legacy fixtures permitted both producers to share one directory.
		if c.FinalFileContract == "" {
			allowed = e.Name() == "access.jsonl" || e.Name() == "error.log" || e.Name() == "new-api.log"
		}
		if !allowed || !e.Type().IsRegular() {
			return nil, errors.New("unexpected final source file")
		}
		if required {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 || (c.Kind == "nginx" && len(names) != 2) {
		return nil, errors.New("required final source file missing")
	}
	sort.Strings(names)
	return names, nil
}
