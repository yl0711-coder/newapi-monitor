package main

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
)

// Monitor creates mandatory migration snapshots under its own backups folder.
// Permit that bounded private tree on resume, not arbitrary directories or
// symlinks. WalkDir does not follow links; every entry is explicitly checked.
func validateBackupTree(root string) error {
	entries := 0
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		entries++
		rel, err := filepath.Rel(root, path)
		if err != nil || entries > 4096 || strings.Count(rel, string(filepath.Separator)) > 1 {
			return errors.New("acceptance backup tree exceeds bounds")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Mode().Perm()&0077 != 0 {
				return errors.New("acceptance backup directories must be private")
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return errors.New("acceptance backups must contain only regular files")
		}
		return validateIndependentStateFile(info)
	})
}
