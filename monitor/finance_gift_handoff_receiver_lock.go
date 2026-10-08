//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
	"gorm.io/gorm"
)

// A separate advisory lock serializes authorized local runs across Monitor
// objects/processes without holding a SQLite transaction during cooldown.
// Never unlink this file: unlinking a held lock could admit a second inode.
// Normal SQLite writers do not use this lock; atomic revalidation still applies.
func lockFinanceGiftLocalReceiver(ctx context.Context, db *gorm.DB, configuredPath string) (*os.File, error) {
	canonical, err := filepath.EvalSymlinks(configuredPath)
	if err != nil {
		return nil, err
	}
	var databases []struct{ Name, File string }
	if err := db.WithContext(ctx).Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		return nil, err
	}
	actual := ""
	for _, database := range databases {
		if database.Name == "main" {
			actual = database.File
		}
	}
	actualCanonical, err := filepath.EvalSymlinks(actual)
	if err != nil || canonical != actualCanonical {
		return nil, errors.New("configured receiver differs from open database")
	}
	// Reject multiply-linked receiver files, which could acquire different locks.
	receiver, err := giftLocalOpenRegular(canonical, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	receiver.Close()
	path := canonical + ".gift-handoff.lock"
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Mode&0077 != 0 {
		f.Close()
		return nil, errors.New("receiver lock must be a private regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("authorized receiver already in use")
	}
	// Close (including process exit) releases ownership; no timestamp lease or
	// stale-file deletion is needed after a crash. The empty file may remain.
	return f, nil
}
