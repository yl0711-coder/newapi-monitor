package exportjobs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	sessionPrefix      = "bill-jobs-v1-"
	sessionMarker      = "session.v1"
	sessionLease       = ".lease"
	baseGuard          = ".bill-jobs.guard"
	maxRecoveryEntries = 128
)

type RecoveryReport struct {
	Removed int
	Active  int
	Skipped int
}

func privateBase(base string) error {
	info, err := os.Lstat(base)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return ErrInvalid
	}
	return nil
}

// All lock files are regular, private, no-follow files. In particular, recovery
// never creates a missing session lock: absence is not evidence of ownership.
func lockFile(path string, create bool) (*os.File, error) {
	flags := os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0) {
		err = ErrInvalid
	}
	if err == nil {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	}
	if err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return f, nil
}

func unlockFile(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

func validSessionName(name string) bool {
	if !strings.HasPrefix(name, sessionPrefix) {
		return false
	}
	suffix := strings.TrimPrefix(name, sessionPrefix)
	decoded, err := hex.DecodeString(suffix)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == suffix
}

func markerText(name string) string { return "nexusapi-usage-bill-ephemeral-v1\n" + name + "\n" }

func knownMarker(root, name string) bool {
	f, err := os.OpenFile(filepath.Join(root, sessionMarker), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	want := markerText(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() != int64(len(want)) {
		return false
	}
	content, err := io.ReadAll(io.LimitReader(f, int64(len(want))+1))
	return err == nil && string(content) == want
}

// Recover removes only recognized v1 sessions for which an exclusive lifetime
// lock can be obtained. No PID, age or filename alone is treated as proof that
// a process is gone. Unknown versions, partial initialization and symlinks stay.
// The caller must supply the dedicated server-owned private staging directory.
func Recover(base string) (RecoveryReport, error) {
	var err error
	base, err = filepath.Abs(base)
	if err != nil {
		return RecoveryReport{}, err
	}
	if err := privateBase(base); err != nil {
		return RecoveryReport{}, err
	}
	guard, err := lockFile(filepath.Join(base, baseGuard), true)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer unlockFile(guard)
	return recoverLocked(base)
}

func recoverLocked(base string) (RecoveryReport, error) {
	var report RecoveryReport
	dir, err := os.Open(base)
	if err != nil {
		return report, err
	}
	entries, readErr := dir.ReadDir(maxRecoveryEntries + 1)
	_ = dir.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return report, readErr
	}
	// Do not do a partially scanned sweep or let repeated crashes cause an
	// unlimited startup scan. An overfull/foreign directory needs inspection.
	if len(entries) > maxRecoveryEntries {
		return report, ErrBusy
	}
	for _, entry := range entries {
		if entry.Name() == baseGuard {
			continue
		}
		if !validSessionName(entry.Name()) {
			report.Skipped++
			continue
		}
		root := filepath.Join(base, entry.Name())
		info, err := os.Lstat(root)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return report, err
		}
		if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !knownMarker(root, entry.Name()) {
			report.Skipped++
			continue
		}
		lease, err := lockFile(filepath.Join(root, sessionLease), false)
		if errors.Is(err, ErrBusy) {
			report.Active++
			continue
		}
		if err != nil {
			report.Skipped++
			continue
		}
		// A closing owner may have removed the old path while we waited to open
		// it. Check directory + lock inode again under the acquired lock.
		current, statErr := os.Lstat(root)
		leaseInfo, leaseErr := lease.Stat()
		pathInfo, pathErr := os.Lstat(filepath.Join(root, sessionLease))
		if statErr != nil || leaseErr != nil || pathErr != nil || !os.SameFile(info, current) || !os.SameFile(leaseInfo, pathInfo) || !knownMarker(root, entry.Name()) {
			unlockFile(lease)
			report.Skipped++
			continue
		}
		removeErr := os.RemoveAll(root)
		unlockFile(lease)
		if removeErr != nil {
			return report, removeErr
		}
		report.Removed++
	}
	return report, nil
}

func newSession(base string) (string, *os.File, error) {
	var err error
	base, err = filepath.Abs(base)
	if err != nil {
		return "", nil, err
	}
	if err := privateBase(base); err != nil {
		return "", nil, err
	}
	guard, err := lockFile(filepath.Join(base, baseGuard), true)
	if err != nil {
		return "", nil, err
	}
	defer unlockFile(guard)
	if _, err := recoverLocked(base); err != nil {
		return "", nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", nil, err
	}
	name := sessionPrefix + hex.EncodeToString(id[:])
	root := filepath.Join(base, name)
	if err := os.Mkdir(root, 0700); err != nil {
		return "", nil, err
	}
	lease, err := lockFile(filepath.Join(root, sessionLease), true)
	if err == nil {
		err = os.WriteFile(filepath.Join(root, sessionMarker), []byte(markerText(name)), 0600)
	}
	if err != nil {
		_ = os.RemoveAll(root)
		unlockFile(lease)
		return "", nil, err
	}
	return root, lease, nil
}

// Called with the manager lock held, only after generation and all downloads
// have released ownership. The lifetime lock remains held until removal ends.
func (m *Manager) removeRootLocked() error {
	if m.sessionLock == nil {
		return nil
	}
	rootInfo, err := os.Lstat(m.root)
	if os.IsNotExist(err) {
		unlockFile(m.sessionLock)
		m.sessionLock = nil
		return nil
	}
	if err != nil {
		return err
	}
	if !rootInfo.IsDir() || !knownMarker(m.root, filepath.Base(m.root)) {
		return ErrInvalid
	}
	leaseInfo, err := m.sessionLock.Stat()
	if err != nil {
		return err
	}
	pathInfo, err := os.Lstat(filepath.Join(m.root, sessionLease))
	if err != nil {
		return err
	}
	if !os.SameFile(leaseInfo, pathInfo) {
		return ErrInvalid
	}
	if err := os.RemoveAll(m.root); err != nil {
		return err
	}
	unlockFile(m.sessionLock)
	m.sessionLock = nil
	return nil
}
