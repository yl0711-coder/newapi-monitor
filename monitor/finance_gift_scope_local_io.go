//go:build unix

package monitor

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const financeGiftLocalJSONLimit = 2 << 20

func giftLocalDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func giftLocalReadJSON(path string, value any) ([]byte, error) {
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, financeGiftLocalJSONLimit+1))
	if err != nil || len(data) > financeGiftLocalJSONLimit {
		return nil, errors.New("local JSON unreadable or exceeds size limit")
	}
	if err := giftLocalDecodeJSON(data, value); err != nil {
		return nil, err
	}
	return data, nil
}

func giftLocalDecodeJSON(data []byte, value any) error {
	// Strict decoding also rejects concatenated JSON documents.
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("unexpected trailing JSON content")
	}
	return nil
}

// Files in a job are private regular files, never links to a live database.
func giftLocalOpenRegular(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 {
		f.Close()
		return nil, errors.New("expected an unlinked-to-others regular local file")
	}
	return f, nil
}

func giftLocalWriteNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func giftLocalClosedBackup(path string) error {
	for _, suffix := range []string{"-wal", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if err == nil && info.Size() != 0 {
			return errors.New("use a closed SQLite backup without pending WAL/journal")
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func giftLocalCopyBackup(source, destination string) (string, error) {
	if err := giftLocalClosedBackup(source); err != nil {
		return "", err
	}
	in, err := giftLocalOpenRegular(source, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer in.Close()
	before, err := in.Stat()
	if err != nil {
		return "", err
	}
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	defer out.Close()
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(out, hash), in); err != nil {
		return "", err
	}
	after, err := in.Stat()
	if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return "", errors.New("backup changed during copy")
	}
	if err := giftLocalClosedBackup(source); err != nil {
		return "", err
	}
	if err := out.Sync(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func giftLocalLock(dir string) (*os.File, error) {
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("job directory must be a private regular directory (0700)")
	}
	f, err := giftLocalOpenRegular(filepath.Join(dir, "job.lock"), os.O_RDWR)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("local job is already in use")
	}
	// Closing this file releases the kernel lock, including after process death.
	return f, nil
}

func giftLocalValidateAudit(path string, plan FinanceGiftLocalPlan) error {
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() > 8<<20 {
		return errors.New("audit unreadable or exceeds offline job limit")
	}
	if info.Size() > 0 {
		var last [1]byte
		if _, err = f.ReadAt(last[:], info.Size()-1); err != nil || last[0] != '\n' {
			return errors.New("audit has a partial final entry; inspect before resuming")
		}
	}
	allowed := map[financeGiftScopeTarget]bool{}
	for _, target := range plan.Targets {
		allowed[target] = true
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var entry financeGiftScopeBatchEntry
		if err = giftLocalDecodeJSON(scanner.Bytes(), &entry); err != nil || !allowed[entry.financeGiftScopeTarget] ||
			(entry.Status != "repaired" && entry.Status != "unchanged" && entry.Status != "failed") || entry.RowsChecked < 0 || entry.RowsUpdated < 0 || entry.RowsUpdated > entry.RowsChecked {
			return errors.New("audit contains an invalid or unrelated entry")
		}
	}
	return scanner.Err()
}
