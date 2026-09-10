package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const maxConfigBytes = 64 << 10

func loadConfig(path string) (monitor.ECSAcceptanceConfig, string, error) {
	var c monitor.ECSAcceptanceConfig
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > maxConfigBytes {
		return c, "", errors.New("configuration must be a private regular file, at most 64 KiB")
	}
	f, err := os.Open(path)
	if err != nil {
		return c, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil || len(b) > maxConfigBytes {
		return c, "", errors.New("configuration read failed or exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var tail any
	if d.Decode(&c) != nil || !errors.Is(d.Decode(&tail), io.EOF) {
		return c, "", errors.New("invalid isolated configuration; unknown fields are forbidden")
	}
	canonical, err := json.Marshal(c)
	if err != nil {
		return c, "", err
	}
	sum := sha256.Sum256(canonical)
	return c, hex.EncodeToString(sum[:]), nil
}

// A resume may only reopen this tool's own store with the SAME identity/config.
// Never adopt an existing directory lacking the marker, or overwrite its data.
func prepareState(dir, digest string) (*os.File, error) {
	if !filepath.IsAbs(dir) || dir == string(filepath.Separator) || len(digest) != 64 {
		return nil, errors.New("dedicated absolute state directory and config fingerprint required")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return nil, errors.New("invalid config fingerprint")
	}
	err := os.Mkdir(dir, 0700)
	fresh := err == nil
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory must be private and not a symlink")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.Name() == "backups" && e.IsDir() {
			if err := validateBackupTree(filepath.Join(dir, e.Name())); err != nil {
				return nil, err
			}
			continue
		}
		if !e.Type().IsRegular() {
			return nil, errors.New("state directory must contain only regular files")
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if err := validateIndependentStateFile(info); err != nil {
			return nil, err
		}
	}
	marker := filepath.Join(dir, ".ecs-acceptance-v1")
	if fresh {
		if err := os.WriteFile(marker, []byte(digest), 0600); err != nil {
			return nil, err
		}
	} else {
		b, err := os.ReadFile(marker)
		if err != nil || string(b) != digest {
			return nil, errors.New("refusing to adopt an unrelated store or changed acceptance identity")
		}
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".receiver.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("another receiver already owns this acceptance store")
	}
	return lock, nil
}

// Different paths are not sufficient isolation: a hard-linked SQLite, WAL,
// marker or lock still refers to another receiver's physical file.
func validateIndependentStateFile(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		return errors.New("acceptance state files must not be hard-linked to another store")
	}
	return nil
}
