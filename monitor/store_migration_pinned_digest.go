package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// The manifest and restore copy cover the standalone database only. A WAL or
// rollback journal with bytes cannot participate in the verified restore view.
// Harmless sidecars still disable the digest-only shortcut. Do not use this
// guard for live databases, where committed WAL frames must remain visible.
func checkSealedSnapshotSidecars(path string) (bool, error) {
	found := false
	for _, suffix := range [...]string{"-wal", "-shm", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if err == nil {
			if !info.Mode().IsRegular() || (suffix != "-shm" && info.Size() != 0) {
				return false, fmt.Errorf("封存快照包含异常 SQLite 边文件 %s；不能核验单文件恢复", suffix)
			}
			found = true
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return found, nil
}

type snapshotDigestReader struct {
	ctx context.Context
	io.Reader
}

func (r snapshotDigestReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func snapshotFileUnchanged(before, after os.FileInfo) bool {
	return after.Mode().IsRegular() && os.SameFile(before, after) &&
		before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}

// verifyPinnedSQLiteDigest reads the entire pinned file once. Only previously
// verified bytes may be reused; a missing, renamed, truncated or changed file
// must fail closed. False/nil means the caller must perform full SQLite checks.
func verifyPinnedSQLiteDigest(ctx context.Context, path string, store preMigrationSnapshotStore) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	initial, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	if !initial.Mode().IsRegular() || initial.Size() != store.SizeBytes {
		return false, errors.New("固定快照不是预期大小的普通文件")
	}
	if sidecars, err := checkSealedSnapshotSidecars(path); sidecars || err != nil {
		return false, err
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return false, err
	}
	if !snapshotFileUnchanged(initial, opened) {
		return false, errors.New("固定快照文件在打开时已变化")
	}
	digest := sha256.New()
	n, err := io.Copy(digest, snapshotDigestReader{ctx: ctx, Reader: io.LimitReader(f, store.SizeBytes)})
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if n != store.SizeBytes || hex.EncodeToString(digest.Sum(nil)) != store.FileSHA256 {
		return false, errors.New("固定快照文件哈希/大小与 manifest 不一致")
	}
	final, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	if !snapshotFileUnchanged(initial, final) {
		return false, errors.New("固定快照文件在校验期间已变化")
	}
	sidecars, err := checkSealedSnapshotSidecars(path)
	return !sidecars && err == nil, err
}
