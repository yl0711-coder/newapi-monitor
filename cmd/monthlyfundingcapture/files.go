package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func privateInfo(path string, directory bool) (os.FileInfo, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode().Perm()&0077 != 0 || (directory && !st.IsDir()) || (!directory && !st.Mode().IsRegular()) {
		return nil, errInput
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || sys.Uid != uint32(os.Geteuid()) || (!directory && sys.Nlink != 1) {
		return nil, errInput
	}
	return st, nil
}

func privateRead(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errInput
	}
	before, err := privateInfo(path, false)
	if err != nil || before.Size() <= 0 || before.Size() > limit {
		return nil, errInput
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errInput
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	after, err := privateInfo(path, false)
	if err != nil || !os.SameFile(opened, after) || len(raw) == 0 || int64(len(raw)) > limit || after.Size() != int64(len(raw)) || !after.ModTime().Equal(opened.ModTime()) {
		return nil, errInput
	}
	return raw, nil
}

func workspaceLock(root string, exclusive, create bool) (*os.File, error) {
	path := filepath.Join(root, ".funding-capture.lock")
	flags, kind := syscall.O_RDONLY, syscall.LOCK_SH
	if exclusive {
		flags, kind = syscall.O_RDWR, syscall.LOCK_EX
	}
	if create {
		flags |= syscall.O_CREAT
	}
	fd, err := syscall.Open(path, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := privateInfo(path, false)
	opened, statErr := f.Stat()
	if err != nil || statErr != nil || !os.SameFile(st, opened) {
		_ = f.Close()
		return nil, errInput
	}
	if err := syscall.Flock(fd, kind|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errBusy
		}
		return nil, err
	}
	return f, nil // Close releases the lock; never unlink its inode.
}

func openReceiver(ctx context.Context, path string, write bool) (*sql.DB, error) {
	before, err := privateInfo(path, false)
	if err != nil {
		return nil, err
	}
	for _, suffix := range []string{"-journal", "-wal", "-shm"} {
		if _, err := privateInfo(path+suffix, false); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	mode := "ro"
	pragmas := []string{"busy_timeout(500)", "query_only(1)"}
	if write {
		mode, pragmas = "rw", []string{"busy_timeout(500)", "journal_mode(DELETE)", "synchronous(FULL)"}
	}
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"mode": {mode}, "_pragma": pragmas}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	after, err := privateInfo(path, false)
	if err != nil || !os.SameFile(before, after) {
		_ = db.Close()
		return nil, errInput
	}
	return db, nil
}

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func exclusiveFile(path string, raw []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	return errors.Join(err, f.Sync(), f.Close())
}

// Empty schema bytes are retained only during one new batch initialization.
// They come from a freshly created, closed DB before its manifest is published,
// never from a pre-existing job, captured observations or a caller-supplied file.
type emptyFundingSchema struct{ raw []byte }

func initialize(ctx context.Context, workspace, job string, m manifest, create bool) (receipt, error) {
	return initializeWithEmptySchema(ctx, workspace, job, m, create, nil)
}

func initializeWithEmptySchema(ctx context.Context, workspace, job string, m manifest, create bool, seed *emptyFundingSchema) (result receipt, err error) {
	root, path, err := jobPath(workspace, job)
	if err != nil {
		return result, err
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return result, errInput
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result = receipt{Mode: "checked_new_job_no_source_connection", GroupID: m.GroupID, Month: m.Month, Accounts: len(m.Older.UserIDs)}
	if !create {
		return result, nil
	}
	lock, err := workspaceLock(root, true, true)
	if err != nil {
		return receipt{}, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	// Exclusive directory creation avoids overwriting or repairing existing jobs.
	// Any partial creation stays incomplete for operator inspection.
	if err := os.Mkdir(path, 0700); err != nil {
		return receipt{}, err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return receipt{}, err
	}
	pending := filepath.Join(path, "manifest.pending")
	if err := exclusiveFile(pending, raw); err != nil {
		return receipt{}, err
	}
	database := filepath.Join(path, "funding.sqlite")
	var empty []byte
	if seed != nil {
		empty = seed.raw
	}
	if err := exclusiveFile(database, empty); err != nil {
		return receipt{}, err
	}
	if len(empty) == 0 {
		db, err := openReceiver(ctx, database, true)
		if err != nil {
			return receipt{}, err
		}
		err = monthlysource.InitFundingPageStore(ctx, db)
		err = errors.Join(err, db.Close())
		if err != nil {
			return receipt{}, err
		}
		if seed != nil {
			// The workspace lock is still held and manifest.json does not exist.
			// Later writes to this first job therefore cannot enter the seed.
			seed.raw, err = privateRead(database, 1<<20)
			if err != nil {
				return receipt{}, err
			}
		}
	}
	if err := syncPath(database); err != nil {
		return receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return receipt{}, err
	}
	if err := os.Link(pending, filepath.Join(path, "manifest.json")); err != nil {
		return receipt{}, err
	}
	// Both names temporarily reference our own manifest; remove only pending.
	if err := os.Remove(pending); err != nil {
		return receipt{}, err // Incomplete marker remains; execution rejects it.
	}
	result = manifestReceipt(m, raw)
	result.Mode, result.Created = "created_empty_capture_job_not_bill", true
	return result, errors.Join(syncPath(path), syncPath(root))
}

func manifestReceipt(m manifest, raw []byte) receipt {
	return receipt{ManifestSHA256: hash(raw), GroupID: m.GroupID, Month: m.Month, Accounts: len(m.Older.UserIDs), OlderScanID: m.Older.ID, NewerScanID: m.Newer.ID}
}

func readManifest(path, expected string) (manifest, []byte, error) {
	var m manifest
	if _, err := privateInfo(path, true); err != nil {
		return m, nil, err
	}
	if _, err := os.Lstat(filepath.Join(path, "manifest.pending")); !os.IsNotExist(err) {
		return m, nil, errInput
	}
	raw, err := privateRead(filepath.Join(path, "manifest.json"), 32<<10)
	if err != nil || hash(raw) != expected || json.Unmarshal(raw, &m) != nil || m.validate(time.Now()) != nil {
		return manifest{}, nil, errInput
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(raw, canonical) {
		return manifest{}, nil, errInput
	}
	return m, raw, nil
}
