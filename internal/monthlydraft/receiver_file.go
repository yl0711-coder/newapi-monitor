package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

type ReceiverFileResult struct {
	Mode    string `json:"mode"`
	Applied bool   `json:"applied"`
}

// InitializeGenerationReceiverFile creates ONLY a new independent receiver in
// an existing private directory. It never migrates/overwrites an existing file.
// Schemas are completed in a private temporary file, then published with an
// exclusive hard link. No application startup calls this function.
func InitializeGenerationReceiverFile(parent context.Context, path string, apply bool) (result ReceiverFileResult, err error) {
	if parent == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return result, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return result, ErrGenerationInput
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return result, ErrGenerationInput
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.Mode = "checked_new_receiver_path"
	if !apply {
		return result, nil
	}
	f, err := os.CreateTemp(dir, ".monthly-receiver-*.sqlite")
	if err != nil {
		return ReceiverFileResult{}, err
	}
	temporary := f.Name()
	temporaryPresent := true
	defer func() {
		// Only this invocation's private temporary name, never the target or a
		// directory. If publication succeeded, preserve its receipt on failure.
		if temporaryPresent {
			err = errors.Join(err, os.Remove(temporary))
		}
	}()
	if err := f.Close(); err != nil {
		return ReceiverFileResult{}, err
	}
	u := url.URL{Scheme: "file", Path: temporary}
	u.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"journal_mode(DELETE)", "synchronous(FULL)", "busy_timeout(500)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return ReceiverFileResult{}, err
	}
	db.SetMaxOpenConns(1)
	err = InitGenerationReceiver(ctx, db)
	if err == nil {
		err = InitGenerationSelections(ctx, db)
	}
	err = errors.Join(err, db.Close())
	if err != nil {
		return ReceiverFileResult{}, err
	}
	f, err = os.OpenFile(temporary, os.O_RDWR, 0)
	if err != nil {
		return ReceiverFileResult{}, err
	}
	err = errors.Join(f.Sync(), f.Close())
	if err != nil {
		return ReceiverFileResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ReceiverFileResult{}, err
	}
	// Link fails if ANY target already exists, including another initializer's
	// completed receiver or a dangling symlink. Rename would overwrite it.
	if err := os.Link(temporary, path); err != nil {
		return ReceiverFileResult{}, err
	}
	result = ReceiverFileResult{Mode: "created_empty_receiver_not_publication", Applied: true}
	if err := os.Remove(temporary); err != nil {
		return result, err
	}
	temporaryPresent = false
	directory, err := os.Open(dir)
	if err != nil {
		return result, err
	}
	return result, errors.Join(directory.Sync(), directory.Close())
}
