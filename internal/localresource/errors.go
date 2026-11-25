// Package localresource classifies typed, retryable local storage pressure.
// It owns no scheduler and never decides that business data is valid.
package localresource

import (
	"errors"
	"syscall"

	sqlite "github.com/glebarez/go-sqlite"
)

func Temporary(err error) bool {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return true
	}
	var e *sqlite.Error
	if errors.As(err, &e) {
		switch e.Code() & 0xff {
		case 5, 6, 13: // SQLITE_BUSY, LOCKED, FULL; not IOERR/CORRUPT/READONLY
			return true
		}
	}
	return false
}
