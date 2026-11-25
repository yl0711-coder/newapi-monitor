package monitor

import (
	"errors"
	"github.com/yl0711-coder/newapi-monitor/internal/localresource"
)

// Classify only typed local resource failures, never upstream response text.
// SQLITE_IOERR, CORRUPT, READONLY and permission failures require diagnosis;
// free space alone is not evidence that those conditions have recovered.
func localResourceTemporarilyUnavailable(err error) bool {
	return localresource.Temporary(err) || errors.Is(err, errUsageFactHistoryDiskPressure) || errors.Is(err, errAICodeWithRecordCapacity)
}
