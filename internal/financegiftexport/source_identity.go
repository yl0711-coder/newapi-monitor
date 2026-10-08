package financegiftexport

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

type SourceIdentity struct{ Database, ServerUUID string }

type identityQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// VerifySourceIdentity must also run inside each evidence page transaction;
// a reconnected pool must not silently continue against a different server.
func VerifySourceIdentity(ctx context.Context, source identityQuerier, expected SourceIdentity) error {
	if expected.Database == "" || expected.ServerUUID == "" {
		return errors.New("explicit source identity required")
	}
	var uuid, database, user string
	if err := source.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE(), CURRENT_USER()").Scan(&uuid, &database, &user); err != nil {
		return errors.New("cannot verify source identity; no evidence query executed")
	}
	if uuid != expected.ServerUUID || database != expected.Database || !strings.HasPrefix(user, "monitor_ro@") {
		return errors.New("source server, database or authenticated user mismatch; no evidence query executed")
	}
	return nil
}
