package monitor

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ErrMonthlyGenerationUnauthorized denotes a proven missing/revoked session,
// not an unavailable database or inconsistent membership projection.
var ErrMonthlyGenerationUnauthorized = errors.New("monthly generation authorization revoked")

// MonthlyGenerationScope is internal authorization metadata, not a customer DTO.
// It deliberately excludes credentials, email addresses and financial records.
type MonthlyGenerationScope struct {
	GroupID, AuthVersion int64
	MemberFingerprint    string
	MemberManifest       string
	UserIDs              []int64
}

// LoadMonthlyGenerationScope reads existing Usage controls without initializing
// or repairing them. gid/version MUST come from a verified server-side session,
// never request-body fields. The single transaction keeps group credentials and
// member revisions in one SQLite snapshot. Call again before returning results.
// This function installs no customer route or generation worker.
func LoadMonthlyGenerationScope(ctx context.Context, db *gorm.DB, gid, version int64) (MonthlyGenerationScope, error) {
	var out MonthlyGenerationScope
	if gid <= 0 || version < 0 {
		return out, ErrMonthlyGenerationUnauthorized
	}
	if db == nil {
		return out, errors.New("monthly authorization database unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		scope, err := loadMonthlyBillAuthorization(ctx, tx, gid)
		if errors.Is(err, errPortalAuthorizationInvalid) {
			return ErrMonthlyGenerationUnauthorized
		}
		if err != nil {
			return err
		}
		if scope.Group.PortalAuthVer != version || strings.TrimSpace(scope.Group.PortalEmail) == "" {
			return ErrMonthlyGenerationUnauthorized
		}
		out = MonthlyGenerationScope{GroupID: gid, AuthVersion: version,
			MemberFingerprint: scope.Fingerprint, MemberManifest: scope.Manifest,
			UserIDs: make([]int64, 0, len(scope.Members))}
		for _, member := range scope.Members {
			out.UserIDs = append(out.UserIDs, member.UserID)
		}
		sort.Slice(out.UserIDs, func(i, j int) bool { return out.UserIDs[i] < out.UserIDs[j] })
		return ctx.Err()
	}, &sql.TxOptions{ReadOnly: true})
	if err != nil || ctx.Err() != nil {
		// database/sql can roll a canceled transaction back before GORM commits
		// or returns its query error, leaving only sql.ErrTxDone. Preserve the
		// cancellation cause for retry/status classification, but never expose
		// a partially read owner or turn a storage failure into revocation.
		return MonthlyGenerationScope{}, errors.Join(err, ctx.Err())
	}
	return out, nil
}
