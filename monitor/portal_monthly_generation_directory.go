package monitor

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

// MonthlyGenerationDirectory contains display labels from the SAME authorized
// local read view as the complete member roster. It contains no credentials,
// contact details, balances or assertions about historical wallet semantics.
type MonthlyGenerationDirectory struct {
	Scope   MonthlyGenerationScope
	Name    string
	Members []MonthlyBundleMember
}

// Session-derived IDs only. This read starts no collector, schema initialization
// or repair. A current label snapshot never replaces historical financial data.
func LoadMonthlyGenerationDirectory(parent context.Context, db *gorm.DB, gid, version int64) (MonthlyGenerationDirectory, error) {
	var out MonthlyGenerationDirectory
	if parent == nil || db == nil {
		return out, errors.New("monthly directory unavailable")
	}
	if gid <= 0 || version < 0 {
		return out, ErrMonthlyGenerationUnauthorized
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
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
		label := func(s string) bool { return len(s) <= 256 && strings.TrimSpace(s) != "" && utf8.ValidString(s) }
		if !label(scope.Group.Name) || len(scope.Members) == 0 || len(scope.Members) > 200 {
			return errors.New("monthly directory labels or roster incomplete")
		}
		out.Name = scope.Group.Name
		out.Scope = MonthlyGenerationScope{GroupID: gid, AuthVersion: version, MemberFingerprint: scope.Fingerprint, MemberManifest: scope.Manifest}
		for _, member := range scope.Members {
			if member.UserID <= 0 || !label(member.Username) {
				return errors.New("monthly member label unavailable")
			}
			out.Members = append(out.Members, MonthlyBundleMember{UserID: member.UserID, Username: member.Username})
		}
		sort.Slice(out.Members, func(i, j int) bool { return out.Members[i].UserID < out.Members[j].UserID })
		for i, member := range out.Members {
			if i > 0 && out.Members[i-1].UserID == member.UserID {
				return errors.New("monthly directory contains duplicate account")
			}
			out.Scope.UserIDs = append(out.Scope.UserIDs, member.UserID)
		}
		return ctx.Err()
	}, &sql.TxOptions{ReadOnly: true})
	if err != nil || ctx.Err() != nil {
		return MonthlyGenerationDirectory{}, errors.Join(err, ctx.Err())
	}
	return out, nil
}
