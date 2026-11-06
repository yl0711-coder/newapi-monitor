package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// SelectedGenerationRequest reads the CURRENT server-selected immutable input
// descriptor for a group/month. It must not echo client input or infer "current"
// from the existence of retained task files. Unavailable selection is an error,
// not permission to reuse an older version. No production selector is wired yet.
type SelectedGenerationRequest func(context.Context, int64, string) (monthlytasks.Request, error)

// LoadUsageGenerationOwner derives the complete member scope from existing Usage
// controls. gid/version must be obtained from an authenticated server session.
func LoadUsageGenerationOwner(ctx context.Context, db *gorm.DB, gid, version int64) (monthlytasks.Owner, []int64, error) {
	scope, err := monitor.LoadMonthlyGenerationScope(ctx, db, gid, version)
	if err != nil {
		if errors.Is(err, monitor.ErrMonthlyGenerationUnauthorized) {
			return monthlytasks.Owner{}, nil, ErrGenerationChanged
		}
		return monthlytasks.Owner{}, nil, err
	}
	return monthlytasks.Owner{GroupID: scope.GroupID, AuthVersion: scope.AuthVersion,
		MemberFingerprint: scope.MemberFingerprint, MemberManifest: scope.MemberManifest}, scope.UserIDs, nil
}

// NewUsageGenerationGuard combines real Usage authorization reads with a required
// trusted source selector. The second auth read catches changes during selection.
// Transient failures never invalidate a task. This does not publish a bill or
// attest that normalized financial evidence is complete or historically correct.
func NewUsageGenerationGuard(db *gorm.DB, selected SelectedGenerationRequest) (VerifyGenerationCurrent, error) {
	if db == nil || selected == nil {
		return nil, ErrGenerationInput
	}
	return func(parent context.Context, request monthlytasks.Request) error {
		ctx, cancel := context.WithTimeout(parent, 3*time.Second)
		defer cancel()
		month, err := time.Parse("2006-01", request.Month)
		if err != nil || month.Format("2006-01") != request.Month || month.Year() < 2020 ||
			!generationHex(request.SourceIdentity, 32) || !generationHex(request.InputSHA256, 32) || !generationHex(request.RulesSHA256, 32) {
			return ErrGenerationInput
		}
		checkOwner := func() error {
			owner, _, err := LoadUsageGenerationOwner(ctx, db, request.Owner.GroupID, request.Owner.AuthVersion)
			if err != nil {
				return err
			}
			if owner != request.Owner {
				return ErrGenerationChanged
			}
			return nil
		}
		if err := checkOwner(); err != nil {
			return err
		}
		current, err := selected(ctx, request.Owner.GroupID, request.Month)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if current.Owner.GroupID != request.Owner.GroupID || current.Month != request.Month || current.Owner.AuthVersion < 0 ||
			!generationHex(current.Owner.MemberFingerprint, 16) || !generationHex(current.Owner.MemberManifest, 32) ||
			!generationHex(current.SourceIdentity, 32) || !generationHex(current.InputSHA256, 32) || !generationHex(current.RulesSHA256, 32) {
			return ErrGenerationIntegrity // Missing/corrupt selection is not proven revocation.
		}
		if current != request {
			return ErrGenerationChanged
		}
		if err := checkOwner(); err != nil {
			return err
		}
		return ctx.Err()
	}, nil
}
