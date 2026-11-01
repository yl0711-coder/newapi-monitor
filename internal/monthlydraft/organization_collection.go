package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// OrganizationCollectionRequest contains only a current organization, closed
// month and independently pinned source identity. NO caller-selected user IDs,
// money, source credentials, history attestations or rollout changes.
type OrganizationCollectionRequest struct {
	AuthorityPath, OutputRoot string
	GroupID                   int64
	Month, SourceIdentity     string
	Write                     bool
}

type OrganizationCollectionReceipt struct {
	Mode        string `json:"mode"`
	GroupID     int64  `json:"group_id"`
	Month       string `json:"month"`
	Accounts    int    `json:"accounts"`
	ScopeSHA256 string `json:"scope_sha256"`
	Directory   string `json:"directory,omitempty"`
	Complete    bool   `json:"complete"`
}

var organizationCollectionSlots = make(chan struct{}, 1)

// PrepareOrganizationCollection builds one immutable current roster from the
// existing Usage authority. It opens only that local DB in read-only mode;
// default inspection writes nothing. Actual capture/history/preparation still
// require their existing independent source and current-authorization checks.
func PrepareOrganizationCollection(parent context.Context, request OrganizationCollectionRequest) (OrganizationCollectionReceipt, error) {
	return prepareOrganizationCollection(parent, request, writeArchiveFile)
}

func prepareOrganizationCollection(parent context.Context, request OrganizationCollectionRequest,
	writeFile func(context.Context, *os.Root, string, []byte) error) (receipt OrganizationCollectionReceipt, err error) {
	if parent == nil || writeFile == nil || request.GroupID <= 0 || !generationHex(request.SourceIdentity, 32) ||
		!filepath.IsAbs(request.AuthorityPath) || filepath.Clean(request.AuthorityPath) != request.AuthorityPath {
		return receipt, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	select {
	case organizationCollectionSlots <- struct{}{}:
		defer func() { <-organizationCollectionSlots }()
	default:
		return receipt, ErrGenerationBusy
	}
	before, err := os.Lstat(request.AuthorityPath)
	actual, pathErr := filepath.EvalSymlinks(request.AuthorityPath)
	if err != nil || pathErr != nil || !before.Mode().IsRegular() || actual != request.AuthorityPath {
		return receipt, ErrGenerationInput
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return receipt, ErrGenerationIntegrity
	}
	output, err := filepath.EvalSymlinks(request.OutputRoot)
	if err != nil || output != request.OutputRoot {
		return receipt, ErrGenerationInput
	}
	root, err := openArchiveRoot(request.OutputRoot)
	if err != nil {
		return receipt, err
	}
	defer root.Close()
	if err := automaticPreparationOutputEmpty(root); err != nil {
		return receipt, err
	}
	u := url.URL{Scheme: "file", Path: request.AuthorityPath}
	u.RawQuery = url.Values{"mode": {"ro"}, "_pragma": {"query_only(1)", "busy_timeout(500)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return receipt, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	auth, err := gorm.Open(&sqlite.Dialector{Conn: db}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if err != nil {
		return receipt, err
	}
	var group monitor.CustomerGroup
	if err := auth.WithContext(ctx).Select("id", "portal_auth_ver").First(&group, request.GroupID).Error; err != nil {
		return receipt, err
	}
	owner, ids, err := LoadUsageGenerationOwner(ctx, auth, request.GroupID, group.PortalAuthVer)
	if err != nil {
		return receipt, err
	}
	scope := monthlysource.OrganizationMonthScope{Version: 1, Mode: monthlysource.OrganizationMonthScopeMode, GroupID: owner.GroupID,
		AuthVersion: owner.AuthVersion, MemberFingerprint: owner.MemberFingerprint, MemberManifest: owner.MemberManifest,
		Funding: monthlysource.FundingMonthRequest{Month: request.Month, SourceIdentity: request.SourceIdentity,
			Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserIDs: ids}}
	raw, err := json.Marshal(scope)
	if err != nil {
		return receipt, err
	}
	if _, err := monthlysource.DecodeOrganizationMonthScope(raw, time.Now()); err != nil {
		return receipt, ErrGenerationInput
	}
	check := func() error {
		current, _, err := LoadUsageGenerationOwner(ctx, auth, request.GroupID, group.PortalAuthVer)
		if err != nil {
			return err
		}
		if current != owner {
			return ErrGenerationChanged
		}
		after, err := os.Lstat(request.AuthorityPath)
		if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
			return ErrGenerationIntegrity
		}
		stat, ok := after.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return ErrGenerationIntegrity
		}
		return automaticPreparationRootCurrent(root, request.OutputRoot)
	}
	if err := check(); err != nil {
		return receipt, err
	}
	receipt = OrganizationCollectionReceipt{Mode: "checked_current_roster_no_capture_or_output", GroupID: owner.GroupID, Month: request.Month, Accounts: len(ids), ScopeSHA256: generationHash(raw)}
	if !request.Write {
		return receipt, nil
	}
	if err := automaticPreparationOutputEmpty(root); err != nil {
		return receipt, err
	}
	receipt.Mode, receipt.Directory = "incomplete_current_roster_do_not_use", request.OutputRoot
	if err := writeFile(ctx, root, "organization.pending.json", raw); err != nil {
		return receipt, err
	}
	if err := check(); err != nil {
		return receipt, err
	}
	stored, err := readArchiveFile(ctx, root, "organization.pending.json", monthlysource.OrganizationMonthScopeLimit)
	if err != nil || generationHash(stored) != receipt.ScopeSHA256 {
		return receipt, errors.Join(ErrGenerationIntegrity, err)
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	if err := root.Link("organization.pending.json", "organization.json"); err != nil {
		return receipt, err
	}
	// Consumers reject hardlink aliases. Remove only this call's own pending
	// name after exclusive publication, leaving a single private retained file.
	if err := root.Remove("organization.pending.json"); err != nil {
		return receipt, err
	}
	receipt.Mode, receipt.Complete = "sealed_current_roster_not_capture_history_or_bill", true
	return receipt, errors.Join(syncArchiveDirectory(root), ctx.Err())
}
