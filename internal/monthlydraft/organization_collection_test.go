package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

func organizationCollectionFixture(t *testing.T) (*gorm.DB, OrganizationCollectionRequest) {
	t.Helper()
	db, _, path := usageAuthorityFixture(t)
	actual, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	return db, OrganizationCollectionRequest{AuthorityPath: actual, OutputRoot: root, GroupID: 1, Month: "2026-09", SourceIdentity: strings.Repeat("a", 64)}
}

func TestOrganizationCollectionCompleteCurrentRosterReadOnlyAndGeneric(t *testing.T) {
	db, r := organizationCollectionFixture(t)
	before, err := os.ReadFile(r.AuthorityPath)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := PrepareOrganizationCollection(context.Background(), r)
	if err != nil || preview.Complete || preview.Accounts != 2 || preview.Directory != "" {
		t.Fatal("read-only roster failed", preview, err)
	}
	again, err := PrepareOrganizationCollection(context.Background(), r)
	if err != nil || !reflect.DeepEqual(preview, again) {
		t.Fatal("current roster inspection unstable", err)
	}
	if entries, err := os.ReadDir(r.OutputRoot); err != nil || len(entries) != 0 {
		t.Fatal("preflight wrote files", err)
	}
	r.Write = true
	result, err := PrepareOrganizationCollection(context.Background(), r)
	if err != nil || !result.Complete || result.ScopeSHA256 != preview.ScopeSHA256 {
		t.Fatal("current roster creation failed", result, err)
	}
	raw, err := os.ReadFile(filepath.Join(r.OutputRoot, "organization.json"))
	if err != nil {
		t.Fatal(err)
	}
	scope, err := monthlysource.DecodeOrganizationMonthScope(raw, time.Now())
	if err != nil || scope.Mode != monthlysource.OrganizationMonthScopeMode || !reflect.DeepEqual(scope.Funding.UserIDs, []int64{201, 202}) || generationHash(raw) != result.ScopeSHA256 {
		t.Fatal("complete authoritative roster not retained", err)
	}
	for _, bad := range []string{"owner@example.invalid", "password", "balance", "quota\"", "approved", "wallet_only", "current_group_id"} {
		if strings.Contains(string(raw), bad) {
			t.Fatal("credentials, money or history assertion entered current roster", bad)
		}
	}
	after, err := os.ReadFile(r.AuthorityPath)
	if err != nil || string(before) != string(after) {
		t.Fatal("metadata planner wrote authority DB", err)
	}
	if _, err := PrepareOrganizationCollection(context.Background(), r); err == nil {
		t.Fatal("existing roster overwritten")
	}
	// Enroll a genuinely NEW synthetic organization; no code or user-ID flags.
	group := monitor.CustomerGroup{ID: 81733, Name: "new generic collection group", PortalEmail: "new-collection@example.invalid", PortalAuthVer: 9}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&monitor.TrackedUser{UserID: 871601, GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	r.GroupID, r.OutputRoot = group.ID, root
	other, err := PrepareOrganizationCollection(context.Background(), r)
	if err != nil || other.Accounts != 1 || other.ScopeSHA256 == result.ScopeSHA256 {
		t.Fatal("new organization not handled generically", err)
	}
}

func TestOrganizationCollectionInvalidInputNoMarkerOrInitialization(t *testing.T) {
	for _, kind := range []string{"unknown", "disabled", "empty_members", "missing_controls", "month", "source", "missing_db", "public_output", "nonempty_output", "symlink", "hardlink", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			db, r := organizationCollectionFixture(t)
			r.Write = true
			ctx := context.Background()
			switch kind {
			case "unknown":
				r.GroupID = 999999
			case "disabled":
				if err := db.Exec("UPDATE customer_groups SET portal_email='' WHERE id=1").Error; err != nil {
					t.Fatal(err)
				}
			case "empty_members":
				r.GroupID = 88701
				if err := db.Create(&monitor.CustomerGroup{ID: r.GroupID, Name: "empty collection group", PortalEmail: "empty-collection@example.invalid"}).Error; err != nil {
					t.Fatal(err)
				}
			case "missing_controls":
				if err := db.Exec("DELETE FROM usage_member_controls WHERE user_id=201").Error; err != nil {
					t.Fatal(err)
				}
			case "month":
				r.Month = "2099-01"
			case "source":
				r.SourceIdentity = "unknown"
			case "missing_db":
				r.AuthorityPath = filepath.Join(filepath.Dir(r.AuthorityPath), "never-create.sqlite")
			case "public_output":
				if err := os.Chmod(r.OutputRoot, 0755); err != nil {
					t.Fatal(err)
				}
			case "nonempty_output":
				if err := os.WriteFile(filepath.Join(r.OutputRoot, "keep"), []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink", "hardlink":
				alias := filepath.Join(filepath.Dir(r.AuthorityPath), "alias.sqlite")
				var err error
				if kind == "symlink" {
					err = os.Symlink(r.AuthorityPath, alias)
				} else {
					err = os.Link(r.AuthorityPath, alias)
				}
				if err != nil {
					t.Fatal(err)
				}
				r.AuthorityPath = alias
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			out, err := PrepareOrganizationCollection(ctx, r)
			if err == nil || out.Complete || out.Directory != "" {
				t.Fatal("invalid current roster accepted", out, err)
			}
			if _, err := os.Stat(filepath.Join(r.OutputRoot, "organization.json")); !os.IsNotExist(err) {
				t.Fatal("invalid scope published", err)
			}
			if kind == "missing_db" {
				if _, err := os.Stat(r.AuthorityPath); !os.IsNotExist(err) {
					t.Fatal("missing source initialized", err)
				}
			}
		})
	}
}

func TestOrganizationCollectionLateChangeAndDiskFailureNeverComplete(t *testing.T) {
	for _, kind := range []string{"member", "auth", "corrupt", "disk", "cancel", "directory"} {
		t.Run(kind, func(t *testing.T) {
			db, r := organizationCollectionFixture(t)
			r.Write = true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if kind == "disk" {
					return errors.New("synthetic disk full")
				}
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				switch kind {
				case "member":
					return db.Create(&monitor.TrackedUser{UserID: 3001, GroupID: 1}).Error
				case "auth":
					return db.Exec("UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1").Error
				case "corrupt":
					return os.WriteFile(filepath.Join(r.OutputRoot, name), []byte("bad"), 0600)
				case "cancel":
					cancel()
				case "directory":
					if err := os.Rename(r.OutputRoot, r.OutputRoot+"-moved"); err != nil {
						return err
					}
					return os.Mkdir(r.OutputRoot, 0700)
				}
				return nil
			}
			out, err := prepareOrganizationCollection(ctx, r, write)
			if err == nil || out.Complete || out.Directory != r.OutputRoot {
				t.Fatal("late failure marked roster complete", out, err)
			}
			if _, err := os.Stat(filepath.Join(r.OutputRoot, "organization.json")); !os.IsNotExist(err) {
				t.Fatal("late failure published completion marker", err)
			}
		})
	}
}

func TestOrganizationCollectionBusySlotAndRecovery(t *testing.T) {
	_, r := organizationCollectionFixture(t)
	organizationCollectionSlots <- struct{}{}
	_, err := PrepareOrganizationCollection(context.Background(), r)
	<-organizationCollectionSlots
	if !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("unbounded parallel collection planning", err)
	}
	if _, err := PrepareOrganizationCollection(context.Background(), r); err != nil {
		t.Fatal("busy slot not released", err)
	}
}

func TestOrganizationCollectionReadsLiveWALAndDoesNotBlockOtherOrganizations(t *testing.T) {
	db, r := organizationCollectionFixture(t)
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE customer_groups SET portal_auth_ver=7 WHERE id=1").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&monitor.TrackedUser{UserID: 3011, GroupID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	r.Write = true
	write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
		if err := writeArchiveFile(ctx, root, name, raw); err != nil {
			return err
		}
		return db.Create(&monitor.TrackedUser{UserID: 4011, GroupID: 2}).Error
	}
	receipt, err := prepareOrganizationCollection(context.Background(), r, write)
	if err != nil || !receipt.Complete || receipt.Accounts != 3 {
		t.Fatal("WAL current members missed or unrelated change blocked scope", err)
	}
	raw, err := os.ReadFile(filepath.Join(r.OutputRoot, "organization.json"))
	if err != nil {
		t.Fatal(err)
	}
	scope, err := monthlysource.DecodeOrganizationMonthScope(raw, time.Now())
	if err != nil || scope.AuthVersion != 7 || !reflect.DeepEqual(scope.Funding.UserIDs, []int64{201, 202, 3011}) {
		t.Fatal("live authority treated as immutable stale main file", err)
	}
}

func TestOrganizationCollectionOldRosterCannotApproveChangedMemberBill(t *testing.T) {
	f, cfg, prepared := automaticPreparationFileFixture(t)
	auth := generationDB(t, cfg.AuthorityPath, false)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	authority, err := filepath.EvalSymlinks(cfg.AuthorityPath)
	if err != nil {
		t.Fatal(err)
	}
	r := OrganizationCollectionRequest{AuthorityPath: authority, OutputRoot: root, GroupID: f.plan.Owner.GroupID, Month: f.plan.Funding.Month,
		SourceIdentity: f.plan.Funding.SourceIdentity, Write: true}
	receipt, err := PrepareOrganizationCollection(context.Background(), r)
	if err != nil || !receipt.Complete {
		t.Fatal(err)
	}
	// Change the CURRENT roster after the immutable file has been completed.
	// It is still a valid historical copy of a collection target, not authority
	// for retaining a bill with that now-stale two-account generation owner.
	if _, err := auth.Exec(`UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201`); err != nil {
		t.Fatal(err)
	}
	prepared.Write = true
	out, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, prepared)
	if !errors.Is(err, ErrGenerationChanged) || out.Complete || out.Directory != "" {
		t.Fatal("completed collection roster bypassed current bill authority", out, err)
	}
	if entries, err := os.ReadDir(prepared.OutputRoot); err != nil || len(entries) != 0 {
		t.Fatal("stale roster wrote bill preparation", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "organization.json"))
	if err != nil || generationHash(raw) != receipt.ScopeSHA256 {
		t.Fatal("old roster was rewritten", err)
	}
	preparationCounts(t, f, 0, 0)
}

// The existing mandatory preparation CLI gate invokes this helper, so the new
// planner is tested with the current actual binary instead of silently skipped.
func testOrganizationCollectionCLI(t *testing.T, binary string) {
	t.Helper()
	_, r := organizationCollectionFixture(t)
	args := []string{"plan-collection", "-authority", r.AuthorityPath, "-group-id=1", "-month", r.Month, "-source-identity", r.SourceIdentity, "-output-root", r.OutputRoot}
	for _, write := range []bool{false, true} {
		current := append([]string{}, args...)
		if write {
			current = append(current, "-write")
		}
		raw, err := exec.CommandContext(t.Context(), binary, current...).CombinedOutput()
		var out OrganizationCollectionReceipt
		if err != nil || json.Unmarshal(raw, &out) != nil || out.Complete != write || out.Accounts != 2 {
			t.Fatal("actual current-roster CLI failed", err, string(raw))
		}
	}
}
