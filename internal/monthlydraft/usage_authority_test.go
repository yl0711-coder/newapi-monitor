package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestUsageGenerationAuthorityNewOrganizationsAndMembersAreGeneric(t *testing.T) {
	db, ro, _ := usageAuthorityFixture(t)
	ctx := context.Background()
	owners := map[int64]monthlytasks.Owner{}
	for i := 0; i < 8; i++ {
		gid := int64(70001 + i*97)
		group := monitor.CustomerGroup{ID: gid, Name: fmt.Sprintf("synthetic new organization %d", gid), PortalEmail: fmt.Sprintf("new-%d@example.invalid", gid), PortalAuthVer: 1}
		if err := db.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
		ids := []int64{}
		for j := 0; j < 1+i%5; j++ {
			uid := int64(800000 + i*100 + j)
			if err := db.Create(&monitor.TrackedUser{UserID: uid, GroupID: gid, Username: "same user label"}).Error; err != nil {
				t.Fatal(err)
			}
			ids = append(ids, uid)
		}
		owner, actual, err := LoadUsageGenerationOwner(ctx, ro, gid, 1)
		if err != nil || !reflect.DeepEqual(actual, ids) || owner.MemberFingerprint != generationMemberHash(ids) {
			t.Fatal("new organization needs a special case", gid, err)
		}
		owners[gid] = owner
	}
	changedID := int64(70001 + 3*97)
	if err := db.Create(&monitor.TrackedUser{UserID: 990001, GroupID: changedID, Username: "newly enrolled"}).Error; err != nil {
		t.Fatal(err)
	}
	for gid, owner := range owners {
		current, _, err := LoadUsageGenerationOwner(ctx, ro, gid, 1)
		if err != nil {
			t.Fatal(err)
		}
		if gid == changedID {
			if current == owner {
				t.Fatal("new member did not change full generation scope")
			}
		} else if current != owner {
			t.Fatal("another organization was changed by new enrollment", gid)
		}
		request := monthlytasks.Request{Owner: owner, Month: "2026-09", SourceIdentity: strings.Repeat("a", 64), InputSHA256: strings.Repeat("b", 64), RulesSHA256: strings.Repeat("c", 64)}
		guard, err := NewUsageGenerationGuard(ro, func(_ context.Context, selectedGroup int64, _ string) (monthlytasks.Request, error) {
			if selectedGroup != gid {
				t.Fatal("cross-organization selection")
			}
			return request, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		err = guard(ctx, request)
		if gid == changedID {
			if !errors.Is(err, ErrGenerationChanged) {
				t.Fatal("old member set accepted after enrollment", err)
			}
		} else if err != nil {
			t.Fatal("unrelated organization became unavailable", gid, err)
		}
	}
}

func usageAuthorityFixture(t *testing.T) (*gorm.DB, *gorm.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "usage.sqlite")
	open := func(mode string) *gorm.DB {
		db, err := gorm.Open(sqlite.Open("file:"+path+"?mode="+mode+"&_pragma=busy_timeout(500)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.SetMaxOpenConns(2)
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	db := open("rwc")
	if err := db.AutoMigrate(&monitor.CustomerGroup{}, &monitor.TrackedUser{}, &monitor.UsageMemberControl{}, &monitor.UsageMemberAudit{}, &monitor.UsageMemberControlMigration{}); err != nil {
		t.Fatal(err)
	}
	for _, v := range []any{
		&monitor.UsageMemberControlMigration{ID: 1, Version: 1, ManifestHash: strings.Repeat("a", 64), InitializedAt: 1},
		&monitor.CustomerGroup{ID: 1, Name: "synthetic org", PortalEmail: "owner@example.invalid", PortalAuthVer: 1},
		&monitor.CustomerGroup{ID: 2, Name: "other org", PortalEmail: "other@example.invalid", PortalAuthVer: 1},
		&monitor.TrackedUser{UserID: 202, GroupID: 1},
		&monitor.TrackedUser{UserID: 201, GroupID: 1},
		&monitor.TrackedUser{UserID: 999, GroupID: 2},
	} {
		if err := db.Create(v).Error; err != nil {
			t.Fatal(err)
		}
	}
	return db, open("ro"), path
}

func usageAuthorityRequest(t *testing.T, db *gorm.DB) monthlytasks.Request {
	t.Helper()
	owner, ids, err := LoadUsageGenerationOwner(context.Background(), db, 1, 1)
	if err != nil || !reflect.DeepEqual(ids, []int64{201, 202}) || owner.MemberFingerprint != generationMemberHash(ids) {
		t.Fatalf("wrong authoritative scope: %+v %v %v", owner, ids, err)
	}
	return monthlytasks.Request{Owner: owner, Month: "2026-09", SourceIdentity: strings.Repeat("a", 64), InputSHA256: strings.Repeat("b", 64), RulesSHA256: strings.Repeat("c", 64)}
}

func TestUsageGenerationAuthorityReadOnlyAndNoCrossOrganization(t *testing.T) {
	_, ro, path := usageAuthorityFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	req := usageAuthorityRequest(t, ro)
	calls := 0
	guard, err := NewUsageGenerationGuard(ro, func(_ context.Context, gid int64, month string) (monthlytasks.Request, error) {
		calls++
		if gid != 1 || month != "2026-09" {
			t.Fatal("selection scope changed")
		}
		return req, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := guard(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	foreign := req
	foreign.Owner.GroupID = 2
	if err := guard(context.Background(), foreign); !errors.Is(err, ErrGenerationChanged) || calls != 3 {
		t.Fatal("foreign scope reached selection", err, calls)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("authorization read modified source database")
	}
	if _, err := NewUsageGenerationGuard(ro, nil); !errors.Is(err, ErrGenerationInput) {
		t.Fatal(err)
	}
	if _, err := NewUsageGenerationGuard(nil, func(context.Context, int64, string) (monthlytasks.Request, error) { return req, nil }); !errors.Is(err, ErrGenerationInput) {
		t.Fatal(err)
	}
}

func TestUsageGenerationAuthorityConsistentSnapshotDuringMemberChange(t *testing.T) {
	db, ro, _ := usageAuthorityFixture(t)
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		t.Fatal(err)
	}
	before := usageAuthorityRequest(t, ro).Owner
	changed := false
	const callback = "monthly_test_concurrent_membership"
	if err := ro.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if changed || tx.Statement.Table != "customer_groups" || tx.Error != nil {
			return
		}
		changed = true
		// Simulate one committed administrative change after the group SELECT,
		// before the membership SELECT. All mutations are in the synthetic DB.
		err := db.Transaction(func(writer *gorm.DB) error {
			if err := writer.Exec("UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1").Error; err != nil {
				return err
			}
			return writer.Exec("UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201").Error
		})
		if err != nil {
			_ = tx.AddError(err) // GORM propagates the recorded callback error.
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ro.Callback().Query().Remove(callback) })
	got, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil || !changed || got != before {
		t.Fatal("mixed credentials/member snapshot", got, err)
	}
	if _, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("next read missed committed revocation", err)
	}
	newOwner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 2)
	if err != nil || newOwner.MemberManifest == before.MemberManifest || newOwner.AuthVersion != 2 {
		t.Fatal("new session did not observe new membership", err)
	}
}

func TestUsageGenerationAuthorityRevocationAndSelectionChanges(t *testing.T) {
	for _, scenario := range []string{"password_version", "disabled", "removed_group", "member_removed", "member_rejoined", "member_moved", "source_version", "input_version", "rules_version", "during_selection"} {
		t.Run(scenario, func(t *testing.T) {
			db, ro, _ := usageAuthorityFixture(t)
			req := usageAuthorityRequest(t, ro)
			exec := func(query string) {
				t.Helper()
				if err := db.Exec(query).Error; err != nil {
					t.Fatal(err)
				}
			}
			selected := req
			switch scenario {
			case "password_version":
				exec("UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1")
			case "disabled":
				exec("UPDATE customer_groups SET portal_email='' WHERE id=1")
			case "removed_group":
				exec("DELETE FROM customer_groups WHERE id=1")
			case "member_removed":
				exec("DELETE FROM tracked_users WHERE user_id=201")
				exec("UPDATE usage_member_controls SET active=0,tracked_revision=2 WHERE user_id=201")
			case "member_rejoined":
				exec("UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201")
			case "member_moved":
				exec("UPDATE tracked_users SET group_id=2 WHERE user_id=201")
				exec("UPDATE usage_member_controls SET current_group_id=2,tracked_revision=2 WHERE user_id=201")
			case "source_version":
				selected.SourceIdentity = strings.Repeat("d", 64)
			case "input_version":
				selected.InputSHA256 = strings.Repeat("d", 64)
			case "rules_version":
				selected.RulesSHA256 = strings.Repeat("d", 64)
			}
			guard, err := NewUsageGenerationGuard(ro, func(context.Context, int64, string) (monthlytasks.Request, error) {
				if scenario == "during_selection" {
					exec("UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201")
				}
				return selected, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := guard(context.Background(), req); !errors.Is(err, ErrGenerationChanged) {
				t.Fatal("stale scope accepted", err)
			}
		})
	}
}

func TestUsageGenerationAuthorityUnavailableDoesNotRevoke(t *testing.T) {
	for _, scenario := range []string{"cancelled", "source_unavailable", "empty_selection", "wrong_selection_scope", "corrupt_control", "missing_migration", "closed_db"} {
		t.Run(scenario, func(t *testing.T) {
			db, ro, _ := usageAuthorityFixture(t)
			req := usageAuthorityRequest(t, ro)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "cancelled":
				cancel()
			case "corrupt_control":
				if err := db.Exec("DELETE FROM usage_member_controls WHERE user_id=201").Error; err != nil {
					t.Fatal(err)
				}
			case "missing_migration":
				if err := db.Exec("DELETE FROM usage_member_control_migration_state").Error; err != nil {
					t.Fatal(err)
				}
			case "closed_db":
				sqlDB, err := ro.DB()
				if err != nil {
					t.Fatal(err)
				}
				if err := sqlDB.Close(); err != nil {
					t.Fatal(err)
				}
			}
			guard, err := NewUsageGenerationGuard(ro, func(context.Context, int64, string) (monthlytasks.Request, error) {
				if scenario == "source_unavailable" {
					return monthlytasks.Request{}, context.DeadlineExceeded
				}
				if scenario == "empty_selection" {
					return monthlytasks.Request{}, nil
				}
				out := req
				if scenario == "wrong_selection_scope" {
					out.Owner.GroupID = 2
				}
				return out, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			err = guard(ctx, req)
			if err == nil || errors.Is(err, ErrGenerationChanged) {
				t.Fatal("unavailable state misclassified as revocation", err)
			}
		})
	}
}

func TestUsageGenerationAuthorityTaskInvalidationVersusTransientFailure(t *testing.T) {
	db, ro, _ := usageAuthorityFixture(t)
	f := generationFixtureNew(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	_, raw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	req := generationRequest(f.plan, raw)
	unavailable := false
	guard, err := NewUsageGenerationGuard(ro, func(context.Context, int64, string) (monthlytasks.Request, error) {
		if unavailable {
			return monthlytasks.Request{}, context.DeadlineExceeded
		}
		return req, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent = guard
	task := generationSubmit(t, f)
	unavailable = true
	if err := f.g.currentTask(context.Background(), task); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	got, err := f.g.tasks.Get(context.Background(), owner, task.ID)
	if err != nil || got.State != monthlytasks.Queued {
		t.Fatal("temporary failure invalidated task", got.State, err)
	}
	unavailable = false
	if err := db.Exec("UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201").Error; err != nil {
		t.Fatal(err)
	}
	if err := f.g.currentTask(context.Background(), task); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal(err)
	}
	got, err = f.g.tasks.Get(context.Background(), owner, task.ID)
	if err != nil || got.State != monthlytasks.Invalidated {
		t.Fatal("revocation did not invalidate task", got.State, err)
	}
}

func TestUsageGenerationAuthorityFullDraftAndChangedSource(t *testing.T) {
	_, ro, _ := usageAuthorityFixture(t)
	f := generationFixtureNew(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	plan, raw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	selected := generationRequest(plan, raw)
	guard, err := NewUsageGenerationGuard(ro, func(context.Context, int64, string) (monthlytasks.Request, error) { return selected, nil })
	if err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent = guard
	task := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), owner, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	cached, err := f.g.Run(context.Background(), owner, task.ID)
	if err != nil || !reflect.DeepEqual(cached, artifact) {
		t.Fatal("verified cache changed amounts", err)
	}
	selected.InputSHA256 = strings.Repeat("e", 64)
	blocked, err := f.g.Run(context.Background(), owner, task.ID)
	if !errors.Is(err, ErrGenerationChanged) || !reflect.DeepEqual(blocked, GenerationArtifact{}) {
		t.Fatal("outdated draft returned after source change", err)
	}
	got, err := f.g.tasks.Get(context.Background(), owner, task.ID)
	if err != nil || got.State != monthlytasks.Invalidated {
		t.Fatal("completed stale task not invalidated", err)
	}
}
