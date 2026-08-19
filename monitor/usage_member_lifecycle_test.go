package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func createUsageMemberTestGroups(t *testing.T, m *Monitor) (CustomerGroup, CustomerGroup) {
	t.Helper()
	a := CustomerGroup{Name: "usage-member-a"}
	b := CustomerGroup{Name: "usage-member-b"}
	if err := m.storeDB.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&b).Error; err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestUsageMemberLifecyclePreservesFactsAndFencesOldPublication(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	a, b := createUsageMemberTestGroups(t, m)
	ctx := context.Background()

	added, err := m.addUsageMember(ctx, TrackedUser{UserID: 11, Username: "alice", Email: "old@example.test"}, a.ID,
		usageMemberMutationMeta{RequestID: "lifecycle-add"})
	if err != nil {
		t.Fatal(err)
	}
	if added.Action != "add" || added.TrackedRevision != 1 || added.AddedAt <= 0 {
		t.Fatalf("unexpected add result: %+v", added)
	}
	firstAddedAt := added.AddedAt

	day := usageFactTestDay().Unix()
	if err := m.usageFactsStore().Create(&UsageDailyFact{
		DateTs: day, UserID: 11, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1,
		Requests: 1, ConsumeQuota: 123,
	}).Error; err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{11}, day, day+usageFactDaySeconds)
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", 11).
		Update("tracked_revision", 1).Error; err != nil {
		t.Fatal(err)
	}
	if visible, err := m.listTrackedForUsageRead(ctx); err != nil || len(visible) != 1 {
		t.Fatalf("revision-1 publication should be visible: rows=%+v err=%v", visible, err)
	}

	refreshed, err := m.addUsageMember(ctx, TrackedUser{UserID: 11, Username: "alice-new", Email: "new@example.test"}, a.ID,
		usageMemberMutationMeta{RequestID: "lifecycle-refresh"})
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.TrackedRevision != 1 || refreshed.AddedAt != firstAddedAt ||
		refreshed.User.Username != "alice-new" || refreshed.User.Email != "new@example.test" {
		t.Fatalf("same-company add must only refresh profile: %+v", refreshed)
	}
	if _, err := m.addUsageMember(ctx, TrackedUser{UserID: 11, Username: "alice-new"}, b.ID,
		usageMemberMutationMeta{RequestID: "lifecycle-wrong-company"}); !errors.Is(err, errUsageMemberDifferentCompany) {
		t.Fatalf("generic add must reject a different company: %v", err)
	}

	corrected, err := m.correctUsageMemberCompany(ctx, 11, b.ID, usageMemberMutationMeta{RequestID: "lifecycle-correct"})
	if err != nil {
		t.Fatal(err)
	}
	if corrected.TrackedRevision != 1 || corrected.GroupID != b.ID {
		t.Fatalf("company correction must not move facts revision: %+v", corrected)
	}

	removed, err := m.removeUsageMember(ctx, 11, usageMemberMutationMeta{RequestID: "lifecycle-remove"})
	if err != nil {
		t.Fatal(err)
	}
	if removed.Active || removed.TrackedRevision != 2 {
		t.Fatalf("unexpected remove result: %+v", removed)
	}
	if visible, err := m.listTrackedForUsageRead(ctx); err != nil || len(visible) != 0 {
		t.Fatalf("removed member must disappear immediately: rows=%+v err=%v", visible, err)
	}

	rejoined, err := m.addUsageMember(ctx, TrackedUser{UserID: 11, Username: "alice-new", Email: "new@example.test"}, b.ID,
		usageMemberMutationMeta{RequestID: "lifecycle-rejoin"})
	if err != nil {
		t.Fatal(err)
	}
	if rejoined.Action != "rejoin" || rejoined.TrackedRevision != 3 || rejoined.AddedAt != firstAddedAt {
		t.Fatalf("unexpected rejoin result: %+v", rejoined)
	}
	var currentControl UsageMemberControl
	var stalePublished UsageFactPublishedMember
	if err := m.storeDB.First(&currentControl, "user_id = ?", 11).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&stalePublished, "user_id = ?", 11).Error; err != nil {
		t.Fatal(err)
	}
	if currentControl.TrackedRevision != 3 || stalePublished.TrackedRevision != 1 {
		t.Fatalf("rejoin must advance only the authoritative control: control=%+v published=%+v", currentControl, stalePublished)
	}
	if state, usable, err := m.usageFactPublishedSnapshot(ctx); err != nil || !usable {
		t.Fatalf("seeded publication unexpectedly unusable: state=%+v usable=%t err=%v", state, usable, err)
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usageFactPublishedMemberCompatible(stalePublished, snapshot.Controls[11]) {
		t.Fatalf("stale publication unexpectedly compatible: control=%+v published=%+v", snapshot.Controls[11], stalePublished)
	}
	if direct, err := m.currentPublishedUsageMembers(ctx, nil); err != nil || len(direct) != 0 {
		t.Fatalf("direct published/member intersection leaked stale revision: rows=%+v err=%v", direct, err)
	}
	if !m.usageFactsReadEnabled() {
		t.Fatalf("facts read gate unexpectedly closed: enabled=%t requested=%t ready=%t through=%d", m.usageFactsEnabled(), m.usageFactsReadRequested(), m.usageFactsReadReady.Load(), m.usageFactsReadyThrough.Load())
	}
	if visible, err := m.listTrackedForUsageRead(ctx); err != nil || len(visible) != 0 {
		t.Fatalf("old published revision must not resurrect after rejoin: rows=%+v err=%v", visible, err)
	}
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", 11).
		Update("tracked_revision", 3).Error; err != nil {
		t.Fatal(err)
	}
	if visible, err := m.listTrackedForUsageRead(ctx); err != nil || len(visible) != 1 || visible[0].GroupID != b.ID {
		t.Fatalf("current signed revision should follow the user into corrected company: rows=%+v err=%v", visible, err)
	}

	var factCount, auditCount int64
	if err := m.usageFactsStore().Model(&UsageDailyFact{}).Where("user_id = ?", 11).Count(&factCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageMemberAudit{}).Where("user_id = ?", 11).Count(&auditCount).Error; err != nil {
		t.Fatal(err)
	}
	if factCount != 1 || auditCount != 5 {
		t.Fatalf("facts must survive and successful transitions must be audited: facts=%d audits=%d", factCount, auditCount)
	}
}

func TestUsageMemberIdempotencyIsDeterministicUnderConcurrency(t *testing.T) {
	m := newTestMonitor(t)
	a, _ := createUsageMemberTestGroups(t, m)
	ctx := context.Background()
	const workers = 16

	run := func(call func() (usageMemberMutationResult, error), wantRevision int64) {
		t.Helper()
		var wg sync.WaitGroup
		results := make(chan usageMemberMutationResult, workers)
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := call()
				results <- result
				errs <- err
			}()
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent idempotent mutation failed: %v", err)
			}
		}
		firstResponses := 0
		for result := range results {
			if result.TrackedRevision != wantRevision {
				t.Fatalf("revision=%d want=%d result=%+v", result.TrackedRevision, wantRevision, result)
			}
			if !result.Replayed {
				firstResponses++
			}
		}
		if firstResponses != 1 {
			t.Fatalf("exactly one request should commit, got %d", firstResponses)
		}
	}

	resolved := TrackedUser{UserID: 21, Username: "concurrent", Email: "same@example.test"}
	run(func() (usageMemberMutationResult, error) {
		return m.addUsageMember(ctx, resolved, a.ID, usageMemberMutationMeta{RequestID: "concurrent-add"})
	}, 1)
	run(func() (usageMemberMutationResult, error) {
		return m.removeUsageMember(ctx, 21, usageMemberMutationMeta{RequestID: "concurrent-remove"})
	}, 2)
	run(func() (usageMemberMutationResult, error) {
		return m.addUsageMember(ctx, resolved, a.ID, usageMemberMutationMeta{RequestID: "concurrent-rejoin"})
	}, 3)

	var audits int64
	if err := m.storeDB.Model(&UsageMemberAudit{}).Where("user_id = ?", 21).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if audits != 3 {
		t.Fatalf("same request_id must create one audit per transition, got %d", audits)
	}
	if _, err := m.removeUsageMember(ctx, 21, usageMemberMutationMeta{RequestID: "concurrent-add"}); !errors.Is(err, errUsageMemberRequestConflict) {
		t.Fatalf("reusing a key for another operation must fail: %v", err)
	}
}

func lifecycleAdminDo(r http.Handler, method, path, body, key string, cookie *http.Cookie) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	r.ServeHTTP(w, req)
	return w
}

func TestUsageMemberHTTPIdempotencyRemoveAndRejoin(t *testing.T) {
	m, admin, _ := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email) VALUES(31,'http-user','http@example.test')"); err != nil {
		t.Fatal(err)
	}
	g := CustomerGroup{Name: "http-company"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	root := &http.Cookie{Name: sessionCookie, Value: m.signSession("root", roleRoot, time.Now().Unix())}

	for i := 0; i < 2; i++ {
		w := lifecycleAdminDo(admin, http.MethodPost, "/usage/users", `{"input":"31","group_id":`+jsonNumber(g.ID)+`}`, "http-add-key", root)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"tracked_revision":1`) {
			t.Fatalf("add retry failed: %d %s", w.Code, w.Body.String())
		}
	}
	for i := 0; i < 2; i++ {
		w := lifecycleAdminDo(admin, http.MethodPost, "/usage/users/delete", `{"user_id":31}`, "http-remove-key", root)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"tracked_revision":2`) {
			t.Fatalf("remove retry failed: %d %s", w.Code, w.Body.String())
		}
	}
	for i := 0; i < 2; i++ {
		w := lifecycleAdminDo(admin, http.MethodPost, "/usage/users", `{"input":"31","group_id":`+jsonNumber(g.ID)+`}`, "http-rejoin-key", root)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"tracked_revision":3`) {
			t.Fatalf("rejoin retry failed: %d %s", w.Code, w.Body.String())
		}
	}
	var audits int64
	if err := m.storeDB.Model(&UsageMemberAudit{}).Where("user_id = ?", 31).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if audits != 3 {
		t.Fatalf("HTTP retries must not advance revision twice, audits=%d", audits)
	}
}

func jsonNumber(value int64) string {
	return strings.TrimSpace(string(mustJSONNumber(value)))
}

func mustJSONNumber(value int64) []byte {
	payload, _ := json.Marshal(value)
	return payload
}

func TestUsageMemberMutationRollsBackWhenAuditInsertFails(t *testing.T) {
	m := newTestMonitor(t)
	a, _ := createUsageMemberTestGroups(t, m)
	ctx := context.Background()
	installFailure := func() {
		t.Helper()
		if err := m.storeDB.Exec(`CREATE TRIGGER usage_member_audit_fail_test BEFORE INSERT ON usage_member_audits
			BEGIN SELECT RAISE(ABORT, 'audit failure injection'); END`).Error; err != nil {
			t.Fatal(err)
		}
	}
	dropFailure := func() {
		t.Helper()
		if err := m.storeDB.Exec("DROP TRIGGER usage_member_audit_fail_test").Error; err != nil {
			t.Fatal(err)
		}
	}

	installFailure()
	if _, err := m.addUsageMember(ctx, TrackedUser{UserID: 41, Username: "rollback"}, a.ID,
		usageMemberMutationMeta{RequestID: "rollback-add"}); err == nil {
		t.Fatal("audit failure must roll back add")
	}
	dropFailure()
	var tracked, controls int64
	_ = m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", 41).Count(&tracked).Error
	_ = m.storeDB.Model(&UsageMemberControl{}).Where("user_id = ?", 41).Count(&controls).Error
	if tracked != 0 || controls != 0 {
		t.Fatalf("failed add left partial control state: tracked=%d controls=%d", tracked, controls)
	}

	if _, err := m.addUsageMember(ctx, TrackedUser{UserID: 41, Username: "rollback"}, a.ID,
		usageMemberMutationMeta{RequestID: "rollback-add-ok"}); err != nil {
		t.Fatal(err)
	}
	installFailure()
	if _, err := m.removeUsageMember(ctx, 41, usageMemberMutationMeta{RequestID: "rollback-remove"}); err == nil {
		t.Fatal("audit failure must roll back remove")
	}
	dropFailure()
	var control UsageMemberControl
	if err := m.storeDB.First(&control, "user_id = ?", 41).Error; err != nil {
		t.Fatal(err)
	}
	if !control.Active || control.TrackedRevision != 1 {
		t.Fatalf("failed remove advanced durable control: %+v", control)
	}
	if err := m.storeDB.First(&TrackedUser{}, "user_id = ?", 41).Error; err != nil {
		t.Fatalf("failed remove deleted projection: %v", err)
	}
}

func TestUsageMemberControlCorruptionFailsClosed(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	a, b := createUsageMemberTestGroups(t, m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 51, Username: "secure", GroupID: a.ID}).Error; err != nil {
		t.Fatal(err)
	}
	day := usageFactTestDay().Unix()
	seedPublishedUsageFactsForTest(t, m, []int64{51}, day, day+usageFactDaySeconds)
	if rows, err := m.listTrackedForUsageRead(context.Background()); err != nil || len(rows) != 1 {
		t.Fatalf("legacy rev0 should serve only active rev1: rows=%+v err=%v", rows, err)
	}
	if err := m.storeDB.Delete(&UsageMemberControl{}, "user_id = ?", 51).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.listTrackedForUsageRead(context.Background()); !errors.Is(err, errUsageFactsNotReady) {
		t.Fatalf("missing initialized control must fail closed: %v", err)
	}
	wrong := UsageMemberControl{UserID: 51, Active: true, TrackedRevision: 1, CurrentGroupID: b.ID, FirstAddedAt: 1, LastActivatedAt: 1, UpdatedAt: 1}
	if err := m.storeDB.Create(&wrong).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.listTrackedForUsageRead(context.Background()); !errors.Is(err, errUsageFactsNotReady) {
		t.Fatalf("group mismatch must fail closed: %v", err)
	}
}

func TestUsageMemberMissingInactiveControlCannotResetRevision(t *testing.T) {
	m := newTestMonitor(t)
	group, _ := createUsageMemberTestGroups(t, m)
	ctx := context.Background()
	if _, err := m.addUsageMember(ctx, TrackedUser{UserID: 52, Username: "revision-owner"}, group.ID,
		usageMemberMutationMeta{RequestID: "inactive-corruption-add"}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{UserID: 52, TrackedRevision: 1, PublishedAt: time.Now().Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	if removed, err := m.removeUsageMember(ctx, 52, usageMemberMutationMeta{RequestID: "inactive-corruption-remove"}); err != nil || removed.TrackedRevision != 2 {
		t.Fatalf("删除未正常推进 revision: result=%+v err=%v", removed, err)
	}
	if err := m.storeDB.Delete(&UsageMemberControl{}, "user_id = ?", 52).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.addUsageMember(ctx, TrackedUser{UserID: 52, Username: "revision-owner"}, group.ID,
		usageMemberMutationMeta{RequestID: "inactive-corruption-rejoin"}); !errors.Is(err, errUsageMemberControlIntegrity) {
		t.Fatalf("缺失 inactive control 不得将重加误判为 rev=1: %v", err)
	}
	// 内部导入/测试夹具直接 Create 也必须被 hook 挡住，否则旧 rev=1
	// published 行会被重新认为有效。
	if err := m.storeDB.Create(&TrackedUser{UserID: 52, Username: "bypass", GroupID: group.ID}).Error; !errors.Is(err, errUsageMemberControlIntegrity) {
		t.Fatalf("直接导入不得绕过 revision 链: %v", err)
	}
	var tracked int64
	if err := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", 52).Count(&tracked).Error; err != nil || tracked != 0 {
		t.Fatalf("损坏状态下必须保持隐藏: tracked=%d err=%v", tracked, err)
	}
}

func TestUsageMemberGroupDeleteGuardsAndAuditIsAppendOnly(t *testing.T) {
	m := newTestMonitor(t)
	a, b := createUsageMemberTestGroups(t, m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 61, GroupID: a.ID}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.removeCustomerGroup(context.Background(), a.ID, usageMemberMutationMeta{RequestID: "delete-group-with-member"}); !errors.Is(err, errCustomerGroupHasMembers) {
		t.Fatalf("group with members must be retained: %v", err)
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", b.ID).Updates(map[string]any{
		"portal_email": "enabled@example.test", "portal_pw_admin": "hash",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.removeCustomerGroup(context.Background(), b.ID, usageMemberMutationMeta{RequestID: "delete-group-with-portal"}); !errors.Is(err, errCustomerGroupPortalEnabled) {
		t.Fatalf("Portal-enabled group must be retained: %v", err)
	}

	empty := CustomerGroup{Name: "empty-delete"}
	if err := m.storeDB.Create(&empty).Error; err != nil {
		t.Fatal(err)
	}
	if replayed, err := m.removeCustomerGroup(context.Background(), empty.ID, usageMemberMutationMeta{RequestID: "delete-empty-group"}); err != nil || replayed {
		t.Fatalf("first empty delete failed: replayed=%v err=%v", replayed, err)
	}
	if replayed, err := m.removeCustomerGroup(context.Background(), empty.ID, usageMemberMutationMeta{RequestID: "delete-empty-group"}); err != nil || !replayed {
		t.Fatalf("delete retry should replay: replayed=%v err=%v", replayed, err)
	}
	var audit UsageMemberAudit
	if err := m.storeDB.Where("request_id = ?", "delete-empty-group").First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec("UPDATE usage_member_audits SET reason = 'tampered' WHERE id = ?", audit.ID).Error; err == nil {
		t.Fatal("database trigger must reject audit update")
	}
	if err := m.storeDB.Exec("DELETE FROM usage_member_audits WHERE id = ?", audit.ID).Error; err == nil {
		t.Fatal("database trigger must reject audit delete")
	}
}

func TestUsageMemberRevisionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/monitor.db"
	m1 := &Monitor{cfg: Settings{StorePath: path}, chNames: map[string]string{}}
	if err := m1.openStore(path); err != nil {
		t.Fatal(err)
	}
	g := CustomerGroup{Name: "restart-company"}
	if err := m1.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m1.addUsageMember(context.Background(), TrackedUser{UserID: 71, Username: "restart"}, g.ID,
		usageMemberMutationMeta{RequestID: "restart-add"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m1.removeUsageMember(context.Background(), 71, usageMemberMutationMeta{RequestID: "restart-remove"}); err != nil {
		t.Fatal(err)
	}
	m1.Close()

	m2 := &Monitor{cfg: Settings{StorePath: path}, chNames: map[string]string{}}
	if err := m2.openStore(path); err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	rejoined, err := m2.addUsageMember(context.Background(), TrackedUser{UserID: 71, Username: "restart"}, g.ID,
		usageMemberMutationMeta{RequestID: "restart-rejoin"})
	if err != nil {
		t.Fatal(err)
	}
	if rejoined.TrackedRevision != 3 || rejoined.Action != "rejoin" {
		t.Fatalf("restart lost inactive revision: %+v", rejoined)
	}
	var migration UsageMemberControlMigration
	if err := m2.storeDB.First(&migration, 1).Error; err != nil || migration.Version != usageMemberControlVersion || migration.ManifestHash == "" {
		t.Fatalf("migration manifest missing after restart: %+v err=%v", migration, err)
	}
}

func TestUsageMemberMigrationSeedsLegacyTrackedUsersAtomically(t *testing.T) {
	path := t.TempDir() + "/legacy-monitor.db"
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE tracked_users (user_id INTEGER PRIMARY KEY, username TEXT, email TEXT, group_id INTEGER, note TEXT, added_at INTEGER)`,
		`INSERT INTO tracked_users(user_id,username,email,group_id,note,added_at) VALUES (801,'legacy','legacy@example.test',9,'legacy-note',12345)`,
	} {
		if _, err := legacy.Exec(statement); err != nil {
			_ = legacy.Close()
			t.Fatal(err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	m := &Monitor{cfg: Settings{StorePath: path}, chNames: map[string]string{}}
	if err := m.openStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	var control UsageMemberControl
	if err := m.storeDB.First(&control, "user_id = ?", 801).Error; err != nil {
		t.Fatal(err)
	}
	if !control.Active || control.TrackedRevision != 1 || control.CurrentGroupID != 9 || control.FirstAddedAt != 12345 {
		t.Fatalf("旧名单迁移控制行不正确: %+v", control)
	}
	var migration UsageMemberControlMigration
	if err := m.storeDB.First(&migration, 1).Error; err != nil {
		t.Fatal(err)
	}
	if migration.Version != usageMemberControlVersion || migration.InitializedAt <= 0 || migration.ManifestHash == "" {
		t.Fatalf("迁移 manifest 未原子初始化: %+v", migration)
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(context.Background())
	if err != nil || len(snapshot.Tracked) != 1 || snapshot.Controls[801].TrackedRevision != 1 {
		t.Fatalf("迁移后首次读取应通过严格控制校验: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestUsageFactJobRevisionIsFencedAfterRemoval(t *testing.T) {
	m := newTestMonitor(t)
	group, _ := createUsageMemberTestGroups(t, m)
	if _, err := m.addUsageMember(context.Background(), TrackedUser{UserID: 802, Username: "job-user"}, group.ID,
		usageMemberMutationMeta{RequestID: "job-fence-add"}); err != nil {
		t.Fatal(err)
	}
	userID := int64(802)
	job := UsageFactJob{ID: "job-fence-802-r1", Kind: "history", UserID: &userID, TrackedRevision: 1, Status: "pending"}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactJobRevisionCurrent(context.Background(), job); err != nil {
		t.Fatalf("当前 revision job 应可执行: %v", err)
	}
	if _, err := m.removeUsageMember(context.Background(), userID, usageMemberMutationMeta{RequestID: "job-fence-remove"}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactJobRevisionCurrent(context.Background(), job); !errors.Is(err, errUsageMemberControlIntegrity) {
		t.Fatalf("删除后旧 revision job 必须失效: %v", err)
	}
}

func TestUsageFactAdditiveHistorySchemaDoesNotPromoteLegacyProof(t *testing.T) {
	m := newTestMonitor(t)
	legacy := UsageFactMemberDayState{UserID: 81, DateTs: usageFactTestDay().Unix(), ContentHash: "legacy-content"}
	if err := m.usageFactsStore().Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	var stored UsageFactMemberDayState
	if err := m.usageFactsStore().First(&stored, "user_id = ?", 81).Error; err != nil {
		t.Fatal(err)
	}
	if stored.ContentHash == "" || usageFactMemberDayHistoryReady(stored) {
		t.Fatalf("legacy serving proof must remain usable but not become all-history-ready: %+v", stored)
	}
	ready := stored
	ready.Status = "complete"
	ready.SourceResultHash = "source"
	ready.FactContentHash = "facts"
	ready.ClassificationVersion = userTrafficClassificationVersion
	ready.QuerySemanticsVersion = usageFactQuerySemanticsVersion
	ready.SourceEpoch = "test-source-v1"
	ready.SourceCheckedAt = time.Now().Unix()
	ready.CompletedAt = ready.SourceCheckedAt
	if !usageFactMemberDayHistoryReady(ready) {
		t.Fatalf("fully sourced proof should be history-ready: %+v", ready)
	}

	if err := m.usageFactsStore().Create(&UsageFactJob{ID: "nil-key-1", Kind: "discover"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactJob{ID: "nil-key-2", Kind: "discover"}).Error; err != nil {
		t.Fatalf("multiple NULL idempotency keys must be allowed: %v", err)
	}
	key := "durable-job-key"
	if err := m.usageFactsStore().Create(&UsageFactJob{ID: "key-1", Kind: "discover", IdempotencyKey: &key}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactJob{ID: "key-2", Kind: "discover", IdempotencyKey: &key}).Error; err == nil {
		t.Fatal("duplicate non-NULL job idempotency key must fail")
	}
}

func TestUsageMemberRejoinStillHonorsActiveMemberLimit(t *testing.T) {
	m := newTestMonitor(t)
	a, _ := createUsageMemberTestGroups(t, m)
	members := make([]TrackedUser, 0, maxTrackedUsers)
	for i := 1; i <= maxTrackedUsers; i++ {
		members = append(members, TrackedUser{UserID: int64(1000 + i), GroupID: a.ID, AddedAt: int64(i)})
	}
	if err := m.storeDB.CreateInBatches(members, 50).Error; err != nil {
		t.Fatal(err)
	}
	inactive := UsageMemberControl{
		UserID: 9999, Active: false, TrackedRevision: 2, CurrentGroupID: a.ID,
		FirstAddedAt: 1, LastDeactivatedAt: 2, UpdatedAt: 2,
	}
	if err := m.storeDB.Create(&inactive).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.addUsageMember(context.Background(), TrackedUser{UserID: 9999}, a.ID,
		usageMemberMutationMeta{RequestID: "limit-rejoin"}); err == nil || !strings.Contains(err.Error(), "上限") {
		t.Fatalf("rejoin must not bypass maxTrackedUsers: %v", err)
	}
}
