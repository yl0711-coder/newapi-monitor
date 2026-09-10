package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func ownershipFixture(t *testing.T) *Monitor {
	t.Helper()
	m := newECSLogTestMonitor(t)
	m.cfg.ECSLogOwnershipEnabled = true
	if err := m.initECSLogOwnership(m.storeDB); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestECSLogOwnershipAtomicClaimAndRetry(t *testing.T) {
	m := ownershipFixture(t)
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		source, key := registerECSTestSource(t, m, strings.Repeat("a", 32), lane)
		var before ECSLogOwnership
		if err := m.storeDB.First(&before, "lane = ?", lane).Error; err != nil {
			t.Fatal(err)
		}
		if before.Purpose != ecsLogCandidatePurpose || before.AssignedAt <= 0 {
			t.Fatal("wrong responsibility", before)
		}
		for i := 0; i < 3; i++ {
			if _, err := m.registerECSLogLease(context.Background(), source, source.PublicKey, time.Now().Unix()+int64(i)); err != nil {
				t.Fatal(err)
			}
		}
		var after ECSLogOwnership
		if err := m.storeDB.First(&after, "lane = ?", lane).Error; err != nil || after != before {
			t.Fatal("renewal rewrote responsibility", err)
		}
		body := ecsLaneTestBody(t, source.Node, lane, 1)
		for i := 0; i < 2; i++ {
			if w := postSignedECS(m, source, key, body, nil); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
		}
	}
	assertIsolationCounts(t, m, 1)
	var count int64
	if err := m.storeDB.Model(&ECSLogOwnership{}).Count(&count).Error; err != nil || count != 4 {
		t.Fatal("duplicate claims", count, err)
	}
}

func TestECSLogOwnershipRejectsTakeoverAndRollsBack(t *testing.T) {
	m := ownershipFixture(t)
	source, _ := registerECSTestSource(t, m, strings.Repeat("b", 32), "reject")
	for _, change := range []func(*ECSLogSource){
		func(s *ECSLogSource) { s.RuntimeID = "replacement-runtime" },
		func(s *ECSLogSource) { s.ServiceARN += "-changed" },
		func(s *ECSLogSource) { s.TaskRoleARN += "-changed" },
		func(s *ECSLogSource) { s.TaskDefinitionARN += "-changed" },
	} {
		bad := source
		change(&bad)
		if _, err := m.registerECSLogLease(context.Background(), bad, source.PublicKey, time.Now().Unix()); err == nil {
			t.Fatal("takeover accepted")
		}
	}
	var count int64
	if err := m.storeDB.Model(&ECSLogSource{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("conflict wrote source", count, err)
	}
	if err := m.storeDB.Model(&ECSLogOwnership{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("conflict wrote owner", count, err)
	}
	// Force failure AFTER claim insertion; neither the claim nor source/lease
	// may survive an aborted source transaction.
	if err := m.storeDB.Exec(`CREATE TRIGGER fixture_owner_rollback BEFORE INSERT ON ecs_log_sources BEGIN SELECT RAISE(ABORT, 'fixture source failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	fresh := source
	fresh.TaskARN = strings.Replace(source.TaskARN, strings.Repeat("b", 32), strings.Repeat("c", 32), 1)
	if _, err := m.registerECSLogLease(context.Background(), fresh, source.PublicKey, time.Now().Unix()); err == nil {
		t.Fatal("injected failure ignored")
	}
	if err := m.storeDB.Model(&ECSLogOwnership{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("orphan claim survived rollback", count, err)
	}
}

func TestECSLogOwnershipMissingClaimBlocksLiveRenewalAndHeartbeat(t *testing.T) {
	m := ownershipFixture(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("c", 32), "reject")
	if err := m.storeDB.Where("node = ?", source.Node).Delete(&ECSLogOwnership{}).Error; err != nil {
		t.Fatal(err)
	}
	// Disabling a mutable test setting cannot bypass the activated store.
	m.cfg.ECSLogOwnershipEnabled = false
	if w := postSignedECS(m, source, key, ecsLaneTestBody(t, source.Node, "reject", 1), nil); w.Code != 503 {
		t.Fatal("missing owner accepted", w.Code)
	}
	heartbeat := []byte(`{"node":"` + source.Node + `"}`)
	if w := postSignedECSPath(m, source, key, "/internal/ecs/v1/heartbeat/reject", heartbeat, nil); w.Code != 503 {
		t.Fatal("missing owner heartbeat accepted", w.Code)
	}
	if _, err := m.registerECSLogLease(context.Background(), source, source.PublicKey, time.Now().Unix()); !errors.Is(err, errECSLogResponsibilityMissing) {
		t.Fatal("missing owner silently reconstructed", err)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 0 {
		t.Fatal("unowned facts stored")
	}
	e, err := ecsarchive.Seal(m.cfg.ECSLogAudience, source.Node, "reject", ecsLaneTestBody(t, source.Node, "reject", 1), key)
	if err != nil {
		t.Fatal(err)
	}
	objectKey, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("c", 32), e)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	o := ecsarchive.Object{Key: objectKey, Body: raw, ETag: "fixture", Modified: time.Now()}
	if got := newECSArchiveReplayer(m, "isolated/").replay(context.Background(), o, time.Now()); got.Status != 503 {
		t.Fatalf("replay bypassed missing responsibility: %+v", got)
	}
}

func TestECSLogOwnershipConcurrentSources(t *testing.T) {
	m := ownershipFixture(t)
	pub, _ := ecsTestKey(t)
	in := testECSRegistration(strings.Repeat("d", 32), pub)
	base := ECSLogSource{ServiceARN: in.ServiceARN, TaskARN: in.TaskARN, TaskDefinitionARN: "arn:aws:ecs:us-west-2:123456789012:task-definition/fixture:1", TaskRoleARN: testECSLogPolicy().TaskRoleARN, Container: in.Container, RuntimeID: in.RuntimeID, Lane: "reject"}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.registerECSLogLease(context.Background(), base, pub, time.Now().Unix())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := m.storeDB.Model(&ECSLogOwnership{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("concurrent owner duplication", count, err)
	}
	// Five task lifetimes own independent lanes; scaling down must not free
	// those responsibility records for later reuse.
	for i := 0; i < 4; i++ {
		s := base
		s.TaskARN = strings.Replace(base.TaskARN, strings.Repeat("d", 32), fmt.Sprintf("%032x", i+1), 1)
		if _, err := m.registerECSLogLease(context.Background(), s, pub, time.Now().Unix()); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&ECSLogSource{}).Where("task_arn <> ?", base.TaskARN).Update("stopped_at", time.Now().Unix()).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ECSLogOwnership{}).Count(&count).Error; err != nil || count != 5 {
		t.Fatal("scale-in erased ownership", count, err)
	}
}

func TestECSLogOwnershipStartupBindingAndClosedProductionGate(t *testing.T) {
	m := ownershipFixture(t)
	for _, mutate := range []func(*Settings){
		func(s *Settings) { s.ECSLogOwnershipEnabled = false },
		func(s *Settings) { s.ECSLogAudience = "different-audience" },
		func(s *Settings) { s.ECSLogScope = "production" },
		func(s *Settings) { s.ECSLogEnabled = false },
	} {
		cfg := m.cfg
		mutate(&cfg)
		other := &Monitor{cfg: cfg}
		if err := other.initECSLogOwnership(m.storeDB); err == nil {
			t.Fatal("store identity downgraded")
		}
	}
	if _, err := parseECSLogPolicies(Settings{ECSLogOwnershipEnabled: true}); err == nil {
		t.Fatal("ownership enabled without isolated ECS gate")
	}
	if err := m.storeDB.Model(&ECSLogOwnershipBinding{}).Where("id = 1").Update("purpose", "primary").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.initECSLogOwnership(m.storeDB); err == nil {
		t.Fatal("candidate promoted to primary")
	}
}

func TestECSLogOwnershipCannotAdoptExistingReceiver(t *testing.T) {
	m := newECSLogTestMonitor(t)
	registerECSTestSource(t, m, strings.Repeat("e", 32), "reject")
	m.cfg.ECSLogOwnershipEnabled = true
	if err := m.initECSLogOwnership(m.storeDB); err == nil {
		t.Fatal("adopted old registered sources")
	}
	if m.storeDB.Migrator().HasTable(&ECSLogOwnershipBinding{}) {
		t.Fatal("failed activation created binding")
	}
	plain := newTestMonitor(t)
	t.Cleanup(plain.Close)
	if plain.storeDB.Migrator().HasTable(&ECSLogOwnership{}) {
		t.Fatal("default-off created responsibility schema")
	}
}

func TestECSLogOwnershipAcceptanceConstructor(t *testing.T) {
	c := acceptanceFixtureConfig()
	c.OwnershipEnabled = true
	dir := t.TempDir()
	m, _, err := NewECSAcceptance(c, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !m.ecsLogOwnershipActive.Load() {
		t.Fatal("opt-in was not activated")
	}
	m.Close()
	reopened, _, err := NewECSAcceptance(c, dir)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	c.OwnershipEnabled = false
	if bad, _, err := NewECSAcceptance(c, dir); err == nil {
		bad.Close()
		t.Fatal("reopen disabled persisted responsibility")
	}
	// Preserve the old command's config fingerprint when this feature is off.
	b, err := json.Marshal(c)
	if err != nil || strings.Contains(string(b), "ownership_enabled") {
		t.Fatal("default config changed", err)
	}
}

func TestECSLogOwnershipRegisteredSourceSurvivesReopen(t *testing.T) {
	m := ownershipFixture(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("f", 32), "reject")
	body := ecsLaneTestBody(t, source.Node, "reject", 1)
	if w := postSignedECS(m, source, key, body, nil); w.Code != 200 {
		t.Fatal(w.Code)
	}
	var before ECSLogOwnership
	if err := m.storeDB.First(&before).Error; err != nil {
		t.Fatal(err)
	}
	path := m.cfg.StorePath
	m.Close()
	reopened := &Monitor{cfg: m.cfg, ecsLogPolicies: m.ecsLogPolicies}
	if err := reopened.openStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	var after ECSLogOwnership
	if err := reopened.storeDB.First(&after).Error; err != nil || after != before {
		t.Fatal("restart changed owner", err)
	}
	if w := postSignedECS(reopened, source, key, body, nil); w.Code != 200 || !strings.Contains(w.Body.String(), `"duplicate":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if rows := reopened.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 1 {
		t.Fatal("restart duplicate", rows)
	}
}
