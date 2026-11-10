package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func tokenMeteringFixture(t *testing.T) (*Monitor, int64, string) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "facts.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&TokenMeteringFact{}, &TokenMeteringRef{}, &TokenMeteringState{}, &TokenMeteringWindow{}, &TokenMeteringLaneState{}); err != nil {
		t.Fatal(err)
	}
	if err := initTokenMeteringWindowIndexes(db); err != nil {
		t.Fatal(err)
	}
	dbSQL, _ := db.DB()
	dbSQL.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = dbSQL.Close() })
	source, err := sql.Open("sqlite", filepath.Join(root, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	source.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = source.Close() })
	_, err = source.Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, created_at INTEGER, type INTEGER, user_id INTEGER, channel_id INTEGER, model_name TEXT, `group` TEXT, prompt_tokens, completion_tokens, quota, is_stream, use_time, other TEXT, request_id TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := gorm.Open(sqlite.Open(filepath.Join(root, "evidence.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ev.AutoMigrate(&NginxRequestEvidence{}); err != nil {
		t.Fatal(err)
	}
	evSQL, _ := ev.DB()
	evSQL.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = evSQL.Close() })
	now := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC).Unix()
	start := (now-tokenMeteringLagSeconds)/tokenMeteringWindowSeconds*tokenMeteringWindowSeconds - tokenMeteringWindowSeconds
	m := &Monitor{cfg: tokenMeteringConfig(), storeDB: db, prodDB: source, nginxEvidenceDB: ev}
	if err := db.Create(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, SourceMode: "complete", StartAt: start, ScanThrough: start}).Error; err != nil {
		t.Fatal(err)
	}
	return m, now, path
}
func tokenMeteringSourceRow(t *testing.T, m *Monitor, id, created int64, request string) {
	t.Helper()
	_, err := m.prodDB.Exec("INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,2,3,'synthetic-model','actual-group',100,10,200,1,1,?,?)", id, created, `{"claude":true,"usage_semantic":"anthropic","cache_tokens":0,"cache_creation_tokens":0}`, request)
	if err != nil {
		t.Fatal(err)
	}
}

// Isolate historical transport/commit tests from the separately tested recent
// lane. These windows are deferred, never certified complete. New latest-path
// tests use tokenMeteringFixture directly and exercise every reservation.
func tokenMeteringHistoryFixture(t *testing.T) (*Monitor, int64, string) {
	t.Helper()
	m, now, file := tokenMeteringFixture(t)
	tokenMeteringDeferRecentFixture(t, m, now)
	return m, now, file
}

func tokenMeteringDeferRecentFixture(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	state := tokenMeteringStateRead(t, m)
	for start := state.ScanThrough + 300; start <= now; start += 300 {
		w := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start,
			NextAt: now + 86400, LastReason: "fixture_unrelated_recent_cooldown", Lane: tokenMeteringLaneRealtimeTail}
		if err := m.tokenStore().Create(&w).Error; err != nil {
			t.Fatal(err)
		}
	}
}
func tokenMeteringStateRead(t *testing.T, m *Monitor) TokenMeteringState {
	t.Helper()
	var s TokenMeteringState
	if err := m.storeDB.First(&s, "source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Error; err != nil {
		t.Fatal(err)
	}
	return s
}
func tokenMeteringFactRead(t *testing.T, m *Monitor, id int64) TokenMeteringFact {
	t.Helper()
	var f TokenMeteringFact
	if err := m.storeDB.First(&f, "source_epoch = ? AND log_id = ?", m.cfg.TokenFactsSourceEpoch, id).Error; err != nil {
		t.Fatal(err)
	}
	return f
}
func tokenMeteringRun(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err != nil {
		t.Fatal(err)
	}
}
func tokenMeteringReplay(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringRun(t, m, now)
}
func TestTokenMeteringNormalFactsOverlapLateAndRevision(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+20, "one")
	tokenMeteringRun(t, m, now)
	s := tokenMeteringStateRead(t, m)
	if s.ScanThrough != start+300 || s.CompleteThrough != s.ScanThrough {
		t.Fatalf("%+v", s)
	}
	f := tokenMeteringFactRead(t, m, 1)
	if f.Revision != 1 || f.Semantic != "anthropic_native" {
		t.Fatalf("%+v", f)
	}
	tokenMeteringReplay(t, m, now+1)
	f = tokenMeteringFactRead(t, m, 1)
	if f.Revision != 1 {
		t.Fatal("duplicate scan revised unchanged fact")
	}
	tokenMeteringSourceRow(t, m, 2, start+5, "late-earlier-time")
	if _, err := m.prodDB.Exec("UPDATE logs SET prompt_tokens=150 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	tokenMeteringReplay(t, m, now+2)
	if tokenMeteringFactRead(t, m, 1).Revision != 2 || tokenMeteringFactRead(t, m, 2).Revision != 1 {
		t.Fatal("late repair/revision failed")
	}
	var count int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("%d %v", count, err)
	}
}
func TestTokenMeteringPaginationCrashResumeAndFencing(t *testing.T) {
	m, now, path := tokenMeteringFixture(t)
	ctx := context.Background()
	start := tokenMeteringStateRead(t, m).StartAt
	tx, err := m.prodDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 501; i++ {
		_, err := tx.Exec("INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,2,3,'m','g',0,0,0,0,1,'{}',?)", i, start+1, fmt.Sprint("request-", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := m.claimTokenMetering(ctx, now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, lease.SourceEpoch, lease.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal(err)
	}
	rows, done, err := m.readTokenMeteringPage(ctx, w)
	if err != nil || done || len(rows) != tokenMeteringPageRows {
		t.Fatalf("%d %v %v", len(rows), done, err)
	}
	facts := []TokenMeteringFact{}
	refs := []TokenMeteringRef{}
	for _, r := range rows {
		f, rs := parseTokenMetering(m.cfg, r, now)
		facts = append(facts, f)
		refs = append(refs, rs...)
	}
	if err := m.commitTokenMeteringPage(ctx, lease.LeaseToken, w, rows, facts, refs, done, now); err != nil {
		t.Fatal(err)
	}
	if tokenMeteringStateRead(t, m).ScanThrough != start {
		t.Fatal("partial page advanced watermark")
	}
	// Open a new connection/Monitor instance, as after process restart.
	reopened, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	reopenedSQL, _ := reopened.DB()
	defer reopenedSQL.Close()
	replacement := &Monitor{cfg: m.cfg, storeDB: reopened, prodDB: m.prodDB}
	next, ok, err := replacement.claimTokenMetering(ctx, now+31)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := m.commitTokenMeteringPage(ctx, lease.LeaseToken, w, rows, facts, refs, done, now); !errors.Is(err, errTokenMeteringLease) {
		t.Fatalf("stale worker committed: %v", err)
	}
	if err := reopened.Model(&TokenMeteringState{}).Where("source_epoch = ?", next.SourceEpoch).Update("lease_until", 0).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringRun(t, replacement, now+32)
	var count int64
	if err := reopened.Model(&TokenMeteringFact{}).Count(&count).Error; err != nil || count != 501 {
		t.Fatalf("%d %v", count, err)
	}
	if got := tokenMeteringStateRead(t, replacement); got.CompleteThrough != start+300 {
		t.Fatalf("%+v", got)
	}
}
func TestTokenMeteringRollbackAndFailedStatePersistenceDoNotConsume(t *testing.T) {
	for _, failure := range []string{"fact", "failure-record", "watermark"} {
		t.Run(failure, func(t *testing.T) {
			m, now, _ := tokenMeteringFixture(t)
			start := tokenMeteringStateRead(t, m).StartAt
			tokenMeteringSourceRow(t, m, 1, start+10, "one")
			switch failure {
			case "fact":
				if err := m.storeDB.Exec("CREATE TRIGGER fail_fact BEFORE INSERT ON token_metering_facts BEGIN SELECT RAISE(FAIL,'injected'); END").Error; err != nil {
					t.Fatal(err)
				}
			case "failure-record":
				if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
					t.Fatal(err)
				}
				if err := m.storeDB.Exec("CREATE TRIGGER fail_record BEFORE UPDATE ON token_metering_windows BEGIN SELECT RAISE(FAIL,'injected'); END").Error; err != nil {
					t.Fatal(err)
				}
			case "watermark":
				if err := m.storeDB.Exec("CREATE TRIGGER fail_complete BEFORE UPDATE ON token_metering_states WHEN NEW.complete_through > OLD.start_at BEGIN SELECT RAISE(FAIL,'injected'); END").Error; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err == nil {
				t.Fatal("failure swallowed")
			}
			s := tokenMeteringStateRead(t, m)
			if s.ScanThrough != start || s.CompleteThrough > start {
				t.Fatalf("consumed failed window %+v", s)
			}
			var count int64
			if err := m.storeDB.Model(&TokenMeteringFact{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("partial transaction %d %v", count, err)
			}
		})
	}
}
func TestTokenMeteringSourceProofDeclarationAndFailureRecovery(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	m.cfg.TokenFactsSourceMode = "unverified"
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	if s := tokenMeteringStateRead(t, m); s.CompleteThrough != 0 || s.ScanThrough != start+300 {
		t.Fatalf("%+v", s)
	}
	m.cfg.TokenFactsSourceMode = "complete"
	tokenMeteringReplay(t, m, now+1)
	if s := tokenMeteringStateRead(t, m); s.CompleteThrough != s.ScanThrough {
		t.Fatalf("%+v", s)
	}
	if _, err := m.prodDB.Exec("ALTER TABLE logs RENAME TO unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); err == nil {
		t.Fatal("source failure hidden")
	}
	var w TokenMeteringWindow
	if err := m.storeDB.First(&w).Error; err != nil {
		t.Fatal(err)
	}
	if w.Attempts != 1 || w.NextAt <= now+2 || !w.Replay {
		t.Fatalf("%+v", w)
	}
	if _, err := m.prodDB.Exec("ALTER TABLE unavailable RENAME TO logs"); err != nil {
		t.Fatal(err)
	}
	tokenMeteringRun(t, m, w.NextAt+1)
	if err := m.storeDB.First(&w).Error; err != nil {
		t.Fatal(err)
	}
	if w.Replay || w.LastReason != "" {
		t.Fatalf("%+v", w)
	}
}
func TestTokenMeteringGateReleasedBeforeLocalProcessing(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	seen := false
	err := m.storeDB.Callback().Create().Before("gorm:create").Register("test-token-gate", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		seen = true
		m.backgroundSourceScheduleMu.Lock()
		active := m.backgroundSourceActive
		m.backgroundSourceScheduleMu.Unlock()
		if active {
			_ = tx.AddError(errors.New("source slot held during local persistence"))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	tokenMeteringRun(t, m, now)
	if !seen {
		t.Fatal("callback not reached")
	}
}
func TestTokenMeteringConcurrentRoundClaims(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
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
	if f := tokenMeteringFactRead(t, m, 1); f.Revision != 1 {
		t.Fatalf("%+v", f)
	}
}
func TestTokenMeteringCapacityDisabledCleanupAndHealth(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	m.cfg.TokenFactsMaxRows = 1
	tokenMeteringRun(t, m, now)
	tokenMeteringSourceRow(t, m, 2, start+2, "two")
	if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+1, 0)); err != nil && !errors.Is(err, errTokenMeteringCapacity) {
		t.Fatal(err)
	}
	m.cfg.TokenFactsEnabled = false
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	h, err := m.tokenMeteringHealth(context.Background(), "", now+2)
	if err != nil {
		t.Fatal(err)
	}
	if h.Enabled || h.Facts != 1 || h.FieldStates["prompt_tokens"]["valid"] != 1 || h.ScanLagSeconds != 0 || h.TargetThrough != start+300 {
		t.Fatalf("%+v", h)
	}
	if err := m.cleanupTokenMetering(context.Background(), now+8*86400); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("%d %v", count, err)
	}
	var refs int64
	if err := m.storeDB.Model(&TokenMeteringRef{}).Count(&refs).Error; err != nil || refs != 1 {
		t.Fatalf("orphan refs %d %v", refs, err)
	}
}
func TestTokenMeteringHealthErrorsAndAuthorization(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/health", noStoreSensitive, m.requireRole(roleAdmin), m.serveTokenMeteringHealth)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	if response.Code == http.StatusOK {
		t.Fatal("unauthenticated health exposed")
	}
	direct := gin.New()
	direct.GET("/health", m.serveTokenMeteringHealth)
	response = httptest.NewRecorder()
	direct.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health?source_epoch=bad%27", nil))
	if response.Code != 400 {
		t.Fatal(response.Code)
	}
	if err := m.storeDB.Migrator().DropTable(&TokenMeteringFact{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.tokenMeteringHealth(context.Background(), "", now); err == nil {
		t.Fatal("query error treated as no data")
	}
	response = httptest.NewRecorder()
	direct.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	var health tokenMeteringHealth
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &health) != nil || health.Available || health.HealthCountsReady || health.HealthCountsState != "unavailable" || !strings.Contains(strings.Join(health.GapReasons, ","), "local_health_unavailable") || strings.Contains(response.Body.String(), "SELECT") {
		t.Fatal(response.Body.String())
	}
}

func TestTokenMeteringSourceTypeByteBudgetAndOversizedOther(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	bulk, err := m.prodDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 41; i++ {
		n := 60000
		if i == 41 {
			n = tokenMeteringOtherBytes + 1
		}
		other := `{"ignored":"` + strings.Repeat("x", n) + `"}`
		if _, err := bulk.Exec("INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,1,1,'m','g',0,0,0,0,0,?,'')", i, start+1, other); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := bulk.Exec("INSERT INTO logs VALUES(999,?,5,1,1,'m','g',0,0,0,0,0,'{}','')", start+1); err != nil {
		t.Fatal(err)
	}
	if err := bulk.Commit(); err != nil {
		t.Fatal(err)
	}
	w := TokenMeteringWindow{StartAt: start, EndAt: start + 300, CursorAt: start}
	rows, done, err := m.readTokenMeteringPage(context.Background(), w)
	if err != nil || done || len(rows) >= 41 || len(rows) == 0 {
		t.Fatalf("%d %v %v", len(rows), done, err)
	}
	tokenMeteringRun(t, m, now)
	f := tokenMeteringFactRead(t, m, 41)
	if p := tokenMeteringDecode(t, f); p.OtherState != "size_limit" {
		t.Fatalf("%+v", p)
	}
	var count int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id=999").Count(&count).Error; err != nil || count != 0 {
		t.Fatal("non-consumption collected")
	}
}
func TestTokenMeteringRetryExhaustionIsDurableGap(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	if _, err := m.prodDB.Exec("ALTER TABLE logs RENAME TO temporarily_unavailable_logs"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("next_at", 0).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+int64(i), 0)); err == nil {
			t.Fatal("source failure hidden")
		}
	}
	var w TokenMeteringWindow
	if err := m.storeDB.First(&w).Error; err != nil {
		t.Fatal(err)
	}
	s := tokenMeteringStateRead(t, m)
	if w.Done || w.SourceComplete || w.LastReason != "retry_exhausted" || w.NextAt <= now+11 || s.ScanThrough != start || s.CompleteThrough != start {
		t.Fatalf("w=%+v s=%+v", w, s)
	}
	if _, err := m.prodDB.Exec("ALTER TABLE temporarily_unavailable_logs RENAME TO logs"); err != nil {
		t.Fatal(err)
	}
	tokenMeteringSourceRow(t, m, 1, start+10, "restored-source")
	tokenMeteringRun(t, m, w.NextAt)
	if s := tokenMeteringStateRead(t, m); s.FactCount != 1 || s.ScanThrough < start+300 || s.CompleteThrough < start+300 {
		t.Fatalf("exhausted window failed to recover automatically: %+v", s)
	}
}
func TestTokenMeteringEpochIsolationAndSnapshotNoWrites(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	m.cfg.TokenFactsSourceEpoch = "source-b"
	if err := m.storeDB.Create(&TokenMeteringState{SourceEpoch: "source-b", StartAt: start, ScanThrough: start, SourceMode: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringRun(t, m, now+1)
	var count int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("%d %v", count, err)
	}
	m.cfg.LocalSnapshotOnly = true
	if n, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); err != nil || n != 0 {
		t.Fatal("snapshot collection")
	}
	if err := m.cleanupTokenMetering(context.Background(), now+10*86400); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("snapshot cleanup wrote data")
	}
}
func TestTokenMeteringDiskFailureDoesNotQuerySource(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	m.cfg.StorePath = filepath.Join(t.TempDir(), "missing", "facts.db")
	before := m.backgroundSourceStarts.Load()
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); !errors.Is(err, errTokenMeteringCapacity) {
		t.Fatal(err)
	}
	if m.backgroundSourceStarts.Load() != before {
		t.Fatal("queried source while disk unavailable")
	}
	if s := tokenMeteringStateRead(t, m); s.LastReason != "local_disk_limit" {
		t.Fatalf("%+v", s)
	}
}

func TestTokenMeteringHMACVersionPinSurvivesRestartAndAllowsRotation(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	initial := m.backgroundSourceStarts.Load()
	replacement := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	replacement.cfg.NginxEvidenceHMACKey = strings.Repeat("x", 32)
	if _, err := replacement.runTokenMeteringRound(context.Background(), time.Unix(now+1, 0)); err == nil {
		t.Fatal("reused key version accepted after restart")
	}
	if replacement.backgroundSourceStarts.Load() != 0 || m.backgroundSourceStarts.Load() != initial {
		t.Fatal("read source with conflicting version")
	}
	h, err := replacement.tokenMeteringHealth(context.Background(), "", now+1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(h.GapReasons, ","), "hmac_key_version_conflict") {
		t.Fatal(h.GapReasons)
	}
	replacement.cfg.NginxEvidenceHMACKeyID = "key-2"
	replacement.cfg.NginxEvidencePreviousHMACKeyID = m.cfg.NginxEvidenceHMACKeyID
	replacement.cfg.NginxEvidencePreviousHMACKey = m.cfg.NginxEvidenceHMACKey
	tokenMeteringReplay(t, replacement, now+2)
	var refs []TokenMeteringRef
	if err := m.storeDB.Find(&refs).Error; err != nil || len(refs) != 2 {
		t.Fatalf("%+v %v", refs, err)
	}
}
