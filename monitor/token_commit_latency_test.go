package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var tokenCommitLatencyDriverID atomic.Int64

type tokenCommitLatencyDriver struct {
	driver.Driver
	delay time.Duration
}

func (d *tokenCommitLatencyDriver) Open(name string) (driver.Conn, error) {
	c, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &tokenCommitLatencyConn{Conn: c, delay: d.delay}, nil
}

type tokenCommitLatencyConn struct {
	driver.Conn
	delay time.Duration
}

func (c *tokenCommitLatencyConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *tokenCommitLatencyConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *tokenCommitLatencyConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *tokenCommitLatencyConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	if begin, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = begin.BeginTx(ctx, opts)
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &tokenCommitLatencyTx{Tx: tx, ctx: ctx, delay: c.delay}, nil
}

type tokenCommitLatencyTx struct {
	driver.Tx
	ctx   context.Context
	delay time.Duration
}

func (tx *tokenCommitLatencyTx) Commit() error {
	// Delay the actual driver COMMIT, not a Create callback: the production
	// adaptation measures SQL work and COMMIT separately. Lease transactions
	// have no batch timing context and keep their ordinary behavior.
	if tx.ctx.Value(tokenWriteTimingKey{}) != nil || tx.ctx.Value(tokenWriteOperationKey{}) == "detection_cleanup" {
		if !waitSourceLifecycle(tx.ctx, tx.delay) {
			_ = tx.Tx.Rollback()
			return tx.ctx.Err()
		}
	}
	return tx.Tx.Commit()
}

func TestTokenDetectionCleanupAmortizesCommitWithinOriginalBudget(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	m.cfg.TokenProcessingBatchRows = 250
	epoch := m.cfg.TokenFactsSourceEpoch
	for id := int64(1); id <= 322; id++ {
		expires := now - 1
		if id == 322 {
			expires = now + 60
		}
		if err := m.tokenStore().Create(&TokenDetectionCurrent{SourceEpoch: epoch, LogID: id, ExpiresAt: expires, ResultJSON: "{}"}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.tokenStore().Create(&TokenDetectionRevision{SourceEpoch: epoch, LogID: id, Revision: 1, FrozenJSON: "{}", StoredBytes: 2}).Error; err != nil {
			t.Fatal(err)
		}
	}
	raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
	tokenCleanupAllowExpiredHeads(t, m, now)
	raw.ID, raw.CreatedAt = 321, now-600
	fact, _ := parseTokenMetering(m.cfg, raw, now)
	if err := m.tokenStore().Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokenInjectCommitLatency(t, m, 800*time.Millisecond)
	for round := 0; round < 5; round++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := m.cleanupTokenDetection(ctx, now)
		cancel()
		if err != nil {
			t.Fatal("cleanup lost its committed prefix", err)
		}
	}
	var left int64
	if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("log_id<=320").Count(&left).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("cleanup fixed_commit=800ms rounds=5 budget_per_round=2s removed=%d learned_batch=%d", 320-left, m.tokenDetectionCleanupRows.Load())
	// The existing next-turn guard reserves 1.5 times the measured turn. An
	// 800-ms COMMIT therefore permits one turn per two-second sweep, not two.
	// Five turns grow from 8 to 16 to 32 to 64 to 128; the v159 cap instead
	// freed at most 8+16+32+32+32=120. Require enough progress to distinguish
	// real commit amortization from the earlier one-statement transaction cap.
	if 320-left < 208 || m.tokenDetectionCleanupRows.Load() > 250 {
		t.Fatal("bounded cleanup did not amortize fixed commits", left, m.tokenDetectionCleanupRows.Load())
	}
	for _, model := range []any{&TokenDetectionCurrent{}, &TokenDetectionRevision{}} {
		var retained int64
		if err := m.tokenReadStore().Model(model).Where("log_id IN ?", []int64{321, 322}).Count(&retained).Error; err != nil || retained != 2 {
			t.Fatal("cleanup removed unexpired or retained-fact evidence", retained, err)
		}
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}

func tokenInjectCommitLatency(t *testing.T, m *Monitor, delay time.Duration) {
	t.Helper()
	previous := m.tokenDB
	sqlDB, err := previous.DB()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("token_commit_latency_%d", tokenCommitLatencyDriverID.Add(1))
	sql.Register(name, &tokenCommitLatencyDriver{Driver: sqlDB.Driver(), delay: delay})
	pool, err := sql.Open(name, m.cfg.TokenStorePath+"?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	db, err := gorm.Open(sqlite.Dialector{Conn: pool}, &gorm.Config{Logger: logger.Discard, PrepareStmt: true})
	if err != nil {
		_ = pool.Close()
		t.Fatal(err)
	}
	m.tokenDB = db
	t.Cleanup(func() {
		m.tokenDB = previous
		if prepared, ok := db.ConnPool.(*gorm.PreparedStmtDB); ok {
			prepared.Close()
		}
		_ = pool.Close()
	})
}

func TestTokenDetectionFixedCommitCostDoesNotCollapseBatch(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	m.cfg.TokenProcessingBatchRows = 250
	for id := int64(1); id <= 120; id++ {
		// Detection only consumes facts after association has produced a
		// unique exact identity.  Keep the throughput fixture explicit about
		// that prerequisite so it measures commit latency rather than queue
		// deferral.
		detectionFact(t, m, id, now-600+id, 3, 100, 0, 0)
	}
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokenInjectCommitLatency(t, m, 800*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	n, remaining, err := m.runTokenDetectionRoundProgress(ctx, now)
	var durable int64
	if readErr := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Count(&durable).Error; readErr != nil {
		t.Fatal(readErr)
	}
	t.Logf("fixed_commit=800ms processed=%d durable=%d remaining=%v learned_batch=%d error=%v", n, durable, remaining, m.tokenDetectionCommitRows, err)
	// Real SQLite statement work remains subject to the existing 250-ms cap;
	// do not demand a large batch when those individual rows are expensive.
	// The old fixed-COMMIT rule shrinks to one and commits only 13 rows here.
	// Require useful multi-row throughput and preserve that work/time cap.
	if err != nil || durable != int64(n) || n < 24 || m.tokenDetectionCommitRows < 4 {
		t.Fatalf("fixed COMMIT overhead collapsed useful work: processed=%d durable=%d batch=%d error=%v", n, durable, m.tokenDetectionCommitRows, err)
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}

func TestTokenCheapAssociationSharesCommitBeforeCollectionHandoff(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	for id := int64(1); id <= 24; id++ {
		request := fmt.Sprintf("cheap-association-%d", id)
		tokenMeteringSourceRow(t, m, id, start+id, request)
		tokenMeteringEvidence(t, m, request, start+id, 1000+id, request)
	}
	tokenMeteringRun(t, m, now)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("link_next_at>0").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	m.tokenLinkCommitRows.Store(32)
	collection := make(chan error, 1)
	var once sync.Once
	chunks := map[gorm.ConnPool]int{}
	var first gorm.ConnPool
	if err := m.tokenStore().Callback().Update().Before("gorm:update").Register("queue_during_cheap_association", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		if first == nil {
			first = tx.Statement.ConnPool
		}
		chunks[tx.Statement.ConnPool]++
		once.Do(func() {
			go func() {
				ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), tokenWriteOperationKey{}, "collection"), 550*time.Millisecond)
				defer cancel()
				collection <- m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
					return tx.Create(&TokenMeteringWindow{SourceEpoch: "cheap-collection", StartAt: now, EndAt: now + 300}).Error
				})
			}()
			awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
		})
	}); err != nil {
		t.Fatal(err)
	}
	defer m.tokenStore().Callback().Update().Remove("queue_during_cheap_association")
	n, _, err := m.linkTokenMeteringBatchProgress(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-collection:
		if err != nil {
			t.Fatal("bounded association did not hand off to collection", err)
		}
	case <-time.After(time.Second):
		t.Fatal("collection was not serviced")
	}
	if chunks[first] < 2 || chunks[first] > tokenMeteringLinkMaxCommitRows {
		t.Fatal("cheap queued work paid a commit per row instead of sharing bounded work", chunks[first])
	}
	var durable int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("link_attempts>0").Count(&durable).Error; err != nil || durable != int64(n) {
		t.Fatal("association did not preserve its committed prefix", durable, n, err)
	}
}
