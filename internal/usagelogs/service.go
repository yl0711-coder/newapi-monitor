package usagelogs

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// Service owns the admission gates and cancellation pool for this feature.
// The caller owns replica. The service must never replace it with a writer.
type Service struct {
	replica  *sql.DB
	dsn      string
	openPool func(string) (*sql.DB, error)

	gateOnce             sync.Once
	gate, backgroundGate chan struct{}
	threadIDFn           func(context.Context, *sql.Conn) int64
	killFn               func(context.Context, int64) error
	killOnce             sync.Once
	killDB               *sql.DB
}

// New receives the same read-only replica and dialer as the existing source.
func New(replica *sql.DB, replicaDSN string, openPool func(string) (*sql.DB, error)) *Service {
	return &Service{replica: replica, dsn: replicaDSN, openPool: openPool}
}

// Request owns one SQL operation. The caller should defer Finish before
// invoking an operation, so asynchronous cancellation does not delay the HTTP
// response and admission stays held until the replica cleanup is attempted.
type Request struct {
	service *Service
	ctx     context.Context
	used    bool
	finish  func()
}

// Request creates a one-operation request that inherits the caller's cancellation.
func (s *Service) Request(ctx context.Context) Request {
	return Request{service: s, ctx: ctx}
}

// Finish releases the request's query resources after the response has been written.
func (r *Request) Finish() {
	if r.finish != nil {
		r.finish()
		r.finish = nil
	}
}

func (r *Request) open(background bool) (usageLogsQuerier, context.Context, error) {
	if r.used {
		return nil, nil, errors.New("usage log request already used")
	}
	r.used = true
	db, ctx, finish, err := r.service.acquire(r.ctx, background)
	r.finish = finish
	return db, ctx, err
}

// usageLogsGate bounds concurrent replica queries from this page only.
func (s *Service) acquireUsageLogsGate(ctx context.Context) (func(), error) {
	s.initUsageLogsGates()
	return acquireUsageLogsSlot(ctx, s.gate)
}

func (s *Service) initUsageLogsGates() {
	s.gateOnce.Do(func() {
		s.gate = make(chan struct{}, usageLogsGateCapacity)
		s.backgroundGate = make(chan struct{}, 1)
	})
}

func acquireUsageLogsSlot(ctx context.Context, gate chan struct{}) (func(), error) {
	waitCtx, cancel := context.WithTimeout(ctx, usageLogsGateWait)
	defer cancel()
	select {
	case gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate
			return nil, err
		}
		return func() { <-gate }, nil
	case <-waitCtx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, ctx.Err()
		}
		return nil, errUsageLogsBusy
	}
}

func (s *Service) acquire(parent context.Context, background bool) (usageLogsQuerier, context.Context, func(), error) {
	if s.replica == nil {
		return nil, nil, nil, errUsageLogsSourceUnavailable
	}
	// One deadline includes admission, connection-pool wait and all SQL.
	ctx, cancel := context.WithTimeout(parent, usageLogsQueryTimeout)
	backgroundRelease := func() {}
	if background {
		s.initUsageLogsGates()
		var err error
		backgroundRelease, err = acquireUsageLogsSlot(ctx, s.backgroundGate)
		if err != nil {
			err = usageLogsQueryErr(ctx, err)
			cancel()
			return nil, nil, nil, err
		}
	}
	release, err := s.acquireUsageLogsGate(ctx)
	if err != nil {
		backgroundRelease()
		err = usageLogsQueryErr(ctx, err)
		cancel()
		return nil, nil, nil, err
	}
	conn, err := s.replica.Conn(ctx)
	if err != nil {
		release()
		backgroundRelease()
		err = usageLogsQueryErr(ctx, err)
		cancel()
		return nil, nil, nil, fmt.Errorf("连接 new-api 只读从库失败: %w", err)
	}
	threadID := s.threadID(ctx, conn)
	done := func() {
		// A cancelled or timed-out request may leave its statement running on
		// the replica: closing the client socket is not noticed by MySQL until
		// the statement ends. Kill it on our own thread id (MySQL thread ids
		// are never reused while the server runs) and only then free the
		// admission slot, so the gate bounds the replica.
		//
		// Discard our (already aborted) connection before the kill so it is not
		// handed back to other features. The replica statement keeps running
		// regardless of the socket, so the thread id stays valid; the kill goes
		// through its own one-connection pool (usageLogsKillPool).
		kill := ctx.Err() != nil && threadID > 0
		if kill {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
		cancel()
		if !kill {
			release()
			backgroundRelease()
			return
		}
		// The response is already written; do not hold it for up to the kill
		// timeout. The admission slots stay taken until the kill was sent, so
		// the gate still bounds what runs on the replica.
		s.killAsync(threadID, func() { release(); backgroundRelease() })
	}
	return conn, ctx, done, nil
}

func (s *Service) killAsync(threadID int64, releaseSlots func()) {
	go func() {
		defer releaseSlots()
		killCtx, killCancel := context.WithTimeout(context.Background(), usageLogsKillTimeout)
		defer killCancel()
		// 1094 (unknown thread): the statement already ended on its own.
		var killErr *mysqlDriver.MySQLError
		if err := s.kill(killCtx, threadID); err != nil && !(errors.As(err, &killErr) && killErr.Number == 1094) {
			slog.Warn("使用日志取消后终止从库查询失败", "err", err)
		}
	}()
}

// usageLogsThreadID returns the replica thread id of conn, or 0 when it is
// unknown (non-MySQL test source, or the lookup failed). 0 disables the kill.
func (s *Service) threadID(ctx context.Context, conn *sql.Conn) int64 {
	if s.threadIDFn != nil {
		return s.threadIDFn(ctx, conn)
	}
	if _, ok := s.replica.Driver().(*mysqlDriver.MySQLDriver); !ok {
		if _, ok := s.replica.Driver().(mysqlDriver.MySQLDriver); !ok {
			return 0
		}
	}
	var id int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		slog.Warn("使用日志读取从库连接编号失败", "err", err)
		return 0
	}
	return id
}

func (s *Service) kill(ctx context.Context, threadID int64) error {
	if s.killFn != nil {
		return s.killFn(ctx, threadID)
	}
	// threadID is an int64 from CONNECTION_ID(); formatting it is not an
	// injection surface, and KILL does not need a prepared statement.
	_, err := s.killPool().ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", threadID))
	return err
}

// usageLogsKillPool is a dedicated single-connection pool for KILL QUERY.
// The shared source pool (4, one held by the lease) hands freed capacity to
// callers already queued, so a kill sent through it could wait behind other
// pages exactly when the replica is busiest. This pool holds no idle
// connection: it opens one only while a kill is being sent.
func (s *Service) killPool() *sql.DB {
	s.killOnce.Do(func() {
		dsn := strings.TrimSpace(s.dsn)
		if dsn == "" {
			return
		}
		open := s.openPool
		if open == nil {
			open = func(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }
		}
		db, err := open(dsn)
		if err != nil {
			slog.Warn("使用日志终止查询专用连接创建失败，改用共享连接池", "err", err)
			return
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(0)
		s.killDB = db
	})
	if s.killDB != nil {
		return s.killDB
	}
	return s.replica
}
