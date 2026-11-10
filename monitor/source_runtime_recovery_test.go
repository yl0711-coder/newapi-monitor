package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestSourceRuntimeTransportErrorsRemainRecoverable(t *testing.T) {
	for _, err := range []error{mysql.ErrInvalidConn, mysql.ErrMalformPkt, mysql.ErrPktSync,
		mysql.ErrPktSyncMul, mysql.ErrBusyBuffer, sql.ErrConnDone} {
		t.Run(err.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("source read: %w", err)
			if classifySourceError(wrapped) != sourceErrorTransient || !isSourceConnectionFailure(wrapped) {
				t.Fatal("transport error would permanently block the source", wrapped)
			}
			m := &Monitor{sourceFailureNotify: make(chan struct{}, 1)}
			m.reportSourceQueryError(wrapped)
			if m.sourceRuntimeFailureClass.Load() != 1 || len(m.sourceFailureNotify) != 1 {
				t.Fatal("transport error was not routed to recoverable epoch restart")
			}
		})
	}
}

func TestReadonlySourceBindingPreservesExplicitAndUnsafeDSN(t *testing.T) {
	for _, tc := range []struct {
		name, options         string
		logchain, interpolate bool
		uncompressed          bool
	}{
		{name: "readonly default", logchain: true, interpolate: true},
		{name: "explicit prepared", options: "&interpolateParams=false", logchain: true},
		{name: "explicit interpolation", options: "&interpolateParams=true", logchain: true, interpolate: true},
		{name: "explicit compression disabled", options: "&compress=false", logchain: true, interpolate: true, uncompressed: true},
		{name: "explicit compression enabled", options: "&compress=true", logchain: true, interpolate: true},
		{name: "unsafe collation", options: "&collation=big5_chinese_ci", logchain: true},
		{name: "full worker unchanged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
			cfg.ProdDSN += tc.options
			cfg.LogChainOnlySource = tc.logchain
			original, err := mysql.ParseDSN(cfg.ProdDSN)
			if err != nil {
				t.Fatal("invalid synthetic DSN")
			}
			cfg.sourceOpen = func(dsn string) (*sql.DB, error) {
				actual, err := mysql.ParseDSN(dsn)
				if err != nil || actual.InterpolateParams != tc.interpolate {
					t.Fatal("unexpected parameter binding choice")
				}
				if compressed := strings.Contains(actual.FormatDSN(), "compress=true"); compressed != (tc.logchain && !tc.uncompressed) {
					t.Fatal("unexpected transport compression choice")
				}
				actual.InterpolateParams = original.InterpolateParams
				_ = actual.Apply(mysql.EnableCompression(strings.Contains(original.FormatDSN(), "compress=true")))
				if actual.FormatDSN() != original.FormatDSN() {
					t.Fatal("binding optimization changed another connection setting")
				}
				return sql.Open("sqlite", ":memory:")
			}
			m := &Monitor{cfg: cfg}
			if err := m.initializeSource(); err != nil {
				t.Fatal(err)
			}
			defer m.prodDB.Close()
			if m.cfg.ProdDSN != cfg.ProdDSN {
				t.Fatal("configured DSN was mutated")
			}
		})
	}
}

func TestSourceRuntimeUnknownFailureRevalidatesWithoutRestart(t *testing.T) {
	for _, failure := range []error{mysql.ErrInvalidConn, errors.New("unclassified runtime transport failure")} {
		t.Run(failure.Error(), func(t *testing.T) {
			var failProbe atomic.Bool
			var starts, stops atomic.Int32
			cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error {
				if failProbe.Swap(false) {
					return failure
				}
				return nil
			})
			cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) {
				starts.Add(1)
				<-ctx.Done()
				stops.Add(1)
			}
			m, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m.Start(ctx)
			lifecycleEventually(t, time.Second, func() bool { return starts.Load() == 1 }, "initial source did not start")
			failProbe.Store(true)
			lifecycleEventually(t, time.Second, func() bool {
				return starts.Load() >= 2 && stops.Load() >= 1 && m.currentSourceState() == sourceStateReady
			}, "source never recovered after a runtime error")
		})
	}
}

func TestSourceRuntimeUnknownQueryDoesNotMasqueradeAsConfiguration(t *testing.T) {
	err := errors.New("unclassified row read failure")
	if classifySourceError(err) != sourceErrorPermanent || classifySourceRuntimeError(err) != sourceErrorTransient {
		t.Fatal("startup validation and runtime recovery must have separate fallbacks")
	}
	m := &Monitor{sourceFailureNotify: make(chan struct{}, 1)}
	m.reportSourceQueryError(err)
	if m.sourceRuntimeFailureClass.Load() != 0 || len(m.sourceFailureNotify) != 0 {
		t.Fatal("an unclassified workload error stopped all source consumers")
	}
	for _, code := range []uint16{1045, 1054, 1064, 1142, 1176} {
		if classifySourceRuntimeError(&mysql.MySQLError{Number: code}) != sourceErrorPermanent {
			t.Fatal("identified permission/schema error became retryable", code)
		}
	}
}
