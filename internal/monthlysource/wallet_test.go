package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func walletPointer(n int64) *int64 { return &n }

func walletFixture() WalletObservation {
	return WalletObservation{Version: 1, Basis: walletObservationBasis, SourceIdentity: strings.Repeat("a", 64), StartedAtMS: 1790783999900, CompletedAtMS: 1790784000100, Users: []WalletUser{
		{UserID: 1, Exists: true, BalanceQuota: walletPointer(-1), UsedQuota: walletPointer(200000000)},
		{UserID: 2, Exists: true, BalanceQuota: walletPointer(0), UsedQuota: walletPointer(0)},
		{UserID: 3},
	}}
}

func TestWalletCapturePreservesExactMoneyAndMissingUsers(t *testing.T) {
	db, state, identity := walletTestSource(t, [][]driver.Value{{int64(1), int64(-1), int64(0)}, {int64(3), int64(math.MaxInt64), int64(math.MaxInt64)}})
	ids := []int64{3, 2, 1}
	observation, err := CaptureWallet(context.Background(), db, identity, ids)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []int64{3, 2, 1}) || len(observation.Users) != 3 {
		t.Fatal("input changed or member lost")
	}
	if *observation.Users[0].BalanceQuota != -1 || observation.Users[1].Exists || observation.Users[1].BalanceQuota != nil || *observation.Users[2].BalanceQuota != math.MaxInt64 {
		t.Fatal("missing/negative/exact amounts changed")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.options.ReadOnly || state.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !state.committed || len(state.queries) != 2 {
		t.Fatal("source read contract changed")
	}
	if state.queries[1] != "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,quota,used_quota FROM users WHERE id IN (?,?,?) ORDER BY id LIMIT 201" {
		t.Fatal("unexpected query scope", state.queries[1])
	}
	for i, arg := range state.args[1] {
		if arg.Value != int64(i+1) {
			t.Fatal("unbound user IDs")
		}
	}
	if db.Stats().InUse != 0 {
		t.Fatal("source connection held after capture")
	}
	raw, err := json.Marshal(observation)
	if err != nil || !strings.Contains(string(raw), `"exists":false,"balance_quota":null,"used_quota":null`) || strings.Contains(string(raw), `"month"`) {
		t.Fatal("missing user became zero or month assertion added", err)
	}
}

func TestWalletSourceIdentityAndInvalidScopeDoNotReadUsers(t *testing.T) {
	for name, change := range map[string]func(*walletDriverState, *string, *[]int64){
		"empty":          func(_ *walletDriverState, _ *string, ids *[]int64) { *ids = nil },
		"duplicate":      func(_ *walletDriverState, _ *string, ids *[]int64) { *ids = []int64{1, 1} },
		"zero":           func(_ *walletDriverState, _ *string, ids *[]int64) { *ids = []int64{0} },
		"too many":       func(_ *walletDriverState, _ *string, ids *[]int64) { *ids = make([]int64, 201) },
		"missing pin":    func(_ *walletDriverState, pin *string, _ *[]int64) { *pin = "" },
		"wrong server":   func(s *walletDriverState, _ *string, _ *[]int64) { s.uuid = "another-server" },
		"wrong database": func(s *walletDriverState, _ *string, _ *[]int64) { s.database = "another-database" },
	} {
		t.Run(name, func(t *testing.T) {
			db, state, pin := walletTestSource(t, nil)
			ids := []int64{1}
			change(state, &pin, &ids)
			got, err := CaptureWallet(context.Background(), db, pin, ids)
			if err == nil || got.Version != 0 || len(got.Users) != 0 {
				t.Fatal("invalid request returned observation")
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.queries) > 1 {
				t.Fatal("data read before identity/scope validated")
			}
		})
	}
}

func TestWalletCaptureFailuresNeverReturnPartialDataOrRetry(t *testing.T) {
	failure := errors.New("synthetic failure")
	for name, change := range map[string]func(*walletDriverState){
		"begin":            func(s *walletDriverState) { s.beginErr = failure },
		"query":            func(s *walletDriverState) { s.queryErr = failure },
		"rows":             func(s *walletDriverState) { s.rowErr = failure },
		"commit":           func(s *walletDriverState) { s.commitErr = failure },
		"null balance":     func(s *walletDriverState) { s.data[0][1] = nil },
		"null counter":     func(s *walletDriverState) { s.data[0][2] = nil },
		"negative counter": func(s *walletDriverState) { s.data[0][2] = int64(-1) },
		"nonnumeric":       func(s *walletDriverState) { s.data[0][1] = "unknown" },
		"foreign user":     func(s *walletDriverState) { s.data[0][0] = int64(3) },
		"duplicate":        func(s *walletDriverState) { s.data = append(s.data, s.data[0]) },
		"unordered": func(s *walletDriverState) {
			s.data = [][]driver.Value{{int64(2), int64(0), int64(1)}, {int64(1), int64(0), int64(1)}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, state, pin := walletTestSource(t, [][]driver.Value{{int64(1), int64(10), int64(20)}})
			change(state)
			got, err := CaptureWallet(context.Background(), db, pin, []int64{1, 2})
			if err == nil || got.Version != 0 || len(got.Users) != 0 {
				t.Fatal("partial observation returned")
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.queries) > 2 || state.committed {
				t.Fatal("retry or failed transaction committed")
			}
			if name != "begin" && name != "commit" && !state.rolledBack {
				t.Fatal("transaction not rolled back")
			}
		})
	}
}

func TestWalletCaptureBoundedConcurrencyCancellationAndRecovery(t *testing.T) {
	db, state, pin := walletTestSource(t, nil)
	state.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := CaptureWallet(ctx, db, pin, []int64{1}); done <- err }()
	select {
	case <-state.block:
	case <-time.After(time.Second):
		t.Fatal("capture did not begin")
	}
	other, _, otherPin := walletTestSource(t, nil)
	if _, err := CaptureWallet(context.Background(), other, otherPin, []int64{1}); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("parallel capture not bounded", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled source not released")
	}
	if _, err := CaptureWallet(context.Background(), other, otherPin, []int64{1}); err != nil {
		t.Fatal("capture slot not recovered", err)
	}
	if _, err := CaptureWallet(ctx, other, otherPin, []int64{1}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled capture started")
	}
}

func TestWalletObservationDoesNotInventMonthBoundary(t *testing.T) {
	o := walletFixture()
	if err := ValidateWalletObservation(o); err != nil {
		t.Fatal("cross-month observation is a valid observation, not a month cutoff", err)
	}
	for name, change := range map[string]func(*WalletObservation){
		"boundary claim":          func(o *WalletObservation) { o.Basis = "exact_wallet_month_boundaries" },
		"missing basis":           func(o *WalletObservation) { o.Basis = "" },
		"missing time":            func(o *WalletObservation) { o.StartedAtMS = 0 },
		"backward clock":          func(o *WalletObservation) { o.CompletedAtMS = o.StartedAtMS - 1 },
		"too slow":                func(o *WalletObservation) { o.CompletedAtMS = o.StartedAtMS + 3001 },
		"unknown balance":         func(o *WalletObservation) { o.Users[0].BalanceQuota = nil },
		"missing fabricated zero": func(o *WalletObservation) { o.Users[2].BalanceQuota = walletPointer(0) },
		"duplicate user":          func(o *WalletObservation) { o.Users[1].UserID = 1 },
		"missing scope":           func(o *WalletObservation) { o.Users = nil },
	} {
		t.Run(name, func(t *testing.T) {
			o := walletFixture()
			change(&o)
			if ValidateWalletObservation(o) == nil {
				t.Fatal("invalid observation accepted")
			}
		})
	}
	db, _, pin := walletTestSource(t, nil)
	times := []time.Time{time.UnixMilli(2000), time.UnixMilli(1999)}
	i := 0
	if _, err := captureWallet(context.Background(), db, pin, []int64{1}, func() time.Time { v := times[i]; i++; return v }); err == nil {
		t.Fatal("clock rollback accepted")
	}
}

func TestWalletCaptureDeadlineDiscardsPartialObservation(t *testing.T) {
	db, state, pin := walletTestSource(t, nil)
	state.block = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	observation, err := CaptureWallet(ctx, db, pin, []int64{1})
	if !errors.Is(err, context.DeadlineExceeded) || observation.Version != 0 || len(observation.Users) != 0 {
		t.Fatal("timeout became a usable observation", err)
	}
	other, _, identity := walletTestSource(t, nil)
	if _, err := CaptureWallet(context.Background(), other, identity, []int64{1}); err != nil {
		t.Fatal("timeout prevented later recovery", err)
	}
}
