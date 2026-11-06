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

func TestRegisteredWalletSingleOriginalViewExactAmountsAndNoBoundaryClaim(t *testing.T) {
	at := time.Now().Add(-24 * time.Hour).Unix()
	db, state, pin := walletTestSource(t, [][]driver.Value{{int64(1), at, int64(-1), int64(0)}, {int64(2), at + 1, "9007199254740993", []byte("9223372036854775807")}})
	ids := []int64{2, 1}
	got, err := CaptureRegisteredWallet(t.Context(), db, pin, ids)
	if err != nil || len(got.Users) != 2 || !reflect.DeepEqual(ids, []int64{2, 1}) {
		t.Fatal("original scope changed", err)
	}
	if got.Users[0].RegisteredAt != at || *got.Users[0].BalanceQuota != -1 || *got.Users[0].UsedQuota != 0 || got.Users[1].RegisteredAt != at+1 || *got.Users[1].BalanceQuota != 9007199254740993 || *got.Users[1].UsedQuota != math.MaxInt64 {
		t.Fatal("registration, negative balance or exact integer changed")
	}
	state.mu.Lock()
	if !state.options.ReadOnly || state.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !state.committed || len(state.queries) != 2 || len(state.args[1]) != 2 {
		t.Fatal("same read-only transaction was not used")
	}
	if state.queries[1] != "SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,quota,used_quota FROM users WHERE id IN (?,?) ORDER BY id LIMIT 201" || state.args[1][0].Value != int64(1) || state.args[1][1].Value != int64(2) {
		t.Fatal("unbounded or unparameterized original read")
	}
	state.mu.Unlock()
	if db.Stats().InUse != 0 || ValidateRegisteredWalletObservation(got) != nil {
		t.Fatal("source connection not released or observation invalid")
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, claim := range []string{`"month"`, `"opening"`, `"closing"`, `"complete"`, `"reviewed"`, `"username"`, `"email"`} {
		if strings.Contains(string(raw), claim) {
			t.Fatal("source collector invented a financial boundary or read private labels", claim)
		}
	}
}

func TestRegisteredWalletMissingOrMalformedRowsNeverReturnPartialSource(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	for _, name := range []string{"missing", "duplicate", "foreign", "unordered", "null_registration", "zero_registration", "future_registration", "null_balance", "null_used", "negative_used", "float_integer", "float_fraction", "overflow", "noncanonical", "exponent", "unknown"} {
		t.Run(name, func(t *testing.T) {
			data := [][]driver.Value{{int64(1), at, int64(10), int64(20)}, {int64(2), at, int64(30), int64(40)}}
			switch name {
			case "missing":
				data = data[:1]
			case "duplicate":
				data[1][0] = int64(1)
			case "foreign":
				data[1][0] = int64(99)
			case "unordered":
				data[0], data[1] = data[1], data[0]
			case "null_registration":
				data[1][1] = nil
			case "zero_registration":
				data[1][1] = int64(0)
			case "future_registration":
				data[1][1] = time.Now().Add(time.Hour).Unix()
			case "null_balance":
				data[1][2] = nil
			case "null_used":
				data[1][3] = nil
			case "negative_used":
				data[1][3] = int64(-1)
			case "float_integer":
				data[1][2] = float64(9007199254740992)
			case "float_fraction":
				data[1][2] = float64(1.5)
			case "overflow":
				data[1][2] = "9223372036854775808"
			case "noncanonical":
				data[1][2] = "0001"
			case "exponent":
				data[1][2] = "1e6"
			case "unknown":
				data[1][2] = "unknown"
			}
			db, state, pin := walletTestSource(t, data)
			got, err := CaptureRegisteredWallet(t.Context(), db, pin, []int64{1, 2})
			if err == nil || got.Version != 0 || got.Users != nil || state.committed || !state.rolledBack {
				t.Fatal("partial or guessed account source escaped", err)
			}
		})
	}
}

func TestRegisteredWalletScopeAndIdentityFailBeforeUserRead(t *testing.T) {
	for _, name := range []string{"empty", "duplicate", "zero", "oversized", "invalid_pin", "wrong_server", "wrong_database", "cancelled", "nil_context", "nil_database"} {
		t.Run(name, func(t *testing.T) {
			db, state, pin := walletTestSource(t, nil)
			ids := []int64{1}
			ctx := t.Context()
			switch name {
			case "empty":
				ids = nil
			case "duplicate":
				ids = []int64{1, 1}
			case "zero":
				ids = []int64{0}
			case "oversized":
				ids = make([]int64, 201)
			case "invalid_pin":
				pin = "invalid"
			case "wrong_server":
				state.uuid += "-different"
			case "wrong_database":
				state.database += "-different"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nil_context":
				ctx = nil //nolint:staticcheck // Verify invalid context is rejected without I/O.
			case "nil_database":
				db = nil
			}
			got, err := CaptureRegisteredWallet(ctx, db, pin, ids)
			if err == nil || got.Version != 0 || got.Users != nil || len(state.queries) > 1 {
				t.Fatal("unvalidated source read users or returned source", err)
			}
		})
	}
}

func TestRegisteredWalletTransactionFailureNoRetryOrPartialResult(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	for _, name := range []string{"begin", "query", "rows", "commit"} {
		t.Run(name, func(t *testing.T) {
			db, state, pin := walletTestSource(t, [][]driver.Value{{int64(1), at, int64(10), int64(20)}})
			failure := errors.New("synthetic original source failure")
			switch name {
			case "begin":
				state.beginErr = failure
			case "query":
				state.queryErr = failure
			case "rows":
				state.rowErr = failure
			case "commit":
				state.commitErr = failure
			}
			got, err := CaptureRegisteredWallet(t.Context(), db, pin, []int64{1})
			if !errors.Is(err, failure) || got.Version != 0 || got.Users != nil || state.committed || len(state.queries) > 2 {
				t.Fatal("failed capture retried or returned usable source", err)
			}
			if name != "begin" && name != "commit" && !state.rolledBack {
				t.Fatal("failed read-only transaction did not roll back")
			}
			if db.Stats().InUse != 0 {
				t.Fatal("failed source held a connection")
			}
		})
	}
}

func TestRegisteredWalletSharesCaptureLaneCancellationAndRecovery(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	db, state, pin := walletTestSource(t, [][]driver.Value{{int64(1), at, int64(10), int64(20)}})
	state.block = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := CaptureRegisteredWallet(ctx, db, pin, []int64{1}); done <- err }()
	select {
	case <-state.block:
	case <-time.After(time.Second):
		t.Fatal("original read did not begin")
	}
	other, _, otherPin := walletTestSource(t, [][]driver.Value{{int64(1), at, int64(0), int64(0)}})
	if _, err := CaptureRegisteredWallet(t.Context(), other, otherPin, []int64{1}); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("parallel original read was not bounded", err)
	}
	if _, err := CaptureWallet(t.Context(), other, otherPin, []int64{1}); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("existing wallet collector can bypass the shared lane", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled reader did not drain")
	}
	if got, err := CaptureRegisteredWallet(t.Context(), other, otherPin, []int64{1}); err != nil || len(got.Users) != 1 {
		t.Fatal("capture lane did not recover", err)
	}
}

func TestRegisteredWalletDeadlineAndClockFailureCannotReturnSource(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	for _, name := range []string{"deadline", "backward_clock", "excessive_clock", "nil_clock"} {
		t.Run(name, func(t *testing.T) {
			db, state, pin := walletTestSource(t, [][]driver.Value{{int64(1), at, int64(0), int64(0)}})
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			var got RegisteredWalletObservation
			var err error
			if name == "deadline" {
				state.block = make(chan struct{})
				got, err = CaptureRegisteredWallet(ctx, db, pin, []int64{1})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("source read exceeded caller deadline", err)
				}
			} else {
				var clock func() time.Time
				if name != "nil_clock" {
					start := time.Now()
					i := 0
					clock = func() time.Time {
						i++
						if i == 1 {
							return start
						}
						if name == "backward_clock" {
							return start.Add(-time.Second)
						}
						return start.Add(3001 * time.Millisecond)
					}
				}
				got, err = captureRegisteredWallet(t.Context(), db, pin, []int64{1}, clock)
			}
			if err == nil || got.Version != 0 || got.Users != nil || state.committed {
				t.Fatal("unusable observation became original source", err)
			}
		})
	}
}

func TestRegisteredWalletFullBatchAccountsRemainIndependent(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	data := make([][]driver.Value, WalletBatchLimit)
	ids := make([]int64, WalletBatchLimit)
	for i := range ids {
		id := int64(i + 1)
		ids[i] = int64(WalletBatchLimit - i)
		data[i] = []driver.Value{id, at + id, id*500000 - 9007199254740993, 9007199254740993 + id}
	}
	db, state, pin := walletTestSource(t, data)
	got, err := CaptureRegisteredWallet(t.Context(), db, pin, ids)
	if err != nil || len(got.Users) != WalletBatchLimit || len(state.args[1]) != WalletBatchLimit {
		t.Fatal("full explicit batch failed", err)
	}
	for i, row := range got.Users {
		id := int64(i + 1)
		if row.UserID != id || row.RegisteredAt != at+id || *row.BalanceQuota != id*500000-9007199254740993 || *row.UsedQuota != 9007199254740993+id {
			t.Fatal("another account's data was mixed into row", id)
		}
	}
}

func TestRegisteredWalletValidatorRejectsUnknownAndBoundaryAssertions(t *testing.T) {
	for _, name := range []string{"version", "boundary_claim", "source", "missing_scope", "missing_balance", "missing_used", "duplicate", "unknown_registration", "future_registration", "negative_used"} {
		t.Run(name, func(t *testing.T) {
			now := time.Now().UnixMilli()
			o := RegisteredWalletObservation{Version: 1, Basis: registeredWalletObservationBasis, SourceIdentity: strings.Repeat("a", 64), StartedAtMS: now, CompletedAtMS: now, Users: []RegisteredWalletUser{{UserID: 1, RegisteredAt: now/1000 - 3600, BalanceQuota: walletPointer(0), UsedQuota: walletPointer(0)}}}
			switch name {
			case "version":
				o.Version++
			case "boundary_claim":
				o.Basis = "historical_month_opening_balance"
			case "source":
				o.SourceIdentity = "unknown"
			case "missing_scope":
				o.Users = nil
			case "missing_balance":
				o.Users[0].BalanceQuota = nil
			case "missing_used":
				o.Users[0].UsedQuota = nil
			case "duplicate":
				o.Users = append(o.Users, o.Users[0])
			case "unknown_registration":
				o.Users[0].RegisteredAt = 0
			case "future_registration":
				o.Users[0].RegisteredAt = now/1000 + 1
			case "negative_used":
				o.Users[0].UsedQuota = walletPointer(-1)
			}
			if ValidateRegisteredWalletObservation(o) == nil {
				t.Fatal("unknown source accepted or month-boundary assumption introduced")
			}
		})
	}
}
