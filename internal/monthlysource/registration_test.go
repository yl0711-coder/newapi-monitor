package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRegistrationReadIsBoundedCompleteAndDoesNotUseTraffic(t *testing.T) {
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	db, state, pin := walletTestSource(t, [][]driver.Value{{int64(201), at}, {int64(202), at + 100}})
	got, err := ReadRegistrationTimes(t.Context(), db, pin, []int64{202, 201})
	if err != nil || !reflect.DeepEqual(got, []RegistrationTime{{201, at}, {202, at + 100}}) {
		t.Fatal("registration identity changed", got, err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.options.ReadOnly || state.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !state.committed || len(state.queries) != 2 || len(state.args[1]) != 2 {
		t.Fatal("registration lookup not bounded/read-only")
	}
	q := state.queries[1]
	if !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") || !strings.Contains(q, "ORDER BY id LIMIT 201") || strings.Contains(q, "logs") || strings.Contains(q, "quota") || strings.Contains(q, "username") {
		t.Fatal("registration lookup reads unexpected data")
	}
}

func TestRegistrationReadRejectsMissingInvalidAndPartialMetadata(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	for _, scenario := range []string{"missing", "null", "zero", "negative", "future", "duplicate", "foreign", "wrong_pin", "query", "row", "commit", "cancelled", "duplicate_scope"} {
		t.Run(scenario, func(t *testing.T) {
			data := [][]driver.Value{{int64(201), at}, {int64(202), at}}
			switch scenario {
			case "missing":
				data = data[:1]
			case "null":
				data[1][1] = nil
			case "zero":
				data[1][1] = int64(0)
			case "negative":
				data[1][1] = int64(-1)
			case "future":
				data[1][1] = time.Now().Add(time.Hour).Unix()
			case "duplicate":
				data[1][0] = int64(201)
			case "foreign":
				data[1][0] = int64(999)
			}
			db, state, pin := walletTestSource(t, data)
			ids := []int64{201, 202}
			state.mu.Lock()
			switch scenario {
			case "wrong_pin":
				pin = strings.Repeat("f", 64)
			case "query":
				state.queryErr = errors.New("synthetic query failure")
			case "row":
				state.rowErr = errors.New("synthetic row failure")
			case "commit":
				state.commitErr = errors.New("synthetic commit failure")
			case "duplicate_scope":
				ids = []int64{201, 201}
			}
			state.mu.Unlock()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "cancelled" {
				cancel()
			}
			got, err := ReadRegistrationTimes(ctx, db, pin, ids)
			if err == nil || got != nil {
				t.Fatal("invalid metadata became a usable partial range", got, err)
			}
		})
	}
}

func TestRegistrationLookupCancellationReleasesGlobalSlot(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	db, state, pin := walletTestSource(t, [][]driver.Value{{int64(201), at}})
	entered := make(chan struct{})
	state.mu.Lock()
	state.block = entered
	state.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		rows, err := ReadRegistrationTimes(ctx, db, pin, []int64{201})
		if rows != nil {
			done <- errors.New("cancelled lookup returned rows")
			return
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("lookup never entered source")
	}
	if rows, err := ReadRegistrationTimes(t.Context(), db, pin, []int64{201}); !errors.Is(err, ErrRegistrationBusy) || rows != nil {
		t.Fatal("concurrent lookup was not bounded", rows, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lookup did not cancel")
	}
	state.mu.Lock()
	state.block = nil
	state.mu.Unlock()
	if rows, err := ReadRegistrationTimes(t.Context(), db, pin, []int64{201}); err != nil || len(rows) != 1 {
		t.Fatal("cancelled lookup blocked future requests", rows, err)
	}
}
