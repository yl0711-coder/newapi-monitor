package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestObservationTransportFailureSafeClassification(t *testing.T) {
	const secret = "private-user:private-password@private-host/private-database"
	for _, tc := range []struct {
		name, class string
		cause       error
	}{
		{"timeout", "deadline", context.DeadlineExceeded},
		{"canceled", "canceled", context.Canceled},
		{"driver", "connection_error", errors.New(secret)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := &observationTransportFailure{cause: fmt.Errorf("%s: %w", secret, tc.cause)}
			if !errors.Is(failure, tc.cause) {
				t.Fatal("transport classification lost the original cause")
			}
			got := safeError(errors.Join(failure, errors.New(secret)))
			if !strings.Contains(got, "stage=transport_prepare, class="+tc.class) || !strings.Contains(got, "未开始业务查询") || strings.Contains(got, secret) || strings.Contains(failure.Error(), secret) {
				t.Fatal("transport output is ambiguous or leaks private information")
			}
		})
	}
	if strings.Contains(safeError(context.DeadlineExceeded), "transport_prepare") {
		t.Fatal("unclassified deadlines must not be relabeled as transport failures")
	}
}

func TestObservationTransportBudgetIsSeparateBoundedAndSingleAttempt(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"connected", nil},
		{"timeout", context.DeadlineExceeded},
		{"driver", errors.New("private driver diagnostic")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			before := time.Now()
			err := prepareObservationTransport(context.Background(), func(ctx context.Context) error {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || deadline.Before(before.Add(4*time.Second)) || deadline.After(time.Now().Add(4*time.Second)) {
					t.Fatal("transport did not receive its independent bounded budget")
				}
				return tc.err
			})
			if calls != 1 || !errors.Is(err, tc.err) {
				t.Fatal("transport retried or changed the failure cause")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := prepareObservationTransport(ctx, func(ctx context.Context) error { return ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Fatal("transport lost parent cancellation")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	parentDeadline, _ := ctx.Deadline()
	if err := prepareObservationTransport(ctx, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(parentDeadline) {
			t.Fatal("transport extended its parent deadline")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestObservationTransportAllowsBoundedColdHandshakeWithoutRetry(t *testing.T) {
	const simulatedHandshake = 2100 * time.Millisecond
	started := time.Now()
	calls := 0
	err := prepareObservationTransport(context.Background(), func(ctx context.Context) error {
		calls++
		timer := time.NewTimer(simulatedHandshake)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	})
	if err != nil || calls != 1 || time.Since(started) < simulatedHandshake {
		t.Fatal("a bounded cold handshake was rejected or retried", err, calls)
	}
}
