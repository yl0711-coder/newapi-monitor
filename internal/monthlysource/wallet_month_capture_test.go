package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func walletMonthCaptureFixture(t *testing.T) (*sql.DB, *walletDriverState, *sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan) {
	t.Helper()
	source, s, pin := walletTestSource(t, [][]driver.Value{{int64(201), int64(-1), int64(9007199254740993)}, {int64(202), int64(500000), int64(1)}})
	receiver, path, key, scope, plan := fundingMonthCaptureFixture(t, pin)
	if err := InitWalletMonthCaptureStore(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	return source, s, receiver, path, key, scope, plan
}

func TestWalletMonthCaptureReadOnlyWaitAndExactScope(t *testing.T) {
	source, s, receiver, path, key, scope, _ := walletMonthCaptureFixture(t)
	progress, err := CaptureNextWalletMonthView(context.Background(), source, receiver, key, scope)
	if err != nil || progress.Completed != 1 || !progress.Captured || len(progress.IDs) != 1 {
		t.Fatal(progress, err)
	}
	o, err := LoadWalletObservation(context.Background(), receiver, progress.IDs[0])
	if err != nil || len(o.Users) != 2 || *o.Users[0].BalanceQuota != -1 || *o.Users[0].UsedQuota != 9007199254740993 {
		t.Fatal(o, err)
	}
	if !s.options.ReadOnly || s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || source.Stats().InUse != 0 || receiver.Stats().InUse != 0 {
		t.Fatal("wallet capture retained or weakened a transaction")
	}
	queries := len(s.queries)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := CaptureNextWalletMonthView(context.Background(), source, receiver, key, scope)
	if err != nil || paused.Completed != 1 || paused.Captured || paused.NextEligibleMS <= time.Now().UnixMilli() || len(s.queries) != queries {
		t.Fatal("wait recaptured", paused, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("wait changed source", err)
	}
}

func TestWalletMonthCaptureRestartMovingBalancesAndNoRecapture(t *testing.T) {
	source, s, receiver, path, key, scope, plan := walletMonthCaptureFixture(t)
	older, err := captureWallet(context.Background(), source, scope.Funding.SourceIdentity, scope.Funding.UserIDs, func() time.Time { return time.Now().Add(-6 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := saveWalletCaptureSlot(context.Background(), receiver, plan, 0, older); err != nil {
		t.Fatal(err)
	}
	if err := saveWalletCaptureSlot(context.Background(), receiver, plan, 0, older); err != nil {
		t.Fatal("exact replay failed", err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	s.data[0][1] = int64(7)
	s.data[0][2] = int64(9007199254740994)
	progress, err := CaptureNextWalletMonthView(context.Background(), source, receiver, key, scope)
	if err != nil || progress.Completed != 2 || !progress.Captured {
		t.Fatal(progress, err)
	}
	newer, err := LoadWalletObservation(context.Background(), receiver, progress.IDs[1])
	if err != nil || *newer.Users[0].BalanceQuota != 7 || newer.StartedAtMS-older.CompletedAtMS < 300000 {
		t.Fatal("moving live wallet changed historical labels", newer, err)
	}
	queries := len(s.queries)
	progress, err = CaptureNextWalletMonthView(context.Background(), source, receiver, key, scope)
	if err != nil || progress.Completed != 2 || progress.Captured || len(s.queries) != queries {
		t.Fatal("finished pair recaptured", progress, err)
	}
	if err := saveWalletCaptureSlot(context.Background(), receiver, plan, 0, newer); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("original wallet overwritten", err)
	}
}

func TestWalletMonthCaptureFailureNoPartialAndMissingNeverZero(t *testing.T) {
	for _, scenario := range []string{"source", "wrong_source", "original_write", "slot_write", "missing_user", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			source, s, receiver, _, key, scope, _ := walletMonthCaptureFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "source":
				s.queryErr = errors.New("synthetic source outage")
			case "wrong_source":
				s.uuid = "wrong-server"
			case "original_write":
				if _, err := receiver.Exec(`CREATE TRIGGER fail_wallet_original BEFORE INSERT ON portal_monthly_wallet_observations BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "slot_write":
				if _, err := receiver.Exec(`CREATE TRIGGER fail_wallet_slot BEFORE INSERT ON portal_monthly_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "missing_user":
				s.data = s.data[:1]
			case "cancel":
				cancel()
			}
			if _, err := CaptureNextWalletMonthView(ctx, source, receiver, key, scope); err == nil {
				t.Fatal("bad wallet accepted")
			}
			for _, q := range []string{`SELECT count(*) FROM portal_monthly_wallet_capture_slots`, `SELECT count(*) FROM portal_monthly_wallet_observations`} {
				var n int
				if err := receiver.QueryRow(q).Scan(&n); err != nil {
					t.Fatal(err)
				}
				want := 0
				if scenario == "missing_user" {
					want = 1
				}
				if n != want {
					t.Fatal("failure lost forensic original or left partial pair", scenario, n)
				}
			}
			if len(s.queries) > 2 {
				t.Fatal("source failure retried")
			}
		})
	}
}

func TestWalletMonthCaptureTransientReadIsNotIntegrityFailure(t *testing.T) {
	_, _, receiver, _, _, scope, _ := walletMonthCaptureFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := capturedWalletForScope(ctx, receiver, strings.Repeat("a", 64), scope); !errors.Is(err, context.Canceled) || errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("cancellation misclassified as bad money", err)
	}
	if _, err := capturedWalletForScope(context.Background(), receiver, strings.Repeat("a", 64), scope); !errors.Is(err, ErrWalletMonthCaptureIntegrity) {
		t.Fatal("missing original not rejected", err)
	}
}
