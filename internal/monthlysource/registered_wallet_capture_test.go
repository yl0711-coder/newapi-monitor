package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func registeredCaptureFixture(t *testing.T) (*sql.DB, *walletDriverState, *sql.DB, string, string, OrganizationMonthScope, FundingMonthCapturePlan) {
	t.Helper()
	source, state, receiver, path, key, scope, plan := walletMonthCaptureFixture(t)
	if err := InitRegisteredWalletMonthCaptureStore(t.Context(), receiver); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-24 * time.Hour).Unix()
	state.data = [][]driver.Value{{int64(201), at, int64(-1), int64(9007199254740993)}, {int64(202), at + 1, int64(500000), int64(1)}}
	return source, state, receiver, path, key, scope, plan
}

func registeredCaptureCounts(t *testing.T, db *sql.DB, observations, slots int) {
	t.Helper()
	for table, want := range map[string]int{"portal_monthly_registered_wallet_observations": observations, "portal_monthly_registered_wallet_capture_slots": slots} {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != want {
			t.Fatal("original/checkpoint partial commit", table, n, want, err)
		}
	}
}

func TestRegisteredCaptureAtomicRestartAndOfflineReuse(t *testing.T) {
	source, state, receiver, path, key, scope, plan := registeredCaptureFixture(t)
	first, err := CaptureNextRegisteredWalletMonthView(t.Context(), source, receiver, key, scope)
	if err != nil || first.Completed != 1 || !first.Captured || first.ID == "" {
		t.Fatal(first, err)
	}
	_, original, err := loadRegisteredWalletCapture(t.Context(), receiver, key, registeredCapturePlanSHA(plan), scope)
	if err != nil || *original.Users[0].BalanceQuota != -1 || *original.Users[0].UsedQuota != 9007199254740993 {
		t.Fatal("exact original changed", err)
	}
	registeredCaptureCounts(t, receiver, 1, 1)
	for _, query := range []string{"UPDATE portal_monthly_registered_wallet_capture_slots SET observation_id='changed' WHERE job_key=?", "DELETE FROM portal_monthly_registered_wallet_capture_slots WHERE job_key=?"} {
		if _, err := receiver.Exec(query, key); err == nil {
			t.Fatal("committed checkpoint is mutable")
		}
	}
	if _, err := receiver.Exec("PRAGMA recursive_triggers=OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Exec("INSERT OR REPLACE INTO portal_monthly_registered_wallet_capture_slots VALUES(?,?,?,?)", key, registeredCapturePlanSHA(plan), registeredCaptureScopeSHA(scope), strings.Repeat("c", 64)); err == nil {
		t.Fatal("REPLACE changed pinned original reference")
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	state.mu.Lock()
	queries := len(state.queries)
	state.queryErr = errors.New("offline source after committed checkpoint")
	state.mu.Unlock()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := CaptureNextRegisteredWalletMonthView(t.Context(), source, reopened, key, scope)
	if err != nil || again.Completed != 1 || again.Captured || again.ID != first.ID {
		t.Fatal("restart re-read source or changed committed original", again, err)
	}
	state.mu.Lock()
	after := len(state.queries)
	state.mu.Unlock()
	if after != queries {
		t.Fatal("offline reuse contacted source")
	}
	_, got, err := loadRegisteredWalletCapture(t.Context(), reopened, key, registeredCapturePlanSHA(plan), scope)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatal("restart changed original fields", err)
	}
}

func TestRegisteredCaptureFailuresRollbackOriginalAndSlot(t *testing.T) {
	for _, fault := range []string{"source", "missing_user", "original_write", "slot_write", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			source, state, receiver, _, key, scope, _ := registeredCaptureFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "source":
				state.queryErr = errors.New("source unavailable")
			case "missing_user":
				state.data = state.data[:1]
			case "original_write":
				if _, err := receiver.Exec(`CREATE TRIGGER registered_fixture_failure BEFORE INSERT ON portal_monthly_registered_wallet_observations BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "slot_write":
				if _, err := receiver.Exec(`CREATE TRIGGER registered_fixture_failure BEFORE INSERT ON portal_monthly_registered_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			}
			if out, err := CaptureNextRegisteredWalletMonthView(ctx, source, receiver, key, scope); err == nil || out.ID != "" || out.Completed != 0 {
				t.Fatal("failure returned a completed checkpoint")
			}
			registeredCaptureCounts(t, receiver, 0, 0)
		})
	}
}

func TestRegisteredCaptureRejectsDanglingCheckpointWithoutRecapture(t *testing.T) {
	source, state, receiver, _, key, scope, plan := registeredCaptureFixture(t)
	if _, err := receiver.Exec(`INSERT INTO portal_monthly_registered_wallet_capture_slots VALUES(?,?,?,?)`, key, registeredCapturePlanSHA(plan), registeredCaptureScopeSHA(scope), strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	if out, err := CaptureNextRegisteredWalletMonthView(t.Context(), source, receiver, key, scope); !errors.Is(err, ErrWalletMonthCaptureIntegrity) || out.ID != "" {
		t.Fatal("lost original treated as not captured", out, err)
	}
	if len(state.queries) != 0 {
		t.Fatal("dangling checkpoint was repaired using new source data")
	}
	registeredCaptureCounts(t, receiver, 0, 1)
}

func TestRegisteredCapturePinsScopeAndPlanAndSchemaBeforeSource(t *testing.T) {
	for _, fault := range []string{"scope", "source", "month", "plan_hash", "schema", "trigger"} {
		t.Run(fault, func(t *testing.T) {
			source, state, receiver, _, key, scope, plan := registeredCaptureFixture(t)
			switch fault {
			case "scope":
				scope.AuthVersion++
			case "source":
				scope.Funding.SourceIdentity = strings.Repeat("c", 64)
			case "month":
				scope.Funding.Month = "2026-08"
			case "plan_hash":
				if _, err := receiver.Exec(`INSERT INTO portal_monthly_registered_wallet_capture_slots VALUES(?,?,?,?)`, key, strings.Repeat("c", 64), registeredCaptureScopeSHA(scope), strings.Repeat("d", 64)); err != nil {
					t.Fatal(err)
				}
			case "schema":
				if _, err := receiver.Exec("DROP TABLE portal_monthly_registered_wallet_capture_slots"); err != nil {
					t.Fatal(err)
				}
			case "trigger":
				if _, err := receiver.Exec("DROP TRIGGER monthly_registered_capture_no_replace"); err != nil {
					t.Fatal(err)
				}
				if err := ValidateRegisteredWalletMonthCaptureStore(t.Context(), receiver); err == nil {
					t.Fatal("missing guard passed preflight")
				}
				return
			}
			if out, err := CaptureNextRegisteredWalletMonthView(t.Context(), source, receiver, key, scope); err == nil || out.ID != "" {
				t.Fatal("changed plan/scope/schema accepted")
			}
			if len(state.queries) != 0 {
				t.Fatal("invalid checkpoint read source", plan.Key)
			}
		})
	}
}

func registeredCopyFixture(t *testing.T) (*sql.DB, string, []byte, RegisteredWalletObservation) {
	t.Helper()
	source, path, key, scope := capturedSelectionFixture(t, false)
	if err := InitRegisteredWalletMonthCaptureStore(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	plan, err := loadFundingMonthCapturePlan(t.Context(), source, key, scope, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute).UnixMilli()
	o := RegisteredWalletObservation{Version: 1, Basis: registeredWalletObservationBasis, SourceIdentity: scope.Funding.SourceIdentity, StartedAtMS: now, CompletedAtMS: now + 1, Users: []RegisteredWalletUser{}}
	for _, id := range scope.Funding.UserIDs {
		o.Users = append(o.Users, RegisteredWalletUser{UserID: id, RegisteredAt: now/1000 - 86400, BalanceQuota: walletPointer(-1), UsedQuota: walletPointer(9007199254740993)})
	}
	if err := saveRegisteredWalletCapture(t.Context(), source, plan, o); err != nil {
		t.Fatal(err)
	}
	assembled, err := BuildCapturedMonthSelection(t.Context(), source, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	return source, path, assembled.Selection, o
}

func TestRegisteredCaptureCopyExactSelectedOnlyAndReadOnlyReopen(t *testing.T) {
	source, file, selection, want := registeredCopyFixture(t)
	orphan := want
	orphan.StartedAtMS += 10
	orphan.CompletedAtMS += 10
	if _, err := SaveRegisteredWalletObservation(t.Context(), source, orphan); err != nil {
		t.Fatal(err)
	}
	var originalSelection CapturedMonthSelection
	if json.Unmarshal(selection, &originalSelection) != nil {
		t.Fatal("bad selection")
	}
	otherPlan, err := EnsureFundingMonthCapture(t.Context(), source, digest([]byte("unrelated-synthetic-job")), originalSelection.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveRegisteredWalletCapture(t.Context(), source, otherPlan, orphan); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var s CapturedMonthSelection
	if json.Unmarshal(selection, &s) != nil {
		t.Fatal("bad selection fixture")
	}
	destination, output := emptyCapturedCopy(t)
	base, err := CopyCapturedMonthObservations(t.Context(), source, destination, s.Key, s.Scope)
	if err != nil || !bytes.Equal(base.Selection, selection) {
		t.Fatal("core source copy changed", err)
	}
	if err := CopySelectedRegisteredWallet(t.Context(), source, destination, selection); err != nil {
		t.Fatal(err)
	}
	registeredCaptureCounts(t, destination, 1, 1)
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+output+"?mode=ro&immutable=1&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	id, got, err := ReadSelectedRegisteredWallet(t.Context(), ro, selection)
	if err != nil || id == "" || !reflect.DeepEqual(got, want) {
		t.Fatal("sealed original changed", err)
	}
	if err := VerifyCapturedObservationSelection(t.Context(), ro, selection); err != nil {
		t.Fatal("old core source validation was affected", err)
	}
	after, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("copy changed source", err)
	}
}

func TestRegisteredCaptureCopyRejectsForeignAliasAndPlanMismatch(t *testing.T) {
	for _, fault := range []string{"alias", "empty", "empty_core", "foreign", "selection", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			source, file, selection, _ := registeredCopyFixture(t)
			var s CapturedMonthSelection
			if json.Unmarshal(selection, &s) != nil {
				t.Fatal("bad fixture")
			}
			destination, _ := emptyCapturedCopy(t)
			if fault != "empty" && fault != "alias" && fault != "empty_core" {
				if _, err := CopyCapturedMonthObservations(t.Context(), source, destination, s.Key, s.Scope); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "empty_core":
				for _, init := range []func(context.Context, *sql.DB) error{InitFundingPageStore, InitUsageWindowStore, InitWalletObservationStore} {
					if err := init(t.Context(), destination); err != nil {
						t.Fatal(err)
					}
				}
			case "alias":
				alias, err := sql.Open("sqlite", file)
				if err != nil {
					t.Fatal(err)
				}
				defer alias.Close()
				destination = alias
			case "foreign":
				if _, err := destination.Exec("CREATE TABLE private_other_customer(value TEXT)"); err != nil {
					t.Fatal(err)
				}
			case "selection":
				s.PlanSHA256 = strings.Repeat("c", 64)
				selection, _ = json.Marshal(s)
			case "cancel":
				cancel()
			}
			if err := CopySelectedRegisteredWallet(ctx, source, destination, selection); err == nil {
				t.Fatal("unsafe destination/selection accepted")
			}
		})
	}
}
