package monitor

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func balanceStoreFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount) {
	t.Helper()
	m := newChannelUpstreamTestMonitor(t)
	t.Cleanup(m.Close)
	row := ChannelUpstreamAccount{
		Domain: "balance.example", Provider: upstreamProviderNewAPI,
		BaseURL: "https://balance.example", Account: "7", UserID: 7, Enabled: true,
		BalanceKnown: true, BalanceUSD: 20, BalanceRaw: 10000000, BalanceUnit: 500000,
		Status: upstreamStatusOK, LastSuccessAt: 1, UsageSyncEnabled: true,
		UsageBackfillCursor: 3600, UpdatedAt: 3600,
	}
	if err := m.persistSyncedUpstreamAccount(t.Context(), &row, newAPICredential{AccessToken: "fixture-token"}); err != nil {
		t.Fatal(err)
	}
	return m, row
}

func loadBalanceAccount(t *testing.T, m *Monitor, domain string) ChannelUpstreamAccount {
	t.Helper()
	var row ChannelUpstreamAccount
	if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func TestBalanceStorePreservesUsageAndWritesZero(t *testing.T) {
	m, observed := balanceStoreFixture(t)
	next := observed
	applyUpstreamSyncResult(&next, upstreamBalanceResult{BalanceUnit: 500000}, nil, 8000, m.cfg)
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", observed.Domain).
		Updates(map[string]any{"usage_backfill_cursor": 7200, "usage_last_error": "new usage state", "records_started_at": 42}).Error; err != nil {
		t.Fatal(err)
	}
	beforeWrite := loadBalanceAccount(t, m, observed.Domain)
	if err := m.persistUpstreamBalanceResult(t.Context(), observed, &next); err != nil {
		t.Fatal(err)
	}
	stored := loadBalanceAccount(t, m, observed.Domain)
	if stored.BalanceUSD != 0 || stored.BalanceRaw != 0 || !stored.BalanceKnown ||
		stored.UsageBackfillCursor != 7200 || stored.UsageLastError != "new usage state" || stored.RecordsStartedAt != 42 ||
		stored.UpdatedAt != beforeWrite.UpdatedAt || stored.LastSuccessAt != 8000 || next != stored {
		t.Fatal("balance write lost zero values, usage progress, configuration timestamp or returned stale state")
	}
}

func TestBalanceStoreRejectsSupersededSnapshot(t *testing.T) {
	for _, field := range []string{"deleted", "credential", "base_url", "balance_unit", "enabled", "usage_sync_enabled", "last_success_at"} {
		t.Run(field, func(t *testing.T) {
			m, observed := balanceStoreFixture(t)
			next := observed
			next.BalanceUSD = 999
			q := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", observed.Domain)
			var err error
			switch field {
			case "deleted":
				err = q.Delete(&ChannelUpstreamAccount{}).Error
			case "credential", "base_url":
				err = q.UpdateColumn(field, "replacement").Error // Same UpdatedAt.
			case "enabled", "usage_sync_enabled":
				err = q.UpdateColumn(field, false).Error
			default:
				err = q.UpdateColumn(field, 9000).Error
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := m.persistUpstreamBalanceResult(t.Context(), observed, &next); !errors.Is(err, errUpstreamBalanceSuperseded) {
				t.Fatalf("stale snapshot accepted: %v", err)
			}
			if field == "deleted" {
				var count int64
				if err := q.Count(&count).Error; err != nil || count != 0 {
					t.Fatal("deleted account resurrected")
				}
			} else if loadBalanceAccount(t, m, observed.Domain).BalanceUSD != observed.BalanceUSD {
				t.Fatal("stale balance overwrote current account")
			}
		})
	}
}

func TestBalanceStoreUnitBoundaryRollbackAndRetry(t *testing.T) {
	for _, busy := range []bool{false, true} {
		name := "permanent_failure"
		if busy {
			name = "retry"
		}
		t.Run(name, func(t *testing.T) {
			m, observed := balanceStoreFixture(t)
			next := observed
			next.BalanceUnit, next.LastAttemptAt = 1000000, 8000
			original := next
			attempts := 0
			failure := errors.New("injected permanent local failure")
			if busy {
				failure = errors.New("database is locked (SQLITE_BUSY)")
			}
			if err := m.storeDB.Callback().Update().Before("gorm:update").Register("test:balance-fail", func(tx *gorm.DB) {
				if tx.Statement.Table == "channel_upstream_accounts" {
					attempts++
					if attempts == 1 {
						_ = tx.AddError(failure)
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			err := m.persistUpstreamBalanceResult(t.Context(), observed, &next)
			var markers []ChannelUpstreamUsageHour
			if readErr := m.storeDB.Where("domain = ?", observed.Domain).Find(&markers).Error; readErr != nil {
				t.Fatal(readErr)
			}
			stored := loadBalanceAccount(t, m, observed.Domain)
			if !busy {
				if !errors.Is(err, failure) || attempts != 1 || len(markers) != 0 || next != original || stored != observed {
					t.Fatal("failed transaction changed caller, account or unit boundary")
				}
				return
			}
			if err != nil || attempts != 2 || len(markers) != 1 {
				t.Fatalf("retry not atomic: %v attempts=%d markers=%d", err, attempts, len(markers))
			}
			if markers[0].HourTs != 7200 || markers[0].UnitPerUSD != 500000 ||
				stored.BalanceUnit != 1000000 || stored.BalanceUnitPrevious != 500000 || stored.BalanceUnitEffectiveAt != 10800 || stored.UpdatedAt != observed.UpdatedAt {
				t.Fatal("retry altered historical accounting unit or used old configuration time")
			}
		})
	}
}

func TestBalanceStoreRealLockDoesNotRepeatCredentialRefresh(t *testing.T) {
	m, row := balanceStoreFixture(t)
	var refreshes, profiles atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			refreshes.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}}`))
		case "/api/v1/user/profile":
			profiles.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"data":{"balance":19.25}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	row.Provider, row.BaseURL = upstreamProviderSub2API, server.URL
	if err := m.persistSyncedUpstreamAccount(t.Context(), &row, sub2APICredential{AccessToken: "expired", RefreshToken: "old-refresh", ExpiresAt: 1}); err != nil {
		t.Fatal(err)
	}
	locker, err := sql.Open("sqlite", m.cfg.StorePath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = locker.Close() })
	locker.SetMaxOpenConns(1)
	committed := make(chan error, 1)
	attempts := 0
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("test:real-lock", func(tx *gorm.DB) {
		if tx.Statement.Table != "channel_upstream_accounts" {
			return
		}
		attempts++
		if attempts != 1 {
			return
		}
		if _, err := locker.Exec("BEGIN IMMEDIATE"); err != nil {
			_ = tx.AddError(err)
			return
		}
		if _, err := locker.Exec("INSERT INTO tracked_users(user_id, username) VALUES (99301, 'lock-fixture')"); err != nil {
			_, _ = locker.Exec("ROLLBACK")
			_ = tx.AddError(err)
			return
		}
		go func() {
			time.Sleep(120 * time.Millisecond)
			_, err := locker.Exec("COMMIT")
			committed <- err
		}()
	}); err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamAccount(t.Context(), row.Domain)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-committed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("real lock not released")
	}
	if attempts < 2 || refreshes.Load() != 1 || profiles.Load() != 1 || synced.BalanceUSD != 19.25 {
		t.Fatalf("local retry refetched upstream or lost balance: attempts=%d refreshes=%d profiles=%d", attempts, refreshes.Load(), profiles.Load())
	}
	stored := loadBalanceAccount(t, m, row.Domain)
	var cred sub2APICredential
	if err := m.openUpstreamCredential(stored, &cred); err != nil {
		t.Fatal(err)
	}
	if cred.AccessToken != "new-access" || cred.RefreshToken != "new-refresh" {
		t.Fatal("rotated credential not retained")
	}
}

func TestBalanceStoreRevalidatesBetweenRetries(t *testing.T) {
	m, observed := balanceStoreFixture(t)
	next := observed
	next.BalanceUSD = 999
	attempts := 0
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("test:concurrent-config", func(tx *gorm.DB) {
		if tx.Statement.Table != "channel_upstream_accounts" {
			return
		}
		attempts++
		// External writer commits after this attempt's read snapshot. The
		// retry must reread and reject, not apply a stale full-row upsert.
		if err := m.storeDB.Exec("UPDATE channel_upstream_accounts SET credential = ? WHERE domain = ?", "new-ciphertext", observed.Domain).Error; err != nil {
			_ = tx.AddError(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.persistUpstreamBalanceResult(t.Context(), observed, &next); !errors.Is(err, errUpstreamBalanceSuperseded) {
		t.Fatalf("retry failed to revalidate: %v", err)
	}
	stored := loadBalanceAccount(t, m, observed.Domain)
	if attempts != 1 || stored.Credential != "new-ciphertext" || stored.BalanceUSD != observed.BalanceUSD {
		t.Fatal("concurrent configuration overwritten")
	}
}

func TestUpstreamLocalStoreRetryBounds(t *testing.T) {
	for _, mode := range []string{"cancelled", "cancel_during_wait", "non_busy", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			attempts := 0
			failure := errors.New("SQLITE_BUSY")
			if mode == "non_busy" {
				failure = errors.New("disk I/O error")
			}
			err := retryUpstreamLocalStore(ctx, func() error {
				attempts++
				if mode == "cancel_during_wait" {
					cancel()
				}
				return failure
			})
			wantAttempts := map[string]int{"cancelled": 0, "cancel_during_wait": 1, "non_busy": 1, "exhausted": 4}[mode]
			wantErr := failure
			if mode == "cancelled" || mode == "cancel_during_wait" {
				wantErr = context.Canceled
			}
			if !errors.Is(err, wantErr) || attempts != wantAttempts {
				t.Fatalf("attempts=%d err=%v", attempts, err)
			}
		})
	}
}

func TestBalanceSyncAuthFailurePreservesLastBalance(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "upstream_401"
		if corrupt {
			name = "local_decryption"
		}
		t.Run(name, func(t *testing.T) {
			m, row := balanceStoreFixture(t)
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, `{"message":"invalid token"}`, http.StatusUnauthorized)
			}))
			t.Cleanup(server.Close)
			row.BaseURL = server.URL
			if corrupt {
				row.Credential = "invalid-ciphertext"
			}
			if err := m.persistUpstreamAccount(t.Context(), &row); err != nil {
				t.Fatal(err)
			}
			_, err := m.syncStoredUpstreamAccount(t.Context(), row.Domain)
			var syncErr *upstreamStoredSyncError
			if !errors.As(err, &syncErr) {
				t.Fatalf("auth failure not reported: %v", err)
			}
			stored := loadBalanceAccount(t, m, row.Domain)
			if !stored.BalanceKnown || stored.BalanceUSD != row.BalanceUSD || stored.LastSuccessAt != row.LastSuccessAt ||
				stored.Status != upstreamStatusReconnect || stored.NextSyncAt != upstreamAccountIsolatedUntil || stored.ConsecutiveFails != 1 {
				t.Fatal("authentication failure cleared balance or lost isolation")
			}
			if corrupt {
				if stored.Credential != row.Credential || requests.Load() != 0 {
					t.Fatal("decrypt failure overwrote recoverable ciphertext or called upstream")
				}
			} else if requests.Load() != 1 {
				t.Fatal("auth failure repeated request")
			}
		})
	}
}

func TestBalanceStoreCancelledContextDoesNotWrite(t *testing.T) {
	m, observed := balanceStoreFixture(t)
	next := observed
	next.BalanceUSD = 999
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.persistUpstreamBalanceResult(ctx, observed, &next); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel not propagated: %v", err)
	}
	if loadBalanceAccount(t, m, observed.Domain) != observed {
		t.Fatal("cancelled save changed account")
	}
}
