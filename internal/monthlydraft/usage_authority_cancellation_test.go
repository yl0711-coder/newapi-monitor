package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"gorm.io/gorm"
)

// database/sql may finish the rollback before GORM observes a canceled
// transaction. Preserve both causes without weakening the authorization fence.
func TestUsageGenerationAuthorityCancellationPreservesTransactionCause(t *testing.T) {
	for _, tc := range []struct {
		name              string
		canceled, expired bool
	}{{"canceled_transaction", true, false}, {"expired_transaction", false, true}, {"transaction_error_only", false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			_, ro, path := usageAuthorityFixture(t)
			want, wantIDs, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var ctx context.Context
			var cancel context.CancelFunc
			if tc.expired {
				ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			called := false
			const callback = "monthly_test_transaction_cancellation"
			if err := ro.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
				if called || tx.Statement.Table != "customer_groups" || tx.Error != nil {
					return
				}
				called = true
				if tc.canceled {
					cancel()
				}
				if tc.canceled || tc.expired {
					<-tx.Statement.Context.Done()
				}
				_ = tx.AddError(sql.ErrTxDone)
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ro.Callback().Query().Remove(callback) })
			owner, ids, err := LoadUsageGenerationOwner(ctx, ro, 1, 1)
			if !called || !errors.Is(err, sql.ErrTxDone) || errors.Is(err, ErrGenerationChanged) ||
				owner != (monthlytasks.Owner{}) || len(ids) != 0 {
				t.Fatal("failed read returned an owner or misclassified revocation", called, owner, ids, err)
			}
			if errors.Is(err, context.Canceled) != tc.canceled || errors.Is(err, context.DeadlineExceeded) != tc.expired {
				t.Fatal("transaction cancellation cause was lost or invented", err)
			}
			if err := ro.Callback().Query().Remove(callback); err != nil {
				t.Fatal(err)
			}
			got, gotIDs, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
			if err != nil || got != want || !reflect.DeepEqual(gotIDs, wantIDs) {
				t.Fatal("temporary failure changed permission scope or prevented recovery", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("authorization read changed the source database", err)
			}
		})
	}
}
