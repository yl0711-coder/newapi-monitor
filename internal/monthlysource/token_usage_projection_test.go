package monthlysource

import (
	"context"
	"reflect"
	"testing"
)

func TestTokenUsageProjectionExactDetachedAndNotIndependent(t *testing.T) {
	db, _, request := tokenUsageSource(t)
	original, err := CaptureTokenUsageWindow(context.Background(), db, request)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := UsageObservationFromToken(original)
	if err != nil || !reflect.DeepEqual(projected.Rows, original.Totals) || !reflect.DeepEqual(projected.Wallet, original.Wallet) ||
		projected.StartedAtMS != original.StartedAtMS || projected.CompletedAtMS != original.CompletedAtMS || projected.Request != original.Request {
		t.Fatal("projection changed actual money, scope, wallet or source time", err)
	}
	// The exact same capture time is deliberately preserved, not refreshed to
	// let the projection impersonate another independent source observation.
	projected.Rows[0].Quota++
	*projected.Wallet.BalanceQuota++
	*projected.Wallet.UsedQuota++
	if projected.Rows[0].Quota == original.Totals[0].Quota || *projected.Wallet.BalanceQuota == *original.Wallet.BalanceQuota || *projected.Wallet.UsedQuota == *original.Wallet.UsedQuota {
		t.Fatal("projection aliases retained original")
	}
	original.Rows[0].Quota++
	if got, err := UsageObservationFromToken(original); err == nil || !reflect.DeepEqual(got, UsageWindowObservation{}) {
		t.Fatal("inconsistent token source projected as valid user totals")
	}
}
