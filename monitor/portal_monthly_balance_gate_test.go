package monitor

import (
	"testing"
	"time"
)

// Persistent differences must never be hidden by rounding to cents, an amount
// tolerance, or aggregation across organization members. Synthetic fixtures only.
func TestMonthlyBalanceDifferenceNeverTolerated(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	for _, difference := range []int64{1, -1, 909785, -909785} {
		e := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
		e.Users[0].ReverseClosingQuota += difference
		if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
			t.Fatalf("accepted unexplained difference %d quota", difference)
		}
	}
}

func TestMonthlyOrganizationCannotNetMemberBalanceDifferences(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	first := &e.Users[0]
	first.ReverseClosingQuota += 909785
	second := *first
	second.UserID = 44
	second.Events = append([]MonthlyBillCreditEvidence(nil), first.Events...)
	for i := range second.Events {
		second.Events[i].ID += 100
	}
	second.Reversals = []MonthlyBillReversal{{CreditID: 103, DebitID: 104, Reason: "synthetic correction"}}
	second.ReverseClosingQuota = second.ClosingQuota - 909785
	e.Users = append(e.Users, second)
	if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
		t.Fatal("offsetting member differences were accepted")
	}
}
