package monitor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestFinanceSourceTracePreservesDigestAndNamesOnlyChanges(t *testing.T) {
	parts := make(financeSourceTrace)
	h := newFinanceSourceHasher(context.WithValue(context.Background(), financeSourceTraceKey{}, parts))
	plain := sha256.New()
	for _, source := range []string{"user-hours", "gift-proofs"} {
		h.section = source
		_, _ = fmt.Fprintf(h, "%s|sensitive-value\n", source)
		_, _ = fmt.Fprintf(plain, "%s|sensitive-value\n", source)
	}
	if fmt.Sprintf("%x", h.Sum(nil)) != fmt.Sprintf("%x", plain.Sum(nil)) {
		t.Fatal("diagnostics changed fingerprint")
	}
	changed := financeSourceTrace{"user-hours": parts["user-hours"], "gift-proofs": "new"}
	if got := financeChangedSources(parts, changed); got != "gift-proofs" {
		t.Fatal(got)
	}
}

func TestFinanceSourceTraceIdentifiesRealPublication(t *testing.T) {
	m, request := financePublicationRaceFixture(t)
	before, after := make(financeSourceTrace), make(financeSourceTrace)
	ctx := context.Background()
	first, err := m.financeReportSourceFingerprint(context.WithValue(ctx, financeSourceTraceKey{}, before), request.from.Unix(), request.to.Unix())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec("UPDATE channel_snaps SET models='changed' WHERE id=41").Error; err != nil {
		t.Fatal(err)
	}
	second, err := m.financeReportSourceFingerprint(context.WithValue(ctx, financeSourceTraceKey{}, after), request.from.Unix(), request.to.Unix())
	if err != nil || first == second {
		t.Fatalf("version did not change: %v", err)
	}
	if got := financeChangedSources(before, after); got != "channel-directory" {
		t.Fatal(got)
	}
}
