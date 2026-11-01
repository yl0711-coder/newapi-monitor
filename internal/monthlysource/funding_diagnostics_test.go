package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestFundingDiagnosticsCategoricalAndUnwrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := sql.Open("mysql", "readonly@tcp(127.0.0.1:1)/not_used")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// An invalid input fails before any connection is attempted.
	_, err = CaptureFundingPage(ctx, db, FundingScan{}, nil)
	if FundingFailureStage(err) != "input" {
		t.Fatal("safe input stage absent")
	}
	private := errors.New("private SQL and customer data must remain private")
	failure := &fundingCaptureFailure{stage: "source_rows", cause: private}
	if !errors.Is(failure, private) || strings.Contains(failure.Error(), private.Error()) || FundingFailureStage(failure) != "source_rows" {
		t.Fatal("private failure leaked or unwrap lost")
	}
	if FundingFailureStage(&fundingCaptureFailure{stage: "private customer value", cause: private}) != "" {
		t.Fatal("unknown diagnostic stage escaped allowlist")
	}
}
