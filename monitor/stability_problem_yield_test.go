package monitor

import (
	"errors"
	"fmt"
	"testing"
)

func TestStabilityProblemGateWaitIsAYieldNotAFailure(t *testing.T) {
	m := &Monitor{}
	m.problemLastSuccess.Store(100)
	gate := fmt.Errorf("%w: %w", errStabilityProblemSourceGateWait, errors.New("context deadline exceeded"))
	if !m.noteStabilityProblemLiveResult(200, gate) {
		t.Fatal("gate wait must be reported as a yield")
	}
	if m.problemLastFailure.Load() != 0 || m.problemLastSuccess.Load() != 100 {
		t.Fatalf("gate wait must not stamp failure or success: failure=%d success=%d", m.problemLastFailure.Load(), m.problemLastSuccess.Load())
	}
	if m.noteStabilityProblemLiveResult(300, errors.New("query failed")) {
		t.Fatal("a real failure is not a yield")
	}
	if m.problemLastFailure.Load() != 300 {
		t.Fatal("a real failure must be stamped")
	}
	if m.noteStabilityProblemLiveResult(400, nil) || m.problemLastSuccess.Load() != 400 {
		t.Fatal("success must be stamped")
	}
}
