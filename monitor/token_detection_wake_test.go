package monitor

import (
	"testing"
)

func TestTokenDetectionWakeCoalescesAndIsReusable(t *testing.T) {
	m := &Monitor{}
	wake := m.ensureTokenDetectionWake()
	if wake == nil {
		t.Fatal("detector wake channel was not initialized")
	}
	m.signalTokenDetectionWake()
	m.signalTokenDetectionWake()
	select {
	case <-wake:
	default:
		t.Fatal("committed-work wake was not published")
	}
	select {
	case <-wake:
		t.Fatal("wake notifications did not coalesce")
	default:
	}
	m.signalTokenDetectionWake()
	select {
	case <-wake:
	default:
		t.Fatal("wake channel was not reusable after a receive")
	}
}
