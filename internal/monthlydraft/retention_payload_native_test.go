//go:build !race

package monthlydraft

import "testing"

// TestRetentionFullReceiverPayloadPerformance measures the complete 32-record,
// 16 MiB payload fixture against the unchanged five-second production timeout.
// CI requires this test to execute and pass independently of race coverage.
func TestRetentionFullReceiverPayloadPerformance(t *testing.T) {
	testRetentionReceiverPayload(t, 512<<10)
}
