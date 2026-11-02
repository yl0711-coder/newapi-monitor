//go:build !race

package monthlydraft

import "testing"

// This mandatory native gate keeps the actual 20-second production deadline
// and the full 200-account / 6000-pair end-to-end money and download checks.
// The same test body runs at bounded scale with -race, and the large original
// projection audit is also covered by monthlysource's race suite. CI verifies
// the union of both inventories; this is not an omitted or optional scale test.
// Detector overhead: https://go.dev/doc/articles/race_detector#Runtime_Overhead
func TestPartitionedCapturedGenerationLargeCompleteMonth(t *testing.T) {
	testPartitionedCapturedGenerationScale(t, 200)
}
