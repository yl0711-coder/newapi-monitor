//go:build race

package monthlydraft

// raceDetector reports whether tests run under the race detector, which slows
// CPU-bound work several times; only test-only deadlines may depend on it.
const raceDetector = true
