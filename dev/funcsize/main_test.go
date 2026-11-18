package main

import "testing"

func TestRatchetRejectsNewAndGrowingLongFunctions(t *testing.T) {
	base := map[string]size{"monitor A": {200, 50}, "monitor B": {150, 10}}
	cur := map[string]size{
		"monitor A": {210, 50}, // grew
		"monitor B": {140, 10}, // shrank
		"monitor C": {121, 0},  // new, too long
		"monitor D": {50, 41},  // new, too many branches
		"monitor E": {120, 40}, // new, at the limit
	}
	failures, shrunk := check(base, cur)
	if len(failures) != 3 {
		t.Fatalf("failures = %v", failures)
	}
	if len(shrunk) != 1 || shrunk[0] != "monitor B" {
		t.Fatalf("shrunk = %v", shrunk)
	}
	if f, _ := check(base, map[string]size{"monitor A": {200, 50}}); len(f) != 0 {
		t.Fatalf("unchanged listed function must pass: %v", f)
	}
}
