package main

import (
	"bytes"
	"testing"
)

func TestFinanceFactBackfillCommandRejectsUnsafeArgumentsBeforeStartup(t *testing.T) {
	for _, args := range [][]string{{"-max-hours=0"}, {"-max-hours=169"}, {"-timeout=0s"}, {"unexpected"}} {
		if err := runFinanceFactBackfillCommand(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("unsafe args accepted: %v", args)
		}
	}
}
