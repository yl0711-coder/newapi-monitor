package main

import (
	"bytes"
	"testing"
)

func TestStabilityFactBackfillCommandRejectsUnsafeArgumentsBeforeStartup(t *testing.T) {
	for _, args := range [][]string{
		{}, {"-from=1", "-to=3600"}, {"-from=3600", "-to=3600"},
		{"-from=3600", "-to=266400"}, {"-from=3600", "-to=7200", "-timeout=0s"}, {"unexpected"},
	} {
		if err := runStabilityFactBackfillCommand(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("unsafe args accepted: %v", args)
		}
	}
}
