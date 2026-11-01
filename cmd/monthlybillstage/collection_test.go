package main

import (
	"bytes"
	"context"
	"testing"
)

func TestCollectionPlanCommandCannotActivateOrManuallyPickMembers(t *testing.T) {
	for _, args := range [][]string{{"plan-collection"}, {"plan-collection", "-write"}, {"plan-collection", "-apply"}, {"plan-collection", "-user-ids=201"}, {"plan-collection", "-capture"}, {"plan-collection", "extra"}} {
		var out, stderr bytes.Buffer
		if err := run(context.Background(), args, &out, &stderr); err == nil || out.Len() != 0 {
			t.Fatal("invalid or activating roster command accepted", args, err)
		}
	}
}
