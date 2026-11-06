package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestUsageSplitCLIIsPurePlanAndKeepsExactScope(t *testing.T) {
	args := []string{"split-usage-window", "-source-identity", strings.Repeat("a", 64), "-user-id", "201", "-from", "1788192000", "-to", "1788278400"}
	var out bytes.Buffer
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Version  int                                 `json:"version"`
		Mode     string                              `json:"mode"`
		Children [2]monthlysource.UsageWindowRequest `json:"children"`
	}
	if json.Unmarshal(out.Bytes(), &result) != nil || result.Version != 1 || result.Mode != "query_budget_child_plan_not_source_data" ||
		result.Children[0].From != 1788192000 || result.Children[0].To != result.Children[1].From || result.Children[1].To != 1788278400 {
		t.Fatal("not a continuous exact smaller-window plan")
	}
	for _, child := range result.Children {
		if child.UserID != 201 || child.SourceIdentity != strings.Repeat("a", 64) || child.Rules.QuotaPerUnit != 500000 {
			t.Fatal("scope widened")
		}
	}
	for _, extra := range [][]string{{"-capture"}, {"-connection-file", "/unused"}, {"-user-id", "0"}, {"-to", "1788195600"}, {"-source-identity", "bad"}} {
		out.Reset()
		if err := run(context.Background(), append(append([]string{}, args...), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("invalid plan accepted", extra)
		}
	}
}
