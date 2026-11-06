package monthlysource

import (
	"reflect"
	"testing"
)

func TestUsageWindowSplitPreservesScopeAndCompleteCoverageWithoutMoney(t *testing.T) {
	_, _, request := usageTestSource(t)
	request.To = request.From + 86400
	initial := request
	leaves := []UsageWindowRequest{request}
	for round := 0; round < 4; round++ {
		var next []UsageWindowRequest
		for _, r := range leaves {
			c, err := SplitUsageWindow(r)
			if err != nil || c[0].From != r.From || c[0].To != c[1].From || c[1].To != r.To {
				t.Fatal("lost, duplicated or crossed scope", c, err)
			}
			for _, child := range c {
				if child.UserID != request.UserID || child.SourceIdentity != request.SourceIdentity || child.Rules != request.Rules || child.To-child.From < 3600 {
					t.Fatal("split changed account, source, units or minimum")
				}
				next = append(next, child)
			}
		}
		leaves = next
	}
	if len(leaves) != 16 || !reflect.DeepEqual(initial, request) {
		t.Fatal("unbounded splitting or input mutation")
	}
	for _, r := range leaves {
		if _, err := SplitUsageWindow(r); err == nil {
			t.Fatal("split below one-hour minimum accepted")
		}
	}
	for _, mutate := range []func(*UsageWindowRequest){
		func(r *UsageWindowRequest) { r.UserID = 0 },
		func(r *UsageWindowRequest) { r.SourceIdentity = "bad" },
		func(r *UsageWindowRequest) { r.Rules.QuotaPerUnit = 1 },
		func(r *UsageWindowRequest) { r.To = r.From + 86401 },
		func(r *UsageWindowRequest) { r.To = r.From + 7199 },
	} {
		r := request
		mutate(&r)
		c, err := SplitUsageWindow(r)
		if err == nil || c != ([2]UsageWindowRequest{}) {
			t.Fatal("invalid split returned children")
		}
	}
}
