package financecredit

import "testing"

func TestParseLegacyAdjustment(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
		action  string
		unit    string
		value   int64
		before  int64
		after   int64
		ok      bool
	}{
		{name: "add", content: "管理员增加用户额度 ＄100.000000 额度", action: ActionAdd, unit: UnitUSD, value: 100_000_000, ok: true},
		{name: "subtract", content: "管理员减少用户额度 ￥10.500000 额度", action: ActionSubtract, unit: UnitCNY, value: 10_500_000, ok: true},
		{name: "override", content: "管理员覆盖用户额度从 ＄1.000000 额度 为 ＄201.000000 额度", action: ActionOverride, unit: UnitUSD, before: 1_000_000, after: 201_000_000, ok: true},
		{name: "points", content: "管理员增加用户额度 500 点额度", action: ActionAdd, unit: UnitPoints, value: 500_000_000, ok: true},
		{name: "too precise", content: "管理员增加用户额度 ＄1.0000001 额度", ok: false},
		{name: "other", content: "admin cleared github binding", ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ParseLegacyAdjustment(test.content)
			if ok != test.ok {
				t.Fatalf("ParseLegacyAdjustment() ok=%v want %v", ok, test.ok)
			}
			if ok && (got.Action != test.action || got.Unit != test.unit || got.ValueMicro != test.value || got.BeforeMicro != test.before || got.AfterMicro != test.after) {
				t.Fatalf("ParseLegacyAdjustment()=%+v", got)
			}
		})
	}
}

func TestCohort(t *testing.T) {
	created := int64(1_000_000)
	trial := Adjustment{Action: ActionAdd, Unit: UnitUSD, ValueMicro: 100_000_000}
	if got := Cohort(trial, created, created+3600); got != "trial_candidate_within_24h" {
		t.Fatalf("same-day cohort=%q", got)
	}
	if got := Cohort(trial, created, created+2*24*3600); got != "trial_candidate_within_7d" {
		t.Fatalf("seven-day cohort=%q", got)
	}
	if got := Cohort(trial, created, created+8*24*3600); got != "positive_after_7d" {
		t.Fatalf("late cohort=%q", got)
	}
	if got := Cohort(Adjustment{Action: ActionSubtract, Unit: UnitUSD, ValueMicro: 10_000_000}, created, created+1); got != "non_positive" {
		t.Fatalf("negative cohort=%q", got)
	}
}

func TestParseStructuredAdjustment(t *testing.T) {
	add, ok := ParseStructuredAdjustment(`{"op":{"action":"user.quota_add","params":{"quota":"＄150.000000 额度","target_user_id":42}}}`)
	if !ok || add.Action != ActionAdd || add.Unit != UnitUSD || add.ValueMicro != 150_000_000 || add.TargetUserID != 42 {
		t.Fatalf("structured add=%+v ok=%v", add, ok)
	}
	override, ok := ParseStructuredAdjustment(`{"op":{"action":"user.quota_override","params":{"from":"＄1.000000 额度","to":"＄201.000000 额度","target_user_id":"43"}}}`)
	if !ok || override.Action != ActionOverride || override.BeforeMicro != 1_000_000 || override.AfterMicro != 201_000_000 || override.TargetUserID != 43 {
		t.Fatalf("structured override=%+v ok=%v", override, ok)
	}
	for _, raw := range []string{
		`{"op":{"action":"channel.update","params":{"quota":"＄100.000000 额度"}}}`,
		`{"op":{"action":"user.quota_add","params":{"quota":"not money"}}}`,
		`not-json`,
	} {
		if _, ok := ParseStructuredAdjustment(raw); ok {
			t.Fatalf("accepted non-adjustment %q", raw)
		}
	}
}

func TestPotentialAdjustment(t *testing.T) {
	for _, test := range []struct {
		content string
		other   string
		want    bool
	}{
		{content: "管理员增加用户额度 新格式", want: true},
		{other: `{"op":{"action":"user.quota_add","params":{}}}`, want: true},
		{other: `{"op":{"action":"user.quota_override","params":{}}}`, want: true},
		{content: "管理员强制禁用了用户的两步验证", want: false},
		{other: `{"op":{"action":"channel.update"}}`, want: false},
		{other: `not-json`, want: false},
	} {
		if got := PotentialAdjustment(test.content, test.other); got != test.want {
			t.Fatalf("PotentialAdjustment(%q,%q)=%v want %v", test.content, test.other, got, test.want)
		}
	}
}
