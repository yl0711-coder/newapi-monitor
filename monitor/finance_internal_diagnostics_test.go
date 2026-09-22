package monitor

import (
	"context"
	"reflect"
	"testing"
)

func TestFinanceInternalDiagnosticReasons(t *testing.T) {
	for _, tc := range []struct {
		d     financeInternalPairDiagnostic
		valid int
		want  string
	}{
		{financeInternalPairDiagnostic{}, 0, "channel_cost_not_published"},
		{financeInternalPairDiagnostic{HasPublication: true, MissingCost: true, ManifestUnverified: true}, 0, "upstream_cost_missing"},
		{financeInternalPairDiagnostic{HasPublication: true, ManifestUnverified: true}, 0, "hour_manifest_unverified"},
		{financeInternalPairDiagnostic{HasPublication: true}, 0, "cost_publication_unverified"},
		{financeInternalPairDiagnostic{HasPublication: true}, 2, "ambiguous_publication"},
	} {
		if got := tc.d.reason(tc.valid); got != tc.want {
			t.Fatalf("got %s want %s", got, tc.want)
		}
	}
}

func TestFinanceInternalDiagnosticSQLAndDailyConservation(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	for index := 0; index < 2; index++ {
		hour := scope.FromTs + int64(index)*86400
		if err := m.storeDB.Create(&ChannelTestHourSample{HourTs: hour, ChannelID: 1, Requests: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	pub := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs, 1, 1, 0, 0, 1)
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", pub.PublicationID).Updates(map[string]any{"coverage_status": "upstream_cost_missing", "profit_known": false}).Error; err != nil {
		t.Fatal(err)
	}
	insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
	evidence, err := m.loadFinanceInternalCostEvidence(context.Background(), scope, financeConfiguredInternalEvidence{Complete: true, VerifiedScope: scope}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"upstream_cost_missing": 1, "channel_cost_not_published": 1}
	if evidence.UnverifiedPairs != 2 || !reflect.DeepEqual(evidence.UnverifiedReasons, want) || evidence.Total.Rows != 0 || evidence.Complete {
		t.Fatalf("incorrect evidence: %+v", evidence)
	}
	days, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, evidence, financeConfiguredInternalEvidence{Complete: true, VerifiedScope: scope}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for index, reason := range []string{"upstream_cost_missing", "channel_cost_not_published"} {
		if !reflect.DeepEqual(days[index].InternalCostUnverifiedReasons, map[string]int64{reason: 1}) || days[index].Statement.InternalTestUnverifiedPairs != 1 || days[index].InternalCostComplete {
			t.Fatalf("daily reason lost: %+v", days[index])
		}
	}
	// A legacy unresolved event still counts; daily projection must not alias
	// or mutate the full-report reason map.
	evidence.Events[0].Reason = ""
	part, err := financeInternalTestCostSubrange(evidence, stabilityScope{FromTs: scope.FromTs, ToTs: scope.FromTs + 86400})
	if err != nil || part.UnverifiedReasons["unspecified"] != 1 || !reflect.DeepEqual(evidence.UnverifiedReasons, want) {
		t.Fatal("legacy reason lost or source mutated")
	}
}
