package financecur

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func projectionTime(t *testing.T, month time.Month, day int) int64 {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	return time.Date(2026, month, day, 0, 0, 0, 0, loc).Unix()
}

func projectionFixture(t *testing.T, artifacts ...Artifact) *SourceProjector {
	t.Helper()
	set, err := NewSourceSet(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewSourceProjector(set)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func assertProjectionConserved(t *testing.T, r SourceProjection) {
	t.Helper()
	days, months := []TimeBucket{}, []TimeBucket{}
	for _, day := range r.Days {
		days = append(days, day.Bucket)
	}
	for _, month := range r.Months {
		months = append(months, month.Bucket)
	}
	products := []TimeBucket{}
	for _, product := range r.Products {
		products = append(products, product.Days...)
	}
	want, err := sumTimeBuckets([]TimeBucket{r.Known})
	if err != nil {
		t.Fatal(err)
	}
	for _, buckets := range [][]TimeBucket{days, months, products} {
		got, err := sumTimeBuckets(buckets)
		if err != nil || got != want {
			t.Fatalf("monetary reconciliation failed: %+v != %+v; %v", got, want, err)
		}
	}
}

func TestSourceProjectionMergesShanghaiBoundaryWithoutDoubleCounting(t *testing.T) {
	sep := sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai")
	oct := sourceSetTestArtifact(t, 10, 744_000, true, "Asia/Shanghai")
	p := projectionFixture(t, oct, sep)
	from, to := projectionTime(t, 10, 1), projectionTime(t, 10, 2)
	r, err := p.Query(from, to)
	if err != nil {
		t.Fatal(err)
	}
	// Each synthetic hour costs 1,000 nano-USD: Sep supplies 8 hours and Oct 16.
	if !r.RangeComplete || r.Known.NexusAPINanoUSD != 24_000 || len(r.Days) != 1 || !r.Days[0].RangeComplete {
		t.Fatalf("boundary mismatch: %+v", r)
	}
	assertProjectionConserved(t, r)
	r, err = p.Query(projectionTime(t, 9, 1), projectionTime(t, 11, 2))
	if err != nil {
		t.Fatal(err)
	}
	if r.Known.NexusAPINanoUSD != 1_464_000 || r.RangeComplete || len(r.MissingBillingPeriods) != 2 {
		t.Fatalf("total/source gaps mismatch: %+v", r)
	}
	assertProjectionConserved(t, r)
	// Month sums equal separate queries, including the UTC spill into Nov 1.
	for _, m := range r.Months {
		sub, err := p.Query(m.Bucket.FromUnix, m.Bucket.ToUnix)
		if err != nil || sub.Known.NexusAPINanoUSD != m.Bucket.NexusAPINanoUSD || sub.RangeComplete != m.RangeComplete {
			t.Fatal("month query drift", err)
		}
	}
}

func TestSourceProjectionMissingAdjacentSourceDoesNotBecomeZero(t *testing.T) {
	sep := sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai")
	p := projectionFixture(t, sep)
	r, err := p.Query(projectionTime(t, 10, 1), projectionTime(t, 10, 2))
	if err != nil {
		t.Fatal(err)
	}
	if r.Known.NexusAPINanoUSD != 8_000 || r.RangeComplete || len(r.Days) != 1 || r.Days[0].RangeComplete || len(r.MissingBillingPeriods) != 1 {
		t.Fatalf("missing October mislabeled: %+v", r)
	}
	r, err = p.Query(projectionTime(t, 9, 1), projectionTime(t, 9, 2))
	if err != nil {
		t.Fatal(err)
	}
	if r.RangeComplete || r.Known.NexusAPINanoUSD != 16_000 || r.MissingBillingPeriods[0].ToUnix-r.MissingBillingPeriods[0].FromUnix != 8*3600 {
		t.Fatalf("missing August mislabeled: %+v", r)
	}
	assertProjectionConserved(t, r)
}

func TestSourceProjectionDraftScopeDoesNotBlockUnrelatedMonth(t *testing.T) {
	sep := sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai")
	oct := sourceSetTestArtifact(t, 10, 744_000, false, "Asia/Shanghai")
	p := projectionFixture(t, sep, oct)
	r, err := p.Query(projectionTime(t, 9, 2), projectionTime(t, 9, 30))
	if err != nil {
		t.Fatal(err)
	}
	if !r.RangeComplete || len(r.UnverifiedStatementIDs) != 0 {
		t.Fatal("October draft blocked closed September range")
	}
	r, err = p.Query(projectionTime(t, 9, 30), projectionTime(t, 10, 2))
	if err != nil {
		t.Fatal(err)
	}
	if r.RangeComplete || len(r.UnverifiedStatementIDs) != 1 || !r.Days[0].RangeComplete || r.Days[1].RangeComplete {
		t.Fatalf("source-specific closure lost: %+v", r)
	}
}

func TestSourceProjectionGapPartialDayAndEmptyRange(t *testing.T) {
	sep := sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai")
	nov := sourceSetTestArtifact(t, 11, 720_000, true, "Asia/Shanghai")
	p := projectionFixture(t, sep, nov)
	r, err := p.Query(projectionTime(t, 9, 30), projectionTime(t, 11, 3))
	if err != nil {
		t.Fatal(err)
	}
	if r.RangeComplete || len(r.MissingBillingPeriods) != 1 {
		t.Fatal("October hole hidden", r.MissingBillingPeriods)
	}
	assertProjectionConserved(t, r)
	r, err = p.Query(projectionTime(t, 9, 2)+3600, projectionTime(t, 9, 4))
	if err != nil {
		t.Fatal(err)
	}
	if r.RangeComplete || len(r.PartialBoundaryDays) != 1 || r.Known.NexusAPINanoUSD != 24_000 || len(r.Days) != 1 {
		t.Fatal("partial day was prorated", r)
	}
	r, err = p.Query(projectionTime(t, 10, 10), projectionTime(t, 10, 11))
	if err != nil {
		t.Fatal(err)
	}
	if r.RangeComplete || len(r.Days) != 0 || r.Known.Complete || len(r.MissingBillingPeriods) != 1 {
		t.Fatal("missing source shown as verified zero", r)
	}
	if _, err := p.Query(10, 9); err == nil {
		t.Fatal("invalid range accepted")
	}
}

func TestSourceProjectionCreditsRemainSignedAndUnknownCancellationIncomplete(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	entries := []CostEntry{
		{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "AmazonECS", LineItemType: "Usage", Rows: 1, NanoUSD: 720_000},
		{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "AmazonECS", LineItemType: "Credit", Rows: 1, NanoUSD: -72_000},
		{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "Unknown", LineItemType: "Usage", Rows: 1, NanoUSD: 72_000},
		{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "Unknown", LineItemType: "Credit", Rows: 1, NanoUSD: -72_000},
	}
	rules := []AllocationRule{{ID: "ecs", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "synthetic ownership"}}
	allocation, err := Allocate(entries, rules)
	if err != nil {
		t.Fatal(err)
	}
	audit := Audit{Currency: "USD", Rows: 4, BillingPeriodStart: start.Unix(), BillingPeriodEnd: end.Unix(), FirstUsageUnix: start.Unix(), LastUsageThroughUnix: end.Unix(), TotalNanoUSD: 648_000}
	s, timeline, err := BuildStatement(audit, allocation, testDigest, testDigest, "Asia/Shanghai", true)
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewArtifact(s, timeline)
	if err != nil {
		t.Fatal(err)
	}
	p := projectionFixture(t, a)
	r, err := p.Query(projectionTime(t, 9, 2), projectionTime(t, 9, 3))
	if err != nil {
		t.Fatal(err)
	}
	if r.Known.NexusAPINanoUSD != 21_600 || r.Known.UnallocatedNanoUSD != 0 || r.Known.UnallocatedAbsNanoUSD == 0 || r.RangeComplete || r.Known.Complete {
		t.Fatalf("credits or unknown cancellation mishandled: %+v", r)
	}
	assertProjectionConserved(t, r)
}

func TestSourceProjectionDetachedEvidenceAndOverflow(t *testing.T) {
	a := sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai")
	p := projectionFixture(t, a)
	from, to := projectionTime(t, 9, 2), projectionTime(t, 9, 3)
	before, err := p.Query(from, to)
	if err != nil {
		t.Fatal(err)
	}
	a.Timeline.Days[1].NexusAPINanoUSD++
	a.Timeline.Products[0].Days[1].NexusAPINanoUSD++
	after, err := p.Query(from, to)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("caller changed frozen index")
	}
	after.Days[0].Bucket.NexusAPINanoUSD++
	after.Products[0].Days[0].NexusAPINanoUSD++
	again, err := p.Query(from, to)
	if err != nil || !reflect.DeepEqual(before, again) {
		t.Fatal("result changed frozen index")
	}
	p = projectionFixture(t, sourceSetTestArtifact(t, 9, 6_000_000_000_000_000_000, true, "Asia/Shanghai"), sourceSetTestArtifact(t, 10, 6_000_000_000_000_000_000, true, "Asia/Shanghai"))
	if _, err := p.Query(projectionTime(t, 9, 1), projectionTime(t, 11, 2)); err == nil {
		t.Fatal("signed overflow accepted")
	}
}

func TestSourceProjectionRejectsRehashedDailyProductMismatch(t *testing.T) {
	a := sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai")
	// Preserve every monthly/product/global sum while moving costs to a wrong day.
	a.Timeline.Products[0].Days[1].SourceNanoUSD++
	a.Timeline.Products[0].Days[1].NexusAPINanoUSD++
	a.Timeline.Products[0].Days[2].SourceNanoUSD--
	a.Timeline.Products[0].Days[2].NexusAPINanoUSD--
	var err error
	a.Statement.TimelineSHA256, err = TimelineHash(a.Timeline)
	if err != nil {
		t.Fatal(err)
	}
	a.Statement.StatementID, err = statementDigest(a.Statement)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(a); err != nil {
		t.Fatal("fixture must pass aggregate-only checks", err)
	}
	set, err := NewSourceSet([]Artifact{a})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSourceProjector(set); err == nil {
		t.Fatal("per-day product mismatch accepted")
	}
}

func TestSourceProjectionConcurrentQueries(t *testing.T) {
	p := projectionFixture(t, sourceSetTestArtifact(t, 9, 720_000, true, "Asia/Shanghai"), sourceSetTestArtifact(t, 10, 744_000, true, "Asia/Shanghai"))
	from, to := projectionTime(t, 10, 1), projectionTime(t, 10, 2)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				r, err := p.Query(from, to)
				if err != nil || r.Known.NexusAPINanoUSD != 24_000 || !r.RangeComplete {
					t.Errorf("concurrent projection drift: %v", err)
					return
				}
				r.Days[0].Bucket.NexusAPINanoUSD++
			}
		}()
	}
	wg.Wait()
}

func BenchmarkSourceProjection24Months(b *testing.B) {
	artifacts := []Artifact{}
	for month := time.Month(1); month <= 24; month++ {
		artifacts = append(artifacts, sourceSetTestArtifact(b, month, 720_000, true, "Asia/Shanghai"))
	}
	set, err := NewSourceSet(artifacts)
	if err != nil {
		b.Fatal(err)
	}
	p, err := NewSourceProjector(set)
	if err != nil {
		b.Fatal(err)
	}
	from, to := artifacts[0].Timeline.Days[0].FromUnix, artifacts[len(artifacts)-1].Timeline.Days[len(artifacts[len(artifacts)-1].Timeline.Days)-1].ToUnix
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Query(from, to); err != nil {
			b.Fatal(err)
		}
	}
}
