package financecur

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sourceSetTestArtifact(t testing.TB, month time.Month, cost int64, finalized bool, zone string) Artifact {
	t.Helper()
	start := time.Date(2026, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	r, err := Allocate([]CostEntry{{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "AmazonECS", LineItemType: "Usage", Rows: 1, NanoUSD: cost}}, []AllocationRule{{ID: "ecs", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "synthetic fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	audit := Audit{Currency: "USD", Rows: 1, BillingPeriodStart: start.Unix(), BillingPeriodEnd: end.Unix(), FirstUsageUnix: start.Unix(), LastUsageThroughUnix: end.Unix(), TotalNanoUSD: cost}
	s, timeline, err := BuildStatement(audit, r, testDigest, testDigest, zone, finalized)
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewArtifact(s, timeline)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestSourceSetCanonicalIdentityAndExplicitRevision(t *testing.T) {
	sep := sourceSetTestArtifact(t, 9, 31_000, true, "Asia/Shanghai")
	oct := sourceSetTestArtifact(t, 10, 42_000, false, "Asia/Shanghai")
	original := []Artifact{oct, sep}
	set, err := NewSourceSet(original)
	if err != nil {
		t.Fatal(err)
	}
	if original[0].Statement.StatementID != oct.Statement.StatementID {
		t.Fatal("caller order changed")
	}
	r, err := ReviewSourceSet(set)
	if err != nil {
		t.Fatal(err)
	}
	backwards := set
	backwards.Artifacts = original
	r2, err := ReviewSourceSet(backwards)
	if err != nil || r.SourceSetID != r2.SourceSetID {
		t.Fatal("source order changed identity", err)
	}
	if len(r.Gaps) != 0 || r.Statements[0].Status != StatementStatusPublishable || r.Statements[1].Status != StatementStatusDraft {
		t.Fatal("per-source approval states changed", r)
	}
	updated := sourceSetTestArtifact(t, 10, 43_000, false, "Asia/Shanghai")
	if _, err := NewSourceSet([]Artifact{sep, oct, updated}); err == nil {
		t.Fatal("two revisions of October accepted")
	}
	replaced, err := NewSourceSet([]Artifact{sep, updated})
	if err != nil {
		t.Fatal(err)
	}
	r3, err := ReviewSourceSet(replaced)
	if err != nil || r3.SourceSetID == r.SourceSetID {
		t.Fatal("corrected source did not change identity", err)
	}
	// Both UTC sources legitimately contribute to October 1 in Shanghai.
	last := sep.Timeline.Days[len(sep.Timeline.Days)-1]
	first := oct.Timeline.Days[0]
	if last.Key != first.Key || last.NexusAPINanoUSD == 0 || first.NexusAPINanoUSD == 0 {
		t.Fatal("fixture must retain both parts of boundary day")
	}
	if !reflect.DeepEqual(set.Artifacts[0], sep) || !reflect.DeepEqual(set.Artifacts[1], oct) {
		t.Fatal("monthly evidence modified by grouping")
	}
}

func TestSourceSetMissingPeriodsIncludeEdgesAndHoles(t *testing.T) {
	sep := sourceSetTestArtifact(t, 9, 31_000, true, "Asia/Shanghai")
	nov := sourceSetTestArtifact(t, 11, 42_000, false, "Asia/Shanghai")
	set, err := NewSourceSet([]Artifact{nov, sep})
	if err != nil {
		t.Fatal(err)
	}
	r, err := ReviewSourceSet(set)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Gaps, []SourceInterval{{sep.Statement.BillingPeriodEnd, nov.Statement.BillingPeriodStart}}) {
		t.Fatal(r.Gaps)
	}
	from, to := sep.Statement.BillingPeriodStart-8*3600, nov.Statement.BillingPeriodEnd+3600
	missing, err := r.MissingPeriods(from, to)
	want := []SourceInterval{{from, sep.Statement.BillingPeriodStart}, {sep.Statement.BillingPeriodEnd, nov.Statement.BillingPeriodStart}, {nov.Statement.BillingPeriodEnd, to}}
	if err != nil || !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing=%+v want=%+v err=%v", missing, want, err)
	}
	missing, err = r.MissingPeriods(sep.Statement.BillingPeriodStart, sep.Statement.BillingPeriodEnd)
	if err != nil || len(missing) != 0 {
		t.Fatal(missing, err)
	}
	missing, err = r.MissingPeriods(from-3600, from)
	if err != nil || !reflect.DeepEqual(missing, []SourceInterval{{from - 3600, from}}) {
		t.Fatal(missing, err)
	}
	missing, err = r.MissingPeriods(to, to+3600)
	if err != nil || !reflect.DeepEqual(missing, []SourceInterval{{to, to + 3600}}) {
		t.Fatal(missing, err)
	}
	if _, err := r.MissingPeriods(to, from); err == nil {
		t.Fatal("invalid range accepted")
	}
}

func TestSourceSetRejectsUnsafeSources(t *testing.T) {
	a := sourceSetTestArtifact(t, 9, 1_000, false, "Asia/Shanghai")
	b := sourceSetTestArtifact(t, 10, 1_000, false, "UTC")
	bad := a
	bad.Statement.SourceNanoUSD++
	overlap := sourceSetTestArtifact(t, 10, 1_000, false, "Asia/Shanghai")
	overlap.Statement.BillingPeriodStart--
	var err error
	overlap.Statement.StatementID, err = statementDigest(overlap.Statement)
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]Artifact{nil, {a, a}, {a, b}, {bad}, {a, overlap}, make([]Artifact, MaxSourceSetArtifacts+1)}
	for i, artifacts := range tests {
		if _, err := NewSourceSet(artifacts); err == nil {
			t.Fatalf("unsafe source set %d accepted", i)
		}
	}
}

func TestSourceSetStrictBoundedRoundTrip(t *testing.T) {
	a := sourceSetTestArtifact(t, 9, 1_000, false, "Asia/Shanghai")
	set, err := NewSourceSet([]Artifact{a})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := EncodeSourceSet(&buf, set); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadSourceSet(bytes.NewReader(buf.Bytes()))
	if err != nil || !reflect.DeepEqual(set, decoded) {
		t.Fatal("round trip failed", err)
	}
	encoded := buf.String()
	for _, input := range []string{encoded + "{}", strings.TrimSpace(encoded) + ",", `{"schema_version":1,"kind":"cur_source_set","unknown":1}`, encoded + strings.Repeat(" ", maxSourceSetBytes-len(encoded)+1)} {
		if _, err := ReadSourceSet(strings.NewReader(input)); err == nil {
			t.Fatal("unsafe input accepted")
		}
	}
	if _, err := ReadSourceSet(strings.NewReader(encoded + strings.Repeat(" ", maxSourceSetBytes-len(encoded)))); err != nil {
		t.Fatal("exact size limit rejected", err)
	}
	if _, err := ReadSourceSet(nil); err == nil {
		t.Fatal("nil input accepted")
	}
	if err := EncodeSourceSet(nil, set); err == nil {
		t.Fatal("nil output accepted")
	}
	// The old single-month reader remains strict, preventing accidental use of
	// this new envelope by a deployment that has not enabled multi-source logic.
	if _, err := ReadArtifact(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("old reader accepted source set")
	}
	legacy, _ := json.Marshal(a)
	if _, err := ReadSourceSet(bytes.NewReader(legacy)); err == nil {
		t.Fatal("source-set reader silently coerced old artifact")
	}
}

func TestVerifyStatementRejectsRehashedButUnreconciledAmounts(t *testing.T) {
	a := sourceSetTestArtifact(t, 9, 1_000, true, "Asia/Shanghai")
	a.Statement.SourceNanoUSD++
	a.Statement.NexusAPINanoUSD++
	a.Statement.TotalAbsNanoUSD++
	a.Statement.AllocatedAbsNanoUSD++
	var err error
	a.Statement.StatementID, err = statementDigest(a.Statement)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(a); err == nil {
		t.Fatal("self-consistent hashes accepted divergent statement and timeline amounts")
	}
}
