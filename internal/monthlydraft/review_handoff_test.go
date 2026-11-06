package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Explicit synthetic metadata, never a production classifier or approval.
func handoffContext(t *testing.T, f generationFixture) []byte {
	t.Helper()
	c := monitor.MonthlyBundleContext{Version: 1, InputSHA256: generationHash(f.raw),
		Groups: []monitor.MonthlyBundleGroup{{GroupID: 1, Name: "synthetic org", Source: "fixture#scope", Months: []string{"2026-09"}}}}
	for _, a := range f.input.Accounts {
		c.Groups[0].Members = append(c.Groups[0].Members, monitor.MonthlyBundleMember{UserID: a.UserID, Username: a.Username})
		meta := monitor.MonthlyBundleAccountContext{UserID: a.UserID, WalletOnly: true, WalletSource: "fixture#wallet", Events: []monitor.MonthlyBundleEvent{}}
		for _, movement := range a.Movements {
			kind, method := "recharge", "后台充值"
			if movement.DeltaQuota < 0 {
				kind, method = "adjustment", "余额扣减"
			}
			meta.Events = append(meta.Events, monitor.MonthlyBundleEvent{ID: movement.ID, At: movement.At, DeltaQuota: movement.DeltaQuota, Kind: kind, Method: method, Source: movement.Source})
		}
		c.Accounts = append(c.Accounts, meta)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestGenerationReviewHandoffExactReadOnlyAndExplicitReview(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	job := generationSubmit(t, f)
	meta := handoffContext(t, f)
	if out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta); err == nil || len(out.Bundle) != 0 {
		t.Fatal("queued task exported or silently executed", err)
	}
	if _, err := f.g.Run(ctx, job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		t.Fatal("handoff reran generation calculation")
		return Review{}, nil
	}
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta)
	if err != nil {
		bundle, detailErr := monitor.PrepareMonthlyCandidateBundle(f.raw, meta, time.Now())
		t.Fatalf("handoff: %v; synthetic candidate: %+v / %v", err, bundle.Entries, detailErr)
	}
	again, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta)
	if err != nil || !reflect.DeepEqual(out, again) || !bytes.Equal(out.Input, f.raw) || !bytes.Equal(out.Context, meta) {
		t.Fatal("handoff not deterministic or mixed input", err)
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("handoff wrote receiver", err)
	}
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("handoff wrote funding", err)
	}
	if bytes.Contains(out.Bundle, []byte("reviewed_by")) || bytes.Contains(out.Bundle, []byte("reviewed_at")) {
		t.Fatal("invented approval")
	}
	if _, err := monitor.ReviewMonthlyCandidate(out.Input, out.Context, out.Bundle, []byte(`{}`), time.Now()); err == nil {
		t.Fatal("missing review accepted")
	}
	var bundle monitor.MonthlyPreparationBundle
	if err := json.Unmarshal(out.Bundle, &bundle); err != nil {
		t.Fatal(err)
	}
	users := bundle.Entries[0].Candidate.Users
	if len(users) != 2 || users[0].ClosingQuota != 104000000 || users[1].ClosingQuota != 899000000 || users[0].ConsumeQuota != 1500000 || users[0].RefundQuota != 500000 {
		t.Fatal("candidate lost per-account money")
	}
	// Output is private copied data; caller mutation cannot corrupt retained input.
	out.Input[0] = 'x'
	out.Context[0] = 'x'
	out.Bundle[0] = 'x'
	unchanged, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta)
	if err != nil || !reflect.DeepEqual(unchanged, again) {
		t.Fatal("returned bytes aliased storage", err)
	}
}

func TestGenerationReviewHandoffRejectsContextAndStaleEvidence(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	job := generationSubmit(t, f)
	if _, err := f.g.Run(ctx, job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	meta := handoffContext(t, f)
	for name, mutate := range map[string]func(*monitor.MonthlyBundleContext){
		"foreign group":        func(c *monitor.MonthlyBundleContext) { c.Groups[0].GroupID = 2 },
		"other month":          func(c *monitor.MonthlyBundleContext) { c.Groups[0].Months[0] = "2026-08" },
		"missing member":       func(c *monitor.MonthlyBundleContext) { c.Groups[0].Members = c.Groups[0].Members[:1] },
		"missing wallet basis": func(c *monitor.MonthlyBundleContext) { c.Accounts[0].WalletOnly = false },
		"wrong movement":       func(c *monitor.MonthlyBundleContext) { c.Accounts[0].Events[0].DeltaQuota++ },
		"multiple months":      func(c *monitor.MonthlyBundleContext) { c.Groups[0].Months = append(c.Groups[0].Months, "2026-08") },
	} {
		t.Run(name, func(t *testing.T) {
			var c monitor.MonthlyBundleContext
			if err := json.Unmarshal(meta, &c); err != nil {
				t.Fatal(err)
			}
			mutate(&c)
			raw, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			if out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, raw); err == nil || len(out.Input) != 0 || len(out.Bundle) != 0 {
				t.Fatal("invalid context returned material", err)
			}
		})
	}
	foreign := job.Request.Owner
	foreign.GroupID++
	if _, err := f.g.PrepareReviewHandoff(ctx, foreign, job.ID, meta); !errors.Is(err, monthlytasks.ErrNotFound) {
		t.Fatal("foreign owner accessed task", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if out, err := f.g.PrepareReviewHandoff(cancelled, job.Request.Owner, job.ID, meta); err == nil || len(out.Bundle) != 0 {
		t.Fatal("cancelled handoff succeeded", err)
	}
	if err := os.WriteFile(f.input.Sources[0].Path, []byte("synthetic changed evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta); !errors.Is(err, ErrGenerationIntegrity) || len(out.Bundle) != 0 {
		t.Fatal("replaced evidence accepted", err)
	}
}

func TestGenerationReviewHandoffBoundedAndRechecksAuthority(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	job := generationSubmit(t, f)
	if _, err := f.g.Run(ctx, job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	meta := handoffContext(t, f)
	for _, raw := range [][]byte{nil, bytes.Repeat([]byte("x"), monitor.MonthlyPreparationInputLimit+1), append([]byte(`{"version":1,`), meta[1:]...)} {
		if out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, raw); err == nil || len(out.Bundle) != 0 {
			t.Fatal("invalid envelope returned", err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error {
		if first {
			first = false
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { _, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		<-done
		t.Fatal("handoff never started")
	}
	_, busyErr := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(busyErr, ErrGenerationBusy) {
		t.Fatal("unbounded concurrent handoff", busyErr)
	}
	transient := errors.New("synthetic temporary authorization read failure")
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return transient }
	if out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta); !errors.Is(err, transient) || len(out.Bundle) != 0 {
		t.Fatal("transient read accepted", err)
	}
	stored, err := f.g.tasks.Get(ctx, job.Request.Owner, job.ID)
	if err != nil || stored.State != monthlytasks.DraftReady {
		t.Fatal("transient read invalidated draft", err)
	}
	calls := 0
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error {
		calls++
		if calls == 4 {
			return ErrGenerationChanged
		}
		return nil
	}
	if out, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta); !errors.Is(err, ErrGenerationChanged) || len(out.Bundle) != 0 || calls != 4 {
		t.Fatal("final authority change released stale handoff", err, calls)
	}
	stored, err = f.g.tasks.Get(ctx, job.Request.Owner, job.ID)
	if err != nil || stored.State != monthlytasks.Invalidated {
		t.Fatal("changed authority did not invalidate", err)
	}
}

func TestGenerationReviewHandoffArchivedReadDoesNotRestore(t *testing.T) {
	f, old, _, receipt, asOf := completedArchiveFixture(t)
	ctx := context.Background()
	meta := handoffContext(t, f)
	want, err := f.g.PrepareReviewHandoff(ctx, old.Request.Owner, old.ID, meta)
	if err != nil {
		t.Fatal(err)
	}
	completedArchiveEvict(t, f, old, receipt, asOf)
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(generationDB(t, f.receiverPath, true), f.funding, f.root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	got, err := g.PrepareReviewHandoff(ctx, old.Request.Owner, old.ID, meta)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("archived candidate changed", err)
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil || !bytes.Equal(before, after) || evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 {
		t.Fatal("archived handoff wrote or restored payload", err)
	}
}
