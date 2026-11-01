package monthlytasks

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestTaskFindReadOnlyExactScopeAndLifecycle(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	req := requestFixture()
	if got, err := f.store.Find(ctx, req); !errors.Is(err, ErrNotFound) || got != (Task{}) {
		t.Fatal("missing task was not empty", err)
	}
	job := submit(t, f.store, req)
	ro, err := sql.Open("sqlite", "file:"+f.path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	reader, err := New(ro)
	if err != nil {
		t.Fatal(err)
	}
	check := func(want Task) {
		t.Helper()
		before, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatal(err)
		}
		for range 3 {
			got, err := reader.Find(ctx, req)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("lookup changed task", got, err)
			}
		}
		after, err := os.ReadFile(f.path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("lookup wrote receiver", err)
		}
	}
	check(job)
	_, lease, err := f.store.Claim(ctx, req.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	running, err := f.store.Get(ctx, req.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	check(running)
	if err := f.store.Complete(ctx, lease, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
	ready, err := f.store.Get(ctx, req.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	check(ready)
	if err := f.store.Invalidate(ctx, req.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	invalid, err := f.store.Get(ctx, req.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	check(invalid)
	for _, change := range []func(*Request){
		func(r *Request) { r.Owner.GroupID++ }, func(r *Request) { r.Owner.AuthVersion++ },
		func(r *Request) { r.Owner.MemberManifest = strings.Repeat("f", 64) }, func(r *Request) { r.Month = "2026-08" },
		func(r *Request) { r.InputSHA256 = strings.Repeat("f", 64) }, func(r *Request) { r.SourceIdentity = strings.Repeat("f", 64) }, func(r *Request) { r.RulesSHA256 = strings.Repeat("f", 64) },
	} {
		other := req
		change(&other)
		if got, err := reader.Find(ctx, other); !errors.Is(err, ErrNotFound) || got != (Task{}) {
			t.Fatal("other scope matched task", err)
		}
	}
}

func TestTaskFindRejectsCancellationAndCorruption(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	req := requestFixture()
	_ = submit(t, f.store, req)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if got, err := f.store.Find(cancelled, req); !errors.Is(err, context.Canceled) || got != (Task{}) {
		t.Fatal("cancelled read returned data", err)
	}
	if _, err := f.db.Exec(`UPDATE portal_monthly_draft_tasks SET descriptor='{}'`); err != nil {
		t.Fatal(err)
	}
	if got, err := f.store.Find(ctx, req); !errors.Is(err, ErrCorrupt) || got != (Task{}) {
		t.Fatal("corrupt task returned data", err)
	}
	invalid := req
	invalid.Month = "../secret"
	if got, err := f.store.Find(ctx, invalid); !errors.Is(err, ErrInvalid) || got != (Task{}) {
		t.Fatal("invalid descriptor accepted", err)
	}
}
