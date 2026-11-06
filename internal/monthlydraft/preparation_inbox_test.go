package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func writePreparationInboxTest(t *testing.T, root, name string, entries ...PreparedInputInboxEntry) {
	t.Helper()
	if entries == nil {
		entries = []PreparedInputInboxEntry{}
	}
	raw, err := json.Marshal(PreparedInputInbox{Version: 1, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func preparationInboxFixture(t *testing.T) (generationFixture, DeploymentConfig, PreparedInputInboxEntry) {
	t.Helper()
	f, cfg, request := automaticPreparationFileFixture(t)
	request.Write = true
	receipt, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, request)
	if err != nil || !receipt.Complete {
		t.Fatal(receipt, err)
	}
	return f, cfg, PreparedInputInboxEntry{GroupID: f.plan.Owner.GroupID, Month: f.plan.Funding.Month,
		Revision: 1, Directory: "prepared", ManifestSHA256: receipt.ManifestSHA256}
}

func TestPreparationInboxCanonicalIndexAndScope(t *testing.T) {
	entry := PreparedInputInboxEntry{GroupID: 1, Month: "2026-09", Revision: 1, Directory: "prepared", ManifestSHA256: strings.Repeat("a", 64)}
	scope := &generationScope{exact: map[GenerationTarget]struct{}{{GroupID: 1, Month: "2026-09"}: {}}}
	raw, _ := json.Marshal(PreparedInputInbox{Version: 1, Entries: []PreparedInputInboxEntry{entry}})
	if _, err := decodePreparationInbox(raw, scope); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		[]byte(`{}`), []byte(`{"version":1,"entries":null}`),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"Version":1`), 1),
		bytes.Replace(raw, []byte(`"revision":1`), []byte(`"revision":0`), 1),
		bytes.Replace(raw, []byte(`"directory":"prepared"`), []byte(`"directory":"../prepared"`), 1),
		bytes.Replace(raw, []byte(`"expected_head":""`), []byte(`"unknown":""`), 1),
		bytes.Replace(raw, []byte(`"group_id":1`), []byte(`"group_id":2`), 1),
		bytes.Repeat([]byte("x"), (64<<10)+1),
	} {
		if _, err := decodePreparationInbox(bad, scope); err == nil {
			t.Fatal("invalid index accepted")
		}
	}
	dup, _ := json.Marshal(PreparedInputInbox{Version: 1, Entries: []PreparedInputInboxEntry{entry, entry}})
	if _, err := decodePreparationInbox(dup, scope); err == nil {
		t.Fatal("duplicate target accepted")
	}
	if _, err := decodePreparationInbox(raw, nil); err == nil {
		t.Fatal("legacy unbounded scope accepted")
	}
}

func TestPreparationInboxRetryFairnessPauseAndExplicitRecovery(t *testing.T) {
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	a := PreparedInputInboxEntry{GroupID: 1, Month: "2026-09", Revision: 1, Directory: "a", ManifestSHA256: strings.Repeat("a", 64)}
	b := a
	b.GroupID, b.Directory = 2, "b"
	d := &Deployment{g: &Generation{root: root}, runtime: &LocalRuntime{scope: &generationScope{exact: map[GenerationTarget]struct{}{
		{GroupID: 1, Month: a.Month}: {}, {GroupID: 2, Month: a.Month}: {},
	}}}}
	p := &preparationInbox{name: "inbox.json", sourceID: "scope"}
	writePreparationInboxTest(t, path, p.name, a, b)
	now := time.Now()
	calls := map[int64]int{}
	busy := true
	receive := func(_ context.Context, e PreparedInputInboxEntry, _ string) (PreparationResult, error) {
		calls[e.GroupID]++
		if e.GroupID == 1 && busy {
			return PreparationResult{}, ErrGenerationBusy
		}
		return PreparationResult{Applied: true}, nil
	}
	ctx := context.Background()
	p.step(ctx, d, now, receive) // a busy
	p.step(ctx, d, now, receive) // b still gets its turn
	p.step(ctx, d, now, receive) // a not yet eligible
	if calls[1] != 1 || calls[2] != 1 {
		t.Fatal("busy target starved others or immediate retry", calls)
	}
	p.step(ctx, d, now.Add(31*time.Second), receive)
	p.step(ctx, d, now.Add(150*time.Second), receive)
	if calls[1] != 2 {
		t.Fatal("retry ignored second backoff", calls)
	}
	p.step(ctx, d, now.Add(152*time.Second), receive)
	p.step(ctx, d, now.Add(24*time.Hour), receive)
	key := GenerationTarget{GroupID: 1, Month: a.Month}
	if calls[1] != 3 || p.states[key].status != "paused" {
		t.Fatal("unbounded retry", calls, p.states[key])
	}
	busy = false
	a.Revision++
	writePreparationInboxTest(t, path, p.name, a, b)
	p.step(ctx, d, now.Add(24*time.Hour), receive)
	if calls[1] != 4 || calls[2] != 1 || p.states[key].status != "selected" {
		t.Fatal("explicit recovery reset unrelated entry or failed", calls)
	}
	// A partial/malformed index never activates a target or clears its state.
	if err := os.WriteFile(filepath.Join(path, p.name), []byte(`{"version":`), 0600); err != nil {
		t.Fatal(err)
	}
	p.step(ctx, d, now.Add(48*time.Hour), receive)
	if calls[1] != 4 || calls[2] != 1 || len(p.states) != 2 {
		t.Fatal("invalid index caused work", calls)
	}
	writePreparationInboxTest(t, path, p.name)
	p.step(ctx, d, now, receive)
	if len(p.states) != 0 {
		t.Fatal("empty index did not pause all pending work")
	}
	writePreparationInboxTest(t, path, p.name, a)
	p.step(ctx, d, now, func(context.Context, PreparedInputInboxEntry, string) (PreparationResult, error) {
		return PreparationResult{}, ErrGenerationIntegrity
	})
	if p.states[key].status != "paused" || p.states[key].attempts != 1 {
		t.Fatal("integrity failure was retried")
	}
}

func TestPreparationInboxReceiveRestartIdempotenceAndWithdrawal(t *testing.T) {
	f, cfg, entry := preparationInboxFixture(t)
	ctx := context.Background()
	open := func() *Deployment {
		t.Helper()
		d, err := OpenDeployment(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeDeploymentTest(t, d) })
		if err := d.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return d
	}
	d := open()
	before := map[string]string{}
	for _, path := range []string{cfg.FundingPath, cfg.AuthorityPath} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	first, err := d.receivePreparedInput(ctx, entry, syntheticScopeID)
	if err != nil || !first.Applied || !first.Changed {
		t.Fatal(first, err)
	}
	preparationCounts(t, f, 1, 1)
	closeDeploymentTest(t, d)
	d = open()
	again, err := d.receivePreparedInput(ctx, entry, syntheticScopeID)
	if err != nil || !again.Applied || again.Changed || again.SelectionHead != first.SelectionHead {
		t.Fatal("restart blindly replayed original expected head", again, err)
	}
	preparationCounts(t, f, 1, 1)
	withdrawn, err := WithdrawGenerationSelection(ctx, f.receiver, entry.GroupID, entry.Month, first.SelectionHead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.receivePreparedInput(ctx, entry, syntheticScopeID); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal("old index revived withdrawn input", err)
	}
	entry.ExpectedHead = withdrawn.Head
	entry.Revision++
	if _, err := d.receivePreparedInput(ctx, entry, syntheticScopeID); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal("new retry revision revived withdrawn input", err)
	}
	preparationCounts(t, f, 1, 2)
	for path, want := range before {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != want {
			t.Fatal("source/authority database changed", err)
		}
	}
}

func TestPreparationInboxRejectsCorruptionWithoutWrites(t *testing.T) {
	f, cfg, entry := preparationInboxFixture(t)
	d, err := OpenDeployment(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"manifest.json", "plan.json", "input.json", "scope.json"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(f.root, entry.Directory, name)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}()
			if err := os.WriteFile(path, append(raw, ' '), 0600); err != nil {
				t.Fatal(err)
			}
			if result, err := d.receivePreparedInput(context.Background(), entry, syntheticScopeID); err == nil || result.Applied {
				t.Fatal("corrupt package selected", name, result, err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
	for _, kind := range []string{"wrong_scope", "public_directory", "symlink_directory", "missing_manifest", "public_file", "symlink_file"} {
		t.Run(kind, func(t *testing.T) {
			changed := entry
			source := syntheticScopeID
			dir := filepath.Join(f.root, entry.Directory)
			manifest := filepath.Join(dir, "manifest.json")
			switch kind {
			case "wrong_scope":
				source = "different-scope"
			case "public_directory":
				if err := os.Chmod(dir, 0755); err != nil {
					t.Fatal(err)
				}
				defer os.Chmod(dir, 0700) //nolint:errcheck // cleanup only
			case "symlink_directory":
				changed.Directory = "alias"
				if err := os.Symlink(entry.Directory, filepath.Join(f.root, "alias")); err != nil {
					t.Fatal(err)
				}
			case "missing_manifest":
				if err := os.Rename(manifest, manifest+".hold"); err != nil {
					t.Fatal(err)
				}
				defer os.Rename(manifest+".hold", manifest) //nolint:errcheck // cleanup only
			case "public_file":
				if err := os.Chmod(manifest, 0644); err != nil {
					t.Fatal(err)
				}
				defer os.Chmod(manifest, 0600) //nolint:errcheck // cleanup only
			case "symlink_file":
				if err := os.Rename(manifest, manifest+".hold"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("manifest.json.hold", manifest); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.Remove(manifest); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(manifest+".hold", manifest); err != nil {
						t.Fatal(err)
					}
				}()
			}
			if result, err := d.receivePreparedInput(context.Background(), changed, source); err == nil || result.Applied {
				t.Fatal("unsafe package selected", kind, result, err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
	if _, err := d.receiver.Exec(`CREATE TRIGGER inbox_failure BEFORE INSERT ON portal_monthly_generation_selections BEGIN SELECT RAISE(ABORT,'synthetic'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.receivePreparedInput(context.Background(), entry, syntheticScopeID); err == nil {
		t.Fatal("failed transaction accepted")
	}
	preparationCounts(t, f, 0, 0)
	if _, err := d.receiver.Exec(`DROP TRIGGER inbox_failure`); err != nil {
		t.Fatal(err)
	}
	if result, err := d.receivePreparedInput(context.Background(), entry, syntheticScopeID); err != nil || !result.Applied {
		t.Fatal("write fault could not recover", result, err)
	}
}

func TestPreparationInboxOptInLoopAndStop(t *testing.T) {
	f, cfg, entry := preparationInboxFixture(t)
	cfg.PreparationInbox = "inbox.json"
	ctx := context.Background()
	if d, err := OpenDeployment(ctx, cfg); err == nil || d != nil {
		t.Fatal("draft mode silently enabled intake")
	}
	writePreparationInboxTest(t, f.root, cfg.PreparationInbox, entry)
	d, err := OpenRetainedAutomaticDeployment(ctx, cfg, syntheticScopeID, func(context.Context, monthlytasks.Request, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	preparationCounts(t, f, 0, 0) // construction must never start intake
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		selected, err := ReadGenerationSelection(ctx, f.receiver, entry.GroupID, entry.Month)
		if err != nil {
			t.Fatal(err)
		}
		if selected.Request != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("optional inbox never selected complete package")
		}
		time.Sleep(10 * time.Millisecond)
	}
	preparationCounts(t, f, 1, 1)
	// Stop must interrupt its 30-second poll wait, join intake, then close.
	closeDeploymentTest(t, d)
	if d.runtime.Phase() != "stopped" || d.receiver.Ping() == nil {
		t.Fatal("intake was not drained before closing resources")
	}
}

func TestPreparationInboxMoneyAndAuthorityAreStillAudited(t *testing.T) {
	for _, kind := range []string{"money", "authority"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, entry := preparationInboxFixture(t)
			d, err := OpenDeployment(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeDeploymentTest(t, d) })
			if err := d.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if kind == "authority" {
				auth := generationDB(t, cfg.AuthorityPath, false)
				if _, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`); err != nil {
					t.Fatal(err)
				}
			} else {
				read := func(name string) []byte {
					t.Helper()
					raw, err := os.ReadFile(filepath.Join(f.root, entry.Directory, name))
					if err != nil {
						t.Fatal(err)
					}
					return raw
				}
				input, err := monitor.DecodeMonthlyPreparationInput(read("input.json"))
				if err != nil {
					t.Fatal(err)
				}
				input.Accounts[0].Movements[0].DeltaQuota++
				inputRaw := draftJSON(t, input)
				var plan GenerationPlan
				var manifest automaticPreparationManifest
				if json.Unmarshal(read("plan.json"), &plan) != nil || json.Unmarshal(read("manifest.json"), &manifest) != nil {
					t.Fatal("could not read synthetic package")
				}
				plan.PreparationSHA256 = generationHash(inputRaw)
				planRaw, err := json.Marshal(plan)
				if err != nil {
					t.Fatal(err)
				}
				manifest.PlanSHA256, manifest.PreparationSHA256 = generationHash(planRaw), generationHash(inputRaw)
				manifestRaw, err := json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				entry.ManifestSHA256 = generationHash(manifestRaw)
				for name, raw := range map[string][]byte{"input.json": inputRaw, "plan.json": planRaw, "manifest.json": manifestRaw} {
					if err := os.WriteFile(filepath.Join(f.root, entry.Directory, name), raw, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if result, err := d.receivePreparedInput(context.Background(), entry, syntheticScopeID); err == nil || result.Applied {
				t.Fatal("hash-consistent but financially/authorization invalid package accepted", result, err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestPreparationInboxBackgroundDrainsBeforeResourceClose(t *testing.T) {
	_, cfg := preparationFixture(t)
	d, err := OpenDeployment(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	if err := d.runtime.start(context.Background(), func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
		<-release // simulate a file operation still unwinding after cancellation
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(release); closeDeploymentTest(t, d) })
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := d.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close did not wait for preparation loop", err)
	}
	if d.receiver.Ping() != nil {
		t.Fatal("SQL handle closed under preparation I/O")
	}
	if another, err := OpenDeployment(context.Background(), cfg); another != nil || !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("process lock released before background drain", err)
	}
}
