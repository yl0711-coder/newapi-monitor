package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func observationMonthFixture(t *testing.T) (string, observationMonthPlan, observationMonthProgress) {
	t.Helper()
	root, _, _ := observationFixture(t, "observe-wallet")
	p := observationMonthPlan{Version: 1, GroupID: 1, Funding: monthlysource.FundingMonthRequest{Month: "2026-02",
		SourceIdentity: strings.Repeat("a", 64), UserIDs: []int64{201},
		Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}}}
	r, err := initializeObservationMonth(context.Background(), root, "observations", p, true)
	if err != nil {
		t.Fatal(err)
	}
	return root, p, r
}

func syntheticMonthObservation(t *testing.T, request observationRequest, now time.Time) []byte {
	t.Helper()
	quota, used := int64(-1), int64(9007199254740993)
	wallet := monthlysource.WalletUser{UserID: request.UserIDs[0], Exists: true, BalanceQuota: &quota, UsedQuota: &used}
	var value any
	if request.Kind == "observe-wallet" {
		users := make([]monthlysource.WalletUser, 0, len(request.UserIDs))
		for _, id := range request.UserIDs {
			u := wallet
			u.UserID = id
			users = append(users, u)
		}
		value = monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary",
			SourceIdentity: request.SourceIdentity, StartedAtMS: now.UnixMilli(), CompletedAtMS: now.UnixMilli(), Users: users}
	} else {
		o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary",
			Request: usageObservationRequest(request), StartedAtMS: now.UnixMilli(), CompletedAtMS: now.UnixMilli(), Wallet: wallet,
			Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
		first, _ := time.ParseInLocation("2006-01", "2026-02", time.FixedZone("UTC+8", 8*3600))
		if request.From == first.Unix() {
			for i, amount := range []int64{9007199254740993, 123} {
				kind := 2
				if i == 1 {
					kind = 6
				}
				row := monthlysource.UsageWindowRow{UserID: wallet.UserID, Type: kind, BillingSource: "wallet", Records: 1, Quota: amount,
					MinQuota: amount, MaxQuota: amount, MinID: int64(i + 1), MaxID: int64(i + 1), FirstAt: request.From + 1, LastAt: request.From + 1}
				o.Totals = append(o.Totals, row)
				o.Rows = append(o.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: row, TokenID: int64(i + 1), Name: "synthetic-token"})
			}
		}
		value = o
	}
	raw, err := json.Marshal(value)
	if err != nil || validateObservation(raw, request) != nil {
		t.Fatal("invalid synthetic observation", err)
	}
	return raw
}

func TestObservationMonthDefaultsScopeAndPrivateInitialization(t *testing.T) {
	root, _, _ := observationFixture(t, "observe-wallet")
	args := []string{"init-observation-month", "-workspace", root, "-batch", "observations", "-group-id", "1",
		"-month", "2026-02", "-source-identity", strings.Repeat("a", 64), "-user-ids", "202,201"}
	var out bytes.Buffer
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("default created files", err)
	}
	out.Reset()
	if err := run(context.Background(), append(args, "-create"), &out); err != nil {
		t.Fatal(err)
	}
	var result observationMonthProgress
	if json.Unmarshal(out.Bytes(), &result) != nil || result.Captures != 114 || result.Completed != 0 || result.Captured {
		t.Fatal("incorrect empty schedule", out.String())
	}
	inspect := []string{"inspect-observation-month", "-workspace", root, "-batch", "observations", "-expected-plan", result.PlanSHA256,
		"-connection-file", "/nonexistent-do-not-read"}
	planPath := filepath.Join(root, "observations", "observation-month.json")
	digest := fileHash(t, planPath)
	out.Reset()
	if err := run(context.Background(), inspect, &out); err != nil || digest != fileHash(t, planPath) {
		t.Fatal("inspection read credentials or changed schedule", err)
	}
	if err := run(context.Background(), append(args, "-create"), &out); err == nil {
		t.Fatal("existing batch replaced")
	}
	if err := run(context.Background(), append(inspect, "-capture"), &out); err == nil {
		t.Fatal("inspect command admitted collection")
	}
	for _, path := range []string{root, filepath.Dir(planPath), planPath} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatal("nonprivate schedule", err)
		}
	}
}

func TestObservationMonthSingleStepResumeWaitAndOriginalTokenAssembly(t *testing.T) {
	root, p, initial := observationMonthFixture(t)
	now := time.Now().Add(-10 * time.Hour).Truncate(time.Millisecond)
	clock := func() time.Time { return now }
	calls := 0
	collect := func(ctx context.Context, connection string, request observationRequest) ([]byte, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 11*time.Second || connection != "/synthetic-only-not-opened" || request.SourceIdentity != p.Funding.SourceIdentity {
			t.Fatal("scope/budget changed")
		}
		return syntheticMonthObservation(t, request, now), nil
	}
	previous := 0
	waited := false
	for previous < initial.Captures {
		before, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "unused", false, collect, clock)
		if err != nil || before.Completed != previous || calls != previous || before.Captured {
			t.Fatal("restart/inspect repeated a capture", err)
		}
		if before.NextEligibleMS > now.UnixMilli() {
			waiting, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "/synthetic-only-not-opened", true, collect, clock)
			if err != nil || waiting.Captured || calls != previous || waiting.Phase != "waiting_for_capture_interval" {
				t.Fatal("waiting window contacted source", err)
			}
			if before.Next == "token-0000-newer" {
				waited = true
			}
			now = time.UnixMilli(before.NextEligibleMS)
		}
		after, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "/synthetic-only-not-opened", true, collect, clock)
		if err != nil || !after.Captured || after.Completed != previous+1 || calls != previous+1 {
			t.Fatal("step did not collect exactly one next window", err)
		}
		previous++
	}
	final, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "/synthetic-only-not-opened", true, collect, clock)
	if err != nil || final.Phase != "captured_needs_assembly_and_audit" || final.Captured || calls != initial.Captures || !waited {
		t.Fatal("complete batch repeated work or bypassed the independent sweep gap", err)
	}
	state, err := inspectObservationMonth(context.Background(), filepath.Join(root, "observations"), initial.PlanSHA256, now)
	if err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(root, "observations", "observation-index.json")
	preview, err := sealObservationMonthIndex(context.Background(), root, "observations", initial.PlanSHA256, false)
	if err != nil || !validSHA(preview.IndexSHA256) {
		t.Fatal("index inspection failed", err)
	}
	if _, err := os.Lstat(indexPath); !os.IsNotExist(err) {
		t.Fatal("index preview wrote files", err)
	}
	created, err := sealObservationMonthIndex(context.Background(), root, "observations", initial.PlanSHA256, true)
	if err != nil || created.IndexSHA256 != preview.IndexSHA256 {
		t.Fatal("index output changed selection", err)
	}
	again, err := sealObservationMonthIndex(context.Background(), root, "observations", initial.PlanSHA256, true)
	if err != nil || again.Mode != "existing_private_observation_index_rechecked" || again.IndexSHA256 != created.IndexSHA256 {
		t.Fatal("index restart was not idempotent", err)
	}
	indexRaw, err := privateRead(indexPath, 2<<20)
	var index observationMonthSelection
	if err != nil || json.Unmarshal(indexRaw, &index) != nil || len(index.TokenUsage) != 28 || len(index.Wallet) != 2 || index.GroupID != p.GroupID ||
		index.PlanSHA256 != initial.PlanSHA256 || hash(indexRaw) != preview.IndexSHA256 {
		t.Fatal("private index lost scope or capture references", err)
	}
	want, err := observationMonthIndex(state)
	if err != nil || !bytes.Equal(want, indexRaw) {
		t.Fatal("index does not match the verified original captures", err)
	}
	selection := tokenMonthAssemblyPlan{Version: 1, SourceIdentity: p.Funding.SourceIdentity, Rules: p.Funding.Rules,
		UserID: 201, Month: p.Funding.Month, Windows: index.TokenUsage}
	raw, _ := json.Marshal(selection)
	result, err := assembleTokenMonth(context.Background(), root, "tokens", selection, raw, true)
	if err != nil || result.Captures != 56 {
		t.Fatal("batch results do not work with the real existing token assembler", err)
	}
	archive, err := os.ReadFile(filepath.Join(root, "tokens", "token-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, report, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), archive, time.Now())
	if err != nil || report.ConsumeQuota != 9007199254740993 || report.RefundQuota != 123 {
		t.Fatal("raw integer amounts/refunds were lost", err)
	}
	if err := os.WriteFile(indexPath, append(indexRaw[:len(indexRaw)-1], []byte(`,"approved":true}`)...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "synthetic", true, collect, clock); err == nil || calls != initial.Captures {
		t.Fatal("changed index was accepted or recaptured source")
	}
}

func TestObservationMonthCalendarScopeBoundsAndNoPartialIndex(t *testing.T) {
	root, p, initial := observationMonthFixture(t)
	for _, tc := range []struct {
		month string
		days  int
	}{{"2024-02", 29}, {"2026-04", 30}, {"2026-01", 31}} {
		q := p
		q.Funding.Month = tc.month
		jobs, err := observationMonthJobs(context.Background(), q)
		if err != nil || len(jobs) != 2*(tc.days+1) || jobs[tc.days-1].Request.To != jobs[tc.days+tc.days].Request.To {
			t.Fatal("calendar coverage changed", tc.month, err)
		}
	}
	for _, kind := range []string{"future", "bad_month", "old_year", "duplicate", "unsorted", "capacity", "rules", "group"} {
		t.Run(kind, func(t *testing.T) {
			q := p
			switch kind {
			case "future":
				q.Funding.Month = time.Now().AddDate(0, 1, 0).Format("2006-01")
			case "bad_month":
				q.Funding.Month = "2026-2"
			case "old_year":
				q.Funding.Month = "2019-12"
			case "duplicate":
				q.Funding.UserIDs = []int64{201, 201}
			case "unsorted":
				q.Funding.UserIDs = []int64{202, 201}
			case "capacity":
				q.Funding.UserIDs = make([]int64, 200)
				for i := range q.Funding.UserIDs {
					q.Funding.UserIDs[i] = int64(i + 1)
				}
			case "rules":
				q.Funding.Rules.QuotaPerUnit = 1
			case "group":
				q.GroupID = 0
			}
			if _, err := observationMonthJobs(context.Background(), q); err == nil {
				t.Fatal("invalid/broadened scope accepted")
			}
		})
	}
	if _, err := sealObservationMonthIndex(context.Background(), root, "observations", initial.PlanSHA256, true); err == nil {
		t.Fatal("incomplete month obtained an index")
	}
	if _, err := os.Lstat(filepath.Join(root, "observations", "observation-index.json")); !os.IsNotExist(err) {
		t.Fatal("incomplete month wrote index", err)
	}
}

func TestObservationMonthActualCLIInitializationAndSourceFreeInspection(t *testing.T) {
	binary := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY")
	if !filepath.IsAbs(binary) {
		t.Skip("explicit compiled local command required")
	}
	root, _, _ := observationFixture(t, "observe-wallet")
	args := []string{"init-observation-month", "-workspace", root, "-batch", "observations", "-group-id", "1",
		"-month", "2026-02", "-source-identity", strings.Repeat("a", 64), "-user-ids", "201", "-create"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, binary, args...).Output()
	var created observationMonthProgress
	if err != nil || json.Unmarshal(raw, &created) != nil || !validSHA(created.PlanSHA256) {
		t.Fatal("actual initialization failed", err)
	}
	for _, cmd := range []string{"inspect-observation-month", "run-observation-month"} {
		args := []string{cmd, "-workspace", root, "-batch", "observations", "-expected-plan", created.PlanSHA256, "-connection-file", "/must-not-read"}
		raw, err := exec.CommandContext(ctx, binary, args...).Output()
		var r observationMonthProgress
		if err != nil || json.Unmarshal(raw, &r) != nil || r.Completed != 0 || r.Captured || r.Next != "token-0000-older" {
			t.Fatal("actual default connected source or lost scope", err)
		}
	}
}

func TestObservationMonthSourceFailureCancellationAndExclusiveAdmission(t *testing.T) {
	root, _, initial := observationMonthFixture(t)
	now := time.Now().Add(-time.Hour)
	clock := func() time.Time { return now }
	calls := 0
	fail := func(context.Context, string, observationRequest) ([]byte, error) {
		calls++
		return nil, context.DeadlineExceeded
	}
	if _, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "synthetic", true, fail, clock); !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatal("source failure was retried or hidden", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := advanceObservationMonth(ctx, root, "observations", initial.PlanSHA256, "synthetic", true, fail, clock); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("canceled step contacted source", err)
	}
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var release sync.Once
	defer release.Do(func() { close(resume) })
	go func() {
		_, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "synthetic", true,
			func(ctx context.Context, _ string, request observationRequest) ([]byte, error) {
				close(entered)
				select {
				case <-resume:
					return syntheticMonthObservation(t, request, now), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}, clock)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not start")
	}
	if _, err := advanceObservationMonth(context.Background(), root, "observations", initial.PlanSHA256, "synthetic", true, fail, clock); !errors.Is(err, errBusy) || calls != 1 {
		t.Fatal("concurrent client entered or attempted source", err)
	}
	release.Do(func() { close(resume) })
	if err := <-done; err != nil {
		t.Fatal("explicit retry did not reuse the unstarted original job", err)
	}
}

func TestObservationMonthTamperPartialForeignScopeAndUnknownFilesBlockCapture(t *testing.T) {
	for _, scenario := range []string{"wrong_hash", "unknown_field", "hardlink", "public", "batch_symlink", "partial", "fork", "unexpected", "changed_request"} {
		t.Run(scenario, func(t *testing.T) {
			root, _, initial := observationMonthFixture(t)
			batch := filepath.Join(root, "observations")
			planPath := filepath.Join(batch, "observation-month.json")
			expected := initial.PlanSHA256
			switch scenario {
			case "wrong_hash":
				expected = strings.Repeat("b", 64)
			case "unknown_field":
				raw, err := os.ReadFile(planPath)
				if err != nil {
					t.Fatal(err)
				}
				raw = append(raw[:len(raw)-1], []byte(`,"complete":true}`)...)
				if err := os.WriteFile(planPath, raw, 0600); err != nil {
					t.Fatal(err)
				}
				expected = hash(raw)
			case "hardlink":
				if err := os.Link(planPath, filepath.Join(root, "plan-alias")); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(planPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "batch_symlink":
				if err := os.Rename(batch, filepath.Join(root, "moved")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "moved"), batch); err != nil {
					t.Fatal(err)
				}
			case "partial", "fork", "unexpected":
				name := "token-0000-older"
				if scenario == "fork" {
					name = "token-0001-older"
				} else if scenario == "unexpected" {
					name = "foreign-batch"
				}
				if err := os.Mkdir(filepath.Join(batch, name), 0700); err != nil {
					t.Fatal(err)
				}
			case "changed_request":
				request := observationRequest{Version: 1, Kind: "observe-token-usage", SourceIdentity: strings.Repeat("a", 64),
					UserIDs: []int64{202}, From: 1769875200, To: 1769961600}
				now := time.Now().Add(-time.Hour)
				writeAssemblyFixtureCapture(t, batch, "token-0000-older", request, syntheticMonthObservation(t, request, now))
			}
			calls := 0
			_, err := advanceObservationMonth(context.Background(), root, "observations", expected, "synthetic", true,
				func(context.Context, string, observationRequest) ([]byte, error) { calls++; return nil, nil }, time.Now)
			if err == nil || calls != 0 {
				t.Fatal("invalid persisted scope contacted source or skipped corrupt work", err)
			}
		})
	}
}
