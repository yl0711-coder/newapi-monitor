package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"
)

func fundingRunTestLimits() FundingCaptureLimits {
	return FundingCaptureLimits{MaxPages: 2, Interval: time.Second, Timeout: 10 * time.Second}
}

func TestFundingRunBudgetRestartAndTerminalNoSourceRead(t *testing.T) {
	source, driver, scan := fundingTestSource(t, 201)
	receiver, path := fundingTestStore(t)
	limits := fundingRunTestLimits()
	limits.MaxPages = 1
	waits := 0
	wait := func(ctx context.Context, d time.Duration) error {
		waits++
		if d != time.Second || source.Stats().InUse != 0 || receiver.Stats().InUse != 0 {
			t.Fatal("pacing changed or transaction held while waiting")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("missing run deadline")
		}
		return ctx.Err()
	}
	first, err := runFundingCapture(context.Background(), source, receiver, scan, limits, wait)
	if err != nil || first.PagesSaved != 1 || first.RowsSaved != 200 || !first.BudgetEnded || first.EndReached || first.HeadID == "" || waits != 1 {
		t.Fatal("page budget not enforced", first, err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	driver.s.mu.Lock()
	driver.s.data = driver.s.data[200:]
	driver.s.mu.Unlock()
	second, err := runFundingCapture(context.Background(), source, receiver, scan, limits, wait)
	if err != nil || second.PagesSaved != 1 || second.RowsSaved != 1 || !second.EndReached || second.BudgetEnded || second.HeadID == first.HeadID || waits != 2 {
		t.Fatal("restart did not resume persisted cursor", second, err)
	}
	driver.s.mu.Lock()
	queries := len(driver.s.queries)
	driver.s.mu.Unlock()
	finished, err := RunFundingCapture(context.Background(), source, receiver, scan, limits)
	if err != nil || finished.HeadID != second.HeadID || !finished.EndReached || finished.PagesSaved != 0 || finished.RowsSaved != 0 {
		t.Fatal("terminal traversal restarted", finished, err)
	}
	driver.s.mu.Lock()
	defer driver.s.mu.Unlock()
	if len(driver.s.queries) != queries {
		t.Fatal("terminal traversal queried source")
	}
}

func TestFundingRunFailureStopsAndDurableHeadDoesNotAdvance(t *testing.T) {
	for _, kind := range []string{"source", "receiver", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			source, driver, scan := fundingTestSource(t, 201)
			receiver, _ := fundingTestStore(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := 0
			wait := func(ctx context.Context, _ time.Duration) error {
				waits++
				if waits == 2 {
					driver.s.mu.Lock()
					driver.s.data = driver.s.data[200:]
					if kind == "source" {
						driver.s.queryErr = errors.New("synthetic source failure")
					}
					driver.s.mu.Unlock()
					if kind == "receiver" {
						if _, err := receiver.Exec("CREATE TRIGGER runner_fail BEFORE INSERT ON portal_monthly_funding_records WHEN NEW.log_id=201 BEGIN SELECT RAISE(ABORT,'synthetic failure'); END"); err != nil {
							t.Fatal(err)
						}
					}
					if kind == "cancel" {
						cancel()
					}
				}
				return ctx.Err()
			}
			got, err := runFundingCapture(ctx, source, receiver, scan, fundingRunTestLimits(), wait)
			if err == nil || got.PagesSaved != 1 || got.RowsSaved != 200 || got.EndReached || got.BudgetEnded || waits != 2 {
				t.Fatal("failure retried or unsaved progress reported", got, err)
			}
			last, err := LoadLatestFundingPage(context.Background(), receiver, scan.ID)
			if err != nil || last.Next.ID != 200 || last.EndReached {
				t.Fatal("failure advanced checkpoint", err)
			}
			var rows int
			if err := receiver.QueryRow("SELECT count(*) FROM portal_monthly_funding_records").Scan(&rows); err != nil || rows != 200 {
				t.Fatal("partial local writes", rows, err)
			}
			if kind == "receiver" {
				if _, err := receiver.Exec("DROP TRIGGER runner_fail"); err != nil {
					t.Fatal(err)
				}
			}
			driver.s.mu.Lock()
			driver.s.queryErr = nil
			driver.s.mu.Unlock()
			// A separate explicit invocation recovers, not an internal retry.
			recovered, err := runFundingCapture(context.Background(), source, receiver, scan, fundingRunTestLimits(), func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
			if err != nil || recovered.PagesSaved != 1 || recovered.RowsSaved != 1 || !recovered.EndReached {
				t.Fatal("explicit recovery failed", recovered, err)
			}
		})
	}
}

func TestFundingRunRejectsUnsafeLimitsAndScopeBeforeSourceRead(t *testing.T) {
	source, driver, scan := fundingTestSource(t, 1)
	receiver, _ := fundingTestStore(t)
	for _, limits := range []FundingCaptureLimits{
		{}, {0, time.Second, time.Second}, {11, time.Second, time.Second},
		{1, time.Millisecond, time.Second}, {1, 11 * time.Second, time.Second},
		{1, time.Second, 0}, {1, time.Second, 31 * time.Second},
	} {
		if _, err := RunFundingCapture(context.Background(), source, receiver, scan, limits); err == nil {
			t.Fatal("unsafe capture budget accepted", limits)
		}
	}
	limits := fundingRunTestLimits()
	if _, err := RunFundingCapture(context.Background(), source, source, scan, limits); err == nil {
		t.Fatal("same source and receiver accepted")
	}
	bad := scan
	bad.To = bad.From + 3601
	if _, err := RunFundingCapture(context.Background(), source, receiver, bad, limits); err == nil {
		t.Fatal("unbounded time window accepted")
	}
	page, err := CaptureFundingPage(context.Background(), source, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SaveFundingPage(context.Background(), receiver, page); err != nil {
		t.Fatal(err)
	}
	driver.s.mu.Lock()
	queries := len(driver.s.queries)
	driver.s.mu.Unlock()
	for _, change := range []func(*FundingScan){
		func(s *FundingScan) { s.UserIDs = []int64{201} },
		func(s *FundingScan) { s.From++ },
		func(s *FundingScan) { s.SourceIdentity = digest([]byte("different")) },
	} {
		bad := scan
		change(&bad)
		if _, err := RunFundingCapture(context.Background(), source, receiver, bad, limits); err == nil {
			t.Fatal("changed resume scope accepted")
		}
	}
	driver.s.mu.Lock()
	defer driver.s.mu.Unlock()
	if len(driver.s.queries) != queries {
		t.Fatal("rejected scope accessed source")
	}
	last, err := LoadLatestFundingPage(context.Background(), receiver, scan.ID)
	if err != nil || !reflect.DeepEqual(last, page) {
		t.Fatal("rejected scope changed saved page", err)
	}
}

func TestFundingRunPacingCancellationAndConcurrentRejection(t *testing.T) {
	source, driver, scan := fundingTestSource(t, 0)
	receiver, _ := fundingTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	got, err := RunFundingCapture(ctx, source, receiver, scan, fundingRunTestLimits())
	if !errors.Is(err, context.DeadlineExceeded) || got.PagesSaved != 0 {
		t.Fatal("real pacing ignored cancellation", got, err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := runFundingCapture(context.Background(), source, receiver, scan, fundingRunTestLimits(), func(context.Context, time.Duration) error {
			close(entered)
			<-release
			return context.Canceled
		})
		result <- err
	}()
	<-entered
	_, busy := RunFundingCapture(context.Background(), source, receiver, scan, fundingRunTestLimits())
	close(release)
	if !errors.Is(busy, ErrFundingRunBusy) || !errors.Is(<-result, context.Canceled) {
		t.Fatal("concurrent run not rejected")
	}
	driver.s.mu.Lock()
	defer driver.s.mu.Unlock()
	if len(driver.s.queries) != 0 {
		t.Fatal("cancelled/busy invocation queried source")
	}
}

func TestFundingRunEmptyObservationRequiresSuccessfulReadAndSave(t *testing.T) {
	source, driver, scan := fundingTestSource(t, 0)
	receiver, _ := fundingTestStore(t)
	wait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	driver.s.mu.Lock()
	driver.s.queryErr = errors.New("synthetic unavailable source")
	driver.s.mu.Unlock()
	failed, err := runFundingCapture(context.Background(), source, receiver, scan, fundingRunTestLimits(), wait)
	if err == nil || failed.EndReached || failed.HeadID != "" || failed.PagesSaved != 0 {
		t.Fatal("unavailable source became empty completed observation", failed, err)
	}
	if _, err := LoadLatestFundingPage(context.Background(), receiver, scan.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("failed first page left a checkpoint", err)
	}
	driver.s.mu.Lock()
	driver.s.queryErr = nil
	driver.s.mu.Unlock()
	got, err := runFundingCapture(context.Background(), source, receiver, scan, fundingRunTestLimits(), wait)
	if err != nil || !got.EndReached || got.PagesSaved != 1 || got.RowsSaved != 0 || got.HeadID == "" || got.BudgetEnded {
		t.Fatal("successful empty observation not durably distinguished", got, err)
	}
}

func TestFundingRunMissingReceiverDoesNotInitializeOrReadSource(t *testing.T) {
	source, driver, scan := fundingTestSource(t, 0)
	receiver, _ := walletTestStore(t) // No funding schema.
	if _, err := RunFundingCapture(context.Background(), source, receiver, scan, fundingRunTestLimits()); err == nil {
		t.Fatal("missing receiver schema accepted")
	}
	var tables int
	if err := receiver.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'portal_monthly_funding_%'").Scan(&tables); err != nil || tables != 0 {
		t.Fatal("runner initialized receiver", tables, err)
	}
	driver.s.mu.Lock()
	defer driver.s.mu.Unlock()
	if len(driver.s.queries) != 0 {
		t.Fatal("missing receiver still queried source")
	}
}

func TestFundingRunOwnDeadlineStopsWait(t *testing.T) {
	source, driver, scan := fundingTestSource(t, 0)
	receiver, _ := fundingTestStore(t)
	// The run's own deadline (not a caller timeout) stops before its first query.
	got, err := RunFundingCapture(context.Background(), source, receiver, scan, FundingCaptureLimits{MaxPages: 1, Interval: 2 * time.Second, Timeout: time.Second})
	if !errors.Is(err, context.DeadlineExceeded) || got.PagesSaved != 0 || got.EndReached {
		t.Fatal("run timeout ignored", got, err)
	}
	driver.s.mu.Lock()
	defer driver.s.mu.Unlock()
	if len(driver.s.queries) != 0 {
		t.Fatal("run queried source after deadline")
	}
}
