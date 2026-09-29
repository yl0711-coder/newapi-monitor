package monitor

import (
	"context"
	"testing"
	"time"
)

func TestFinanceQueuePublicationRaceBoundedRetryAndRecovery(t *testing.T) {
	var q financeReportQueue
	defer q.shutdown(context.Background())
	fail := func(context.Context) error { return financeFactChange("report-publication", "source-fingerprint") }
	for attempt := 1; attempt <= financeReportRaceLimit; attempt++ {
		if state := q.submit(context.Background(), "report", false, fail); state != "queued" {
			t.Fatal(state)
		}
		waitFinanceQueue(t, &q)
		want := "retrying"
		if attempt == financeReportRaceLimit {
			want = "failed"
		}
		if state := q.submit(context.Background(), "report", true, fail); state != want {
			t.Fatalf("attempt=%d state=%s", attempt, state)
		}
		s := q.stats()
		if attempt < financeReportRaceLimit && (s.Retrying != 1 || s.UnresolvedFailures != 0) {
			t.Fatal(s)
		}
		if attempt == financeReportRaceLimit && s.UnresolvedFailures != 1 {
			t.Fatal("persistent race was hidden", s)
		}
		q.mu.Lock()
		q.jobs["report"].finished = time.Now().Add(-2 * financeReportVerifyInterval)
		q.mu.Unlock()
	}
	q.submit(context.Background(), "report", false, func(context.Context) error { return nil })
	waitFinanceQueue(t, &q)
	if s := q.stats(); s.UnresolvedFailures != 0 || s.Retrying != 0 {
		t.Fatal(s)
	}
}
