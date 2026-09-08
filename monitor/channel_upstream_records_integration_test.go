package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordTestServer struct {
	mu           sync.Mutex
	calls        []string
	wrongControl bool
	status       int
	duplicateKey bool
}

func (s *recordTestServer) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	s.calls = append(s.calls, r.Header.Get("Authorization")+"/"+q.Get("group_by")+"/"+q.Get("cursor"))
	if s.status != 0 {
		w.WriteHeader(s.status)
		_, _ = w.Write([]byte(`{"error":"unavailable"}`))
		return
	}
	day := recordTestDay()
	key := 17
	if (r.Header.Get("Authorization") == "Bearer two" || r.Header.Get("Authorization") == "Bearer sk-acw-record-test-two") && !s.duplicateKey {
		key = 18
	}
	if q.Get("group_by") == "day" {
		cost := "1.0"
		if s.wrongControl {
			cost = "1.1"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"api_key_id": key, "period": map[string]string{"start": "2026-09-06", "end": "2026-09-06"}, "group_by": "day", "summary": map[string]any{"cost": cost, "requests": 4, "total_tokens": 100}}})
		return
	}
	if q.Get("limit") != "100" {
		w.WriteHeader(400)
		return
	}
	items := []map[string]any{recordTestItem(day, 1, 0, "0.1", 10), recordTestItem(day, 2, 1, "0.2", 20)}
	more := true
	cursor := "next"
	if q.Get("cursor") == "next" {
		items = []map[string]any{recordTestItem(day, 3, 2, "0.3", 30), recordTestItem(day, 4, 23, "0.4", 40)}
		more = false
		cursor = ""
	}
	_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, key, items, more, cursor))
}

func TestAICodeWithRecordCursorResumesAndReconcilesExactClosedDay(t *testing.T) {
	m := newStabilityTestMonitor(t)
	fixture := &recordTestServer{}
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	defer server.Close()
	m.upstreamClient = server.Client()
	day := recordTestDay()
	row := ChannelUpstreamAccount{Domain: "spring.test", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1}
	round := AICodeWithUsageRound{Domain: row.Domain, RoundID: "round", WindowFrom: day, WindowTo: day + 86400, RecordMode: true}
	for i := 0; i < 3; i++ {
		result, ready, err := m.fetchAICodeWithRecordWindow(context.Background(), row, "one", round, "slot", day+86400, intBudget(1))
		if err != nil || ready != (i == 2) {
			t.Fatalf("step=%d ready=%v err=%v", i, ready, err)
		}
		if ready {
			var total float64
			var tokens, requests int64
			for _, h := range result.Hours {
				total += h.CostUSD
				tokens += h.Tokens
				requests += h.Requests
				if h.Provisional || h.SourceKind != upstreamUsageAdapterAICodeWithRecord {
					t.Fatal(h)
				}
			}
			if len(result.Hours) != 24 || math.Abs(total-1) > 1e-12 || tokens != 100 || requests != 4 {
				t.Fatal(result)
			}
		}
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if strings.Join(fixture.calls, ",") != "Bearer one/record/,Bearer one/record/next,Bearer one/day/" {
		t.Fatal(fixture.calls)
	}
}
func intBudget(n int) *upstreamUsageRequestPacer { return newUpstreamUsageRequestPacer(n, 0) }

func TestAICodeWithRecordMultiKeyPublicationArchivesAndNeverDoublesDailyBill(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.UpstreamAICodeWithRecordsEnabled = true
	m.upstreamAICodeWithInterval = time.Nanosecond
	fixture := &recordTestServer{}
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	defer server.Close()
	m.upstreamClient = server.Client()
	day := recordTestDay()
	row := ChannelUpstreamAccount{Domain: "spring.test", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1}
	old := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: row.Provider, HourTs: day, BucketSeconds: 86400, CostUSD: 2, Requests: 8, Tokens: 200}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	cred, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKeys: []string{"sk-acw-record-test-one", "sk-acw-record-test-two"}})
	if err != nil {
		t.Fatal(err)
	}
	version, _ := aiCodeWithCredentialSetVersion(cred)
	for i := 0; i < 6; i++ {
		published, _, calls, err := m.processAICodeWithRound(context.Background(), &row, cred, version, "backfill", day, day+86400, day+86400+int64(i)*60, 1)
		if err != nil || calls > 1 || published != (i == 5) {
			t.Fatalf("step=%d published=%v calls=%d err=%v", i, published, calls, err)
		}
		var rows []ChannelUpstreamUsageHour
		if err := m.storeDB.Where("domain = ?", row.Domain).Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			if len(rows) != 1 || rows[0].BucketSeconds != 86400 || rows[0].CostUSD != 2 {
				t.Fatal(rows)
			}
		} else {
			var sum float64
			for _, r := range rows {
				sum += r.CostUSD
			}
			if len(rows) != 24 || math.Abs(sum-2) > 1e-12 {
				t.Fatal(rows)
			}
		}
	}
	var archives []ChannelUpstreamUsageArchive
	if err := m.storeDB.Find(&archives).Error; err != nil || len(archives) != 1 || archives[0].CostUSD != 2 {
		t.Fatalf("archive=%+v err=%v", archives, err)
	}
	for _, model := range []any{&AICodeWithRecordCheckpoint{}, &AICodeWithRecordSeen{}, &AICodeWithUsageStage{}, &AICodeWithUsageRound{}} {
		var n int64
		if err := m.storeDB.Model(model).Count(&n).Error; err != nil || n != 0 {
			t.Fatalf("leaked %T: %d %v", model, n, err)
		}
	}
}

func TestAICodeWithRecordMismatchAndRateLimitPreservePublishedMoney(t *testing.T) {
	for _, code := range []int{0, 401, 429, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			fixture := &recordTestServer{wrongControl: true, status: code}
			server := httptest.NewServer(http.HandlerFunc(fixture.serve))
			defer server.Close()
			m.upstreamClient = server.Client()
			day := recordTestDay()
			row := ChannelUpstreamAccount{Domain: "spring.test", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1}
			if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: day, BucketSeconds: 86400, CostUSD: 123}).Error; err != nil {
				t.Fatal(err)
			}
			round := AICodeWithUsageRound{Domain: row.Domain, RoundID: "r", WindowFrom: day, WindowTo: day + 86400, RecordMode: true}
			_, ready, err := m.fetchAICodeWithRecordWindow(context.Background(), row, "one", round, "s", day+86400, intBudget(4))
			if err == nil || ready {
				t.Fatalf("ready=%v err=%v", ready, err)
			}
			var old ChannelUpstreamUsageHour
			if e := m.storeDB.First(&old, "domain = ?", row.Domain).Error; e != nil || old.CostUSD != 123 {
				t.Fatal(old, e)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if code != 0 && len(fixture.calls) != 1 {
				t.Fatalf("retried failing upstream: %v", fixture.calls)
			}
		})
	}
}
