package monitor

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestModelReportReadGateBoundsWaitersAndReleasesOnCancellation(t *testing.T) {
	var gate modelReportReadGate
	release, err := gate.acquire(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		queuedRelease, queuedErr := gate.acquire(ctx, 5*time.Second)
		if queuedRelease != nil {
			queuedRelease()
		}
		result <- queuedErr
	}()
	for until := time.Now().Add(time.Second); len(gate.waiting) == 0; runtime.Gosched() {
		if time.Now().After(until) {
			t.Fatal("second request did not wait")
		}
	}
	if _, err := gate.acquire(context.Background(), time.Second); !errors.Is(err, errModelReportBusy) {
		t.Fatalf("third request should not enter the queue: %v", err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || len(gate.waiting) != 0 {
			t.Fatalf("cancelled waiter still occupies the queue: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not exit")
	}
	release()
	release = nil
	recovered, err := gate.acquire(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	recovered()
}

func TestModelReportEndpointsShareHeavyReadSlot(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	entered, unblock := make(chan struct{}, 1), make(chan struct{})
	if err := m.storeDB.Callback().Row().Before("gorm:row").Register("hold-model-report", func(*gorm.DB) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-unblock
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- runModelReportHandler(m.serveModelFRTReport, "") }()
	defer func() {
		close(unblock)
		if active := <-done; active.Code != http.StatusOK {
			t.Errorf("admitted report failed: %d %s", active.Code, active.Body.String())
		}
		if next := runModelReportHandler(m.serveModelStatisticsReport, ""); next.Code != http.StatusOK {
			t.Errorf("report did not recover after contention: %d %s", next.Code, next.Body.String())
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("FRT did not start reading")
	}
	busy := runModelReportHandler(m.serveModelStatisticsReport, "")
	if busy.Code != http.StatusServiceUnavailable || busy.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected a retryable busy response, got %d %s", busy.Code, busy.Body.String())
	}
	select {
	case <-entered:
		t.Fatal("the competing report reached SQLite while FRT was still reading")
	default:
	}
}

func TestModelReportEncodingAndDeadlineRelease(t *testing.T) {
	var gate modelReportReadGate
	handle := func(c *gin.Context) {
		serveModelReport(c, &gate, "test", func(context.Context) (gin.H, error) {
			return gin.H{"data": "报表"}, nil
		})
	}
	plain := runModelReportHandler(handle, "gzip;q=0")
	compressed := runModelReportHandler(handle, "gzip")
	if plain.Code != http.StatusOK || compressed.Code != http.StatusOK || !json.Valid(plain.Body.Bytes()) ||
		plain.Header().Get("Content-Encoding") != "" || compressed.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("incorrect report encoding")
	}
	r, err := gzip.NewReader(compressed.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if data, err := io.ReadAll(r); err != nil || string(data) != plain.Body.String() {
		t.Fatalf("compressed and plain output differ: %s %v", data, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if payload, _, err := prepareModelReport(ctx, &gate, true, func(ctx context.Context) (gin.H, error) {
		<-ctx.Done()
		return gin.H{"data": "stale"}, nil
	}); payload != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline returned a stale success: %q %v", payload, err)
	}
	if next := runModelReportHandler(handle, ""); next.Code != http.StatusOK {
		t.Fatal("slot was not released after deadline")
	}
}

func runModelReportHandler(handle gin.HandlerFunc, encoding string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/report", nil)
	c.Request.Header.Set("Accept-Encoding", encoding)
	handle(c)
	return w
}
