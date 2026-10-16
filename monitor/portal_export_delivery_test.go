package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type portalExportDelayWriter struct {
	dst   io.Writer
	delay time.Duration
	wrote bool
}

func (w *portalExportDelayWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	if !w.wrote {
		w.wrote = true
		time.Sleep(w.delay)
	}
	return n, err
}

func TestPortalExportGenerationTimeoutCleansAndAllowsRetry(t *testing.T) {
	for _, bill := range []bool{false, true} {
		t.Run(fmt.Sprint(bill), func(t *testing.T) {
			m, handler, cookie, _ := billHTTPFixture(t)
			path := "/api/logs/export/v1?from=2026-10-13&to=2026-10-13"
			route := "/api/logs/export/v1"
			if bill {
				route = "/api/logs/bill/v1"
				path = route + "?ticket=" + billPrepare(t, handler, cookie)
			}
			router := gin.New()
			var delayed *portalExportDelayWriter
			router.GET(route, m.requirePortal(true), func(c *gin.Context) {
				m.portalLogsExportWithBudgets(c, portalExportMaxBytes, func(dst io.Writer) io.Writer {
					delayed = &portalExportDelayWriter{dst: dst, delay: 1100 * time.Millisecond}
					return delayed
				}, time.Second, time.Second)
			})
			w := billGet(router, cookie, path)
			if w.Code != 504 || !strings.Contains(w.Body.String(), "export_generation_timeout") || w.Header().Get("Content-Disposition") != "" || strings.Contains(w.Body.String(), "\xEF\xBB\xBF") {
				t.Fatalf("timeout exposed partial file: %d %s", w.Code, w.Body.String())
			}
			if delayed == nil || !delayed.wrote {
				t.Fatal("must exercise timeout after partial file staging")
			}
			portalLargeNoStaleFiles(t, m)
			retry := billGet(handler, cookie, path)
			if retry.Code != 200 || retry.Header().Get("Content-Length") != strconv.Itoa(retry.Body.Len()) || !strings.HasPrefix(retry.Body.String(), "\xEF\xBB\xBF") {
				t.Fatalf("retry not a complete CSV: %d %s", retry.Code, retry.Body.String())
			}
			if retry.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("customer export must not be cached")
			}
		})
	}
}

// Flush errors often occur only after the last buffered bytes. They must not
// count as a completed download or consume the customer's successful quota.
type portalExportFlushFailure struct{ *httptest.ResponseRecorder }

func (w *portalExportFlushFailure) FlushError() error { return io.ErrClosedPipe }

type portalExportDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *portalExportDeadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestPortalExportGenerationConsumesTotalDeliveryBudget(t *testing.T) {
	m, handler, cookie, _ := billHTTPFixture(t)
	path := "/api/logs/bill/v1?ticket=" + billPrepare(t, handler, cookie)
	router := gin.New()
	var started time.Time
	router.GET("/api/logs/bill/v1", m.requirePortal(true), func(c *gin.Context) {
		started = time.Now()
		m.portalLogsExportWithBudgets(c, portalExportMaxBytes, func(dst io.Writer) io.Writer {
			return &portalExportDelayWriter{dst: dst, delay: 150 * time.Millisecond}
		}, time.Second, 2*time.Second)
	})
	w := &portalExportDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest("GET", path, nil)
	r.AddCookie(cookie)
	router.ServeHTTP(w, r)
	if w.Code != 200 || len(w.deadlines) != 2 || !w.deadlines[1].IsZero() {
		t.Fatalf("unexpected delivery/deadline reset: %d %v", w.Code, w.deadlines)
	}
	if delta := w.deadlines[0].Sub(started); delta < 2*time.Second || delta > 2050*time.Millisecond {
		t.Fatalf("generation incorrectly extended total delivery budget: %s", delta)
	}
}

func TestPortalExportFlushFailureAllowsRetry(t *testing.T) {
	m, handler, cookie, _ := billHTTPFixture(t)
	path := "/api/logs/bill/v1?ticket=" + billPrepare(t, handler, cookie)
	w := &portalExportFlushFailure{httptest.NewRecorder()}
	r := httptest.NewRequest("GET", path, nil)
	r.AddCookie(cookie)
	handler.ServeHTTP(w, r)
	if w.Header().Get("Content-Length") == "" || !strings.HasPrefix(w.Body.String(), "\xEF\xBB\xBF") {
		t.Fatal("must exercise actual delivery, not preparation error")
	}
	portalLargeNoStaleFiles(t, m)
	if retry := billGet(handler, cookie, path); retry.Code != 200 {
		t.Fatalf("flush failure consumed success quota: %d", retry.Code)
	}
}

func TestPortalExportGenerationCancellationIsNotTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	if !portalExportGenerationStopped(ctx, c) || w.Body.Len() != 0 {
		t.Fatal("disconnected client received invented timeout response")
	}
}

type portalExportZeroReader struct{}

func (portalExportZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestPortalExportRealSlowConnectionHasWriteDeadline(t *testing.T) {
	// Real loopback net/http + Gin, not a simulated context-only timeout.
	result := make(chan error, 1)
	router := gin.New()
	router.GET("/", func(c *gin.Context) {
		result <- portalSendExport(c, portalExportZeroReader{}, 64<<20, "local.csv", 100*time.Millisecond)
	})
	s := httptest.NewServer(router)
	defer s.Close()
	conn, err := net.DialTimeout("tcp", s.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	// Deliberately do not read; the server must free the writer itself.
	select {
	case err := <-result:
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("expected write timeout, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow client retained export writer")
	}
}

func TestPortalExportRealConnectionDetectsTruncatedBody(t *testing.T) {
	router := gin.New()
	router.GET("/", func(c *gin.Context) {
		_ = portalSendExport(c, strings.NewReader("partial"), 100, "local.csv", time.Second)
	})
	s := httptest.NewServer(router)
	defer s.Close()
	resp, err := s.Client().Get(s.URL)
	// A failed transfer may close before any headers reach the client, also safe.
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	if err == nil {
		t.Fatal("truncated body appeared successful")
	}
}

func TestPortalExportSuccessfulConnectionDeadlineIsCleared(t *testing.T) {
	router := gin.New()
	router.GET("/", func(c *gin.Context) {
		if err := portalSendExport(c, strings.NewReader("complete"), 8, "local.csv", 100*time.Millisecond); err != nil {
			t.Errorf("complete delivery failed: %v", err)
		}
	})
	s := httptest.NewServer(router)
	defer s.Close()
	for i := 0; i < 2; i++ {
		var reused bool
		trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}
		r, _ := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), "GET", s.URL, nil)
		resp, err := s.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || string(body) != "complete" {
			t.Fatalf("body=%q err=%v", body, err)
		}
		if i == 1 && !reused {
			t.Fatal("success broke keep-alive connection")
		}
		if i == 0 {
			time.Sleep(150 * time.Millisecond)
		}
	}
}
