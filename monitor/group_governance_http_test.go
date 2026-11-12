package monitor

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestGroupGovernanceLargeReportTransport(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = true
	m.prodDB = &sql.DB{} // Expanding and refreshing must never query production.
	seed := ggTestMetrics(1, 2000)[0]
	r := ggRealtimeReport{Status: "ready", Channels: make([]ggChannelMetrics, 2400)}
	for i := range r.Channels {
		v := seed
		v.Key = fmt.Sprintf("transport-%d", i)
		v.Model = fmt.Sprintf("model-%d", i)
		r.Channels[i] = ggChannelMetrics{ggMetrics: v}
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 4<<20 {
		t.Fatalf("fixture no longer reproduces proxy temp limit: %d bytes", len(raw))
	}
	if err = m.storeDB.Create(&GroupGovernanceRealtimeState{ID: 1, UpdatedAt: time.Now().Unix(), ReportJSON: string(raw)}).Error; err != nil {
		t.Fatal(err)
	}
	read := func(accept string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, "/group-governance/report", nil)
		c.Request.Header.Set("Accept-Encoding", accept)
		m.serveGroupGovernanceReport(c)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status %d", recorder.Code)
		}
		for key, want := range map[string]string{"Cache-Control": "private, no-store", "X-Accel-Buffering": "no", "Vary": "Accept-Encoding", "Content-Type": "application/json; charset=utf-8"} {
			if got := recorder.Header().Get(key); got != want {
				t.Fatalf("%s = %q, want %q", key, got, want)
			}
		}
		return recorder
	}
	identity := read("identity")
	for _, accept := range []string{"gzip, deflate, br", "br, gzip;q=0.5", "*;q=1", "", "gzip;q=0, *;q=1"} {
		t.Run(accept, func(t *testing.T) {
			response := read(accept)
			body := response.Body.Bytes()
			if response.Header().Get("Content-Encoding") == "gzip" {
				if len(body) >= 4<<20 || len(body)*4 >= identity.Body.Len() {
					t.Fatalf("compression ineffective: %d => %d", identity.Body.Len(), len(body))
				}
				gz, err := gzip.NewReader(bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				body, err = io.ReadAll(gz)
				if err != nil {
					t.Fatal(err)
				}
				if err = gz.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("complete JSON %d bytes, gzip %d bytes", len(body), response.Body.Len())
			} else if accept != "" && accept != "gzip;q=0, *;q=1" {
				t.Fatal("gzip negotiation ignored")
			}
			if !bytes.Equal(body, identity.Body.Bytes()) {
				t.Fatal("transport changed or truncated detail, cutoff, or recommendation fields")
			}
		})
	}
}

func TestGroupGovernanceGzipNegotiation(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   bool
	}{
		{"", false}, {"identity", false}, {"br", false}, {"gzip", true},
		{"GZIP;Q=0.5, br", true}, {"*", true}, {"*;q=0", false},
		{"gzip;q=0, *;q=1", false}, {"*;q=1, gzip;q=0", false},
		{"gzip;q=invalid", false}, {"gzip;q=NaN", false}, {"gzip;q=2", false},
	} {
		t.Run(tc.header, func(t *testing.T) {
			if got := groupGovernanceAcceptsGzip(tc.header); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
