package monitor

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestLocalReportsDistinguishClientCancellationFromReadFailure(t *testing.T) {
	for _, endpoint := range []struct {
		name    string
		handle  func(*Monitor, *gin.Context)
		failure int
	}{
		{"customer-health", (*Monitor).serveCustomerHealthReport, 500},
		{"model-statistics", (*Monitor).serveModelStatisticsReport, 503},
		{"model-frt", (*Monitor).serveModelFRTReport, 503},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			m.cfg.RetentionDays = 7
			var injected error
			failRead := func(tx *gorm.DB) {
				if injected != nil {
					_ = tx.AddError(injected)
				}
			}
			if err := m.storeDB.Callback().Query().Before("gorm:query").Register("report-read-failure", failRead); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := m.storeDB.Callback().Query().Remove("report-read-failure"); err != nil {
					t.Error(err)
				}
			}()
			// Model statistics aggregates use Raw().Scan(), which runs the row
			// callback. A failure only in optional channel names is not a failed
			// facts read and correctly leaves the report available with unknown names.
			if err := m.storeDB.Callback().Row().Before("gorm:row").Register("report-read-failure", failRead); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := m.storeDB.Callback().Row().Remove("report-read-failure"); err != nil {
					t.Error(err)
				}
			}()
			for _, tc := range []struct {
				name string
				ctx  context.Context
				err  error
				want int
			}{
				{"browser_cancellation", canceledContext(), errors.New("driver interrupted"), statusClientClosedRequest},
				{"query_deadline", context.Background(), context.DeadlineExceeded, endpoint.failure},
				{"database_failure", context.Background(), errors.New("database unavailable"), endpoint.failure},
			} {
				t.Run(tc.name, func(t *testing.T) {
					injected = tc.err
					w := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(w)
					c.Request = httptest.NewRequest("GET", "/"+endpoint.name+"/report", nil).WithContext(tc.ctx)
					endpoint.handle(m, c)
					if w.Code != tc.want {
						t.Fatalf("status %d, want %d: %s", w.Code, tc.want, w.Body.String())
					}
					if tc.want == statusClientClosedRequest && w.Body.Len() != 0 {
						t.Fatal("cancelled client received a misleading server-error response")
					}
				})
			}
		})
	}
}
