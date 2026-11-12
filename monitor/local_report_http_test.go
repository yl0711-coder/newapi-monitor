package monitor

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestModelReportsCompressedResponseKeepsData(t *testing.T) {
	now := time.Now()
	end := metricFinalizeTarget(now.Unix())
	m := modelStatisticsWindowFixture(t, false, now, end, end, end)
	modelFRTInsert(t, m, end, 1, "g", "m", 40, 1000)
	for _, item := range []struct {
		name  string
		serve func(*gin.Context)
		key   string
	}{{"frt", m.serveModelFRTReport, "channels"}, {"requests", m.serveModelStatisticsReport, "models"}} {
		t.Run(item.name, func(t *testing.T) {
			var values []any
			for _, accept := range []string{"identity", "gzip"} {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodGet, "/?from="+time.Unix(end-86400, 0).UTC().Format(time.RFC3339)+"&to="+time.Unix(end, 0).UTC().Format(time.RFC3339), nil)
				c.Request.Header.Set("Accept-Encoding", accept)
				item.serve(c)
				if rec.Code != 200 {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
				var body []byte
				if accept == "gzip" {
					if rec.Header().Get("Content-Encoding") != "gzip" || rec.Header().Get("X-Accel-Buffering") != "no" {
						t.Fatal("compression/stream headers missing")
					}
					r, err := gzip.NewReader(rec.Body)
					if err != nil {
						t.Fatal(err)
					}
					body, err = io.ReadAll(r)
					r.Close()
					if err != nil {
						t.Fatal(err)
					}
				} else {
					body = rec.Body.Bytes()
				}
				var value map[string]any
				if err := json.Unmarshal(body, &value); err != nil {
					t.Fatal(err)
				}
				if rows, ok := value[item.key].([]any); !ok || len(rows) == 0 {
					t.Fatalf("fixture returned no %s: %s", item.key, body)
				}
				values = append(values, value[item.key])
			}
			if !reflect.DeepEqual(values[0], values[1]) {
				t.Fatal("compressed result changed data")
			}
		})
	}
}
