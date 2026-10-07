package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func financeMarginResponseFixture(revenue string) []byte {
	statement := `{"paired_contribution_margin_percent":"old-paired","contribution_margin_percent":"old-exact","known_contribution_profit":{"micro_usd":"-1234567","display":"-$1.23"}}`
	if revenue != "" {
		statement = strings.TrimSuffix(statement, "}") + `,"paired_user_consumption":` + revenue + `}`
	}
	entry := `{"from":1786377600,"to":1786464000,"status":"partial","statement":` + statement + `}`
	return []byte(`{"enabled":true,"generated_at":1791280800,"unknown_integer":9223372036854775807,"unknown_decimal":0.1234567890123456789,"source_fingerprint":"source-v1","statement":` + statement + `,"periods":[` + entry + `],"days":[` + entry + `],"extra":{"state":"stale"}}`)
}

func decodeFinanceMarginJSON(t *testing.T, payload []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestFinanceResponseMarginPreservesAmountsAndMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, revenue string
		clear         bool
	}{
		{"zero", `{"micro_usd":"0","display":"$0.00"}`, true},
		{"refund", `{"micro_usd":"-1000000","display":"-$1.00"}`, true},
		{"missing", "", true},
		{"null", "null", true},
		{"invalid", `{"micro_usd":"invalid"}`, true},
		{"out_of_range", `{"micro_usd":"9223372036854775808"}`, true},
		{"positive", `{"micro_usd":"1","display":"$0.00"}`, false},
		{"positive_int64", `{"micro_usd":"9223372036854775807"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := financeMarginResponseFixture(tc.revenue)
			original := bytes.Clone(payload)
			got, err := clearFinanceInvalidMargins(payload)
			if err != nil {
				t.Fatal(err)
			}
			want := string(original)
			if tc.clear {
				want = strings.NewReplacer(`"old-paired"`, "null", `"old-exact"`, "null").Replace(want)
			} else if !bytes.Equal(got, original) {
				t.Fatal("positive-revenue report bytes changed")
			}
			if !reflect.DeepEqual(decodeFinanceMarginJSON(t, got), decodeFinanceMarginJSON(t, []byte(want))) {
				t.Fatal("response changed amounts, metadata, precision or unrelated fields")
			}
			if !bytes.Equal(payload, original) {
				t.Fatal("response correction mutated the shared cache input")
			}
			second, err := clearFinanceInvalidMargins(got)
			if err != nil || !bytes.Equal(got, second) {
				t.Fatal("response correction was not idempotent", err)
			}
		})
	}
}

func TestFinanceResponseMarginLeavesEmptyAndValidReportsUntouched(t *testing.T) {
	for _, payload := range []string{
		`{"enabled":false}`, `{"statement":null,"days":null,"periods":[]}`,
		`{"statement":{"paired_user_consumption":{"micro_usd":"0"}}}`,
		`{"statement":{"paired_contribution_margin_percent":null,"contribution_margin_percent":null}}`,
	} {
		got, err := clearFinanceInvalidMargins([]byte(payload))
		if err != nil || string(got) != payload {
			t.Fatal("unchanged or absent margins were rewritten", err)
		}
	}
}

func TestFinanceCachedResponseMarginDoesNotRebuildOrMutateCache(t *testing.T) {
	// A cache hit must work without a database or workers. Correct only the
	// delivered percentage; do not invalidate historical data or change age.
	m := &Monitor{}
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0)}
	original := financeMarginResponseFixture(`{"micro_usd":"0"}`)
	m.getFinanceReportCache().PutWithStale(request.cacheKey(), original, time.Minute, time.Minute, time.Now())
	payload, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "hit" {
		t.Fatal("expected a database-free cache hit", err)
	}
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Header("X-Monitor-Finance-Cache", status)
	c.Header("X-Monitor-Finance-Update", "pending")
	writeFinanceReportJSON(c, payload)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "old-paired") || strings.Contains(response.Body.String(), "old-exact") ||
		response.Header().Get("X-Monitor-Finance-Cache") != "hit" || response.Header().Get("X-Monitor-Finance-Update") != "pending" {
		t.Fatal("cached response did not clear obsolete margins or retain cache metadata")
	}
	cached, nextStatus, err := m.financeReportPayload(context.Background(), request, false)
	entries, size := m.getFinanceReportCache().size()
	if err != nil || nextStatus != "hit" || !bytes.Equal(cached, original) || entries != 1 || size != len(original) {
		t.Fatal("delivery altered or rebuilt the stored report", err)
	}
}

func TestFinanceResponseMarginMalformedPayloadFailsClosed(t *testing.T) {
	for _, payload := range []string{`{`, `{"statement":[]}`, `{"periods":"private-payload"}`, `{"days":[42]}`} {
		response := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(response)
		writeFinanceReportJSON(c, []byte(payload))
		if response.Code != http.StatusServiceUnavailable || response.Body.String() != `{"error":"经营核算报表格式异常，请重试"}` {
			t.Fatal("malformed report was published or leaked the internal parse error")
		}
	}
}

func BenchmarkFinanceResponseMargin(b *testing.B) {
	var report map[string]json.RawMessage
	if err := json.Unmarshal(financeMarginResponseFixture(`{"micro_usd":"0"}`), &report); err != nil {
		b.Fatal(err)
	}
	var days []json.RawMessage
	if err := json.Unmarshal(report["days"], &days); err != nil {
		b.Fatal(err)
	}
	repeated := bytes.Repeat(append(bytes.Clone(days[0]), ','), 180)
	report["days"] = append(append([]byte{'['}, repeated[:len(repeated)-1]...), ']')
	payload, err := json.Marshal(report)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := clearFinanceInvalidMargins(payload); err != nil {
			b.Fatal(err)
		}
	}
}
