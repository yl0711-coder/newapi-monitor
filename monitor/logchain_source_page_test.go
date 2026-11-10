package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

type logchainMySQLPackedSource struct {
	payload []byte
	tailErr error
	query   string
}

func (s *logchainMySQLPackedSource) Driver() driver.Driver { return s }
func (s *logchainMySQLPackedSource) Open(string) (driver.Conn, error) {
	return s.Connect(context.Background())
}
func (s *logchainMySQLPackedSource) Connect(context.Context) (driver.Conn, error) {
	return &logchainPackedConn{source: s}, nil
}

type logchainPackedConn struct {
	logchainFailureConn
	source *logchainMySQLPackedSource
}

func (c *logchainPackedConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.source.query = query
	return &logchainPackedResult{source: c.source}, nil
}

type logchainPackedResult struct {
	source *logchainMySQLPackedSource
	done   bool
}

func (*logchainPackedResult) Columns() []string { return []string{"packed_page"} }
func (*logchainPackedResult) Close() error      { return nil }
func (r *logchainPackedResult) Next(dest []driver.Value) error {
	if r.done {
		if r.source.tailErr != nil {
			return r.source.tailErr
		}
		return io.EOF
	}
	r.done = true
	if r.source.payload != nil {
		dest[0] = r.source.payload
	}
	return nil
}

func packedLogFixtureRow(id, at int64, useTime any) []any {
	return []any{id, at, 2, 17, "客户'\\名称", "fixture", "令牌名", 3, "fixture-model",
		int64(9007199254740993), 20, useTime, 1, 150,
		"渠道完整错误\n引号\"反斜杠\\", `{"request_path":"/v1/messages","stream_status":{"end_reason":"eof"},"cache_tokens":11}`, "request-真实"}
}

func TestLogChainPackedPagePreservesRowsOrderingAndPagination(t *testing.T) {
	// JSON aggregation may reorder rows. Same-second ids and >2^53 operands
	// must survive decoding before the page lookahead/cursor is calculated.
	fixture := [][]any{packedLogFixtureRow(9007199254740993, 101, nil), packedLogFixtureRow(8, 100, 0), packedLogFixtureRow(9007199254740994, 101, 4)}
	for _, asc := range []bool{false, true} {
		payload, _ := json.Marshal(fixture)
		source := &logchainMySQLPackedSource{payload: payload}
		db := sql.OpenDB(source)
		defer db.Close()
		m := &Monitor{prodDB: db}
		rows, more, err := m.queryLogChain(context.Background(), logChainScope{FromTs: 100, ToTs: 102, Limit: 2, Asc: asc}, nil)
		if err != nil || len(rows) != 2 || !more {
			t.Fatal(len(rows), more, err)
		}
		want := []int64{9007199254740994, 9007199254740993}
		if asc {
			want = []int64{8, 9007199254740993}
		}
		if !reflect.DeepEqual([]int64{rows[0].ID, rows[1].ID}, want) {
			t.Fatal("aggregate order leaked into page cursor", rows[0].ID, rows[1].ID)
		}
		for _, row := range rows {
			if row.PromptTokens != 9007199254740993 || row.Member != fixture[0][4] || row.Content != fixture[0][14] || row.RequestID != "request-真实" || row.RequestPath != "/v1/messages" || row.CacheReadTokens != 11 {
				t.Fatal("packed source changed a field")
			}
			if row.ID == 9007199254740993 && row.UseTimeKnown {
				t.Fatal("missing source duration became a known zero")
			}
			if row.ID == 8 && (!row.UseTimeKnown || row.UseTime != 0) {
				t.Fatal("known zero source duration became missing")
			}
		}
		if !strings.Contains(source.query, "LIMIT 3) logchain_page") || !strings.Contains(source.query, "JSON_ARRAYAGG") {
			t.Fatal("aggregation did not retain the bounded source page")
		}
	}
}

func TestLogChainPackedPageEmptyAndFailuresNeverPublishPartial(t *testing.T) {
	valid, _ := json.Marshal([][]any{packedLogFixtureRow(1, 101, nil)})
	oversize, _ := json.Marshal([][]any{packedLogFixtureRow(1, 101, nil), packedLogFixtureRow(2, 101, nil), packedLogFixtureRow(3, 101, nil)})
	for _, tc := range []struct {
		name    string
		payload []byte
		tailErr error
		ok      bool
	}{
		{name: "empty SQL aggregate", ok: true},
		{name: "empty JSON aggregate", payload: []byte("null"), ok: true},
		{name: "truncated JSON", payload: valid[:len(valid)-1]},
		{name: "wrong columns", payload: []byte("[[1,101]]")},
		{name: "row cap", payload: oversize},
		{name: "error after payload", payload: valid, tailErr: context.DeadlineExceeded},
		{name: "operand overflow", payload: []byte(strings.Replace(string(valid), "9007199254740993", "9223372036854775808", 1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(&logchainMySQLPackedSource{payload: tc.payload, tailErr: tc.tailErr})
			defer db.Close()
			m := &Monitor{prodDB: db}
			rows, more, err := m.queryLogChain(context.Background(), logChainScope{FromTs: 100, ToTs: 200, Limit: 1}, nil)
			if (err == nil) != tc.ok || len(rows) != 0 || more {
				t.Fatal("invalid page exposed success or data", len(rows), more, err)
			}
			if tc.tailErr != nil && !errors.Is(err, tc.tailErr) {
				t.Fatal("lost source receive failure")
			}
			if m.usageSourceStats().InUse != 0 || db.Stats().InUse != 0 {
				t.Fatal("packed source retained shared resources")
			}
		})
	}
}
