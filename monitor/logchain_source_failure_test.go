package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type logchainFailureSource struct{ phase string }

func (s logchainFailureSource) Open(string) (driver.Conn, error) {
	return s.Connect(context.Background())
}
func (s logchainFailureSource) Driver() driver.Driver { return s }
func (s logchainFailureSource) Connect(context.Context) (driver.Conn, error) {
	if s.phase == "connection" {
		return nil, context.DeadlineExceeded
	}
	return logchainFailureConn(s), nil
}

type logchainFailureConn struct{ phase string }

func (logchainFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (logchainFailureConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (logchainFailureConn) Close() error { return nil }
func (c logchainFailureConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.phase == "execute" {
		return nil, context.DeadlineExceeded
	}
	return &logchainFailureRows{phase: c.phase}, nil
}

type logchainFailureRows struct {
	phase string
	index int
}

func (*logchainFailureRows) Columns() []string {
	return strings.Split("id,created_at,type,user_id,username,group,token_name,channel_id,model_name,prompt_tokens,completion_tokens,use_time,is_stream,quota,content,other,request_id", ",")
}
func (*logchainFailureRows) Close() error { return nil }
func (r *logchainFailureRows) Next(dest []driver.Value) error {
	if r.index > 0 {
		if r.phase == "receive" {
			return context.DeadlineExceeded
		}
		return io.EOF
	}
	r.index++
	copy(dest, []driver.Value{int64(1), int64(101), int64(2), int64(7), "fixture", "fixture", "fixture", int64(3), "fixture", int64(10), int64(5), nil, int64(0), int64(1), "", "{}", "fixture-request"})
	if r.phase == "decode" {
		dest[0] = "invalid integer"
	}
	return nil
}

func TestLogChainSourceFailuresKeepPhaseAndReleaseSharedGate(t *testing.T) {
	for phase, want := range map[string]string{"connection": "连接获取失败", "execute": "查询失败", "receive": "接收失败", "decode": "解析失败"} {
		t.Run(phase, func(t *testing.T) {
			db := sql.OpenDB(logchainFailureSource{phase})
			defer db.Close()
			m := &Monitor{prodDB: db}
			rows, more, err := m.queryLogChain(context.Background(), logChainScope{FromTs: 100, ToTs: 200, Limit: 5}, nil)
			if err == nil || !strings.Contains(err.Error(), want) || rows != nil || more {
				t.Fatalf("failed page exposed partial success: rows=%d more=%v error=%v", len(rows), more, err)
			}
			if phase != "decode" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("lost underlying deadline")
			}
			stats := m.usageSourceStats()
			if stats.InUse != 0 || stats.Detail.Active != 0 || stats.ConnectionPool.InUse != 0 {
				t.Fatal("failed query retained shared source resources", stats)
			}
		})
	}
}
