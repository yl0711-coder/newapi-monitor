package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type healthDecimalSource struct{ sum string }

func (s *healthDecimalSource) Connect(context.Context) (driver.Conn, error) {
	return &healthDecimalConn{s.sum}, nil
}
func (s *healthDecimalSource) Driver() driver.Driver { return s }
func (s *healthDecimalSource) Open(string) (driver.Conn, error) {
	return &healthDecimalConn{s.sum}, nil
}

type healthDecimalConn struct{ sum string }

func (*healthDecimalConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*healthDecimalConn) Close() error                        { return nil }
func (*healthDecimalConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *healthDecimalConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &healthDecimalRows{sum: c.sum}, nil
}

type healthDecimalRows struct {
	sum  string
	done bool
}

func (*healthDecimalRows) Columns() []string {
	return []string{"bucket", "channel_id", "model_name", "grp", "user_id", "username", "success", "anomaly", "failed", "customer_health_anomaly", "customer_health_failed", "tokens", "quota", "refund_quota", "ttft_500", "ttft_1k", "ttft_2k", "ttft_5k", "ttft_10k", "ttft_inf", "ttft_max_ms", "ttft_observed", "ttft_over_3s", "frt_sum_ms"}
}
func (*healthDecimalRows) Close() error { return nil }
func (r *healthDecimalRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, []driver.Value{int64(60), int64(1), "fixture", "g", int64(1), "fixture", int64(1), int64(0), int64(0), int64(0), int64(0), int64(10), int64(2), int64(1), int64(0), int64(0), int64(0), int64(0), int64(0), int64(1), int64(9007199254740993), int64(1), int64(1), []byte(r.sum)})
	return nil
}
func TestCustomerHealthFRTDecimalScanPrecisionAndOverflow(t *testing.T) {
	for _, sum := range []string{"9007199254740993", "9223372036854775808", "1.25"} {
		t.Run(sum, func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			m.prodDB = sql.OpenDB(&healthDecimalSource{sum})
			defer m.prodDB.Close()
			_, err := m.sampleCustomerHealthRangeLow(context.Background(), 60, 120)
			if sum == "9007199254740993" {
				if err != nil {
					t.Fatal(err)
				}
				var row CapacityUserMinuteSample
				if err = m.storeDB.First(&row).Error; err != nil {
					t.Fatal(err)
				}
				if row.FRTSumMs == nil || *row.FRTSumMs != 9007199254740993 || row.FRTSumObserved != 1 || row.FRTSumVersion != frtSumSemanticsVersion {
					t.Fatal(row)
				}
			} else {
				if err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("invalid DECIMAL silently rounded or saturated", err)
				}
				var count int64
				if e := m.storeDB.Model(&CapacityUserMinuteSample{}).Count(&count).Error; e != nil || count != 0 {
					t.Fatal("failed scan published facts", count, e)
				}
			}
		})
	}
}
