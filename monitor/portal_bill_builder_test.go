package monitor

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/usagebill/exportjobs"
)

func TestPortalBillBuilderTaskPublishesOnlyVerifiedFile(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(strconv.FormatBool(fail), func(t *testing.T) {
			base := t.TempDir()
			if err := os.Chmod(base, 0700); err != nil {
				t.Fatal(err)
			}
			m, err := exportjobs.New(base, exportjobs.Limits{MaxJobs: 1, MaxRows: 10, MaxBytes: 1 << 20, GenerateTimeout: 5 * time.Second, Retention: time.Minute}, func(context.Context, exportjobs.Owner) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			owner := exportjobs.Owner{GroupID: 1, MemberFingerprint: "local-only"}
			r := LogRow{ID: 1, Type: 2, billingFactsValid: true, billingQuota: 123}
			want := "123"
			if fail {
				want = "124"
			}
			v, err := m.Submit(context.Background(), exportjobs.Request{Owner: owner, Key: strings.Repeat("a", 64), ExpectedRows: 1, Build: func(ctx context.Context, w io.Writer, p func(int64) error) (int64, error) {
				return portalBuildVerifiedBill(ctx, w, portalBillBuildPlan{1, want, 1, 1}, func(_ context.Context, c logPageCursor, _ int) ([]LogRow, error) {
					if c.HasBefore {
						return nil, nil
					}
					return []LogRow{r}, nil
				}, func(context.Context) error { return nil }, p)
			}})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for v.State == "queued" || v.State == "running" {
				if time.Now().After(deadline) {
					t.Fatal("job timeout")
				}
				time.Sleep(time.Millisecond)
				v, err = m.View(context.Background(), owner, v.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			a, err := m.Open(context.Background(), owner, v.ID)
			if fail {
				if v.State != "failed" || v.FailureCode != "generation_failed" || err == nil {
					if a != nil {
						a.Close()
					}
					t.Fatal("partial file published")
				}
				return
			}
			if err != nil || v.State != "ready" {
				t.Fatalf("state=%s failure=%s err=%v", v.State, v.FailureCode, err)
			}
			defer a.Close()
			b, err := io.ReadAll(a)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(b), "\ufeff"))).ReadAll()
			if err != nil || len(rows) != 2 || rows[1][12] != "0.000246" {
				t.Fatal("wrong completed artifact")
			}
		})
	}
}

func TestPortalBillBuilderPreservesApprovedColumns(t *testing.T) {
	r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
	r.ID = 10
	var out bytes.Buffer
	calls, checks := 0, 0
	n, err := portalBuildVerifiedBill(context.Background(), &out, portalBillBuildPlan{1, "1212", 10, 1},
		func(_ context.Context, c logPageCursor, limit int) ([]LogRow, error) {
			calls++
			if limit != 1 || !c.HasUpper || c.UpperID != 10 {
				t.Fatal("unbounded page")
			}
			if !c.HasBefore {
				return []LogRow{r}, nil
			}
			return nil, nil
		}, func(context.Context) error { checks++; return nil }, func(n int64) error {
			if n != 1 {
				t.Fatal("invalid progress")
			}
			return nil
		})
	if err != nil || n != 1 || calls != 2 || checks != 2 {
		t.Fatalf("n=%d calls=%d checks=%d err=%v", n, calls, checks, err)
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(out.String(), "\ufeff"))).ReadAll()
	want, _ := portalBillRecord(r)
	if err != nil || len(rows) != 2 || !reflect.DeepEqual(rows[0], portalBillHeader()) || !reflect.DeepEqual(rows[1], want) {
		t.Fatal("approved format changed")
	}
}

func TestPortalBillBuilderRejectsSourceFaults(t *testing.T) {
	base := LogRow{ID: 3, CreatedAt: 10, Type: 2, billingFactsValid: true, billingQuota: 7}
	for _, name := range []string{"missing", "extra", "duplicate", "moved-duplicate", "order", "upper", "type", "quota", "missing-quota", "subscription", "page-error", "over-page", "verify-before", "verify-after", "progress", "cancel"} {
		t.Run(name, func(t *testing.T) {
			a, b := base, base
			b.ID = 2
			pages := [][]LogRow{{a}, {b}, nil}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "missing":
				pages[1] = nil
			case "extra":
				c := base
				c.ID = 1
				pages[2] = []LogRow{c}
			case "duplicate":
				pages[1][0] = a
			case "moved-duplicate":
				pages[1][0] = a
				pages[1][0].CreatedAt--
			case "order":
				pages[1][0].CreatedAt++
			case "upper":
				pages[0][0].ID = 4
			case "type":
				pages[0][0].Type = 6
			case "quota":
				pages[1][0].billingQuota++
			case "missing-quota":
				pages[0][0].billingFactsValid = false
			case "subscription":
				pages[0][0].BillingSource = "subscription"
			case "over-page":
				pages[0] = []LogRow{a, b}
			case "cancel":
				cancel()
			}
			pageIndex, checks := 0, 0
			_, err := portalBuildVerifiedBill(ctx, io.Discard, portalBillBuildPlan{2, "14", 3, 1},
				func(context.Context, logPageCursor, int) ([]LogRow, error) {
					if name == "page-error" {
						return nil, errors.New("source failed")
					}
					if pageIndex >= len(pages) {
						return nil, nil
					}
					p := pages[pageIndex]
					pageIndex++
					return p, nil
				}, func(context.Context) error {
					checks++
					if (name == "verify-before" && checks == 1) || (name == "verify-after" && checks == 2) {
						return errors.New("source generation changed")
					}
					return nil
				}, func(int64) error {
					if name == "progress" {
						return errors.New("cancelled")
					}
					return nil
				})
			if err == nil {
				t.Fatal("invalid bill accepted")
			}
		})
	}
}

func TestPortalBillBuilderExactLargeSumAndEmpty(t *testing.T) {
	for _, count := range []int64{0, 2} {
		r := LogRow{ID: 2, Type: 2, billingFactsValid: true, billingQuota: 9223372036854775807}
		sum := new(big.Int).Mul(big.NewInt(r.billingQuota), big.NewInt(count)).String()
		i := int64(0)
		var out bytes.Buffer
		n, err := portalBuildVerifiedBill(context.Background(), &out, portalBillBuildPlan{count, sum, 2, 1}, func(context.Context, logPageCursor, int) ([]LogRow, error) {
			if i == count {
				return nil, nil
			}
			row := r
			row.ID -= i
			i++
			return []LogRow{row}, nil
		}, func(context.Context) error { return nil }, func(int64) error { return nil })
		if err != nil || n != count {
			t.Fatalf("count=%d err=%v", count, err)
		}
		rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(out.String(), "\ufeff"))).ReadAll()
		if err != nil || int64(len(rows)) != count+1 {
			t.Fatal("bad CSV")
		}
		actual := new(big.Rat)
		for _, row := range rows[1:] {
			v, ok := new(big.Rat).SetString(row[12])
			if !ok {
				t.Fatal("bad amount")
			}
			actual.Add(actual, v)
		}
		want, _ := new(big.Rat).SetString(sum)
		want.Quo(want, big.NewRat(500000, 1))
		if actual.Cmp(want) != 0 {
			t.Fatal("integer sum overflow or rounding loss")
		}
	}
}

type portalBillShortWriter struct{ writes int }

func (w *portalBillShortWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		return len(p), nil
	} // allow BOM, fail CSV flush
	return len(p) - 1, nil
}

func TestPortalBillBuilderInvalidPlanAndShortWrite(t *testing.T) {
	for _, raw := range []string{"", "-1", "01", "+1", "1.0", strings.Repeat("9", 10000)} {
		_, err := portalBuildVerifiedBill(context.Background(), io.Discard, portalBillBuildPlan{Quota: raw, PageSize: 1},
			func(context.Context, logPageCursor, int) ([]LogRow, error) {
				t.Fatal("invalid plan queried source")
				return nil, nil
			},
			func(context.Context) error { return nil }, func(int64) error { return nil })
		if err == nil {
			t.Fatalf("invalid quota accepted: length=%d", len(raw))
		}
	}
	_, err := portalBuildVerifiedBill(context.Background(), &portalBillShortWriter{}, portalBillBuildPlan{Quota: "0", PageSize: 1},
		func(context.Context, logPageCursor, int) ([]LogRow, error) { return nil, nil }, func(context.Context) error { return nil }, func(int64) error { return nil })
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}
}

// Actual Monitor SQL pagination and all 14 CSV columns, using only a disposable
// local database. No production reads; source remains unchanged during build.
func TestPortalBillBuilderLocalDatabase(t *testing.T) {
	count := 2100
	if os.Getenv("USAGE_BILL_BUILDER_LARGE") == "1" {
		count = 100000
	}
	m, _, _, _ := billHTTPFixture(t)
	if _, err := m.prodDB.Exec("DELETE FROM logs"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("CREATE INDEX bill_builder_page ON logs(user_id,created_at,type,id)"); err != nil {
		t.Fatal(err)
	}
	tx, err := m.prodDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	s, err := tx.Prepare("INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,model_name,token_name,`group`,other,request_id) VALUES(?,101,?,2,?,10,5,'local-user','unknown-model','token','group',?,?)")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 1; i <= count; i++ {
		_, err := s.Exec(i, 1700000000+i/3, i%31, `{"billing_source":"wallet","usage_semantic":"openai","cache_tokens":2,"cache_creation_tokens":3,"request_path":"/v1/responses"}`, fmt.Sprintf("local-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var expected, quota, upper int64
	if err := m.prodDB.QueryRow("SELECT COUNT(*),SUM(quota),MAX(id) FROM logs WHERE user_id=101 AND type=2").Scan(&expected, &quota, &upper); err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "bill-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lastProgress := int64(0)
	started := time.Now()
	n, err := portalBuildVerifiedBill(context.Background(), f, portalBillBuildPlan{expected, strconv.FormatInt(quota, 10), upper, 1000},
		func(ctx context.Context, c logPageCursor, limit int) ([]LogRow, error) {
			return m.queryGroupLogsForExportCursor(ctx, []int64{101}, 1699999999, 1800000000, 0, 2, "", "", "", "", "", c, limit)
		},
		func(context.Context) error { return nil }, func(n int64) error {
			if n <= lastProgress || n > expected {
				t.Fatal("bad progress")
			}
			lastProgress = n
			return nil
		})
	if err != nil || n != expected {
		t.Fatalf("count=%d err=%v", n, err)
	}
	elapsed := time.Since(started)
	if _, err := f.Seek(3, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil || !reflect.DeepEqual(header, portalBillHeader()) {
		t.Fatal("bad header")
	}
	amount := new(big.Rat)
	for id := count; id > 0; id-- {
		row, err := reader.Read()
		if err != nil || len(row) != 14 {
			t.Fatalf("row=%d err=%v", id, err)
		}
		if row[5] != fmt.Sprintf("local-%d", id) || row[6] != "文本" || row[7] != "5" || row[8] != "5" || row[9] != "2" || row[10] != "3" || row[11] != "" || row[13] != "" {
			t.Fatal("row order/columns mismatch")
		}
		v, ok := new(big.Rat).SetString(row[12])
		if !ok {
			t.Fatal("bad amount")
		}
		amount.Add(amount, v)
	}
	if _, err := reader.Read(); err != io.EOF {
		t.Fatal("extra row")
	}
	if amount.Cmp(big.NewRat(quota, 500000)) != 0 {
		t.Fatal("amount mismatch")
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("local SQL rows=%d bytes=%d generation=%s; all IDs, 14 columns and exact charges checked", n, info.Size(), elapsed)
}
