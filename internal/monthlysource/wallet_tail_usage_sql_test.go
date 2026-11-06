package monthlysource

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"
)

func serverUsageWire(b int64) []driver.Value {
	return []driver.Value{int64(1), b + 1, int64(2), int64(1), int64(201), int64(2), "wallet", int64(1), int64(1), "9", int64(9), int64(9), int64(1), int64(1), b + 1, b + 1}
}

func TestWalletTailServerUsageQueryLimitsBeforeAggregation(t *testing.T) {
	_, d, _ := pageTailSource(t, "user_usage", nil)
	for _, after := range []WalletTailPageCursor{{}, {At: d.r.Wallet.Boundary + 1, Type: 2, ID: 9007199254740993}, {At: d.r.Wallet.Boundary + 1, Type: 6, ID: 999}} {
		r := d.r
		r.After = after
		q, args, err := serverTailUsageQuery(r)
		limits := 1
		if after.ID != 0 {
			limits = 4
		}
		if err != nil || strings.Count(q, "LIMIT 8192") != limits || strings.Contains(q, "JSON_TABLE") || strings.Contains(q, "OFFSET") || !strings.Contains(q, "STRAIGHT_JOIN logs AS l FORCE INDEX(PRIMARY)") || !strings.Contains(q, "LIMIT 11") || !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") {
			t.Fatal("usage query lost bounded keys or primary-key aggregation", err)
		}
		if !reflect.DeepEqual(args[len(args)-4:], []any{r.Wallet.UserID, after.At, after.Type, after.ID}) {
			t.Fatal("header fallback or body scope differs")
		}
	}
	for _, bad := range []WalletTailPageCursor{{At: d.r.Wallet.Boundary - 1, Type: 2, ID: 1}, {At: d.r.Wallet.Boundary, Type: 3, ID: 1}, {At: d.r.Wallet.Boundary, Type: 2}} {
		r := d.r
		r.After = bad
		if _, _, err := serverTailUsageQuery(r); err == nil {
			t.Fatal("invalid keyset accepted")
		}
	}
}

func TestWalletTailServerUsageMalformedOrMissingTransportIsNotZeroMoney(t *testing.T) {
	for _, fault := range []string{"absent", "null_amount", "null_count", "float_amount", "exponent_amount", "overflow", "null_quota_count", "missing_join", "empty_hides_money", "two_empty", "backwards", "wrong_class", "wrong_user", "range", "different_headers", "duplicate_group", "negative"} {
		t.Run(fault, func(t *testing.T) {
			source, d, _ := pageTailSource(t, "user_usage", nil)
			b := d.r.Wallet.Boundary
			row := serverUsageWire(b)
			d.serverOverride = [][]driver.Value{row}
			switch fault {
			case "absent":
				d.serverOverride = [][]driver.Value{}
			case "null_amount":
				row[9] = nil
			case "null_count":
				row[0] = nil
			case "float_amount":
				row[9] = "9.0"
			case "exponent_amount":
				row[9] = "9e0"
			case "overflow":
				row[9] = "9223372036854775808"
			case "null_quota_count":
				row[8] = int64(0)
			case "missing_join":
				row[0] = int64(2)
			case "empty_hides_money":
				row[0] = int64(0)
			case "two_empty":
				row = []driver.Value{int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), "", int64(0), int64(0), "0", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)}
				d.serverOverride = [][]driver.Value{row, append([]driver.Value(nil), row...)}
			case "backwards":
				row[1], row[3] = int64(0), int64(0)
			case "wrong_class":
				row[6] = "invented_wallet"
			case "wrong_user":
				row[4] = int64(202)
			case "range":
				row[15] = b + 2
			case "different_headers":
				other := append([]driver.Value(nil), row...)
				other[0] = int64(2)
				d.serverOverride = append(d.serverOverride, other)
			case "duplicate_group":
				row[0] = int64(2)
				d.serverOverride = append(d.serverOverride, append([]driver.Value(nil), row...))
			case "negative":
				row[10] = int64(-1)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			q, args, err := serverTailUsageQuery(d.r)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := source.QueryContext(ctx, q, args...)
			if err != nil {
				t.Fatal(err)
			}
			usage, _, _, err := readServerTailUsage(rows, d.r)
			if err == nil || usage != nil || source.Stats().InUse != 0 {
				t.Fatal("invalid transport yielded partial money or leaked rows", usage, err)
			}
		})
	}
}

func TestWalletTailServerUsageFullPageRequiresAnExplicitFinalRead(t *testing.T) {
	source, d, _ := pageTailSource(t, "user_usage", nil)
	b := d.r.Wallet.Boundary
	row := serverUsageWire(b)
	row[0], row[3], row[7], row[8], row[9], row[10], row[11], row[13] = int64(walletTailPageRows), int64(walletTailPageRows), int64(walletTailPageRows), int64(walletTailPageRows), "8192", int64(1), int64(1), int64(walletTailPageRows)
	d.serverOverride = [][]driver.Value{row}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	q, args, _ := serverTailUsageQuery(d.r)
	rows, err := source.QueryContext(ctx, q, args...)
	if err != nil {
		t.Fatal(err)
	}
	usage, next, end, err := readServerTailUsage(rows, d.r)
	if err != nil || end || len(usage) != 1 || usage[0].Quota != walletTailPageRows {
		t.Fatal("full page was assumed to be complete", err)
	}
	r := d.r
	r.After = next
	d.serverOverride = [][]driver.Value{{int64(0), next.At, next.Type, next.ID, int64(0), int64(0), "", int64(0), int64(0), "0", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)}}
	q, args, _ = serverTailUsageQuery(r)
	rows, err = source.QueryContext(ctx, q, args...)
	if err != nil {
		t.Fatal(err)
	}
	usage, got, end, err := readServerTailUsage(rows, r)
	if err != nil || !end || len(usage) != 0 || got != next {
		t.Fatal("empty terminal page invented data or advanced the cursor", err)
	}
}
