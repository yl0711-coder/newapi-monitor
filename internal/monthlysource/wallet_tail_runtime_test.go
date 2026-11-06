package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWalletTailRuntimeResumeFailureDoesNotPublishPartialOriginal(t *testing.T) {
	for _, stream := range []string{"usage", "funding", "cancel"} {
		t.Run(stream, func(t *testing.T) {
			_, legacy, receiver, _, key, scope, _, now := tailCaptureFixture(t)
			boundary := walletTailMonthStart(now.UnixMilli())
			d := &pageTailDriver{s: legacy.s, r: WalletTailPageRequest{Wallet: WalletTailRequest{Boundary: boundary}}}
			// No fixture aggregate is substituted for raw source rows. Usage
			// exceeds one page; funding exceeds two, with identical timestamps.
			n := walletTailPageRows + 1
			if stream == "funding" {
				n = 1
			}
			for i := 0; i < n; i++ {
				d.items = append(d.items, pageTailItem{id: int64(10000 + i), at: boundary + 1, typ: 2, user: 201, quota: 1, billing: "wallet"})
			}
			if stream == "funding" {
				for i := 0; i < 401; i++ {
					d.items = append(d.items, pageTailItem{id: int64(100000 + i), at: boundary + 2, typ: 1, user: 201, content: "使用Creem充值成功，充值额度: 1，支付金额：0.01"})
				}
			}
			d.s.data[0][2], d.s.data[0][3] = int64(10000), int64(20000)
			name := fmt.Sprintf("tail-runtime-%d", walletDriverSequence.Add(1))
			sql.Register(name, d)
			source, err := sql.Open(name, "")
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			source.SetMaxOpenConns(1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			resumed := false
			d.onQuery = func(q string) {
				if strings.Contains(q, "AS usage_candidates") || stream == "funding" && strings.Contains(q, "AS funding_candidates") && strings.Contains(q, "id>?") {
					resumed = true
					if stream == "cancel" {
						cancel()
					} else {
						d.fail = "index"
					}
				}
			}
			p, err := captureNextWalletTailMonthView(ctx, source, receiver, key, scope, true, func() time.Time { return now })
			if err == nil || p != (WalletTailCaptureProgress{}) || !resumed {
				t.Fatal("incomplete pages became a complete original", p, err)
			}
			if stream == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("runtime lost cancellation", err)
			}
			var count int
			if err := receiver.QueryRow("SELECT count(*) FROM portal_monthly_wallet_tail_captures").Scan(&count); err != nil || count != 0 {
				t.Fatal("failed page traversal left a committed original", count, err)
			}
			// The next explicit queue step may retry, but must start a fresh
			// complete source view rather than reuse the failed view's prefix.
			d.fail, d.onQuery = "", nil
			p = tailCaptureStep(t, source, receiver, key, scope, true, now)
			_, set, err := inspectWalletTailSet(t.Context(), receiver, key, scope, now)
			if err != nil || !p.Captured || p.Completed != 1 || len(set.pairs[0].Older.Usage) != 1 || set.pairs[0].Older.Usage[0].Records != int64(n) {
				t.Fatal("explicit recovery lost or duplicated exact usage", p, err)
			}
			if stream == "funding" && len(set.pairs[0].Older.Funding) != 401 {
				t.Fatal("explicit recovery lost funding rows")
			}
		})
	}
}
