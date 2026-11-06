package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Fault injection at the second explicit cancellation check: the independent
// file is written, but the job must not become executable.
type cancelAfterSchemaWrite struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelAfterSchemaWrite) Err() error {
	c.checks++
	if c.checks == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestEmptyFundingSchemaNeverCopiesCapturedObservations(t *testing.T) {
	for _, seconds := range []int64{3600, 86400} {
		name := "hourly"
		if seconds == 86400 {
			name = "daily"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := auditPrivateDir(t)
			p, err := newMonthBatch(ctx, 1, auditPlanFixture(t).Funding, seconds)
			if err != nil {
				t.Fatal(err)
			}
			var seed emptyFundingSchema
			first, second := p.Windows[0], p.Windows[1]
			create := func(w monthCaptureWindow) receipt {
				t.Helper()
				r, err := initializeWithEmptySchema(ctx, root, w.Job, batchManifest(p, w), true, &seed)
				if err != nil || !r.Created || r.ManifestSHA256 != w.ManifestSHA256 {
					t.Fatal("window identity or creation changed", r, err)
				}
				return r
			}
			create(first)
			if len(seed.raw) == 0 {
				t.Fatal("empty schema not retained")
			}
			seedHash := hash(seed.raw)
			writePage := func(w monthCaptureWindow) {
				t.Helper()
				db, err := openReceiver(ctx, filepath.Join(root, w.Job, "funding.sqlite"), true)
				if err != nil {
					t.Fatal(err)
				}
				scan := batchManifest(p, w).Older
				_, err = monthlysource.SaveFundingPage(ctx, db, monthlysource.FundingPage{Version: 1, Scan: scan,
					FenceID: 100, EndReached: true, StartedAtMS: 1790870400000, CompletedAtMS: 1790870400010,
					Rows: []monthlysource.FundingLog{{ID: 1, UserID: 201, CreatedAt: w.From, Type: 1,
						Content: "使用在线充值成功，充值金额: $1.000000，支付金额：1.00"}},
					Next: monthlysource.FundingCursor{At: w.From, ID: 1}})
				if err := errors.Join(err, db.Close()); err != nil {
					t.Fatal(err)
				}
			}
			checkCounts := func(w monthCaptureWindow, want int) {
				t.Helper()
				db, err := openReceiver(ctx, filepath.Join(root, w.Job, "funding.sqlite"), false)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				for _, table := range []string{"portal_monthly_funding_pages", "portal_monthly_funding_records"} {
					var n int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil || n != want {
						t.Fatal("captured observations leaked between windows", table, n, want, err)
					}
				}
				var integrity string
				if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
					t.Fatal("cloned schema invalid", integrity, err)
				}
			}
			// Simulate genuine captures AFTER the first manifest becomes usable.
			// The next window must still start empty, not copy the live first DB.
			writePage(first)
			firstPath := filepath.Join(root, first.Job, "funding.sqlite")
			firstHash := fileHash(t, firstPath)
			create(second)
			checkCounts(first, 1)
			checkCounts(second, 0)
			firstInfo, err := os.Stat(firstPath)
			if err != nil {
				t.Fatal(err)
			}
			secondInfo, err := os.Stat(filepath.Join(root, second.Job, "funding.sqlite"))
			if err != nil || os.SameFile(firstInfo, secondInfo) || secondInfo.Mode().Perm() != 0600 {
				t.Fatal("windows share an inode or have unsafe permissions", err)
			}
			writePage(second)
			checkCounts(second, 1)
			db, err := openReceiver(ctx, filepath.Join(root, second.Job, "funding.sqlite"), true)
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{"UPDATE portal_monthly_funding_pages SET payload=payload", "DELETE FROM portal_monthly_funding_records"} {
				if _, err := db.ExecContext(ctx, query); err == nil {
					t.Fatal("empty schema copy lost append-only protection")
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			checkCounts(second, 1)
			if fileHash(t, firstPath) != firstHash || hash(seed.raw) != seedHash {
				t.Fatal("another window changed the captured first DB or empty seed")
			}
			if r, err := initializeWithEmptySchema(ctx, root, first.Job, batchManifest(p, first), true, &seed); err == nil || r.Created {
				t.Fatal("seeded initialization overwrote an existing job")
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			third := p.Windows[2]
			if _, err := initializeWithEmptySchema(canceled, root, third.Job, batchManifest(p, third), true, &seed); !errors.Is(err, context.Canceled) {
				t.Fatal("seeded initialization ignored cancellation", err)
			}
			if _, err := os.Lstat(filepath.Join(root, third.Job)); !os.IsNotExist(err) {
				t.Fatal("canceled initialization created files", err)
			}
			pending, stop := context.WithCancel(ctx)
			defer stop()
			interrupted := &cancelAfterSchemaWrite{Context: pending, cancel: stop}
			if r, err := initializeWithEmptySchema(interrupted, root, third.Job, batchManifest(p, third), true, &seed); !errors.Is(err, context.Canceled) || r.Created {
				t.Fatal("interrupted schema copy was published", r, err)
			}
			thirdPath := filepath.Join(root, third.Job)
			if _, err := os.Stat(filepath.Join(thirdPath, "manifest.pending")); err != nil {
				t.Fatal("interrupted job lost its incomplete marker", err)
			}
			if _, err := os.Stat(filepath.Join(thirdPath, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("interrupted job became executable", err)
			}
			if _, _, err := readManifest(thirdPath, third.ManifestSHA256); err == nil {
				t.Fatal("incomplete seeded job accepted for capture")
			}
			checkCounts(third, 0)
			if fileHash(t, firstPath) != firstHash || hash(seed.raw) != seedHash {
				t.Fatal("interrupted copy changed existing observations")
			}
		})
	}
}
