package monthlydraft

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Site inception is not every customer's inception. Source ranges are attested
// server-side: neither a Monitor enrollment date nor an earliest retained file
// is proof of when an account started. This test checks the actual background
// worker/publication path after the distinct customer ranges have been chosen.
func TestAutomaticDisplaySeriesCustomerSpecificMayAndSeptemberStarts(t *testing.T) {
	for _, first := range []time.Month{time.May, time.September} {
		t.Run(first.String(), func(t *testing.T) {
			f := newDisplaySeriesFixtureFrom(t, first)
			ctx := context.Background()
			w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
			if err != nil {
				t.Fatal(err)
			}
			// Request the latest supported month first, then fill backwards.
			// Starting from the requested/latest month would lose earlier cents.
			var frozenLatest monitor.PortalMonthlyBill
			for i := len(f.plans) - 1; i >= 0; i-- {
				if _, err := f.g.Submit(ctx, f.plans[i], f.inputs[i]); err != nil {
					t.Fatal(err)
				}
				if worked, err := w.step(ctx); err != nil || !worked || w.Status().Published != uint64(len(f.plans)-i) {
					t.Fatal("customer-specific start did not publish", worked, err, w.Status())
				}
				var record monitor.PortalMonthlyBill
				if err := f.auth.First(&record, "group_id=1 AND month=?", f.requests[i].Month).Error; err != nil {
					t.Fatal(err)
				}
				var payload struct {
					Display struct {
						Organization struct{ Opening, Closing string } `json:"organization"`
					} `json:"display_balances"`
				}
				if err := json.Unmarshal([]byte(record.Payload), &payload); err != nil {
					t.Fatal(err)
				}
				// Independent integer cents: initial 2325.74, cost .005+.02
				// rounds to .03 per month, carried only within this customer's range.
				cents := int64(232574 - i*3)
				amount := func(n int64) string { return fmt.Sprintf("%d.%02d0000", n/100, n%100) }
				if payload.Display.Organization.Opening != amount(cents) || payload.Display.Organization.Closing != amount(cents-3) {
					t.Fatal("latest request moved the customer-specific origin", first, i, payload.Display)
				}
				if i == len(f.plans)-1 {
					frozenLatest = record
				}
				var latest monitor.PortalMonthlyBill
				if err := f.auth.First(&latest, "group_id=1 AND month='2026-09'").Error; err != nil || latest != frozenLatest {
					t.Fatal("filling an earlier month rewrote the issued latest month", err)
				}
			}
			want := int64(10 - first)
			displaySeriesCount(t, f, want)
			var tasks, selected int64
			if err := f.g.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_draft_tasks").Scan(&tasks); err != nil || tasks != want {
				t.Fatal("extra pre-start customer tasks", tasks, err)
			}
			if err := f.g.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_generation_inputs").Scan(&selected); err != nil || selected != want {
				t.Fatal("unexpected pre-start source requirements", selected, err)
			}
			cache, err := filepath.Glob(filepath.Join(f.g.root.Name(), "display-source-*.json"))
			if err != nil || int64(len(cache)) != want {
				t.Fatal("did not prepare exactly this customer's source range", len(cache), err)
			}
			for _, name := range cache {
				if info, err := os.Stat(name); err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("source cache is not private", err)
				}
			}
		})
	}
}
