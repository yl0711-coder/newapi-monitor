package monitor

import (
	"context"
	"testing"
	"time"
)

func TestTokenMeteringPressureReservesRepairWithoutStoppingForward(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rows       int64
		wantRepair bool
	}{{"soft", 80, true}, {"hard", 100, false}} {
		t.Run(tc.name, func(t *testing.T) {
			m, now, _ := tokenMeteringFixture(t)
			defer m.Close()
			m.cfg.TokenDetectionMaxRows = 100
			if err := m.storeDB.AutoMigrate(&tokenDetectionCapacity{}); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Create(&tokenDetectionCapacity{ID: 1, CurrentRows: tc.rows}).Error; err != nil {
				t.Fatal(err)
			}
			state := tokenMeteringStateRead(t, m)
			start := state.StartAt
			old := start - tokenMeteringWindowSeconds
			if err := m.storeDB.Model(&state).Update("start_at", old).Error; err != nil {
				t.Fatal(err)
			}
			window := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: old, EndAt: start, CursorAt: old, Done: true, SourceComplete: true}
			if err := m.storeDB.Create(&window).Error; err != nil {
				t.Fatal(err)
			}
			tokenMeteringSourceRow(t, m, 1, old+1, "retained-raw-request-id")
			for turn := int64(0); turn < 4; turn++ {
				at := now + turn*tokenMeteringWindowSeconds
				tokenMeteringSourceRow(t, m, 10+turn, start+turn*tokenMeteringWindowSeconds+1, "")
				if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(at, 0)); err != nil {
					t.Fatal(err)
				}
				var count int64
				if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id=1").Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				want := int64(0)
				if tc.wantRepair && turn == 3 {
					want = 1
				}
				if count != want {
					t.Fatalf("turn %d: repair count=%d want=%d", turn+1, count, want)
				}
				if got := tokenMeteringStateRead(t, m); got.ScanThrough <= start+turn*tokenMeteringWindowSeconds {
					t.Fatal("pressure stopped forward progress", got)
				}
			}
			if tc.wantRepair {
				if f := tokenMeteringFactRead(t, m, 1); f.RequestID != "retained-raw-request-id" {
					t.Fatal("repair lost original identity", f.RequestID)
				}
			} else {
				var saved TokenMeteringWindow
				if err := m.storeDB.First(&saved, "source_epoch=? AND start_at=?", state.SourceEpoch, old).Error; err != nil {
					t.Fatal(err)
				}
				if saved.CursorAt != old || saved.CursorID != 0 || !saved.Done {
					t.Fatal("hard pressure changed unstarted repair cursor", saved)
				}
			}
		})
	}
}
