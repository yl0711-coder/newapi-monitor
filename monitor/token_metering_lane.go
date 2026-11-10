package monitor

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// Observations are diagnostic only. Window cursors and committed row counts
// are updated in the fact transaction, never from these in-memory values.
type tokenMeteringLaneRun struct {
	Window        TokenMeteringWindow
	Read          backgroundSourceReadTiming
	RowsRead      int64
	RowsCommitted int64
	Write         tokenWriteTiming
	Reason, Stage string
	Deferred      bool
}

type tokenMeteringCommitTimingKey struct{}

func addTokenMeteringCommitTiming(ctx context.Context, timing *tokenWriteTiming) {
	if total, ok := ctx.Value(tokenMeteringCommitTimingKey{}).(*tokenWriteTiming); ok {
		total.Wait += timing.Wait
		total.Connection += timing.Connection
		total.Lock += timing.Lock
		total.Work += timing.Work
		total.Commit += timing.Commit
		total.Stage = timing.Stage
	}
}

func tokenMeteringReleaseLanes(tx *gorm.DB, epoch, lease string, now int64, runs []*tokenMeteringLaneRun) error {
	for _, run := range runs {
		lane := run.Window.Lane
		var w TokenMeteringWindow
		if err := tx.First(&w, "source_epoch=? AND start_at=?", epoch, run.Window.StartAt).Error; err != nil {
			return err
		}
		updates := map[string]any{
			"last_read_ms": run.Read.Query.Milliseconds(), "last_decode_ms": run.Read.Decode.Milliseconds(),
			"last_writer_wait_ms": (run.Write.Wait + run.Write.Connection + run.Write.Lock).Milliseconds(),
			"last_commit_ms":      run.Write.Commit.Milliseconds(), "rows_read": gorm.Expr("rows_read+?", run.RowsRead),
			"cursor_at": w.CursorAt, "cursor_id": w.CursorID, "next_at": w.NextAt, "attempt": w.Attempts,
			"updated_at": now,
		}
		elapsed := run.Read.Query + run.Read.Wait + run.Write.Wait + run.Write.Connection + run.Write.Lock + run.Write.Work + run.Write.Commit
		if elapsed > 0 && run.RowsCommitted > 0 {
			updates["throughput_rows_per_second"] = float64(run.RowsCommitted) / elapsed.Seconds()
		}
		if run.Deferred {
			updates["deferred"] = gorm.Expr("deferred+1")
			updates["last_reason"], updates["last_failure_stage"] = run.Reason, run.Stage
		}
		if err := tx.Model(&TokenMeteringLaneState{}).Where("source_epoch=? AND lane=? AND lease_token=?", epoch, lane, lease).Updates(updates).Error; err != nil {
			return err
		}
		w.RowsRead += run.RowsRead
		w.ReadWaitMS, w.ReadQueryMS = run.Read.Wait.Milliseconds(), run.Read.Query.Milliseconds()
		w.ReadConnectionMS, w.ReadExecuteMS = run.Read.Connection.Milliseconds(), run.Read.Execute.Milliseconds()
		w.ReadReceiveMS, w.ReadDecodeMS = run.Read.Receive.Milliseconds(), run.Read.Decode.Milliseconds()
		w.WriterWaitMS, w.CommitMS = (run.Write.Wait + run.Write.Connection + run.Write.Lock).Milliseconds(), run.Write.Commit.Milliseconds()
		w.UpdatedAt = now
		if err := tx.Save(&w).Error; err != nil {
			return err
		}
		fields := map[string]any{
			"last_read_wait_ms": w.ReadWaitMS, "last_read_query_ms": w.ReadQueryMS,
			"last_read_connection_ms": w.ReadConnectionMS, "last_read_execute_ms": w.ReadExecuteMS,
			"last_read_receive_ms": w.ReadReceiveMS, "last_read_decode_ms": w.ReadDecodeMS,
			"last_writer_wait_ms": w.WriterWaitMS, "last_commit_ms": w.CommitMS,
		}
		if run.Read.Query > 0 {
			fields["last_read_at"] = now
		}
		if run.Deferred {
			fields["last_reason"] = run.Reason
		}
		if err := tx.Model(&TokenMeteringState{}).Where("source_epoch=? AND lease_token=?", epoch, lease).Updates(fields).Error; err != nil {
			return err
		}
	}
	return tx.Model(&TokenMeteringLaneState{}).Where("source_epoch=? AND lease_token=?", epoch, lease).
		Updates(map[string]any{"running": 0, "lease_until": 0, "lease_token": "", "updated_at": now}).Error
}

// Limits are per lane, inside the hard 25-second collection round. One slow
// historical read cannot consume the realtime lane's SQL allowance. Production
// reads remain serial through the existing shared source gate.
func tokenMeteringLaneBudget(lane tokenMeteringLane) time.Duration {
	if lane == tokenMeteringLaneReplay {
		return 4 * time.Second
	}
	return 5 * time.Second
}

func tokenMeteringLaneWaitBudget(lane tokenMeteringLane) time.Duration {
	if lane == tokenMeteringLaneReplay {
		return 2 * time.Second
	}
	return 6 * time.Second
}
