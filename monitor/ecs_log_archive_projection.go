package monitor

import (
	"errors"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

// A single grouped query inside the source snapshot, never one AWS/SQL query
// per row. Closure is a collector-delivery proof, not report coverage.
func projectArchiveDelivery(tx *gorm.DB, sources []ECSLogSource) error {
	var rows []struct {
		Node, Lane                          string
		Closed, Pending, Rejected, ClosedAt int64
		FinalVerified                       int64
	}
	err := tx.Model(&ECSLogArchiveReceipt{}).Select(`node,lane,
		SUM(CASE WHEN status=200 AND kind=? THEN 1 ELSE 0 END) AS closed,
		MAX(CASE WHEN status=200 AND kind=? THEN archive_at ELSE 0 END) AS closed_at,
		SUM(CASE WHEN status IN (400,403,409,410,422) THEN 1 ELSE 0 END) AS rejected,
		SUM(CASE WHEN status NOT IN (200,400,403,409,410,422) THEN 1 ELSE 0 END) AS pending,
		SUM(CASE WHEN status=200 AND final_boundary_verified=1 THEN 1 ELSE 0 END) AS final_verified`,
		ecsarchive.CollectorClosure, ecsarchive.CollectorClosure).
		Group("node,lane").Limit(ecsLogSourceLimit + 1).Scan(&rows).Error
	if err != nil {
		return err
	}
	if len(rows) > ecsLogSourceLimit {
		return errors.New("archive source projection exceeds status budget")
	}
	positions := map[string]int{}
	for i := range sources {
		sources[i].ArchiveStatus = "awaiting_closure"
		sources[i].FinalBoundaryStatus = "awaiting_final_boundary"
		positions[sources[i].Node+"/"+sources[i].Lane] = i
	}
	for _, row := range rows {
		i, ok := positions[row.Node+"/"+row.Lane]
		if !ok {
			continue
		}
		s := &sources[i]
		switch {
		case row.Rejected > 0 || row.Closed > 1:
			s.ArchiveStatus = "archive_delivery_conflict"
		case row.Pending > 0:
			s.ArchiveStatus = "recovery_pending"
		case row.Closed == 1:
			s.ArchiveStatus, s.ArchiveClosedAt = "collector_chain_closed", row.ClosedAt
			if row.FinalVerified == 1 && s.StoppedAt > 0 && s.ProducerExitCode != nil && *s.ProducerExitCode == 0 && (s.StopCode == "ServiceSchedulerInitiated" || s.StopCode == "UserInitiated") {
				s.FinalBoundaryStatus = "retained_files_verified"
			}
		}
	}
	return nil
}
