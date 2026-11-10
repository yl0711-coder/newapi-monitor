package monitor

import "gorm.io/gorm"

func tokenDetectionIndexedJobs(db *gorm.DB, epoch, hash string, now int64, limit, cadence int, progress ...*tokenDetectionWindowProgress) ([]TokenMeteringFact, error) {
	var position *tokenDetectionWindowProgress
	if len(progress) > 0 {
		position = progress[0]
	}
	var jobs []TokenMeteringFact
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		jobs, err = tokenDetectionBoundedQueueJobs(tx, epoch, hash, now, limit, cadence, position)
		return err
	})
	return jobs, err
}
