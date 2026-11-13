package monitor

import (
	"context"
	"errors"
	"sync"

	"gorm.io/gorm"
)

func initTokenDetectionClaims(tx *gorm.DB) error {
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS token_detection_claims_v1 (
	 source_epoch TEXT NOT NULL,log_id INTEGER NOT NULL,lease_token TEXT NOT NULL,lease_until INTEGER NOT NULL,
	 PRIMARY KEY(source_epoch,log_id))`).Error; err != nil {
		return err
	}
	return tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_claim_expiry_v1 ON token_detection_claims_v1(lease_until,source_epoch,log_id)`).Error
}

func (m *Monitor) claimTokenDetectionJobs(ctx context.Context, jobs []TokenMeteringFact, lease string, now int64) ([]TokenMeteringFact, error) {
	claimed := make([]TokenMeteringFact, 0, len(jobs))
	for start := 0; start < len(jobs); start += 32 {
		end := min(start+32, len(jobs))
		var batch []TokenMeteringFact
		err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
			if start == 0 {
				if err := tx.Exec(`DELETE FROM token_detection_claims_v1 WHERE (source_epoch,log_id) IN
				 (SELECT source_epoch,log_id FROM token_detection_claims_v1 WHERE lease_until<=? ORDER BY lease_until LIMIT 32)`, now).Error; err != nil {
					return err
				}
			}
			for _, job := range jobs[start:end] {
				r := tx.Exec(`INSERT INTO token_detection_claims_v1(source_epoch,log_id,lease_token,lease_until) VALUES(?,?,?,?)
				 ON CONFLICT(source_epoch,log_id) DO UPDATE SET lease_token = excluded.lease_token,
				 lease_until = excluded.lease_until
				 WHERE token_detection_claims_v1.lease_until<=? OR token_detection_claims_v1.lease_token IN (?,?)`, job.SourceEpoch, job.LogID, lease, now+35, now, lease, m.tokenDetectionFinishedLease)
				if r.Error != nil {
					return r.Error
				}
				if r.RowsAffected == 1 {
					batch = append(batch, job)
				}
			}
			return nil
		})
		if err != nil {
			return claimed, err
		}
		claimed = append(claimed, batch...)
	}
	return claimed, nil
}

func (m *Monitor) prepareTokenDetectionBatch(ctx context.Context, jobs []TokenMeteringFact, p tokenDetectionPolicy, lease string, now int64) ([]*tokenDetectionPrepared, []error, error) {
	plans := make([]*tokenDetectionPrepared, len(jobs))
	errs := make([]error, len(jobs))
	work := make(chan int, len(jobs))
	for i := range jobs {
		work <- i
	}
	close(work)
	var wg sync.WaitGroup
	for worker := 0; worker < min(m.cfg.tokenDetectionWorkers(), len(jobs)); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				errs[i] = m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
					return reader.Transaction(func(tx *gorm.DB) error {
						var err error
						plans[i], err = prepareTokenDetection(tx, jobs[i].SourceEpoch, jobs[i].LogID, p, now)
						if plans[i] != nil {
							plans[i].ClaimToken = lease
						}
						return err
					})
				})
			}
		}()
	}
	wg.Wait()
	var failed error
	for _, err := range errs {
		if err != nil && !errors.Is(err, errTokenDetectionFactRemoved) && !errors.Is(err, errTokenDetectionPending) && !errors.Is(err, errTokenDetectionCapacity) {
			failed = errors.Join(failed, err)
		}
	}
	return plans, errs, failed
}
