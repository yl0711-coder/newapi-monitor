package monitor

import (
	"context"
	"testing"
)

func TestTokenDetectionLargeCorrectionQueueKeepsDueChangedRetries(t *testing.T) {
	for _, shape := range []struct {
		name  string
		count int
		base  bool
	}{{"fact_revision", 320, true}, {"reference_generation", 4200, false}} {
		t.Run(shape.name, func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
			hash := tokenDetectionPolicyHash(detectionPolicy())
			if err := db.Exec(`WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<?)
				INSERT INTO token_metering_facts(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,revision,link_revision)
				SELECT ?,n,?-14400+n/10,1,1,'queue-fixture','group',1,1 FROM ids`, shape.count, epoch, now).Error; err != nil {
				t.Fatal(err)
			}
			factRevision, bucket := 1, "reference-fixture"
			if shape.base {
				factRevision, bucket = 0, ""
			} else if err := db.Create(&TokenDetectionBucket{ID: bucket, Generation: 1, ChangedAt: now}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Exec(`INSERT INTO token_detection_currents
				(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,fact_revision,link_revision,policy_hash,reference_bucket,reference_generation,source_complete,next_at,candidate,result_json)
				SELECT source_epoch,log_id,created_at,user_id,channel_id,model,user_group,?,link_revision,?,?,0,0,0,0,'{"rules":[]}' FROM token_metering_facts`, factRevision, hash, bucket).Error; err != nil {
				t.Fatal(err)
			}
			fact := detectionFact(t, m, 90001, now-600, 1, 10000, 0, 0)
			fact.RequestLength = nil
			if err := db.Save(&fact).Error; err != nil {
				t.Fatal(err)
			}
			retry := TokenDetectionRetry{SourceEpoch: epoch, LogID: fact.LogID, FactRevision: fact.Revision - 1, LinkRevision: fact.LinkRevision, PolicyHash: hash, Attempts: 2, NextAt: now - 60, ExpiresAt: now + 86400, Reason: "local_capacity_exhausted"}
			if err := db.Create(&retry).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Transaction(initTokenDetectionReportIndex); err != nil {
				t.Fatal(err)
			}
			for rotation := int64(0); rotation < 4; rotation++ {
				jobs, err := tokenDetectionJobsBounded(db, epoch, hash, now+rotation*15, 250, 15)
				if err != nil {
					t.Fatal(err)
				}
				seen := map[int64]bool{}
				for _, job := range jobs {
					if seen[job.LogID] {
						t.Fatal("duplicate selected request", job.LogID)
					}
					seen[job.LogID] = true
				}
				if len(jobs) > 250 || !seen[fact.LogID] {
					t.Fatalf("due changed retry is absent from bounded queue: rotation=%d jobs=%d", rotation, len(jobs))
				}
			}
			// Selection alone cannot clear pending. Process the retained identity
			// under the normal lease, then verify the durable retry was removed.
			if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&TokenDetectionState{SourceEpoch: epoch, LeaseToken: "retry-proof", LeaseUntil: now + 30}).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.processTokenDetection(context.Background(), epoch, fact.LogID, detectionPolicy(), "retry-proof", now); err != nil {
				t.Fatal(err)
			}
			var count int64
			if err := db.Model(&TokenDetectionRetry{}).Where("source_epoch=? AND log_id=?", epoch, fact.LogID).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("successful processing did not clear retry: %d %v", count, err)
			}
			var head TokenDetectionCurrent
			if err := db.First(&head, "source_epoch=? AND log_id=?", epoch, fact.LogID).Error; err != nil || head.FactRevision != fact.Revision {
				t.Fatalf("missing committed current decision: %+v %v", head, err)
			}
			// Nothing in this recovery may modify the other queued facts.
			if err := db.Model(&TokenMeteringFact{}).Where("source_epoch=?", epoch).Count(&count).Error; err != nil || count != int64(shape.count+1) {
				t.Fatalf("retained fact population changed: %d %v", count, err)
			}
		})
	}
}
