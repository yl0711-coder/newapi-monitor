package monitor

import (
	"errors"

	"gorm.io/gorm"
)

var errTokenDetectionInputsChanged = errors.New("token detection inputs changed before commit")

// Preparation is read-only. Expensive reference searches, parsing and rule
// evaluation never reserve the SQLite writer. The commit validates every
// version dependency before publishing a result.
type tokenDetectionPrepared struct {
	Fact                  TokenMeteringFact
	ClaimToken            string
	Input                 tokenDetectionInput
	Previous              *tokenDetectionStoredCurrent
	Current               TokenDetectionCurrent
	Generation            TokenDetectionBucket
	Guard                 tokenReferenceGuard
	Policy                tokenDetectionPolicy
	Prior                 int64
	Reference             tokenReference
	NewReference          bool
	Complete              bool
	Stage                 int
	Unchanged, ClearTimer bool
	Frozen                tokenDetectionFrozen
	Brief                 tokenDetectionResult
	Body, Hash            string
	Assessment            tokenDetectionAssessment
}

func prepareTokenDetection(tx *gorm.DB, epoch string, id int64, p tokenDetectionPolicy, now int64) (*tokenDetectionPrepared, error) {
	a := &tokenDetectionPrepared{Policy: p}
	if err := tx.First(&a.Fact, "source_epoch=? AND log_id=?", epoch, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errTokenDetectionFactRemoved
		}
		return nil, err
	}
	// Association is a dependency of comparison rules, not of logged usage or
	// historical-price arithmetic. Every committed fact can receive a first
	// decision; link revision changes requeue it through the same fenced path.
	var stored tokenDetectionStoredCurrent
	found := tx.Model(&TokenDetectionCurrent{}).Select("*, ("+tokenDetectionCurrentNullCheck+") AS has_null_fields").Where("source_epoch=? AND log_id=?", epoch, id).Limit(1).Find(&stored)
	if found.Error != nil {
		return nil, found.Error
	}
	a.Current = stored.TokenDetectionCurrent
	if found.RowsAffected > 0 {
		a.Previous = &stored
	}
	var window TokenMeteringWindow
	if err := tx.Where("source_epoch=? AND start_at=?", epoch, a.Fact.CreatedAt/300*300).Limit(1).Find(&window).Error; err != nil {
		return nil, err
	}
	a.Complete = window.Done && window.SourceComplete && !window.Replay
	if a.Current.ReferenceBucket != "" {
		if err := tx.Where("id=?", a.Current.ReferenceBucket).Limit(1).Find(&a.Generation).Error; err != nil {
			return nil, err
		}
		a.Generation.ID = a.Current.ReferenceBucket
	}
	c := a.Current
	due := c.NextAt > 0 && c.NextAt <= now
	matches := !stored.HasNullFields && c.Revision > 0 && c.DetectionState != "" && c.FactRevision == a.Fact.Revision && c.LinkRevision == a.Fact.LinkRevision && c.PolicyHash == tokenDetectionPolicyHash(p) && c.SourceComplete == a.Complete && c.ReferenceGeneration == a.Generation.Generation
	if matches && !due {
		a.Unchanged = true
		return a, nil
	}
	a.Stage = c.Stage
	if due && a.Stage < 3 {
		a.Stage++
	}
	var err error
	a.Input, err = tokenInputForPolicy(a.Fact, p)
	if err != nil {
		return nil, err
	}
	if matches && due && !tokenComparable(a.Input) {
		ref, err := tokenReferenceFrom(tx, a.Input, p, c.Stage, c.EvaluatedAt)
		if err != nil {
			return nil, err
		}
		expected := tokenDetectionEvaluate(a.Input, p, ref, 0, a.Complete)
		expected.Reference.Samples = nil
		if tokenDetectionJSON(expected) == c.ResultJSON {
			a.Unchanged, a.ClearTimer = true, true
			return a, nil
		}
	}
	if err := tx.Where("source_epoch=? AND log_id=?", epoch, id).Order("key_id").Find(&a.Input.Refs).Error; err != nil {
		return nil, err
	}
	a.Reference, err = tokenReferenceFrom(tx, a.Input, p, a.Stage, now)
	if err != nil {
		return nil, err
	}
	a.Reference.SampleCount = len(a.Reference.Samples)
	if a.Reference.Status == "statistical" && a.Reference.ID == "" {
		a.Reference.ID = tokenDetectionHash(a.Reference)
		a.NewReference = true
	}
	a.Guard, err = tokenReferenceDependencies(tx, a.Input, p, a.referenceScope(now))
	if err != nil {
		return nil, err
	}
	prior, err := tokenCachePriorSignals(tx, a.Fact, a.Reference.Bucket, p)
	if err != nil {
		return nil, err
	}
	a.Prior = prior
	result := tokenDetectionEvaluate(a.Input, p, a.Reference, prior, a.Complete)
	a.Brief = result
	a.Brief.Reference.Samples = nil
	a.Frozen = tokenDetectionFrozen{Version: tokenDetectionVersion, Input: a.Input, Policy: p, Stage: a.Stage, CachePriorSignals: prior, Result: result}
	a.Body, a.Hash = tokenDetectionJSON(a.Frozen), tokenDetectionHash(a.Frozen)
	if len(a.Body) > 1<<20 {
		return nil, errTokenDetectionCapacity
	}
	a.Assessment = tokenAssessFrozen(a.Frozen)
	return a, nil
}

func (a *tokenDetectionPrepared) validate(tx *gorm.DB) error {
	f := a.Fact
	if a.ClaimToken != "" {
		var owner string
		if err := tx.Table("token_detection_claims_v1").Select("lease_token").Where("source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Scan(&owner).Error; err != nil {
			return err
		}
		if owner != a.ClaimToken {
			return errTokenDetectionLease
		}
	}
	var fact TokenMeteringFact
	err := tx.Select("revision,link_revision,link_state").First(&fact, "source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errTokenDetectionFactRemoved
	}
	if err != nil {
		return err
	}
	if fact.Revision != f.Revision || fact.LinkRevision != f.LinkRevision || fact.LinkState != f.LinkState {
		return errTokenDetectionInputsChanged
	}
	var head TokenDetectionCurrent
	found := tx.Select("revision,decision_hash,next_at,evaluated_at,reference_generation").Where("source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Limit(1).Find(&head)
	if found.Error != nil {
		return found.Error
	}
	if (found.RowsAffected > 0) != (a.Previous != nil) || head.Revision != a.Current.Revision || head.DecisionHash != a.Current.DecisionHash || head.NextAt != a.Current.NextAt || head.EvaluatedAt != a.Current.EvaluatedAt || head.ReferenceGeneration != a.Current.ReferenceGeneration {
		return errTokenDetectionInputsChanged
	}
	var w TokenMeteringWindow
	if err := tx.Select("done,source_complete,replay").Where("source_epoch=? AND start_at=?", f.SourceEpoch, f.CreatedAt/300*300).Limit(1).Find(&w).Error; err != nil {
		return err
	}
	if (w.Done && w.SourceComplete && !w.Replay) != a.Complete {
		return errTokenDetectionInputsChanged
	}
	if a.Unchanged {
		for _, before := range []TokenDetectionBucket{a.Generation} {
			if before.ID == "" {
				continue
			}
			var after TokenDetectionBucket
			if err := tx.Select("generation").Where("id=?", before.ID).Limit(1).Find(&after).Error; err != nil {
				return err
			}
			if after.Generation != before.Generation {
				return errTokenDetectionInputsChanged
			}
		}
		return nil
	}
	scope := a.Reference
	scope.From, scope.Through = a.Guard.From, a.Guard.Through
	after, err := tokenReferenceDependencies(tx, a.Input, a.Policy, scope)
	if err != nil {
		return err
	}
	if tokenDetectionHash(after) != tokenDetectionHash(a.Guard) {
		return errTokenDetectionInputsChanged
	}
	prior, err := tokenCachePriorSignals(tx, f, a.Reference.Bucket, a.Policy)
	if err != nil {
		return err
	}
	if prior != a.Prior {
		return errTokenDetectionInputsChanged
	}
	return nil
}

func (a *tokenDetectionPrepared) referenceScope(now int64) tokenReference {
	scope := a.Reference
	scope.From = a.Fact.CreatedAt - a.Policy.LookbackSeconds
	delays := []int64{0, 300, 600, 3600}
	scope.Through = min(a.Fact.CreatedAt+delays[min(3, max(0, a.Stage))], now+1)
	return scope
}
