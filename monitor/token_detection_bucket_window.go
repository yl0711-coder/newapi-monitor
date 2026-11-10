package monitor

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const tokenDetectionBucketWindowSeconds int64 = 3600

// Bound a correction's metadata work independently of the retained fact count.
// Parents and channel/hour scopes also share the original total bucket budget.
const tokenDetectionMaxBucketWindows = 4096

// A new benign request can change controls only within the reference lookback
// and the staged one-hour lookahead. It must not invalidate every retained
// result for that user/model. Real revisions and quarantines still invalidate
// every existing scope, including decisions using an older frozen anchor.
// The existing generation reads remain authoritative; no head/fact watermark
// or pending projection is advanced by this metadata update.
func (m *Monitor) tokenDetectionReferenceScope(tx *gorm.DB, in tokenDetectionInput, p tokenDetectionPolicy, previous *tokenDetectionStoredCurrent, oldBucket TokenDetectionBucket, changed, quarantined bool, now int64) (string, int64, error) {
	parent := ""
	if tokenComparable(in) {
		parent = tokenReferenceBucket(in, p)
	}
	oldParent := oldBucket.ID
	if oldBucket.ParentBucket != "" {
		oldParent = oldBucket.ParentBucket
	}
	if parent != "" {
		if err := m.ensureTokenDetectionParentBucket(tx, parent, now); err != nil {
			return "", 0, err
		}
	}
	if changed {
		parents := []string{parent}
		if oldParent != "" && oldParent != parent {
			parents = append(parents, oldParent)
		}
		// An earlier missing-entry decision has never supplied comparable
		// evidence. Its first successful association is a new control with the
		// same time/channel bounds as a first evaluation, not a revision to an
		// old frozen anchor. Already comparable or revised facts still require
		// conservative invalidation of retained anchors.
		hadComparableEvidence := previous != nil && previous.ReferenceBucket != ""
		global := hadComparableEvidence || quarantined || in.Fact.Revision > 1
		for _, key := range parents {
			if key == "" {
				continue
			}
			if err := tx.Model(&TokenDetectionBucket{}).Where("id=? AND parent_bucket=''", key).
				Updates(map[string]any{"generation": gorm.Expr("generation+1"), "changed_at": now}).Error; err != nil {
				return "", 0, err
			}
			children := tx.Model(&TokenDetectionBucket{}).Where("parent_bucket=?", key)
			if !global {
				from := (in.Fact.CreatedAt - 3600) / tokenDetectionBucketWindowSeconds * tokenDetectionBucketWindowSeconds
				through := (in.Fact.CreatedAt + p.LookbackSeconds) / tokenDetectionBucketWindowSeconds * tokenDetectionBucketWindowSeconds
				// Same-channel requests never enter the control population. They
				// may affect sustained cache signals only when quarantined, which
				// still takes the conservative correction path above.
				children = children.Where("window_start>=? AND window_start<=? AND channel_id<>?", from, through, in.Fact.ChannelID)
				if length := in.Fact.RequestLength; length != nil && *length > 0 {
					// A benign new control outside every target size's absolute
					// tolerance cannot alter any decision in this scope. Keep a
					// conservative extent, not another per-size bucket population.
					// Legacy scopes have unknown bounds and remain notified. Actual
					// revisions/quarantines above still invalidate frozen anchors
					// regardless of their former lengths or event-time horizons.
					low := max(int64(1), *length-p.SizeTolerance)
					high := *length + min(p.SizeTolerance, int64(^uint64(0)>>1)-*length)
					children = children.Where("(length_bounds_known=0 OR (length_min<=? AND length_max>=?))", high, low)
				}
			}
			if err := children.Updates(map[string]any{"generation": gorm.Expr("(SELECT generation FROM token_detection_buckets WHERE id=?)", key), "changed_at": now}).Error; err != nil {
				return "", 0, err
			}
		}
	}
	if parent == "" {
		return "", 0, nil
	}
	start := in.Fact.CreatedAt / tokenDetectionBucketWindowSeconds * tokenDetectionBucketWindowSeconds
	id := tokenDetectionScopeID(parent, start, in.Fact.ChannelID)
	var scope TokenDetectionBucket
	found := tx.Where("id=?", id).Limit(1).Find(&scope)
	if found.Error != nil {
		return "", 0, found.Error
	}
	if found.RowsAffected == 0 {
		var count int64
		if err := tx.Model(&TokenDetectionBucket{}).Where("parent_bucket=?", parent).Count(&count).Error; err != nil {
			return "", 0, err
		}
		if count >= tokenDetectionMaxBucketWindows {
			return "", 0, errTokenDetectionCapacity
		}
		if err := m.tokenDetectionBucketCapacity(tx); err != nil {
			return "", 0, err
		}
		var root TokenDetectionBucket
		if err := tx.First(&root, "id=?", parent).Error; err != nil {
			return "", 0, err
		}
		scope = TokenDetectionBucket{ID: id, ParentBucket: parent, WindowStart: start, ChannelID: in.Fact.ChannelID, Generation: root.Generation, ChangedAt: now,
			LengthMin: *in.Fact.RequestLength, LengthMax: *in.Fact.RequestLength, LengthBoundsKnown: true}
		if err := tx.Create(&scope).Error; err != nil {
			return "", 0, err
		}
	} else if scope.LengthBoundsKnown && (*in.Fact.RequestLength < scope.LengthMin || *in.Fact.RequestLength > scope.LengthMax) {
		// Bounds only widen. Moving/removing a head may leave extra wakeups,
		// but cannot exclude a retained head or require a full-scope rescan.
		// Never declare an upgraded, previously unknown population complete
		// merely because one of its heads was evaluated after migration.
		if err := tx.Model(&TokenDetectionBucket{}).Where("id=?", id).Updates(map[string]any{
			"length_min": min(scope.LengthMin, *in.Fact.RequestLength), "length_max": max(scope.LengthMax, *in.Fact.RequestLength),
		}).Error; err != nil {
			return "", 0, err
		}
	}
	return scope.ID, scope.Generation, nil
}

func tokenDetectionScopeID(parent string, start, channel int64) string {
	return tokenDetectionHash([]any{"reference-notification-window-v2", parent, start, channel})
}

func (m *Monitor) ensureTokenDetectionParentBucket(tx *gorm.DB, key string, now int64) error {
	var exists bool
	if err := tx.Raw("SELECT EXISTS(SELECT 1 FROM token_detection_buckets WHERE id=?)", key).Scan(&exists).Error; err != nil || exists {
		return err
	}
	if err := m.tokenDetectionBucketCapacity(tx); err != nil {
		return err
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&TokenDetectionBucket{ID: key, ChangedAt: now}).Error
}

func (m *Monitor) tokenDetectionBucketCapacity(tx *gorm.DB) error {
	var count int64
	if err := tx.Model(&TokenDetectionBucket{}).Count(&count).Error; err != nil {
		return err
	}
	maxReferences, _ := m.cfg.tokenDetectionReferenceLimits()
	if count >= maxReferences {
		return errTokenDetectionCapacity
	}
	return nil
}

// A scope is created only with a committed head, and is removed after its
// last head moves or expires. Parents keep the original bounded identities.
func cleanupTokenDetectionBucketScopes(tx *gorm.DB, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return tx.Where("id IN ? AND parent_bucket<>''", ids).
		Where("NOT EXISTS (SELECT 1 FROM token_detection_currents c WHERE c.reference_bucket=token_detection_buckets.id)").
		Delete(&TokenDetectionBucket{}).Error
}
