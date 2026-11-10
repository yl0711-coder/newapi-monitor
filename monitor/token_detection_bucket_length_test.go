package monitor

import (
	"errors"
	"testing"

	"gorm.io/gorm"
)

func TestTokenDetectionSizeExtentFiltersOnlyImpossibleNewControls(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-120, 3, 10000, 0, 0)
	in, err := tokenDetectionInputOf(f)
	if err != nil {
		t.Fatal(err)
	}
	db, policy := m.tokenStore(), detectionPolicy()
	write := func(input tokenDetectionInput, previous *tokenDetectionStoredCurrent, old TokenDetectionBucket) (TokenDetectionBucket, error) {
		var scope TokenDetectionBucket
		err := db.Transaction(func(tx *gorm.DB) error {
			id, _, err := m.tokenDetectionReferenceScope(tx, input, policy, previous, old, true, false, now)
			if err != nil {
				return err
			}
			return tx.First(&scope, "id=?", id).Error
		})
		return scope, err
	}
	scope, err := write(in, nil, TokenDetectionBucket{})
	if err != nil || !scope.LengthBoundsKnown || scope.LengthMin != 1000 || scope.LengthMax != 1000 {
		t.Fatal("new scope did not capture its entire initial population", scope, err)
	}
	read := func() TokenDetectionBucket {
		t.Helper()
		var current TokenDetectionBucket
		if err := db.First(&current, "id=?", scope.ID).Error; err != nil {
			t.Fatal(err)
		}
		return current
	}
	arrival := in
	arrival.Fact.ChannelID = 4
	arrival.Fact.RequestLength = ptrTokenMetering(5000)
	if _, err := write(arrival, nil, TokenDetectionBucket{}); err != nil || read().Generation != scope.Generation {
		t.Fatal("impossible new size invalidated the scope", err)
	}
	arrival.Fact.RequestLength = ptrTokenMetering(1000 + policy.SizeTolerance)
	if _, err := write(arrival, nil, TokenDetectionBucket{}); err != nil || read().Generation == scope.Generation {
		t.Fatal("inclusive tolerance boundary failed to notify", err)
	}
	scope = read()
	arrival.Fact.RequestLength = ptrTokenMetering(5000)
	arrival.Fact.Revision++
	if _, err := write(arrival, nil, TokenDetectionBucket{}); err != nil || read().Generation == scope.Generation {
		t.Fatal("real historical evidence revision was hidden by current sizes", err)
	}
	arrival.Fact.Revision--
	if err := db.Model(&TokenDetectionBucket{}).Where("id=?", scope.ID).Updates(map[string]any{"length_bounds_known": false, "length_min": 0, "length_max": 0}).Error; err != nil {
		t.Fatal(err)
	}
	scope = read()
	if _, err := write(arrival, nil, TokenDetectionBucket{}); err != nil || read().Generation == scope.Generation {
		t.Fatal("unknown legacy population was excluded", err)
	}
	if _, err := write(in, nil, TokenDetectionBucket{}); err != nil || read().LengthBoundsKnown {
		t.Fatal("one evaluated head fabricated complete legacy size coverage", err)
	}
}

func TestTokenDetectionSizeExtentWidensAtomicallyWithoutNewBuckets(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now, 3, 10000, 0, 0)
	in, err := tokenDetectionInputOf(f)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	write := func(tx *gorm.DB) error {
		var err error
		id, _, err = m.tokenDetectionReferenceScope(tx, in, detectionPolicy(), nil, TokenDetectionBucket{}, true, false, now)
		return err
	}
	if err := m.tokenStore().Transaction(write); err != nil {
		t.Fatal(err)
	}
	read := func() TokenDetectionBucket {
		var scope TokenDetectionBucket
		if err := m.tokenStore().First(&scope, "id=?", id).Error; err != nil {
			t.Fatal(err)
		}
		return scope
	}
	before := read()
	in.Fact.RequestLength = ptrTokenMetering(5000)
	abort := errors.New("rollback extent")
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		if err := write(tx); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) || read() != before {
		t.Fatal("rolled back head leaked an extent update", err)
	}
	if err := m.tokenStore().Transaction(write); err != nil {
		t.Fatal(err)
	}
	after := read()
	var count int64
	if err := m.tokenStore().Model(&TokenDetectionBucket{}).Count(&count).Error; err != nil || count != 2 || after.LengthMin != 1000 || after.LengthMax != 5000 {
		t.Fatal("extent expansion created extra buckets or lost an earlier size", after, count, err)
	}
}
