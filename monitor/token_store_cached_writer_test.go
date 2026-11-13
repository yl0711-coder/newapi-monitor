package monitor

import (
	"context"
	"errors"
	"testing"

	"gorm.io/gorm"
)

func TestTokenStoreCachedWriterCancellationAndSnapshot(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	row := TokenMeteringWindow{SourceEpoch: "cached-writer-fixture", StartAt: now, EndAt: now + 300}
	ctx, cancel := context.WithCancel(context.Background())
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	})
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatal("canceled transaction was reported as committed", err)
	}
	reader := m.tokenReadStore().Begin()
	if reader.Error != nil {
		t.Fatal(reader.Error)
	}
	defer reader.Rollback()
	var count int64
	if err := reader.Model(&TokenMeteringWindow{}).Where("source_epoch=?", row.SourceEpoch).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("canceled write leaked into reader snapshot", count, err)
	}
	if err := m.tokenLocalWriteTransaction(context.Background(), func(tx *gorm.DB) error { return tx.Create(&row).Error }); err != nil {
		t.Fatal("prepared write could not be reused after cancellation", err)
	}
	if err := reader.Model(&TokenMeteringWindow{}).Where("source_epoch=?", row.SourceEpoch).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("writer changed an existing read snapshot", count, err)
	}
	if err := reader.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenReadStore().Model(&TokenMeteringWindow{}).Where("source_epoch=?", row.SourceEpoch).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("fresh read lost the committed retry", count, err)
	}
}
