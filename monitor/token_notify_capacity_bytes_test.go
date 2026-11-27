package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestTokenNotifyCapacityCountsUTF8BytesAndRecoversAfterPayloadClear(t *testing.T) {
	db, e := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		t.Fatal(e)
	}
	pool, e := db.DB()
	if e != nil {
		t.Fatal(e)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = pool.Close() })
	if e = db.AutoMigrate(&TokenNotifyOutbox{}, &tokenNotifyTarget{}, &TokenDeliveryAudit{}); e != nil {
		t.Fatal(e)
	}
	g := tokenNotifyGroup{payload: tokenNotifyPayload{Version: tokenDeliveryVersion, Severity: "SEV3", Evidence: "exact", Model: "中文模型", Count: 1}, fingerprints: []string{"one"}}
	p := tokenNotifyPolicy{Version: "synthetic", MaxAgeSeconds: 3600, CooldownSeconds: 60, EscalationFactor: 2}
	body := tokenDetectionJSON(g.payload)
	const budget = 16 << 20
	// A one-byte overflow in aggregate, with every preexisting payload still
	// under the 128KiB per-notification cap and all row-count caps respected.
	remaining := budget - len(body) + 1
	var batch []TokenNotifyOutbox
	for i := 0; remaining > 0; i++ {
		size := min(remaining, 90<<10)
		payload := strings.Repeat("中", size/3) + strings.Repeat("a", size%3)
		batch = append(batch, TokenNotifyOutbox{ID: fmt.Sprintf("retained-%03d", i), TargetKey: "other", State: "sent", Payload: payload})
		remaining -= len(payload)
	}
	if e = db.CreateInBatches(&batch, 20).Error; e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return tokenNotifyEnqueue(tx, "new-target", g, p, 1000) })
	if !errors.Is(err, errTokenDeliveryCapacity) {
		t.Fatalf("UTF8 byte overflow must be rejected, not counted as characters: %v", err)
	}
	for _, model := range []any{&tokenNotifyTarget{}, &TokenDeliveryAudit{}} {
		var n int64
		if e = db.Model(model).Count(&n).Error; e != nil || n != 0 {
			t.Fatalf("rejected capacity attempt left side effects: count=%d err=%v", n, e)
		}
	}
	var count int64
	if e = db.Model(&TokenNotifyOutbox{}).Count(&count).Error; e != nil || count != int64(len(batch)) {
		t.Fatal("rejected attempt changed outbox rows")
	}
	if e = db.Model(&TokenNotifyOutbox{}).Where("id=?", batch[0].ID).Update("payload", "").Error; e != nil {
		t.Fatal(e)
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return tokenNotifyEnqueue(tx, "new-target", g, p, 1000) })
	if err != nil {
		t.Fatalf("clearing retained payload must restore byte capacity: %v", err)
	}
	var got TokenNotifyOutbox
	if e = db.Where("target_key=?", "new-target").First(&got).Error; e != nil || got.Payload != body || got.State != "pending" {
		t.Fatalf("normal enqueue changed: %v", e)
	}
	// Restore exactly the byte budget, then replace a terminal notification.
	// Its old payload must be subtracted in the same unit, not counted twice.
	if e = db.Model(&TokenNotifyOutbox{}).Where("id=?", batch[0].ID).Update("payload", strings.Repeat("a", len(batch[0].Payload)-1)).Error; e != nil {
		t.Fatal(e)
	}
	if e = db.Model(&TokenNotifyOutbox{}).Where("id=?", got.ID).Update("state", "sent").Error; e != nil {
		t.Fatal(e)
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return tokenNotifyEnqueue(tx, "new-target", g, p, 1001) })
	if err != nil {
		t.Fatalf("same-size reuse at exact byte cap must fit: %v", err)
	}
	if e = db.Model(&TokenNotifyOutbox{}).Where("id=?", got.ID).Update("state", "sent").Error; e != nil {
		t.Fatal(e)
	}
	g.payload.From = 1000000000 // same frozen identity, a longer window field
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return tokenNotifyEnqueue(tx, "new-target", g, p, 1002) })
	if !errors.Is(err, errTokenDeliveryCapacity) {
		t.Fatalf("growing terminal payload above cap must be rejected: %v", err)
	}
	var after TokenNotifyOutbox
	if e = db.Where("id=?", got.ID).First(&after).Error; e != nil || after.State != "sent" || after.Payload != body {
		t.Fatal("rejected terminal reuse changed frozen payload/state")
	}
	var used int64
	if e = db.Model(&TokenNotifyOutbox{}).Select(tokenNotifyPayloadBytesSQL).Scan(&used).Error; e != nil || used != budget {
		t.Fatalf("stored byte count changed: got=%d want=%d err=%v", used, budget, e)
	}
}

func TestTokenNotifyStoredPayloadByteUnits(t *testing.T) {
	db, e := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		t.Fatal(e)
	}
	pool, e := db.DB()
	if e != nil {
		t.Fatal(e)
	}
	pool.SetMaxOpenConns(1)
	defer pool.Close()
	if e = db.AutoMigrate(&TokenNotifyOutbox{}); e != nil {
		t.Fatal(e)
	}
	var got int64
	if e = db.Model(&TokenNotifyOutbox{}).Select(tokenNotifyPayloadBytesSQL).Scan(&got).Error; e != nil || got != 0 {
		t.Fatal("empty outbox is not zero bytes")
	}
	values := []any{"ascii", "中文通知", "🧾", "a\x00尾", nil, []byte{0, 255, 1}, ""}
	var want int64
	for i, v := range values {
		switch s := v.(type) {
		case string:
			want += int64(len(s))
		case []byte:
			want += int64(len(s))
		}
		if e = db.Exec("INSERT INTO token_notify_outboxes(id,payload) VALUES(?,?)", fmt.Sprint(i), v).Error; e != nil {
			t.Fatal(e)
		}
	}
	if e = db.Model(&TokenNotifyOutbox{}).Select(tokenNotifyPayloadBytesSQL).Scan(&got).Error; e != nil || got != want {
		t.Fatalf("byte units differ: got=%d want=%d err=%v", got, want, e)
	}
}
