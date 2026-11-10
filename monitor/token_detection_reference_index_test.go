package monitor

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenReferenceCandidateIndexAvoidsRejectedPayloads(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-60, 9, 20000, 0, 0)
	const count = 12000
	// Same user/model traffic is the real slow-query shape. Most rows fail
	// channel, size, group or association eligibility, after the old index seek.
	rows := make([]TokenMeteringFact, 0, count)
	for i := 0; i < count; i++ {
		r := f
		r.LogID, r.CreatedAt = int64(i+100), now-1800+int64(i%1700)
		r.SourceFacts = strings.Repeat("x", 8192)
		r.ChannelID = 3
		switch i % 5 {
		case 0:
			r.ChannelID = f.ChannelID
		case 1:
			r.RequestLength = ptrTokenMetering(5000)
		case 2:
			r.UserGroup = "different-group"
		case 3:
			r.LinkState = "ambiguous"
		case 4:
			r.EvidenceLevel = "missing"
		}
		rows = append(rows, r)
	}
	if err := m.tokenStore().CreateInBatches(rows, 128).Error; err != nil {
		t.Fatal(err)
	}
	query := func() *gorm.DB {
		return tokenReferenceCandidateQuery(m.tokenStore(), f, detectionPolicy(), now-3600, now)
	}
	var legacy, covered []int64
	started := time.Now()
	if err := query().Scan(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	legacyElapsed := time.Since(started)
	if err := initTokenDetectionReferenceIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	if err := query().Scan(&covered).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("rejected facts=%d legacy=%s indexed=%s", count, legacyElapsed, time.Since(started))
	if len(covered) != 0 || !reflect.DeepEqual(legacy, covered) {
		t.Fatal("candidate access path changed eligibility", legacy, covered)
	}
	// Check the actual bytecode, since SQLite may label a partial covering
	// index simply USING INDEX. A deferred table seek is harmless until a table
	// column is requested. No rejected fact may read its payload or a temp sort.
	var ids []int64
	q := query().Session(&gorm.Session{DryRun: true}).Scan(&ids)
	var program []struct {
		Opcode string
		P1, P2 int
		P4     string
	}
	if err := m.tokenStore().Raw("EXPLAIN "+q.Statement.SQL.String(), q.Statement.Vars...).Scan(&program).Error; err != nil {
		t.Fatal(err)
	}
	var root int
	if err := m.tokenStore().Raw("SELECT rootpage FROM sqlite_master WHERE type='table' AND name='token_metering_facts'").Scan(&root).Error; err != nil {
		t.Fatal(err)
	}
	factCursor := -1
	for _, op := range program {
		if op.Opcode == "OpenRead" && op.P2 == root {
			factCursor = op.P1
		}
	}
	for _, op := range program {
		if op.Opcode == "Column" && op.P1 == factCursor || op.Opcode == "SorterOpen" || op.Opcode == "Sort" {
			t.Fatalf("candidate page still reads wide facts or sorts: %+v", op)
		}
	}
}

func TestTokenReferenceCandidateIndexOrderCapsAndLateAdditions(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-60, 9, 20000, 0, 0)
	var controls []TokenMeteringFact
	for i := int64(0); i < 300; i++ {
		c := f
		c.LogID, c.CreatedAt, c.ChannelID = 100+i, now-1800+i/3, 2+i%2
		controls = append(controls, c)
	}
	if err := m.tokenStore().CreateInBatches(controls, 100).Error; err != nil {
		t.Fatal(err)
	}
	read := func(late bool) []int64 {
		t.Helper()
		q := tokenReferenceCandidateQuery(m.tokenStore(), f, detectionPolicy(), now-3600, now)
		if late {
			q = q.Where("f.created_at>?", now-1730).Limit(65)
		}
		var ids []int64
		if err := q.Scan(&ids).Error; err != nil {
			t.Fatal(err)
		}
		return ids
	}
	legacy, late := read(false), read(true)
	if len(legacy) != 201 || legacy[0] != 100 || legacy[200] != 300 || len(late) != 65 || late[0] != 313 {
		t.Fatal("fixture does not cover ordered caps and additions after first page", legacy, late)
	}
	if err := initTokenDetectionReferenceIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy, read(false)) || !reflect.DeepEqual(late, read(true)) {
		t.Fatal("candidate index changed the original cap/order or hid late additions")
	}
	if err := m.tokenStore().Migrator().DropIndex(&TokenMeteringFact{}, tokenReferenceCandidateIndex); err != nil {
		t.Fatal(err)
	}
	abort := errors.New("rollback index upgrade")
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		if err := initTokenDetectionReferenceIndex(tx); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) || m.tokenStore().Migrator().HasIndex(&TokenMeteringFact{}, tokenReferenceCandidateIndex) {
		t.Fatal("index upgrade escaped transaction", err)
	}
}
