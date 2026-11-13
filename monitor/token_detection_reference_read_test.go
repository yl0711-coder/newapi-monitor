package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type tokenReferenceReadTrace struct {
	logger.Interface
	SQL []string
}

func (l *tokenReferenceReadTrace) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	if strings.Contains(sql, "f.*") && strings.Contains(sql, "f.log_id IN") {
		l.SQL = append(l.SQL, sql)
	}
}

func TestTokenReferencePayloadReadSeeksSelectedIdentities(t *testing.T) {
	for _, size := range []int{3, 4, 64, 200} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			if err := initTokenDetectionReferenceIndex(m.tokenStore()); err != nil {
				t.Fatal(err)
			}
			f := detectionFact(t, m, 1, now-60, 9, 100000, 0, 0)
			var rows []TokenMeteringFact
			for i := 0; i < size; i++ {
				r := f
				r.LogID, r.CreatedAt, r.ChannelID = int64(100+i), now-600+int64((size-i)/2), int64(1+i%2)
				rows = append(rows, r)
			}
			if err := m.tokenStore().CreateInBatches(rows, 100).Error; err != nil {
				t.Fatal(err)
			}
			in, err := tokenDetectionInputOf(f)
			if err != nil {
				t.Fatal(err)
			}
			trace := &tokenReferenceReadTrace{Interface: logger.Default.LogMode(logger.Silent)}
			ref, err := tokenReferenceFrom(m.tokenStore().Session(&gorm.Session{Logger: trace}), in, detectionPolicy(), 0, now)
			if err != nil {
				t.Fatal(err)
			}
			if size <= 64 {
				if len(ref.Samples) != size {
					t.Fatalf("lost selected samples: %+v", ref)
				}
				for i := 1; i < len(ref.Samples); i++ {
					a, b := ref.Samples[i-1], ref.Samples[i]
					if a.CreatedAt > b.CreatedAt || a.CreatedAt == b.CreatedAt && a.LogID > b.LogID {
						t.Fatal("reference event ordering changed")
					}
				}
			}
			if len(trace.SQL) != 1 {
				t.Fatalf("expected one bounded payload read, got %d", len(trace.SQL))
			}
			for _, sql := range trace.SQL {
				var plan []struct{ Detail string }
				if err := m.tokenStore().Raw("EXPLAIN QUERY PLAN " + sql).Scan(&plan).Error; err != nil {
					t.Fatal(err)
				}
				seek := false
				for _, step := range plan {
					if strings.Contains(step.Detail, "SEARCH f ") {
						seek = strings.Contains(step.Detail, "source_epoch=? AND log_id=?")
					}
				}
				if !seek {
					t.Fatalf("selected controls scan unrelated retained facts: %+v", plan)
				}
			}
		})
	}
}

func TestTokenReferenceIdentityReadsPreserveEpochAndEmptySelection(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 81, now-60, 9, 100000, 0, 0)
	other := f
	other.SourceEpoch, other.Model = "other-epoch", "other-model"
	if err := m.tokenStore().Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]int64{nil, {81, 81, 999}} {
		var rows []TokenMeteringFact
		if err := tokenReferenceFactIdentities(m.tokenStore(), f.SourceEpoch, ids).Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if len(ids) == 0 {
			if len(rows) != 0 {
				t.Fatal("empty identities became an unbounded read")
			}
		} else if len(rows) != 1 || rows[0].SourceEpoch != f.SourceEpoch || rows[0].Model != f.Model {
			t.Fatal("identity selection mixed epochs, duplicated or invented a fact", rows)
		}
	}
}

// No source connection, migration, claim or write. Opt in with an existing
// local read-only mount to reproduce reference preparation on retained facts.
func TestTokenReferenceReadOnlyLivePreparation(t *testing.T) {
	file := os.Getenv("TOKEN_REFERENCE_READONLY_FILE")
	if file == "" {
		t.Skip("set TOKEN_REFERENCE_READONLY_FILE for a bounded local reference probe")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(os.Getenv("TOKEN_REFERENCE_POLICY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := parseTokenDetectionPolicy(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	epoch := os.Getenv("TOKEN_REFERENCE_EPOCH")
	parts := strings.Split(os.Getenv("TOKEN_REFERENCE_IDS"), ",")
	if epoch == "" || len(parts) > 4 {
		t.Fatal("one epoch and at most four identities required")
	}
	db, err := gorm.Open(sqlite.Open("file:"+file+"?mode=ro"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	if err := db.Exec("PRAGMA query_only=ON").Error; err != nil {
		t.Fatal(err)
	}
	type sample struct {
		ID                  int64 `json:"log_id"`
		ElapsedMS           int64 `json:"elapsed_ms"`
		Stage, Error, Trace string
		Reason              string                 `json:"reason"`
		Search              []tokenReferenceSearch `json:"search"`
		Samples             []tokenReferenceSample `json:"samples"`
		Guard               tokenReferenceGuard    `json:"guard"`
	}
	proof := struct {
		At            string   `json:"at"`
		ReadOnly      bool     `json:"read_only"`
		SourceQueries int      `json:"source_queries"`
		Samples       []sample `json:"samples"`
	}{At: time.Now().UTC().Format(time.RFC3339), ReadOnly: true}
	for _, part := range parts {
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil || id <= 0 {
			t.Fatal("invalid identity")
		}
		row := sample{ID: id}
		var trace strings.Builder
		reader := db.Session(&gorm.Session{Logger: logger.New(log.New(&trace, "", 0), logger.Config{LogLevel: logger.Info})})
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = reader.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			row.Stage = "fact"
			var f TokenMeteringFact
			if e := tx.Where("source_epoch=? AND log_id=?", epoch, id).First(&f).Error; e != nil {
				return e
			}
			row.Stage = "input"
			in, e := tokenInputForPolicy(f, p)
			if e != nil {
				return e
			}
			row.Stage = "reference"
			ref, e := tokenReferenceFrom(tx, in, p, 0, time.Now().Unix())
			row.Reason, row.Search, row.Samples = ref.Reason, ref.Search, ref.Samples
			if e != nil {
				return e
			}
			row.Stage = "guard"
			row.Guard, e = tokenReferenceDependencies(tx, in, p, ref)
			if e != nil {
				return e
			}
			row.Stage = "cache_prior"
			_, e = tokenCachePriorSignals(tx, f, ref.Bucket, p)
			if e == nil {
				row.Stage = "complete"
			}
			return e
		})
		cancel()
		row.ElapsedMS = time.Since(started).Milliseconds()
		row.Trace = trace.String()
		if err != nil {
			row.Error = err.Error()
		}
		proof.Samples = append(proof.Samples, row)
	}
	body, _ := json.MarshalIndent(proof, "", "  ")
	if err := os.WriteFile(os.Getenv("TOKEN_REFERENCE_REPORT"), append(body, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	for _, row := range proof.Samples {
		t.Logf("id=%d ms=%d stage=%s reason=%s samples=%d error=%s", row.ID, row.ElapsedMS, row.Stage, row.Reason, len(row.Samples), row.Error)
		if row.Error != "" {
			t.Fail()
		}
	}
}
