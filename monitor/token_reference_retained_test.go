package monitor

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in, bounded read-only calibration against an administrator's retained
// store. No migrations, task claims or decision writes run against that store.
func TestTokenRetainedRecentCalibration(t *testing.T) {
	path, policyPath := os.Getenv("TOKEN_CALIBRATION_DB"), os.Getenv("TOKEN_CALIBRATION_POLICY")
	if path == "" || policyPath == "" {
		t.Skip("retained read-only store and scoped policy not supplied")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("absolute database path required")
	}
	b, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	p, err := parseTokenDetectionPolicy(string(b))
	if err != nil || p.SourceContract == nil {
		t.Fatal("scoped policy required", err)
	}
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(500)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db = db.WithContext(ctx)
	now := time.Now().Unix()
	from := now - 900
	through := now
	if os.Getenv("TOKEN_CALIBRATION_SETTLED") == "1" {
		through = now - 180
	}
	var facts []TokenMeteringFact
	if err := db.Where("source_epoch=? AND created_at>=? AND created_at<?", p.SourceContract.Epoch, from, through).Order("created_at DESC,log_id DESC").Limit(30).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) == 0 {
		t.Fatal("no retained recent facts in bounded window")
	}
	out := map[string]any{"from": from, "through": through, "limit": 30, "rows": len(facts), "read_only": true, "policy": p}
	type record struct {
		RequestID              string `json:"request_id"`
		LogID, At              int64
		Reference              tokenReference
		Price                  tokenPriceResult
		Rules                  []tokenDetectionRule
		Raw                    tokenMeteringParsed
		PreviousExactReference tokenReference `json:"previous_exact_reference"`
	}
	var records []record
	reasons := map[string]int{}
	prices := map[string]int{}
	for _, f := range facts {
		in, err := tokenInputForPolicy(f, p)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := tokenReferenceFrom(db, in, p, 0, now)
		if err != nil {
			t.Fatal(err)
		}
		ref.SampleCount = len(ref.Samples)
		oldPolicy := p
		oldPolicy.MinSamples = 8
		oldPolicy.MinChannels = 3
		oldPolicy.SizeTolerance = 0
		previous, err := tokenReferenceFrom(db, in, oldPolicy, 0, now)
		if err != nil {
			t.Fatal(err)
		}
		previous.SampleCount = len(previous.Samples)
		previous.Samples = nil
		r := tokenDetectionEvaluate(in, p, ref, 0, false)
		price := tokenPrice(in.Usage, p, big.NewRat(1, 1))
		reasons[ref.Reason]++
		if price.Supported {
			prices["supported"]++
		} else {
			prices[price.Reason]++
		}
		ref.Samples = nil
		records = append(records, record{f.RequestID, f.LogID, f.CreatedAt, ref, price, r.Rules, in.Usage, previous})
	}
	out["reference_reasons"], out["pricing_support"], out["requests"] = reasons, prices, records
	if output := os.Getenv("TOKEN_CALIBRATION_OUTPUT"); output != "" {
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("recent retained rows=%d references=%v pricing=%v", len(facts), reasons, prices)
}
