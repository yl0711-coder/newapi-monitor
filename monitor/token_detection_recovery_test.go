package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"gorm.io/gorm"
	"math/big"
	"strings"
	"testing"
)

func TestTokenDetectionReplayCompletenessRevisesWithoutFactChange(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	w := TokenMeteringWindow{SourceEpoch: f.SourceEpoch, StartAt: f.CreatedAt / 300 * 300, EndAt: (f.CreatedAt/300 + 1) * 300, Done: true, SourceComplete: true}
	if err := m.storeDB.Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	h, r := detectionHead(t, m, 1)
	if !r.SourceComplete {
		t.Fatal("complete window not observed")
	}
	if err := m.storeDB.Model(&w).Updates(map[string]any{"done": false, "source_complete": false, "replay": true}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	incomplete, rr := detectionHead(t, m, 1)
	if rr.SourceComplete || incomplete.Revision != h.Revision+1 {
		t.Fatal("replay coverage not revised")
	}
	if err := m.storeDB.Model(&w).Updates(map[string]any{"done": true, "source_complete": true, "replay": false}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+2)
	complete, rr := detectionHead(t, m, 1)
	if !rr.SourceComplete || complete.Revision != incomplete.Revision+1 {
		t.Fatal("completion not revised")
	}
}
func TestTokenDetectionLateCorrectionReplacesContributionAndIsolatesCandidate(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	f := detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	detectionRun(t, m, now)
	old, _ := detectionHead(t, m, 10)
	u := tokenMeteringDecode(t, f)
	inputField := u.Fields["prompt_tokens"]
	inputField.Value = "10000"
	u.Fields["prompt_tokens"] = inputField
	u.TotalInput = ptrTokenMetering(10000)
	quotaField := u.Fields["quota"]
	quotaField.Value = tokenPrice(u, detectionPolicy(), big.NewRat(1, 1)).Quota
	u.Fields["quota"] = quotaField
	f.SourceFacts = tokenDetectionJSON(u)
	f.Revision++
	f.FactHash = "corrected"
	if err := m.storeDB.Save(&f).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	h, r := detectionHead(t, m, 10)
	if r.Candidate || !h.Quarantined || h.Revision != old.Revision+1 {
		t.Fatal("correction or isolation lost")
	}
	s, e := tokenDetectionSummarize([]TokenDetectionCurrent{h})
	if e != nil || s.Candidates != 0 || len(s.Amounts) != 0 {
		t.Fatal("old contribution accumulated")
	}
	var frozen TokenDetectionRevision
	if err := m.storeDB.First(&frozen, "log_id = ? AND revision = ?", 10, old.Revision).Error; err != nil {
		t.Fatal(err)
	}
	var first tokenDetectionFrozen
	if json.Unmarshal([]byte(frozen.FrozenJSON), &first) != nil || !first.Result.Candidate {
		t.Fatal("original judgment overwritten")
	}
}
func TestTokenDetectionSameQuotaDifferentRequestsAndUnits(t *testing.T) {
	r := tokenDetectionResult{Candidate: true, Price: tokenPriceResult{Subject: "customer_wallet", Unit: "USD", QuotaPerUnit: "500000"}, ActualQuota: ptrTokenString("7"), FormulaDifference: ptrTokenString("2"), EstimatedLoss: ptrTokenString("3")}
	rows := []TokenDetectionCurrent{{LogID: 1, ResultJSON: tokenDetectionJSON(r)}, {LogID: 2, ResultJSON: tokenDetectionJSON(r)}}
	r.Price.Subject = "customer_subscription"
	rows = append(rows, TokenDetectionCurrent{LogID: 3, ResultJSON: tokenDetectionJSON(r)})
	s, e := tokenDetectionSummarize(rows)
	if e != nil || s.Candidates != 3 || len(s.Amounts) != 6 {
		t.Fatalf("%+v %v", s, e)
	}
	for _, a := range s.Amounts {
		if a.Subject == "customer_wallet" && a.Method == "observed_consumption_not_loss" && (a.Count != 2 || a.Quota != "14") {
			t.Fatal("SUM DISTINCT or method mixing")
		}
	}
}
func TestTokenDetectionUnknownMonetaryIdentityCannotBeSummed(t *testing.T) {
	for _, price := range []tokenPriceResult{
		{Unit: "USD", QuotaPerUnit: "500000"},
		{Subject: "customer_wallet", QuotaPerUnit: "500000"},
		{Subject: "customer_wallet", Unit: "USD"},
		{Subject: "customer_wallet", Unit: "USD", QuotaPerUnit: "0"},
		{Subject: "customer_wallet", Unit: "USD", QuotaPerUnit: "invalid"},
	} {
		r := tokenDetectionResult{Candidate: true, Price: price, ActualQuota: ptrTokenString("7")}
		rows := []TokenDetectionCurrent{{LogID: 1, ResultJSON: tokenDetectionJSON(r)}, {LogID: 2, ResultJSON: tokenDetectionJSON(r)}}
		summary, err := tokenDetectionSummarize(rows)
		if err != nil || summary.Candidates != 2 || len(summary.Amounts) != 0 || summary.Unavailable["observed_consumption_not_loss:billing_subject_or_unit_unknown"] != 2 {
			t.Fatalf("unknown identity combined: %+v %v", summary, err)
		}
	}
}
func ptrTokenString(s string) *string { return &s }
func TestTokenDetectionCapacityDoesNotConsumeAndCleanupClosed(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	sentinel := TokenDetectionRevision{SourceEpoch: "budget", LogID: 99, Revision: 1, FrozenJSON: "{}", StoredBytes: tokenDetectionMaxBytes, ExpiresAt: now + 100}
	if err := m.storeDB.Create(&sentinel).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now); !errors.Is(err, errTokenDetectionCapacity) {
		t.Fatalf("capacity err %v", err)
	}
	var n int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&n).Error; err != nil || n != 0 {
		t.Fatal("capacity consumed request")
	}
	if err := m.storeDB.Delete(&sentinel).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	m.cfg.TokenDetectionEnabled = false
	if err := m.storeDB.Delete(&f).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now+tokenDetectionRetention+2); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&n).Error; err != nil || n != 0 {
		t.Fatal("disabled cleanup failed")
	}
}
func TestTokenDetectionReferenceBoundedTruncationAndSizeIsolation(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	f := detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
	length := int64(2000)
	f.RequestLength = &length
	if err := m.storeDB.Save(&f).Error; err != nil {
		t.Fatal(err)
	}
	in, _ := tokenDetectionInputOf(f)
	ref, err := tokenReferenceFrom(m.storeDB, in, detectionPolicy(), 0, now)
	if err != nil || ref.Status == "statistical" {
		t.Fatal("incomparable size matched")
	}
	rows := make([]TokenMeteringFact, 201)
	for i := range rows {
		rows[i] = f
		rows[i].LogID = int64(100 + i)
		rows[i].ChannelID = int64(1 + i%2)
		rows[i].CreatedAt = now - 300
		rows[i].Revision = 1
	}
	if err := m.storeDB.CreateInBatches(&rows, 50).Error; err != nil {
		t.Fatal(err)
	}
	ref, err = tokenReferenceFrom(m.storeDB, in, detectionPolicy(), 0, now)
	if err != nil || !ref.Truncated || ref.Upper != nil {
		t.Fatal("partial reference presented as sufficient")
	}
}
func TestTokenDetectionNoDefaultSwitchAndMigrationPreservesFacts(t *testing.T) {
	if err := validateTokenDetectionSettings(Settings{}); err != nil {
		t.Fatal(err)
	}
	if err := validateTokenDetectionSettings(Settings{TokenDetectionEnabled: true}); err == nil {
		t.Fatal("enabled without explicit policy")
	}
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	old := f.SourceFacts
	if err := m.storeDB.AutoMigrate(&TokenMeteringFact{}, &TokenDetectionCurrent{}, &TokenDetectionRevision{}, &TokenDetectionReference{}, &TokenDetectionState{}, &TokenDetectionRetry{}, &TokenDetectionBucket{}); err != nil {
		t.Fatal(err)
	}
	var again TokenMeteringFact
	if err := m.storeDB.First(&again, "log_id = ?", 1).Error; err != nil || again.SourceFacts != old || again.RequestLength == nil {
		t.Fatal("migration changed existing facts")
	}
	if !m.storeDB.Migrator().HasIndex(&TokenMeteringFact{}, "idx_token_reference_input") {
		t.Fatal("local reference index absent")
	}
}
func TestTokenDetectionExpiredAnchorCannotAutoLearnShift(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 10000, 0, 0)
	detectionRun(t, m, now)
	if err := m.storeDB.Model(&TokenDetectionReference{}).Where("1=1").Update("expires_at", now-1).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	f := detectionFact(t, m, 11, now-100, 3, 100000, 0, 0)
	in, _ := tokenDetectionInputOf(f)
	ref, err := tokenReferenceFrom(m.storeDB, in, detectionPolicy(), 0, now)
	if err != nil || ref.Reason != "anchor_expired_requires_calibration" || ref.Upper != nil {
		t.Fatalf("anchor reseeded %+v %v", ref, err)
	}
}
func TestTokenDetectionLeaseCommitFailureHasNoPartialReference(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
	if err := m.storeDB.Exec("CREATE TRIGGER fail_head BEFORE INSERT ON token_detection_currents WHEN NEW.log_id=10 BEGIN SELECT RAISE(ABORT,'fixture'); END").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now); err == nil {
		t.Fatal("head error swallowed")
	}
	var n int64
	if err := m.storeDB.Model(&TokenDetectionRevision{}).Where("log_id = ?", 10).Count(&n).Error; err != nil || n != 0 {
		t.Fatal("revision orphan")
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error { return tx.Exec("DROP TRIGGER fail_head").Error }); err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	h, _ := detectionHead(t, m, 10)
	if h.Revision != 1 {
		t.Fatal("failed head consumed revision")
	}
}

func TestTokenDetectionLateReferenceAfterFinalStage(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionFact(t, m, 10, now-4000, 3, 100000, 0, 0)
	for _, at := range []int64{now, now + 61, now + 122, now + 183} {
		detectionRun(t, m, at)
	}
	h, r := detectionHead(t, m, 10)
	if h.Stage != 3 || r.Rules[1].Eligible {
		t.Fatal("initial stages incorrect")
	}
	for i := int64(1); i <= 3; i++ {
		detectionFact(t, m, i, now-4100+i, 1+i%2, 10000, 0, 0)
	}
	detectionRun(t, m, now+200)
	detectionRun(t, m, now+201)
	_, r = detectionHead(t, m, 10)
	if !r.Rules[1].Hit {
		t.Fatal("late reference generation did not wake final-stage result")
	}
}

func TestTokenDetectionExplicitInvalidPriceAndOpenAICacheCannotFallBack(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	u := tokenMeteringDecode(t, f)
	field := u.Fields["model_ratio"]
	field.State = "invalid"
	u.Fields["effective_model_ratio"] = field
	if p := tokenPrice(u, detectionPolicy(), big.NewRat(1, 1)); p.Supported {
		t.Fatal("invalid effective price fell back")
	}
	u.Semantic = "openai_inclusive"
	field = u.Fields["cache_tokens"]
	field.State = "invalid"
	u.Fields["cache_tokens"] = field
	if r := tokenFormatRule(u); !r.Hit {
		t.Fatal("invalid optional OpenAI cache escaped format rule")
	}
}

func TestTokenDetectionUnknownFeeExtensionPreservesPrivacy(t *testing.T) {
	f, _ := parseTokenMetering(tokenMeteringConfig(), tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0,"future_fee":{"header":"PRIVATE_CANARY"}}`), 1)
	u := tokenMeteringDecode(t, f)
	if u.Fields["unsupported_cost_context"].State != "invalid" {
		t.Fatal("unknown potential fee ignored")
	}
	if strings.Contains(f.SourceFacts, "PRIVATE_CANARY") || strings.Contains(f.SourceFacts, "future_fee") {
		t.Fatal("unknown payload retained")
	}
}
