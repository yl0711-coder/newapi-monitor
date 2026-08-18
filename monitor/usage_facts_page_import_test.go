package monitor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"
)

type usageFactRawPageTestSource struct {
	events        []usageFactRawEvent
	fetchCalls    int
	failFetchCall int
}

func newUsageFactRawPageTestSource(events []usageFactRawEvent) *usageFactRawPageTestSource {
	sort.Slice(events, func(i, j int) bool {
		return usageFactRawEventAfter(events[j], events[i])
	})
	return &usageFactRawPageTestSource{events: events}
}

func (s *usageFactRawPageTestSource) FetchUsageFactRawPage(_ context.Context, userID, fromTs, throughTs, afterCreatedAt int64, afterType int, afterID int64, limit int) ([]usageFactRawEvent, error) {
	s.fetchCalls++
	if limit != usageFactRawPageSize {
		return nil, fmt.Errorf("unexpected page limit %d", limit)
	}
	if s.failFetchCall > 0 && s.fetchCalls == s.failFetchCall {
		return nil, errors.New("simulated source interruption")
	}
	page := make([]usageFactRawEvent, 0, limit)
	for _, event := range s.events {
		if event.UserID != userID || event.CreatedAt < fromTs || event.CreatedAt >= throughTs {
			continue
		}
		if !usageFactRawEventAfter(event, usageFactRawEvent{CreatedAt: afterCreatedAt, Type: afterType, ID: afterID}) {
			continue
		}
		page = append(page, event)
		if len(page) == limit {
			break
		}
	}
	return page, nil
}

func makeUsageFactRawPageEvents(hourTs, userID int64, count int) []usageFactRawEvent {
	events := make([]usageFactRawEvent, 0, count)
	for i := 0; i < count; i++ {
		event := usageFactRawEvent{
			ID:               int64(i + 1),
			CreatedAt:        hourTs + int64(i/8), // 20k+ rows still fit one hour; equal timestamps exercise the ID tiebreaker
			UserID:           userID,
			ChannelID:        int64(i % 11),
			Grp:              fmt.Sprintf("group-%d", i%5),
			ModelName:        fmt.Sprintf("model-%d", i%7),
			TokenID:          int64(i % 13),
			TokenName:        fmt.Sprintf("token-%03d", i%13),
			Type:             2,
			PromptTokens:     int64(i%101 + 1),
			CompletionTokens: int64(i%37 + 1),
			Quota:            int64(i%997 + 1),
		}
		if i%9 == 0 {
			event.Type = 6
			event.Quota = -event.Quota
			event.PromptTokens, event.CompletionTokens = 0, 0
		}
		events = append(events, event)
	}
	return events
}

type usageFactRawJobTestExecutor func(context.Context, usageFactHistoryClaim, time.Time) error

// resumeUsageFactRawJobUntil simulates independent scheduler turns. A large
// member must yield between bounded import and verification pages, while the
// durable job cursor remains unchanged until the entire hour is proven.
func resumeUsageFactRawJobUntil(
	t *testing.T,
	m *Monitor,
	jobID string,
	targetHour int64,
	now time.Time,
	execute usageFactRawJobTestExecutor,
) UsageFactJob {
	t.Helper()
	for turn := 0; turn < 20; turn++ {
		var job UsageFactJob
		if err := m.usageFactsStore().First(&job, "id = ?", jobID).Error; err != nil {
			t.Fatal(err)
		}
		if job.NextHour >= targetHour {
			return job
		}
		leaseOwner := fmt.Sprintf("raw-page-resume-%s-%d", jobID, turn)
		if err := m.usageFactsStore().Model(&UsageFactJob{}).Where("id = ?", jobID).Updates(map[string]any{
			"status": usageFactHistoryJobRunning, "lease_owner": leaseOwner,
			"lease_until": now.Add(time.Duration(turn+2) * time.Minute).Unix(),
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().First(&job, "id = ?", jobID).Error; err != nil {
			t.Fatal(err)
		}
		claim := usageFactHistoryClaim{
			Jobs: []UsageFactJob{job}, LeaseOwner: leaseOwner,
			From: job.NextHour, Through: job.NextHour + usageFactHourSeconds,
		}
		if err := execute(context.Background(), claim, now.Add(time.Duration(turn+1)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	var job UsageFactJob
	if err := m.usageFactsStore().First(&job, "id = ?", jobID).Error; err != nil {
		t.Fatal(err)
	}
	t.Fatalf("bounded raw job did not reach target after fair turns: target=%d job=%+v", targetHour, job)
	return job
}

func TestUsageFactRawPageImportHighVolumeResumesAndControls(t *testing.T) {
	m := newTestMonitor(t)
	hour := int64(1_786_996_800) // 2026-08-18 00:00:00 UTC, aligned to an hour
	const userID int64 = 901
	// More than twenty pages represents a future heavy member, not the current
	// one-off customer. The same bounded turn/cursor protocol must hold without
	// increasing page size or source concurrency.
	source := newUsageFactRawPageTestSource(makeUsageFactRawPageEvents(hour, userID, 20_345))
	wantMetrics := usageFactRawEventMetrics(source.events)

	// The first turn deliberately ends after three pages.  This is the fairness
	// boundary: a high-volume member cannot hold the source worker forever.
	complete, err := importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, userID, hour, "test-epoch", 3)
	if err != nil || complete {
		t.Fatalf("first bounded turn complete=%v err=%v", complete, err)
	}
	var checkpoint UsageFactPageIngestState
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.Pages != 3 || checkpoint.SourceRows != 3*usageFactRawPageSize || checkpoint.CursorID <= 0 {
		t.Fatalf("checkpoint was not atomically advanced after three pages: %+v", checkpoint)
	}

	// Simulate a process/source interruption after the checkpoint.  Retrying
	// begins strictly after the durable cursor; it must never double-count the
	// three committed pages.
	source.failFetchCall = 4
	if _, err := importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, userID, hour, "test-epoch", 3); err == nil {
		t.Fatal("expected injected source interruption")
	}
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.SourceRows != 3*usageFactRawPageSize || checkpoint.CursorID <= 0 {
		t.Fatalf("failed page changed committed checkpoint: %+v", checkpoint)
	}
	source.failFetchCall = 0
	for turns := 0; turns < 30; turns++ {
		complete, err = importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, userID, hour, "test-epoch", 3)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			break
		}
	}
	if !complete {
		t.Fatal("high-volume hour did not finish through bounded turns")
	}
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.VerifyPages == 0 || checkpoint.RawHash == "" || checkpoint.RawHash != checkpoint.VerifyRawHash {
		t.Fatalf("completion must use bounded second-pass verification, checkpoint=%+v", checkpoint)
	}
	if checkpoint.Status != "complete" || checkpoint.SourceRows != int64(len(source.events)) ||
		checkpoint.Requests != wantMetrics.Requests || checkpoint.RefundRecords != wantMetrics.RefundRecords ||
		checkpoint.PromptTokens != wantMetrics.PromptTokens || checkpoint.CompletionTokens != wantMetrics.CompletionTokens ||
		checkpoint.ConsumeQuota != wantMetrics.ConsumeQuota || checkpoint.RefundQuota != wantMetrics.RefundQuota {
		t.Fatalf("completed checkpoint does not match bounded source pages: %+v", checkpoint)
	}
	wantFetchCalls := 2*((len(source.events)+usageFactRawPageSize-1)/usageFactRawPageSize) + 1 // two passes + injected failure
	if source.fetchCalls != wantFetchCalls {
		t.Fatalf("high-volume import used %d source pages; want %d bounded pages without redundant EOF scans", source.fetchCalls, wantFetchCalls)
	}
	rows, err := loadUsageFactHour(m.usageFactsStore(), hour, []int64{userID})
	if err != nil {
		t.Fatal(err)
	}
	if !usageFactMetricsEqual(factsMetrics(rows), wantMetrics) || usageFactContentHash(rows) != checkpoint.ContentHash {
		t.Fatalf("local aggregation differs from the bounded source pages: got=%+v want=%+v", factsMetrics(rows), wantMetrics)
	}
}

func TestUsageFactRawPageEventCountIsIndependentFromDimensionRows(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 917
	hour := time.Date(2026, 8, 18, 9, 0, 0, 0, usageCST).Unix()
	events := makeUsageFactRawPageEvents(hour, userID, 2_500)
	for i := range events {
		events[i].ChannelID = 7
		events[i].Grp = "same-group"
		events[i].ModelName = "same-model"
		events[i].TokenID = 9
		events[i].TokenName = "same-token"
	}
	source := newUsageFactRawPageTestSource(events)
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	complete, err := importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, userID, hour, m.cfg.UsageFactsHistorySourceEpoch, usageFactRawPagesPerTurn)
	if err != nil || complete {
		t.Fatalf("first bounded turn complete=%v err=%v", complete, err)
	}
	for turn := 0; turn < 4 && !complete; turn++ {
		complete, err = importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, userID, hour, m.cfg.UsageFactsHistorySourceEpoch, usageFactRawPagesPerTurn)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !complete {
		t.Fatal("bounded second pass did not complete the high-volume hour")
	}
	if err := m.commitUsageFactRawPageHourProof(context.Background(), userID, hour, 1); err != nil {
		t.Fatal(err)
	}
	var facts []UsageHourFact
	if err := m.usageFactsStore().Where("user_id = ? AND hour_ts = ?", userID, hour).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].Requests+facts[0].RefundRecords != int64(len(events)) {
		t.Fatalf("2500 events must collapse into one exact dimension row: %+v", facts)
	}
	var page UsageFactPageIngestState
	if err := m.usageFactsStore().First(&page, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	var proof UsageFactMemberHourState
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	wantMetrics := usageFactRawEventMetrics(events)
	if page.SourceRows != int64(len(events)) || proof.Rows != 1 || proof.Requests != wantMetrics.Requests || proof.Tokens != wantMetrics.tokens() {
		t.Fatalf("event and dimension cardinalities were conflated: page=%+v proof=%+v", page, proof)
	}
}

func TestUsageFactRawPageImportRefusesPublicationOnControlMismatch(t *testing.T) {
	m := newTestMonitor(t)
	hour := int64(1_786_996_800)
	source := newUsageFactRawPageTestSource(makeUsageFactRawPageEvents(hour, 902, 1001))
	originalQuota := source.events[0].Quota
	complete, err := importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, 902, hour, "test-epoch", 4)
	if err != nil || complete {
		t.Fatalf("first pass must yield before bounded verification: complete=%v err=%v", complete, err)
	}
	// A source correction between the two bounded passes must fail closed.
	source.events[0].Quota++
	complete, err = importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, 902, hour, "test-epoch", 4)
	if err != nil || complete {
		t.Fatalf("mismatched verification must not publish: complete=%v err=%v", complete, err)
	}
	var checkpoint UsageFactPageIngestState
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", 902, hour).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.Status != "repair" || checkpoint.LastError != errUsageFactRawPageControl.Error() {
		t.Fatalf("control mismatch was not durably held for repair: %+v", checkpoint)
	}
	// A later source correction must repair only this hour. The next turn resets
	// its local page staging and proves it again; it does not leave a permanent
	// circuit-breaker state for this member or any unrelated member.
	source.events[0].Quota = originalQuota
	for turn := 0; turn < 6 && !complete; turn++ {
		complete, err = importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, 902, hour, "test-epoch", 4)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !complete {
		t.Fatal("corrected source did not repair the bounded hour")
	}
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", 902, hour).Error; err != nil {
		t.Fatal(err)
	}
	wantMetrics := usageFactRawEventMetrics(source.events)
	if checkpoint.Status != "complete" || checkpoint.SourceRows != int64(len(source.events)) ||
		checkpoint.Requests != wantMetrics.Requests || checkpoint.RefundRecords != wantMetrics.RefundRecords ||
		checkpoint.PromptTokens != wantMetrics.PromptTokens || checkpoint.CompletionTokens != wantMetrics.CompletionTokens ||
		checkpoint.ConsumeQuota != wantMetrics.ConsumeQuota || checkpoint.RefundQuota != wantMetrics.RefundQuota {
		t.Fatalf("corrected source left the hour unrepaired: %+v", checkpoint)
	}
	// Simulate the normal post-repair staging cleanup (or a local corruption).
	// A stale complete cursor must detect that its fingerprinted rows are gone
	// and rebuild this hour rather than reporting a false success.
	if err := m.usageFactsStore().Where("user_id = ? AND hour_ts = ?", int64(902), hour).Delete(&UsageHourFact{}).Error; err != nil {
		t.Fatal(err)
	}
	complete = false
	for turn := 0; turn < 6 && !complete; turn++ {
		complete, err = importUsageFactRawPages(context.Background(), m.usageFactsStore(), source, 902, hour, "test-epoch", 4)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !complete {
		t.Fatal("missing local facts were not rebuilt from the completed cursor")
	}
	rows, err := loadUsageFactHour(m.usageFactsStore(), hour, []int64{902})
	if err != nil {
		t.Fatal(err)
	}
	if !usageFactMetricsEqual(factsMetrics(rows), wantMetrics) {
		t.Fatalf("rebuilt local facts differ from source control: %+v", factsMetrics(rows))
	}
}

func TestUsageFactRawShardSparseRangeExpandsIntoHourlyProofs(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 920
	from := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	through := from + usageFactRawShardDefaultHours*usageFactHourSeconds
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "raw_page_span_hours": usageFactRawShardDefaultHours,
	}).Error; err != nil {
		t.Fatal(err)
	}
	events := make([]usageFactRawEvent, 0, usageFactRawShardDefaultHours)
	for offset := 0; offset < usageFactRawShardDefaultHours; offset++ {
		event := makeUsageFactRawPageEvents(from+int64(offset)*usageFactHourSeconds, userID, 1)[0]
		event.ID = int64(offset + 1)
		events = append(events, event)
	}
	source := newUsageFactRawPageTestSource(events)
	complete, err := importUsageFactRawShardPages(context.Background(), m.usageFactsStore(), source, userID, from, through,
		m.cfg.UsageFactsHistorySourceEpoch, usageFactRawPagesPerTurn)
	if err != nil || complete {
		t.Fatalf("sparse shard first pass complete=%v err=%v", complete, err)
	}
	complete, err = importUsageFactRawShardPages(context.Background(), m.usageFactsStore(), source, userID, from, through,
		m.cfg.UsageFactsHistorySourceEpoch, usageFactRawPagesPerTurn)
	if err != nil || !complete {
		t.Fatalf("sparse shard verification complete=%v err=%v", complete, err)
	}
	if source.fetchCalls != 2 {
		t.Fatalf("sparse day shard used %d source queries; want exactly one bounded query per independent pass", source.fetchCalls)
	}
	if err := m.commitUsageFactRawPageShardProof(context.Background(), userID, from, through, 1); err != nil {
		t.Fatal(err)
	}
	var proofs, pages int64
	if err := m.usageFactsStore().Model(&UsageFactMemberHourState{}).
		Where("user_id = ? AND hour_ts >= ? AND hour_ts < ? AND status = ?", userID, from, through, "complete").Count(&proofs).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactPageIngestState{}).
		Where("user_id = ? AND hour_ts >= ? AND hour_ts < ? AND status = ?", userID, from, through, "complete").Count(&pages).Error; err != nil {
		t.Fatal(err)
	}
	if proofs != usageFactRawShardDefaultHours || pages != usageFactRawShardDefaultHours {
		t.Fatalf("wide shard did not expand to hourly proofs: proofs=%d pages=%d", proofs, pages)
	}
	for hour := from; hour < through; hour += usageFactHourSeconds {
		ready, err := usageFactRawPageHourReady(m.usageFactsStore(), userID, hour, m.cfg.UsageFactsHistorySourceEpoch)
		if err != nil || !ready {
			t.Fatalf("hour %d ready=%v err=%v", hour, ready, err)
		}
	}
}

func TestUsageFactRawShardDenseRangeShrinksBeforeWritingFacts(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	const userID int64 = 921
	from := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	events := makeUsageFactRawPageEvents(from, userID, usageFactRawPageSize+1)
	source := newUsageFactRawPageTestSource(events)
	for _, span := range []int{usageFactRawShardDefaultHours, usageFactRawShardWideHours, usageFactRawShardMediumHours} {
		complete, err := importUsageFactRawShardPages(context.Background(), m.usageFactsStore(), source, userID, from,
			from+int64(span)*usageFactHourSeconds, "test-epoch", usageFactRawPagesPerTurn)
		if complete || !errors.Is(err, errUsageFactRawShardDense) {
			t.Fatalf("span=%d complete=%v err=%v; want dense shrink", span, complete, err)
		}
		var checkpoints, facts int64
		if err := m.usageFactsStore().Model(&UsageFactPageIngestState{}).Where("user_id = ?", userID).Count(&checkpoints).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Model(&UsageHourFact{}).Where("user_id = ?", userID).Count(&facts).Error; err != nil {
			t.Fatal(err)
		}
		if checkpoints != 0 || facts != 0 {
			t.Fatalf("dense probe wrote local state before shrink: checkpoints=%d facts=%d", checkpoints, facts)
		}
	}
	complete, err := importUsageFactRawShardPages(context.Background(), m.usageFactsStore(), source, userID, from,
		from+usageFactHourSeconds, "test-epoch", 1)
	if err != nil || complete {
		t.Fatalf("one-hour dense shard first page complete=%v err=%v", complete, err)
	}
	var checkpoint UsageFactPageIngestState
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", userID, from).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.Pages != 1 || checkpoint.SourceRows != usageFactRawPageSize || checkpoint.ThroughTs != from+usageFactHourSeconds {
		t.Fatalf("one-hour dense shard cursor=%+v", checkpoint)
	}
}

func TestRawPageColdWorkerAdvancesSparseDayShard(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 922
	from := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "raw_page_span_hours": usageFactRawShardDefaultHours,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < usageFactRawShardDefaultHours; offset++ {
		event := makeUsageFactRawPageEvents(from+int64(offset)*usageFactHourSeconds, userID, 1)[0]
		event.ID = int64(offset + 1)
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
			event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota,
			event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(from+12*usageFactHourSeconds, 0)
	job := UsageFactJob{ID: "raw-sparse-day-922", Kind: usageFactHistoryKindBackfill, Priority: 50, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: from, ThroughTs: from + 2*usageFactDaySeconds,
		NextHour: from, TotalHours: 48, Status: usageFactHistoryJobRunning, LeaseOwner: "raw-day",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix()}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: from, Through: from + 2*usageFactDaySeconds}
	if err := m.executeUsageFactHistoryBackfillRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactJob{}).Where("id = ?", job.ID).Updates(map[string]any{
		"status": usageFactHistoryJobRunning, "lease_owner": "raw-day-verify",
		"lease_until": now.Add(time.Minute).Unix(), "updated_at": now.Add(time.Second).Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	verifyClaim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: from, Through: from + 2*usageFactDaySeconds}
	if err := m.executeUsageFactHistoryBackfillRawPages(context.Background(), verifyClaim, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NextHour != from+usageFactRawShardDefaultHours*usageFactHourSeconds || job.CompletedHours != usageFactRawShardDefaultHours {
		t.Fatalf("sparse cold shard did not advance one complete day: %+v", job)
	}
}

func TestMonitorUsageFactRawPageSourceUsesOnlyOrderedBoundedRows(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	const (
		userID = int64(907)
		hour   = int64(1_786_996_800)
	)
	// Insert deliberately out of cursor order. Types 2/6 are facts; a type 1
	// row must never enter either bounded pass.
	for _, event := range []usageFactRawEvent{
		{ID: 9, CreatedAt: hour + 20, UserID: userID, ChannelID: 4, Grp: "g", ModelName: "m", TokenID: 7, TokenName: "t", Type: 2, PromptTokens: 3, CompletionTokens: 5, Quota: 11},
		{ID: 3, CreatedAt: hour + 10, UserID: userID, ChannelID: 4, Grp: "g", ModelName: "m", TokenID: 7, TokenName: "t", Type: 6, Quota: -2},
		{ID: 5, CreatedAt: hour + 10, UserID: userID, ChannelID: 4, Grp: "g", ModelName: "m", TokenID: 7, TokenName: "t", Type: 2, PromptTokens: 2, CompletionTokens: 4, Quota: 9},
		{ID: 7, CreatedAt: hour + 10, UserID: userID, Type: 1, Quota: 999},
	} {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota, event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	source := m.usageFactRawPageSource(false)
	first, err := source.FetchUsageFactRawPage(context.Background(), userID, hour, hour+usageFactHourSeconds, 0, 0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].ID != 5 || first[1].ID != 3 {
		t.Fatalf("first bounded raw page=%+v; want ids [5 3]", first)
	}
	second, err := source.FetchUsageFactRawPage(context.Background(), userID, hour, hour+usageFactHourSeconds, first[1].CreatedAt, first[1].Type, first[1].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != 9 {
		t.Fatalf("second cursor page=%+v; want id 9", second)
	}
}

func TestRawPageHistoryBackfillYieldsHighVolumeHourThenAdvancesDurably(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 908
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	hour := day + 8*usageFactHourSeconds
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	// A prior implementation may have left a staged GROUP BY row for this
	// exact hour. Entering the page protocol must replace that hour, never add
	// its pages on top of it.
	if err := m.usageFactsStore().Create(&UsageHourFact{HourTs: hour, DayTs: day, UserID: userID, ChannelID: 999, Grp: "stale", ModelName: "stale", TokenID: 999, Requests: 1, ConsumeQuota: 999_999}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactMemberHourState{UserID: userID, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, ContentHash: "old", SourceEpoch: "old"}).Error; err != nil {
		t.Fatal(err)
	}
	for _, event := range makeUsageFactRawPageEvents(hour, userID, 3_501) {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota, event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(hour+2*usageFactHourSeconds, 0)
	job := UsageFactJob{
		ID: "raw-page-backfill-908", Kind: usageFactHistoryKindBackfill, Priority: 50, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour, ThroughTs: day + usageFactDaySeconds,
		NextHour: hour, TotalHours: 16, Status: usageFactHistoryJobRunning, LeaseOwner: "raw-page-test",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: hour, Through: hour + usageFactDaySeconds}
	if err := m.executeUsageFactHistoryBackfillRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	var checkpoint UsageFactPageIngestState
	if err := m.usageFactsStore().First(&checkpoint, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.Pages != usageFactRawPagesPerTurn || checkpoint.SourceRows != int64(usageFactRawPagesPerTurn*usageFactRawPageSize) {
		t.Fatalf("first history turn did not persist the bounded raw cursor: %+v", checkpoint)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NextHour != hour || job.Status != usageFactHistoryJobQueued || job.Attempts != 0 {
		t.Fatalf("partial raw hour must yield without advancing/charging the job: %+v", job)
	}
	job = resumeUsageFactRawJobUntil(t, m, job.ID, hour+usageFactHourSeconds, now, m.executeUsageFactHistoryBackfillRawPages)
	if job.NextHour != hour+usageFactHourSeconds || job.Status != usageFactHistoryJobQueued {
		t.Fatalf("completed raw hour did not advance exactly one hour: %+v", job)
	}
	var proof UsageFactMemberHourState
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND hour_ts = ?", userID, hour).Error; err != nil {
		t.Fatal(err)
	}
	if proof.Status != "complete" || proof.SourceEpoch != m.cfg.UsageFactsHistorySourceEpoch || proof.Rows != 3_501 || proof.ContentHash == "" {
		t.Fatalf("raw page hour was not converted to a normal durable proof: %+v", proof)
	}
	var stale int64
	if err := m.usageFactsStore().Model(&UsageHourFact{}).Where("hour_ts = ? AND user_id = ? AND channel_id = ?", hour, userID, 999).Count(&stale).Error; err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Fatalf("raw page import retained stale staged facts, rows=%d", stale)
	}
}

func TestRawPageLegacyPartialDayRealignsDurablyBeforeSourceRead(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 913
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	legacyNext := day + 23*usageFactHourSeconds
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "source_history_status": "no_history",
		"coverage_through_hour": legacyNext, "tail_through_hour": legacyNext,
	}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(legacyNext+usageFactHourSeconds, 0)
	job := UsageFactJob{
		ID: "raw-page-legacy-partial-day-913", Kind: usageFactHistoryKindBackfill, Priority: 50, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: day, ThroughTs: day + usageFactDaySeconds,
		NextHour: legacyNext, CompletedHours: 23, TotalHours: 24, Status: usageFactHistoryJobRunning, LeaseOwner: "legacy-raw-realign",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: legacyNext, Through: day + usageFactDaySeconds}
	if err := m.executeUsageFactHistoryBackfillRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NextHour != day || job.CompletedHours != 0 || job.Status != usageFactHistoryJobQueued || job.LeaseOwner != "" || job.LastError != "" {
		t.Fatalf("legacy partial day was not durably realigned: %+v", job)
	}
	var checkpoints int64
	if err := m.usageFactsStore().Model(&UsageFactPageIngestState{}).Where("user_id = ?", userID).Count(&checkpoints).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoints != 0 {
		t.Fatalf("realignment must not query or stage the old 23:00 hour, checkpoints=%d", checkpoints)
	}
	var member UsageFactMemberState
	if err := m.usageFactsStore().First(&member, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if member.CoverageThroughHour == nil || *member.CoverageThroughHour != day || member.TailThroughHour == nil || *member.TailThroughHour != day {
		t.Fatalf("member waterlines were not lowered with the open-day cursor: %+v", member)
	}

	// A later worker/restart resumes from exactly midnight and advances one
	// independently controlled empty hour; it never resets prior days.
	job = resumeUsageFactRawJobUntil(t, m, job.ID, day+usageFactHourSeconds, now, m.executeUsageFactHistoryBackfillRawPages)
	if job.NextHour != day+usageFactHourSeconds || job.CompletedHours != 1 || job.Status != usageFactHistoryJobQueued {
		t.Fatalf("realigned job did not resume exactly one hour: %+v", job)
	}
}

func TestRawPageClaimBypassesOnlyLegacyDayProofBackoff(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, usageCST)
	hour := now.Add(-time.Hour).Unix() / usageFactHourSeconds * usageFactHourSeconds
	legacyUser, realFailureUser := int64(915), int64(916)
	jobs := []UsageFactJob{
		{ID: "raw-legacy-retry-915", Kind: usageFactHistoryKindTail, Priority: 100, UserID: &legacyUser,
			TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour - usageFactDaySeconds,
			ThroughTs: hour + usageFactHourSeconds, NextHour: hour, Status: usageFactHistoryJobPaused, Attempts: 5,
			NextRetryAt: now.Add(time.Hour).Unix(), LastError: usageFactRawPageLegacyDayProofError, CreatedAt: now.Unix(), UpdatedAt: now.Unix()},
		{ID: "raw-real-retry-916", Kind: usageFactHistoryKindTail, Priority: 200, UserID: &realFailureUser,
			TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour - usageFactDaySeconds,
			ThroughTs: hour + usageFactHourSeconds, NextHour: hour, Status: usageFactHistoryJobPaused, Attempts: 5,
			NextRetryAt: now.Add(time.Hour).Unix(), LastError: "source deadline exceeded", CreatedAt: now.Unix(), UpdatedAt: now.Unix()},
	}
	if err := m.usageFactsStore().Create(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	claim, err := m.claimUsageFactHistoryJobs(context.Background(), usageFactHistoryKindTail, "raw-compat-claim", 1, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.Jobs) != 1 || claim.Jobs[0].ID != jobs[0].ID {
		t.Fatalf("only the exact legacy day-proof retry may bypass backoff: %+v", claim.Jobs)
	}
	var realFailure UsageFactJob
	if err := m.usageFactsStore().First(&realFailure, "id = ?", jobs[1].ID).Error; err != nil {
		t.Fatal(err)
	}
	if realFailure.Status != usageFactHistoryJobPaused || realFailure.LeaseOwner != "" {
		t.Fatalf("real source failure backoff was bypassed: %+v", realFailure)
	}
}

func TestRawPageClaimRotatesHighVolumeMemberBehindPeer(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, usageCST)
	oldHour := now.Add(-48*time.Hour).Unix() / usageFactHourSeconds * usageFactHourSeconds
	newHour := now.Add(-24*time.Hour).Unix() / usageFactHourSeconds * usageFactHourSeconds
	firstUser, peerUser := int64(918), int64(919)
	jobs := []UsageFactJob{
		{ID: "raw-fair-first-918", Kind: usageFactHistoryKindBackfill, Priority: 50, UserID: &firstUser,
			TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: oldHour,
			ThroughTs: now.Unix(), NextHour: oldHour, Status: usageFactHistoryJobQueued,
			CreatedAt: now.Add(-2 * time.Hour).Unix(), UpdatedAt: now.Add(-time.Minute).Unix()},
		{ID: "raw-fair-peer-919", Kind: usageFactHistoryKindBackfill, Priority: 50, UserID: &peerUser,
			TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: newHour,
			ThroughTs: now.Unix(), NextHour: newHour, Status: usageFactHistoryJobQueued,
			CreatedAt: now.Add(-time.Hour).Unix(), UpdatedAt: now.Add(-2 * time.Hour).Unix()},
	}
	if err := m.usageFactsStore().Create(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	claim, err := m.claimUsageFactHistoryJobs(context.Background(), usageFactHistoryKindBackfill, "raw-fair-claim", 1, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.Jobs) != 1 || claim.Jobs[0].ID != jobs[1].ID {
		t.Fatalf("least-recently-served raw peer must run before an older cursor that just consumed a turn: %+v", claim.Jobs)
	}
}

func TestRawPageClaimFairnessScalesWhenEveryMemberIsHighVolume(t *testing.T) {
	for _, members := range []int{50, 200, 500} {
		t.Run(fmt.Sprintf("members_%d", members), func(t *testing.T) {
			m := newUsageHistoryTestMonitor(t)
			m.cfg.UsageFactsRawPageImportEnabled = true
			now := time.Date(2026, 8, 18, 12, 0, 0, 0, usageCST)
			hour := now.Add(-48*time.Hour).Unix() / usageFactHourSeconds * usageFactHourSeconds
			jobs := make([]UsageFactJob, 0, members)
			for i := 0; i < members; i++ {
				userID := int64(10_000 + i)
				jobs = append(jobs, UsageFactJob{
					ID: fmt.Sprintf("raw-all-hot-%d", userID), Kind: usageFactHistoryKindBackfill,
					Priority: 50, UserID: ptrInt64(userID), TrackedRevision: 1,
					SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour,
					ThroughTs: now.Unix(), NextHour: hour, Status: usageFactHistoryJobQueued,
					CreatedAt: now.Unix(), UpdatedAt: now.Add(-time.Hour).Unix(),
				})
			}
			if err := m.usageFactsStore().CreateInBatches(jobs, 25).Error; err != nil {
				t.Fatal(err)
			}

			seen := make(map[int64]struct{}, members)
			for turn := 0; turn < members; turn++ {
				turnNow := now.Add(time.Duration(turn+1) * time.Second)
				owner := fmt.Sprintf("raw-all-hot-owner-%d", turn)
				claim, err := m.claimUsageFactHistoryJobs(context.Background(), usageFactHistoryKindBackfill, owner, 1, 1, turnNow)
				if err != nil {
					t.Fatal(err)
				}
				if len(claim.Jobs) != 1 || claim.Jobs[0].UserID == nil {
					t.Fatalf("turn %d produced invalid claim: %+v", turn, claim.Jobs)
				}
				userID := *claim.Jobs[0].UserID
				if _, duplicate := seen[userID]; duplicate {
					t.Fatalf("member %d received a second turn before all %d high-volume peers progressed", userID, members)
				}
				seen[userID] = struct{}{}
				// A partial 20-page shard yields exactly this way: its durable page
				// cursor remains, while the job lease is released without charging
				// an attempt or advancing the member-hour waterline.
				if err := m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactAdaptiveBudget, turnNow, true); err != nil {
					t.Fatal(err)
				}
			}
			if len(seen) != members {
				t.Fatalf("only %d/%d high-volume members received a bounded turn", len(seen), members)
			}
		})
	}
}

func TestRawPageNoHistoryFirstActivityRevokesBeforeStaging(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 914
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": epoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "source_history_status": "no_history",
		"source_floor_hour": day, "coverage_status": "ready", "coverage_through_hour": day,
		"tail_through_hour": day, "verification_status": "complete", "verified_through_hour": day,
	}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(day+2*usageFactHourSeconds, 0)
	job := UsageFactJob{
		ID: usageFactHistoryJobID(userID, 1), Kind: usageFactHistoryKindTail, Priority: 100, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: epoch, FromTs: day, ThroughTs: day + 2*usageFactHourSeconds,
		NextHour: day, TotalHours: 2, Status: usageFactHistoryJobRunning, LeaseOwner: "raw-no-history",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: userID, TrackedRevision: 1, SourceEpoch: epoch,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour: day, VerifiedThroughHour: day, PublishedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"generation": int64(7), "serving_generation": int64(7), "published_fingerprint": portalMemberFingerprintFromIDs([]int64{userID}),
		"published_range_start": day, "published_through": day + usageFactHourSeconds, "published_window_days": 1, "published_at": now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	event := makeUsageFactRawPageEvents(day, userID, 1)[0]
	if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota, event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: day, Through: day + usageFactHourSeconds}
	if err := m.executeUsageFactHistoryTailRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Kind != usageFactHistoryKindDiscover || job.Status != usageFactHistoryJobQueued || job.LeaseOwner != "" {
		t.Fatalf("no-history activity did not return the durable job to discovery: %+v", job)
	}
	var member UsageFactMemberState
	if err := m.usageFactsStore().First(&member, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if member.SourceHistoryStatus != "discovering" || member.CoverageStatus != "discovering" {
		t.Fatalf("no-history member was not fail-closed for rediscovery: %+v", member)
	}
	var published, pages, facts int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", userID).Count(&published).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactPageIngestState{}).Where("user_id = ?", userID).Count(&pages).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageHourFact{}).Where("user_id = ?", userID).Count(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if published != 0 || pages != 0 || facts != 0 {
		t.Fatalf("activity must revoke before any page is staged: published=%d pages=%d facts=%d", published, pages, facts)
	}
	var global UsageFactSyncState
	if err := m.usageFactsStore().First(&global, 1).Error; err != nil {
		t.Fatal(err)
	}
	if global.Generation != 8 || global.ServingGeneration != 8 || global.PublishedAt != 0 {
		t.Fatalf("no-history publication generations were not rotated: %+v", global)
	}
}

func TestRawPageTailYieldsHighVolumeHourWithoutFallingBackToGroupQuery(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 909
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	hour := day + 9*usageFactHourSeconds
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "source_history_status": "complete_hot",
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, event := range makeUsageFactRawPageEvents(hour, userID, 2_501) {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota, event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(hour+2*usageFactHourSeconds, 0)
	job := UsageFactJob{
		ID: "raw-page-tail-909", Kind: usageFactHistoryKindTail, Priority: 100, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour, ThroughTs: hour + 2*usageFactHourSeconds,
		NextHour: hour, TotalHours: 2, Status: usageFactHistoryJobRunning, LeaseOwner: "raw-tail-test",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: hour, Through: hour + usageFactHourSeconds}
	if err := m.executeUsageFactHistoryTailRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NextHour != hour || job.Attempts != 0 || job.Status != usageFactHistoryJobQueued {
		t.Fatalf("partial raw tail must yield without cursor loss: %+v", job)
	}
	job = resumeUsageFactRawJobUntil(t, m, job.ID, hour+usageFactHourSeconds, now, m.executeUsageFactHistoryTailRawPages)
	if job.NextHour != hour+usageFactHourSeconds || job.Status != usageFactHistoryJobQueued {
		t.Fatalf("raw tail did not advance exactly one verified hour: %+v", job)
	}
}

func TestRawPageRepairHourYieldsHighVolumeHourAndKeepsRepairCursor(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 910
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	hour := day + 10*usageFactHourSeconds
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, event := range makeUsageFactRawPageEvents(hour, userID, 2_501) {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota, event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(hour+2*usageFactHourSeconds, 0)
	job := UsageFactJob{
		ID: "raw-page-repair-910", Kind: usageFactHistoryKindRepairHour, Priority: 200, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour, ThroughTs: day + usageFactDaySeconds,
		NextHour: hour, TotalHours: 14, Status: usageFactHistoryJobRunning, LeaseOwner: "raw-repair-test",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: hour, Through: hour + usageFactHourSeconds}
	if err := m.executeUsageFactHistoryRepairHourRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NextHour != hour || job.Status != usageFactHistoryJobQueued || job.Attempts != 0 {
		t.Fatalf("partial raw repair must yield without charging the cursor: %+v", job)
	}
	job = resumeUsageFactRawJobUntil(t, m, job.ID, hour+usageFactHourSeconds, now, m.executeUsageFactHistoryRepairHourRawPages)
	if job.NextHour != hour+usageFactHourSeconds || job.Kind != usageFactHistoryKindRepairHour || job.Status != usageFactHistoryJobQueued {
		t.Fatalf("raw repair did not advance exactly one verified hour: %+v", job)
	}
}

func TestRawPageDayFinalizerUsesDurableHourlyControlsWithoutSourceDayQuery(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 911
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for hour := day; hour < day+usageFactDaySeconds; hour += usageFactHourSeconds {
		row := UsageHourFact{HourTs: hour, DayTs: day, UserID: userID, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, TokenName: "t", Requests: 1, PromptTokens: 2, CompletionTokens: 3, ConsumeQuota: 5}
		if err := m.usageFactsStore().Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		metrics := factsMetrics([]UsageHourFact{row})
		hash := usageFactContentHash([]UsageHourFact{row})
		if err := m.usageFactsStore().Create(&UsageFactMemberHourState{UserID: userID, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, Tokens: 5, ContentHash: hash, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Create(&UsageFactPageIngestState{UserID: userID, HourTs: hour, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, Status: "complete", Pages: 1, SourceRows: metrics.Rows, Requests: metrics.Requests, PromptTokens: metrics.PromptTokens, CompletionTokens: metrics.CompletionTokens, ConsumeQuota: metrics.ConsumeQuota, ContentHash: hash, CompletedAt: time.Now().Unix()}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// If the finalizer accidentally takes the old source-day path, this test
	// would panic or fail: raw mode must use only the 24 durable controls above.
	m.prodDB = nil
	if err := m.finalizeUsageFactHistoryDayFromHours(context.Background(), day, []int64{userID}, map[int64]int64{userID: 1}, "raw-page-day"); err != nil {
		t.Fatal(err)
	}
	var proof UsageFactMemberDayState
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND date_ts = ?", userID, day).Error; err != nil {
		t.Fatal(err)
	}
	if proof.SourceRows != 24 || proof.Requests != 24 || proof.PromptTokens != 48 || proof.CompletionTokens != 72 || proof.ConsumeQuota != 120 {
		t.Fatalf("day proof did not sum verified hourly controls: %+v", proof)
	}
	// The first source log can occur at any hour. Verification must audit the
	// enclosing natural-day proof rather than looking for a bogus 21:00 day key.
	if err := auditUsageFactFullHistoryDayRange(m.usageFactsStore(), UsageFactMemberState{
		UserID: userID, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
	}, day+21*usageFactHourSeconds, day+usageFactDaySeconds); err != nil {
		t.Fatalf("unaligned first-log hour falsely failed natural-day proof: %v", err)
	}
}

func TestFullHistoryAuditUsesHourlyProofsForPartialFirstDay(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	const userID int64 = 913
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	start := day + 21*usageFactHourSeconds
	for hour := start; hour < day+usageFactDaySeconds; hour += usageFactHourSeconds {
		row := UsageHourFact{HourTs: hour, DayTs: day, UserID: userID, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, TokenName: "t", Requests: 1, PromptTokens: 2, CompletionTokens: 3, ConsumeQuota: 5}
		if err := m.usageFactsStore().Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Create(&UsageFactMemberHourState{
			UserID: userID, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, Tokens: 5,
			ContentHash: usageFactContentHash([]UsageHourFact{row}), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var dailyCount int64
	if err := m.usageFactsStore().Model(&UsageFactMemberDayState{}).Where("user_id = ? AND date_ts = ?", userID, day).Count(&dailyCount).Error; err != nil {
		t.Fatal(err)
	}
	if dailyCount != 0 {
		t.Fatalf("partial first day unexpectedly has a daily proof: %d", dailyCount)
	}
	state := UsageFactMemberState{UserID: userID, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch}
	if err := auditUsageFactFullHistoryDayRange(m.usageFactsStore(), state, start, day+usageFactDaySeconds); err != nil {
		t.Fatalf("partial first day should be verified by hourly proofs: %v", err)
	}
	if err := m.usageFactsStore().Where("user_id = ? AND hour_ts = ?", userID, start+usageFactHourSeconds).Delete(&UsageFactMemberHourState{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := auditUsageFactFullHistoryDayRange(m.usageFactsStore(), state, start, day+usageFactDaySeconds); err == nil {
		t.Fatal("partial first day audit accepted a missing hourly proof")
	}
}

func TestRawPageSourceAuditHourYieldsHighVolumeHour(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 912
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	hour := day + 11*usageFactHourSeconds
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, event := range makeUsageFactRawPageEvents(hour, userID, 2_501) {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota, event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(hour+2*usageFactHourSeconds, 0)
	job := UsageFactJob{
		ID: "raw-page-audit-912", Kind: usageFactHistoryKindAuditHour, Priority: 10, UserID: ptrInt64(userID),
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, FromTs: hour, ThroughTs: day + 3*usageFactDaySeconds,
		NextHour: hour, VerifyNextHour: day + usageFactDaySeconds, TotalHours: 62, Status: usageFactHistoryJobRunning, LeaseOwner: "raw-audit-test",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: hour, Through: hour + usageFactHourSeconds}
	if err := m.executeUsageFactHistorySourceAuditHourRawPages(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NextHour != hour || job.Status != usageFactHistoryJobQueued || job.Attempts != 0 {
		t.Fatalf("partial raw source audit must yield without cursor loss: %+v", job)
	}
	job = resumeUsageFactRawJobUntil(t, m, job.ID, hour+usageFactHourSeconds, now, m.executeUsageFactHistorySourceAuditHourRawPages)
	if job.NextHour != hour+usageFactHourSeconds || job.Kind != usageFactHistoryKindAuditHour || job.Status != usageFactHistoryJobQueued {
		t.Fatalf("raw source audit did not advance exactly one verified hour: %+v", job)
	}
}

func ptrInt64(value int64) *int64 { return &value }
