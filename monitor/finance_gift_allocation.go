package monitor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
	"gorm.io/gorm"
)

const financeGiftUserQueryChunk = 400

// financeGiftCoverageView is the proof attached to a gift allocation. The
// amount is publishable only when Complete is true; partial evidence must not
// be extrapolated into revenue.
type financeGiftCoverageView struct {
	SeedFromTs                 int64 `json:"seed_from_ts"`
	FromTs                     int64 `json:"from_ts"`
	ToTs                       int64 `json:"to_ts"`
	RequestedToTs              int64 `json:"requested_to_ts,omitempty"`
	ProvisionalSeconds         int64 `json:"provisional_seconds,omitempty"`
	LatestHourPending          bool  `json:"latest_hour_pending"`
	ExpectedHours              int64 `json:"expected_hours"`
	UserCompletedHours         int64 `json:"user_completed_hours"`
	CreditCompletedHours       int64 `json:"credit_completed_hours"`
	SourceMismatchHours        int64 `json:"source_mismatch_hours"`
	UnattributedRecords        int64 `json:"unattributed_records"`
	EligibleGrants             int64 `json:"eligible_grants"`
	GiftUsers                  int64 `json:"gift_users"`
	ExpectedBoundaryUserHours  int64 `json:"expected_boundary_user_hours"`
	CompletedBoundaryUserHours int64 `json:"completed_boundary_user_hours"`
	Complete                   bool  `json:"complete"`
}

type financeGiftAllocationResult struct {
	Allocation financecredit.GiftAllocation
	Coverage   financeGiftCoverageView
	// ledger is retained only in memory for deriving month/day subranges from
	// the already verified overall evidence. It is never serialized to clients.
	ledger []financecredit.LedgerEvent
}

type financeGiftUserHourKey struct {
	HourTs int64
	UserID int64
}

func validFinanceGiftRange(seedFrom, from, to int64) bool {
	return seedFrom >= 0 && seedFrom%3600 == 0 && from >= seedFrom && from%3600 == 0 && to > from && to%3600 == 0
}

// loadFinanceGiftAllocation reads only locally published facts. It does not
// access NewAPI. Exact request/refund ordering is required for every monetary
// hour of every eligible gift recipient from their first grant onward.
func (m *Monitor) loadFinanceGiftAllocation(ctx context.Context, seedFrom, from, to int64) (financeGiftAllocationResult, error) {
	result := financeGiftAllocationResult{Coverage: financeGiftCoverageView{
		SeedFromTs: seedFrom, FromTs: from, ToTs: to,
	}}
	if m == nil || !validFinanceGiftRange(seedFrom, from, to) {
		return result, errors.New("invalid finance gift allocation range")
	}
	db := m.usageFactsStore()
	if db == nil {
		return result, errors.New("finance gift facts store is unavailable")
	}
	result.Coverage.ExpectedHours = (to - seedFrom) / 3600

	var userStates []FinanceUserHourState
	if err := db.WithContext(ctx).Where("hour_ts>=? AND hour_ts<?", seedFrom, to).Order("hour_ts").Find(&userStates).Error; err != nil {
		return result, fmt.Errorf("read finance user hour proofs: %w", err)
	}
	var creditStates []FinanceCreditHourState
	if err := db.WithContext(ctx).Where("hour_ts>=? AND hour_ts<?", seedFrom, to).Order("hour_ts").Find(&creditStates).Error; err != nil {
		return result, fmt.Errorf("read finance credit hour proofs: %w", err)
	}
	userByHour := make(map[int64]FinanceUserHourState, len(userStates))
	creditByHour := make(map[int64]FinanceCreditHourState, len(creditStates))
	for _, state := range userStates {
		if state.Status == "complete" && state.SemanticsVersion == financeUserFactSemanticsVersion && state.SourceEpoch != "" {
			userByHour[state.HourTs] = state
			result.Coverage.UserCompletedHours++
			if err := addEconomicsInt64(&result.Coverage.UnattributedRecords, state.UnattributedRecords); err != nil {
				return result, err
			}
		}
	}
	for _, state := range creditStates {
		if state.Status == "complete" && state.SemanticsVersion == financeCreditFactSemanticsVersion && state.SourceEpoch != "" && state.UnknownRegistrationRows == 0 {
			creditByHour[state.HourTs] = state
			result.Coverage.CreditCompletedHours++
		}
	}
	for hour := seedFrom; hour < to; hour += 3600 {
		user, userOK := userByHour[hour]
		credit, creditOK := creditByHour[hour]
		if userOK && creditOK && user.SourceEpoch != credit.SourceEpoch {
			result.Coverage.SourceMismatchHours++
		}
	}
	if result.Coverage.UserCompletedHours != result.Coverage.ExpectedHours ||
		result.Coverage.CreditCompletedHours != result.Coverage.ExpectedHours ||
		result.Coverage.SourceMismatchHours != 0 || result.Coverage.UnattributedRecords != 0 {
		return result, nil
	}

	var credits []FinanceCreditEvent
	if err := db.WithContext(ctx).Where("event_at>=? AND event_at<? AND eligible_trial=?", seedFrom, to, true).
		Order("event_at,source_log_id").Find(&credits).Error; err != nil {
		return result, fmt.Errorf("read eligible finance credit events: %w", err)
	}
	var expectedEligibleGrants int64
	for _, state := range creditByHour {
		if err := addEconomicsInt64(&expectedEligibleGrants, state.EligibleTrialRows); err != nil {
			return result, err
		}
	}
	firstGrantHour := make(map[int64]int64)
	ledger := make([]financecredit.LedgerEvent, 0, len(credits)*2)
	for _, credit := range credits {
		hour := credit.EventAt / 3600 * 3600
		state := creditByHour[hour]
		// Replayed hours retain old source epochs for auditability. Only the epoch
		// selected by the current complete hour proof is part of this allocation.
		if credit.SourceEpoch != state.SourceEpoch {
			continue
		}
		if credit.EvidenceHash != financeCreditEventHash(credit) || credit.NetChangeMicro <= 0 {
			return result, fmt.Errorf("eligible gift evidence failed verification at source log %d", credit.SourceLogID)
		}
		if previous, ok := firstGrantHour[credit.TargetUserID]; !ok || hour < previous {
			firstGrantHour[credit.TargetUserID] = hour
		}
		ledger = append(ledger, financecredit.LedgerEvent{
			UserID: credit.TargetUserID, At: credit.EventAt, Sequence: credit.SourceLogID,
			Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: credit.NetChangeMicro,
		})
	}
	result.Coverage.EligibleGrants = int64(len(ledger))
	if result.Coverage.EligibleGrants != expectedEligibleGrants {
		return result, errors.New("eligible gift events do not match current hour proofs")
	}
	result.Coverage.GiftUsers = int64(len(firstGrantHour))
	if len(firstGrantHour) == 0 {
		allocation, err := financecredit.AllocateTrialGiftConsumption(ledger, from, to)
		if err != nil {
			return result, err
		}
		result.Allocation, result.Coverage.Complete, result.ledger = allocation, true, ledger
		return result, nil
	}

	users := make([]int64, 0, len(firstGrantHour))
	for userID := range firstGrantHour {
		users = append(users, userID)
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })
	facts, err := loadFinanceGiftUserFacts(ctx, db, seedFrom, to, users)
	if err != nil {
		return result, err
	}
	required := make(map[financeGiftUserHourKey]FinanceUserHourFact)
	for _, fact := range facts {
		if fact.HourTs >= firstGrantHour[fact.UserID] {
			required[financeGiftUserHourKey{HourTs: fact.HourTs, UserID: fact.UserID}] = fact
		}
	}
	result.Coverage.ExpectedBoundaryUserHours = int64(len(required))
	states, err := loadFinanceGiftBoundaryStates(ctx, db, seedFrom, to, users)
	if err != nil {
		return result, err
	}
	stateByKey := make(map[financeGiftUserHourKey]FinanceGiftBoundaryState, len(states))
	for _, state := range states {
		key := financeGiftUserHourKey{HourTs: state.HourTs, UserID: state.UserID}
		fact, needed := required[key]
		userState := userByHour[state.HourTs]
		if !needed || state.Status != "complete" || state.SourceEpoch != userState.SourceEpoch ||
			state.Requests != fact.Requests || state.RefundRecords != fact.RefundRecords ||
			state.ConsumeQuota != fact.ConsumeQuota || state.RefundQuota != fact.RefundQuota {
			continue
		}
		stateByKey[key] = state
		result.Coverage.CompletedBoundaryUserHours++
	}
	if result.Coverage.CompletedBoundaryUserHours != result.Coverage.ExpectedBoundaryUserHours {
		return result, nil
	}

	events, err := loadFinanceGiftBoundaryEvents(ctx, db, seedFrom, to, users)
	if err != nil {
		return result, err
	}
	eventsByKey := make(map[financeGiftUserHourKey][]FinanceGiftBoundaryEvent, len(required))
	for _, event := range events {
		key := financeGiftUserHourKey{HourTs: event.HourTs, UserID: event.UserID}
		state, needed := stateByKey[key]
		if !needed || event.SourceEpoch != state.SourceEpoch || event.EvidenceHash != financeGiftBoundaryEventHash(event) {
			continue
		}
		eventsByKey[key] = append(eventsByKey[key], event)
	}
	for key, state := range stateByKey {
		hourEvents := eventsByKey[key]
		if financeGiftBoundaryContentHash(hourEvents) != state.ContentHash || int64(len(hourEvents)) != state.Rows {
			return result, fmt.Errorf("gift boundary content failed verification for user %d hour %d", key.UserID, key.HourTs)
		}
		for _, event := range hourEvents {
			amount, conversionErr := signedUnitsToMicroUSDCanonical(event.Quota, strconv.FormatInt(int64(quotaPerUSD), 10))
			if conversionErr != nil {
				return result, conversionErr
			}
			if event.Kind == "refund" {
				amount = -amount
			}
			ledger = append(ledger, financecredit.LedgerEvent{
				UserID: event.UserID, At: event.EventAt, Sequence: event.SourceLogID,
				Kind: financecredit.EventNetUsage, AmountMicroUSD: amount,
			})
		}
	}
	allocation, err := financecredit.AllocateTrialGiftConsumption(ledger, from, to)
	if err != nil {
		return result, err
	}
	result.Allocation, result.Coverage.Complete, result.ledger = allocation, true, ledger
	return result, nil
}

func financeGiftSubrange(result financeGiftAllocationResult, from, to int64) (financeGiftAllocationResult, error) {
	subrange := financeGiftAllocationResult{Coverage: financeGiftCoverageView{FromTs: from, ToTs: to}}
	if !result.Coverage.Complete || from < result.Coverage.FromTs || to > result.Coverage.ToTs || to <= from {
		return subrange, errors.New("gift allocation subrange is outside verified coverage")
	}
	allocation, err := financecredit.AllocateTrialGiftConsumption(result.ledger, from, to)
	if err != nil {
		return subrange, err
	}
	subrange.Allocation = allocation
	subrange.Coverage.Complete = true
	return subrange, nil
}

// excludeFinanceGiftUsers 在全部赠送证据通过后，从经营收入口径中
// 移除已配置的内部账号。这只改变本次内存中的分配账本，不改原始
// 额度调整事件或小时事实。
func excludeFinanceGiftUsers(result financeGiftAllocationResult, excluded map[int64]bool) (financeGiftAllocationResult, error) {
	if !result.Coverage.Complete || len(excluded) == 0 {
		return result, nil
	}
	ledger := make([]financecredit.LedgerEvent, 0, len(result.ledger))
	grantUsers := map[int64]bool{}
	var grants int64
	for _, event := range result.ledger {
		if excluded[event.UserID] {
			continue
		}
		ledger = append(ledger, event)
		if event.Kind == financecredit.EventTrialGiftGrant {
			grants++
			grantUsers[event.UserID] = true
		}
	}
	allocation, err := financecredit.AllocateTrialGiftConsumption(ledger, result.Coverage.FromTs, result.Coverage.ToTs)
	if err != nil {
		return result, err
	}
	result.ledger = ledger
	result.Allocation = allocation
	result.Coverage.EligibleGrants = grants
	result.Coverage.GiftUsers = int64(len(grantUsers))
	return result, nil
}

func loadFinanceGiftUserFacts(ctx context.Context, db *gorm.DB, from, to int64, users []int64) ([]FinanceUserHourFact, error) {
	var result []FinanceUserHourFact
	for start := 0; start < len(users); start += financeGiftUserQueryChunk {
		end := min(len(users), start+financeGiftUserQueryChunk)
		var rows []FinanceUserHourFact
		if err := db.WithContext(ctx).Where("hour_ts>=? AND hour_ts<? AND user_id IN ?", from, to, users[start:end]).
			Order("hour_ts,user_id").Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("read gifted user hour facts: %w", err)
		}
		result = append(result, rows...)
	}
	return result, nil
}

func loadFinanceGiftBoundaryStates(ctx context.Context, db *gorm.DB, from, to int64, users []int64) ([]FinanceGiftBoundaryState, error) {
	var result []FinanceGiftBoundaryState
	for start := 0; start < len(users); start += financeGiftUserQueryChunk {
		end := min(len(users), start+financeGiftUserQueryChunk)
		var rows []FinanceGiftBoundaryState
		if err := db.WithContext(ctx).Where("hour_ts>=? AND hour_ts<? AND user_id IN ?", from, to, users[start:end]).
			Order("hour_ts,user_id").Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("read gift boundary proofs: %w", err)
		}
		result = append(result, rows...)
	}
	return result, nil
}

func loadFinanceGiftBoundaryEvents(ctx context.Context, db *gorm.DB, from, to int64, users []int64) ([]FinanceGiftBoundaryEvent, error) {
	var result []FinanceGiftBoundaryEvent
	for start := 0; start < len(users); start += financeGiftUserQueryChunk {
		end := min(len(users), start+financeGiftUserQueryChunk)
		var rows []FinanceGiftBoundaryEvent
		if err := db.WithContext(ctx).Where("hour_ts>=? AND hour_ts<? AND user_id IN ?", from, to, users[start:end]).
			Order("event_at,source_log_id").Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("read gift boundary events: %w", err)
		}
		result = append(result, rows...)
	}
	return result, nil
}
