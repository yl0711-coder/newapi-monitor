package monthlysource

// Read-only month assembly is deliberately separate from source scheduling.
// It selects original daily records OR original child records, never both,
// and does not synthesize a daily observation from child statistics.
import (
	"context"
	"database/sql"
	"errors"
	"math"
	"reflect"
	"sort"
	"time"
)

// LoadRecoveredTokenMonth opts into exact adaptive day partitions. Missing
// days, unresolved roots and lost projections remain failures, not zeroes.
func LoadRecoveredTokenMonth(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64) ([]TokenUsageWindowPair, TokenUsageMonthReport, error) {
	pairs, report, _, err := loadRecoveredTokenMonth(parent, receiver, key, scope, userID)
	return pairs, report, err
}

func loadRecoveredTokenMonth(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64) ([]TokenUsageWindowPair, TokenUsageMonthReport, bool, error) {
	var empty TokenUsageMonthReport
	if parent == nil || receiver == nil {
		return nil, empty, false, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	tx, err := receiver.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, empty, false, err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := loadFundingMonthCapturePlan(ctx, tx, key, scope, time.Now())
	if err != nil {
		return nil, empty, false, err
	}
	member := -1
	for i, id := range plan.Scope.Funding.UserIDs {
		if id == userID {
			member = i
		}
	}
	if member < 0 {
		return nil, empty, false, ErrTokenMonthCaptureIntegrity
	}
	var adaptive int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_adaptive_token_roots'`).Scan(&adaptive); err != nil {
		return nil, empty, false, err
	}
	if err := recoveredTokenBudget(ctx, tx, plan, adaptive != 0); err != nil {
		return nil, empty, false, err
	}
	n := len(plan.Scope.Funding.UserIDs)
	half := len(plan.Scans) * n / 2
	pairs := make([]TokenUsageWindowPair, 0, len(plan.Scans)/2)
	selectedSplit := false
	for day := 0; day < len(plan.Scans)/2; day++ {
		ordinal := day*n + member
		req, err := tokenCaptureRequest(plan, ordinal)
		if err != nil {
			return nil, empty, false, err
		}
		a, aerr := loadTokenCaptureSlot(ctx, tx, plan, ordinal)
		b, berr := loadTokenCaptureSlot(ctx, tx, plan, ordinal+half)
		// Only the explicit missing SLOT marker can be recovered, not every
		// sql.ErrNoRows (a lost projection is existing evidence corruption).
		if aerr != nil && !errors.Is(aerr, errTokenCaptureSlotMissing) || berr != nil && !errors.Is(berr, errTokenCaptureSlotMissing) {
			return nil, empty, false, errors.Join(ErrTokenMonthCaptureIntegrity, aerr, berr)
		}
		var root adaptiveTokenState
		rootErr := sql.ErrNoRows
		if adaptive != 0 {
			root, rootErr = loadAdaptiveTokenState(ctx, tx, key, req, time.Now())
		}
		if rootErr != nil && !errors.Is(rootErr, sql.ErrNoRows) {
			return nil, empty, false, rootErr
		}
		if aerr == nil && berr == nil {
			// Do not silently choose one of two competing full representations.
			if rootErr == nil || !TokenUsageWindowsMatch(a, b) {
				return nil, empty, false, ErrTokenMonthCaptureIntegrity
			}
			pairs = append(pairs, TokenUsageWindowPair{Older: a, Newer: b})
			continue
		}
		// A second sweep without its first is damaged old evidence, not a
		// recoverable source deadline. Do not cover it with new child captures.
		if aerr != nil && berr == nil || rootErr != nil || len(root.leaves) < 2 {
			return nil, empty, false, ErrTokenMonthCaptureIntegrity
		}
		children, err := loadAdaptiveTokenPairs(ctx, tx, root)
		if err != nil {
			return nil, empty, false, err
		}
		// If the first daily observation already exists, the selected child
		// groups must independently conserve its exact original aggregates.
		// This compares data only; no fake parent or wallet/time is produced.
		if aerr == nil && !tokenPartitionMatchesParent(a, children) {
			return nil, empty, false, ErrTokenMonthCaptureIntegrity
		}
		selectedSplit = true
		pairs = append(pairs, children...)
	}
	r := plan.Scope.Funding
	r.UserIDs = []int64{userID}
	report, err := SummarizeTokenUsageMonth(ctx, r, pairs, time.Now())
	if err != nil {
		return nil, empty, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, empty, false, err
	}
	return pairs, report, selectedSplit, nil
}

// Both stores share the original cap on retained raw/projection bytes. Count
// abandoned adaptive parents too, not merely the child records being selected.
// This is an assembly guard, not a substitute for the future writer's atomic
// admission guard. Nothing is deleted to make a month fit.
func recoveredTokenBudget(ctx context.Context, db adaptiveTokenReader, plan FundingMonthCapturePlan, adaptive bool) error {
	var used, invalid int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(length(t.payload)+COALESCE(length(u.payload),0)),0),COALESCE(sum(CASE WHEN ordinal>=? THEN 1 ELSE 0 END),0)
FROM portal_monthly_token_capture_slots t LEFT JOIN portal_monthly_usage_observations u ON t.usage_id=u.id WHERE t.job_key=?`, len(plan.Scans)*len(plan.Scope.Funding.UserIDs), plan.Key).Scan(&used, &invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return ErrTokenMonthCaptureIntegrity
	}
	if adaptive {
		var children int64
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(length(payload)+length(projection)),0) FROM monthly_adaptive_token_observations`).Scan(&children); err != nil {
			return err
		}
		if children < 0 || used > tokenMonthCaptureBytes-children {
			return ErrTokenMonthCaptureBudget
		}
	}
	if used < 0 || used > tokenMonthCaptureBytes {
		return ErrTokenMonthCaptureBudget
	}
	return nil
}

func tokenPartitionMatchesParent(parent TokenUsageWindowObservation, pairs []TokenUsageWindowPair) bool {
	type key struct {
		token  int64
		kind   int
		source string
	}
	rows := map[key]TokenUsageWindowRow{}
	for _, pair := range pairs {
		for _, row := range pair.Newer.Rows {
			k := key{row.TokenID, row.Type, row.BillingSource}
			old, exists := rows[k]
			if !exists {
				rows[k] = row
				continue
			}
			if row.Quota > math.MaxInt64-old.Quota || row.Records > math.MaxInt64-old.Records {
				return false
			}
			if row.MaxID > old.MaxID {
				old.Name = row.Name
			}
			old.Quota += row.Quota
			old.Records += row.Records
			old.MinQuota, old.MaxQuota = min(old.MinQuota, row.MinQuota), max(old.MaxQuota, row.MaxQuota)
			old.MinID, old.MaxID = min(old.MinID, row.MinID), max(old.MaxID, row.MaxID)
			old.FirstAt, old.LastAt = min(old.FirstAt, row.FirstAt), max(old.LastAt, row.LastAt)
			rows[k] = old
		}
	}
	got := make([]TokenUsageWindowRow, 0, len(rows))
	for _, r := range rows {
		got = append(got, r)
	}
	sort.Slice(got, func(i, j int) bool { return tokenUsageRowBefore(got[i], got[j]) })
	return reflect.DeepEqual(got, parent.Rows)
}

// Check the sealed projection's shape without retaining scheduler tables or
// inventing parent data. Children must be an exact recursive midpoint split
// of each CST day; the normal usage audit separately checks money/coverage.
func verifyRecoveredWindowShape(ctx context.Context, db *sql.DB, request FundingMonthRequest, windows []UsageWindowVersion) error {
	start, err := time.ParseInLocation("2006-01", request.Month, fundingMonthZone)
	if err != nil || len(request.UserIDs) != 1 || len(windows) > 31*16 {
		return ErrTokenMonthCaptureIntegrity
	}
	byDay := map[int64][]UsageWindowRequest{}
	for _, w := range windows {
		o, err := LoadUsageWindowObservation(ctx, db, w.OlderID)
		if err != nil {
			return err
		}
		r := o.Request
		day := start.Unix() + (r.From-start.Unix())/86400*86400
		if r.From < start.Unix() || r.To > start.AddDate(0, 1, 0).Unix() || r.To > day+86400 || r.UserID != request.UserIDs[0] || r.SourceIdentity != request.SourceIdentity || r.Rules != request.Rules {
			return ErrTokenMonthCaptureIntegrity
		}
		byDay[day] = append(byDay[day], r)
	}
	for day := start.Unix(); day < start.AddDate(0, 1, 0).Unix(); day += 86400 {
		leaves := byDay[day]
		sort.Slice(leaves, func(i, j int) bool { return leaves[i].From < leaves[j].From })
		if !isRecoveredTokenPartition(day, day+86400, leaves) {
			return ErrTokenMonthCaptureIntegrity
		}
	}
	return ctx.Err()
}

func isRecoveredTokenPartition(from, to int64, leaves []UsageWindowRequest) bool {
	if len(leaves) == 0 || len(leaves) > 16 {
		return false
	}
	if len(leaves) == 1 && leaves[0].From == from && leaves[0].To == to {
		return true
	}
	if to-from < 7200 {
		return false
	}
	mid := from + (to-from)/2
	split := sort.Search(len(leaves), func(i int) bool { return leaves[i].From >= mid })
	return isRecoveredTokenPartition(from, mid, leaves[:split]) && isRecoveredTokenPartition(mid, to, leaves[split:])
}
