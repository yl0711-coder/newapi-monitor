package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
	"gorm.io/gorm"
)

const (
	financeGiftEvidenceCacheEntries       = 8192
	financeGiftEvidenceCacheBytes         = 8 << 20
	financeGiftEvidenceCacheMaxEntryBytes = 1 << 20
	financeGiftEvidenceCacheTTL           = 30 * time.Minute
	financeGiftEvidenceQueryKeys          = 64     // Three bound parameters per hour/user/epoch.
	financeGiftEvidenceQueryRows          = 25_000 // Soft bound; an indivisible large hour is read alone.
)

// This caches verified per-hour inputs, not wallet balances or final money.
// The caller still checks every user/credit/boundary proof on every build.
// Publishers atomically replace events and their content hash; repaired scope,
// changed epochs and changed business policies therefore cannot reuse a hit.
// Nothing is persisted to SQLite and a miss/error never uses stale evidence.
type financeGiftHourEvidence struct {
	Version            int
	Rows               int64
	Ledger             []financecredit.LedgerEvent
	UnknownScopeEvents int64
}

func (m *Monitor) getFinanceGiftEvidenceCache() *boundedByteCache {
	m.financeGiftEvidenceCacheOnce.Do(func() {
		m.financeGiftEvidenceCache = newBoundedByteCache(financeGiftEvidenceCacheEntries, financeGiftEvidenceCacheBytes)
	})
	return m.financeGiftEvidenceCache
}

func financeGiftEvidenceScopeKey(policies map[string]bool, excluded map[int64]bool) string {
	encoded, _ := json.Marshal(struct {
		Groups map[string]bool
		Users  map[int64]bool
	}{policies, excluded})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func financeGiftEvidenceKey(state FinanceGiftBoundaryState, scope string) string {
	encoded, _ := json.Marshal(state)
	hash := sha256.New()
	_, _ = hash.Write([]byte("gift-hour-evidence-v1|" + scope + "|"))
	_, _ = hash.Write(encoded)
	return hex.EncodeToString(hash.Sum(nil))
}

func projectFinanceGiftHourEvidence(state FinanceGiftBoundaryState, events []FinanceGiftBoundaryEvent, policies map[string]bool, excluded map[int64]bool) (financeGiftHourEvidence, error) {
	result := financeGiftHourEvidence{Version: 1, Rows: state.Rows}
	if int64(len(events)) != state.Rows || financeGiftBoundaryContentHash(events) != state.ContentHash {
		return result, fmt.Errorf("gift boundary content failed verification for user %d hour %d", state.UserID, state.HourTs)
	}
	hasExcludedGroups := channelBusinessGroupsExcluded(policies)
	for _, event := range events {
		if excluded[event.UserID] {
			continue
		}
		if hasExcludedGroups && !event.GroupKnown && event.Quota != 0 {
			result.UnknownScopeEvents++
			continue
		}
		amount, err := signedUnitsToMicroUSDCanonical(event.Quota, strconv.FormatInt(int64(quotaPerUSD), 10))
		if err != nil {
			return result, err
		}
		if event.Kind == "refund" {
			amount = -amount
		}
		excludedGroup := !channelBusinessGroupIncluded(policies, event.Group)
		scope := "business"
		if excludedGroup {
			scope = "excluded"
		}
		result.Ledger = append(result.Ledger, financecredit.LedgerEvent{
			UserID: event.UserID, At: event.EventAt, Sequence: event.SourceLogID,
			Kind: financecredit.EventNetUsage, AmountMicroUSD: amount, Scope: scope, Excluded: excludedGroup,
		})
	}
	return result, nil
}

// Walk verified hours in stable order. Misses are fetched in small indexed
// batches and released before the next batch, rather than retaining every raw
// event from the entire history alongside its normalized ledger.
func (m *Monitor) walkFinanceGiftHourEvidence(ctx context.Context, db *gorm.DB, states map[financeGiftUserHourKey]FinanceGiftBoundaryState, policies map[string]bool, excluded map[int64]bool, visit func(financeGiftUserHourKey, financeGiftHourEvidence)) (err error) {
	started := time.Now()
	defer func() { logFinanceReadStageTiming("gift-boundary-events", started, err) }()
	cache := m.getFinanceGiftEvidenceCache()
	version, reuse := m.financeGiftEvidenceGuard.version(ctx, db)
	usedCache := false
	defer func() {
		if err == nil && usedCache {
			current, ok := m.financeGiftEvidenceGuard.version(ctx, db)
			if !ok || current != version {
				err = errFinanceFactsChanged
			}
		}
	}()
	scope := version + "|" + financeGiftEvidenceScopeKey(policies, excluded)
	keys := make([]financeGiftUserHourKey, 0, len(states))
	for key := range states {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].HourTs != keys[j].HourTs {
			return keys[i].HourTs < keys[j].HourTs
		}
		return keys[i].UserID < keys[j].UserID
	})
	missing := make([]financeGiftUserHourKey, 0, financeGiftEvidenceQueryKeys)
	var pendingRows int64
	flush := func() error {
		if len(missing) == 0 {
			return nil
		}
		batch, err := loadFinanceGiftEvidenceBatch(ctx, db, missing, states, policies, excluded)
		if err != nil {
			return err
		}
		for _, key := range missing {
			if err := ctx.Err(); err != nil {
				return err
			}
			state := states[key]
			value := batch[key]
			payload, err := json.Marshal(value)
			if err != nil {
				return err
			}
			if reuse && len(payload) <= financeGiftEvidenceCacheMaxEntryBytes {
				cache.Put(financeGiftEvidenceKey(state, scope), payload, financeGiftEvidenceCacheTTL, time.Now())
			}
			visit(key, value)
		}
		missing = missing[:0]
		pendingRows = 0
		return nil
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		cacheKey := financeGiftEvidenceKey(states[key], scope)
		if payload, ok := cache.Get(cacheKey, time.Now()); reuse && ok {
			var value financeGiftHourEvidence
			if json.Unmarshal(payload, &value) == nil && value.Version == 1 && value.Rows == states[key].Rows && value.UnknownScopeEvents >= 0 {
				usedCache = true
				visit(key, value)
				continue
			}
			cache.Delete(cacheKey)
		}
		if len(missing) > 0 && pendingRows+states[key].Rows > financeGiftEvidenceQueryRows {
			if err := flush(); err != nil {
				return err
			}
		}
		missing = append(missing, key)
		pendingRows += states[key].Rows
		if len(missing) == financeGiftEvidenceQueryKeys {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func loadFinanceGiftEvidenceBatch(ctx context.Context, db *gorm.DB, keys []financeGiftUserHourKey, states map[financeGiftUserHourKey]FinanceGiftBoundaryState, policies map[string]bool, excluded map[int64]bool) (map[financeGiftUserHourKey]financeGiftHourEvidence, error) {
	conditions := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)*3)
	for _, key := range keys {
		conditions = append(conditions, "(hour_ts=? AND user_id=? AND source_epoch=?)")
		args = append(args, key.HourTs, key.UserID, states[key].SourceEpoch)
	}
	byKey := make(map[financeGiftUserHourKey][]FinanceGiftBoundaryEvent, len(keys))
	query := db.WithContext(ctx).Model(&FinanceGiftBoundaryEvent{}).Where(strings.Join(conditions, " OR "), args...)
	if err := walkFinanceGiftBoundaryQuery(ctx, query, func(event FinanceGiftBoundaryEvent) error {
		key := financeGiftUserHourKey{HourTs: event.HourTs, UserID: event.UserID}
		state, ok := states[key]
		if ok && event.SourceEpoch == state.SourceEpoch && event.EvidenceHash == financeGiftBoundaryEventHash(event) {
			byKey[key] = append(byKey[key], event)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read gift boundary events: %w", err)
	}
	result := make(map[financeGiftUserHourKey]financeGiftHourEvidence, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := projectFinanceGiftHourEvidence(states[key], byKey[key], policies, excluded)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}
