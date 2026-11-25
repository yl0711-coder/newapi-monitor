package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// OrionUsageCheckpoint binds staging to one account, lane and fixed window.
// Staging is not a public bill. It contains typed billing fields and hashed
// request identities only: no prompts, responses, model names or credentials.
// Lane/epoch/window bind a cursor to exactly one immutable read operation.
type OrionUsageCheckpoint struct {
	Domain       string `gorm:"primaryKey;size:253"`
	Lane         string `gorm:"primaryKey;size:16"`
	Epoch        string `gorm:"size:64"`
	WindowFrom   int64
	WindowTo     int64
	NextPage     int
	NextCursor   string `gorm:"type:text" json:"-"`
	FirstHash    string `gorm:"size:64"`
	Complete     bool
	VerifyPage   int
	VerifyCursor string `gorm:"type:text" json:"-"`
	StagedBytes  int64
	UpdatedAt    int64
}

// OrionUsagePage contains only normalized billing fields and hashed IDs.
type OrionUsagePage struct {
	Domain     string `gorm:"primaryKey;size:253"`
	Lane       string `gorm:"primaryKey;size:16"`
	Page       int    `gorm:"primaryKey"`
	CursorHash string `gorm:"size:64"`
	ItemsJSON  string `gorm:"type:text" json:"-"`
}

const orionMaxStagedBytes = 32 << 20

func orionHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func sealedOrionPage(page orionRequestPage, row ChannelUpstreamAccount, from, to int64) ([]byte, error) {
	if _, err := aggregateOrionUsage(row, page.Items, from, to); err != nil {
		return nil, err
	}
	items := append([]orionRequestItem{}, page.Items...)
	for i := range items {
		items[i].RequestID = orionHash([]byte(items[i].RequestID))
		items[i].ModelRequested = ""
	}
	return json.Marshal(items)
}

func (m *Monitor) resetOrionCheckpoint(ctx context.Context, cp OrionUsageCheckpoint) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("domain = ? AND lane = ?", cp.Domain, cp.Lane).Delete(&OrionUsagePage{}).Error; err != nil {
			return err
		}
		return tx.Where("domain = ? AND lane = ?", cp.Domain, cp.Lane).Delete(&OrionUsageCheckpoint{}).Error
	})
}

func (m *Monitor) saveOrionPage(ctx context.Context, cp *OrionUsageCheckpoint, page OrionUsagePage) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&page).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(cp).Error
	})
}

// A changing open-hour end is frozen while its pages are pending; otherwise
// each scheduler turn would discard the cursor. On completion the next turn
// re-reads that hour through the newer end, preserving the existing late-row
// correction behavior. A full closed hour is never partially published.
func (m *Monitor) loadOrionUsageCheckpoint(ctx context.Context, row ChannelUpstreamAccount, lane string, from, to int64) (OrionUsageCheckpoint, error) {
	if from <= 0 || from%3600 != 0 || to <= from || to-from > orionMaxWindowSeconds {
		return OrionUsageCheckpoint{}, fmt.Errorf("Orion 用量窗口无效")
	}
	var cp OrionUsageCheckpoint
	err := m.storeDB.WithContext(ctx).First(&cp, "domain = ? AND lane = ?", row.Domain, lane).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return cp, err
	}
	epoch := newAPIUpstreamAccountEpoch(row)
	published := (lane == "tail" && row.UsageDataUntil >= cp.WindowTo) || (lane == "history" && row.UsageBackfillCursor >= cp.WindowTo)
	if err == nil && (cp.Epoch != epoch || cp.WindowFrom != from || cp.WindowTo > to || cp.WindowTo <= from || cp.NextPage < 2 || len(cp.FirstHash) != 64 || (cp.Complete && published && cp.WindowTo < to)) {
		if err = m.resetOrionCheckpoint(ctx, cp); err != nil {
			return cp, err
		}
		cp = OrionUsageCheckpoint{}
	}
	if cp.NextPage == 0 {
		cp = OrionUsageCheckpoint{Domain: row.Domain, Lane: lane, Epoch: epoch, WindowFrom: from, WindowTo: to, NextPage: 1}
	}
	return cp, nil
}

func (m *Monitor) syncOrionUsageCheckpoint(ctx context.Context, row ChannelUpstreamAccount, cred orionCredential, lane string, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, bool, error) {
	cp, err := m.loadOrionUsageCheckpoint(ctx, row, lane, from, to)
	if err != nil {
		return upstreamUsageResult{}, false, err
	}
	to = cp.WindowTo
	var size struct{ Bytes int64 }
	if err = m.storeDB.WithContext(ctx).Model(&OrionUsagePage{}).Select("COALESCE(SUM(length(items_json)),0) AS bytes").Where("domain = ? AND lane = ?", row.Domain, lane).Scan(&size).Error; err != nil {
		return upstreamUsageResult{}, false, err
	}
	if size.Bytes > orionMaxStagedBytes {
		return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页暂存超过安全容量，保留原账单")
	}
	var pages []OrionUsagePage
	if err = m.storeDB.WithContext(ctx).Where("domain = ? AND lane = ?", row.Domain, lane).Order("page").Find(&pages).Error; err != nil {
		return upstreamUsageResult{}, false, err
	}
	items := []orionRequestItem{}
	seenIDs, seenCursors := map[string]bool{}, map[string]bool{}
	var stagedBytes int64
	for i, page := range pages {
		if page.Page != i+1 || page.ItemsJSON == "" {
			return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页检查点不连续，保留原账单")
		}
		if i == 0 && orionHash([]byte(page.ItemsJSON)) != cp.FirstHash {
			return upstreamUsageResult{}, false, fmt.Errorf("Orion 首页检查点指纹不一致，保留原账单")
		}
		stagedBytes += int64(len(page.ItemsJSON))
		if stagedBytes > orionMaxStagedBytes {
			return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页暂存超过安全容量，保留原账单")
		}
		var saved []orionRequestItem
		if json.Unmarshal([]byte(page.ItemsJSON), &saved) != nil {
			return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页检查点无法解析，保留原账单")
		}
		for _, item := range saved {
			if len(item.RequestID) != 64 || seenIDs[item.RequestID] {
				return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页检查点身份重复，保留原账单")
			}
			seenIDs[item.RequestID] = true
		}
		seenCursors[page.CursorHash] = true
		items = append(items, saved...)
	}
	if len(pages) != cp.NextPage-1 || stagedBytes != cp.StagedBytes {
		return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页检查点计数不一致，保留原账单")
	}
	startedPage := cp.NextPage
	requestsThisRound := 0
	for ; !cp.Complete && requestsThisRound < orionMaxPages; requestsThisRound++ {
		page, fetchErr := fetchOrionRequestPage(ctx, m.channelUpstreamUsageHTTPClient(), row, cred, from, to, cp.NextCursor, pacer)
		if upstreamUsageRunBudgetWasExhausted(fetchErr) || isUpstreamLocalQueueTimeout(fetchErr) {
			return upstreamUsageResult{}, true, nil
		}
		if fetchErr != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && errors.Is(fetchErr, context.DeadlineExceeded) && cp.NextPage > startedPage {
				return upstreamUsageResult{}, true, nil
			}
			var status *upstreamHTTPError
			if cp.NextCursor != "" && errors.As(fetchErr, &status) && (status.Status == http.StatusBadRequest || status.Status == http.StatusGone) {
				if resetErr := m.resetOrionCheckpoint(ctx, cp); resetErr != nil {
					return upstreamUsageResult{}, false, resetErr
				}
			}
			return upstreamUsageResult{}, false, fetchErr
		}
		sealed, sealErr := sealedOrionPage(page, row, from, to)
		if sealErr != nil {
			return upstreamUsageResult{}, false, sealErr
		}
		nextCursor := ""
		if page.NextCursor != nil {
			nextCursor = *page.NextCursor
		}
		if len(nextCursor) > 4096 || (nextCursor != "" && (len(page.Items) == 0 || nextCursor == cp.NextCursor || seenCursors[orionHash([]byte(nextCursor))])) {
			return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页游标未前进，未发布部分账单")
		}
		if cp.StagedBytes+int64(len(sealed)) > orionMaxStagedBytes {
			return upstreamUsageResult{}, false, fmt.Errorf("Orion 分页暂存超过安全容量，保留原账单")
		}
		var sealedItems []orionRequestItem
		if err = json.Unmarshal(sealed, &sealedItems); err != nil {
			return upstreamUsageResult{}, false, err
		}
		for _, item := range sealedItems {
			if seenIDs[item.RequestID] {
				return upstreamUsageResult{}, false, fmt.Errorf("Orion 请求明细身份跨页重复，未发布部分账单")
			}
			seenIDs[item.RequestID] = true
		}
		stored := OrionUsagePage{Domain: row.Domain, Lane: lane, Page: cp.NextPage, CursorHash: orionHash([]byte(cp.NextCursor)), ItemsJSON: string(sealed)}
		if cp.NextPage == 1 {
			cp.FirstHash = orionHash(sealed)
		}
		cp.NextPage++
		cp.NextCursor, cp.Complete = nextCursor, nextCursor == ""
		cp.StagedBytes += int64(len(sealed))
		cp.UpdatedAt = time.Now().Unix()
		if err = m.saveOrionPage(ctx, &cp, stored); err != nil {
			return upstreamUsageResult{}, false, err
		}
		seenCursors[stored.CursorHash] = true
		items = append(items, sealedItems...)
	}
	if !cp.Complete {
		return upstreamUsageResult{}, true, nil
	}
	if yielded, err := m.verifyOrionUsageCheckpoint(ctx, row, cred, cp, pacer, requestsThisRound); err != nil || yielded {
		return upstreamUsageResult{}, yielded, err
	}
	result, err := aggregateOrionUsage(row, items, from, to)
	return result, false, err
}

func (m *Monitor) verifyOrionUsageCheckpoint(ctx context.Context, row ChannelUpstreamAccount, cred orionCredential, cp OrionUsageCheckpoint, pacer *upstreamUsageRequestPacer, requestsThisRound int) (bool, error) {
	// Orion has no independently reported total or documented immutable
	// snapshot. A first-page-only probe cannot detect middle-page corrections.
	// Re-read ALL typed pages through a fresh cursor chain before publishing;
	// proof itself is resumable and obeys the same per-turn request allowance.
	if cp.VerifyPage == 0 {
		cp.VerifyPage = 1
	}
	if cp.VerifyPage < 1 || cp.VerifyPage > cp.NextPage {
		return false, fmt.Errorf("Orion 分页复核检查点无效")
	}
	for cp.VerifyPage < cp.NextPage {
		if requestsThisRound >= upstreamUsageMaxRequestsPerRun {
			return true, nil
		}
		requestsThisRound++
		proof, proofErr := fetchOrionRequestPage(ctx, m.channelUpstreamUsageHTTPClient(), row, cred, cp.WindowFrom, cp.WindowTo, cp.VerifyCursor, pacer)
		if upstreamUsageRunBudgetWasExhausted(proofErr) || isUpstreamLocalQueueTimeout(proofErr) ||
			(errors.Is(ctx.Err(), context.DeadlineExceeded) && (proofErr == nil || errors.Is(proofErr, context.DeadlineExceeded))) {
			return true, nil
		}
		if proofErr != nil {
			var status *upstreamHTTPError
			if cp.VerifyCursor != "" && errors.As(proofErr, &status) && (status.Status == http.StatusBadRequest || status.Status == http.StatusGone) {
				if resetErr := m.resetOrionCheckpoint(ctx, cp); resetErr != nil {
					return false, resetErr
				}
			}
			return false, proofErr
		}
		sealed, sealErr := sealedOrionPage(proof, row, cp.WindowFrom, cp.WindowTo)
		if sealErr != nil {
			return false, sealErr
		}
		next := ""
		if proof.NextCursor != nil {
			next = *proof.NextCursor
		}
		var original OrionUsagePage
		if err := m.storeDB.WithContext(ctx).First(&original, "domain = ? AND lane = ? AND page = ?", row.Domain, cp.Lane, cp.VerifyPage).Error; err != nil {
			return false, err
		}
		last := cp.VerifyPage == cp.NextPage-1
		if orionHash(sealed) != orionHash([]byte(original.ItemsJSON)) || (next == "") != last || len(next) > 4096 {
			if err := m.resetOrionCheckpoint(ctx, cp); err != nil {
				return false, err
			}
			return true, nil
		}
		cp.VerifyPage++
		cp.VerifyCursor = next
		cp.UpdatedAt = time.Now().Unix()
		if err := m.storeDB.WithContext(ctx).Save(&cp).Error; err != nil {
			return false, err
		}
	}
	return false, nil
}

func (m *Monitor) syncOrionHistoryCheckpoint(ctx context.Context, row *ChannelUpstreamAccount, cred orionCredential, plan upstreamUsageSyncPlan, now int64, pacer *upstreamUsageRequestPacer, operationRetryAt *int64, secrets ...string) error {
	history, yielded, err := m.syncOrionUsageCheckpoint(ctx, *row, cred, "history", plan.backfillFrom, plan.backfillTo, pacer)
	*operationRetryAt = max(*operationRetryAt, upstreamRetryAt(err))
	if errors.Is(err, context.Canceled) {
		return err
	}
	if err == nil && !yielded {
		err = m.persistUpstreamUsageWindow(ctx, row.Domain, plan.backfillFrom, history.DataUntil, history.Hours, now)
	}
	if err != nil {
		applyUpstreamUsageBackfillResult(row, err, now, m.cfg, secrets...)
	} else if yielded {
		applyUpstreamUsageBackfillYield(row, "Orion 已保存分页进度，等待续跑", now)
	} else {
		applyUpstreamUsageBackfillResult(row, nil, now, m.cfg, secrets...)
		row.UsageBackfillCursor = history.DataUntil
		row.UsageBackfillDone = row.UsageBackfillCursor >= cstDayStart(now)
		if row.UsageBackfillDone {
			row.UsageBackfillNextSyncAt = 0
		}
	}
	return err
}
