package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"time"

	"gorm.io/gorm"
)

type aiCodeWithRecordRestartError struct{ message string }

func (e *aiCodeWithRecordRestartError) Error() string { return e.message }

func recordRestart(message string) error { return &aiCodeWithRecordRestartError{message: message} }

func aiCodeWithRecordAuthError(err error) error {
	var status *upstreamHTTPError
	if errors.As(err, &status) && (status.Status == 401 || status.Status == 403) {
		return &upstreamAuthError{err: err}
	}
	return err
}

func clearAICodeWithRecordCheckpoint(tx *gorm.DB, domain, round, slot string) error {
	query := func() *gorm.DB {
		q := tx.Where("domain = ?", domain)
		if round != "" {
			q = q.Where("round_id = ?", round)
		}
		if slot != "" {
			q = q.Where("slot_id = ?", slot)
		}
		return q
	}
	if err := query().Delete(&AICodeWithRecordSeen{}).Error; err != nil {
		return err
	}
	return query().Delete(&AICodeWithRecordCheckpoint{}).Error
}

func loadAICodeWithRecordHours(checkpoint AICodeWithRecordCheckpoint) ([]aiCodeWithRecordHour, error) {
	hours := make([]aiCodeWithRecordHour, 24)
	if checkpoint.HoursJSON != "" {
		if err := json.Unmarshal([]byte(checkpoint.HoursJSON), &hours); err != nil || len(hours) != 24 {
			return nil, fmt.Errorf("AICodeWith record 小时断点无效")
		}
	}
	for _, h := range hours {
		if h.Requests < 0 || h.Tokens < 0 || h.CostUnits < 0 {
			return nil, fmt.Errorf("AICodeWith record 小时断点金额无效")
		}
	}
	return hours, nil
}

func (m *Monitor) commitAICodeWithRecordPage(ctx context.Context, cp *AICodeWithRecordCheckpoint, round AICodeWithUsageRound, page aiCodeWithRecordPage, now int64) error {
	if err := m.ensureAICodeWithRecordCapacity(); err != nil {
		return err
	}
	if cp.SourceKeyID != 0 && cp.SourceKeyID != page.KeyID {
		return recordRestart("AICodeWith record 翻页期间 api_key_id 变化")
	}
	hours, err := loadAICodeWithRecordHours(*cp)
	if err != nil {
		return err
	}
	var cursors []string
	if cp.CursorHashesJSON != "" {
		if err := json.Unmarshal([]byte(cp.CursorHashesJSON), &cursors); err != nil {
			return fmt.Errorf("AICodeWith record 游标断点损坏")
		}
	}
	if page.HasMore {
		hash := fmt.Sprintf("%x", sha256Sum(page.Cursor))
		for _, old := range cursors {
			if hash == old {
				return recordRestart("AICodeWith record 游标重复，拒绝循环请求")
			}
		}
		cursors = append(cursors, hash)
	}
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		newRecords := 0
		for _, record := range page.Records {
			if record.CreatedAt > now+300 {
				return fmt.Errorf("AICodeWith record 时间超过当前时钟安全范围")
			}
			idHash := fmt.Sprintf("%x", sha256Sum(record.ID))
			contentHash := fmt.Sprintf("%x", sha256Sum(fmt.Sprintf("%d:%d:%d", record.CreatedAt, record.Tokens, record.CostUnits)))
			var seen AICodeWithRecordSeen
			err := tx.First(&seen, "domain = ? AND round_id = ? AND slot_id = ? AND record_hash = ?", cp.Domain, cp.RoundID, cp.SlotID, idHash).Error
			if err == nil {
				if seen.ContentHash != contentHash {
					return recordRestart("AICodeWith record 同 ID 内容变化，需重新校验")
				}
				continue
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			seen = AICodeWithRecordSeen{Domain: cp.Domain, RoundID: cp.RoundID, SlotID: cp.SlotID, RecordHash: idHash, ContentHash: contentHash}
			if err := tx.Create(&seen).Error; err != nil {
				return err
			}
			newRecords++
			for _, pair := range []struct {
				target *int64
				value  int64
			}{{&cp.Requests, 1}, {&cp.Tokens, record.Tokens}, {&cp.CostUnits, record.CostUnits}} {
				if err := addPricingCounter(pair.target, pair.value); err != nil {
					return err
				}
			}
			if record.CreatedAt < round.WindowTo {
				h := &hours[(record.CreatedAt-round.WindowFrom)/3600]
				for _, pair := range []struct {
					target *int64
					value  int64
				}{{&h.Requests, 1}, {&h.Tokens, record.Tokens}, {&h.CostUnits, record.CostUnits}} {
					if err := addPricingCounter(pair.target, pair.value); err != nil {
						return err
					}
				}
			}
		}
		if page.HasMore && newRecords == 0 {
			return recordRestart("AICodeWith record 分页未推进")
		}
		hourJSON, _ := json.Marshal(hours)
		cursorJSON, _ := json.Marshal(cursors)
		cp.HoursJSON, cp.CursorHashesJSON = string(hourJSON), string(cursorJSON)
		cp.SourceKeyID, cp.Cursor, cp.RecordsDone, cp.UpdatedAt = page.KeyID, page.Cursor, !page.HasMore, now
		cp.Pages++
		return tx.Save(cp).Error
	})
}

func verifyAICodeWithRecordDay(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, secret string, day int64, cp AICodeWithRecordCheckpoint, pacer *upstreamUsageRequestPacer) error {
	if err := pacer.beforeRequest(ctx); err != nil {
		return err
	}
	date := time.Unix(day, 0).In(cstLocation).Format("2006-01-02")
	q := url.Values{"start": {date}, "end": {date}, "group_by": {"day"}}
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, aicodeWithEndpoint(row.BaseURL, "/api/v1/api-keys/usage")+"?"+q.Encode(), map[string]string{"Authorization": "Bearer " + secret}, nil)
	if err != nil {
		return aiCodeWithRecordAuthError(err)
	}
	var env struct {
		Data struct {
			KeyID   json.RawMessage `json:"api_key_id"`
			GroupBy string          `json:"group_by"`
			Period  struct{ Start, End string }
			Summary struct {
				Cost, Requests json.RawMessage
				Tokens         json.RawMessage `json:"total_tokens"`
			}
		}
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("AICodeWith 日账单控制总数无效")
	}
	d := env.Data
	id, ie := rawJSONInt64Exact(d.KeyID)
	requests, re := rawJSONInt64Exact(d.Summary.Requests)
	tokens, te := rawJSONInt64Exact(d.Summary.Tokens)
	cost, ce := aiCodeWithExactCost(d.Summary.Cost)
	if ie != nil || re != nil || te != nil || ce != nil || id != cp.SourceKeyID || d.GroupBy != "day" || d.Period.Start != date || d.Period.End != date {
		return fmt.Errorf("AICodeWith 日账单控制身份、范围或金额无效")
	}
	if requests != cp.Requests || tokens != cp.Tokens || cost != cp.CostUnits {
		return fmt.Errorf("AICodeWith record 与日账单控制总数不一致，保留已发布账单")
	}
	return nil
}

// Returns ready=false on a normal budget yield. No incomplete key is staged
// as complete. An open day is an observed snapshot; the history lane seals it
// against the independent daily control after midnight.
func (m *Monitor) fetchAICodeWithRecordWindow(ctx context.Context, row ChannelUpstreamAccount, secret string, round AICodeWithUsageRound, slot string, now int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, bool, error) {
	if round.WindowFrom != cstDayStart(round.WindowFrom) || round.WindowTo <= round.WindowFrom || round.WindowTo > round.WindowFrom+86400 || round.WindowTo%3600 != 0 {
		return upstreamUsageResult{}, false, fmt.Errorf("AICodeWith record 窗口须为单日完整小时")
	}
	unit := row.BalanceUnit
	if unit <= 0 {
		unit = 1
	}
	if math.IsNaN(unit) || math.IsInf(unit, 0) {
		return upstreamUsageResult{}, false, fmt.Errorf("AICodeWith record 换算单位无效")
	}
	cp := AICodeWithRecordCheckpoint{Domain: row.Domain, RoundID: round.RoundID, SlotID: slot, Unit: unit}
	err := m.storeDB.WithContext(ctx).First(&cp, "domain = ? AND round_id = ? AND slot_id = ?", row.Domain, round.RoundID, slot).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return upstreamUsageResult{}, false, err
	}
	if cp.Unit != unit {
		return upstreamUsageResult{}, false, fmt.Errorf("AICodeWith record 扫描期间换算单位变化")
	}
	for !cp.RecordsDone {
		if err := m.ensureAICodeWithRecordCapacity(); err != nil {
			return upstreamUsageResult{}, false, err
		}
		if cp.Pages >= aiCodeWithRecordMaxPages {
			return upstreamUsageResult{}, false, fmt.Errorf("AICodeWith record 超出安全分页上限")
		}
		page, err := fetchAICodeWithRecordPage(ctx, m.channelUpstreamHTTPClient(), row, secret, round.WindowFrom, cp.Cursor, pacer)
		if err != nil {
			var budget *upstreamUsageRunBudgetExhausted
			if errors.As(err, &budget) {
				return upstreamUsageResult{}, false, nil
			}
			var status *upstreamHTTPError
			if cp.Cursor != "" && errors.As(err, &status) && status.Status == http.StatusBadRequest {
				if resetErr := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return clearAICodeWithRecordCheckpoint(tx, row.Domain, round.RoundID, slot) }); resetErr != nil {
					return upstreamUsageResult{}, false, resetErr
				}
			}
			return upstreamUsageResult{}, false, err
		}
		if err := m.commitAICodeWithRecordPage(ctx, &cp, round, page, now); err != nil {
			var restart *aiCodeWithRecordRestartError
			if errors.As(err, &restart) {
				if resetErr := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return clearAICodeWithRecordCheckpoint(tx, row.Domain, round.RoundID, slot) }); resetErr != nil {
					return upstreamUsageResult{}, false, resetErr
				}
			}
			return upstreamUsageResult{}, false, err
		}
	}
	if round.WindowFrom < cstDayStart(now) {
		if err := verifyAICodeWithRecordDay(ctx, m.channelUpstreamHTTPClient(), row, secret, round.WindowFrom, cp, pacer); err != nil {
			var budget *upstreamUsageRunBudgetExhausted
			if errors.As(err, &budget) {
				return upstreamUsageResult{}, false, nil
			}
			// On semantic drift start a fresh snapshot next time. Network/auth
			// errors retain the completed scan and only retry its control GET.
			var status *upstreamHTTPError
			var auth *upstreamAuthError
			if !errors.As(err, &status) && !errors.As(err, &auth) && !isUpstreamUsageTransientFailure(err) && ctx.Err() == nil {
				if cleanup := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return clearAICodeWithRecordCheckpoint(tx, row.Domain, round.RoundID, slot) }); cleanup != nil {
					return upstreamUsageResult{}, false, cleanup
				}
			}
			return upstreamUsageResult{}, false, err
		}
	}
	hours, err := loadAICodeWithRecordHours(cp)
	if err != nil {
		return upstreamUsageResult{}, false, err
	}
	result := upstreamUsageResult{Adapter: upstreamUsageAdapterAICodeWithRecord, SourceKeyID: cp.SourceKeyID, DataUntil: round.WindowTo}
	for stamp := round.WindowFrom; stamp < round.WindowTo; stamp += 3600 {
		h := hours[(stamp-round.WindowFrom)/3600]
		quota := float64(h.CostUnits) / float64(aiCodeWithRecordScale)
		result.Hours = append(result.Hours, ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: stamp, BucketSeconds: 3600, Requests: h.Requests, Tokens: h.Tokens, Quota: quota, CostUSD: quota / unit, UnitPerUSD: unit, SourceCostUnits: h.CostUnits, Provider: row.Provider, SourceKind: upstreamUsageAdapterAICodeWithRecord, Provisional: round.WindowFrom >= cstDayStart(now)})
	}
	return result, true, nil
}
