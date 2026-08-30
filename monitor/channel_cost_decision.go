package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var (
	errFinanceActivationConflict  = errors.New("待生效倍率变更冲突")
	errFinanceIdempotencyConflict = errors.New("幂等键已用于不同请求")
)

type channelPricingProposalDecisionInput struct {
	Action                 string `json:"action"`
	ExpectedStatus         string `json:"expected_status"`
	ExpectedBaseVersion    int64  `json:"expected_base_version"`
	ExpectedEvidenceDigest string `json:"expected_evidence_digest"`
	IdempotencyKey         string `json:"idempotency_key"`
	Reason                 string `json:"reason"`
	EffectiveFrom          int64  `json:"effective_from"`
}

type channelFinanceActivationCancelInput struct {
	Reason string `json:"reason"`
}

type channelFinanceActivationRate struct {
	ChannelID         int     `json:"channel_id"`
	Group             string  `json:"group"`
	UpstreamGroupName string  `json:"upstream_group_name"`
	Multiplier        float64 `json:"multiplier"`
	DiscountFactor    float64 `json:"discount_factor"`
}

type channelFinanceActivationPatchRow struct {
	Before channelFinanceActivationRate `json:"before"`
	After  channelFinanceActivationRate `json:"after"`
}

type channelFinanceActivationPatch struct {
	Rows []channelFinanceActivationPatchRow `json:"rows"`
}

func nextWholeHour(now int64) int64 { return now - now%3600 + 3600 }

func canonicalPositiveRat(value string) (*big.Rat, error) {
	value = strings.TrimSpace(value)
	rat, ok := new(big.Rat).SetString(value)
	if !ok || rat.Sign() <= 0 || rat.RatString() != value {
		return nil, errors.New("倍率候选值无效")
	}
	return rat, nil
}

func financeRatFloat(value *big.Rat) (float64, error) {
	if value == nil || value.Sign() <= 0 {
		return 0, errors.New("倍率不是正数")
	}
	result, _ := value.Float64()
	if !validChannelFinanceNumber(result) {
		return 0, errors.New("倍率候选值超出安全范围")
	}
	return result, nil
}

func channelFinanceSnapshotHash(raw string) (string, string, error) {
	normalized, err := normalizeChannelFinanceVersionJSON(raw)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256([]byte(normalized))
	return normalized, hex.EncodeToString(digest[:]), nil
}

func channelFinanceDecisionRequestHash(proposalKey string, in channelPricingProposalDecisionInput) string {
	parts := []string{
		proposalKey, in.Action, in.ExpectedStatus, strconv.FormatInt(in.ExpectedBaseVersion, 10),
		in.ExpectedEvidenceDigest, in.IdempotencyKey, in.Reason, strconv.FormatInt(in.EffectiveFrom, 10),
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

func financeActivationID(proposalKey, idempotencyKey, requestHash string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{proposalKey, idempotencyKey, requestHash}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func latestChannelFinanceVersion(tx *gorm.DB, domain string) (ChannelFinanceVersion, int64, error) {
	var latest ChannelFinanceVersion
	err := tx.Where("domain = ?", domain).Order("version DESC").First(&latest).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ChannelFinanceVersion{}, 0, nil
	}
	return latest, latest.Version, err
}

func activationRateFromRow(row ChannelFinanceChannelCost) channelFinanceActivationRate {
	return channelFinanceActivationRate{
		ChannelID: row.ChannelID, Group: row.Grp, UpstreamGroupName: strings.TrimSpace(row.UpstreamGroupName),
		Multiplier: row.Multiplier, DiscountFactor: normalizedUpstreamDiscountFactor(row.DiscountFactor),
	}
}

func sameActivationRate(a, b channelFinanceActivationRate) bool {
	return a.ChannelID == b.ChannelID && a.Group == b.Group && a.UpstreamGroupName == b.UpstreamGroupName &&
		a.Multiplier == b.Multiplier && normalizedUpstreamDiscountFactor(a.DiscountFactor) == normalizedUpstreamDiscountFactor(b.DiscountFactor)
}

func buildFinanceActivationPatch(tx *gorm.DB, proposal ChannelPricingChangeProposal) (channelFinanceActivationPatch, error) {
	var rows []ChannelFinanceChannelCost
	if err := tx.Where("channel_id = ? AND upstream_group_name = ?", proposal.LocalChannelID, proposal.SourceGroup).Order("grp ASC").Find(&rows).Error; err != nil {
		return channelFinanceActivationPatch{}, err
	}
	return buildFinanceActivationPatchFromRows(proposal, rows)
}

func buildFinanceActivationPatchFromRows(proposal ChannelPricingChangeProposal, rows []ChannelFinanceChannelCost) (channelFinanceActivationPatch, error) {
	newEffective, err := canonicalPositiveRat(proposal.NewValue)
	if err != nil {
		return channelFinanceActivationPatch{}, err
	}
	patch := channelFinanceActivationPatch{Rows: make([]channelFinanceActivationPatchRow, 0, len(rows))}
	for _, row := range rows {
		if row.ChannelID != proposal.LocalChannelID || strings.TrimSpace(row.UpstreamGroupName) != strings.TrimSpace(proposal.SourceGroup) {
			continue
		}
		before := activationRateFromRow(row)
		discount, ok := new(big.Rat).SetString(strconv.FormatFloat(before.DiscountFactor, 'g', -1, 64))
		if !ok || discount.Sign() <= 0 {
			return channelFinanceActivationPatch{}, errors.New("现有渠道折扣不是有效正数")
		}
		// Preserve the configured discount decomposition. The observed effective
		// rate changes only the base multiplier; rollback stores/restores the
		// complete original row rather than reconstructing it from a product.
		afterMultiplier, err := financeRatFloat(new(big.Rat).Quo(new(big.Rat).Set(newEffective), discount))
		if err != nil {
			return channelFinanceActivationPatch{}, err
		}
		after := before
		after.Multiplier = afterMultiplier
		patch.Rows = append(patch.Rows, channelFinanceActivationPatchRow{Before: before, After: after})
	}
	if len(patch.Rows) == 0 {
		return channelFinanceActivationPatch{}, errors.New("没有找到与上游分组精确匹配的渠道倍率行")
	}
	sort.Slice(patch.Rows, func(i, j int) bool { return patch.Rows[i].Before.Group < patch.Rows[j].Before.Group })
	return patch, nil
}

func reverseFinanceActivationPatch(patch channelFinanceActivationPatch) channelFinanceActivationPatch {
	reversed := channelFinanceActivationPatch{Rows: make([]channelFinanceActivationPatchRow, len(patch.Rows))}
	for i, row := range patch.Rows {
		reversed.Rows[i] = channelFinanceActivationPatchRow{Before: row.After, After: row.Before}
	}
	return reversed
}

func applyFinancePatchToSnapshot(raw string, patch channelFinanceActivationPatch) (string, string, error) {
	var snapshot channelFinanceVersionSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return "", "", err
	}
	afterByKey := make(map[string]channelFinanceActivationRate, len(patch.Rows))
	for _, row := range patch.Rows {
		key := strconv.Itoa(row.Before.ChannelID) + "\x00" + row.Before.Group
		if _, duplicate := afterByKey[key]; duplicate {
			return "", "", errors.New("倍率激活补丁包含重复渠道分组")
		}
		afterByKey[key] = row.After
	}
	matched := 0
	for i := range snapshot.ChannelRates {
		key := strconv.Itoa(snapshot.ChannelRates[i].ChannelID) + "\x00" + snapshot.ChannelRates[i].Group
		after, ok := afterByKey[key]
		if !ok {
			continue
		}
		snapshot.ChannelRates[i].UpstreamGroupName = after.UpstreamGroupName
		snapshot.ChannelRates[i].UpstreamMultiplier = after.Multiplier
		snapshot.ChannelRates[i].UpstreamDiscountFactor = after.DiscountFactor
		matched++
	}
	if matched != len(patch.Rows) {
		return "", "", errors.New("倍率激活补丁与当前完整快照不一致")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "", "", err
	}
	return channelFinanceSnapshotHash(string(encoded))
}

func encodeFinanceActivationPatch(patch channelFinanceActivationPatch) (string, error) {
	if len(patch.Rows) == 0 || len(patch.Rows) > maxChannelFinanceRows {
		return "", errors.New("倍率激活补丁行数无效")
	}
	sort.Slice(patch.Rows, func(i, j int) bool {
		if patch.Rows[i].Before.ChannelID != patch.Rows[j].Before.ChannelID {
			return patch.Rows[i].Before.ChannelID < patch.Rows[j].Before.ChannelID
		}
		return patch.Rows[i].Before.Group < patch.Rows[j].Before.Group
	})
	raw, err := json.Marshal(patch)
	if err != nil {
		return "", err
	}
	if len(raw) > maxChannelFinanceBody {
		return "", errors.New("倍率激活补丁超过安全大小")
	}
	return string(raw), nil
}

func decodeFinanceActivationPatch(raw string) (channelFinanceActivationPatch, error) {
	if len(raw) == 0 || len(raw) > maxChannelFinanceBody {
		return channelFinanceActivationPatch{}, errors.New("倍率激活补丁大小无效")
	}
	var patch channelFinanceActivationPatch
	if err := json.Unmarshal([]byte(raw), &patch); err != nil {
		return channelFinanceActivationPatch{}, err
	}
	if len(patch.Rows) == 0 || len(patch.Rows) > maxChannelFinanceRows {
		return channelFinanceActivationPatch{}, errors.New("倍率激活补丁行数无效")
	}
	for _, row := range patch.Rows {
		if row.Before.ChannelID <= 0 || strings.TrimSpace(row.Before.Group) == "" ||
			!validChannelFinanceNumber(row.Before.Multiplier) || !validChannelFinanceNumber(row.After.Multiplier) ||
			!validChannelFinanceNumber(row.Before.DiscountFactor) || !validChannelFinanceNumber(row.After.DiscountFactor) {
			return channelFinanceActivationPatch{}, errors.New("倍率激活补丁内容无效")
		}
		if row.Before.ChannelID != row.After.ChannelID || row.Before.Group != row.After.Group {
			return channelFinanceActivationPatch{}, errors.New("倍率激活补丁主键发生变化")
		}
	}
	return patch, nil
}

func (m *Monitor) decideChannelPricingProposalHandler(c *gin.Context) {
	if !m.cfg.ChannelCostClosureEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "渠道成本闭环未进入灰度"})
		return
	}
	proposalKey := strings.TrimSpace(c.Param("proposal_key"))
	if !validSHA256Hex(proposalKey) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "倍率候选标识无效"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var in channelPricingProposalDecisionInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "审批参数无效"})
		return
	}
	in.Action = strings.ToLower(strings.TrimSpace(in.Action))
	in.ExpectedStatus = strings.ToLower(strings.TrimSpace(in.ExpectedStatus))
	in.ExpectedEvidenceDigest = strings.TrimSpace(in.ExpectedEvidenceDigest)
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)
	in.Reason = strings.TrimSpace(in.Reason)
	validActionStatus := (in.Action == "approve" && in.ExpectedStatus == "pending") ||
		(in.Action == "reject" && in.ExpectedStatus == "pending") ||
		(in.Action == "rollback" && in.ExpectedStatus == "applied")
	if !validActionStatus || in.IdempotencyKey == "" || len(in.IdempotencyKey) > 64 || !safeChannelCostKeyID(in.IdempotencyKey) ||
		in.Reason == "" || len(in.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "审批动作、状态、幂等键或原因无效"})
		return
	}
	now := time.Now().Unix()
	requestedEffectiveFrom := in.EffectiveFrom
	if in.Action == "approve" || in.Action == "rollback" {
		if in.EffectiveFrom == 0 {
			in.EffectiveFrom = nextWholeHour(now)
		}
		if in.EffectiveFrom < 0 || in.EffectiveFrom%3600 != 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "倍率生效时间必须是整点"})
			return
		}
	}
	requestHashInput := in
	requestHashInput.EffectiveFrom = requestedEffectiveFrom
	requestHash := channelFinanceDecisionRequestHash(proposalKey, requestHashInput)
	actor := c.GetString("uname")
	var result ChannelPricingChangeProposal
	var activation ChannelFinanceActivation
	var idempotent bool
	err := m.storeDB.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		transactionNow := time.Now().Unix()
		var priorEvent ChannelPricingProposalEvent
		if err := tx.Where("proposal_key = ? AND idempotency_key = ?", proposalKey, in.IdempotencyKey).First(&priorEvent).Error; err == nil {
			if priorEvent.RequestHash != requestHash {
				return errFinanceIdempotencyConflict
			}
			idempotent = true
			if err := tx.First(&result, "proposal_key = ?", proposalKey).Error; err != nil {
				return err
			}
			_ = tx.Where("proposal_key = ? AND idempotency_key = ?", proposalKey, in.IdempotencyKey).First(&activation).Error
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// Re-check the boundary only for a new request. A successful request may
		// be replayed with the same idempotency key after the hour boundary.
		if (in.Action == "approve" || in.Action == "rollback") && in.EffectiveFrom != nextWholeHour(transactionNow) {
			return errors.New("审批跨越整点，请刷新后重新提交")
		}
		if err := tx.First(&result, "proposal_key = ?", proposalKey).Error; err != nil {
			return err
		}
		if !m.channelCostAPIAllowed(result.Domain) || result.Status != in.ExpectedStatus {
			return errors.New("倍率候选状态已变化")
		}
		if in.Action == "reject" {
			updated := tx.Model(&ChannelPricingChangeProposal{}).
				Where("proposal_key = ? AND status = 'pending'", proposalKey).
				Updates(map[string]any{"status": "rejected", "resolved_by": actor, "resolved_at": transactionNow, "updated_at": transactionNow})
			if updated.Error != nil || updated.RowsAffected != 1 {
				return errors.New("倍率候选并发变化")
			}
			result.Status, result.ResolvedBy, result.ResolvedAt, result.UpdatedAt = "rejected", actor, transactionNow, transactionNow
			return tx.Create(&ChannelPricingProposalEvent{ProposalKey: proposalKey, IdempotencyKey: in.IdempotencyKey, RequestHash: requestHash, Event: "rejected", FromStatus: "pending", ToStatus: "rejected", Actor: actor, Reason: in.Reason, CreatedAt: transactionNow}).Error
		}

		var account ChannelUpstreamAccount
		if err := tx.Where("domain = ?", result.Domain).First(&account).Error; err != nil {
			return err
		}
		if newAPIUpstreamAccountEpoch(account) != result.AccountEpoch || result.HMACKeyID != m.cfg.ChannelCostHMACKeyID {
			return errors.New("上游账户代际或证据密钥已变化")
		}
		_, latestVersion, err := latestChannelFinanceVersion(tx, result.Domain)
		if err != nil {
			return err
		}

		var patch channelFinanceActivationPatch
		expectedVersion := result.BaseVersion
		proposalFrom, proposalTo, eventName := "pending", "scheduled", "scheduled"
		rollbackOfActivation, rollbackOfVersion := "", int64(0)
		if in.Action == "approve" {
			if in.ExpectedBaseVersion != result.BaseVersion || in.ExpectedEvidenceDigest != result.EvidenceDigest {
				return errors.New("倍率候选基准版本或证据已变化")
			}
			digest, requests, err := channelPricingProposalEvidenceDigestAt(tx, account, result.SourceRef, result.SourceGroup, result.NewValue, result.EvidenceFromHour, result.EvidenceToHour-3600, m.cfg.ChannelCostHMACKeyID)
			if err != nil || digest != result.EvidenceDigest || requests != result.EvidenceRequests {
				return errors.New("倍率候选证据已过期，请重新生成")
			}
			patch, err = buildFinanceActivationPatch(tx, result)
			if err != nil {
				return err
			}
		} else {
			if result.AppliedVersion <= 0 {
				return errors.New("倍率候选缺少已应用版本")
			}
			expectedVersion = result.AppliedVersion
			var applied ChannelFinanceActivation
			if err := tx.Where("proposal_key = ? AND action = 'approve' AND status = 'applied' AND applied_version = ?", proposalKey, result.AppliedVersion).First(&applied).Error; err != nil {
				return errors.New("找不到可回滚的完整倍率补丁")
			}
			original, err := decodeFinanceActivationPatch(applied.PatchJSON)
			if err != nil {
				return err
			}
			patch = reverseFinanceActivationPatch(original)
			proposalFrom, proposalTo, eventName = "applied", "rollback_scheduled", "rollback_scheduled"
			rollbackOfActivation, rollbackOfVersion = applied.ActivationID, applied.AppliedVersion
		}
		if latestVersion != expectedVersion {
			return errors.New("计价版本已变化，拒绝自动合并")
		}
		currentRaw, err := currentChannelFinanceVersionJSON(tx, result.Domain)
		if err != nil {
			return err
		}
		currentNormalized, currentHash, err := channelFinanceSnapshotHash(currentRaw)
		if err != nil {
			return err
		}
		targetNormalized, targetHash, err := applyFinancePatchToSnapshot(currentNormalized, patch)
		if err != nil {
			return err
		}
		patchJSON, err := encodeFinanceActivationPatch(patch)
		if err != nil {
			return err
		}
		activation = ChannelFinanceActivation{
			ActivationID: financeActivationID(proposalKey, in.IdempotencyKey, requestHash),
			ProposalKey:  proposalKey, Domain: result.Domain, AccountEpoch: result.AccountEpoch,
			Action: in.Action, Status: "scheduled", ExpectedBaseVersion: expectedVersion,
			ExpectedCurrentSnapshotHash: currentHash, PatchJSON: patchJSON,
			TargetSnapshotJSON: targetNormalized, TargetSnapshotHash: targetHash,
			EvidenceDigest: result.EvidenceDigest, HMACKeyID: result.HMACKeyID,
			EffectiveAt: in.EffectiveFrom, RequestedBy: actor, RequestedAt: transactionNow,
			Reason: in.Reason, IdempotencyKey: in.IdempotencyKey, RequestHash: requestHash,
			RollbackOfActivationID: rollbackOfActivation, RollbackOfVersion: rollbackOfVersion,
			NextAttemptAt: in.EffectiveFrom, UpdatedAt: transactionNow,
		}
		if err := tx.Create(&activation).Error; err != nil {
			return err
		}
		if err := tx.Create(&ChannelFinanceActivationSlot{Domain: result.Domain, ActivationID: activation.ActivationID, EffectiveAt: activation.EffectiveAt, CreatedAt: transactionNow}).Error; err != nil {
			return errFinanceActivationConflict
		}
		updated := tx.Model(&ChannelPricingChangeProposal{}).
			Where("proposal_key = ? AND status = ?", proposalKey, proposalFrom).
			Updates(map[string]any{"status": proposalTo, "resolved_by": actor, "resolved_at": transactionNow, "updated_at": transactionNow})
		if updated.Error != nil || updated.RowsAffected != 1 {
			return errors.New("倍率候选并发变化")
		}
		result.Status, result.ResolvedBy, result.ResolvedAt, result.UpdatedAt = proposalTo, actor, transactionNow, transactionNow
		if err := tx.Create(&ChannelPricingProposalEvent{ProposalKey: proposalKey, IdempotencyKey: in.IdempotencyKey, RequestHash: requestHash, Event: eventName, FromStatus: proposalFrom, ToStatus: proposalTo, Actor: actor, Reason: in.Reason, CreatedAt: transactionNow}).Error; err != nil {
			return err
		}
		return tx.Create(&ChannelFinanceActivationEvent{ActivationID: activation.ActivationID, Event: "scheduled", Actor: actor, Detail: in.Reason, CreatedAt: transactionNow}).Error
	})
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	response := gin.H{"ok": true, "idempotent": idempotent, "proposal": result}
	if activation.ActivationID != "" {
		response["activation"] = activation
	}
	c.JSON(http.StatusOK, response)
}

func (m *Monitor) cancelChannelFinanceActivationHandler(c *gin.Context) {
	// Cancellation deliberately remains available after the feature flag is
	// switched off. Otherwise a pre-existing slot would block all manual
	// finance maintenance while neither the worker nor the operator could
	// release it.
	activationID := strings.TrimSpace(c.Param("activation_id"))
	if !validSHA256Hex(activationID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "待生效任务标识无效"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<10)
	var in channelFinanceActivationCancelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "取消参数无效"})
		return
	}
	in.Reason = strings.TrimSpace(in.Reason)
	if in.Reason == "" || len(in.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "必须填写不超过 512 字的取消原因"})
		return
	}
	now, actor := time.Now().Unix(), c.GetString("uname")
	var result ChannelFinanceActivation
	err := m.storeDB.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&result, "activation_id = ?", activationID).Error; err != nil {
			return err
		}
		if result.Status == "cancelled" {
			return nil
		}
		if result.Status != "scheduled" {
			return errors.New("只有尚未生效的任务可以取消")
		}
		proposalFrom, proposalTo := "scheduled", "pending"
		if result.Action == "rollback" {
			proposalFrom, proposalTo = "rollback_scheduled", "applied"
		}
		updated := tx.Model(&ChannelFinanceActivation{}).
			Where("activation_id = ? AND status = 'scheduled'", activationID).
			Updates(map[string]any{"status": "cancelled", "last_error": in.Reason, "updated_at": now})
		if updated.Error != nil || updated.RowsAffected != 1 {
			return errFinanceActivationConflict
		}
		updated = tx.Model(&ChannelPricingChangeProposal{}).
			Where("proposal_key = ? AND status = ?", result.ProposalKey, proposalFrom).
			Updates(map[string]any{"status": proposalTo, "resolved_by": "", "resolved_at": int64(0), "updated_at": now})
		if updated.Error != nil || updated.RowsAffected != 1 {
			return errFinanceActivationConflict
		}
		if err := tx.Where("activation_id = ?", activationID).Delete(&ChannelFinanceActivationSlot{}).Error; err != nil {
			return err
		}
		if err := tx.Create(&ChannelFinanceActivationEvent{ActivationID: activationID, Event: "cancelled", Actor: actor, Detail: in.Reason, CreatedAt: now}).Error; err != nil {
			return err
		}
		result.Status, result.LastError, result.UpdatedAt = "cancelled", in.Reason, now
		return nil
	})
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "activation": channelFinanceActivationView{
		ActivationID: result.ActivationID, Action: result.Action, Status: result.Status,
		EffectiveAt: result.EffectiveAt, RequestedBy: result.RequestedBy, RequestedAt: result.RequestedAt,
		Reason: result.Reason, AppliedVersion: result.AppliedVersion, AppliedAt: result.AppliedAt,
		Attempts: result.Attempts, NextAttemptAt: result.NextAttemptAt, LastError: result.LastError,
		RollbackOfVersion: result.RollbackOfVersion, UpdatedAt: result.UpdatedAt,
	}})
}

func markFinanceActivationConflictTx(tx *gorm.DB, activation ChannelFinanceActivation, now int64, cause error) error {
	message := cause.Error()
	if len(message) > 512 {
		message = message[:512]
	}
	if err := tx.Model(&ChannelFinanceActivation{}).Where("activation_id = ? AND status = 'scheduled'", activation.ActivationID).
		Updates(map[string]any{"status": "conflict", "last_error": message, "updated_at": now}).Error; err != nil {
		return err
	}
	if err := tx.Model(&ChannelPricingChangeProposal{}).Where("proposal_key = ? AND status IN ?", activation.ProposalKey, []string{"scheduled", "rollback_scheduled"}).
		Updates(map[string]any{"status": "conflict", "updated_at": now}).Error; err != nil {
		return err
	}
	if err := tx.Where("activation_id = ?", activation.ActivationID).Delete(&ChannelFinanceActivationSlot{}).Error; err != nil {
		return err
	}
	return tx.Create(&ChannelFinanceActivationEvent{ActivationID: activation.ActivationID, Event: "conflict", Actor: "monitor", Detail: message, CreatedAt: now}).Error
}

func applyFinanceActivationPatchTx(tx *gorm.DB, activation ChannelFinanceActivation, patch channelFinanceActivationPatch, now int64) error {
	// Validate the complete patch before the first write. Without this separate
	// pass, a missing or changed row near the end of a multi-row patch could
	// commit the preceding rows when the task is converted to a terminal
	// conflict in the same transaction.
	for _, change := range patch.Rows {
		var current ChannelFinanceChannelCost
		if err := tx.Where("channel_id = ? AND grp = ?", change.Before.ChannelID, change.Before.Group).First(&current).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errFinanceActivationConflict
			}
			return err
		}
		if !sameActivationRate(activationRateFromRow(current), change.Before) {
			return errFinanceActivationConflict
		}
	}
	for _, change := range patch.Rows {
		updated := tx.Model(&ChannelFinanceChannelCost{}).
			Where("channel_id = ? AND grp = ?", change.Before.ChannelID, change.Before.Group).
			Updates(map[string]any{
				"upstream_group_name": change.After.UpstreamGroupName,
				"multiplier":          change.After.Multiplier, "discount_factor": change.After.DiscountFactor,
				"effective_at": activation.EffectiveAt, "updated_at": now, "updated_by": activation.RequestedBy,
			})
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return errFinanceActivationConflict
		}
	}
	return nil
}

func enqueueEconomicsForFinanceActivationTx(tx *gorm.DB, activation ChannelFinanceActivation, now int64) error {
	closedThrough := now - now%3600
	if activation.EffectiveAt >= closedThrough {
		return nil
	}
	var hours []struct {
		AccountEpoch string
		HourTs       int64
	}
	if err := tx.Model(&ChannelUpstreamCostHourState{}).
		Select("account_epoch, hour_ts").
		Where("domain = ? AND hour_ts >= ? AND hour_ts < ? AND semantics_version = ? AND status = 'verified' AND reconcile_status = 'matched'", activation.Domain, activation.EffectiveAt, closedThrough, channelCostEvidenceSemanticsVersion).
		Group("account_epoch, hour_ts").Scan(&hours).Error; err != nil {
		return err
	}
	for _, hour := range hours {
		dirty := ChannelEconomicsDirtyHour{Domain: activation.Domain, AccountEpoch: hour.AccountEpoch, HourTs: hour.HourTs, Reason: "finance_activated", Generation: 1, Status: "pending", NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
		if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
			return err
		}
	}
	return nil
}

// applyOneDueChannelFinanceActivation performs no upstream I/O. The mutable
// projection, immutable version, proposal state, audit event and queue slot are
// committed together; SQLite busy/crash leaves the scheduled item retryable.
func (m *Monitor) applyOneDueChannelFinanceActivation(ctx context.Context, now int64) (bool, error) {
	if !m.cfg.ChannelCostClosureEnabled {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Production SQLite uses a 5-second busy_timeout. Keep the operation context
	// slightly wider than that bound: ordinary calls remain cancellation-aware,
	// while a locked write returns within the store's existing finite lock wait
	// and leaves the durable slot retryable.
	attemptCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	applied := false
	var selectedActivation ChannelFinanceActivation
	err := m.storeDB.WithContext(attemptCtx).Transaction(func(tx *gorm.DB) error {
		var slot ChannelFinanceActivationSlot
		if err := tx.Where("effective_at <= ?", now).Order("effective_at ASC, created_at ASC").First(&slot).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var activation ChannelFinanceActivation
		if err := tx.First(&activation, "activation_id = ?", slot.ActivationID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// An orphan slot must never occupy the head of the durable queue.
				if err := tx.Delete(&slot).Error; err != nil {
					return err
				}
				return tx.Create(&ChannelFinanceActivationEvent{ActivationID: slot.ActivationID, Event: "orphan_slot_removed", Actor: "monitor", Detail: slot.Domain, CreatedAt: now}).Error
			}
			return err
		}
		selectedActivation = activation
		if activation.Status != "scheduled" || activation.Domain != slot.Domain || activation.EffectiveAt != slot.EffectiveAt {
			return markFinanceActivationConflictTx(tx, activation, now, errors.New("待生效倍率槽与任务状态不一致"))
		}
		if !m.channelCostAPIAllowed(activation.Domain) {
			return markFinanceActivationConflictTx(tx, activation, now, errors.New("域名已移出渠道成本闭环白名单"))
		}
		var account ChannelUpstreamAccount
		if err := tx.Where("domain = ?", activation.Domain).First(&account).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return markFinanceActivationConflictTx(tx, activation, now, errors.New("上游账户已不存在"))
			}
			return err
		}
		// The operator already approved an immutable evidence/account snapshot.
		// Runtime collection switches only control future upstream I/O; they must
		// not silently cancel a durable local activation after approval. Account
		// deletion, credential generation, HMAC key and finance snapshot changes
		// remain fail-closed below.
		if newAPIUpstreamAccountEpoch(account) != activation.AccountEpoch || activation.HMACKeyID != m.cfg.ChannelCostHMACKeyID {
			return markFinanceActivationConflictTx(tx, activation, now, errors.New("上游账户代际或证据密钥已变化"))
		}
		_, latestVersion, err := latestChannelFinanceVersion(tx, activation.Domain)
		if err != nil {
			return err
		}
		if latestVersion != activation.ExpectedBaseVersion {
			return markFinanceActivationConflictTx(tx, activation, now, errors.New("计价基准版本已变化"))
		}
		currentRaw, err := currentChannelFinanceVersionJSON(tx, activation.Domain)
		if err != nil {
			return err
		}
		_, currentHash, err := channelFinanceSnapshotHash(currentRaw)
		if err != nil {
			return err
		}
		if currentHash != activation.ExpectedCurrentSnapshotHash {
			return markFinanceActivationConflictTx(tx, activation, now, errors.New("当前倍率完整快照已变化"))
		}
		patch, err := decodeFinanceActivationPatch(activation.PatchJSON)
		if err != nil {
			return markFinanceActivationConflictTx(tx, activation, now, err)
		}
		// Verify the stored target before touching mutable rows. A corrupt or
		// manually modified task is isolated and releases its domain slot.
		expectedTarget, expectedTargetHash, err := applyFinancePatchToSnapshot(currentRaw, patch)
		if err != nil {
			return markFinanceActivationConflictTx(tx, activation, now, err)
		}
		if expectedTargetHash != activation.TargetSnapshotHash || expectedTarget != activation.TargetSnapshotJSON {
			return markFinanceActivationConflictTx(tx, activation, now, errors.New("待生效倍率目标快照校验失败"))
		}
		if err := applyFinanceActivationPatchTx(tx, activation, patch, now); err != nil {
			if errors.Is(err, errFinanceActivationConflict) || errors.Is(err, gorm.ErrRecordNotFound) {
				return markFinanceActivationConflictTx(tx, activation, now, errors.New("当前渠道倍率行已变化"))
			}
			return err
		}
		targetRaw, err := currentChannelFinanceVersionJSON(tx, activation.Domain)
		if err != nil {
			return err
		}
		targetNormalized, targetHash, err := channelFinanceSnapshotHash(targetRaw)
		if err != nil {
			return err
		}
		if targetHash != activation.TargetSnapshotHash || targetNormalized != activation.TargetSnapshotJSON {
			// This is an internal invariant failure after writes. Returning an
			// error rolls the whole transaction back; the outer recovery path then
			// marks the task conflicted in a fresh transaction.
			return fmt.Errorf("%w: 生效后完整快照不一致", errFinanceActivationConflict)
		}
		newVersion := latestVersion + 1
		if err := tx.Create(&ChannelFinanceVersion{
			Domain: activation.Domain, Version: newVersion, SnapshotJSON: targetNormalized,
			EffectiveAt: activation.EffectiveAt, CreatedAt: now, UpdatedBy: activation.RequestedBy,
		}).Error; err != nil {
			return err
		}
		proposalFrom, proposalTo, eventName := "scheduled", "applied", "applied"
		if activation.Action == "rollback" {
			proposalFrom, proposalTo, eventName = "rollback_scheduled", "rolled_back", "rolled_back"
		}
		updatedProposal := tx.Model(&ChannelPricingChangeProposal{}).
			Where("proposal_key = ? AND status = ?", activation.ProposalKey, proposalFrom).
			Updates(map[string]any{"status": proposalTo, "applied_version": newVersion, "resolved_at": now, "updated_at": now})
		if updatedProposal.Error != nil || updatedProposal.RowsAffected != 1 {
			return errFinanceActivationConflict
		}
		updatedActivation := tx.Model(&ChannelFinanceActivation{}).
			Where("activation_id = ? AND status = 'scheduled'", activation.ActivationID).
			Updates(map[string]any{"status": "applied", "applied_version": newVersion, "applied_at": now, "last_error": "", "updated_at": now})
		if updatedActivation.Error != nil || updatedActivation.RowsAffected != 1 {
			return errFinanceActivationConflict
		}
		if err := enqueueEconomicsForFinanceActivationTx(tx, activation, now); err != nil {
			return err
		}
		if err := tx.Where("domain = ? AND activation_id = ?", activation.Domain, activation.ActivationID).Delete(&ChannelFinanceActivationSlot{}).Error; err != nil {
			return err
		}
		if err := tx.Create(&ChannelFinanceActivationEvent{ActivationID: activation.ActivationID, Event: eventName, Actor: "monitor", Detail: fmt.Sprintf("finance version %d", newVersion), CreatedAt: now}).Error; err != nil {
			return err
		}
		// The leading punctuation is forbidden by the public idempotency-key
		// validator, so internal events can never collide with a user request.
		internalEventKey := "!activate:" + activation.ActivationID[:54]
		if err := tx.Create(&ChannelPricingProposalEvent{ProposalKey: activation.ProposalKey, IdempotencyKey: internalEventKey, RequestHash: activation.RequestHash, Event: eventName, FromStatus: proposalFrom, ToStatus: proposalTo, Actor: "monitor", Reason: activation.Reason, RelatedVersion: newVersion, CreatedAt: now}).Error; err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		if errors.Is(err, errFinanceActivationConflict) && selectedActivation.ActivationID != "" {
			// All mutable writes above have already rolled back. Persist the
			// terminal state separately so a corrupt task cannot retry forever or
			// starve later domains.
			markErr := m.storeDB.WithContext(attemptCtx).Transaction(func(tx *gorm.DB) error {
				var current ChannelFinanceActivation
				if loadErr := tx.First(&current, "activation_id = ?", selectedActivation.ActivationID).Error; loadErr != nil {
					return loadErr
				}
				return markFinanceActivationConflictTx(tx, current, now, err)
			})
			if markErr == nil {
				return false, nil
			}
			return false, markErr
		}
		if selectedActivation.ActivationID != "" {
			message := err.Error()
			if len(message) > 512 {
				message = message[:512]
			}
			// Best effort only: SQLITE_BUSY may also block this diagnostic write;
			// the scheduled row remains intact and is retried on the next tick.
			_ = m.storeDB.WithContext(attemptCtx).Model(&ChannelFinanceActivation{}).
				Where("activation_id = ? AND status = 'scheduled'", selectedActivation.ActivationID).
				Updates(map[string]any{"attempts": gorm.Expr("attempts + 1"), "last_error": message, "updated_at": now}).Error
		}
		return false, err
	}
	return applied, nil
}
