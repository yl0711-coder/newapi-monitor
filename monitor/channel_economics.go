package monitor

// Immutable hourly channel economics assembled exclusively from local SQLite
// facts. It never queries NewAPI or an upstream provider from a page request.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const channelEconomicsSemanticsVersion = 1

type channelEconomicsAccumulator struct {
	channelID        int
	localRequests    int64
	localRefunds     int64
	upstreamRequests int64
	localConsume     int64
	localRefund      int64
	chargeUnits      int64
	costRows         []ChannelUpstreamCostHourEvidence
	mappingParts     []string
	hasLocal         bool
	hasCost          bool
	unallocated      bool
}

type channelEconomicsUnallocatedRefund struct {
	Records int64
	Quota   int64
}

func loadChannelEconomicsUnallocatedRefundTx(tx *gorm.DB, hourTs int64) (channelEconomicsUnallocatedRefund, error) {
	var fact channelEconomicsUnallocatedRefund
	err := tx.Raw(`SELECT COALESCE(SUM(refund_records),0) records,
		COALESCE(SUM(refund_quota),0) quota
		FROM stability_hour_samples
		WHERE hour_ts=? AND traffic_class_version=? AND channel_id<=0`, hourTs, userTrafficClassificationVersion).Scan(&fact).Error
	if err != nil {
		return fact, err
	}
	if fact.Records < 0 || fact.Quota < 0 {
		return fact, errors.New("本地收入小时包含无效未归属退款")
	}
	return fact, nil
}

func upsertChannelEconomicsGlobalFactTx(tx *gorm.DB, hourTs, now int64, fact channelEconomicsUnallocatedRefund) error {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		strconv.FormatInt(hourTs, 10), strconv.Itoa(channelEconomicsSemanticsVersion),
		strconv.FormatInt(fact.Records, 10), strconv.FormatInt(fact.Quota, 10),
	}, "\x00")))
	row := ChannelEconomicsGlobalHourFact{
		HourTs: hourTs, SemanticsVersion: channelEconomicsSemanticsVersion,
		UnallocatedRefundRecords: fact.Records, UnallocatedRefundQuota: fact.Quota,
		SourceHash: hex.EncodeToString(digest[:]), UpdatedAt: now,
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hour_ts"}, {Name: "semantics_version"}},
		DoUpdates: clause.AssignmentColumns([]string{"unallocated_refund_records", "unallocated_refund_quota", "source_hash", "updated_at"}),
	}).Create(&row).Error
}

func insertChannelEconomicsGlobalFactIfMissingTx(tx *gorm.DB, hourTs, now int64, fact channelEconomicsUnallocatedRefund) error {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		strconv.FormatInt(hourTs, 10), strconv.Itoa(channelEconomicsSemanticsVersion),
		strconv.FormatInt(fact.Records, 10), strconv.FormatInt(fact.Quota, 10),
	}, "\x00")))
	row := ChannelEconomicsGlobalHourFact{
		HourTs: hourTs, SemanticsVersion: channelEconomicsSemanticsVersion,
		UnallocatedRefundRecords: fact.Records, UnallocatedRefundQuota: fact.Quota,
		SourceHash: hex.EncodeToString(digest[:]), UpdatedAt: now,
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error
}

func nonnegativeFloatRat(value float64) (*big.Rat, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return nil, errors.New("计价参数不是有效非负数")
	}
	rat, ok := new(big.Rat).SetString(strconv.FormatFloat(value, 'g', -1, 64))
	if !ok || rat.Sign() < 0 {
		return nil, errors.New("计价参数无法精确表示")
	}
	return rat, nil
}

func roundedNonnegativeRatInt64(value *big.Rat) (int64, error) {
	if value == nil || value.Sign() < 0 {
		return 0, errors.New("金额不是非负有理数")
	}
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(value.Num(), value.Denom(), remainder)
	if new(big.Int).Lsh(remainder, 1).Cmp(value.Denom()) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, errors.New("金额超出 int64 安全范围")
	}
	return quotient.Int64(), nil
}

func unitsToMicroUSDCanonical(units int64, unitsPerUSD string) (int64, error) {
	if units < 0 {
		return 0, errors.New("计费单位不得为负数")
	}
	unitRat, ok := new(big.Rat).SetString(unitsPerUSD)
	if !ok || unitRat.Sign() <= 0 || unitRat.RatString() != unitsPerUSD {
		return 0, errors.New("上游计费单位无效")
	}
	value := new(big.Rat).SetInt64(units)
	value.Mul(value, big.NewRat(1_000_000, 1))
	value.Quo(value, unitRat)
	return roundedNonnegativeRatInt64(value)
}

func signedUnitsToMicroUSDCanonical(units int64, unitsPerUSD string) (int64, error) {
	if units >= 0 {
		return unitsToMicroUSDCanonical(units, unitsPerUSD)
	}
	if units == math.MinInt64 {
		return 0, errors.New("净计费单位超出 int64 安全范围")
	}
	value, err := unitsToMicroUSDCanonical(-units, unitsPerUSD)
	return -value, err
}

func correctedCostMicroUSD(cost int64, paid, credit float64) (int64, error) {
	if cost < 0 {
		return 0, errors.New("上游成本不得为负数")
	}
	paidRat, err := nonnegativeFloatRat(paid)
	if err != nil || paidRat.Sign() <= 0 {
		return 0, errors.New("充值支付金额无效")
	}
	creditRat, err := nonnegativeFloatRat(credit)
	if err != nil || creditRat.Sign() <= 0 {
		return 0, errors.New("充值到账金额无效")
	}
	value := new(big.Rat).SetInt64(cost)
	value.Mul(value, paidRat)
	value.Quo(value, creditRat)
	return roundedNonnegativeRatInt64(value)
}

func subtractNonnegativeInt64(value, subtract int64) (int64, error) {
	if subtract < 0 {
		return 0, errors.New("待扣金额不得为负数")
	}
	if value < math.MinInt64+subtract {
		return 0, errors.New("利润金额超出 int64 安全范围")
	}
	return value - subtract, nil
}

func economicsFinanceAt(tx *gorm.DB, domain string, hourTs int64) (int64, float64, float64, bool, error) {
	var version ChannelFinanceVersion
	err := tx.Where("domain = ? AND effective_at <= ?", domain, hourTs).Order("effective_at DESC, version DESC").First(&version).Error
	if err == nil {
		var snapshot channelFinanceVersionSnapshot
		if jsonErr := json.Unmarshal([]byte(version.SnapshotJSON), &snapshot); jsonErr != nil {
			return 0, 0, 0, false, fmt.Errorf("解析历史计价版本 %d: %w", version.Version, jsonErr)
		}
		if snapshot.UpstreamRechargePaid <= 0 || snapshot.UpstreamRechargeCredit <= 0 {
			return version.Version, 0, 0, false, errors.New("历史计价版本缺少有效充值比例")
		}
		return version.Version, snapshot.UpstreamRechargePaid, snapshot.UpstreamRechargeCredit, true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, 0, 0, false, err
	}
	// Never apply today's mutable domain configuration to an old hour. The raw
	// upstream cost can still be published, but corrected cost and profit stay
	// explicitly unknown until a version effective at the hour exists.
	return 0, 0, 0, false, nil
}

func economicsLogicalKey(domain, epoch string, hourTs int64, channelID int) string {
	parts := []string{domain, epoch, strconv.FormatInt(hourTs, 10), strconv.Itoa(channelID), strconv.Itoa(channelEconomicsSemanticsVersion)}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func economicsPublicationID(logicalKey, sourceHash string, revision int64) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{logicalKey, sourceHash, strconv.FormatInt(revision, 10)}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func economicsManifestLogicalKey(domain string, hourTs int64) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{domain, strconv.FormatInt(hourTs, 10), strconv.Itoa(channelEconomicsSemanticsVersion)}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func economicsManifestPublicationID(logicalKey, sourceHash string, revision int64) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{"manifest", logicalKey, sourceHash, strconv.FormatInt(revision, 10)}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func economicsManifestCoverage(rows []ChannelEconomicsHourPublication, localFactStatus string, refund channelEconomicsUnallocatedRefund) (string, bool, int64) {
	if refund.Records > 0 || refund.Quota > 0 {
		return "refund_unallocated", false, 0
	}
	if localFactStatus != "verified" {
		return "local_fact_unverified", false, 0
	}
	financeVersion := int64(0)
	for _, row := range rows {
		if row.CoverageStatus == "superseded_empty" {
			continue
		}
		if row.CoverageStatus != "verified_complete" || !row.ProfitKnown {
			status := strings.TrimSpace(row.CoverageStatus)
			if status == "" {
				status = "economics_incomplete"
			}
			return status, false, 0
		}
		if financeVersion == 0 {
			financeVersion = row.FinanceVersion
		} else if financeVersion != row.FinanceVersion {
			return "finance_version_conflict", false, 0
		}
	}
	return "verified_complete", true, financeVersion
}

func publishChannelEconomicsManifestTx(tx *gorm.DB, account ChannelUpstreamAccount, epoch string, costState ChannelUpstreamCostHourState, localFactStatus string, refund channelEconomicsUnallocatedRefund, hourTs int64, reason string, now int64) error {
	var childRows []ChannelEconomicsHourPublication
	if err := tx.Raw(`SELECT p.*
		FROM channel_economics_hour_current cur
		JOIN channel_economics_hour_publications p ON p.publication_id=cur.publication_id
		WHERE p.domain=? AND p.account_epoch=? AND p.hour_ts=? AND p.semantics_version=?
		ORDER BY p.local_channel_id`, account.Domain, epoch, hourTs, channelEconomicsSemanticsVersion).Scan(&childRows).Error; err != nil {
		return err
	}
	publicationIDs := make([]string, 0, len(childRows))
	for _, row := range childRows {
		publicationIDs = append(publicationIDs, row.PublicationID)
	}
	sort.Strings(publicationIDs)
	setDigest := sha256.Sum256([]byte(strings.Join(publicationIDs, "\n")))
	publicationSetHash := hex.EncodeToString(setDigest[:])
	coverage, profitKnown, financeVersion := economicsManifestCoverage(childRows, localFactStatus, refund)
	sourceParts := []string{
		account.Domain, epoch, strconv.FormatInt(hourTs, 10), strconv.Itoa(channelEconomicsSemanticsVersion),
		costState.ContentHash, costState.ChargeUnitsPerUSD, localFactStatus,
		strconv.FormatInt(refund.Records, 10), strconv.FormatInt(refund.Quota, 10),
		strconv.FormatInt(int64(len(childRows)), 10), publicationSetHash, coverage,
		strconv.FormatBool(profitKnown), strconv.FormatInt(financeVersion, 10),
	}
	sourceDigest := sha256.Sum256([]byte(strings.Join(sourceParts, "\x00")))
	sourceHash := hex.EncodeToString(sourceDigest[:])
	logicalKey := economicsManifestLogicalKey(account.Domain, hourTs)
	var current ChannelEconomicsHourManifestCurrent
	currentErr := tx.Where("domain = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, hourTs, channelEconomicsSemanticsVersion).First(&current).Error
	if currentErr == nil {
		var previous ChannelEconomicsHourManifestPublication
		if err := tx.First(&previous, "manifest_id = ?", current.ManifestID).Error; err != nil {
			return err
		}
		if previous.SourceHash == sourceHash && previous.AuthoritativeEpoch == epoch {
			return nil
		}
	} else if !errors.Is(currentErr, gorm.ErrRecordNotFound) {
		return currentErr
	}
	revision := current.Revision + 1
	manifestID := economicsManifestPublicationID(logicalKey, sourceHash, revision)
	manifest := ChannelEconomicsHourManifestPublication{
		ManifestID: manifestID, LogicalKey: logicalKey, Revision: revision, SupersedesManifestID: current.ManifestID,
		Domain: account.Domain, HourTs: hourTs, SemanticsVersion: channelEconomicsSemanticsVersion,
		AuthoritativeEpoch: epoch, RowCount: int64(len(childRows)), PublicationSetHash: publicationSetHash,
		CostSourceHash: costState.ContentHash, LocalFactStatus: localFactStatus, FinanceVersion: financeVersion,
		CoverageStatus: coverage, ProfitKnown: profitKnown, SourceHash: sourceHash,
		PublicationReason: strings.TrimSpace(reason), PublishedAt: now,
	}
	if err := tx.Create(&manifest).Error; err != nil {
		return err
	}
	pointer := ChannelEconomicsHourManifestCurrent{
		Domain: account.Domain, HourTs: hourTs, SemanticsVersion: channelEconomicsSemanticsVersion,
		ManifestID: manifestID, Revision: revision, UpdatedAt: now,
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "domain"}, {Name: "hour_ts"}, {Name: "semantics_version"}},
		DoUpdates: clause.AssignmentColumns([]string{"manifest_id", "revision", "updated_at"}),
	}).Create(&pointer).Error
}

func (m *Monitor) publishChannelEconomicsHour(ctx context.Context, account ChannelUpstreamAccount, hourTs int64, reason string, now int64) error {
	if !m.channelCostEnabledFor(account) {
		return nil
	}
	if hourTs < 0 || hourTs%3600 != 0 {
		return errors.New("渠道经济账只允许整小时")
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// In production the current account row is the authority for an epoch.
		// A stale worker holding credentials from before an account update may
		// finish child work, but it must never replace the domain-hour manifest.
		// Tests and pre-account migrations may legitimately have no account row.
		if tx.Migrator().HasTable(&ChannelUpstreamAccount{}) {
			var authoritativeAccount ChannelUpstreamAccount
			if err := tx.Where("domain = ?", account.Domain).First(&authoritativeAccount).Error; err == nil {
				if newAPIUpstreamAccountEpoch(authoritativeAccount) != epoch {
					return errors.New("上游账户代际已变更，拒绝发布旧代际经济账")
				}
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		var costState ChannelUpstreamCostHourState
		if err := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, hourTs, channelCostEvidenceSemanticsVersion).First(&costState).Error; err != nil {
			return err
		}
		if costState.Status != "verified" || costState.ReconcileStatus != "matched" {
			return errors.New("渠道成本小时尚未核验对平")
		}
		var costRows []ChannelUpstreamCostHourEvidence
		if err := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, hourTs, channelCostEvidenceSemanticsVersion).Order("dimension_hash").Find(&costRows).Error; err != nil {
			return err
		}
		financeVersion, rechargePaid, rechargeCredit, financeKnown, err := economicsFinanceAt(tx, account.Domain, hourTs)
		if err != nil {
			return err
		}
		unitsPerUSD := costState.ChargeUnitsPerUSD
		if !validPositiveCanonicalRat(unitsPerUSD) {
			return errors.New("渠道成本小时缺少历史计费单位")
		}
		byChannel := map[int]*channelEconomicsAccumulator{}
		ensure := func(channelID int) *channelEconomicsAccumulator {
			row := byChannel[channelID]
			if row == nil {
				row = &channelEconomicsAccumulator{channelID: channelID}
				byChannel[channelID] = row
			}
			return row
		}
		for _, evidence := range costRows {
			var binding ChannelCostSourceBinding
			bindingErr := tx.Where("domain = ? AND account_epoch = ? AND source_ref = ? AND status = 'confirmed' AND valid_from <= ? AND (valid_to = 0 OR valid_to > ?)", account.Domain, epoch, evidence.SourceRef, hourTs, hourTs).
				Order("valid_from DESC").First(&binding).Error
			channelID := 0
			allocated := bindingErr == nil && binding.AllocationMode == "allocated" && binding.LocalChannelID > 0
			if bindingErr != nil && !errors.Is(bindingErr, gorm.ErrRecordNotFound) {
				return bindingErr
			}
			if allocated {
				channelID = binding.LocalChannelID
			}
			acc := ensure(channelID)
			if evidence.ChargeUnits > math.MaxInt64-acc.chargeUnits || evidence.Requests > math.MaxInt64-acc.upstreamRequests {
				return errors.New("渠道经济账上游聚合溢出")
			}
			acc.chargeUnits += evidence.ChargeUnits
			acc.upstreamRequests += evidence.Requests
			acc.costRows = append(acc.costRows, evidence)
			acc.hasCost = true
			acc.unallocated = acc.unallocated || !allocated
			if allocated {
				acc.mappingParts = append(acc.mappingParts, strings.Join([]string{evidence.SourceRef, strconv.FormatInt(binding.ValidFrom, 10), strconv.FormatInt(binding.ValidTo, 10), strconv.Itoa(binding.LocalChannelID), binding.AllocationMode}, ":"))
			} else {
				acc.mappingParts = append(acc.mappingParts, evidence.SourceRef+":unallocated")
			}
		}
		// Include every channel that was published by an earlier revision. When
		// a source is remapped (for example unallocated -> channel 59), the old
		// logical key must receive an explicit zero/superseded revision; otherwise
		// a current-pointer report would count both the old and new attribution.
		var previouslyPublished []int
		if err := tx.Model(&ChannelEconomicsHourPublication{}).
			Select("DISTINCT local_channel_id").
			Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, hourTs, channelEconomicsSemanticsVersion).
			Scan(&previouslyPublished).Error; err != nil {
			return err
		}
		for _, channelID := range previouslyPublished {
			ensure(channelID)
		}
		var localRows []struct {
			ChannelID     int
			Requests      int64
			RefundRecords int64
			ConsumeQuota  int64
			RefundQuota   int64
		}
		if err := tx.Raw(`SELECT s.channel_id,
			COALESCE(SUM(s.success+s.anomaly+s.failed),0) requests,
			COALESCE(SUM(s.refund_records),0) refund_records,
			COALESCE(SUM(s.quota),0) consume_quota,
			COALESCE(SUM(s.refund_quota),0) refund_quota
			FROM stability_hour_samples s
			JOIN channel_snaps c ON c.id=s.channel_id
			WHERE s.hour_ts=? AND s.traffic_class_version=? AND c.base_domain=?
			GROUP BY s.channel_id`, hourTs, userTrafficClassificationVersion, account.Domain).Scan(&localRows).Error; err != nil {
			return err
		}
		for _, local := range localRows {
			if local.Requests < 0 || local.RefundRecords < 0 || local.ConsumeQuota < 0 || local.RefundQuota < 0 {
				return errors.New("本地收入小时包含无效负数")
			}
			acc := ensure(local.ChannelID)
			acc.localRequests, acc.localRefunds = local.Requests, local.RefundRecords
			acc.localConsume, acc.localRefund, acc.hasLocal = local.ConsumeQuota, local.RefundQuota, true
		}
		unallocatedRefund, err := loadChannelEconomicsUnallocatedRefundTx(tx, hourTs)
		if err != nil {
			return err
		}
		// Cold-start/upgrade safety: a cost hour may become verified long after
		// the local stability hour was already present, so no local-fact trigger
		// will create the global row. Insert only when absent; late changes are
		// handled by the all-domain enqueue path before its watermark is updated.
		if err := insertChannelEconomicsGlobalFactIfMissingTx(tx, hourTs, now, unallocatedRefund); err != nil {
			return err
		}
		localFactStatus := "observed"
		var ingest StabilityHourIngestState
		if err := tx.First(&ingest, "hour_ts = ?", hourTs).Error; err == nil && ingest.Status == "complete" && ingest.TrafficClassVersion == userTrafficClassificationVersion {
			localFactStatus = "verified"
		} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		channelIDs := make([]int, 0, len(byChannel))
		for channelID := range byChannel {
			channelIDs = append(channelIDs, channelID)
		}
		sort.Ints(channelIDs)
		for _, channelID := range channelIDs {
			acc := byChannel[channelID]
			netQuota := acc.localConsume - acc.localRefund
			revenue, err := signedUnitsToMicroUSDCanonical(netQuota, new(big.Rat).SetInt64(quotaPerUSD).RatString())
			if err != nil {
				return err
			}
			upstreamCost, err := unitsToMicroUSDCanonical(acc.chargeUnits, unitsPerUSD)
			if err != nil {
				return err
			}
			corrected := int64(0)
			if financeKnown {
				corrected, err = correctedCostMicroUSD(upstreamCost, rechargePaid, rechargeCredit)
				if err != nil {
					return err
				}
			}
			var coverage string
			switch {
			case !financeKnown:
				coverage = "finance_version_missing"
			case unallocatedRefund.Records > 0 || unallocatedRefund.Quota > 0:
				coverage = "refund_unallocated"
			case !acc.hasCost && !acc.hasLocal:
				coverage = "superseded_empty"
			case channelID == 0 || acc.unallocated:
				coverage = "unallocated_cost"
			case !acc.hasCost:
				coverage = "upstream_cost_missing"
			case !acc.hasLocal:
				coverage = "local_revenue_missing"
			case localFactStatus != "verified":
				coverage = "local_fact_unverified"
			default:
				coverage = "verified_complete"
			}
			sort.Strings(acc.mappingParts)
			mappingDigest := sha256.Sum256([]byte(strings.Join(acc.mappingParts, "\n")))
			mappingHash := hex.EncodeToString(mappingDigest[:])
			sourceParts := []string{costState.ContentHash, costState.ChargeUnitsPerUSD, localFactStatus, strconv.FormatInt(acc.localRequests, 10), strconv.FormatInt(acc.localRefunds, 10), strconv.FormatInt(acc.localConsume, 10), strconv.FormatInt(acc.localRefund, 10), strconv.FormatInt(unallocatedRefund.Records, 10), strconv.FormatInt(unallocatedRefund.Quota, 10), strconv.FormatInt(acc.upstreamRequests, 10), strconv.FormatInt(acc.chargeUnits, 10), strconv.FormatInt(financeVersion, 10), strconv.FormatBool(financeKnown), strconv.FormatFloat(rechargePaid, 'g', -1, 64), strconv.FormatFloat(rechargeCredit, 'g', -1, 64), mappingHash, coverage}
			for _, costRow := range acc.costRows {
				sourceParts = append(sourceParts, costRow.ContentHash)
			}
			sourceDigest := sha256.Sum256([]byte(strings.Join(sourceParts, "\x00")))
			sourceHash := hex.EncodeToString(sourceDigest[:])
			logicalKey := economicsLogicalKey(account.Domain, epoch, hourTs, channelID)
			var current ChannelEconomicsHourCurrent
			currentErr := tx.First(&current, "logical_key = ?", logicalKey).Error
			if currentErr == nil {
				var previous ChannelEconomicsHourPublication
				if err := tx.First(&previous, "publication_id = ?", current.PublicationID).Error; err != nil {
					return err
				}
				if previous.SourceHash == sourceHash {
					continue
				}
			} else if !errors.Is(currentErr, gorm.ErrRecordNotFound) {
				return currentErr
			}
			revision := current.Revision + 1
			publicationID := economicsPublicationID(logicalKey, sourceHash, revision)
			profitKnown := coverage == "verified_complete"
			profit := int64(0)
			if profitKnown {
				profit, err = subtractNonnegativeInt64(revenue, corrected)
				if err != nil {
					return err
				}
			}
			publication := ChannelEconomicsHourPublication{
				PublicationID: publicationID, LogicalKey: logicalKey, Revision: revision, SupersedesPublicationID: current.PublicationID,
				Domain: account.Domain, AccountEpoch: epoch, HourTs: hourTs, LocalChannelID: channelID,
				SemanticsVersion: channelEconomicsSemanticsVersion, FinanceVersion: financeVersion,
				LocalRequests: acc.localRequests, UpstreamRequests: acc.upstreamRequests,
				LocalConsumeQuota: acc.localConsume, LocalRefundRecords: acc.localRefunds, LocalRefundQuota: acc.localRefund, LocalNetQuota: netQuota,
				UnallocatedRefundRecords: 0, UnallocatedRefundQuota: 0, LocalFactStatus: localFactStatus,
				RevenueMicroUSD: revenue, UpstreamChargeUnits: acc.chargeUnits, UpstreamChargeUnit: channelCostChargeUnitNewAPIQuota, ChargeUnitsPerUSD: unitsPerUSD,
				UpstreamCostMicroUSD: upstreamCost, CorrectedCostMicroUSD: corrected, ProfitMicroUSD: profit,
				CorrectedCostKnown: acc.hasCost && financeKnown, ProfitKnown: profitKnown,
				CoverageStatus: coverage, ReconcileStatus: costState.ReconcileStatus, MappingHash: mappingHash, SourceHash: sourceHash,
				PublicationReason: strings.TrimSpace(reason), PublishedAt: now,
			}
			if err := tx.Create(&publication).Error; err != nil {
				return err
			}
			pointer := ChannelEconomicsHourCurrent{LogicalKey: logicalKey, PublicationID: publicationID, Revision: revision, UpdatedAt: now}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "logical_key"}}, DoUpdates: clause.AssignmentColumns([]string{"publication_id", "revision", "updated_at"})}).Create(&pointer).Error; err != nil {
				return err
			}
		}
		// The manifest and every child current pointer commit in this same
		// transaction. RowCount=0 is intentional and proves a verified zero hour.
		return publishChannelEconomicsManifestTx(tx, account, epoch, costState, localFactStatus, unallocatedRefund, hourTs, reason, now)
	})
}

// enqueueMissingChannelEconomicsHours is the crash-recovery safety net for
// deployments upgraded from an older build and for the narrow window between
// verified-cost publication and queue creation in earlier versions. It scans a
// bounded newest-first slice and never performs upstream I/O.
func (m *Monitor) enqueueMissingChannelEconomicsHours(ctx context.Context, account ChannelUpstreamAccount, limit int) error {
	if !m.channelCostEnabledFor(account) {
		return nil
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 16 {
		limit = 16
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var hours []int64
	err := m.storeDB.WithContext(ctx).Raw(`
		SELECT c.hour_ts
		FROM channel_upstream_cost_hour_states c
		WHERE c.domain=? AND c.account_epoch=? AND c.semantics_version=?
		  AND c.status='verified' AND c.reconcile_status='matched'
		  AND NOT EXISTS (
		    SELECT 1
		    FROM channel_economics_hour_current cur
		    JOIN channel_economics_hour_publications p ON p.publication_id=cur.publication_id
		    WHERE p.domain=c.domain AND p.account_epoch=c.account_epoch AND p.hour_ts=c.hour_ts
		      AND p.semantics_version=?
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM channel_economics_dirty_hours d
		    WHERE d.domain=c.domain AND d.account_epoch=c.account_epoch AND d.hour_ts=c.hour_ts
		  )
		ORDER BY c.hour_ts DESC LIMIT ?`, account.Domain, epoch, channelCostEvidenceSemanticsVersion, channelEconomicsSemanticsVersion, limit).Scan(&hours).Error
	if err != nil {
		return err
	}
	for _, hourTs := range hours {
		if err := m.markChannelEconomicsDirtyHour(ctx, account, hourTs, "missing_publication", errors.New("已核验成本小时缺少经济账发布")); err != nil {
			return err
		}
	}
	return nil
}

func upsertChannelEconomicsDirtyTx(tx *gorm.DB, row ChannelEconomicsDirtyHour) error {
	if row.Generation <= 0 {
		row.Generation = 1
	}
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "domain"}, {Name: "account_epoch"}, {Name: "hour_ts"}},
		DoUpdates: clause.Assignments(map[string]any{
			"reason": row.Reason, "generation": gorm.Expr("generation + 1"), "status": "pending",
			"next_attempt_at": row.NextAttemptAt, "last_error": row.LastError, "updated_at": row.UpdatedAt,
		}),
	}).Create(&row).Error
}

// enqueueEconomicsForLocalFactTx couples an authoritative local-hour replace
// with economics invalidation in the same SQLite transaction. This is the
// online trigger for late NewAPI facts; the missing-publication scanner remains
// the crash/upgrade safety net.
func (m *Monitor) enqueueEconomicsForLocalFactTx(tx *gorm.DB, hourTs, now int64) error {
	if !m.cfg.ChannelCostClosureEnabled || hourTs < 0 || hourTs%3600 != 0 || len(m.cfg.ChannelCostClosureDomains) == 0 {
		return nil
	}
	var states []struct {
		Domain       string
		AccountEpoch string
	}
	if err := tx.Model(&ChannelUpstreamCostHourState{}).
		Select("domain, account_epoch").
		Where("hour_ts = ? AND semantics_version = ? AND status = 'verified' AND reconcile_status = 'matched' AND domain IN ?", hourTs, channelCostEvidenceSemanticsVersion, m.cfg.ChannelCostClosureDomains).
		Group("domain, account_epoch").Scan(&states).Error; err != nil {
		return err
	}
	for _, state := range states {
		dirty := ChannelEconomicsDirtyHour{Domain: state.Domain, AccountEpoch: state.AccountEpoch, HourTs: hourTs, Reason: "local_fact_changed", Generation: 1, Status: "pending", NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
		if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
			return err
		}
	}
	fact, err := loadChannelEconomicsUnallocatedRefundTx(tx, hourTs)
	if err != nil {
		return err
	}
	return upsertChannelEconomicsGlobalFactTx(tx, hourTs, now, fact)
}

// enqueueChangedEconomicsLocalHoursTx detects only material changes between
// the freshly rolled-up all-site billing columns and the currently published
// economics revision. This avoids a permanent five-minute recompute loop while
// still catching late consumption and refunds from the normal sampler path.
func (m *Monitor) enqueueChangedEconomicsLocalHoursTx(tx *gorm.DB, sinceTs, now int64, limit int) error {
	if !m.cfg.ChannelCostClosureEnabled || len(m.cfg.ChannelCostClosureDomains) == 0 {
		return nil
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 32 {
		limit = 32
	}
	type changedHour struct {
		Domain       string
		AccountEpoch string
		HourTs       int64
	}
	// Global unattributed refunds are one all-site fact. Queue every affected
	// domain before advancing its watermark; otherwise the first processed
	// domain could hide the change from domains beyond the bounded scan.
	var globalChangedHours []int64
	if err := tx.Raw(`WITH source AS (
		SELECT hour_ts, SUM(refund_records) records, SUM(refund_quota) quota
		FROM stability_hour_samples
		WHERE hour_ts>=? AND traffic_class_version=? AND channel_id<=0
		GROUP BY hour_ts
	), hours AS (
		SELECT hour_ts FROM source
		UNION
		SELECT hour_ts FROM channel_economics_global_hour_facts WHERE hour_ts>=? AND semantics_version=?
	)
	SELECT h.hour_ts FROM hours h
	LEFT JOIN source s ON s.hour_ts=h.hour_ts
	LEFT JOIN channel_economics_global_hour_facts g ON g.hour_ts=h.hour_ts AND g.semantics_version=?
	WHERE COALESCE(s.records,0)<>COALESCE(g.unallocated_refund_records,0)
	   OR COALESCE(s.quota,0)<>COALESCE(g.unallocated_refund_quota,0)
	ORDER BY h.hour_ts DESC LIMIT ?`, sinceTs, userTrafficClassificationVersion, sinceTs, channelEconomicsSemanticsVersion, channelEconomicsSemanticsVersion, limit).
		Scan(&globalChangedHours).Error; err != nil {
		return err
	}
	for _, hourTs := range globalChangedHours {
		var states []changedHour
		if err := tx.Model(&ChannelUpstreamCostHourState{}).
			Select("domain, account_epoch, hour_ts").
			Where("hour_ts = ? AND semantics_version = ? AND status = 'verified' AND reconcile_status = 'matched' AND domain IN ?", hourTs, channelCostEvidenceSemanticsVersion, m.cfg.ChannelCostClosureDomains).
			Group("domain, account_epoch, hour_ts").Scan(&states).Error; err != nil {
			return err
		}
		for _, hour := range states {
			dirty := ChannelEconomicsDirtyHour{Domain: hour.Domain, AccountEpoch: hour.AccountEpoch, HourTs: hour.HourTs, Reason: "global_refund_changed", Generation: 1, Status: "pending", NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
			if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
				return err
			}
		}
		fact, err := loadChannelEconomicsUnallocatedRefundTx(tx, hourTs)
		if err != nil {
			return err
		}
		if err := upsertChannelEconomicsGlobalFactTx(tx, hourTs, now, fact); err != nil {
			return err
		}
	}
	var changed []changedHour
	err := tx.Raw(`WITH local AS (
		SELECT cs.base_domain domain, sh.hour_ts, sh.channel_id,
		       SUM(sh.success+sh.anomaly+sh.failed) requests,
		       SUM(sh.refund_records) refund_records,
		       SUM(sh.quota) consume_quota, SUM(sh.refund_quota) refund_quota
		FROM stability_hour_samples sh
		JOIN channel_snaps cs ON cs.id=sh.channel_id
		WHERE sh.hour_ts>=? AND sh.traffic_class_version=? AND cs.base_domain IN ?
		GROUP BY cs.base_domain, sh.hour_ts, sh.channel_id
	), current_pub AS (
		SELECT p.* FROM channel_economics_hour_current cur
		JOIN channel_economics_hour_publications p ON p.publication_id=cur.publication_id
	)
	SELECT DISTINCT c.domain, c.account_epoch, c.hour_ts
	FROM channel_upstream_cost_hour_states c
	WHERE c.hour_ts>=? AND c.semantics_version=? AND c.status='verified' AND c.reconcile_status='matched'
	  AND c.domain IN ? AND (
	    EXISTS (SELECT 1 FROM local l
	      LEFT JOIN current_pub p ON p.domain=l.domain AND p.account_epoch=c.account_epoch
	        AND p.hour_ts=l.hour_ts AND p.local_channel_id=l.channel_id AND p.semantics_version=?
	      WHERE l.domain=c.domain AND l.hour_ts=c.hour_ts AND (
	        p.publication_id IS NULL OR p.local_requests<>l.requests OR
	        p.local_refund_records<>l.refund_records OR p.local_consume_quota<>l.consume_quota OR p.local_refund_quota<>l.refund_quota))
	    OR EXISTS (SELECT 1 FROM current_pub p
	      WHERE p.domain=c.domain AND p.account_epoch=c.account_epoch AND p.hour_ts=c.hour_ts
	        AND p.semantics_version=? AND p.local_channel_id>0
	        AND NOT EXISTS (SELECT 1 FROM local l WHERE l.domain=p.domain AND l.hour_ts=p.hour_ts AND l.channel_id=p.local_channel_id))
	  )
	ORDER BY c.hour_ts DESC LIMIT ?`,
		sinceTs, userTrafficClassificationVersion, m.cfg.ChannelCostClosureDomains,
		sinceTs, channelCostEvidenceSemanticsVersion, m.cfg.ChannelCostClosureDomains,
		channelEconomicsSemanticsVersion, channelEconomicsSemanticsVersion, limit).Scan(&changed).Error
	if err != nil {
		return err
	}
	for _, hour := range changed {
		dirty := ChannelEconomicsDirtyHour{Domain: hour.Domain, AccountEpoch: hour.AccountEpoch, HourTs: hour.HourTs, Reason: "local_rollup_changed", Generation: 1, Status: "pending", NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
		if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) markChannelEconomicsDirtyHour(ctx context.Context, account ChannelUpstreamAccount, hourTs int64, reason string, cause error) error {
	if !m.channelCostEnabledFor(account) || hourTs < 0 || hourTs%3600 != 0 {
		return nil
	}
	now := time.Now().Unix()
	if reason == "" || len(reason) > 32 {
		reason = "recompute"
	}
	message := ""
	if cause != nil {
		message = cause.Error()
		if len(message) > 512 {
			message = message[:512]
		}
	}
	row := ChannelEconomicsDirtyHour{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), HourTs: hourTs, Reason: reason, Generation: 1, Status: "pending", NextAttemptAt: now, LastError: message, CreatedAt: now, UpdatedAt: now}
	return upsertChannelEconomicsDirtyTx(m.storeDB.WithContext(ctx), row)
}

func (m *Monitor) publishOneDueChannelEconomicsHour(ctx context.Context, account ChannelUpstreamAccount, now int64) error {
	var dirty ChannelEconomicsDirtyHour
	err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND status = 'pending' AND next_attempt_at <= ?", account.Domain, newAPIUpstreamAccountEpoch(account), now).Order("hour_ts DESC, created_at ASC").First(&dirty).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := m.publishChannelEconomicsHour(ctx, account, dirty.HourTs, dirty.Reason, now); err != nil {
		attempts := dirty.Attempts + 1
		shift := attempts - 1
		if shift > 6 {
			shift = 6
		}
		next := now + int64(60*(1<<shift))
		message := err.Error()
		if len(message) > 512 {
			message = message[:512]
		}
		_ = m.storeDB.WithContext(ctx).Model(&ChannelEconomicsDirtyHour{}).
			Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND generation = ?", account.Domain, newAPIUpstreamAccountEpoch(account), dirty.HourTs, dirty.Generation).
			Updates(map[string]any{"attempts": attempts, "next_attempt_at": next, "last_error": message, "updated_at": now}).Error
		return err
	}
	// Compare-and-delete: if any producer increments generation while this
	// publication is running, the newer wake-up survives for another pass.
	return m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND generation = ?", account.Domain, newAPIUpstreamAccountEpoch(account), dirty.HourTs, dirty.Generation).Delete(&ChannelEconomicsDirtyHour{}).Error
}
