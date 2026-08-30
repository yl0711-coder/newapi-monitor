package monitor

// This file exposes the immutable hourly economics ledger as a bounded,
// read-only report. Page reads never query NewAPI or an upstream provider and
// never advance a worker cursor.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const maxChannelEconomicsReportRows = 500_000

type channelEconomicsMoneyView struct {
	MicroUSD string `json:"micro_usd"`
	Display  string `json:"display"`
}

type channelEconomicsTotalsView struct {
	Revenue                 *channelEconomicsMoneyView `json:"revenue"`
	KnownRevenue            channelEconomicsMoneyView  `json:"known_revenue"`
	RevenueKnown            bool                       `json:"revenue_known"`
	UpstreamCost            *channelEconomicsMoneyView `json:"upstream_cost"`
	KnownUpstreamCost       channelEconomicsMoneyView  `json:"known_upstream_cost"`
	UpstreamCostKnown       bool                       `json:"upstream_cost_known"`
	CorrectedCost           *channelEconomicsMoneyView `json:"corrected_cost"`
	KnownCorrectedCost      channelEconomicsMoneyView  `json:"known_corrected_cost"`
	Profit                  *channelEconomicsMoneyView `json:"profit"`
	KnownProfit             channelEconomicsMoneyView  `json:"known_profit"`
	MarginPercent           *string                    `json:"margin_percent"`
	MarginDisplay           string                     `json:"margin_display"`
	CorrectedCostKnown      bool                       `json:"corrected_cost_known"`
	ProfitKnown             bool                       `json:"profit_known"`
	Partial                 bool                       `json:"partial"`
	UnknownReason           string                     `json:"unknown_reason,omitempty"`
	LocalRequests           int64                      `json:"local_requests"`
	UpstreamRequests        int64                      `json:"upstream_requests"`
	LocalRefundRecords      int64                      `json:"local_refund_records"`
	UpstreamChargeUnits     int64                      `json:"upstream_charge_units"`
	IncludedPublicationRows int64                      `json:"included_publication_rows"`
}

type channelEconomicsCoverageView struct {
	ExpectedHours       int64            `json:"expected_hours"`
	PublishedHours      int64            `json:"published_hours"`
	VerifiedHours       int64            `json:"verified_hours"`
	MissingHours        int64            `json:"missing_hours"`
	UnknownHours        int64            `json:"unknown_hours"`
	AmbiguousEpochHours int64            `json:"ambiguous_epoch_hours"`
	StatusCounts        map[string]int64 `json:"status_counts"`
	Complete            bool             `json:"complete"`
	DataUntil           int64            `json:"data_until"`
}

type channelEconomicsChannelView struct {
	ChannelID int                          `json:"channel_id"`
	Name      string                       `json:"name"`
	Totals    channelEconomicsTotalsView   `json:"totals"`
	Coverage  channelEconomicsCoverageView `json:"coverage"`
}

type channelEconomicsHourView struct {
	HourTs   int64                        `json:"hour_ts"`
	Totals   channelEconomicsTotalsView   `json:"totals"`
	Coverage channelEconomicsCoverageView `json:"coverage"`
}

type channelEconomicsDomainView struct {
	Domain       string                        `json:"domain"`
	AccountEpoch string                        `json:"account_epoch,omitempty"`
	Totals       channelEconomicsTotalsView    `json:"totals"`
	Coverage     channelEconomicsCoverageView  `json:"coverage"`
	Channels     []channelEconomicsChannelView `json:"channels"`
	Hourly       []channelEconomicsHourView    `json:"hourly,omitempty"`
}

type channelEconomicsGlobalRefundView struct {
	Records int64                     `json:"records"`
	Quota   int64                     `json:"quota"`
	Amount  channelEconomicsMoneyView `json:"amount"`
	Hours   int64                     `json:"hours"`
}

type channelEconomicsReport struct {
	Enabled          bool                             `json:"enabled"`
	SemanticsVersion int                              `json:"semantics_version"`
	GeneratedAt      int64                            `json:"generated_at"`
	From             int64                            `json:"from"`
	To               int64                            `json:"to"`
	TimeZone         string                           `json:"time_zone"`
	Source           string                           `json:"source"`
	DomainFilter     string                           `json:"domain_filter,omitempty"`
	Totals           channelEconomicsTotalsView       `json:"totals"`
	Coverage         channelEconomicsCoverageView     `json:"coverage"`
	GlobalRefund     channelEconomicsGlobalRefundView `json:"global_unallocated_refund"`
	Domains          []channelEconomicsDomainView     `json:"domains"`
}

type channelEconomicsReportRow struct {
	PublicationID         string
	Domain                string
	AccountEpoch          string
	HourTs                int64
	LocalChannelID        int
	LocalRequests         int64
	UpstreamRequests      int64
	LocalRefundRecords    int64
	RevenueMicroUSD       int64
	UpstreamChargeUnits   int64
	UpstreamCostMicroUSD  int64
	CorrectedCostMicroUSD int64
	ProfitMicroUSD        int64
	CorrectedCostKnown    bool
	ProfitKnown           bool
	CoverageStatus        string
}

type channelEconomicsAgg struct {
	revenue             int64
	upstreamCost        int64
	knownCorrectedCost  int64
	knownProfit         int64
	localRequests       int64
	upstreamRequests    int64
	localRefundRecords  int64
	upstreamChargeUnits int64
	publicationRows     int64
	correctedKnown      bool
	profitKnown         bool
}

func addEconomicsInt64(dst *int64, value int64) error {
	if (value > 0 && *dst > math.MaxInt64-value) || (value < 0 && *dst < math.MinInt64-value) {
		return errors.New("渠道经济账汇总超出 int64 安全范围")
	}
	*dst += value
	return nil
}

func (a *channelEconomicsAgg) addRow(row channelEconomicsReportRow) error {
	for _, item := range []struct {
		dst   *int64
		value int64
	}{
		{&a.revenue, row.RevenueMicroUSD}, {&a.upstreamCost, row.UpstreamCostMicroUSD},
		{&a.localRequests, row.LocalRequests}, {&a.upstreamRequests, row.UpstreamRequests},
		{&a.localRefundRecords, row.LocalRefundRecords}, {&a.upstreamChargeUnits, row.UpstreamChargeUnits},
	} {
		if err := addEconomicsInt64(item.dst, item.value); err != nil {
			return err
		}
	}
	if row.CorrectedCostKnown {
		if err := addEconomicsInt64(&a.knownCorrectedCost, row.CorrectedCostMicroUSD); err != nil {
			return err
		}
	}
	if row.ProfitKnown {
		if err := addEconomicsInt64(&a.knownProfit, row.ProfitMicroUSD); err != nil {
			return err
		}
	}
	a.publicationRows++
	return nil
}

func economicsMoney(value int64) channelEconomicsMoneyView {
	sign := ""
	abs := value
	if value < 0 {
		sign = "-"
		if value == math.MinInt64 {
			// math.MinInt64 cannot be negated. Division/remainder still have safe
			// magnitudes, so format them independently below.
			whole := -(value / 1_000_000)
			fraction := -(value % 1_000_000)
			return channelEconomicsMoneyView{MicroUSD: strconv.FormatInt(value, 10), Display: fmt.Sprintf("-$%d.%06d", whole, fraction)}
		}
		abs = -value
	}
	return channelEconomicsMoneyView{
		MicroUSD: strconv.FormatInt(value, 10),
		Display:  fmt.Sprintf("%s$%d.%06d", sign, abs/1_000_000, abs%1_000_000),
	}
}

func economicsUnknownReason(coverage channelEconomicsCoverageView, globalRefund bool) string {
	if coverage.AmbiguousEpochHours > 0 {
		return "account_epoch_overlap"
	}
	if globalRefund {
		return "refund_unallocated"
	}
	if coverage.MissingHours > 0 {
		return "publication_missing"
	}
	if coverage.UnknownHours > 0 {
		return "coverage_incomplete"
	}
	return ""
}

func coverageHasStatus(coverage channelEconomicsCoverageView, statuses ...string) bool {
	for _, status := range statuses {
		if coverage.StatusCounts[status] > 0 {
			return true
		}
	}
	return false
}

func (a channelEconomicsAgg) view(coverage channelEconomicsCoverageView, profitBlocked, revenueBlocked bool) channelEconomicsTotalsView {
	view := channelEconomicsTotalsView{
		KnownRevenue: economicsMoney(a.revenue), KnownUpstreamCost: economicsMoney(a.upstreamCost),
		KnownCorrectedCost: economicsMoney(a.knownCorrectedCost), KnownProfit: economicsMoney(a.knownProfit),
		LocalRequests: a.localRequests, UpstreamRequests: a.upstreamRequests,
		LocalRefundRecords: a.localRefundRecords, UpstreamChargeUnits: a.upstreamChargeUnits,
		IncludedPublicationRows: a.publicationRows,
	}
	manifestBaseKnown := coverage.MissingHours == 0 && coverage.AmbiguousEpochHours == 0 &&
		!coverageHasStatus(coverage, "manifest_children_mismatch", "manifest_status_unknown")
	view.RevenueKnown = manifestBaseKnown && !revenueBlocked &&
		!coverageHasStatus(coverage, "local_fact_unverified", "local_revenue_missing")
	view.UpstreamCostKnown = manifestBaseKnown && !coverageHasStatus(coverage, "upstream_cost_missing")
	view.CorrectedCostKnown = a.correctedKnown && manifestBaseKnown &&
		!coverageHasStatus(coverage, "finance_version_conflict")
	view.ProfitKnown = a.profitKnown && view.RevenueKnown && view.CorrectedCostKnown && coverage.UnknownHours == 0 && !profitBlocked
	view.Partial = !view.ProfitKnown
	view.UnknownReason = economicsUnknownReason(coverage, profitBlocked)
	if view.RevenueKnown {
		money := economicsMoney(a.revenue)
		view.Revenue = &money
	}
	if view.UpstreamCostKnown {
		money := economicsMoney(a.upstreamCost)
		view.UpstreamCost = &money
	}
	if view.CorrectedCostKnown {
		money := economicsMoney(a.knownCorrectedCost)
		view.CorrectedCost = &money
	}
	if view.ProfitKnown {
		money := economicsMoney(a.knownProfit)
		view.Profit = &money
		if a.revenue > 0 {
			margin := float64(a.knownProfit) * 100 / float64(a.revenue)
			formatted := strconv.FormatFloat(margin, 'f', 2, 64)
			view.MarginPercent = &formatted
			view.MarginDisplay = formatted + "%"
		} else {
			view.MarginDisplay = "不可判定"
		}
	} else {
		view.MarginDisplay = "不可判定"
	}
	return view
}

func (m *Monitor) channelEconomicsDomains(ctx context.Context, requested string) ([]string, error) {
	requested = strings.ToLower(strings.TrimSpace(requested))
	var accounts []ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Order("domain").Find(&accounts).Error; err != nil {
		return nil, err
	}
	domains := make([]string, 0, len(accounts))
	for _, account := range accounts {
		domain := strings.ToLower(strings.TrimSpace(account.Domain))
		if domain == "" || !m.channelCostAPIAllowed(domain) {
			continue
		}
		if requested != "" && requested != domain {
			continue
		}
		domains = append(domains, domain)
	}
	domains = sortedUnique(domains)
	if requested != "" && len(domains) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	return domains, nil
}

func (m *Monitor) buildChannelEconomicsReport(ctx context.Context, scope stabilityScope, requestedDomain string) (*channelEconomicsReport, error) {
	now := time.Now().Unix()
	report := &channelEconomicsReport{
		Enabled: m.cfg.ChannelCostClosureEnabled && m.cfg.ChannelEconomicsReportEnabled, SemanticsVersion: channelEconomicsSemanticsVersion,
		GeneratedAt: now, From: scope.FromTs, To: scope.ToTs, TimeZone: "Asia/Shanghai",
		Source: "monitor_local_immutable_economics_current", DomainFilter: strings.ToLower(strings.TrimSpace(requestedDomain)),
	}
	if !report.Enabled {
		return report, nil
	}
	domains, err := m.channelEconomicsDomains(ctx, requestedDomain)
	if err != nil {
		return nil, err
	}
	report.Domains = make([]channelEconomicsDomainView, 0, len(domains))
	if len(domains) == 0 {
		return report, nil
	}
	var manifests []ChannelEconomicsHourManifestPublication
	manifestQuery := m.storeDB.WithContext(ctx).Table("channel_economics_hour_manifest_current mc").
		Select("mp.*").
		Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Where("mp.semantics_version = ? AND mp.hour_ts >= ? AND mp.hour_ts < ? AND mp.domain IN ?", channelEconomicsSemanticsVersion, scope.FromTs, scope.ToTs, domains).
		Order("mp.domain,mp.hour_ts")
	if err := manifestQuery.Find(&manifests).Error; err != nil {
		return nil, fmt.Errorf("读取经济账小时发布头: %w", err)
	}
	var rows []channelEconomicsReportRow
	query := m.storeDB.WithContext(ctx).Table("channel_economics_hour_manifest_current mc").
		Select(`p.publication_id,p.domain,p.account_epoch,p.hour_ts,p.local_channel_id,p.local_requests,p.upstream_requests,
			p.local_refund_records,p.revenue_micro_usd,p.upstream_charge_units,p.upstream_cost_micro_usd,
			p.corrected_cost_micro_usd,p.profit_micro_usd,p.corrected_cost_known,p.profit_known,p.coverage_status`).
		Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Joins("JOIN channel_economics_hour_publications p ON p.domain=mp.domain AND p.hour_ts=mp.hour_ts AND p.account_epoch=mp.authoritative_epoch AND p.semantics_version=mp.semantics_version").
		Joins("JOIN channel_economics_hour_current c ON c.publication_id=p.publication_id").
		Where("p.semantics_version = ? AND p.hour_ts >= ? AND p.hour_ts < ? AND p.domain IN ?", channelEconomicsSemanticsVersion, scope.FromTs, scope.ToTs, domains).
		Order("p.domain,p.hour_ts,p.account_epoch,p.local_channel_id").Limit(maxChannelEconomicsReportRows + 1)
	if err := query.Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("读取当前经济账版本: %w", err)
	}
	if len(rows) > maxChannelEconomicsReportRows {
		return nil, fmt.Errorf("经济账明细超过安全上限 %d，请缩小查询范围", maxChannelEconomicsReportRows)
	}

	var names []struct {
		ID   int
		Name string
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelSnap{}).Select("id,name").Find(&names).Error; err != nil {
		return nil, fmt.Errorf("读取渠道名称: %w", err)
	}
	channelNames := map[int]string{0: "未归属上游来源"}
	for _, row := range names {
		channelNames[row.ID] = strings.TrimSpace(row.Name)
	}

	expectedHours := (scope.ToTs - scope.FromTs) / 3600
	byDomain := make(map[string]map[int64][]channelEconomicsReportRow, len(domains))
	for _, row := range rows {
		if byDomain[row.Domain] == nil {
			byDomain[row.Domain] = map[int64][]channelEconomicsReportRow{}
		}
		byDomain[row.Domain][row.HourTs] = append(byDomain[row.Domain][row.HourTs], row)
	}
	manifestByDomain := make(map[string]map[int64]ChannelEconomicsHourManifestPublication, len(domains))
	for _, manifest := range manifests {
		if manifestByDomain[manifest.Domain] == nil {
			manifestByDomain[manifest.Domain] = map[int64]ChannelEconomicsHourManifestPublication{}
		}
		manifestByDomain[manifest.Domain][manifest.HourTs] = manifest
	}

	var globalFacts []ChannelEconomicsGlobalHourFact
	if err := m.storeDB.WithContext(ctx).Where("semantics_version = ? AND hour_ts >= ? AND hour_ts < ?", channelEconomicsSemanticsVersion, scope.FromTs, scope.ToTs).Find(&globalFacts).Error; err != nil {
		return nil, fmt.Errorf("读取全局未归属退款: %w", err)
	}
	globalRefundHours := map[int64]bool{}
	for _, fact := range globalFacts {
		if fact.UnallocatedRefundRecords == 0 && fact.UnallocatedRefundQuota == 0 {
			continue
		}
		if err := addEconomicsInt64(&report.GlobalRefund.Records, fact.UnallocatedRefundRecords); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&report.GlobalRefund.Quota, fact.UnallocatedRefundQuota); err != nil {
			return nil, err
		}
		globalRefundHours[fact.HourTs] = true
	}
	refundMicro, err := unitsToMicroUSDCanonical(report.GlobalRefund.Quota, strconv.FormatInt(quotaPerUSD, 10))
	if err != nil {
		return nil, err
	}
	report.GlobalRefund.Amount = economicsMoney(refundMicro)
	report.GlobalRefund.Hours = int64(len(globalRefundHours))

	var siteAgg channelEconomicsAgg
	siteAgg.correctedKnown, siteAgg.profitKnown = true, true
	report.Coverage = channelEconomicsCoverageView{ExpectedHours: expectedHours * int64(len(domains)), StatusCounts: map[string]int64{}}
	for _, domain := range domains {
		domainAgg := channelEconomicsAgg{correctedKnown: true, profitKnown: true}
		coverage := channelEconomicsCoverageView{ExpectedHours: expectedHours, StatusCounts: map[string]int64{}}
		channelAggs := map[int]*channelEconomicsAgg{}
		channelCoverages := map[int]*channelEconomicsCoverageView{}
		hourViews := make([]channelEconomicsHourView, 0)
		hours := manifestByDomain[domain]
		hourKeys := make([]int64, 0, len(hours))
		for hour := range hours {
			hourKeys = append(hourKeys, hour)
		}
		sort.Slice(hourKeys, func(i, j int) bool { return hourKeys[i] < hourKeys[j] })
		for _, hour := range hourKeys {
			manifest := hours[hour]
			coverage.PublishedHours++
			if hour+3600 > coverage.DataUntil {
				coverage.DataUntil = hour + 3600
			}
			epochRows := byDomain[domain][hour]
			hourAgg := channelEconomicsAgg{correctedKnown: true, profitKnown: true}
			hourCoverage := channelEconomicsCoverageView{ExpectedHours: 1, PublishedHours: 1, StatusCounts: map[string]int64{}, DataUntil: hour + 3600}
			hourVerified := manifest.CoverageStatus == "verified_complete" && manifest.ProfitKnown
			publicationIDs := make([]string, 0, len(epochRows))
			for _, row := range epochRows {
				publicationIDs = append(publicationIDs, row.PublicationID)
			}
			sort.Strings(publicationIDs)
			setDigest := sha256.Sum256([]byte(strings.Join(publicationIDs, "\n")))
			manifestChildrenValid := manifest.RowCount == int64(len(epochRows)) && manifest.PublicationSetHash == hex.EncodeToString(setDigest[:])
			if !manifestChildrenValid {
				hourVerified = false
				coverage.StatusCounts["manifest_children_mismatch"]++
				hourCoverage.StatusCounts["manifest_children_mismatch"]++
			} else {
				status := strings.TrimSpace(manifest.CoverageStatus)
				if status == "" {
					status = "manifest_status_unknown"
				}
				coverage.StatusCounts[status]++
				hourCoverage.StatusCounts[status]++
			}
			for _, row := range epochRows {
				status := strings.TrimSpace(row.CoverageStatus)
				if status == "" {
					status = "unknown"
				}
				neutral := status == "superseded_empty"
				if status != "verified_complete" && !neutral {
					hourVerified = false
				}
				if !row.CorrectedCostKnown && !neutral {
					domainAgg.correctedKnown, hourAgg.correctedKnown = false, false
				}
				if !row.ProfitKnown && !neutral {
					domainAgg.profitKnown, hourAgg.profitKnown = false, false
				}
				if err := domainAgg.addRow(row); err != nil {
					return nil, err
				}
				if err := hourAgg.addRow(row); err != nil {
					return nil, err
				}
				ch := channelAggs[row.LocalChannelID]
				if ch == nil {
					ch = &channelEconomicsAgg{correctedKnown: true, profitKnown: true}
					channelAggs[row.LocalChannelID] = ch
					channelCoverages[row.LocalChannelID] = &channelEconomicsCoverageView{StatusCounts: map[string]int64{}}
				}
				if !row.CorrectedCostKnown && !neutral {
					ch.correctedKnown = false
				}
				if !row.ProfitKnown && !neutral {
					ch.profitKnown = false
				}
				if err := ch.addRow(row); err != nil {
					return nil, err
				}
				cc := channelCoverages[row.LocalChannelID]
				cc.StatusCounts[status]++
				cc.PublishedHours++
				if status == "verified_complete" || neutral {
					cc.VerifiedHours++
				} else {
					cc.UnknownHours++
				}
				if hour+3600 > cc.DataUntil {
					cc.DataUntil = hour + 3600
				}
			}
			if globalRefundHours[hour] {
				hourVerified = false
				hourCoverage.StatusCounts["refund_unallocated"]++
			}
			if hourVerified {
				coverage.VerifiedHours++
				hourCoverage.VerifiedHours = 1
			} else {
				coverage.UnknownHours++
				hourCoverage.UnknownHours = 1
			}
			hourCoverage.Complete = hourVerified
			if report.DomainFilter != "" {
				hasRefund := globalRefundHours[hour]
				hourViews = append(hourViews, channelEconomicsHourView{HourTs: hour, Totals: hourAgg.view(hourCoverage, hasRefund, hasRefund), Coverage: hourCoverage})
			}
		}
		coverage.MissingHours = max(int64(0), coverage.ExpectedHours-coverage.PublishedHours)
		coverage.Complete = coverage.MissingHours == 0 && coverage.UnknownHours == 0 && coverage.AmbiguousEpochHours == 0 && report.GlobalRefund.Records == 0 && report.GlobalRefund.Quota == 0
		if coverage.MissingHours > 0 {
			coverage.StatusCounts["publication_missing"] += coverage.MissingHours
		}
		channels := make([]channelEconomicsChannelView, 0, len(channelAggs))
		channelIDs := make([]int, 0, len(channelAggs))
		for channelID := range channelAggs {
			channelIDs = append(channelIDs, channelID)
		}
		sort.Ints(channelIDs)
		for _, channelID := range channelIDs {
			cc := *channelCoverages[channelID]
			// A verified domain manifest proves that a channel absent from its child
			// set had zero traffic in that hour. A missing/invalid domain manifest,
			// however, cannot prove a per-channel zero, so channel exactness must
			// inherit the full domain-hour coverage and fail closed with the domain.
			cc.ExpectedHours = coverage.ExpectedHours
			cc.PublishedHours = coverage.PublishedHours
			cc.VerifiedHours = coverage.VerifiedHours
			cc.MissingHours = coverage.MissingHours
			cc.UnknownHours = coverage.UnknownHours
			cc.AmbiguousEpochHours = coverage.AmbiguousEpochHours
			cc.DataUntil = coverage.DataUntil
			cc.Complete = coverage.Complete
			// Manifest integrity is domain-hour authority. Propagate its status to
			// every child view; otherwise a corrupt child set could still leak an
			// apparently exact channel revenue/cost while only domain totals fail.
			for status, count := range coverage.StatusCounts {
				if cc.StatusCounts[status] < count {
					cc.StatusCounts[status] = count
				}
			}
			name := channelNames[channelID]
			if name == "" {
				name = fmt.Sprintf("渠道 #%d", channelID)
			}
			hasRefund := len(globalRefundHours) > 0
			channels = append(channels, channelEconomicsChannelView{ChannelID: channelID, Name: name, Totals: channelAggs[channelID].view(cc, hasRefund, hasRefund), Coverage: cc})
		}
		hasRefund := len(globalRefundHours) > 0
		domainView := channelEconomicsDomainView{Domain: domain, Totals: domainAgg.view(coverage, hasRefund, hasRefund), Coverage: coverage, Channels: channels, Hourly: hourViews}
		if len(hourKeys) > 0 {
			domainView.AccountEpoch = hours[hourKeys[len(hourKeys)-1]].AuthoritativeEpoch
		}
		report.Domains = append(report.Domains, domainView)
		for key, value := range coverage.StatusCounts {
			report.Coverage.StatusCounts[key] += value
		}
		report.Coverage.PublishedHours += coverage.PublishedHours
		report.Coverage.VerifiedHours += coverage.VerifiedHours
		report.Coverage.MissingHours += coverage.MissingHours
		report.Coverage.UnknownHours += coverage.UnknownHours
		report.Coverage.AmbiguousEpochHours += coverage.AmbiguousEpochHours
		if coverage.DataUntil > report.Coverage.DataUntil {
			report.Coverage.DataUntil = coverage.DataUntil
		}
		if err := addEconomicsInt64(&siteAgg.revenue, domainAgg.revenue); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.upstreamCost, domainAgg.upstreamCost); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.knownCorrectedCost, domainAgg.knownCorrectedCost); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.knownProfit, domainAgg.knownProfit); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.localRequests, domainAgg.localRequests); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.upstreamRequests, domainAgg.upstreamRequests); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.localRefundRecords, domainAgg.localRefundRecords); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&siteAgg.upstreamChargeUnits, domainAgg.upstreamChargeUnits); err != nil {
			return nil, err
		}
		siteAgg.publicationRows += domainAgg.publicationRows
		siteAgg.correctedKnown = siteAgg.correctedKnown && domainAgg.correctedKnown
		siteAgg.profitKnown = siteAgg.profitKnown && domainAgg.profitKnown
	}
	// Unattributed refunds are a single platform fact, not one fact per domain.
	// They reduce all-site revenue exactly once while making domain profit
	// allocation unknowable.
	if report.DomainFilter == "" {
		if err := addEconomicsInt64(&siteAgg.revenue, -refundMicro); err != nil {
			return nil, err
		}
	}
	report.Coverage.Complete = report.Coverage.MissingHours == 0 && report.Coverage.UnknownHours == 0 && report.Coverage.AmbiguousEpochHours == 0 && len(globalRefundHours) == 0
	hasRefund := len(globalRefundHours) > 0
	report.Totals = siteAgg.view(report.Coverage, hasRefund, hasRefund && report.DomainFilter != "")
	return report, nil
}

func (m *Monitor) serveChannelEconomicsReport(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	maxDays := m.cfg.stabilityQueryDays()
	scope, err := channelManagementRange(c, time.Now(), maxDays)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	requestedDomain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if len(requestedDomain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "domain 无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 6*time.Second)
	defer cancel()
	report, err := m.buildChannelEconomicsReport(ctx, scope, requestedDomain)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "域名未进入渠道成本灰度"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "精确成本报表暂不可用", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, report)
}
