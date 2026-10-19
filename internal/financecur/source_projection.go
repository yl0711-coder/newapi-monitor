package financecur

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// SourceProjector is a validated in-memory index, detached from its caller's
// artifacts. It does not reread files or change source monetary evidence.
type SourceProjector struct {
	review   SourceSetReview
	location *time.Location
	days     []TimeBucket
	products []ProductTimeline
}

type ProjectedPeriod struct {
	Bucket        TimeBucket `json:"bucket"`
	RangeComplete bool       `json:"range_complete"`
}

type SourceProjection struct {
	SourceSetID string `json:"source_set_id"`
	FromUnix    int64  `json:"from_unix"`
	ToUnix      int64  `json:"to_unix"`
	Currency    string `json:"currency"`
	TimeZone    string `json:"time_zone"`
	// Known.Complete describes allocation coverage only. Consumers must use
	// RangeComplete to decide whether the requested cost may be published.
	Known                  TimeBucket        `json:"known"`
	RangeComplete          bool              `json:"range_complete"`
	MissingBillingPeriods  []SourceInterval  `json:"missing_billing_periods"`
	UnverifiedStatementIDs []string          `json:"unverified_statement_ids"`
	PartialBoundaryDays    []SourceInterval  `json:"partial_boundary_days"`
	Days                   []ProjectedPeriod `json:"days"`
	Months                 []ProjectedPeriod `json:"months"`
	Products               []ProductTimeline `json:"products"`
}

func NewSourceProjector(set SourceSet) (*SourceProjector, error) {
	review, err := ReviewSourceSet(set)
	if err != nil {
		return nil, err
	}
	location, err := time.LoadLocation(review.TimeZone)
	if err != nil {
		return nil, err
	}
	allDays := make(map[int64]TimeBucket)
	productDays := make(map[string]map[int64]TimeBucket)
	for _, a := range set.Artifacts {
		if err := validateSourceDailyEvidence(a, location); err != nil {
			return nil, err
		}
		for _, day := range a.Timeline.Days {
			if err := mergeSourceDay(allDays, day); err != nil {
				return nil, err
			}
		}
		for _, product := range a.Timeline.Products {
			if productDays[product.ProductCode] == nil {
				productDays[product.ProductCode] = make(map[int64]TimeBucket)
			}
			for _, day := range product.Days {
				if err := mergeSourceDay(productDays[product.ProductCode], day); err != nil {
					return nil, err
				}
			}
		}
	}
	p := &SourceProjector{review: review, location: location, days: orderedSourceDays(allDays)}
	for code, days := range productDays {
		p.products = append(p.products, ProductTimeline{ProductCode: code, Days: orderedSourceDays(days)})
	}
	sort.Slice(p.products, func(i, j int) bool { return p.products[i].ProductCode < p.products[j].ProductCode })
	return p, nil
}

// Query sums already-accounted day amounts using integers. It never prorates
// a partial day, redistributes credits, or treats a missing month as zero.
func (p *SourceProjector) Query(from, to int64) (SourceProjection, error) {
	if p == nil || p.location == nil || from <= 0 || from >= to {
		return SourceProjection{}, errors.New("invalid CUR projection range or source")
	}
	r := SourceProjection{SourceSetID: p.review.SourceSetID, FromUnix: from, ToUnix: to, Currency: p.review.Currency, TimeZone: p.review.TimeZone,
		UnverifiedStatementIDs: []string{}, PartialBoundaryDays: []SourceInterval{}, Days: []ProjectedPeriod{}, Months: []ProjectedPeriod{}, Products: []ProductTimeline{}}
	var err error
	r.MissingBillingPeriods, err = p.review.MissingPeriods(from, to)
	if err != nil {
		return SourceProjection{}, err
	}
	for _, s := range p.review.Statements {
		if s.BillingPeriodEnd <= from || s.BillingPeriodStart >= to {
			continue
		}
		if s.Status != StatementStatusPublishable || max(from, s.BillingPeriodStart) < s.FirstUsageUnix || min(to, s.BillingPeriodEnd) > s.LastUsageThroughUnix {
			r.UnverifiedStatementIDs = append(r.UnverifiedStatementIDs, s.StatementID)
		}
	}
	days := []TimeBucket{}
	for _, day := range p.days {
		if day.ToUnix <= from || day.FromUnix >= to {
			continue
		}
		if day.FromUnix < from || day.ToUnix > to {
			r.PartialBoundaryDays = append(r.PartialBoundaryDays, SourceInterval{day.FromUnix, day.ToUnix})
			continue
		}
		days = append(days, day)
		r.Days = append(r.Days, ProjectedPeriod{day, p.complete(day.FromUnix, day.ToUnix, []TimeBucket{day})})
	}
	r.Known, err = sourceBucketTotal(days, "selected", from, to)
	if err != nil {
		return SourceProjection{}, err
	}
	r.RangeComplete = p.complete(from, to, days)
	months, err := aggregateMonths(days, p.location)
	if err != nil {
		return SourceProjection{}, err
	}
	for _, month := range months {
		start, end := max(from, month.FromUnix), min(to, month.ToUnix)
		selected := []TimeBucket{}
		for _, day := range days {
			if day.FromUnix >= start && day.ToUnix <= end {
				selected = append(selected, day)
			}
		}
		// A selected partial month is labelled with its actual requested range.
		month.FromUnix, month.ToUnix = start, end
		r.Months = append(r.Months, ProjectedPeriod{month, p.complete(start, end, selected)})
	}
	for _, product := range p.products {
		selected := []TimeBucket{}
		for _, day := range product.Days {
			if day.FromUnix >= from && day.ToUnix <= to {
				selected = append(selected, day)
			}
		}
		if len(selected) == 0 {
			continue
		}
		monthly, err := aggregateMonths(selected, p.location)
		if err != nil {
			return SourceProjection{}, err
		}
		for i := range monthly {
			monthly[i].FromUnix = max(from, monthly[i].FromUnix)
			monthly[i].ToUnix = min(to, monthly[i].ToUnix)
		}
		r.Products = append(r.Products, ProductTimeline{ProductCode: product.ProductCode, Days: selected, Months: monthly})
	}
	return r, nil
}

func (p *SourceProjector) complete(from, to int64, days []TimeBucket) bool {
	missing, err := p.review.MissingPeriods(from, to)
	if err != nil || len(missing) > 0 || len(days) == 0 {
		return false
	}
	for _, s := range p.review.Statements {
		if s.BillingPeriodEnd <= from || s.BillingPeriodStart >= to {
			continue
		}
		if s.Status != StatementStatusPublishable || max(from, s.BillingPeriodStart) < s.FirstUsageUnix || min(to, s.BillingPeriodEnd) > s.LastUsageThroughUnix {
			return false
		}
	}
	cursor := from
	for _, day := range days {
		if !day.Complete || day.FromUnix != cursor {
			return false
		}
		cursor = day.ToUnix
	}
	return cursor == to
}

func sourceBucketTotal(days []TimeBucket, key string, from, to int64) (TimeBucket, error) {
	t, err := sumTimeBuckets(days)
	if err != nil {
		return TimeBucket{}, err
	}
	b := TimeBucket{Key: key, FromUnix: from, ToUnix: to, SourceNanoUSD: t.source, NexusAPINanoUSD: t.nexus, ExcludedNanoUSD: t.excluded, UnallocatedNanoUSD: t.unallocated, ConflictNanoUSD: t.conflict,
		TotalAbsNanoUSD: t.totalAbs, AllocatedAbsNanoUSD: t.allocatedAbs, UnallocatedAbsNanoUSD: t.unallocatedAbs, ConflictAbsNanoUSD: t.conflictAbs}
	for _, d := range days {
		if err := addInt64(&b.Segments, d.Segments); err != nil {
			return TimeBucket{}, err
		}
	}
	b.Complete = len(days) > 0 && b.AllocatedAbsNanoUSD == b.TotalAbsNanoUSD && b.UnallocatedAbsNanoUSD == 0 && b.ConflictAbsNanoUSD == 0
	return b, nil
}

func mergeSourceDay(days map[int64]TimeBucket, day TimeBucket) error {
	previous, ok := days[day.FromUnix]
	if !ok {
		days[day.FromUnix] = day
		return nil
	}
	if previous.Key != day.Key || previous.ToUnix != day.ToUnix {
		return errors.New("inconsistent CUR natural day boundaries")
	}
	combined, err := sourceBucketTotal([]TimeBucket{previous, day}, day.Key, day.FromUnix, day.ToUnix)
	if err != nil {
		return err
	}
	days[day.FromUnix] = combined
	return nil
}

func orderedSourceDays(days map[int64]TimeBucket) []TimeBucket {
	result := make([]TimeBucket, 0, len(days))
	for _, day := range days {
		result = append(result, day)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].FromUnix < result[j].FromUnix })
	return result
}

// Totals alone are insufficient: product amounts must match the SAME day.
// Require canonical natural-day buckets before merging overlapping labels
// from adjacent (nonoverlapping) UTC billing periods.
func validateSourceDailyEvidence(a Artifact, location *time.Location) error {
	global := make(map[int64]TimeBucket)
	products := make(map[int64]TimeBucket)
	check := func(day TimeBucket) error {
		d := time.Unix(day.FromUnix, 0).In(location)
		start := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, location)
		if day.Key != start.Format("2006-01-02") || day.FromUnix != start.Unix() || day.ToUnix != start.AddDate(0, 0, 1).Unix() {
			return errors.New("noncanonical CUR daily bucket")
		}
		if day.ToUnix <= a.Statement.FirstUsageUnix || day.FromUnix >= a.Statement.LastUsageThroughUnix {
			return errors.New("CUR daily bucket outside source evidence range")
		}
		return nil
	}
	for _, day := range a.Timeline.Days {
		if err := check(day); err != nil {
			return err
		}
		global[day.FromUnix] = day
	}
	for _, product := range a.Timeline.Products {
		for _, day := range product.Days {
			if err := check(day); err != nil {
				return err
			}
			if err := mergeSourceDay(products, day); err != nil {
				return err
			}
		}
	}
	if len(global) != len(products) {
		return errors.New("CUR product daily coverage differs from global coverage")
	}
	for ts, day := range global {
		product, ok := products[ts]
		if !ok {
			return errors.New("CUR product daily coverage missing")
		}
		aTotals, err := sumTimeBuckets([]TimeBucket{day})
		if err != nil {
			return err
		}
		bTotals, err := sumTimeBuckets([]TimeBucket{product})
		if err != nil {
			return err
		}
		if aTotals != bTotals {
			return fmt.Errorf("CUR product amounts differ for day %s", day.Key)
		}
	}
	return nil
}
