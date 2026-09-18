package financecur

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const TimelineSchemaVersion = 2

const (
	recognizeSpread  = "spread"
	recognizeAtStart = "at_start"
)

// TimeBucket is an accounting projection in one explicit timezone. Source
// costs always reconcile to the four allocation components. Complete means
// every non-zero source amount in this bucket matched exactly one rule.
type TimeBucket struct {
	Key                   string `json:"key"`
	FromUnix              int64  `json:"from_unix"`
	ToUnix                int64  `json:"to_unix"`
	Segments              int64  `json:"segments"`
	SourceNanoUSD         int64  `json:"source_nano_usd"`
	NexusAPINanoUSD       int64  `json:"nexusapi_nano_usd"`
	ExcludedNanoUSD       int64  `json:"excluded_nano_usd"`
	UnallocatedNanoUSD    int64  `json:"unallocated_nano_usd"`
	ConflictNanoUSD       int64  `json:"conflict_nano_usd"`
	TotalAbsNanoUSD       uint64 `json:"total_abs_nano_usd"`
	AllocatedAbsNanoUSD   uint64 `json:"allocated_abs_nano_usd"`
	UnallocatedAbsNanoUSD uint64 `json:"unallocated_abs_nano_usd"`
	ConflictAbsNanoUSD    uint64 `json:"conflict_abs_nano_usd"`
	Complete              bool   `json:"complete"`
}

type Timeline struct {
	SchemaVersion int               `json:"schema_version"`
	TimeZone      string            `json:"time_zone"`
	Days          []TimeBucket      `json:"days"`
	Months        []TimeBucket      `json:"months"`
	Products      []ProductTimeline `json:"products"`
}

// ProductTimeline keeps only the non-sensitive AWS CUR product dimension.
// Raw resource IDs, usage descriptions and account identifiers never cross
// into the Monitor artifact.
type ProductTimeline struct {
	ProductCode string       `json:"product_code"`
	Days        []TimeBucket `json:"days"`
	Months      []TimeBucket `json:"months"`
}

type allocationComponents struct {
	source, nexus, excluded, unallocated, conflict int64
}

// Periodize converts the exact entry decisions from Allocate into Beijing (or
// another explicit IANA timezone) day and month buckets. Usage and usage-like
// adjustments are spread by elapsed seconds; one-time fee/tax items are
// recognized on their source start date. Unknown non-zero line-item types fail
// closed so a new AWS billing semantic cannot silently change the report.
func Periodize(report AllocationReport, timeZone string) (Timeline, error) {
	location, err := time.LoadLocation(timeZone)
	if err != nil {
		return Timeline{}, fmt.Errorf("load CUR accounting timezone: %w", err)
	}
	expected := report.Entries - report.ZeroCostEntries
	if expected < 0 || int64(len(report.EntryAllocations)) != expected {
		return Timeline{}, errors.New("CUR entry allocation evidence is incomplete")
	}
	days := make(map[string]*TimeBucket)
	productDays := make(map[string]map[string]*TimeBucket)
	for _, decision := range report.EntryAllocations {
		if err := validateEntryAllocation(decision); err != nil {
			return Timeline{}, err
		}
		method, err := recognitionMethod(decision.Entry.LineItemType)
		if err != nil {
			return Timeline{}, err
		}
		components := allocationComponents{
			source: decision.Entry.NanoUSD, nexus: decision.NexusAPINanoUSD,
			excluded: decision.ExcludedNanoUSD, unallocated: decision.UnallocatedNanoUSD,
			conflict: decision.ConflictNanoUSD,
		}
		productCode := strings.TrimSpace(decision.Entry.ProductCode)
		if productDays[productCode] == nil {
			productDays[productCode] = make(map[string]*TimeBucket)
		}
		if method == recognizeAtStart {
			if err := addTimeBucket(days, decision.Entry.FromUnix, components, location); err != nil {
				return Timeline{}, err
			}
			if err := addTimeBucket(productDays[productCode], decision.Entry.FromUnix, components, location); err != nil {
				return Timeline{}, err
			}
			continue
		}
		if err := spreadEntry(days, decision.Entry, components, location); err != nil {
			return Timeline{}, err
		}
		if err := spreadEntry(productDays[productCode], decision.Entry, components, location); err != nil {
			return Timeline{}, err
		}
	}
	timeline := Timeline{SchemaVersion: TimelineSchemaVersion, TimeZone: location.String()}
	timeline.Days, err = finalizeDayBuckets(days)
	if err != nil {
		return Timeline{}, err
	}
	timeline.Months, err = aggregateMonths(timeline.Days, location)
	if err != nil {
		return Timeline{}, err
	}
	productCodes := make([]string, 0, len(productDays))
	for productCode := range productDays {
		productCodes = append(productCodes, productCode)
	}
	sort.Strings(productCodes)
	for _, productCode := range productCodes {
		product := ProductTimeline{ProductCode: productCode}
		product.Days, err = finalizeDayBuckets(productDays[productCode])
		if err != nil {
			return Timeline{}, err
		}
		product.Months, err = aggregateMonths(product.Days, location)
		if err != nil {
			return Timeline{}, err
		}
		timeline.Products = append(timeline.Products, product)
	}
	if err := validateTimelineTotals(timeline, report); err != nil {
		return Timeline{}, err
	}
	return timeline, nil
}

func TimelineHash(timeline Timeline) (string, error) {
	if err := validateTimelineShape(timeline); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(timeline)
	if err != nil {
		return "", fmt.Errorf("encode CUR timeline: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func finalizeDayBuckets(days map[string]*TimeBucket) ([]TimeBucket, error) {
	result := make([]TimeBucket, 0, len(days))
	for _, bucket := range days {
		if err := validateTimeBucket(*bucket); err != nil {
			return nil, err
		}
		bucket.Complete = bucket.AllocatedAbsNanoUSD == bucket.TotalAbsNanoUSD && bucket.UnallocatedAbsNanoUSD == 0 && bucket.ConflictAbsNanoUSD == 0
		result = append(result, *bucket)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].FromUnix < result[j].FromUnix })
	return result, nil
}

func recognitionMethod(lineItemType string) (string, error) {
	switch lineItemType {
	case "Usage", "DiscountedUsage", "SavingsPlanCoveredUsage", "Credit", "Refund", "BundledDiscount", "EdpDiscount", "SavingsPlanNegation":
		return recognizeSpread, nil
	case "Fee", "Tax", "RIFee", "SavingsPlanRecurringFee", "SavingsPlanUpfrontFee":
		return recognizeAtStart, nil
	default:
		return "", fmt.Errorf("unsupported non-zero CUR line item type %q", lineItemType)
	}
}

func validateEntryAllocation(decision EntryAllocation) error {
	if err := validateCostEntry(decision.Entry); err != nil || decision.Entry.NanoUSD == 0 {
		return errors.New("invalid CUR entry allocation")
	}
	total, err := sumNanoUSD(decision.NexusAPINanoUSD, decision.ExcludedNanoUSD, decision.UnallocatedNanoUSD, decision.ConflictNanoUSD)
	if err != nil || total != decision.Entry.NanoUSD {
		return errors.New("CUR entry allocation does not reconcile")
	}
	switch decision.Status {
	case "allocated":
		if len(decision.RuleIDs) != 1 || decision.UnallocatedNanoUSD != 0 || decision.ConflictNanoUSD != 0 {
			return errors.New("invalid allocated CUR entry decision")
		}
	case "unallocated", "partial_effective_period":
		if decision.UnallocatedNanoUSD != decision.Entry.NanoUSD || decision.NexusAPINanoUSD != 0 || decision.ExcludedNanoUSD != 0 || decision.ConflictNanoUSD != 0 {
			return errors.New("invalid unallocated CUR entry decision")
		}
	case "conflict":
		if len(decision.RuleIDs) < 2 || decision.ConflictNanoUSD != decision.Entry.NanoUSD || decision.NexusAPINanoUSD != 0 || decision.ExcludedNanoUSD != 0 || decision.UnallocatedNanoUSD != 0 {
			return errors.New("invalid conflicting CUR entry decision")
		}
	default:
		return fmt.Errorf("unsupported CUR entry allocation status %q", decision.Status)
	}
	return nil
}

func spreadEntry(days map[string]*TimeBucket, entry CostEntry, total allocationComponents, location *time.Location) error {
	duration := entry.ToUnix - entry.FromUnix
	if duration <= 0 {
		return errors.New("invalid CUR spread duration")
	}
	remaining := total
	for cursor := entry.FromUnix; cursor < entry.ToUnix; {
		local := time.Unix(cursor, 0).In(location)
		nextMidnight := time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, location).Unix()
		through := min(entry.ToUnix, nextMidnight)
		seconds := through - cursor
		part := remaining
		if through < entry.ToUnix {
			part = proportionalComponents(total, seconds, duration)
			remaining = subtractComponents(remaining, part)
		}
		if err := addTimeBucket(days, cursor, part, location); err != nil {
			return err
		}
		cursor = through
	}
	return nil
}

func proportionalComponents(total allocationComponents, numerator, denominator int64) allocationComponents {
	result := allocationComponents{
		source:      proportionalInt64(total.source, numerator, denominator),
		nexus:       proportionalInt64(total.nexus, numerator, denominator),
		unallocated: proportionalInt64(total.unallocated, numerator, denominator),
		conflict:    proportionalInt64(total.conflict, numerator, denominator),
	}
	result.excluded = result.source - result.nexus - result.unallocated - result.conflict
	return result
}

func proportionalInt64(value, numerator, denominator int64) int64 {
	// Cost entries and interval durations are already bounded. Split the
	// quotient/remainder separately to avoid value*numerator overflow.
	quotient, remainder := value/denominator, value%denominator
	return quotient*numerator + remainder*numerator/denominator
}

func subtractComponents(left, right allocationComponents) allocationComponents {
	return allocationComponents{
		source: left.source - right.source, nexus: left.nexus - right.nexus,
		excluded: left.excluded - right.excluded, unallocated: left.unallocated - right.unallocated,
		conflict: left.conflict - right.conflict,
	}
}

func addTimeBucket(days map[string]*TimeBucket, unix int64, amount allocationComponents, location *time.Location) error {
	local := time.Unix(unix, 0).In(location)
	dayStart := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	key := dayStart.Format("2006-01-02")
	bucket := days[key]
	if bucket == nil {
		bucket = &TimeBucket{Key: key, FromUnix: dayStart.Unix(), ToUnix: dayStart.AddDate(0, 0, 1).Unix()}
		days[key] = bucket
	}
	bucket.Segments++
	if err := addInt64(&bucket.SourceNanoUSD, amount.source); err != nil {
		return err
	}
	if err := addInt64(&bucket.NexusAPINanoUSD, amount.nexus); err != nil {
		return err
	}
	if err := addInt64(&bucket.ExcludedNanoUSD, amount.excluded); err != nil {
		return err
	}
	if err := addInt64(&bucket.UnallocatedNanoUSD, amount.unallocated); err != nil {
		return err
	}
	if err := addInt64(&bucket.ConflictNanoUSD, amount.conflict); err != nil {
		return err
	}
	for target, value := range map[*uint64]int64{
		&bucket.TotalAbsNanoUSD:       amount.source,
		&bucket.UnallocatedAbsNanoUSD: amount.unallocated,
		&bucket.ConflictAbsNanoUSD:    amount.conflict,
	} {
		absolute, err := absNanoUSD(value)
		if err != nil || *target > ^uint64(0)-absolute {
			return errors.New("CUR timeline absolute cost exceeds integer safety range")
		}
		*target += absolute
	}
	if amount.unallocated == 0 && amount.conflict == 0 {
		absolute, err := absNanoUSD(amount.source)
		if err != nil || bucket.AllocatedAbsNanoUSD > ^uint64(0)-absolute {
			return errors.New("CUR timeline allocated cost exceeds integer safety range")
		}
		bucket.AllocatedAbsNanoUSD += absolute
	}
	return nil
}

func aggregateMonths(days []TimeBucket, location *time.Location) ([]TimeBucket, error) {
	months := make(map[string]*TimeBucket)
	for _, day := range days {
		local := time.Unix(day.FromUnix, 0).In(location)
		monthStart := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location)
		key := monthStart.Format("2006-01")
		bucket := months[key]
		if bucket == nil {
			bucket = &TimeBucket{Key: key, FromUnix: monthStart.Unix(), ToUnix: monthStart.AddDate(0, 1, 0).Unix()}
			months[key] = bucket
		}
		bucket.Segments += day.Segments
		for target, value := range map[*int64]int64{
			&bucket.SourceNanoUSD: day.SourceNanoUSD, &bucket.NexusAPINanoUSD: day.NexusAPINanoUSD,
			&bucket.ExcludedNanoUSD: day.ExcludedNanoUSD, &bucket.UnallocatedNanoUSD: day.UnallocatedNanoUSD,
			&bucket.ConflictNanoUSD: day.ConflictNanoUSD,
		} {
			if err := addInt64(target, value); err != nil {
				return nil, err
			}
		}
		for target, value := range map[*uint64]uint64{
			&bucket.TotalAbsNanoUSD: day.TotalAbsNanoUSD, &bucket.AllocatedAbsNanoUSD: day.AllocatedAbsNanoUSD,
			&bucket.UnallocatedAbsNanoUSD: day.UnallocatedAbsNanoUSD, &bucket.ConflictAbsNanoUSD: day.ConflictAbsNanoUSD,
		} {
			if *target > ^uint64(0)-value {
				return nil, errors.New("CUR monthly timeline exceeds integer safety range")
			}
			*target += value
		}
	}
	result := make([]TimeBucket, 0, len(months))
	for _, bucket := range months {
		if err := validateTimeBucket(*bucket); err != nil {
			return nil, err
		}
		bucket.Complete = bucket.AllocatedAbsNanoUSD == bucket.TotalAbsNanoUSD && bucket.UnallocatedAbsNanoUSD == 0 && bucket.ConflictAbsNanoUSD == 0
		result = append(result, *bucket)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].FromUnix < result[j].FromUnix })
	return result, nil
}

func validateTimeBucket(bucket TimeBucket) error {
	components, err := sumNanoUSD(bucket.NexusAPINanoUSD, bucket.ExcludedNanoUSD, bucket.UnallocatedNanoUSD, bucket.ConflictNanoUSD)
	if err != nil || components != bucket.SourceNanoUSD {
		return fmt.Errorf("CUR time bucket %q does not reconcile", bucket.Key)
	}
	if bucket.FromUnix >= bucket.ToUnix || bucket.Segments <= 0 || bucket.AllocatedAbsNanoUSD > bucket.TotalAbsNanoUSD {
		return fmt.Errorf("CUR time bucket %q has invalid coverage", bucket.Key)
	}
	return nil
}

func validateTimelineTotals(timeline Timeline, report AllocationReport) error {
	totals, err := sumTimeBuckets(timeline.Days)
	if err != nil {
		return err
	}
	if totals.source != report.TotalNanoUSD || totals.nexus != report.NexusAPINanoUSD || totals.excluded != report.ExcludedNanoUSD ||
		totals.unallocated != report.UnallocatedNanoUSD || totals.conflict != report.ConflictNanoUSD ||
		totals.totalAbs != report.TotalAbsNanoUSD || totals.allocatedAbs != report.AllocatedAbsNanoUSD {
		return errors.New("CUR timeline does not reconcile to allocation report")
	}
	return nil
}

type timeBucketTotals struct {
	source, nexus, excluded, unallocated, conflict      int64
	totalAbs, allocatedAbs, unallocatedAbs, conflictAbs uint64
}

func sumTimeBuckets(buckets []TimeBucket) (timeBucketTotals, error) {
	var total timeBucketTotals
	for _, bucket := range buckets {
		for target, value := range map[*int64]int64{
			&total.source: bucket.SourceNanoUSD, &total.nexus: bucket.NexusAPINanoUSD,
			&total.excluded: bucket.ExcludedNanoUSD, &total.unallocated: bucket.UnallocatedNanoUSD,
			&total.conflict: bucket.ConflictNanoUSD,
		} {
			if err := addInt64(target, value); err != nil {
				return timeBucketTotals{}, err
			}
		}
		if total.totalAbs > ^uint64(0)-bucket.TotalAbsNanoUSD || total.allocatedAbs > ^uint64(0)-bucket.AllocatedAbsNanoUSD ||
			total.unallocatedAbs > ^uint64(0)-bucket.UnallocatedAbsNanoUSD || total.conflictAbs > ^uint64(0)-bucket.ConflictAbsNanoUSD {
			return timeBucketTotals{}, errors.New("CUR timeline totals exceed integer safety range")
		}
		total.totalAbs += bucket.TotalAbsNanoUSD
		total.allocatedAbs += bucket.AllocatedAbsNanoUSD
		total.unallocatedAbs += bucket.UnallocatedAbsNanoUSD
		total.conflictAbs += bucket.ConflictAbsNanoUSD
	}
	return total, nil
}

func validateTimelineShape(timeline Timeline) error {
	if timeline.SchemaVersion != TimelineSchemaVersion || strings.TrimSpace(timeline.TimeZone) == "" || len(timeline.Days) == 0 || len(timeline.Months) == 0 || len(timeline.Products) == 0 {
		return errors.New("invalid CUR timeline")
	}
	globalDays, err := validateBucketSeries(timeline.Days)
	if err != nil {
		return err
	}
	globalMonths, err := validateBucketSeries(timeline.Months)
	if err != nil || globalMonths != globalDays {
		return errors.New("CUR monthly timeline does not reconcile")
	}
	var productTotal timeBucketTotals
	previousProduct := ""
	for index, product := range timeline.Products {
		if index > 0 && product.ProductCode <= previousProduct {
			return errors.New("CUR product timelines are not uniquely sorted")
		}
		previousProduct = product.ProductCode
		days, productErr := validateBucketSeries(product.Days)
		if productErr != nil {
			return productErr
		}
		months, productErr := validateBucketSeries(product.Months)
		if productErr != nil || months != days {
			return fmt.Errorf("CUR product timeline %q does not reconcile", product.ProductCode)
		}
		if err := addTimeBucketTotals(&productTotal, days); err != nil {
			return err
		}
	}
	if productTotal != globalDays {
		return errors.New("CUR product timelines do not reconcile to global timeline")
	}
	return nil
}

func validateBucketSeries(buckets []TimeBucket) (timeBucketTotals, error) {
	if len(buckets) == 0 {
		return timeBucketTotals{}, errors.New("CUR timeline bucket series is empty")
	}
	var previousTo int64
	for index, bucket := range buckets {
		if err := validateTimeBucket(bucket); err != nil {
			return timeBucketTotals{}, err
		}
		complete := bucket.AllocatedAbsNanoUSD == bucket.TotalAbsNanoUSD && bucket.UnallocatedAbsNanoUSD == 0 && bucket.ConflictAbsNanoUSD == 0
		if bucket.Complete != complete || (index > 0 && bucket.FromUnix < previousTo) {
			return timeBucketTotals{}, fmt.Errorf("CUR time bucket %q has invalid ordering or coverage state", bucket.Key)
		}
		previousTo = bucket.ToUnix
	}
	return sumTimeBuckets(buckets)
}

func addTimeBucketTotals(total *timeBucketTotals, value timeBucketTotals) error {
	for target, amount := range map[*int64]int64{
		&total.source: value.source, &total.nexus: value.nexus, &total.excluded: value.excluded,
		&total.unallocated: value.unallocated, &total.conflict: value.conflict,
	} {
		if err := addInt64(target, amount); err != nil {
			return err
		}
	}
	if total.totalAbs > ^uint64(0)-value.totalAbs || total.allocatedAbs > ^uint64(0)-value.allocatedAbs ||
		total.unallocatedAbs > ^uint64(0)-value.unallocatedAbs || total.conflictAbs > ^uint64(0)-value.conflictAbs {
		return errors.New("CUR product timeline totals exceed integer safety range")
	}
	total.totalAbs += value.totalAbs
	total.allocatedAbs += value.allocatedAbs
	total.unallocatedAbs += value.unallocatedAbs
	total.conflictAbs += value.conflictAbs
	return nil
}
