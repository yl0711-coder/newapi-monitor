// Package financecur parses AWS CUR 2.0 Parquet exports into a bounded,
// read-only coverage audit. It deliberately does not decide which AWS resource
// belongs to NexusAPI: resource allocation is a separate, auditable ledger.
package financecur

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

const (
	nanoUSDPerUSD  = int64(1_000_000_000)
	defaultRows    = int64(5_000_000)
	defaultKeys    = 100_000
	defaultEntries = 500_000
)

// Limits keep a corrupt or unexpectedly broad export from exhausting the
// Monitor host. Zero values select conservative defaults.
type Limits struct {
	MaxRows        int64
	MaxResources   int
	MaxServices    int
	MaxHours       int
	MaxCostEntries int
}

type CostBucket struct {
	Rows    int64 `json:"rows"`
	NanoUSD int64 `json:"nano_usd"`
}

type ServiceBucket struct {
	ProductCode string `json:"product_code"`
	ServiceCode string `json:"service_code"`
	CostBucket
}

type ResourceBucket struct {
	ProductCode string `json:"product_code"`
	ResourceID  string `json:"resource_id"`
	CostBucket
}

// CostEntry is the smallest source unit accepted by the allocation engine.
// Rows with identical time and AWS dimensions are combined, but ownership is
// deliberately left unresolved.
type CostEntry struct {
	FromUnix     int64  `json:"from_unix"`
	ToUnix       int64  `json:"to_unix"`
	LineItemType string `json:"line_item_type"`
	ProductCode  string `json:"product_code"`
	ServiceCode  string `json:"service_code"`
	UsageType    string `json:"usage_type"`
	Operation    string `json:"operation"`
	Description  string `json:"description"`
	ResourceID   string `json:"resource_id"`
	Rows         int64  `json:"rows"`
	NanoUSD      int64  `json:"nano_usd"`
}

// Audit is source evidence only. TotalNanoUSD is the CUR net-unblended value
// with unblended fallback; it must not become a NexusAPI cost until every row
// is assigned or explicitly excluded by the resource-allocation ledger.
type Audit struct {
	Rows                 int64                 `json:"rows"`
	Currency             string                `json:"currency"`
	BillingPeriodStart   int64                 `json:"billing_period_start_unix"`
	BillingPeriodEnd     int64                 `json:"billing_period_end_unix"`
	FirstUsageUnix       int64                 `json:"first_usage_unix"`
	LastUsageThroughUnix int64                 `json:"last_usage_through_unix"`
	UsageHours           int                   `json:"usage_hours"`
	TotalNanoUSD         int64                 `json:"total_nano_usd"`
	LineItemTypes        map[string]CostBucket `json:"line_item_types"`
	Services             []ServiceBucket       `json:"services"`
	Resources            []ResourceBucket      `json:"resources"`
	CostEntries          []CostEntry           `json:"cost_entries"`
}

type curRow struct {
	LineItemID       string           `parquet:"identity_line_item_id"`
	BillingStart     deprecated.Int96 `parquet:"bill_billing_period_start_date"`
	BillingEnd       deprecated.Int96 `parquet:"bill_billing_period_end_date"`
	UsageStart       deprecated.Int96 `parquet:"line_item_usage_start_date"`
	UsageEnd         deprecated.Int96 `parquet:"line_item_usage_end_date"`
	LineItemType     string           `parquet:"line_item_line_item_type"`
	ProductCode      string           `parquet:"line_item_product_code"`
	UsageType        *string          `parquet:"line_item_usage_type,optional"`
	Operation        *string          `parquet:"line_item_operation,optional"`
	Description      *string          `parquet:"line_item_line_item_description,optional"`
	ServiceCode      *string          `parquet:"product_servicecode,optional"`
	ResourceID       *string          `parquet:"line_item_resource_id,optional"`
	CurrencyCode     string           `parquet:"line_item_currency_code"`
	UnblendedCost    float64          `parquet:"line_item_unblended_cost"`
	NetUnblendedCost *float64         `parquet:"line_item_net_unblended_cost,optional"`
}

type serviceKey struct{ product, service string }
type resourceKey struct{ product, resource string }
type costEntryKey struct {
	from, to                                                                int64
	lineType, product, service, usageType, operation, description, resource string
}

func normalizeLimits(limits Limits) Limits {
	if limits.MaxRows <= 0 {
		limits.MaxRows = defaultRows
	}
	if limits.MaxResources <= 0 {
		limits.MaxResources = defaultKeys
	}
	if limits.MaxServices <= 0 {
		limits.MaxServices = 1_000
	}
	if limits.MaxHours <= 0 {
		limits.MaxHours = 24 * 400
	}
	if limits.MaxCostEntries <= 0 {
		limits.MaxCostEntries = defaultEntries
	}
	return limits
}

// ReadParquet scans a CUR object with bounded memory. It reads only the small
// set of columns needed for coverage and allocation review.
func ReadParquet(input io.ReaderAt, limits Limits) (Audit, error) {
	if input == nil {
		return Audit{}, errors.New("CUR Parquet reader is nil")
	}
	limits = normalizeLimits(limits)
	reader := parquet.NewGenericReader[curRow](input)
	defer reader.Close()

	audit := Audit{LineItemTypes: map[string]CostBucket{}}
	services := map[serviceKey]CostBucket{}
	resources := map[resourceKey]CostBucket{}
	entries := map[costEntryKey]CostBucket{}
	hours := map[int64]struct{}{}
	buffer := make([]curRow, 512)
	for {
		n, readErr := reader.Read(buffer)
		for i := 0; i < n; i++ {
			if err := addRow(&audit, services, resources, entries, hours, buffer[i], limits); err != nil {
				return Audit{}, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return Audit{}, fmt.Errorf("read CUR Parquet: %w", readErr)
		}
	}
	audit.UsageHours = len(hours)
	for key, bucket := range services {
		audit.Services = append(audit.Services, ServiceBucket{ProductCode: key.product, ServiceCode: key.service, CostBucket: bucket})
	}
	for key, bucket := range resources {
		audit.Resources = append(audit.Resources, ResourceBucket{ProductCode: key.product, ResourceID: key.resource, CostBucket: bucket})
	}
	for key, bucket := range entries {
		audit.CostEntries = append(audit.CostEntries, CostEntry{
			FromUnix: key.from, ToUnix: key.to, LineItemType: key.lineType,
			ProductCode: key.product, ServiceCode: key.service, UsageType: key.usageType,
			Operation: key.operation, Description: key.description, ResourceID: key.resource,
			Rows: bucket.Rows, NanoUSD: bucket.NanoUSD,
		})
	}
	sort.Slice(audit.Services, func(i, j int) bool {
		if audit.Services[i].NanoUSD != audit.Services[j].NanoUSD {
			return audit.Services[i].NanoUSD > audit.Services[j].NanoUSD
		}
		if audit.Services[i].ProductCode != audit.Services[j].ProductCode {
			return audit.Services[i].ProductCode < audit.Services[j].ProductCode
		}
		return audit.Services[i].ServiceCode < audit.Services[j].ServiceCode
	})
	sort.Slice(audit.Resources, func(i, j int) bool {
		if audit.Resources[i].NanoUSD != audit.Resources[j].NanoUSD {
			return audit.Resources[i].NanoUSD > audit.Resources[j].NanoUSD
		}
		if audit.Resources[i].ProductCode != audit.Resources[j].ProductCode {
			return audit.Resources[i].ProductCode < audit.Resources[j].ProductCode
		}
		return audit.Resources[i].ResourceID < audit.Resources[j].ResourceID
	})
	sort.Slice(audit.CostEntries, func(i, j int) bool {
		left, right := audit.CostEntries[i], audit.CostEntries[j]
		if left.FromUnix != right.FromUnix {
			return left.FromUnix < right.FromUnix
		}
		if left.ToUnix != right.ToUnix {
			return left.ToUnix < right.ToUnix
		}
		if left.ProductCode != right.ProductCode {
			return left.ProductCode < right.ProductCode
		}
		if left.ServiceCode != right.ServiceCode {
			return left.ServiceCode < right.ServiceCode
		}
		if left.ResourceID != right.ResourceID {
			return left.ResourceID < right.ResourceID
		}
		if left.UsageType != right.UsageType {
			return left.UsageType < right.UsageType
		}
		if left.Operation != right.Operation {
			return left.Operation < right.Operation
		}
		if left.Description != right.Description {
			return left.Description < right.Description
		}
		return left.LineItemType < right.LineItemType
	})
	return audit, nil
}

func addRow(audit *Audit, services map[serviceKey]CostBucket, resources map[resourceKey]CostBucket, entries map[costEntryKey]CostBucket, hours map[int64]struct{}, row curRow, limits Limits) error {
	audit.Rows++
	if audit.Rows > limits.MaxRows {
		return fmt.Errorf("CUR row count exceeds safety limit %d", limits.MaxRows)
	}
	billingStart, billingStartErr := int96ToUnix(row.BillingStart)
	billingEnd, billingEndErr := int96ToUnix(row.BillingEnd)
	if billingStartErr != nil || billingEndErr != nil || billingStart <= 0 || billingEnd <= billingStart {
		return fmt.Errorf("CUR line %q has invalid billing period", row.LineItemID)
	}
	if audit.BillingPeriodStart == 0 {
		audit.BillingPeriodStart, audit.BillingPeriodEnd = billingStart, billingEnd
	} else if audit.BillingPeriodStart != billingStart || audit.BillingPeriodEnd != billingEnd {
		return fmt.Errorf("CUR contains mixed billing periods")
	}
	currency := strings.ToUpper(strings.TrimSpace(row.CurrencyCode))
	if currency != "" {
		if audit.Currency == "" {
			audit.Currency = currency
		} else if audit.Currency != currency {
			return fmt.Errorf("CUR contains mixed currencies %s and %s", audit.Currency, currency)
		}
	}
	cost := row.UnblendedCost
	if row.NetUnblendedCost != nil {
		cost = *row.NetUnblendedCost
	}
	nano, err := costNanoUSD(cost)
	if err != nil {
		return fmt.Errorf("CUR line %q: %w", row.LineItemID, err)
	}
	if nano != 0 && currency == "" {
		return fmt.Errorf("CUR line %q has non-zero cost without a currency", row.LineItemID)
	}
	if err := addInt64(&audit.TotalNanoUSD, nano); err != nil {
		return err
	}
	lineType := strings.TrimSpace(row.LineItemType)
	if lineType == "" {
		lineType = "unknown"
	}
	bucket := audit.LineItemTypes[lineType]
	bucket.Rows++
	if err := addInt64(&bucket.NanoUSD, nano); err != nil {
		return err
	}
	audit.LineItemTypes[lineType] = bucket

	product := strings.TrimSpace(row.ProductCode)
	service := ""
	if row.ServiceCode != nil {
		service = strings.TrimSpace(*row.ServiceCode)
	}
	usageType := ""
	if row.UsageType != nil {
		usageType = strings.TrimSpace(*row.UsageType)
	}
	operation := ""
	if row.Operation != nil {
		operation = strings.TrimSpace(*row.Operation)
	}
	description := ""
	if row.Description != nil {
		description = strings.TrimSpace(*row.Description)
	}
	serviceBucket := services[serviceKey{product, service}]
	serviceBucket.Rows++
	if err := addInt64(&serviceBucket.NanoUSD, nano); err != nil {
		return err
	}
	services[serviceKey{product, service}] = serviceBucket
	if len(services) > limits.MaxServices {
		return fmt.Errorf("CUR service dimension exceeds safety limit %d", limits.MaxServices)
	}

	resource := ""
	if row.ResourceID != nil {
		resource = strings.TrimSpace(*row.ResourceID)
	}
	resourceBucket := resources[resourceKey{product, resource}]
	resourceBucket.Rows++
	if err := addInt64(&resourceBucket.NanoUSD, nano); err != nil {
		return err
	}
	resources[resourceKey{product, resource}] = resourceBucket
	if len(resources) > limits.MaxResources {
		return fmt.Errorf("CUR resource dimension exceeds safety limit %d", limits.MaxResources)
	}

	start, startErr := int96ToUnix(row.UsageStart)
	through, throughErr := int96ToUnix(row.UsageEnd)
	if startErr != nil || throughErr != nil || start <= 0 || through <= start {
		return fmt.Errorf("CUR line %q has invalid cost interval", row.LineItemID)
	}
	entryKey := costEntryKey{from: start, to: through, lineType: lineType, product: product, service: service, usageType: usageType, operation: operation, description: description, resource: resource}
	entry := entries[entryKey]
	entry.Rows++
	if err := addInt64(&entry.NanoUSD, nano); err != nil {
		return err
	}
	entries[entryKey] = entry
	if len(entries) > limits.MaxCostEntries {
		return fmt.Errorf("CUR cost-entry dimension exceeds safety limit %d", limits.MaxCostEntries)
	}

	if isUsageLine(lineType) {
		for hour := start / 3600 * 3600; hour < through; hour += 3600 {
			hours[hour] = struct{}{}
			if len(hours) > limits.MaxHours {
				return fmt.Errorf("CUR hour coverage exceeds safety limit %d", limits.MaxHours)
			}
			if hour > math.MaxInt64-3600 {
				return errors.New("CUR hour coverage exceeds integer safety range")
			}
		}
		if audit.FirstUsageUnix == 0 || start < audit.FirstUsageUnix {
			audit.FirstUsageUnix = start
		}
		if through > audit.LastUsageThroughUnix {
			audit.LastUsageThroughUnix = through
		}
	}
	return nil
}

func isUsageLine(lineType string) bool {
	switch lineType {
	case "Usage", "DiscountedUsage", "SavingsPlanCoveredUsage":
		return true
	default:
		return false
	}
}

func int96ToUnix(value deprecated.Int96) (int64, error) {
	const unixEpochJulianDay = int64(2_440_588)
	nanosOfDay := uint64(value[0]) | uint64(value[1])<<32
	if nanosOfDay >= uint64(24*time.Hour) {
		return 0, errors.New("INT96 nanoseconds-of-day out of range")
	}
	julianDay := int64(value[2])
	days := julianDay - unixEpochJulianDay
	if days > math.MaxInt64/86400 || days < math.MinInt64/86400 {
		return 0, errors.New("INT96 Julian day out of range")
	}
	return days*86400 + int64(nanosOfDay/uint64(time.Second)), nil
}

func costNanoUSD(value float64) (int64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value) > float64(math.MaxInt64)/float64(nanoUSDPerUSD) {
		return 0, errors.New("invalid or out-of-range USD cost")
	}
	return int64(math.Round(value * float64(nanoUSDPerUSD))), nil
}

func addInt64(target *int64, value int64) error {
	if value > 0 && *target > math.MaxInt64-value || value < 0 && *target < math.MinInt64-value {
		return errors.New("CUR cost exceeds integer safety range")
	}
	*target += value
	return nil
}
