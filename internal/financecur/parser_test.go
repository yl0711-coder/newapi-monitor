package financecur

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

func TestReadParquetBuildsBoundedCoverageWithoutAllocatingResources(t *testing.T) {
	service := "AmazonECS"
	resource := "arn:aws:ecs:us-west-2:123:task/nexusapi-prod/one"
	net := 1.25
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := start.AddDate(0, 1, 0)
	rows := []curRow{
		{LineItemID: "usage-1", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(periodEnd), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(time.Hour)), LineItemType: "Usage", ProductCode: "AmazonECS", UsageType: stringPointer("USE1-Fargate-vCPU-Hours:perCPU"), Operation: stringPointer("FargateTask"), Description: stringPointer("Fargate vCPU usage"), ServiceCode: &service, ResourceID: &resource, CurrencyCode: "USD", UnblendedCost: 1.5, NetUnblendedCost: &net},
		{LineItemID: "credit-1", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(periodEnd), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(periodEnd), LineItemType: "Credit", ProductCode: "AmazonECS", CurrencyCode: "USD", UnblendedCost: -0.25},
	}
	var encoded bytes.Buffer
	writer := parquet.NewGenericWriter[curRow](&encoded)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	audit, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if audit.Rows != 2 || audit.Currency != "USD" || audit.BillingPeriodStart != start.Unix() || audit.BillingPeriodEnd != periodEnd.Unix() || audit.UsageHours != 1 || audit.FirstUsageUnix != start.Unix() || audit.LastUsageThroughUnix != start.Add(time.Hour).Unix() {
		t.Fatalf("coverage mismatch: %+v", audit)
	}
	if audit.TotalNanoUSD != 1_000_000_000 || audit.LineItemTypes["Usage"].NanoUSD != 1_250_000_000 || audit.LineItemTypes["Credit"].NanoUSD != -250_000_000 {
		t.Fatalf("cost mismatch: %+v", audit.LineItemTypes)
	}
	if len(audit.Services) != 2 || len(audit.Resources) != 2 {
		t.Fatalf("dimensions mismatch services=%d resources=%d", len(audit.Services), len(audit.Resources))
	}
	if len(audit.CostEntries) != 2 {
		t.Fatalf("cost entries=%d want 2", len(audit.CostEntries))
	}
	var usageEntry CostEntry
	for _, entry := range audit.CostEntries {
		if entry.LineItemType == "Usage" {
			usageEntry = entry
		}
	}
	if usageEntry.UsageType != "USE1-Fargate-vCPU-Hours:perCPU" || usageEntry.Operation != "FargateTask" || usageEntry.Description != "Fargate vCPU usage" {
		t.Fatalf("usage dimensions missing: %+v", usageEntry)
	}
}

func TestReadParquetEnforcesCostEntryLimit(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := start.AddDate(0, 1, 0)
	rows := []curRow{
		{LineItemID: "one", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(periodEnd), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(time.Hour)), LineItemType: "Usage", ProductCode: "one", CurrencyCode: "USD"},
		{LineItemID: "two", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(periodEnd), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(time.Hour)), LineItemType: "Usage", ProductCode: "two", CurrencyCode: "USD"},
	}
	var encoded bytes.Buffer
	writer := parquet.NewGenericWriter[curRow](&encoded)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{MaxCostEntries: 1}); err == nil {
		t.Fatal("cost-entry limit must fail closed")
	}
}

func TestReadParquetRejectsMixedCurrencyAndLimits(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := start.AddDate(0, 1, 0)
	rows := []curRow{
		{LineItemID: "one", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(periodEnd), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(time.Hour)), LineItemType: "Usage", CurrencyCode: "USD"},
		{LineItemID: "two", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(periodEnd), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(time.Hour)), LineItemType: "Usage", CurrencyCode: "CNY"},
	}
	var encoded bytes.Buffer
	writer := parquet.NewGenericWriter[curRow](&encoded)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{MaxRows: 1}); err == nil {
		t.Fatal("row limit must fail closed")
	}
	if _, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{MaxRows: 10}); err == nil {
		t.Fatal("mixed currencies must fail closed")
	}
}

func TestReadParquetRejectsMixedBillingPeriods(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rows := []curRow{
		{LineItemID: "one", BillingStart: unixToInt96(start), BillingEnd: unixToInt96(start.AddDate(0, 1, 0)), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(time.Hour)), LineItemType: "Usage", CurrencyCode: "USD"},
		{LineItemID: "two", BillingStart: unixToInt96(start.AddDate(0, 1, 0)), BillingEnd: unixToInt96(start.AddDate(0, 2, 0)), UsageStart: unixToInt96(start.AddDate(0, 1, 0)), UsageEnd: unixToInt96(start.AddDate(0, 1, 0).Add(time.Hour)), LineItemType: "Usage", CurrencyCode: "USD"},
	}
	var encoded bytes.Buffer
	writer := parquet.NewGenericWriter[curRow](&encoded)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{}); err == nil {
		t.Fatal("mixed billing periods must fail closed")
	}
}

func TestReadParquetExpandsMultiHourUsageAndRejectsMissingCurrency(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 30, 0, 0, time.UTC)
	periodStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rows := []curRow{{
		LineItemID: "wide", BillingStart: unixToInt96(periodStart), BillingEnd: unixToInt96(periodStart.AddDate(0, 1, 0)), UsageStart: unixToInt96(start), UsageEnd: unixToInt96(start.Add(2 * time.Hour)),
		LineItemType: "Usage", CurrencyCode: "USD",
	}}
	var encoded bytes.Buffer
	writer := parquet.NewGenericWriter[curRow](&encoded)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	audit, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if audit.UsageHours != 3 {
		t.Fatalf("multi-hour coverage=%d want 3", audit.UsageHours)
	}

	rows[0].CurrencyCode = ""
	rows[0].UnblendedCost = 1
	encoded.Reset()
	writer = parquet.NewGenericWriter[curRow](&encoded)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadParquet(bytes.NewReader(encoded.Bytes()), Limits{}); err == nil {
		t.Fatal("non-zero cost without currency must fail closed")
	}
}

func unixToInt96(value time.Time) deprecated.Int96 {
	value = value.UTC()
	const unixEpochJulianDay = int64(2_440_588)
	seconds := value.Unix()
	days := seconds / 86400
	secondsOfDay := seconds % 86400
	if secondsOfDay < 0 {
		days--
		secondsOfDay += 86400
	}
	nanos := uint64(secondsOfDay)*uint64(time.Second) + uint64(value.Nanosecond())
	return deprecated.Int96{uint32(nanos), uint32(nanos >> 32), uint32(days + unixEpochJulianDay)}
}

func stringPointer(value string) *string { return &value }
