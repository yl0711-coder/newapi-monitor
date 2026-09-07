package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const infraPresenceMetric = "inventory_present"
const maxLightsailInventoryPages = 100
const maxLightsailKnownResources = 1000

// Inventory reconciliation needs names only, not a fresh all-metric MAX query
// or its large temporary aggregation. On a failed/budgeted local read we omit
// absence markers, rather than retire resources using a truncated inventory.
func (m *Monitor) knownLightsailResources(ctx context.Context) []infraLatestRow {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var rows []infraLatestRow
	err := m.storeDB.WithContext(ctx).Raw(`SELECT DISTINCT resource, rtype FROM infra_samples
		WHERE rtype IN ('instance','database','lb')
		AND resource NOT LIKE 'rds/%' AND resource NOT LIKE 'alb/%' AND resource NOT LIKE 'ecs/%'
		LIMIT ?`, maxLightsailKnownResources+1).Scan(&rows).Error
	if err != nil || len(rows) > maxLightsailKnownResources {
		slog.Warn("infra: 历史资源名读取未完成，本轮不确认退役", "err", err, "rows", len(rows))
		return nil
	}
	return rows
}

// Never publish a partial inventory as authoritative absence. A failed page,
// repeated token or exceeded budget leaves the previous membership unchanged.
func collectLightsailPages[T any](ctx context.Context, fetch func(*string) ([]T, *string, error)) ([]T, error) {
	var rows []T
	var token *string
	seen := map[string]bool{}
	for page := 0; page < maxLightsailInventoryPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, next, err := fetch(token)
		if err != nil {
			return nil, err
		}
		rows = append(rows, items...)
		if next == nil || *next == "" {
			return rows, nil
		}
		if seen[*next] {
			return nil, fmt.Errorf("Lightsail inventory pagination repeated a token")
		}
		seen[*next] = true
		token = next
	}
	return nil, fmt.Errorf("Lightsail inventory exceeded %d pages", maxLightsailInventoryPages)
}

// Presence is a control-plane fact, not telemetry. Keep historic metrics and
// host reports intact; only remove confirmed absent resources from current
// health/alerts. A failed discovery of one resource type cannot retire it.
func lightsailPresenceRows(bucket int64, discovered []infraTarget, previous []infraLatestRow, complete map[string]bool) []InfraSample {
	present := map[string]bool{}
	known := map[string]string{}
	for _, target := range discovered {
		present[target.name] = true
		known[target.name] = target.rtype
	}
	for _, row := range previous {
		if (row.RType == "instance" || row.RType == "database" || row.RType == "lb") && infraPlatform(row.Resource, row.RType) == "Lightsail" {
			known[row.Resource] = row.RType
		}
	}
	rows := make([]InfraSample, 0, len(known))
	for name, rtype := range known {
		if !complete[rtype] {
			continue
		}
		value := float64(0)
		if present[name] {
			value = 1
		}
		rows = append(rows, InfraSample{BucketTs: bucket, Resource: name, RType: rtype, Metric: infraPresenceMetric, Value: value})
	}
	return rows
}

func retiredInfraResources(rows []infraLatestRow) map[string]bool {
	retired := map[string]bool{}
	for _, row := range rows {
		if row.Metric == infraPresenceMetric {
			retired[row.Resource] = row.Value == 0
		}
	}
	return retired
}
