package monitor

import (
	"context"
	"errors"
	"sort"
	"time"

	"gorm.io/gorm"
)

// Collection liveness and delivery completeness are separate dimensions. A
// healthy agent is not evidence of a complete request interval or zero errors.
type ecsLogServiceHealth struct {
	ServiceARN       string `json:"service_arn"`
	Configured       bool   `json:"configured"`
	DiscoveryStatus  string `json:"discovery_status"`
	Status           string `json:"status"`
	TaskCount        int    `json:"task_count"`
	ActiveTasks      int    `json:"active_tasks"`
	SourceCount      int    `json:"source_count"`
	ActiveSources    int    `json:"active_sources"`
	StartingSources  int    `json:"starting_sources"`
	UnhealthySources int    `json:"unhealthy_sources"`
	StoppedPending   int    `json:"stopped_pending"`
	StoppedDrained   int    `json:"stopped_drained"`
	DeliveryGaps     int    `json:"delivery_gaps"`
	CoverageStatus   string `json:"coverage_status"`
}

type ecsLogHealth struct {
	Archive          *ecsArchiveHealth     `json:"archive,omitempty"`
	Enabled          bool                  `json:"enabled"`
	Available        bool                  `json:"available"`
	Status           string                `json:"status"`
	CheckedAt        int64                 `json:"checked_at"`
	SourceCount      int                   `json:"source_count"`
	ActiveSources    int                   `json:"active_sources"`
	UnhealthySources int                   `json:"unhealthy_sources"`
	DiscoveryIssues  int                   `json:"discovery_issues"`
	StoppedPending   int                   `json:"stopped_pending"`
	StoppedDrained   int                   `json:"stopped_drained"`
	DeliveryGaps     int                   `json:"delivery_gaps"`
	CoverageStatus   string                `json:"coverage_status"`
	Services         []ecsLogServiceHealth `json:"services"`
}

// One bounded local read snapshot keeps totals and a paged detail view
// consistent while a discovery transaction is adding or retiring sources.
func (m *Monitor) ecsLogSnapshot(ctx context.Context) ([]ECSLogSource, []ECSLogDiscovery, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var sources []ECSLogSource
	var discoveries []ECSLogDiscovery
	if m.storeDB == nil {
		return nil, nil, errors.New("ECS registry unavailable")
	}
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Public keys never belong in the presentation/health projection.
		if err := tx.Omit("public_key").Order("service_arn, task_arn, node, lane").Limit(ecsLogSourceLimit + 1).Find(&sources).Error; err != nil {
			return err
		}
		if err := tx.Order("service_arn").Limit(ecsLogSourceLimit + 1).Find(&discoveries).Error; err != nil {
			return err
		}
		if len(sources) > ecsLogSourceLimit || len(discoveries) > ecsLogSourceLimit {
			return errors.New("ECS registry exceeds bounded status budget")
		}
		if m.cfg.ECSArchiveEnabled {
			return projectArchiveDelivery(tx, sources)
		}
		return nil
	})
	return sources, discoveries, err
}

func (m *Monitor) ecsLogHealth(ctx context.Context, now int64) ecsLogHealth {
	if !m.cfg.ECSLogEnabled {
		return ecsLogHealth{Status: "disabled", Available: true, CheckedAt: now, Services: []ecsLogServiceHealth{}, CoverageStatus: "not_evaluated"}
	}
	sources, discoveries, err := m.ecsLogSnapshot(ctx)
	if err != nil {
		return ecsLogHealth{Enabled: true, Status: "unavailable", CheckedAt: now, Services: []ecsLogServiceHealth{}, CoverageStatus: "unverified"}
	}
	health := m.projectECSLogHealth(sources, discoveries, now)
	m.attachECSArchiveHealth(ctx, &health, now)
	return health
}

func (m *Monitor) projectECSLogHealth(sources []ECSLogSource, discoveries []ECSLogDiscovery, now int64) ecsLogHealth {
	result := ecsLogHealth{Enabled: m.cfg.ECSLogEnabled, Available: true, Status: "ok", CheckedAt: now, Services: []ecsLogServiceHealth{}, CoverageStatus: "unverified"}
	groups := map[string]*ecsLogServiceHealth{}
	tasks, activeTasks := map[string]map[string]bool{}, map[string]map[string]bool{}
	ensure := func(service string) *ecsLogServiceHealth {
		if groups[service] == nil {
			groups[service] = &ecsLogServiceHealth{ServiceARN: service, DiscoveryStatus: "not_configured", Status: "not_configured", CoverageStatus: "unverified"}
			tasks[service], activeTasks[service] = map[string]bool{}, map[string]bool{}
		}
		return groups[service]
	}
	for _, policy := range m.ecsLogPolicies {
		group := ensure(policy.ServiceARN)
		group.Configured, group.DiscoveryStatus, group.Status = true, "awaiting_discovery", "awaiting_sources"
	}
	for _, discovery := range discoveries {
		group := ensure(discovery.ServiceARN)
		if group.Configured {
			group.DiscoveryStatus = ecsLogDiscoveryStatus(discovery, now)
		}
	}
	for _, source := range sources {
		group := ensure(source.ServiceARN)
		group.SourceCount++
		tasks[source.ServiceARN][source.TaskARN] = true
		if source.StoppedAt == 0 {
			group.ActiveSources++
			activeTasks[source.ServiceARN][source.TaskARN] = true
		}
		switch ecsLogSourceStatus(source, now) {
		case "starting":
			group.StartingSources++
		case "stopped_delivery_unconfirmed":
			group.StoppedPending++
		case "delivery_gap_unresolved":
			group.DeliveryGaps++
		case "stopped_archive_drained":
			group.StoppedDrained++
		case "reporting", "heartbeating_no_log_batches":
			// These are liveness only, never coverage proofs.
		default:
			group.UnhealthySources++
		}
	}
	for service, group := range groups {
		group.TaskCount, group.ActiveTasks = len(tasks[service]), len(activeTasks[service])
		if group.Configured {
			switch {
			case group.DiscoveryStatus != "discovered":
				group.Status = "discovery_degraded"
				result.DiscoveryIssues++
			case group.UnhealthySources > 0:
				group.Status = "collection_degraded"
			case group.ActiveSources == 0:
				// No source rows is not evidence that the service has no tasks.
				group.Status = "awaiting_sources"
				if group.SourceCount > 0 {
					switch {
					case group.DeliveryGaps > 0:
						group.Status = "delivery_gap_unresolved"
					case group.StoppedDrained == group.SourceCount:
						group.Status = "stopped_archives_drained"
					default:
						group.Status = "stopped_delivery_unconfirmed"
					}
				}
			case group.StartingSources > 0:
				group.Status = "starting"
			default:
				group.Status = "reporting"
			}
		}
		result.SourceCount += group.SourceCount
		result.ActiveSources += group.ActiveSources
		result.UnhealthySources += group.UnhealthySources
		result.StoppedPending += group.StoppedPending
		result.StoppedDrained += group.StoppedDrained
		result.DeliveryGaps += group.DeliveryGaps
		if group.Status != "reporting" || group.DeliveryGaps > 0 {
			result.Status = "degraded"
		}
		result.Services = append(result.Services, *group)
	}
	sort.Slice(result.Services, func(i, j int) bool { return result.Services[i].ServiceARN < result.Services[j].ServiceARN })
	if len(m.ecsLogPolicies) == 0 {
		result.Status = "degraded"
	}
	if !m.cfg.ECSLogEnabled {
		result.Status, result.CoverageStatus = "disabled", "not_evaluated"
	}
	return result
}
