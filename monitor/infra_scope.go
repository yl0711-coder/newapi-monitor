package monitor

import (
	"context"
	"net/url"
	"strings"

	"gorm.io/gorm"
)

func managedAWSInfraPlatformNames() []string {
	return []string{"aws", "ecs/fargate", "rds", "alb"}
}

// managedAWSInfraResource is the single ownership boundary for AWS resources
// whose infrastructure metrics are delegated to CloudWatch. Lightsail and host
// agent resources deliberately do not match it.
func managedAWSInfraResource(name, rtype, platform string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	rtype = strings.ToLower(strings.TrimSpace(rtype))
	platform = strings.ToLower(strings.TrimSpace(platform))
	return strings.HasPrefix(name, "ecs/") || strings.HasPrefix(name, "rds/") ||
		strings.HasPrefix(name, "alb/") ||
		(strings.HasPrefix(name, "aws 资源发现/") && !strings.Contains(name, "lightsail/")) ||
		rtype == "ecs_service" || platform == "ecs/fargate" || platform == "rds" || platform == "alb" || platform == "aws"
}

func (m *Monitor) monitorOwnsInfraResource(name, rtype, platform string) bool {
	if managedAWSInfraResource(name, rtype, platform) {
		return !m.cfg.InfraManagedAWSDisabled
	}
	if m.cfg.InfraLightsailInstancesOnly {
		switch rtype {
		case "database", "lb":
			return false
		case "inventory":
			return name != "AWS 资源发现/Lightsail/数据库" && name != "AWS 资源发现/Lightsail/负载均衡"
		}
	}
	return true
}

// delegatedManagedAWSResource also recognizes pre-standard resource names by
// their durable registry metadata. It is used by direct resource endpoints,
// where a caller can supply a name that is not present in the filtered view.
func (m *Monitor) delegatedManagedAWSResource(ctx context.Context, name string) (bool, error) {
	if !m.cfg.InfraManagedAWSDisabled {
		return false, nil
	}
	if managedAWSInfraResource(name, "", "") {
		return true, nil
	}
	var count int64
	err := m.storeDB.WithContext(ctx).Model(&InfraAsset{}).
		Where("resource = ?", name).
		Where("LOWER(platform) IN ? OR LOWER(kind) = ?", managedAWSInfraPlatformNames(), "ecs_service").
		Count(&count).Error
	return count > 0, err
}

// monitorOwnedInfraAssetsQuery applies the ownership boundary before limits or
// keyset cursors. Filtering after pagination could produce an empty page while
// later Lightsail assets still exist.
func (m *Monitor) monitorOwnedInfraAssetsQuery(tx *gorm.DB) *gorm.DB {
	if !m.cfg.InfraManagedAWSDisabled {
		return tx
	}
	return tx.Where(`LOWER(platform) NOT IN ?
		AND LOWER(kind) <> ?
		AND LOWER(resource) NOT LIKE ?
		AND LOWER(resource) NOT LIKE ?
		AND LOWER(resource) NOT LIKE ?`,
		managedAWSInfraPlatformNames(), "ecs_service", "ecs/%", "rds/%", "alb/%")
}

func (m *Monitor) monitorOwnedInfraAlertsQuery(tx *gorm.DB) *gorm.DB {
	if !m.cfg.InfraManagedAWSDisabled {
		return tx
	}
	return tx.Where(`target NOT LIKE ?
		AND target NOT LIKE ?
		AND target NOT LIKE ?
		AND (target NOT LIKE ? OR target LIKE ?)`,
		"ecs/%", "rds/%", "alb/%", "AWS 资源发现/%", "AWS 资源发现/Lightsail/%")
}

func safeInfraManagedAWSDashboardURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return ""
	}
	return u.String()
}

type infraManagedAWSView struct {
	MonitorEnabled bool   `json:"monitor_enabled"`
	Provider       string `json:"provider"`
	DashboardURL   string `json:"dashboard_url,omitempty"`
}

func (m *Monitor) managedAWSInfraView() infraManagedAWSView {
	if !m.cfg.InfraManagedAWSDisabled {
		return infraManagedAWSView{MonitorEnabled: true, Provider: "Monitor"}
	}
	return infraManagedAWSView{MonitorEnabled: false, Provider: "CloudWatch", DashboardURL: m.cfg.InfraManagedAWSDashboardURL}
}

// infraServerPanels only controls presentation. A missing, failed or stale
// discovery is not proof that a monitored resource no longer exists.
type infraServerPanels struct {
	OriginLock   bool `json:"origin_lock"`
	Database     bool `json:"database"`
	LoadBalancer bool `json:"load_balancer"`
}

func (m *Monitor) serverInfraPanels(s *InfraSnapshot) infraServerPanels {
	panels := infraServerPanels{
		OriginLock:   len(probeDomainList(m.cfg.OriginLockTargets)) > 0 || len(s.Locks) > 0,
		Database:     true,
		LoadBalancer: true,
	}
	if m.cfg.InfraLightsailInstancesOnly && m.cfg.InfraManagedAWSDisabled {
		panels.Database, panels.LoadBalancer = false, false
		return panels
	}
	if !m.cfg.InfraManagedAWSDisabled || s.RegistryError != "" {
		return panels
	}
	configured := map[string]bool{}
	for _, t := range parseInfraTargets(m.cfg.InfraResources) {
		configured[t.rtype] = true
	}
	for _, discovery := range s.Discoveries {
		if discovery.Status != "ok" || discovery.AgeSec < 0 || discovery.ResourceCount != 0 {
			continue
		}
		switch discovery.Name {
		case "AWS 资源发现/Lightsail/数据库":
			panels.Database = configured["database"] || len(s.Databases) > 0 || s.Database != nil
		case "AWS 资源发现/Lightsail/负载均衡":
			panels.LoadBalancer = configured["lb"] || len(s.LoadBalancers) > 0 || s.LB != nil
		}
	}
	return panels
}
