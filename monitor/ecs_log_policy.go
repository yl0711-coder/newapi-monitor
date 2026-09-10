package monitor

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	ecsLogLeaseSeconds          = int64(15 * 60)
	ecsLogSignatureSkew         = int64(120)
	ecsLogStartupSeconds        = int64(120)
	ecsLogReportStaleSeconds    = int64(120)
	ecsLogDiscoveryStaleSeconds = int64(120)
	ecsLogReplaySeconds         = int64(24 * 60 * 60)
	ecsLogSourceLimit           = 10000
)

// Authorization is fixed per SERVICE, not per task name. Runtime IDs are
// obtained from authenticated DescribeTasks responses, never trusted metadata
// supplied by an unauthenticated collector. Task IAM is the security boundary;
// containers in one task share that role and are not mutually isolated tenants.
type ECSLogPolicy struct {
	ClusterARN         string              `json:"cluster_arn"`
	ServiceARN         string              `json:"service_arn"`
	TaskRoleARN        string              `json:"task_role_arn"`
	TaskDefinitionARNs []string            `json:"task_definition_arns,omitempty"`
	Containers         map[string][]string `json:"containers"`
}

var ecsLogARNPattern = regexp.MustCompile(`^arn:aws:ecs:([a-z]{2}-[a-z]+-[0-9]):([0-9]{12}):(cluster|service|task)/([A-Za-z0-9_-]+)(?:/([A-Za-z0-9_-]+))?$`)
var ecsTaskIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var ecsRoleARNPattern = regexp.MustCompile(`^arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+$`)
var ecsTaskDefinitionARNPattern = regexp.MustCompile(`^arn:aws:ecs:([a-z]{2}-[a-z]+-[0-9]):([0-9]{12}):task-definition/[A-Za-z0-9_-]+:[1-9][0-9]*$`)
var ecsLogNodePattern = regexp.MustCompile(`^ecs-[a-f0-9]{48}$`)
var ecsLogAudiencePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:./_-]{7,127}$`)

func ecsLogLane(lane string) bool {
	switch lane {
	case "access", "error", "evidence", "reject":
		return true
	}
	return false
}

func parseECSLogPolicies(s Settings) ([]ECSLogPolicy, error) {
	if s.ECSLogProductionEnabled && (!s.ECSLogEnabled || s.ECSLogScope != ecsLogScopeProduction) {
		return nil, errors.New("ECS production gate requires enabled production scope")
	}
	if s.ECSLogOwnershipEnabled && !ecsLogRuntimeEnabled(s) {
		return nil, errors.New("ECS ownership ledger requires an enabled authenticated receiver")
	}
	if !s.ECSLogEnabled {
		return nil, nil
	}
	if !ecsLogAudiencePattern.MatchString(s.ECSLogAudience) {
		return nil, errors.New("ECS logs require an explicit deployment-specific audience")
	}
	if s.LocalSnapshotOnly || s.LocalAuthBypass {
		return nil, errors.New("ECS log runtime cannot be enabled in offline snapshot mode")
	}
	if len(s.ECSLogBridgeToken) < 32 || s.ECSLogBridgeToken == s.IngestToken || s.ECSLogBridgeToken == s.SessionSecret || s.ECSLogBridgeToken == s.UpstreamCredentialSecret {
		return nil, errors.New("ECS registration requires an independent bridge credential of at least 32 bytes")
	}
	if !ecsLogRuntimeEnabled(s) {
		return nil, errors.New("ECS logs require isolated scope or the explicit production gate")
	}
	var policies []ECSLogPolicy
	if s.ECSLogScope == ecsLogScopeIsolated && (strings.TrimSpace(s.ProdDSN) != "" || s.SourceWorkerEnabled) {
		return nil, errors.New("isolated ECS acceptance requires a separate Monitor store with NEWAPI_LOG_DSN unset and MONITOR_SOURCE_WORKER_ENABLED=false")
	}
	if s.ECSLogScope == ecsLogScopeProduction && (!s.ECSLogOwnershipEnabled || !s.ECSArchiveEnabled) {
		return nil, errors.New("production ECS logs require ownership and external archive gates")
	}
	if decodeECSBoundedJSON(strings.NewReader(s.ECSLogPoliciesJSON), 64<<10, &policies) != nil || len(policies) == 0 || len(policies) > 32 {
		return nil, errors.New("invalid bounded ECS log service policies")
	}
	seen := map[string]bool{}
	for _, p := range policies {
		cluster, service := ecsLogARNPattern.FindStringSubmatch(p.ClusterARN), ecsLogARNPattern.FindStringSubmatch(p.ServiceARN)
		if len(cluster) == 0 || len(service) == 0 || cluster[3] != "cluster" || cluster[5] != "" || service[3] != "service" || service[5] == "" || cluster[1] != s.AWSRegion || service[1] != cluster[1] || service[2] != cluster[2] || service[4] != cluster[4] || !ecsRoleARNPattern.MatchString(p.TaskRoleARN) || !strings.HasPrefix(p.TaskRoleARN, "arn:aws:iam::"+cluster[2]+":role/") || seen[p.ServiceARN] || len(p.Containers) == 0 || len(p.Containers) > 10 {
			return nil, errors.New("ECS policy must name a unique same-account region/cluster/service/task-role")
		}
		seen[p.ServiceARN] = true
		definitionSeen := map[string]bool{}
		if s.ECSLogScope == ecsLogScopeProduction && (len(p.TaskDefinitionARNs) == 0 || len(p.TaskDefinitionARNs) > 16) {
			return nil, errors.New("production ECS policy requires a bounded exact task-definition allowlist")
		}
		for _, definition := range p.TaskDefinitionARNs {
			parts := ecsTaskDefinitionARNPattern.FindStringSubmatch(definition)
			if len(parts) == 0 || parts[1] != cluster[1] || parts[2] != cluster[2] || definitionSeen[definition] {
				return nil, errors.New("invalid, duplicate or cross-account ECS task-definition allowlist entry")
			}
			definitionSeen[definition] = true
		}
		for container, lanes := range p.Containers {
			if !nginxNodeNamePattern.MatchString(container) || len(lanes) == 0 || len(lanes) > 4 {
				return nil, errors.New("invalid ECS policy container/lanes")
			}
			laneSeen := map[string]bool{}
			for _, lane := range lanes {
				if !ecsLogLane(lane) || laneSeen[lane] {
					return nil, errors.New("invalid or duplicate ECS collection lane")
				}
				laneSeen[lane] = true
				if (lane == "access" || lane == "error" || lane == "evidence") && !s.NginxEnabled || lane == "error" && !s.NginxErrorEnabled || lane == "evidence" && nginxEvidenceMode(s.NginxEvidenceMode) == "off" {
					return nil, fmt.Errorf("ECS lane %s requires its receiver to be enabled", lane)
				}
			}
		}
	}
	return policies, nil
}

func ecsLogTaskDefinitionAllowed(p ECSLogPolicy, arn string) bool {
	if len(p.TaskDefinitionARNs) == 0 {
		return true // isolated fixtures and the first isolated protocol remain compatible.
	}
	for _, allowed := range p.TaskDefinitionARNs {
		if arn == allowed {
			return true
		}
	}
	return false
}

func (m *Monitor) ecsLogPolicy(service, container, lane string) (ECSLogPolicy, bool) {
	for _, p := range m.ecsLogPolicies {
		if p.ServiceARN != service {
			continue
		}
		for _, allowed := range p.Containers[container] {
			if lane == allowed {
				return p, true
			}
		}
	}
	return ECSLogPolicy{}, false
}
