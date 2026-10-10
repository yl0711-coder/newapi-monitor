package monitor

import (
	"sort"
	"strings"
	"time"
)

// observabilitySourceQuality is a read-only projection, not a governance state.
// A proved collector interval or a successful on-demand query is not proof that
// the entire source is healthy. Uninstrumented metadata is explicitly null.
// All machine times in the new contract use RFC3339 UTC, not legacy Unix values.
type observabilitySourceQuality struct {
	SourceID string `json:"source_id"`
	Name     string `json:"name"`
	Scope    string `json:"scope"`
	State    string `json:"state,omitempty"`
	Enabled  *bool  `json:"enabled"`
	Running  *bool  `json:"running"`

	ObservedAt      *string `json:"observed_at"`
	From            *string `json:"from"`
	To              *string `json:"to"`
	Through         *string `json:"through"`
	Target          *string `json:"target"`
	RequestedFrom   *string `json:"requested_from"`
	ProgressThrough *string `json:"progress_through"`
	// CoverageComplete concerns only the declared [from, target) collector
	// interval. QueryComplete concerns only the particular requested query.
	CoverageComplete *bool  `json:"coverage_complete"`
	QueryComplete    *bool  `json:"query_complete"`
	LagSeconds       *int64 `json:"lag_seconds"`
	TargetAgeSeconds *int64 `json:"target_age_seconds"`

	LastSuccessAt *string `json:"last_success_at"`
	LastFailureAt *string `json:"last_failure_at"`
	ParsedEvents  *uint64 `json:"parsed_events"`
	ParseFailures *uint64 `json:"parse_failures"`
	DroppedEvents *uint64 `json:"dropped_events"`
	Truncated     *bool   `json:"truncated"`
	Partial       *bool   `json:"partial"`

	CollectorVersion    *string `json:"collector_version"`
	GitSHA              *string `json:"git_sha"`
	SourceVersion       *string `json:"source_version"`
	Owner               *string `json:"owner"`
	NodeID              *string `json:"node_id"`
	CredentialExpiresAt *string `json:"credential_expires_at"`
	ClockSkewMS         *int64  `json:"clock_skew_ms"`

	RecoveryIncomplete *bool   `json:"recovery_incomplete"`
	RecoveryFrom       *string `json:"recovery_from"`
	RecoveryTo         *string `json:"recovery_to"`
	RecoveryDetectedAt *string `json:"recovery_detected_at"`
	// MetadataComplete deliberately remains false until missing MON-002
	// observation points (owner/node/credentials/drop counters, etc.) exist.
	MetadataComplete bool     `json:"quality_metadata_complete"`
	MissingFields    []string `json:"missing_fields"`
	ReasonCodes      []string `json:"reason_codes"`
}

// buildObservabilitySourceQuality never probes a DB, network, credential
// provider or runtime state. The caller supplies its existing /ready snapshot.
func buildObservabilitySourceQuality(ready readyStatusResponse, now time.Time) []observabilitySourceQuality {
	ids := make([]string, 0, len(ready.Collectors))
	for id := range ready.Collectors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]observabilitySourceQuality, 0, len(ids))
	for _, id := range ids {
		coverage := ready.Collectors[id]
		q := newObservabilitySourceQuality(id, "collector", now)
		q.CollectorVersion, q.GitSHA = qualityText(ready.ImageVersion), qualityText(ready.GitSHA)
		q.From, q.Through, q.Target = qualityTime(coverage.FromTs), qualityTime(coverage.ThroughTs), qualityTime(coverage.TargetTs)
		q.RequestedFrom, q.ProgressThrough = qualityTime(coverage.RequestedFromTs), qualityTime(coverage.ProgressThroughTs)
		applyObservabilityCollectorRuntime(&q, ready)
		if id == "nginx_evidence" && q.Enabled != nil && *q.Enabled {
			recovery := ready.CloudWatchNginx.EvidenceRecovery
			verified := recovery.Verified && recovery.ThroughTs > 0
			q.RecoveryIncomplete = qualityBool(recovery.Incomplete || !verified)
			q.RecoveryFrom, q.RecoveryTo = qualityTime(recovery.FromTs), qualityTime(recovery.ToTs)
			q.RecoveryDetectedAt = qualityTime(recovery.DetectedAt)
			if !verified {
				// The main-store watermark is not a proof for a replaced evidence
				// volume. Do not reproduce it as a certified through timestamp.
				q.Through, q.LagSeconds = nil, nil
				q.CoverageComplete = qualityBool(false)
				q.ReasonCodes = append(q.ReasonCodes, "evidence_coverage_unverified")
			} else {
				coverage.ThroughTs = min(coverage.ThroughTs, recovery.ThroughTs)
				q.Through = qualityTime(coverage.ThroughTs)
				applyObservabilityCoverage(&q, coverage, now)
				if recovery.Incomplete {
					q.CoverageComplete = qualityBool(false)
					q.ReasonCodes = append(q.ReasonCodes, "evidence_recovery_incomplete")
				}
			}
		} else {
			applyObservabilityCoverage(&q, coverage, now)
		}
		finishObservabilitySourceQuality(&q)
		result = append(result, q)
	}
	return result
}

// buildInvestigationSourceQuality projects only query-local observations. A
// found/empty result can prove query completion, never continuous ingestion or
// the absence of a real-world request. Counters are not lifetime source totals.
func buildInvestigationSourceQuality(sources []logChainCloudWatchSourceStatus, from, to int64, observedAt time.Time) []observabilitySourceQuality {
	result := make([]observabilitySourceQuality, 0, len(sources))
	for _, source := range sources {
		q := newObservabilitySourceQuality(string(source.Source), "query", observedAt)
		q.State = source.Status
		q.From, q.To = qualityTime(from), qualityTime(to)
		q.QueryComplete = qualityBool(false)
		if source.Status == "disabled" {
			q.Enabled = qualityBool(false)
			q.ReasonCodes = append(q.ReasonCodes, "source_disabled")
		} else if source.Status != "" {
			q.Enabled = qualityBool(true)
		}
		queried := source.Status == "found" || source.Status == "empty"
		// A failure may occur after one successful subquery; positive counters
		// remain useful, but zero-valued defaults must not mean zero failures.
		if queried || source.Parsed > 0 || source.ParseFailed > 0 {
			q.ParsedEvents, q.ParseFailures = qualityUint(source.Parsed), qualityUint(source.ParseFailed)
		}
		if queried || source.Truncated {
			q.Truncated = qualityBool(source.Truncated)
		}
		if queried || source.Partial {
			q.Partial = qualityBool(source.Partial)
		}
		if queried && !source.Partial && !source.Truncated && source.ParseFailed == 0 && from > 0 && to > from {
			q.QueryComplete = qualityBool(true)
		} else {
			q.ReasonCodes = append(q.ReasonCodes, "query_incomplete")
		}
		if source.Partial {
			q.ReasonCodes = append(q.ReasonCodes, "query_partial")
		}
		if source.Truncated {
			q.ReasonCodes = append(q.ReasonCodes, "query_truncated")
		}
		if source.ParseFailed > 0 {
			q.ReasonCodes = append(q.ReasonCodes, "query_parse_failed")
		}
		if from <= 0 || to <= from {
			q.ReasonCodes = append(q.ReasonCodes, "query_window_invalid")
		}
		finishObservabilitySourceQuality(&q)
		result = append(result, q)
	}
	return result
}

func newObservabilitySourceQuality(id, scope string, observedAt time.Time) observabilitySourceQuality {
	return observabilitySourceQuality{
		SourceID: id, Name: observabilitySourceName(id), Scope: scope,
		ObservedAt: qualityTime(observedAt.Unix()), ReasonCodes: []string{},
	}
}

func applyObservabilityCollectorRuntime(q *observabilitySourceQuality, ready readyStatusResponse) {
	var success, failure int64
	switch q.SourceID {
	case "source_worker":
		q.Enabled, q.Running = qualityBool(ready.Config.SourceWorkerEnabled), qualityBool(ready.Source.WorkerRunning)
		q.State = ready.Source.State
		// Source.LastSuccessAt measures connection/probe success, not a
		// successful sample. Do not mislabel it as this collector's success.
		success = ready.SampledAt
	case "customer_health":
		q.Enabled = qualityBool(ready.Config.CustomerHealthSourceEnabled || (ready.Config.SourceWorkerEnabled && ready.Config.CapacityEnabled))
	case "customer_health_history_requests", "customer_health_history_frt":
		q.Enabled = qualityBool(ready.Config.CustomerHealthSourceEnabled)
	case "problem_source":
		standardWorker := ready.Config.SourceWorkerEnabled && ready.Config.StabilityEnabled
		q.Enabled = qualityBool(ready.Config.StabilityProblemSourceEnabled || standardWorker)
		q.Running = qualityBool(ready.Source.ProblemSourceRunning || (standardWorker && ready.Source.WorkerRunning))
	case "metric_finalize":
		q.Enabled = qualityBool(ready.Config.SourceWorkerEnabled)
		success, failure = ready.MetricFinalize.LastSuccessAt, ready.MetricFinalize.LastFailureAt
	case "frt_replay":
		q.Enabled = qualityBool(ready.Config.SourceWorkerEnabled)
		// FRT replay and request finalization publish different proofs.
		// Request finalization success does not certify an FRT replay run.
	case "cloudwatch_pre_route":
		q.Enabled, q.Running = qualityBool(ready.CloudWatchPreRoute.Enabled), qualityBool(ready.CloudWatchPreRoute.Running)
		success, failure = ready.CloudWatchPreRoute.LastSuccessAt, ready.CloudWatchPreRoute.LastFailureAt
	case "cloudwatch_nginx":
		q.Enabled, q.Running = qualityBool(ready.CloudWatchNginx.Enabled), qualityBool(ready.CloudWatchNginx.Running)
		success, failure = ready.CloudWatchNginx.LastSuccessAt, ready.CloudWatchNginx.LastFailureAt
	case "nginx_evidence":
		q.Enabled = qualityBool(ready.CloudWatchNginx.Enabled && nginxEvidenceMode(ready.Config.NginxEvidenceMode) != "off")
		success, failure = ready.CloudWatchNginx.EvidenceLastSuccessAt, ready.CloudWatchNginx.EvidenceLastFailureAt
	}
	q.LastSuccessAt, q.LastFailureAt = qualityTime(success), qualityTime(failure)
	if failure > success && failure > 0 {
		q.ReasonCodes = append(q.ReasonCodes, "collector_last_attempt_failed")
	}
	if q.Enabled != nil && !*q.Enabled {
		q.ReasonCodes = append(q.ReasonCodes, "source_disabled")
	}
}

func applyObservabilityCoverage(q *observabilitySourceQuality, coverage readyCoverageStatus, now time.Time) {
	if coverage.TargetTs > 0 && coverage.TargetTs <= now.Unix() {
		age := now.Unix() - coverage.TargetTs
		q.TargetAgeSeconds = &age
	}
	if coverage.FromTs <= 0 || coverage.ThroughTs <= 0 || coverage.TargetTs <= 0 || coverage.CoverageStatus == "unknown" {
		q.ReasonCodes = append(q.ReasonCodes, "coverage_unknown")
		return
	}
	if coverage.FromTs > coverage.ThroughTs || coverage.TargetTs <= coverage.FromTs || coverage.ThroughTs > now.Unix() || coverage.TargetTs > now.Unix() {
		q.CoverageComplete = qualityBool(false)
		q.ReasonCodes = append(q.ReasonCodes, "coverage_window_invalid")
		return
	}
	if coverage.CoverageStatus != "" && coverage.CoverageStatus != "complete" && coverage.CoverageStatus != "partial" && coverage.CoverageStatus != "incomplete" {
		q.ReasonCodes = append(q.ReasonCodes, "coverage_status_unrecognized")
		return
	}
	lag := max(int64(0), coverage.TargetTs-coverage.ThroughTs)
	q.LagSeconds = &lag
	complete := coverage.ThroughTs >= coverage.TargetTs && coverage.CoverageStatus != "partial" && coverage.CoverageStatus != "incomplete"
	q.CoverageComplete = &complete
	if !complete {
		q.ReasonCodes = append(q.ReasonCodes, "coverage_incomplete")
	}
}

func finishObservabilitySourceQuality(q *observabilitySourceQuality) {
	// These observation points are deliberately not inferred from an image
	// tag, last-run timestamp, aggregate count or successful AWS query.
	q.MissingFields = []string{"source_version", "owner", "node_id", "credential_expires_at", "clock_skew_ms", "dropped_events"}
	if q.ParseFailures == nil {
		q.MissingFields = append(q.MissingFields, "parse_failures")
	}
	if q.LastSuccessAt == nil {
		q.MissingFields = append(q.MissingFields, "last_success_at")
	}
	if q.LastFailureAt == nil {
		q.MissingFields = append(q.MissingFields, "last_failure_at")
	}
	if q.Scope == "collector" && q.CoverageComplete == nil {
		q.MissingFields = append(q.MissingFields, "coverage_complete")
	}
	q.ReasonCodes = append(q.ReasonCodes, "quality_metadata_unavailable")
}

func observabilitySourceName(id string) string {
	switch id {
	case "source_worker":
		return "NewAPI 实时采样"
	case "customer_health":
		return "客户维护当日事实"
	case "customer_health_history_requests":
		return "客户维护历史请求事实"
	case "customer_health_history_frt":
		return "客户维护历史 FRT"
	case "problem_source":
		return "渠道问题事实"
	case "metric_finalize":
		return "请求事实定稿"
	case "frt_replay":
		return "FRT 历史回填"
	case "cloudwatch_pre_route":
		return "CloudWatch 前置拒绝"
	case "cloudwatch_nginx":
		return "CloudWatch Nginx 事实"
	case "nginx_evidence":
		return "Nginx 排障证据"
	case string(cwSourceCloudFrontAccess):
		return "CloudFront 访问日志"
	case string(cwSourceCloudFrontDiagnostic):
		return "CloudFront 诊断日志"
	case string(cwSourceWorkerNginx):
		return "Worker Nginx 日志"
	case string(cwSourceWorkerNewAPI):
		return "Worker NewAPI 日志"
	default:
		return id
	}
}

func qualityTime(unix int64) *string {
	if unix <= 0 {
		return nil
	}
	value := time.Unix(unix, 0).UTC().Format(time.RFC3339)
	return &value
}

func qualityText(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" || value == "unknown" || value == "dev" {
		return nil
	}
	return &value
}

func qualityBool(value bool) *bool     { return &value }
func qualityUint(value uint64) *uint64 { return &value }
