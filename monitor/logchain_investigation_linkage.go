package monitor

import (
	"context"
	"strconv"
	"time"
)

// Never infer reverse uniqueness from the customer/Request-ID-filtered rows
// shown in the UI. Scan a tiny independent time envelope without those filters.
// The existing interactive query gate and parent 30s budget still apply.
func (m *Monitor) loadInvestigationAssociationCandidates(ctx context.Context, result *logChainInvestigationResult, in logChainInvestigationInput) {
	result.associationCandidates = nil
	result.associationCandidatesRequired = false
	result.associationCandidatesComplete = false
	if !result.associationEdgesComplete || in.CloudFrontRequestID != "" && in.NewAPIRequestID == "" {
		return
	}
	var firstMS, lastMS int64
	for _, group := range result.Evidence {
		if group.Source != cwSourceCloudFrontAccess {
			continue
		}
		for _, e := range group.Evidence {
			for _, row := range result.Requests {
				if !investigationBusinessMatchesInput(row, in) || !cloudFrontMatchesBusiness(e, row) {
					continue
				}
				if firstMS == 0 || e.EventMS < firstMS {
					firstMS = e.EventMS
				}
				if e.EventMS > lastMS {
					lastMS = e.EventMS
				}
			}
		}
	}
	if firstMS == 0 {
		return
	}
	result.associationCandidatesRequired = true
	fromTs, toTs := (firstMS-5000)/1000, (lastMS+5000)/1000+1
	gap := "入口与业务记录的双向唯一性尚未证明：独立业务候选反查未完成，入口证据仅作候选。"
	// A clipped caller window can hide a second matching edge request just
	// outside its boundary. A successful query alone is not a universe proof.
	for _, row := range result.Requests {
		if investigationBusinessMatchesInput(row, in) &&
			(!in.From.IsZero() && row.CreatedAt*1000-5000 < in.From.UnixMilli() ||
				!in.To.IsZero() && row.CreatedAt*1000+5000 >= in.To.UnixMilli()) {
			result.associationEdgesComplete = false
			result.BlindSpots = append(append([]string(nil), result.BlindSpots...), gap)
			return
		}
	}
	if toTs-fromTs > 120 || fromTs < 0 {
		result.BlindSpots = append(append([]string(nil), result.BlindSpots...), gap)
		return
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, more, err := m.queryLogChain(queryCtx, logChainScope{
		FromTs: fromTs, ToTs: toTs, Asc: true, Limit: logChainInvestigationCandidateLimit,
	}, nil)
	if err != nil || more {
		result.BlindSpots = append(append([]string(nil), result.BlindSpots...), gap)
		return
	}
	result.associationCandidates, result.associationCandidatesComplete = rows, true
}

// Association is computed once per result. Summary, source cards and timeline
// consume these exact per-event decisions, not the filter used to fetch a page.
func (m *Monitor) associateInvestigationEvidence(result *logChainInvestigationResult, in logChainInvestigationInput) {
	complete := make(map[cloudWatchLogSourceID]bool)
	for _, status := range result.SourceStatus {
		complete[status.Source] = investigationSourceComplete(status)
	}
	requestRefs := make(map[string]bool)
	for _, id := range investigationRequestIDs(in.NewAPIRequestID, result.Requests) {
		if (in.NewAPIRequestID != "" || in.CloudFrontRequestID == "") && (in.NewAPIRequestID == "" || id == in.NewAPIRequestID) {
			requestRefs[m.investigationDigest("oneapi-request-id", id)] = true
		}
	}
	var access []cloudWatchStructuredEvidence
	for _, group := range result.Evidence {
		if group.Source == cwSourceCloudFrontAccess {
			access = group.Evidence
		}
	}
	associationComplete := result.associationEdgesComplete && result.associationCandidatesComplete && result.CandidatesComplete && !result.CandidateTruncated
	// A supplied edge ID can identify its own edge event without any business
	// record. It still says nothing about an independently selected business ID.
	if in.CloudFrontRequestID != "" && in.NewAPIRequestID == "" {
		associationComplete = true
	}
	accessLevels := m.cloudFrontEvidenceLevels(result.associationCandidates, access, in,
		complete[cwSourceCloudFrontAccess] && associationComplete)
	for _, e := range access {
		if accessLevels[e.EventRef] == "correlated" && m.cloudFrontConflictsWithWorker(e, result.associationCandidates, result.Evidence) {
			accessLevels[e.EventRef] = "ambiguous"
		}
	}
	// Copy before changing values: delayed rechecks must not mutate a previously
	// published result or a cache entry sharing its backing arrays.
	groups := make([]logChainCloudWatchEvidence, len(result.Evidence))
	for i, group := range result.Evidence {
		groups[i] = logChainCloudWatchEvidence{Source: group.Source, Evidence: append([]cloudWatchStructuredEvidence(nil), group.Evidence...)}
		for j := range groups[i].Evidence {
			e := &groups[i].Evidence[j]
			e.EvidenceLevel = "ambiguous"
			switch group.Source {
			case cwSourceCloudFrontAccess:
				e.EvidenceLevel = accessLevels[e.EventRef]
			case cwSourceCloudFrontDiagnostic:
				if !complete[group.Source] || !complete[cwSourceCloudFrontAccess] || !associationComplete {
					break
				}
				for _, a := range access {
					if e.CloudFrontIDHMAC != "" && e.CloudFrontIDHMAC == a.CloudFrontIDHMAC && e.HMACKeyID == a.HMACKeyID {
						if !cloudFrontEventsCompatible(*e, a) {
							e.EvidenceLevel = "ambiguous"
							break
						}
						e.EvidenceLevel = accessLevels[a.EventRef]
					}
				}
			case cwSourceWorkerNginx, cwSourceWorkerNewAPI:
				if e.OneAPIIDHMAC != "" && e.HMACKeyID == m.cfg.CloudWatchEvidenceHMACKeyID && requestRefs[e.OneAPIIDHMAC] {
					e.EvidenceLevel = "exact"
				}
			case cwSourceMaster, cwSourceRDSError, cwSourceRDSSlowQuery:
				e.EvidenceLevel = "inferred"
			}
			if e.EvidenceLevel == "" {
				e.EvidenceLevel = "ambiguous"
			}
		}
	}
	result.Evidence = groups
	result.SourceStatus = append([]logChainCloudWatchSourceStatus(nil), result.SourceStatus...)
	for i := range result.SourceStatus {
		status := &result.SourceStatus[i]
		// Query selectors are not association evidence. An empty/failed source
		// cannot retain an exact label merely because an ID filter was sent.
		status.Linkage = "ambiguous"
		if status.Status == "skipped" {
			status.Linkage = "skipped"
		}
		for _, group := range groups {
			if group.Source != status.Source || len(group.Evidence) == 0 {
				continue
			}
			// A mixed source must not imply every item is an exact match.
			status.Linkage = group.Evidence[0].EvidenceLevel
			for _, item := range group.Evidence[1:] {
				if investigationLinkageRank(item.EvidenceLevel) < investigationLinkageRank(status.Linkage) {
					status.Linkage = item.EvidenceLevel
				}
			}
		}
	}
}

func investigationSourceComplete(status logChainCloudWatchSourceStatus) bool {
	return (status.Status == "found" || status.Status == "empty") && !status.Partial && !status.Truncated && status.ParseFailed == 0
}

func investigationLinkageRank(level string) int {
	switch level {
	case "exact":
		return 3
	case "correlated":
		return 2
	case "inferred":
		return 1
	default:
		return 0
	}
}

// Both sides must be unique. Two retry rows with one Request ID are one
// logical candidate, but two distinct requests can never become one match.
// Missing pages, parse failures or a failed business query invalidate uniqueness.
func (m *Monitor) cloudFrontEvidenceLevels(rows []LogChainRow, evidence []cloudWatchStructuredEvidence, in logChainInvestigationInput, complete bool) map[string]string {
	out := make(map[string]string, len(evidence))
	for _, e := range evidence {
		out[e.EventRef] = "ambiguous"
	}
	if !complete {
		return out
	}
	edgeToRequests := make(map[string]map[string]bool)
	requestToEdges := make(map[string]map[string]bool)
	valid := make(map[string]bool)
	for i, e := range evidence {
		identityValid := e.CloudFrontIDHMAC != "" && e.HMACKeyID == m.cfg.CloudWatchEvidenceHMACKeyID
		// Repeated events for one edge request must agree; never use a duplicate
		// with conflicting dimensions as extra support for that same identity.
		conflict := false
		for j, other := range evidence {
			if identityValid && i != j && e.CloudFrontIDHMAC == other.CloudFrontIDHMAC && !cloudFrontEventsCompatible(e, other) {
				conflict = true
			}
		}
		valid[e.EventRef] = identityValid && !conflict
		// Invalid identities and contradictory duplicates are not promotable,
		// but remain competing observations. Dropping them would manufacture
		// uniqueness for another, otherwise valid edge request.
		edgeKey := investigationAssociationEdgeKey(e, i, identityValid)
		// An explicit edge ID proves only the edge event, never the equality of
		// independently supplied NewAPI and CloudFront IDs.
		if in.CloudFrontRequestID != "" && in.NewAPIRequestID == "" {
			if valid[e.EventRef] && e.CloudFrontIDHMAC == m.investigationDigest("cloudfront-request-id", in.CloudFrontRequestID) && cloudFrontMatchesInput(e, in) {
				out[e.EventRef] = "exact"
			}
			continue
		}
		for rowIndex, row := range rows {
			if !cloudFrontCouldMatchBusiness(e, row) {
				continue
			}
			requestKey := investigationAssociationRequestKey(row, rowIndex)
			if edgeToRequests[edgeKey] == nil {
				edgeToRequests[edgeKey] = make(map[string]bool)
			}
			edgeToRequests[edgeKey][requestKey] = true
			if requestToEdges[requestKey] == nil {
				requestToEdges[requestKey] = make(map[string]bool)
			}
			requestToEdges[requestKey][edgeKey] = true
		}
	}
	for i, e := range evidence {
		matches := edgeToRequests[investigationAssociationEdgeKey(e, i, true)]
		if !valid[e.EventRef] || len(matches) != 1 || !cloudFrontMatchesInput(e, in) ||
			(in.CloudFrontRequestID != "" && e.CloudFrontIDHMAC != m.investigationDigest("cloudfront-request-id", in.CloudFrontRequestID)) {
			continue
		}
		for request := range matches {
			if len(requestToEdges[request]) != 1 {
				continue
			}
			for rowIndex, row := range rows {
				if investigationAssociationRequestKey(row, rowIndex) == request && investigationBusinessMatchesInput(row, in) && cloudFrontMatchesBusiness(e, row) {
					out[e.EventRef] = "correlated"
				}
			}
		}
	}
	return out
}

func investigationAssociationEdgeKey(e cloudWatchStructuredEvidence, index int, identityValid bool) string {
	if identityValid {
		return "edge:" + e.CloudFrontIDHMAC
	}
	return "unverified-event:" + strconv.Itoa(index)
}

func investigationAssociationRequestKey(row LogChainRow, index int) string {
	if row.RequestID != "" {
		return "request:" + row.RequestID
	}
	return "row:" + strconv.Itoa(index)
}

func investigationBusinessMatchesInput(row LogChainRow, in logChainInvestigationInput) bool {
	return (in.NewAPIRequestID == "" || row.RequestID == in.NewAPIRequestID) &&
		(in.UserID == 0 || row.UserID == in.UserID) &&
		(in.Model == "" || row.ModelName == in.Model) &&
		(in.Group == "" || row.Group == in.Group) &&
		(in.Path == "" || cwRoute(row.RequestPath) == cwRoute(in.Path))
}

func (m *Monitor) cloudFrontConflictsWithWorker(edge cloudWatchStructuredEvidence, candidates []LogChainRow, groups []logChainCloudWatchEvidence) bool {
	for _, row := range candidates {
		if row.RequestID == "" || !cloudFrontMatchesBusiness(edge, row) {
			continue
		}
		requestRef := m.investigationDigest("oneapi-request-id", row.RequestID)
		for _, group := range groups {
			if group.Source != cwSourceWorkerNginx {
				continue
			}
			for _, e := range group.Evidence {
				if e.OneAPIIDHMAC != requestRef || e.HMACKeyID != m.cfg.CloudWatchEvidenceHMACKeyID {
					continue
				}
				// Nginx and CloudFront HTTP status/duration can legitimately differ
				// after a disconnect; request method/route/host cannot identify two
				// different requests as one. Never compare provider status here.
				if e.Method != "" && edge.Method != "" && e.Method != edge.Method ||
					e.Route != "" && edge.Route != "" && e.Route != edge.Route ||
					e.Host != "" && edge.Host != "" && e.Host != edge.Host {
					return true
				}
			}
		}
	}
	return false
}

func cloudFrontMatchesInput(e cloudWatchStructuredEvidence, in logChainInvestigationInput) bool {
	return (in.Path == "" || e.Route == cwRoute(in.Path)) &&
		(in.Status == 0 || e.Status != nil && *e.Status == in.Status) &&
		(in.From.IsZero() || e.EventMS >= in.From.UnixMilli()) &&
		(in.To.IsZero() || e.EventMS < in.To.UnixMilli())
}

func cloudFrontMatchesBusiness(e cloudWatchStructuredEvidence, row LogChainRow) bool {
	// Missing path or duration cannot be compensated by another matching field.
	// Candidates missing these fields still compete in cloudFrontCouldMatchBusiness.
	return row.RequestPath != "" && e.Route != "" && row.UseTimeKnown && row.UseTime >= 0 &&
		e.RequestMS != nil && *e.RequestMS >= 0 && cloudFrontCouldMatchBusiness(e, row)
}

func cloudFrontCouldMatchBusiness(e cloudWatchStructuredEvidence, row LogChainRow) bool {
	// Missing fields cannot eliminate a competing request from uniqueness.
	if row.CreatedAt <= 0 || e.EventMS <= 0 || absInt64(e.EventMS-row.CreatedAt*1000) > 5000 ||
		row.RequestPath != "" && e.Route != "" && e.Route != cwRoute(row.RequestPath) {
		return false
	}
	if row.UseTimeKnown && row.UseTime >= 0 && e.RequestMS != nil && *e.RequestMS >= 0 {
		expected := row.UseTime * 1000
		if absInt64(*e.RequestMS-expected) > max(expected/2, 2000) {
			return false
		}
	}
	// UpstreamStatusCode is a provider status, not CloudFront's HTTP status.
	return true
}

func cloudFrontEventsCompatible(a, b cloudWatchStructuredEvidence) bool {
	return a.HMACKeyID == b.HMACKeyID &&
		(a.Route == "" || b.Route == "" || a.Route == b.Route) &&
		(a.Method == "" || b.Method == "" || a.Method == b.Method) &&
		(a.Host == "" || b.Host == "" || a.Host == b.Host) &&
		(a.Status == nil || b.Status == nil || *a.Status == *b.Status) &&
		(a.EventMS == 0 || b.EventMS == 0 || absInt64(a.EventMS-b.EventMS) <= 1000) &&
		(a.RequestMS == nil || b.RequestMS == nil || absInt64(*a.RequestMS-*b.RequestMS) <= 1000)
}
