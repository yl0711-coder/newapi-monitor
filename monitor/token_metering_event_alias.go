package monitor

import "encoding/json"

// Retained overlap observations may have different source event references.
// Group them only when the same keyed ingress ID AND platform ID identify the
// same millisecond event, and every retained transport/usage operand agrees.
// Distinct retries, nodes, timestamps, unknown IDs or conflicting values remain
// separate candidates. Rows are retained; the snapshot records all event IDs.
func tokenMeteringObservationIdentity(row NginxRequestEvidence) string {
	if row.Node != cloudWatchNginxNode || row.EventMS <= 0 ||
		!nginxEvidenceHex64Pattern.MatchString(row.NginxIDHMAC) ||
		!nginxEvidenceHex64Pattern.MatchString(row.OneAPIIDHMAC) ||
		!nginxEvidenceKeyIDPattern.MatchString(row.HMACKeyID) {
		return ""
	}
	key, _ := json.Marshal([]any{row.Node, row.HMACKeyID, row.NginxIDHMAC, row.OneAPIIDHMAC,
		row.EventMS, row.Route, row.Method, row.Status, row.UpstreamStatus,
		row.UpstreamAttempts, row.UpstreamStatuses, row.RequestMS, row.UpstreamMS,
		row.UpstreamPresent, row.ConnectMS, row.HeaderMS, row.BytesSent, row.Completion,
		row.RequestLength, row.RequestLengthState, row.RequestLengthVersion})
	return string(key)
}

// A known dispute remains sticky unless every previously frozen event is
// present in the single, fully checked observation group. Missing/expired
// evidence cannot erase an ambiguity, and real conflicts never take this path.
func tokenMeteringRepeatedObservationDisputeResolved(previous TokenMeteringFact, candidates []tokenMeteringCandidate) bool {
	if previous.LinkState != "ambiguous" || previous.LinkReason != "multiple_entry_events" || len(candidates) != 1 || len(candidates[0].ObservationEventIDs) < 2 {
		return false
	}
	var old []tokenMeteringCandidate
	// A full legacy snapshot may have omitted further candidates at its cap.
	// It cannot prove that every event in the original dispute is still present.
	if json.Unmarshal([]byte(previous.EvidenceJSON), &old) != nil || len(old) < 2 || len(old) >= 20 {
		return false
	}
	ids := map[string]bool{}
	for _, id := range candidates[0].ObservationEventIDs {
		ids[id] = true
	}
	for _, c := range old {
		if !ids[c.EventID] || tokenMeteringCandidatesDisagree(c, candidates[0]) ||
			c.EventMS != candidates[0].EventMS || c.Node != candidates[0].Node || c.KeyID != candidates[0].KeyID || c.LengthVersion != candidates[0].LengthVersion {
			return false
		}
		for _, id := range c.ObservationEventIDs {
			if !ids[id] {
				return false
			}
		}
	}
	return true
}
