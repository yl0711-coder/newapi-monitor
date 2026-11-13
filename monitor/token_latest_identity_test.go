package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// Optional bounded replay of captured administrator evidence. It never opens
// a runtime database or queries production; default test runs use fixtures.
func TestTokenLatestRetainedObservationReplay(t *testing.T) {
	file := os.Getenv("TOKEN_ALIAS_REPLAY_INPUT")
	if file == "" {
		t.Skip("no captured evidence input requested")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 4<<20 {
		t.Fatal("replay byte budget")
	}
	var in struct {
		Facts []struct {
			TokenMeteringFact
			Evidence string `json:"evidence_json"`
		} `json:"facts"`
		Rows []struct {
			NginxRequestEvidence
			RawID            string `json:"oneapi_request_id"`
			Key              string `json:"hmac_key_id"`
			NginxHash        string `json:"nginx_id_hmac"`
			OneAPIHash       string `json:"oneapi_id_hmac"`
			UpstreamStatuses string `json:"upstream_statuses"`
			UpstreamPresent  int    `json:"upstream_present"`
		} `json:"rows"`
	}
	if err = json.Unmarshal(data, &in); err != nil {
		t.Fatal(err)
	}
	if len(in.Facts) == 0 || len(in.Facts) > 100 || len(in.Rows) > 300 {
		t.Fatal("replay row budget")
	}
	evidence := map[string][]NginxRequestEvidence{}
	for _, r := range in.Rows {
		v := r.NginxRequestEvidence
		v.OneAPIRequestID, v.HMACKeyID, v.NginxIDHMAC, v.OneAPIIDHMAC = r.RawID, r.Key, r.NginxHash, r.OneAPIHash
		if r.UpstreamPresent < 0 || r.UpstreamPresent > 1 {
			t.Fatal("invalid SQLite boolean in capture")
		}
		v.UpstreamStatuses, v.UpstreamPresent = r.UpstreamStatuses, r.UpstreamPresent == 1
		evidence[tokenMeteringRefKey(v.HMACKeyID, v.OneAPIIDHMAC)] = append(evidence[tokenMeteringRefKey(v.HMACKeyID, v.OneAPIIDHMAC)], v)
	}
	for _, raw := range in.Facts {
		t.Run(fmt.Sprint(raw.LogID), func(t *testing.T) {
			f := raw.TokenMeteringFact
			f.EvidenceJSON = raw.Evidence
			var refs []TokenMeteringRef
			for _, r := range in.Rows {
				if r.RawID == f.RequestID {
					refs = append(refs, TokenMeteringRef{SourceEpoch: f.SourceEpoch, LogID: f.LogID, KeyID: r.Key, Hash: r.OneAPIHash})
					break
				}
			}
			if f.LinkState != "ambiguous" || f.LinkReason != "multiple_entry_events" {
				t.Fatal("capture does not reproduce original ambiguity")
			}
			got := resolveTokenMeteringLink(f, refs, evidence, nil, false, "verified", f.CreatedAt+600)
			if got.LinkState != "unique" || got.RequestLength == nil || !strings.Contains(got.EvidenceJSON, "observation_event_ids") {
				t.Fatal("captured overlap aliases remain unresolved", got.LinkState, got.LinkReason)
			}
		})
	}
}

func TestTokenLatestContinuousQueryOverlapPersistsOneEvent(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	parser := newCloudWatchParserForTest(t, false)
	from := now.Add(-3 * time.Minute)
	at := from.Add(91*time.Second + 502*time.Millisecond)
	message := fmt.Sprintf(`{"msec":"%d.502","request_method":"POST","uri":"/v1/responses","status":200,"request_time":"43.386","upstream_status":"200","request_length":1685173,"nginx_request_id":"fixture-ingress-1","oneapi_request_id":"fixture-platform-1","request_completion":"OK"}`, at.Unix())
	client := &fakeCloudWatchLogsClient{}
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		query := aws.ToString(client.startInputs[len(client.startInputs)-1].QueryString)
		if !strings.Contains(query, "@ptr, @message, nginx_request_id") || !strings.Contains(query, "@logStream") {
			t.Fatal("continuous query lost stable identity inputs")
		}
		fields, err := cwJSONFields(message)
		if err != nil {
			t.Fatal(err)
		}
		// Match the selected Insights fields; msec is in the complete message,
		// while the timestamp is supplied by the actual query projection.
		delete(fields, "msec")
		fields["@timestamp"] = at.Format(time.RFC3339Nano)
		fields["@message"], fields["@logStream"] = message, "nginx/fixture"
		fields["@ptr"] = fmt.Sprintf("query-dependent-%d", len(client.startInputs))
		var row []cwtypes.ResultField
		for k, v := range fields {
			row = append(row, cwtypes.ResultField{Field: aws.String(k), Value: aws.String(v)})
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete, Results: [][]cwtypes.ResultField{row}}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	var id string
	for i := 0; i < 3; i++ {
		left, right := from.Add(time.Duration(i)*30*time.Second), now
		queries := 0
		evidence, err := m.queryCloudWatchNginxRange(context.Background(), parser, left, right, &queries, 0)
		if err != nil || len(evidence) != 1 || queries != 1 {
			t.Fatal("query did not produce complete evidence", len(evidence), queries, err)
		}
		if i == 0 {
			id = evidence[0].EventRef
		}
		if evidence[0].EventRef != id || parser.nginxContinuousIdentity {
			t.Fatal("overlap identity changed or shared parser was mutated")
		}
		if err := m.persistCloudWatchNginxEvidenceMode(context.Background(), evidence, left.Unix(), right.Unix(), fmt.Sprintf("fixture-overlap-%d", i), true); err != nil {
			t.Fatal(err)
		}
	}
	var rows []NginxRequestEvidence
	if err := m.nginxEvidenceDB.Find(&rows).Error; err != nil || len(rows) != 1 {
		t.Fatal("same ingress acquired duplicate persisted rows", len(rows), err)
	}
	if rows[0].EventID != id || rows[0].OneAPIRequestID != "fixture-platform-1" || rows[0].RequestLength == nil || *rows[0].RequestLength != 1685173 {
		t.Fatal("persisted identity or operands changed")
	}
}

func TestTokenLatestContinuousNginxIdentitySurvivesDifferentQueryPointers(t *testing.T) {
	ordinary := newCloudWatchParserForTest(t, false)
	continuous := *ordinary
	continuous.nginxContinuousIdentity = true
	message := `{"msec":"1794136080.502","request_method":"POST","uri":"/v1/responses","status":200,"request_time":"43.386","upstream_status":"200","upstream_response_time":"43.174","request_length":1685173,"request_id":"fixture-ingress-1","oneapi_request_id":"fixture-platform-1","request_completion":"OK"}`
	input := cloudWatchEvidenceInput{Source: cwSourceWorkerNginx, EventID: "pointer-one", LogStream: "fixture/nginx", TimestampMS: 1794136080502, Message: message}
	parse := func(p *cloudWatchEvidenceParser, in cloudWatchEvidenceInput) cloudWatchStructuredEvidence {
		t.Helper()
		v, err := p.parse(in)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	one := parse(&continuous, input)
	changed := input
	changed.EventID = "pointer-two"
	changed.Fields, _ = cwJSONFields(message)
	changed.Fields["@message"], changed.Fields["@ptr"], changed.Fields["@logStream"] = message, changed.EventID, input.LogStream
	two := parse(&continuous, changed)
	if one.EventRef != two.EventRef || one.NginxIDHMAC == "" || one.OneAPIIDHMAC == "" {
		t.Fatal("overlap changed ingress identity", one.EventRef, two.EventRef)
	}
	if parse(ordinary, input).EventRef == parse(ordinary, changed).EventRef {
		t.Fatal("shared parser default changed")
	}
	for name, mutate := range map[string]func(*cloudWatchEvidenceInput){
		"another stream": func(v *cloudWatchEvidenceInput) { v.LogStream = "fixture/other" },
		"another ingress retry": func(v *cloudWatchEvidenceInput) {
			v.Message = strings.ReplaceAll(v.Message, "fixture-ingress-1", "fixture-ingress-2")
		},
		"different millisecond": func(v *cloudWatchEvidenceInput) { v.Message = strings.ReplaceAll(v.Message, "6080.502", "6080.503") },
		"conflicting length":    func(v *cloudWatchEvidenceInput) { v.Message = strings.ReplaceAll(v.Message, "1685173", "1685174") },
	} {
		t.Run(name, func(t *testing.T) {
			v := input
			mutate(&v)
			if parse(&continuous, v).EventRef == one.EventRef {
				t.Fatal("distinct evidence collapsed")
			}
		})
	}
	missing := input
	missing.Message = strings.ReplaceAll(message, `"request_id":"fixture-ingress-1",`, "")
	other := missing
	other.EventID = "unknown-second"
	if parse(&continuous, missing).EventRef == parse(&continuous, other).EventRef {
		t.Fatal("missing ingress ID invented an identity")
	}
}

func tokenLatestAliasRows(at int64, request, hash string) []NginxRequestEvidence {
	one := NginxRequestEvidence{EventID: strings.Repeat("a", 64), EventMS: at*1000 + 502, Node: cloudWatchNginxNode,
		Route: "/v1/messages", Method: "POST", Status: 200, UpstreamStatus: 200, UpstreamAttempts: 1, UpstreamStatuses: "[200]",
		RequestMS: 43386, UpstreamMS: 43174, UpstreamPresent: true, HeaderMS: 2058, BytesSent: 448656, Completion: "complete_at_edge",
		NginxIDHMAC: strings.Repeat("c", 64), OneAPIIDHMAC: hash, OneAPIRequestID: request, HMACKeyID: "key-1",
		RequestLength: ptrTokenMetering(1685173), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1", BatchID: "overlap-one", ReceivedAt: at + 5}
	two := one
	two.EventID, two.BatchID, two.ReceivedAt = strings.Repeat("b", 64), "overlap-two", at+30
	return []NginxRequestEvidence{one, two}
}

func tokenLatestLegacyCandidates(rows []NginxRequestEvidence) string {
	var old []tokenMeteringCandidate
	for _, r := range rows {
		old = append(old, tokenMeteringCandidate{EventID: r.EventID, Node: r.Node, KeyID: r.HMACKeyID, EventMS: r.EventMS, Status: r.Status, Route: r.Route, Method: r.Method, Length: r.RequestLength, LengthState: r.RequestLengthState, LengthVersion: r.RequestLengthVersion})
	}
	return tokenDetectionJSON(old)
}

func TestTokenLatestRepeatedObservationsRepairOnlyProvenAliasDispute(t *testing.T) {
	now := int64(1794136080)
	rows := tokenLatestAliasRows(now, "fixture-platform", strings.Repeat("d", 64))
	f := TokenMeteringFact{SourceEpoch: "fixture", LogID: 1, CreatedAt: now, RequestID: "fixture-platform", LinkState: "ambiguous", LinkReason: "multiple_entry_events", EvidenceJSON: tokenLatestLegacyCandidates(rows)}
	refs := []TokenMeteringRef{{SourceEpoch: f.SourceEpoch, LogID: f.LogID, KeyID: "key-1", Hash: rows[0].OneAPIIDHMAC}}
	key := tokenMeteringRefKey(refs[0].KeyID, refs[0].Hash)
	resolve := func(f TokenMeteringFact, values []NginxRequestEvidence) TokenMeteringFact {
		return resolveTokenMeteringLink(f, refs, map[string][]NginxRequestEvidence{key: values}, nil, false, "verified", now+40)
	}
	got := resolve(f, rows)
	if got.LinkState != "unique" || got.RequestLength == nil || *got.RequestLength != 1685173 || !strings.Contains(got.EvidenceJSON, "observation_event_ids") {
		t.Fatal("legacy alias dispute did not resolve with retained provenance", got.LinkState, got.LinkReason)
	}
	reverse := resolve(f, []NginxRequestEvidence{rows[1], rows[0]})
	if got.EvidenceJSON != reverse.EvidenceJSON {
		t.Fatal("observation ordering changes frozen evidence")
	}
	if resolve(f, rows[:1]).LinkState != "ambiguous" {
		t.Fatal("missing old evidence erased known dispute")
	}
	conflict := f
	conflict.LinkState, conflict.LinkReason = "conflict", "entry_evidence_disagrees"
	if resolve(conflict, rows).LinkState != "conflict" {
		t.Fatal("known conflict erased")
	}
	for name, mutate := range map[string]func(*tokenMeteringCandidate){
		"time":           func(c *tokenMeteringCandidate) { c.EventMS++ },
		"node":           func(c *tokenMeteringCandidate) { c.Node = "previous-node" },
		"key":            func(c *tokenMeteringCandidate) { c.KeyID = "previous-key" },
		"length version": func(c *tokenMeteringCandidate) { c.LengthVersion = "previous-version" },
		"length":         func(c *tokenMeteringCandidate) { c.Length = ptrTokenMetering(123) },
		"route":          func(c *tokenMeteringCandidate) { c.Route = "/v1/responses" },
		"missing alias":  func(c *tokenMeteringCandidate) { c.ObservationEventIDs = []string{c.EventID, strings.Repeat("f", 64)} },
	} {
		t.Run("frozen "+name, func(t *testing.T) {
			changed := f
			var old []tokenMeteringCandidate
			if err := json.Unmarshal([]byte(f.EvidenceJSON), &old); err != nil {
				t.Fatal(err)
			}
			mutate(&old[0])
			changed.EvidenceJSON = tokenDetectionJSON(old)
			if got := resolve(changed, rows); got.LinkState != "ambiguous" || got.RequestPath != "" || got.RequestMethod != "" {
				t.Fatal("contradictory frozen evidence erased or supplied route fallbacks")
			}
		})
	}
	for name, mutate := range map[string]func(*NginxRequestEvidence){
		"retry":              func(r *NginxRequestEvidence) { r.NginxIDHMAC = strings.Repeat("e", 64) },
		"time":               func(r *NginxRequestEvidence) { r.EventMS++ },
		"length":             func(r *NginxRequestEvidence) { r.RequestLength = ptrTokenMetering(1685174) },
		"upstream":           func(r *NginxRequestEvidence) { r.UpstreamStatuses = "[502,200]"; r.UpstreamAttempts = 2 },
		"duration":           func(r *NginxRequestEvidence) { r.RequestMS++ },
		"node":               func(r *NginxRequestEvidence) { r.Node = "another-node" },
		"missing ingress ID": func(r *NginxRequestEvidence) { r.NginxIDHMAC = "" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := append([]NginxRequestEvidence(nil), rows...)
			mutate(&changed[1])
			if resolve(f, changed).LinkState == "unique" {
				t.Fatal("distinct/unknown evidence collapsed")
			}
		})
	}
	// Proving an identity does not remove the bounded candidate allowance.
	many := append([]NginxRequestEvidence(nil), rows...)
	for i := 0; i < 20; i++ {
		r := rows[0]
		r.EventID = fmt.Sprintf("%064x", i)
		many = append(many, r)
	}
	if got := resolve(TokenMeteringFact{CreatedAt: now, RequestID: f.RequestID}, many); got.LinkState != "ambiguous" || got.LinkReason != "candidate_budget_exceeded" {
		t.Fatal("observation alias cap was bypassed", got.LinkState, got.LinkReason)
	}
	capped := f
	capped.EvidenceJSON = tokenLatestLegacyCandidates(many[:20])
	if resolve(capped, many[:20]).LinkState != "ambiguous" {
		t.Fatal("a possibly truncated frozen dispute was treated as complete")
	}
}

func TestTokenLatestAliasRepairRequeuesDetectionIdempotently(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 701, now-5, 1, 100, 20, 0)
	rows := tokenLatestAliasRows(f.CreatedAt, f.RequestID, nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", f.RequestID))
	f.LinkState, f.LinkReason, f.EvidenceLevel = "ambiguous", "multiple_entry_events", "unavailable"
	f.RequestLength = nil
	f.LinkRevision = 2
	f.LinkNextAt = now
	f.EvidenceJSON = tokenLatestLegacyCandidates(rows)
	if err := m.tokenStore().Save(&f).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	before, _ := detectionHead(t, m, f.LogID)
	if _, _, err := m.linkTokenMeteringFactsBatchProgress(context.Background(), now+1); err != nil {
		t.Fatal(err)
	}
	afterFact := tokenMeteringFactRead(t, m, f.LogID)
	if afterFact.LinkState != "unique" || afterFact.LinkRevision != 3 {
		t.Fatal("relink did not publish corrected revision", afterFact.LinkState, afterFact.LinkRevision)
	}
	detectionRun(t, m, now+2)
	after, _ := detectionHead(t, m, f.LogID)
	if after.LinkRevision != 3 || after.Revision != before.Revision+1 {
		t.Fatal("corrected association did not update decision", before, after)
	}
	if _, _, err := m.linkTokenMeteringFactsBatchProgress(context.Background(), now+20); err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+21)
	stable, _ := detectionHead(t, m, f.LogID)
	if stable.Revision != after.Revision {
		t.Fatal("identical observation rewrote decision")
	}
	var retained int64
	m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&retained)
	if retained != 2 {
		t.Fatal("legacy evidence was removed", retained)
	}
}
