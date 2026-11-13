package monitor

import (
	"encoding/json"
	"errors"
	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"strconv"

	"gorm.io/gorm"
)

// Display values come from the exact frozen revision. Live facts may already
// have been revised or expired and must not be combined with an older decision.
// Names are optional local snapshots, explicitly distinguished from request facts.
type tokenDetectionDisplay struct {
	SourceContract          *tokenSourceContract        `json:"source_contract,omitempty"`
	DerivedOperands         map[string]metering.Field   `json:"derived_operands,omitempty"`
	Stale                   bool                        `json:"stale"`
	Assessment              *tokenDetectionAssessment   `json:"assessment"`
	RequestID               string                      `json:"request_id"`
	SourceEventID           string                      `json:"source_event_id"`
	ParserVersion           string                      `json:"parser_version"`
	AdapterVersion          string                      `json:"adapter_version"`
	SourceLogID             int64                       `json:"source_log_id"`
	SourceCreatedAt         int64                       `json:"source_created_at"`
	FirstSeenAt             int64                       `json:"first_seen_at"`
	RecordedQuota           *string                     `json:"recorded_quota"`
	RecordedTotalInput      *string                     `json:"recorded_total_input_tokens"`
	RequestMethod           string                      `json:"request_method"`
	RequestPath             string                      `json:"request_path"`
	UpstreamRequestID       string                      `json:"upstream_request_id"`
	UpstreamModel           string                      `json:"upstream_model"`
	AssociationState        string                      `json:"association_state"`
	AssociationReason       string                      `json:"association_reason"`
	DetectionState          string                      `json:"detection_state"`
	MissingFields           []tokenMeteringMissingField `json:"missing_fields"`
	Acquisition             []tokenRuleEvidenceGap      `json:"acquisition"`
	FieldSources            map[string]string           `json:"field_sources"`
	RequestLength           *string                     `json:"request_length_bytes"`
	RequestLengthSource     string                      `json:"request_length_source"`
	RequestLengthMeaning    string                      `json:"request_length_meaning"`
	RequestLengthObservedAt int64                       `json:"request_length_observed_at"`
	PricingSnapshot         string                      `json:"pricing_snapshot"`
	PricingEffectiveAt      int64                       `json:"pricing_effective_at"`
	PricingAdapter          string                      `json:"pricing_adapter"`
	RouteSnapshot           string                      `json:"route_snapshot"`
	BasePathSnapshot        string                      `json:"base_path_snapshot"`
	ModelMappingSnapshot    string                      `json:"model_mapping_snapshot"`
	CustomerNameSource      string                      `json:"customer_name_source"`
	ReferenceFrom           int64                       `json:"reference_from"`
	ReferenceThrough        int64                       `json:"reference_through"`
	ReferenceSamples        int                         `json:"reference_samples"`
	ReferenceReason         string                      `json:"reference_reason"`
	CustomerName            string                      `json:"customer_name"`
	ChannelName             string                      `json:"channel_name"`
	NamesSource             string                      `json:"names_source"`
	Upstream                string                      `json:"upstream"`
	UpstreamSource          string                      `json:"upstream_source"`
	UpstreamObservedAt      int64                       `json:"upstream_observed_at"`
	UpstreamReason          string                      `json:"upstream_reason"`
	EvidenceAvailable       bool                        `json:"evidence_available"`
	Input                   *string                     `json:"input_tokens"`
	RecordedInput           *string                     `json:"recorded_input_tokens"`
	Output                  *string                     `json:"output_tokens"`
	CacheRead               *string                     `json:"cache_read_tokens"`
	CacheWrite              *string                     `json:"cache_write_tokens"`
	ReferenceInput          *string                     `json:"reference_input_tokens"`
	InputDifference         *string                     `json:"input_difference_tokens"`
	ComparisonKind          string                      `json:"comparison_kind"`
	ComparisonReason        string                      `json:"comparison_reason"`
}

func tokenDisplayInteger(n *int64) *string {
	if n == nil || *n < 0 {
		return nil
	}
	s := strconv.FormatInt(*n, 10)
	return &s
}

func tokenDetectionPendingStateForFact(f TokenMeteringFact) string {
	switch f.LinkState {
	case "waiting", "source_missing", "":
		return "association_pending"
	case "evidence_query_failed":
		return "association_pending"
	case "ambiguous":
		return "association_ambiguous"
	case "conflict":
		return "association_conflict"
	default:
		return "detection_pending"
	}
}

// A queued request still has recorded usage. Expose its source operands without
// borrowing an older decision or replacing absent semantics with a guessed total.
func tokenDetectionDisplayFact(d *tokenDetectionDisplay, fact TokenMeteringFact) {
	if d.FieldSources == nil {
		d.FieldSources = map[string]string{}
	}
	if fact.CustomerNameSnapshot != "" {
		d.CustomerName, d.CustomerNameSource = fact.CustomerNameSnapshot, fact.CustomerNameSource
		d.NamesSource = "request_customer_snapshot;channel_local_current_snapshot"
	}
	var usage tokenMeteringParsed
	if json.Unmarshal([]byte(fact.SourceFacts), &usage) != nil {
		return
	}
	d.RecordedInput = tokenDisplayInteger(usage.Fields["prompt_tokens"].Int64())
	d.Input, d.Output = tokenDisplayInteger(usage.TotalInput), tokenDisplayInteger(usage.Fields["completion_tokens"].Int64())
	d.CacheRead, d.CacheWrite = tokenDisplayInteger(usage.Fields["cache_tokens"].Int64()), tokenDisplayInteger(usage.Fields["cache_creation_tokens"].Int64())
	for name, field := range usage.Fields {
		d.FieldSources[name] = usage.Source + ":" + field.Kind + ":" + field.State
	}
}

func tokenDetectionDisplayTransport(d *tokenDetectionDisplay, fact TokenMeteringFact, parsed ...tokenMeteringParsed) {
	d.SourceEventID, d.ParserVersion = fact.SourceEventID, fact.ParserVersion
	d.FirstSeenAt = fact.FirstSeenAt
	var usage tokenMeteringParsed
	if len(parsed) > 0 {
		usage = parsed[0]
	} else if json.Unmarshal([]byte(fact.SourceFacts), &usage) != nil {
		return
	}
	d.AdapterVersion = usage.Adapter
	d.MissingFields = tokenEffectiveMissing(fact, usage)
	d.Acquisition = tokenMissingAcquisition(fact, usage)
	d.SourceContract, d.DerivedOperands = usage.Contract, usage.ContractOperands
	for name, field := range usage.ContractOperands {
		d.FieldSources[name] = "derived_from_versioned_producer:" + field.Reason
	}
	d.RecordedQuota = tokenDisplayInteger(usage.Fields["quota"].Int64())
	d.RecordedTotalInput = tokenDisplayInteger(usage.Fields["input_tokens_total"].Int64())
	if d.RequestLength == nil {
		d.RequestLength = tokenDisplayInteger(usage.Fields["request_length"].Int64())
		if d.RequestLength != nil {
			d.RequestLengthSource = "newapi.logs.other.request_length"
			d.RequestLengthMeaning = usage.Fields["request_length_meaning"].Value
			d.RequestLengthObservedAt = tokenMeteringInt64Value(usage.Fields["request_length_observed_at"].Int64())
		}
	}
}

func (m *Monitor) tokenDetectionDisplays(tx *gorm.DB, epoch string, page []TokenDetectionCurrent) (map[int64]tokenDetectionDisplay, error) {
	out := make(map[int64]tokenDetectionDisplay, len(page))
	if len(page) == 0 {
		return out, nil
	}
	ids, users, channels := []int64{}, []int64{}, []int64{}
	for _, h := range page {
		ids = append(ids, h.LogID)
		users = append(users, h.UserID)
		channels = append(channels, h.ChannelID)
	}
	var revisions []TokenDetectionRevision
	if err := tx.Table("token_detection_revisions AS r").Select("r.*").
		Joins("JOIN token_detection_currents c ON c.source_epoch=r.source_epoch AND c.log_id=r.log_id AND c.revision=r.revision AND c.decision_hash=r.decision_hash").
		Where("r.source_epoch = ? AND r.log_id IN ?", epoch, ids).Find(&revisions).Error; err != nil {
		return nil, err
	}
	frozen := map[int64]tokenDetectionFrozen{}
	for _, rev := range revisions {
		var f tokenDetectionFrozen
		if json.Unmarshal([]byte(rev.FrozenJSON), &f) != nil || f.Version != tokenDetectionVersion || f.Input.Fact.SourceEpoch != epoch || f.Input.Fact.LogID != rev.LogID {
			return nil, errors.New("local_evidence_invalid")
		}
		frozen[rev.LogID] = f
	}
	var facts []TokenMeteringFact
	if err := tx.Select("source_epoch,log_id,created_at,request_id,source_event_id,parser_version,first_seen_at,upstream_request_id,upstream_model_name,user_id,channel_id,model,user_group,request_method,request_path,entry_event_id,entry_node,entry_route,entry_method,entry_status,entry_observed_at,missing_fields,link_state,link_reason,link_revision,revision,evidence_level,request_length,length_state,request_length_version,request_length_meaning,request_length_observed_at,pricing_snapshot,pricing_effective_at,pricing_adapter,route_snapshot,base_path_snapshot,model_mapping_snapshot,customer_name_source,customer_name_snapshot,source_facts").
		Where("source_epoch=? AND log_id IN ?", epoch, ids).Find(&facts).Error; err != nil {
		return nil, err
	}
	factByID := make(map[int64]TokenMeteringFact, len(facts))
	for _, fact := range facts {
		factByID[fact.LogID] = fact
	}
	staleByID, err := tokenDetectionStaleIdentities(tx, epoch, m.tokenDetectionConfiguredPolicyHash(), ids)
	if err != nil {
		return nil, err
	}
	userNames, channelNames := map[int64]string{}, map[int64]string{}
	channelSnapshots := map[int64]ChannelSnap{}
	// Frozen operands stay in this Token transaction. Directory labels are
	// explicitly current snapshots and come from their existing owning stores.
	userDB, channelDB := tx, tx
	if m.usageFactsDB != nil && m.usageFactsDB != m.tokenStore() {
		userDB = m.usageFactsDB.WithContext(tx.Statement.Context)
	} else if m.storeDB != nil && m.storeDB != m.tokenStore() {
		userDB = m.storeDB.WithContext(tx.Statement.Context)
	}
	if m.storeDB != nil && m.storeDB != m.tokenStore() {
		channelDB = m.storeDB.WithContext(tx.Statement.Context)
	}
	// Optional dimensions also work in logchain-only deployments; no production
	// users/channels query or per-row evidence request is performed by this page.
	if epoch == m.cfg.TokenFactsSourceEpoch && userDB.Migrator().HasTable(&UsageUserSnapshot{}) {
		var rows []UsageUserSnapshot
		if err := userDB.Select("user_id, username").Where("user_id IN ?", users).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, r := range rows {
			userNames[r.UserID] = r.Username
		}
	}
	if epoch == m.cfg.TokenFactsSourceEpoch && channelDB.Migrator().HasTable(&ChannelSnap{}) {
		var rows []ChannelSnap
		if err := channelDB.Select("id, name, base_domain, base_host, updated_at, deleted_at").Where("id IN ?", channels).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, r := range rows {
			channelNames[int64(r.ID)] = r.Name
			channelSnapshots[int64(r.ID)] = r
		}
	}
	for _, h := range page {
		d := tokenDetectionDisplay{CustomerName: userNames[h.UserID], ChannelName: channelNames[h.ChannelID], NamesSource: "local_current_snapshot", UpstreamReason: "request_upstream_not_recorded", ComparisonKind: "unavailable", ComparisonReason: "frozen_evidence_unavailable", MissingFields: []tokenMeteringMissingField{}, FieldSources: map[string]string{}}
		if snapshot, ok := channelSnapshots[h.ChannelID]; ok {
			// Reuse the channel-management domain contract. This is a dated
			// directory observation, never proof of where a historical request ran.
			d.Upstream = normalizeChannelBaseDomain(snapshot.BaseDomain)
			if d.Upstream == "" {
				d.Upstream = normalizeChannelBaseDomain(snapshot.BaseHost)
			}
			if d.Upstream != "" {
				d.UpstreamSource, d.UpstreamObservedAt = "local_current_snapshot", snapshot.UpdatedAt
				if snapshot.DeletedAt > 0 {
					d.UpstreamSource = "local_last_snapshot"
				}
			}
		}
		fact, hasFact := factByID[h.LogID]
		stale := hasFact && (staleByID[h.LogID] || h.FactRevision != fact.Revision || h.LinkRevision != fact.LinkRevision || h.DetectionState == "")
		if stale {
			tokenDetectionDisplayFact(&d, fact)
			d.Stale = true
			d.EvidenceAvailable = false
			d.RequestID, d.SourceLogID, d.SourceCreatedAt = fact.RequestID, fact.LogID, fact.CreatedAt
			d.RequestMethod, d.RequestPath = fact.RequestMethod, fact.RequestPath
			d.UpstreamRequestID, d.UpstreamModel = fact.UpstreamRequestID, fact.UpstreamModelName
			d.AssociationState, d.AssociationReason = fact.LinkState, fact.LinkReason
			d.DetectionState = tokenDetectionPendingStateForFact(fact)
			d.MissingFields = tokenMeteringDecodeMissing(fact.MissingFields)
			d.RequestLength, d.RequestLengthMeaning = tokenDisplayInteger(fact.RequestLength), fact.RequestLengthMeaning
			d.RequestLengthObservedAt, d.PricingSnapshot, d.PricingEffectiveAt, d.PricingAdapter = fact.RequestLengthObservedAt, fact.PricingSnapshot, fact.PricingEffectiveAt, fact.PricingAdapter
			d.RequestLengthSource = fact.EntryEventID
			d.RouteSnapshot, d.BasePathSnapshot, d.ModelMappingSnapshot, d.CustomerNameSource = fact.RouteSnapshot, fact.BasePathSnapshot, fact.ModelMappingSnapshot, fact.CustomerNameSource
			if d.RequestID != "" {
				d.FieldSources["request_id"] = "source_fact"
			}
			// Keep stale/pending identities in the response.  The frozen result is
			// intentionally withheld, but dropping the display entry makes the
			// caller see an empty struct and can expose the old result as if it
			// were current.  Pending work must remain visible with its exact
			// Request ID and missing fields until the next durable decision.
			tokenDetectionDisplayTransport(&d, fact)
			out[h.LogID] = d
			continue
		}
		if f, ok := frozen[h.LogID]; ok {
			assessment := tokenAssessFrozen(f)
			d.Assessment = &assessment
			d.EvidenceAvailable = true
			fact := f.Input.Fact
			if fact.CustomerNameSnapshot != "" {
				d.CustomerName, d.CustomerNameSource = fact.CustomerNameSnapshot, fact.CustomerNameSource
				d.NamesSource = "request_customer_snapshot;channel_local_current_snapshot"
			}
			d.RequestID, d.SourceLogID, d.SourceCreatedAt = fact.RequestID, fact.LogID, fact.CreatedAt
			d.RequestMethod, d.RequestPath = fact.RequestMethod, fact.RequestPath
			d.UpstreamRequestID, d.UpstreamModel = fact.UpstreamRequestID, fact.UpstreamModelName
			d.AssociationState, d.AssociationReason = fact.LinkState, fact.LinkReason
			d.DetectionState = h.DetectionState
			if d.DetectionState == "" {
				d.DetectionState = assessment.State
			}
			d.MissingFields = tokenMeteringDecodeMissing(fact.MissingFields)
			d.RequestLength, d.RequestLengthMeaning = tokenDisplayInteger(fact.RequestLength), fact.RequestLengthMeaning
			d.RequestLengthObservedAt, d.PricingSnapshot, d.PricingEffectiveAt, d.PricingAdapter = fact.RequestLengthObservedAt, fact.PricingSnapshot, fact.PricingEffectiveAt, fact.PricingAdapter
			d.RequestLengthSource = fact.EntryEventID
			d.RouteSnapshot, d.BasePathSnapshot, d.ModelMappingSnapshot, d.CustomerNameSource = fact.RouteSnapshot, fact.BasePathSnapshot, fact.ModelMappingSnapshot, fact.CustomerNameSource
			d.ReferenceFrom, d.ReferenceThrough, d.ReferenceSamples, d.ReferenceReason = f.Result.Reference.From, f.Result.Reference.Through, f.Result.Reference.SampleCount, f.Result.Reference.Reason
			for name, field := range f.Input.Usage.Fields {
				if field.State != "" {
					d.FieldSources[name] = field.Kind + ":" + field.State
				}
			}
			if d.RequestID != "" {
				d.FieldSources["request_id"] = "source_fact"
			}
			if fact.EntryEventID != "" {
				d.FieldSources["entry_event_id"] = "nginx_evidence"
			}
			if fact.PricingSnapshot != "" {
				d.FieldSources["pricing_snapshot"] = "source_fact"
			}
			u := f.Input.Usage
			d.RecordedInput = tokenDisplayInteger(u.Fields["prompt_tokens"].Int64())
			d.Input, d.Output = tokenDisplayInteger(u.TotalInput), tokenDisplayInteger(u.Fields["completion_tokens"].Int64())
			d.CacheRead, d.CacheWrite = tokenDisplayInteger(u.Fields["cache_tokens"].Int64()), tokenDisplayInteger(u.Fields["cache_creation_tokens"].Int64())
			d.ComparisonReason = f.Result.Reference.Reason
			ref := f.Result.Reference
			if ref.Status == "statistical" && !ref.Truncated && ref.SampleCount > 0 && ref.Upper != nil && *ref.Upper >= 0 && d.Input != nil {
				d.ReferenceInput = tokenDisplayInteger(ref.Upper)
				diff := strconv.FormatInt(*u.TotalInput-*ref.Upper, 10)
				d.InputDifference = &diff
				d.ComparisonKind, d.ComparisonReason = "statistical_reference", "reference_difference_not_confirmed_overcharge"
			}
			tokenDetectionDisplayTransport(&d, fact, u)
		}
		out[h.LogID] = d
	}
	return out, nil
}
