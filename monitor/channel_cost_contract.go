package monitor

// 渠道成本闭环的数据契约。该层只保存脱敏、按小时聚合的计价证据，且与现有
// 上游消费汇总、余额告警和人工倍率配置物理隔离。第一阶段只读观察，不自动改价。

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	channelCostEvidenceSemanticsVersion = 1
	channelCostSourceHMACDomain         = "newapi-monitor:channel-cost-source:v1"
	channelCostSourceKindNewAPIToken    = "newapi_token_id"
	channelCostChargeUnitNewAPIQuota    = "newapi_quota"
)

// ChannelUpstreamCostHourEvidence stores exact integer upstream charges by an
// anonymous source identity. SourceRef is an HMAC; raw token IDs and names are
// never persisted or returned by this layer.
type ChannelUpstreamCostHourEvidence struct {
	Domain               string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch         string `gorm:"primaryKey;size:64;column:account_epoch"`
	HourTs               int64  `gorm:"primaryKey;column:hour_ts"`
	SemanticsVersion     int    `gorm:"primaryKey;column:semantics_version"`
	SourceRef            string `gorm:"primaryKey;size:64;column:source_ref"`
	DimensionHash        string `gorm:"primaryKey;size:64;column:dimension_hash"`
	Provider             string `gorm:"size:24;column:provider"`
	SourceRefKind        string `gorm:"size:32;column:source_ref_kind"`
	HMACKeyID            string `gorm:"size:64;column:hmac_key_id"`
	PricingDimensionHash string `gorm:"size:64;column:pricing_dimension_hash"`
	SourceGroup          string `gorm:"size:191;column:source_group"`
	UpstreamModel        string `gorm:"size:191;column:upstream_model"`
	BillingMode          string `gorm:"size:64;column:billing_mode"`
	ChargeUnits          int64  `gorm:"column:charge_units"`
	ChargeUnit           string `gorm:"size:24;column:charge_unit"`
	ChargeUnitsPerUSD    string `gorm:"size:80;column:charge_units_per_usd"`
	Requests             int64  `gorm:"column:requests"`
	PromptTokens         int64  `gorm:"column:prompt_tokens"`
	CompletionTokens     int64  `gorm:"column:completion_tokens"`
	FirstSourceAt        int64  `gorm:"column:first_source_at"`
	LastSourceAt         int64  `gorm:"column:last_source_at"`
	ContentHash          string `gorm:"size:64;column:content_hash"`
	FetchedAt            int64  `gorm:"column:fetched_at;index"`
}

func (ChannelUpstreamCostHourEvidence) TableName() string {
	return "channel_upstream_cost_hour_evidence"
}

// ChannelUpstreamCostHourState is the per-hour control total. ReconcileDelta
// and ReconcileTolerance are retained explicitly: a tolerated Sub2 rounding
// difference must never be represented as mathematical equality.
type ChannelUpstreamCostHourState struct {
	Domain              string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch        string `gorm:"primaryKey;size:64;column:account_epoch"`
	HourTs              int64  `gorm:"primaryKey;column:hour_ts"`
	SemanticsVersion    int    `gorm:"primaryKey;column:semantics_version"`
	Provider            string `gorm:"size:24;column:provider"`
	Status              string `gorm:"size:24;column:status;index"`
	VerifiedScans       int    `gorm:"column:verified_scans"`
	ReconcileStatus     string `gorm:"size:24;column:reconcile_status;index"`
	ControlChargeUnits  int64  `gorm:"column:control_charge_units"`
	EvidenceChargeUnits int64  `gorm:"column:evidence_charge_units"`
	ChargeUnitsPerUSD   string `gorm:"size:80;column:charge_units_per_usd"`
	ReconcileDelta      int64  `gorm:"column:reconcile_delta"`
	ReconcileTolerance  int64  `gorm:"column:reconcile_tolerance"`
	Requests            int64  `gorm:"column:requests"`
	EvidenceRows        int64  `gorm:"column:evidence_rows"`
	ContentHash         string `gorm:"size:64;column:content_hash"`
	LastError           string `gorm:"size:512;column:last_error"`
	CompletedAt         int64  `gorm:"column:completed_at"`
	UpdatedAt           int64  `gorm:"column:updated_at;index"`
}

func (ChannelUpstreamCostHourState) TableName() string {
	return "channel_upstream_cost_hour_states"
}

// ChannelCostPageCheckpoint advances atomically with the existing NewAPI
// pricing page checkpoint but is stored separately so the old checkpoint
// format and semantics remain untouched.
type ChannelCostPageCheckpoint struct {
	Domain           string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch     string `gorm:"primaryKey;size:64;column:account_epoch"`
	SemanticsVersion int    `gorm:"primaryKey;column:semantics_version"`
	HourTs           int64  `gorm:"primaryKey;column:hour_ts"`
	NextPage         int    `gorm:"column:next_page"`
	Total            int64  `gorm:"column:total"`
	SourceRows       int64  `gorm:"column:source_rows"`
	AggregatesJSON   string `gorm:"type:text;column:aggregates_json"`
	UpdatedAt        int64  `gorm:"column:updated_at;index"`
}

func (ChannelCostPageCheckpoint) TableName() string { return "channel_cost_page_checkpoints" }

// ChannelCostSourceBinding is append-only. Effective intervals are half-open
// [ValidFrom, ValidTo); ValidTo=0 means open-ended. Only confirmed+allocated
// rows may attribute upstream cost to a local channel.
type ChannelCostSourceBinding struct {
	Domain         string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch   string `gorm:"primaryKey;size:64;column:account_epoch"`
	SourceRef      string `gorm:"primaryKey;size:64;column:source_ref"`
	ValidFrom      int64  `gorm:"primaryKey;column:valid_from"`
	Provider       string `gorm:"size:24;column:provider"`
	SourceRefKind  string `gorm:"size:32;column:source_ref_kind"`
	HMACKeyID      string `gorm:"size:64;column:hmac_key_id"`
	LocalChannelID int    `gorm:"column:local_channel_id;index"`
	ValidTo        int64  `gorm:"column:valid_to;index"`
	Status         string `gorm:"size:16;column:status;index"`
	AllocationMode string `gorm:"size:16;column:allocation_mode"`
	MappingSource  string `gorm:"size:24;column:mapping_source"`
	Reason         string `gorm:"size:512;column:reason"`
	CreatedBy      string `gorm:"size:128;column:created_by"`
	CreatedAt      int64  `gorm:"column:created_at"`
}

func (ChannelCostSourceBinding) TableName() string { return "channel_cost_source_bindings" }

func channelCostBindingSignature(row ChannelCostSourceBinding) string {
	parts := []string{
		row.Domain, row.AccountEpoch, row.SourceRef, strconv.FormatInt(row.ValidFrom, 10),
		row.Provider, row.SourceRefKind, row.HMACKeyID, strconv.Itoa(row.LocalChannelID),
		strconv.FormatInt(row.ValidTo, 10), row.Status, row.AllocationMode, row.MappingSource,
		row.Reason, row.CreatedBy, strconv.FormatInt(row.CreatedAt, 10),
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

// ChannelCostDirtyHour is a durable recomputation queue for late upstream
// revisions or mapping/policy changes. Claiming and publishing are idempotent.
type ChannelCostDirtyHour struct {
	Domain        string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch  string `gorm:"primaryKey;size:64;column:account_epoch"`
	HourTs        int64  `gorm:"primaryKey;column:hour_ts"`
	Reason        string `gorm:"primaryKey;size:32;column:reason"`
	Status        string `gorm:"size:16;column:status;index"`
	Attempts      int    `gorm:"column:attempts"`
	NextAttemptAt int64  `gorm:"column:next_attempt_at;index"`
	LastError     string `gorm:"size:512;column:last_error"`
	CreatedAt     int64  `gorm:"column:created_at"`
	UpdatedAt     int64  `gorm:"column:updated_at"`
}

func (ChannelCostDirtyHour) TableName() string { return "channel_cost_dirty_hours" }

type ChannelCostKeyRegistry struct {
	KeyID      string `gorm:"primaryKey;size:64;column:key_id"`
	KeyCheck   string `gorm:"size:64;column:key_check"`
	CreatedAt  int64  `gorm:"column:created_at"`
	LastUsedAt int64  `gorm:"column:last_used_at"`
}

func (ChannelCostKeyRegistry) TableName() string { return "channel_cost_key_registry" }

// ChannelPricingChangeProposal is an auditable suggestion derived from closed,
// verified hours. It never mutates ChannelFinance* by itself.
type ChannelPricingChangeProposal struct {
	ProposalKey      string `gorm:"primaryKey;size:64;column:proposal_key"`
	Domain           string `gorm:"size:253;column:domain;index"`
	AccountEpoch     string `gorm:"size:64;column:account_epoch"`
	Provider         string `gorm:"size:24;column:provider"`
	LocalChannelID   int    `gorm:"column:local_channel_id;index"`
	SourceRef        string `gorm:"size:64;column:source_ref"`
	SourceGroup      string `gorm:"size:191;column:source_group"`
	ModelName        string `gorm:"size:191;column:model_name"`
	Scope            string `gorm:"size:24;column:scope"`
	ValueKind        string `gorm:"size:32;column:value_kind"`
	OldValue         string `gorm:"size:80;column:old_value"`
	NewValue         string `gorm:"size:80;column:new_value"`
	EvidenceFromHour int64  `gorm:"column:evidence_from_hour"`
	EvidenceToHour   int64  `gorm:"column:evidence_to_hour"`
	VerifiedHours    int    `gorm:"column:verified_hours"`
	EvidenceRequests int64  `gorm:"column:evidence_requests"`
	ReconcileStatus  string `gorm:"size:24;column:reconcile_status"`
	EvidenceDigest   string `gorm:"size:64;column:evidence_digest"`
	PricingSemantics int    `gorm:"column:pricing_semantics"`
	CostSemantics    int    `gorm:"column:cost_semantics"`
	HMACKeyID        string `gorm:"size:64;column:hmac_key_id"`
	BaseVersion      int64  `gorm:"column:base_version"`
	AppliedVersion   int64  `gorm:"column:applied_version"`
	Status           string `gorm:"size:24;column:status;index"`
	Reason           string `gorm:"size:512;column:reason"`
	ResolvedBy       string `gorm:"size:128;column:resolved_by"`
	ResolvedAt       int64  `gorm:"column:resolved_at;index"`
	CreatedAt        int64  `gorm:"column:created_at;index"`
	UpdatedAt        int64  `gorm:"column:updated_at"`
}

func (ChannelPricingChangeProposal) TableName() string {
	return "channel_pricing_change_proposals"
}

type ChannelPricingProposalEvent struct {
	ID             uint64 `gorm:"primaryKey;autoIncrement"`
	ProposalKey    string `gorm:"size:64;column:proposal_key;index;uniqueIndex:idx_pricing_proposal_event_idempotency,priority:1"`
	IdempotencyKey string `gorm:"size:64;column:idempotency_key;uniqueIndex:idx_pricing_proposal_event_idempotency,priority:2"`
	RequestHash    string `gorm:"size:64;column:request_hash"`
	Event          string `gorm:"size:24;column:event"`
	FromStatus     string `gorm:"size:16;column:from_status"`
	ToStatus       string `gorm:"size:16;column:to_status"`
	Actor          string `gorm:"size:128;column:actor"`
	Reason         string `gorm:"size:512;column:reason"`
	RelatedVersion int64  `gorm:"column:related_version"`
	CreatedAt      int64  `gorm:"column:created_at;index"`
}

func (ChannelPricingProposalEvent) TableName() string {
	return "channel_pricing_proposal_events"
}

// ChannelFinanceActivation is a durable future whole-hour change. Approving a
// proposal only schedules this record; current finance rows remain unchanged
// until a due activation is atomically applied.
type ChannelFinanceActivation struct {
	ActivationID                string `gorm:"primaryKey;size:64;column:activation_id"`
	ProposalKey                 string `gorm:"size:64;column:proposal_key;index;uniqueIndex:idx_finance_activation_request,priority:1"`
	Domain                      string `gorm:"size:253;column:domain;index"`
	AccountEpoch                string `gorm:"size:64;column:account_epoch"`
	Action                      string `gorm:"size:16;column:action"`
	Status                      string `gorm:"size:16;column:status;index"`
	ExpectedBaseVersion         int64  `gorm:"column:expected_base_version"`
	ExpectedCurrentSnapshotHash string `gorm:"size:64;column:expected_current_snapshot_hash"`
	PatchJSON                   string `gorm:"type:text;column:patch_json"`
	TargetSnapshotJSON          string `gorm:"type:text;column:target_snapshot_json"`
	TargetSnapshotHash          string `gorm:"size:64;column:target_snapshot_hash"`
	EvidenceDigest              string `gorm:"size:64;column:evidence_digest"`
	HMACKeyID                   string `gorm:"size:64;column:hmac_key_id"`
	EffectiveAt                 int64  `gorm:"column:effective_at;index"`
	RequestedBy                 string `gorm:"size:128;column:requested_by"`
	RequestedAt                 int64  `gorm:"column:requested_at;index"`
	Reason                      string `gorm:"size:512;column:reason"`
	IdempotencyKey              string `gorm:"size:64;column:idempotency_key;uniqueIndex:idx_finance_activation_request,priority:2"`
	RequestHash                 string `gorm:"size:64;column:request_hash"`
	RollbackOfActivationID      string `gorm:"size:64;column:rollback_of_activation_id"`
	RollbackOfVersion           int64  `gorm:"column:rollback_of_version"`
	AppliedVersion              int64  `gorm:"column:applied_version"`
	AppliedAt                   int64  `gorm:"column:applied_at"`
	Attempts                    int    `gorm:"column:attempts"`
	NextAttemptAt               int64  `gorm:"column:next_attempt_at;index"`
	LastError                   string `gorm:"size:512;column:last_error"`
	UpdatedAt                   int64  `gorm:"column:updated_at"`
}

func (ChannelFinanceActivation) TableName() string { return "channel_finance_activations" }

type ChannelFinanceActivationSlot struct {
	Domain       string `gorm:"primaryKey;size:253;column:domain"`
	ActivationID string `gorm:"size:64;column:activation_id;uniqueIndex"`
	EffectiveAt  int64  `gorm:"column:effective_at;index"`
	CreatedAt    int64  `gorm:"column:created_at"`
}

func (ChannelFinanceActivationSlot) TableName() string {
	return "channel_finance_activation_slots"
}

type ChannelFinanceActivationEvent struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement"`
	ActivationID string `gorm:"size:64;column:activation_id;index"`
	Event        string `gorm:"size:24;column:event"`
	Actor        string `gorm:"size:128;column:actor"`
	Detail       string `gorm:"size:512;column:detail"`
	CreatedAt    int64  `gorm:"column:created_at;index"`
}

func (ChannelFinanceActivationEvent) TableName() string {
	return "channel_finance_activation_events"
}

// ChannelEconomicsHourPublication is an immutable revision of one channel's
// hourly economics. Late logs, mapping changes and finance-version changes
// append a new revision; ChannelEconomicsHourCurrent is atomically repointed.
type ChannelEconomicsHourPublication struct {
	PublicationID            string `gorm:"primaryKey;size:64;column:publication_id"`
	LogicalKey               string `gorm:"size:64;column:logical_key;uniqueIndex:idx_channel_economics_revision,priority:1"`
	Revision                 int64  `gorm:"column:revision;uniqueIndex:idx_channel_economics_revision,priority:2"`
	SupersedesPublicationID  string `gorm:"size:64;column:supersedes_publication_id"`
	Domain                   string `gorm:"size:253;column:domain;index"`
	AccountEpoch             string `gorm:"size:64;column:account_epoch;index"`
	HourTs                   int64  `gorm:"column:hour_ts;index"`
	LocalChannelID           int    `gorm:"column:local_channel_id;index"`
	SemanticsVersion         int    `gorm:"column:semantics_version"`
	FinanceVersion           int64  `gorm:"column:finance_version"`
	LocalRequests            int64  `gorm:"column:local_requests"`
	UpstreamRequests         int64  `gorm:"column:upstream_requests"`
	LocalConsumeQuota        int64  `gorm:"column:local_consume_quota"`
	LocalRefundRecords       int64  `gorm:"column:local_refund_records"`
	LocalRefundQuota         int64  `gorm:"column:local_refund_quota"`
	LocalNetQuota            int64  `gorm:"column:local_net_quota"`
	UnallocatedRefundRecords int64  `gorm:"column:unallocated_refund_records"`
	UnallocatedRefundQuota   int64  `gorm:"column:unallocated_refund_quota"`
	LocalFactStatus          string `gorm:"size:24;column:local_fact_status"`
	RevenueMicroUSD          int64  `gorm:"column:revenue_micro_usd"`
	UpstreamChargeUnits      int64  `gorm:"column:upstream_charge_units"`
	UpstreamChargeUnit       string `gorm:"size:24;column:upstream_charge_unit"`
	ChargeUnitsPerUSD        string `gorm:"size:80;column:charge_units_per_usd"`
	UpstreamCostMicroUSD     int64  `gorm:"column:upstream_cost_micro_usd"`
	CorrectedCostMicroUSD    int64  `gorm:"column:corrected_cost_micro_usd"`
	ProfitMicroUSD           int64  `gorm:"column:profit_micro_usd"`
	CorrectedCostKnown       bool   `gorm:"column:corrected_cost_known"`
	ProfitKnown              bool   `gorm:"column:profit_known"`
	CoverageStatus           string `gorm:"size:24;column:coverage_status;index"`
	ReconcileStatus          string `gorm:"size:24;column:reconcile_status"`
	MappingHash              string `gorm:"size:64;column:mapping_hash"`
	SourceHash               string `gorm:"size:64;column:source_hash"`
	PublicationReason        string `gorm:"size:32;column:publication_reason"`
	PublishedAt              int64  `gorm:"column:published_at;index"`
}

func (ChannelEconomicsHourPublication) TableName() string {
	return "channel_economics_hour_publications"
}

type ChannelEconomicsHourCurrent struct {
	LogicalKey    string `gorm:"primaryKey;size:64;column:logical_key"`
	PublicationID string `gorm:"size:64;column:publication_id;uniqueIndex"`
	Revision      int64  `gorm:"column:revision"`
	UpdatedAt     int64  `gorm:"column:updated_at;index"`
}

func (ChannelEconomicsHourCurrent) TableName() string { return "channel_economics_hour_current" }

// ChannelEconomicsHourManifestPublication is the immutable, authoritative
// publication head for one domain hour. Unlike a child logical key, its
// business key deliberately excludes account_epoch: credential rotation must
// replace the authoritative epoch instead of making two epochs current.
// Empty hours are published with RowCount=0, which lets reports distinguish a
// verified zero from a missing publication.
type ChannelEconomicsHourManifestPublication struct {
	ManifestID           string `gorm:"primaryKey;size:64;column:manifest_id"`
	LogicalKey           string `gorm:"size:64;column:logical_key;uniqueIndex:idx_economics_manifest_revision,priority:1"`
	Revision             int64  `gorm:"column:revision;uniqueIndex:idx_economics_manifest_revision,priority:2"`
	SupersedesManifestID string `gorm:"size:64;column:supersedes_manifest_id"`
	Domain               string `gorm:"size:253;column:domain;index:idx_economics_manifest_domain_hour,priority:1"`
	HourTs               int64  `gorm:"column:hour_ts;index:idx_economics_manifest_domain_hour,priority:2"`
	SemanticsVersion     int    `gorm:"column:semantics_version;index:idx_economics_manifest_domain_hour,priority:3"`
	AuthoritativeEpoch   string `gorm:"size:64;column:authoritative_epoch"`
	RowCount             int64  `gorm:"column:row_count"`
	PublicationSetHash   string `gorm:"size:64;column:publication_set_hash"`
	CostSourceHash       string `gorm:"size:64;column:cost_source_hash"`
	LocalFactStatus      string `gorm:"size:24;column:local_fact_status"`
	FinanceVersion       int64  `gorm:"column:finance_version"`
	CoverageStatus       string `gorm:"size:32;column:coverage_status;index"`
	ProfitKnown          bool   `gorm:"column:profit_known"`
	SourceHash           string `gorm:"size:64;column:source_hash"`
	PublicationReason    string `gorm:"size:32;column:publication_reason"`
	PublishedAt          int64  `gorm:"column:published_at;index"`
}

func (ChannelEconomicsHourManifestPublication) TableName() string {
	return "channel_economics_hour_manifest_publications"
}

type ChannelEconomicsHourManifestCurrent struct {
	Domain           string `gorm:"primaryKey;size:253;column:domain"`
	HourTs           int64  `gorm:"primaryKey;column:hour_ts"`
	SemanticsVersion int    `gorm:"primaryKey;column:semantics_version"`
	ManifestID       string `gorm:"size:64;column:manifest_id;uniqueIndex"`
	Revision         int64  `gorm:"column:revision"`
	UpdatedAt        int64  `gorm:"column:updated_at;index"`
}

func (ChannelEconomicsHourManifestCurrent) TableName() string {
	return "channel_economics_hour_manifest_current"
}

// ChannelEconomicsGlobalHourFact stores facts that are known for the whole
// local platform hour but cannot be attributed to any upstream domain. Such
// amounts must never be copied into every domain/channel publication because a
// cross-domain report would multiply them. Domain publications only use the
// presence/value in their source hash to fail profit closed.
type ChannelEconomicsGlobalHourFact struct {
	HourTs                   int64  `gorm:"primaryKey;column:hour_ts"`
	SemanticsVersion         int    `gorm:"primaryKey;column:semantics_version"`
	UnallocatedRefundRecords int64  `gorm:"column:unallocated_refund_records"`
	UnallocatedRefundQuota   int64  `gorm:"column:unallocated_refund_quota"`
	SourceHash               string `gorm:"size:64;column:source_hash"`
	UpdatedAt                int64  `gorm:"column:updated_at;index"`
}

func (ChannelEconomicsGlobalHourFact) TableName() string {
	return "channel_economics_global_hour_facts"
}

type ChannelEconomicsDirtyHour struct {
	Domain        string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch  string `gorm:"primaryKey;size:64;column:account_epoch"`
	HourTs        int64  `gorm:"primaryKey;column:hour_ts"`
	Reason        string `gorm:"size:32;column:reason"`
	Generation    int64  `gorm:"column:generation"`
	Status        string `gorm:"size:16;column:status;index"`
	Attempts      int    `gorm:"column:attempts"`
	NextAttemptAt int64  `gorm:"column:next_attempt_at;index"`
	LastError     string `gorm:"size:512;column:last_error"`
	CreatedAt     int64  `gorm:"column:created_at"`
	UpdatedAt     int64  `gorm:"column:updated_at"`
}

func (ChannelEconomicsDirtyHour) TableName() string { return "channel_economics_dirty_hours" }

func channelCostDomainAllowed(domains []string, domain string) bool {
	return pricingLedgerDomainAllowed(domains, domain)
}

func channelCostSourceRef(key []byte, provider, accountEpoch, sourceKind, rawID string) (string, error) {
	if len(key) < 32 {
		return "", errors.New("渠道成本来源 HMAC 密钥不足 32 字节")
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	accountEpoch = strings.TrimSpace(accountEpoch)
	sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
	rawID = strings.TrimSpace(rawID)
	if provider == "" || len(accountEpoch) != 64 || sourceKind == "" || rawID == "" {
		return "", errors.New("渠道成本来源身份字段不完整")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(strings.Join([]string{
		channelCostSourceHMACDomain, provider, accountEpoch, sourceKind, rawID,
	}, "\x00")))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func channelCostKeyCheck(key []byte) (string, error) {
	if len(key) < 32 {
		return "", errors.New("渠道成本来源 HMAC 密钥不足 32 字节")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(channelCostSourceHMACDomain + "\x00key-check"))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func validateChannelCostAccountIdentity(account ChannelUpstreamAccount) error {
	if strings.TrimSpace(account.Domain) == "" || strings.TrimSpace(account.Provider) == "" || strings.TrimSpace(account.BaseURL) == "" || account.UserID <= 0 {
		return errors.New("渠道成本上游账户身份字段不完整")
	}
	return nil
}

func (m *Monitor) validateChannelCostKeyRegistry() error {
	if !m.cfg.ChannelCostClosureEnabled {
		return nil
	}
	check, err := channelCostKeyCheck([]byte(m.cfg.ChannelCostHMACKey))
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		var rows []ChannelCostKeyRegistry
		if err := tx.Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 {
			return tx.Create(&ChannelCostKeyRegistry{KeyID: m.cfg.ChannelCostHMACKeyID, KeyCheck: check, CreatedAt: now, LastUsedAt: now}).Error
		}
		if len(rows) != 1 || rows[0].KeyID != m.cfg.ChannelCostHMACKeyID || !hmac.Equal([]byte(rows[0].KeyCheck), []byte(check)) {
			return errors.New("渠道成本 HMAC 密钥或 key ID 与已有来源历史不一致；首版禁止静默轮换")
		}
		return tx.Model(&ChannelCostKeyRegistry{}).Where("key_id = ?", rows[0].KeyID).Update("last_used_at", now).Error
	})
}

func channelCostDimensionHash(sourceRef, pricingHash, sourceGroup, upstreamModel, billingMode, chargeUnit string) string {
	parts := []string{sourceRef, pricingHash, sourceGroup, upstreamModel, billingMode, chargeUnit}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

// buildNewAPICostHourEvidence consumes the already decoded page items. It does
// no I/O, performs no price multiplication and preserves NewAPI quota exactly.
func buildNewAPICostHourEvidence(account ChannelUpstreamAccount, items []newAPIPricingUsageItem, hourTs, fetchedAt int64, hmacKey []byte, hmacKeyID string) ([]ChannelUpstreamCostHourEvidence, ChannelUpstreamCostHourState, error) {
	return buildNewAPICostHourEvidenceWithUnit(account, items, hourTs, fetchedAt, hmacKey, hmacKeyID, "")
}

func buildNewAPICostHourEvidenceWithUnit(account ChannelUpstreamAccount, items []newAPIPricingUsageItem, hourTs, fetchedAt int64, hmacKey []byte, hmacKeyID, frozenUnitsPerUSD string) ([]ChannelUpstreamCostHourEvidence, ChannelUpstreamCostHourState, error) {
	if hourTs < 0 || hourTs%3600 != 0 {
		return nil, ChannelUpstreamCostHourState{}, errors.New("渠道成本证据只允许整小时")
	}
	if err := validateChannelCostAccountIdentity(account); err != nil {
		return nil, ChannelUpstreamCostHourState{}, err
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	if len(epoch) != 64 {
		return nil, ChannelUpstreamCostHourState{}, errors.New("上游账户代际无效")
	}
	hmacKeyID = strings.TrimSpace(hmacKeyID)
	if hmacKeyID == "" || len(hmacKeyID) > 64 {
		return nil, ChannelUpstreamCostHourState{}, errors.New("渠道成本 HMAC key ID 无效")
	}
	state := ChannelUpstreamCostHourState{
		Domain: account.Domain, AccountEpoch: epoch, HourTs: hourTs,
		SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider,
		Status: "observed", ReconcileStatus: "pending", CompletedAt: fetchedAt, UpdatedAt: fetchedAt,
	}
	unitsPerUSDCanonical := strings.TrimSpace(frozenUnitsPerUSD)
	if unitsPerUSDCanonical != "" {
		if !validPositiveCanonicalRat(unitsPerUSDCanonical) {
			return nil, ChannelUpstreamCostHourState{}, errors.New("渠道成本历史计费单位无效")
		}
	} else {
		unitsPerUSD := account.BalanceUnit
		if unitsPerUSD <= 0 {
			unitsPerUSD = quotaPerUSD
		}
		unitRat, err := nonnegativeFloatRat(unitsPerUSD)
		if err != nil || unitRat.Sign() <= 0 {
			return nil, ChannelUpstreamCostHourState{}, errors.New("渠道成本计费单位无效")
		}
		unitsPerUSDCanonical = unitRat.RatString()
	}
	state.ChargeUnitsPerUSD = unitsPerUSDCanonical
	byDimension := make(map[string]*ChannelUpstreamCostHourEvidence)
	for _, item := range items {
		if item.CreatedAt < hourTs || item.CreatedAt >= hourTs+3600 {
			continue
		}
		if !item.QuotaExactKnown || !item.TokensExactKnown || item.QuotaExact < 0 || item.PromptTokens < 0 || item.CompletionTokens < 0 {
			return nil, ChannelUpstreamCostHourState{}, errors.New("NewAPI 渠道成本字段不是非负精确整数")
		}
		if item.Pricing.TokenID <= 0 {
			return nil, ChannelUpstreamCostHourState{}, errors.New("NewAPI 计价日志缺少可归属 token_id")
		}
		sourceRef, err := channelCostSourceRef(hmacKey, account.Provider, epoch, channelCostSourceKindNewAPIToken, strconv.FormatInt(item.Pricing.TokenID, 10))
		if err != nil {
			return nil, ChannelUpstreamCostHourState{}, err
		}
		pricingHash := pricingDimensionHash(item.Pricing)
		dimensionHash := channelCostDimensionHash(sourceRef, pricingHash, item.Pricing.GroupName, item.Pricing.ModelName, item.Pricing.BillingMode, channelCostChargeUnitNewAPIQuota)
		row := byDimension[dimensionHash]
		if row == nil {
			row = &ChannelUpstreamCostHourEvidence{
				Domain: account.Domain, AccountEpoch: epoch, HourTs: hourTs,
				SemanticsVersion: channelCostEvidenceSemanticsVersion,
				SourceRef:        sourceRef, DimensionHash: dimensionHash, Provider: account.Provider,
				SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: hmacKeyID, PricingDimensionHash: pricingHash,
				SourceGroup: item.Pricing.GroupName, UpstreamModel: item.Pricing.ModelName,
				BillingMode: item.Pricing.BillingMode, ChargeUnit: channelCostChargeUnitNewAPIQuota, ChargeUnitsPerUSD: unitsPerUSDCanonical,
				FirstSourceAt: item.CreatedAt, LastSourceAt: item.CreatedAt, FetchedAt: fetchedAt,
			}
			byDimension[dimensionHash] = row
		}
		if item.QuotaExact > math.MaxInt64-row.ChargeUnits || item.QuotaExact > math.MaxInt64-state.EvidenceChargeUnits {
			return nil, ChannelUpstreamCostHourState{}, errors.New("NewAPI 渠道成本小时金额溢出")
		}
		if item.PromptTokens > math.MaxInt64-row.PromptTokens || item.CompletionTokens > math.MaxInt64-row.CompletionTokens {
			return nil, ChannelUpstreamCostHourState{}, errors.New("NewAPI 渠道成本小时 token 溢出")
		}
		row.ChargeUnits += item.QuotaExact
		row.Requests++
		row.PromptTokens += item.PromptTokens
		row.CompletionTokens += item.CompletionTokens
		state.EvidenceChargeUnits += item.QuotaExact
		state.Requests++
		if item.CreatedAt < row.FirstSourceAt {
			row.FirstSourceAt = item.CreatedAt
		}
		if item.CreatedAt > row.LastSourceAt {
			row.LastSourceAt = item.CreatedAt
		}
	}
	rows := make([]ChannelUpstreamCostHourEvidence, 0, len(byDimension))
	for _, row := range byDimension {
		row.ContentHash = channelCostEvidenceRowHash(*row)
		rows = append(rows, *row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].DimensionHash < rows[j].DimensionHash })
	state.EvidenceRows = int64(len(rows))
	state.ContentHash = channelCostEvidenceContentHash(rows)
	return rows, state, nil
}

func channelCostEvidenceRowHash(row ChannelUpstreamCostHourEvidence) string {
	parts := []string{
		row.Domain, row.AccountEpoch, strconv.FormatInt(row.HourTs, 10), strconv.Itoa(row.SemanticsVersion),
		row.SourceRef, row.DimensionHash, row.Provider, row.SourceRefKind, row.HMACKeyID, row.PricingDimensionHash,
		row.SourceGroup, row.UpstreamModel, row.BillingMode, row.ChargeUnit, row.ChargeUnitsPerUSD,
		strconv.FormatInt(row.ChargeUnits, 10), strconv.FormatInt(row.Requests, 10),
		strconv.FormatInt(row.PromptTokens, 10), strconv.FormatInt(row.CompletionTokens, 10),
		strconv.FormatInt(row.FirstSourceAt, 10), strconv.FormatInt(row.LastSourceAt, 10),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func channelCostEvidenceContentHash(rows []ChannelUpstreamCostHourEvidence) string {
	parts := make([]string, len(rows))
	for i := range rows {
		parts[i] = rows[i].ContentHash
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func channelCostEvidenceMapToSlice(rows map[string]*ChannelUpstreamCostHourEvidence) []ChannelUpstreamCostHourEvidence {
	out := make([]ChannelUpstreamCostHourEvidence, 0, len(rows))
	for _, row := range rows {
		copyRow := *row
		copyRow.ContentHash = channelCostEvidenceRowHash(copyRow)
		out = append(out, copyRow)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DimensionHash < out[j].DimensionHash })
	return out
}

func mergeChannelCostEvidenceRows(target map[string]*ChannelUpstreamCostHourEvidence, rows []ChannelUpstreamCostHourEvidence) error {
	for _, incoming := range rows {
		existing := target[incoming.DimensionHash]
		if existing == nil {
			copyRow := incoming
			target[incoming.DimensionHash] = &copyRow
			continue
		}
		if existing.Domain != incoming.Domain || existing.AccountEpoch != incoming.AccountEpoch || existing.HourTs != incoming.HourTs ||
			existing.SourceRef != incoming.SourceRef || existing.Provider != incoming.Provider || existing.SourceRefKind != incoming.SourceRefKind ||
			existing.HMACKeyID != incoming.HMACKeyID || existing.PricingDimensionHash != incoming.PricingDimensionHash || existing.SourceGroup != incoming.SourceGroup || existing.UpstreamModel != incoming.UpstreamModel ||
			existing.BillingMode != incoming.BillingMode || existing.ChargeUnit != incoming.ChargeUnit || existing.ChargeUnitsPerUSD != incoming.ChargeUnitsPerUSD {
			return errors.New("渠道成本证据维度摘要冲突")
		}
		for _, pair := range []struct{ target, value *int64 }{
			{&existing.ChargeUnits, &incoming.ChargeUnits}, {&existing.Requests, &incoming.Requests},
			{&existing.PromptTokens, &incoming.PromptTokens}, {&existing.CompletionTokens, &incoming.CompletionTokens},
		} {
			if *pair.value < 0 || *pair.value > math.MaxInt64-*pair.target {
				return errors.New("渠道成本证据聚合溢出")
			}
			*pair.target += *pair.value
		}
		if incoming.FirstSourceAt < existing.FirstSourceAt {
			existing.FirstSourceAt = incoming.FirstSourceAt
		}
		if incoming.LastSourceAt > existing.LastSourceAt {
			existing.LastSourceAt = incoming.LastSourceAt
		}
	}
	return nil
}

func decodeChannelCostCheckpoint(checkpoint ChannelCostPageCheckpoint, expectedProvider, expectedKeyID string) (map[string]*ChannelUpstreamCostHourEvidence, error) {
	if checkpoint.Domain == "" || len(checkpoint.Domain) > 253 || checkpoint.AccountEpoch == "" || !validSHA256Hex(checkpoint.AccountEpoch) ||
		checkpoint.HourTs < 0 || checkpoint.HourTs%3600 != 0 || checkpoint.NextPage < 2 || checkpoint.Total < 0 || checkpoint.SourceRows < 0 || checkpoint.SourceRows > checkpoint.Total {
		return nil, errors.New("渠道成本断点控制字段无效")
	}
	expectedProvider = strings.TrimSpace(expectedProvider)
	expectedKeyID = strings.TrimSpace(expectedKeyID)
	if expectedProvider != upstreamProviderNewAPI || expectedKeyID == "" {
		return nil, errors.New("渠道成本断点验证上下文无效")
	}
	if len(checkpoint.AggregatesJSON) > upstreamPricingMaxCheckpointBytes {
		return nil, errors.New("渠道成本断点超过安全大小")
	}
	var rows []ChannelUpstreamCostHourEvidence
	if err := json.Unmarshal([]byte(checkpoint.AggregatesJSON), &rows); err != nil {
		return nil, fmt.Errorf("渠道成本断点损坏: %w", err)
	}
	if len(rows) > upstreamPricingMaxCheckpointDimensions {
		return nil, errors.New("渠道成本断点维度超过安全上限")
	}
	out := make(map[string]*ChannelUpstreamCostHourEvidence, len(rows))
	var sourceRows int64
	for _, row := range rows {
		if row.Domain != checkpoint.Domain || row.AccountEpoch != checkpoint.AccountEpoch || row.HourTs != checkpoint.HourTs ||
			row.SemanticsVersion != channelCostEvidenceSemanticsVersion || !validSHA256Hex(row.SourceRef) || !validSHA256Hex(row.DimensionHash) || !validSHA256Hex(row.PricingDimensionHash) ||
			row.Provider != expectedProvider || row.SourceRefKind != channelCostSourceKindNewAPIToken || row.HMACKeyID != expectedKeyID ||
			row.ChargeUnit != channelCostChargeUnitNewAPIQuota || !validPositiveCanonicalRat(row.ChargeUnitsPerUSD) || row.Requests <= 0 || row.ChargeUnits < 0 || row.PromptTokens < 0 || row.CompletionTokens < 0 ||
			!validCostDimensionText(row.SourceGroup, 191) || !validCostDimensionText(row.UpstreamModel, 191) || !validCostDimensionText(row.BillingMode, 64) ||
			row.FirstSourceAt < checkpoint.HourTs || row.LastSourceAt < row.FirstSourceAt || row.LastSourceAt >= checkpoint.HourTs+3600 || row.FetchedAt < 0 {
			return nil, errors.New("渠道成本断点包含无效证据")
		}
		expectedDimensionHash := channelCostDimensionHash(row.SourceRef, row.PricingDimensionHash, row.SourceGroup, row.UpstreamModel, row.BillingMode, row.ChargeUnit)
		if row.DimensionHash != expectedDimensionHash || row.ContentHash != channelCostEvidenceRowHash(row) {
			return nil, errors.New("渠道成本断点证据摘要校验失败")
		}
		if _, exists := out[row.DimensionHash]; exists {
			return nil, errors.New("渠道成本断点维度重复")
		}
		if row.Requests > math.MaxInt64-sourceRows {
			return nil, errors.New("渠道成本断点请求数溢出")
		}
		sourceRows += row.Requests
		copyRow := row
		out[row.DimensionHash] = &copyRow
	}
	if sourceRows != checkpoint.SourceRows {
		return nil, errors.New("渠道成本断点行数不一致")
	}
	return out, nil
}

func validSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validCostDimensionText(value string, maxBytes int) bool {
	return utf8.ValidString(value) && len(value) <= maxBytes
}

func validPositiveCanonicalRat(value string) bool {
	rat, ok := new(big.Rat).SetString(strings.TrimSpace(value))
	return ok && rat.Sign() > 0 && rat.RatString() == value && len(value) <= 80
}

func (m *Monitor) channelCostEnabledFor(account ChannelUpstreamAccount) bool {
	return m.cfg.ChannelCostClosureEnabled && account.Provider == upstreamProviderNewAPI && channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, account.Domain)
}

func (m *Monitor) deleteChannelCostCheckpoint(ctx context.Context, account ChannelUpstreamAccount, hourTs int64) {
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), channelCostEvidenceSemanticsVersion, hourTs).Delete(&ChannelCostPageCheckpoint{}).Error; err != nil {
		slog.Warn("清理不可续传的渠道成本影子断点失败", "domain", account.Domain, "hour", hourTs, "err", err)
	}
}

// saveNewAPIPricingAndCostCheckpoint commits both page cursors atomically.
// The old pricing checkpoint schema remains unchanged; when closure is off this
// method is not used and existing behavior is byte-for-byte preserved.
func (m *Monitor) saveNewAPIPricingAndCostCheckpoint(ctx context.Context, pricing *ChannelUpstreamPricingPageCheckpoint, pricingRows map[string]*ChannelUpstreamPricingHourEvidence, costRows map[string]*ChannelUpstreamCostHourEvidence) error {
	if !m.channelCostEnabledFor(ChannelUpstreamAccount{Domain: pricing.Domain, Provider: pricing.Provider}) {
		return m.savePricingPageCheckpoint(ctx, pricing, pricingRows)
	}
	if len(costRows) > upstreamPricingMaxCheckpointDimensions {
		return errors.New("渠道成本断点维度超过安全上限")
	}
	costSlice := channelCostEvidenceMapToSlice(costRows)
	encoded, err := json.Marshal(costSlice)
	if err != nil {
		return err
	}
	if len(encoded) > upstreamPricingMaxCheckpointBytes {
		return errors.New("渠道成本断点超过安全大小")
	}
	pricingEncoded, err := encodePricingCheckpointEvidence(pricingRows)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	pricing.AggregatesJSON, pricing.UpdatedAt = pricingEncoded, now
	cost := ChannelCostPageCheckpoint{
		Domain: pricing.Domain, AccountEpoch: pricing.AccountEpoch, SemanticsVersion: channelCostEvidenceSemanticsVersion,
		HourTs: pricing.HourTs, NextPage: pricing.NextPage, Total: pricing.Total, SourceRows: pricing.SourceRows,
		AggregatesJSON: string(encoded), UpdatedAt: now,
	}
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(pricing).Error; err != nil {
			return err
		}
		return tx.Save(&cost).Error
	})
}

// saveNewAPIPricingCheckpointWithCostFallback preserves the established
// pricing ledger when only the additive cost shadow path fails. The failed
// cost hour is durably queued and can be reread independently later.
func (m *Monitor) saveNewAPIPricingCheckpointWithCostFallback(ctx context.Context, account ChannelUpstreamAccount, pricing *ChannelUpstreamPricingPageCheckpoint, pricingRows map[string]*ChannelUpstreamPricingHourEvidence, costRows map[string]*ChannelUpstreamCostHourEvidence) error {
	if !m.channelCostEnabledFor(account) {
		return m.savePricingPageCheckpoint(ctx, pricing, pricingRows)
	}
	if err := m.saveNewAPIPricingAndCostCheckpoint(ctx, pricing, pricingRows, costRows); err != nil {
		if dirtyErr := m.markChannelCostDirtyHour(ctx, account, pricing.HourTs, "checkpoint_failure", err); dirtyErr != nil {
			slog.Warn("渠道成本断点失败且恢复任务记录失败", "domain", account.Domain, "hour", pricing.HourTs, "err", err, "dirty_err", dirtyErr)
		}
		// The combined write is transactional, so the old checkpoint was not
		// advanced. Save it alone and allow the established ledger to continue.
		if pricingErr := m.savePricingPageCheckpoint(ctx, pricing, pricingRows); pricingErr != nil {
			return fmt.Errorf("渠道成本断点失败: %w; 原计价断点降级保存也失败: %w", err, pricingErr)
		}
		slog.Warn("渠道成本断点失败，原计价断点已降级保存", "domain", account.Domain, "hour", pricing.HourTs, "err", err)
	}
	return nil
}

func (m *Monitor) loadChannelCostCheckpoint(ctx context.Context, account ChannelUpstreamAccount, pricing ChannelUpstreamPricingPageCheckpoint) (map[string]*ChannelUpstreamCostHourEvidence, error) {
	var checkpoint ChannelCostPageCheckpoint
	err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?", account.Domain, pricing.AccountEpoch, channelCostEvidenceSemanticsVersion, pricing.HourTs).First(&checkpoint).Error
	if err != nil {
		return nil, err
	}
	if checkpoint.NextPage != pricing.NextPage || checkpoint.Total != pricing.Total || checkpoint.SourceRows != pricing.SourceRows {
		return nil, errors.New("渠道成本断点与计价断点不同步")
	}
	return decodeChannelCostCheckpoint(checkpoint, account.Provider, m.cfg.ChannelCostHMACKeyID)
}

func (m *Monitor) publishChannelCostHourFromCheckpoint(ctx context.Context, account ChannelUpstreamAccount, pricingState ChannelUpstreamPricingHourState, now int64) error {
	if !m.channelCostEnabledFor(account) {
		return nil
	}
	var checkpoint ChannelCostPageCheckpoint
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?", account.Domain, pricingState.AccountEpoch, channelCostEvidenceSemanticsVersion, pricingState.HourTs).First(&checkpoint).Error; err != nil {
		return err
	}
	if checkpoint.NextPage <= 1 || checkpoint.SourceRows != checkpoint.Total {
		return errors.New("渠道成本小时断点尚未完整")
	}
	evidenceMap, err := decodeChannelCostCheckpoint(checkpoint, account.Provider, m.cfg.ChannelCostHMACKeyID)
	if err != nil {
		return err
	}
	rows := channelCostEvidenceMapToSlice(evidenceMap)
	state := ChannelUpstreamCostHourState{
		Domain: account.Domain, AccountEpoch: pricingState.AccountEpoch, HourTs: pricingState.HourTs,
		SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider,
		Status: "observed", ControlChargeUnits: pricingState.FinalQuota,
		EvidenceRows: int64(len(rows)), ContentHash: channelCostEvidenceContentHash(rows), CompletedAt: now, UpdatedAt: now,
	}
	for _, row := range rows {
		if !validPositiveCanonicalRat(row.ChargeUnitsPerUSD) {
			return errors.New("渠道成本小时缺少可复现的计费单位")
		}
		if state.ChargeUnitsPerUSD == "" {
			state.ChargeUnitsPerUSD = row.ChargeUnitsPerUSD
		} else if state.ChargeUnitsPerUSD != row.ChargeUnitsPerUSD {
			return errors.New("渠道成本小时包含不一致的计费单位")
		}
		if row.ChargeUnits > math.MaxInt64-state.EvidenceChargeUnits || row.Requests > math.MaxInt64-state.Requests {
			return errors.New("渠道成本小时控制总额溢出")
		}
		state.EvidenceChargeUnits += row.ChargeUnits
		state.Requests += row.Requests
	}
	state.ReconcileDelta = state.EvidenceChargeUnits - state.ControlChargeUnits
	if state.ReconcileDelta == 0 {
		state.ReconcileStatus = "matched"
	} else {
		state.ReconcileStatus = "mismatch"
	}
	eligibleForVerification := state.ReconcileStatus == "matched" && pricingState.Status == "verified" && pricingReconcileAccepted(account.Provider, pricingState.ReconcileStatus)
	publishErr := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var previous ChannelUpstreamCostHourState
		lookup := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", state.Domain, state.AccountEpoch, state.HourTs, state.SemanticsVersion).First(&previous)
		if lookup.Error != nil && !errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
			return lookup.Error
		}
		state.VerifiedScans = 1
		if eligibleForVerification && lookup.Error == nil && previous.ContentHash == state.ContentHash &&
			previous.ControlChargeUnits == state.ControlChargeUnits && previous.EvidenceChargeUnits == state.EvidenceChargeUnits &&
			previous.Requests == state.Requests && previous.EvidenceRows == state.EvidenceRows && previous.ReconcileStatus == state.ReconcileStatus &&
			previous.ChargeUnitsPerUSD == state.ChargeUnitsPerUSD {
			state.VerifiedScans = previous.VerifiedScans + 1
		}
		state.Status = "observed"
		if eligibleForVerification && state.VerifiedScans >= 2 {
			state.Status = "verified"
		}
		if err := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", state.Domain, state.AccountEpoch, state.HourTs, state.SemanticsVersion).Delete(&ChannelUpstreamCostHourEvidence{}).Error; err != nil {
			return err
		}
		for i := range rows {
			if err := tx.Create(&rows[i]).Error; err != nil {
				return err
			}
		}
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		// The verified cost state and its downstream recomputation task are one
		// atomic commit. A crash or SQLite busy error must never leave a verified
		// hour without a discoverable economics publication.
		if state.Status == "verified" {
			dirty := ChannelEconomicsDirtyHour{
				Domain: state.Domain, AccountEpoch: state.AccountEpoch, HourTs: state.HourTs,
				Reason: "cost_verified", Generation: 1, Status: "pending", NextAttemptAt: now,
				CreatedAt: now, UpdatedAt: now,
			}
			if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
				return err
			}
		}
		return tx.Delete(&checkpoint).Error
	})
	if publishErr != nil {
		return publishErr
	}
	if state.Status == "verified" {
		if err := m.clearChannelCostDirtyHour(ctx, account, state.HourTs); err != nil {
			slog.Warn("渠道成本小时已核验但清理恢复任务失败", "domain", account.Domain, "hour", state.HourTs, "err", err)
		}
	}
	if state.Status == "verified" {
		if err := m.detectNewAPIChannelPricingProposals(ctx, account, state.HourTs, now); err != nil {
			// Proposal generation is advisory. A failure must not downgrade an
			// already verified cost hour or the established pricing ledger.
			slog.Warn("渠道倍率变更候选生成失败，已保留核验成本小时", "domain", account.Domain, "hour", state.HourTs, "err", err)
		}
	}
	return nil
}

func (m *Monitor) markChannelCostDirtyHour(ctx context.Context, account ChannelUpstreamAccount, hourTs int64, reason string, cause error) error {
	if !m.channelCostEnabledFor(account) || hourTs < 0 || hourTs%3600 != 0 {
		return nil
	}
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 32 {
		reason = "recovery_required"
	}
	message := ""
	if cause != nil {
		message = cause.Error()
		if len(message) > 512 {
			message = message[:512]
		}
	}
	now := time.Now().Unix()
	row := ChannelCostDirtyHour{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), HourTs: hourTs, Reason: reason, Status: "pending", NextAttemptAt: now, LastError: message, CreatedAt: now, UpdatedAt: now}
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}, {Name: "account_epoch"}, {Name: "hour_ts"}, {Name: "reason"}}, DoUpdates: clause.Assignments(map[string]any{"status": "pending", "next_attempt_at": now, "last_error": message, "updated_at": now})}).Create(&row).Error
}

func (m *Monitor) clearChannelCostDirtyHour(ctx context.Context, account ChannelUpstreamAccount, hourTs int64) error {
	return m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hourTs).Delete(&ChannelCostDirtyHour{}).Error
}

// enqueueMissingChannelCostHours discovers already verified pricing hours that
// predate cost-closure enablement, or whose cost publication was lost. It only
// queues a bounded newest-first slice per scheduler pass.
func (m *Monitor) enqueueMissingChannelCostHours(ctx context.Context, account ChannelUpstreamAccount, limit int) error {
	if !m.channelCostEnabledFor(account) {
		return nil
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 16 {
		limit = 16
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var hours []int64
	err := m.storeDB.WithContext(ctx).Raw(`
		SELECT p.hour_ts
		FROM channel_upstream_pricing_hour_states p
		LEFT JOIN channel_upstream_cost_hour_states c
		  ON c.domain=p.domain AND c.account_epoch=p.account_epoch AND c.hour_ts=p.hour_ts
		 AND c.semantics_version=?
		WHERE p.domain=? AND p.account_epoch=? AND p.semantics_version=?
		  AND p.status='verified' AND p.reconcile_status='matched'
		  AND (c.status IS NULL OR c.status<>'verified')
		  AND NOT EXISTS (
		    SELECT 1 FROM channel_cost_dirty_hours d
		    WHERE d.domain=p.domain AND d.account_epoch=p.account_epoch AND d.hour_ts=p.hour_ts
		  )
		ORDER BY p.hour_ts DESC LIMIT ?`, channelCostEvidenceSemanticsVersion, account.Domain, epoch, upstreamPricingSemanticsVersion, limit).Scan(&hours).Error
	if err != nil {
		return err
	}
	for _, hourTs := range hours {
		if err := m.markChannelCostDirtyHour(ctx, account, hourTs, "missing_cost", errors.New("已核验计价小时缺少渠道成本核验")); err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) nextChannelCostDirtyHour(ctx context.Context, account ChannelUpstreamAccount, now int64) (ChannelCostDirtyHour, error) {
	var row ChannelCostDirtyHour
	err := m.storeDB.WithContext(ctx).
		Where("domain = ? AND account_epoch = ? AND status = 'pending' AND next_attempt_at <= ?", account.Domain, newAPIUpstreamAccountEpoch(account), now).
		Order("hour_ts DESC, created_at ASC").First(&row).Error
	return row, err
}

func (m *Monitor) deferChannelCostDirtyHour(ctx context.Context, account ChannelUpstreamAccount, hourTs, now int64, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
		if len(message) > 512 {
			message = message[:512]
		}
	}
	var current struct{ Attempts int }
	_ = m.storeDB.WithContext(ctx).Model(&ChannelCostDirtyHour{}).
		Select("COALESCE(MAX(attempts),0) attempts").
		Where("domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hourTs).Scan(&current).Error
	attempts := current.Attempts + 1
	shift := attempts - 1
	if shift > 6 {
		shift = 6
	}
	next := now + int64(60*(1<<shift))
	return m.storeDB.WithContext(ctx).Model(&ChannelCostDirtyHour{}).
		Where("domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hourTs).
		Updates(map[string]any{"status": "pending", "attempts": attempts, "next_attempt_at": next, "last_error": message, "updated_at": now}).Error
}

func uniqueNewAPIGroupRateAt(tx *gorm.DB, domain, epoch, sourceGroup string, hourTs int64) (string, int64, bool, error) {
	var hourState ChannelUpstreamPricingHourState
	if err := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", domain, epoch, hourTs, upstreamPricingSemanticsVersion).First(&hourState).Error; err != nil {
		return "", 0, false, err
	}
	if hourState.Status != "verified" || !pricingReconcileAccepted(upstreamProviderNewAPI, hourState.ReconcileStatus) {
		return "", 0, false, nil
	}
	var evidence []ChannelUpstreamPricingHourEvidence
	if err := tx.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ? AND source_group = ? AND eligible_requests > 0", domain, epoch, hourTs, upstreamPricingSemanticsVersion, sourceGroup).Find(&evidence).Error; err != nil {
		return "", 0, false, err
	}
	value := ""
	var requests int64
	for _, row := range evidence {
		if !row.OtherValid || row.EvidenceCapability != "full_rate" || row.EffectiveRatioSource == "unknown" || row.EffectiveRatio == "" {
			return "", 0, false, nil
		}
		rat, ok := new(big.Rat).SetString(row.EffectiveRatio)
		if !ok || rat.Sign() < 0 {
			return "", 0, false, nil
		}
		canonical := rat.RatString()
		if value != "" && value != canonical {
			return "", 0, false, nil
		}
		value = canonical
		if row.EligibleRequests > math.MaxInt64-requests {
			return "", 0, false, errors.New("倍率候选请求数溢出")
		}
		requests += row.EligibleRequests
	}
	return value, requests, value != "", nil
}

func currentChannelCostCanonical(tx *gorm.DB, channelID int, upstreamGroup string) (string, error) {
	var rows []ChannelFinanceChannelCost
	query := tx.Where("channel_id = ?", channelID)
	if strings.TrimSpace(upstreamGroup) != "" {
		query = query.Where("upstream_group_name = ?", upstreamGroup)
	}
	if err := query.Find(&rows).Error; err != nil {
		return "", err
	}
	value := ""
	for _, row := range rows {
		multiplier, ok := new(big.Rat).SetString(strconv.FormatFloat(row.Multiplier, 'g', -1, 64))
		if !ok || multiplier.Sign() < 0 {
			return "", errors.New("现有渠道倍率不是有效数值")
		}
		discount, ok := new(big.Rat).SetString(strconv.FormatFloat(normalizedUpstreamDiscountFactor(row.DiscountFactor), 'g', -1, 64))
		if !ok || discount.Sign() < 0 {
			return "", errors.New("现有渠道折扣不是有效数值")
		}
		canonical := new(big.Rat).Mul(multiplier, discount).RatString()
		if value != "" && value != canonical {
			return "", errors.New("现有渠道倍率历史配置不一致")
		}
		value = canonical
	}
	return value, nil
}

func pricingProposalKey(domain, epoch, sourceRef string, channelID int, sourceGroup, value, scope string, baseVersion int64) string {
	parts := []string{domain, epoch, sourceRef, strconv.Itoa(channelID), sourceGroup, value, scope, strconv.FormatInt(baseVersion, 10)}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func verifiedSourceCostRequestsAt(db *gorm.DB, domain, epoch, sourceRef, sourceGroup string, hourTs int64) (int64, bool, error) {
	var state ChannelUpstreamCostHourState
	if err := db.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", domain, epoch, hourTs, channelCostEvidenceSemanticsVersion).First(&state).Error; err != nil {
		return 0, false, err
	}
	if state.Status != "verified" || state.ReconcileStatus != "matched" {
		return 0, false, nil
	}
	var requests int64
	if err := db.Model(&ChannelUpstreamCostHourEvidence{}).Select("COALESCE(SUM(requests),0)").
		Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ? AND source_ref = ? AND source_group = ?", domain, epoch, hourTs, channelCostEvidenceSemanticsVersion, sourceRef, sourceGroup).
		Scan(&requests).Error; err != nil {
		return 0, false, err
	}
	return requests, requests > 0, nil
}

func (m *Monitor) channelPricingProposalEvidenceDigest(ctx context.Context, account ChannelUpstreamAccount, sourceRef, sourceGroup, expectedValue string, previousHour, currentHour int64) (string, int64, error) {
	return channelPricingProposalEvidenceDigestAt(m.storeDB.WithContext(ctx), account, sourceRef, sourceGroup, expectedValue, previousHour, currentHour, m.cfg.ChannelCostHMACKeyID)
}

func channelPricingProposalEvidenceDigestAt(db *gorm.DB, account ChannelUpstreamAccount, sourceRef, sourceGroup, expectedValue string, previousHour, currentHour int64, hmacKeyID string) (string, int64, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	currentValue, _, currentOK, err := uniqueNewAPIGroupRateAt(db, account.Domain, epoch, sourceGroup, currentHour)
	if err != nil {
		return "", 0, err
	}
	previousValue, _, previousOK, err := uniqueNewAPIGroupRateAt(db, account.Domain, epoch, sourceGroup, previousHour)
	if err != nil {
		return "", 0, err
	}
	if !currentOK || !previousOK || currentValue != expectedValue || previousValue != expectedValue {
		return "", 0, errors.New("倍率候选证据已变化")
	}
	currentRequests, currentOK, err := verifiedSourceCostRequestsAt(db, account.Domain, epoch, sourceRef, sourceGroup, currentHour)
	if err != nil {
		return "", 0, err
	}
	previousRequests, previousOK, err := verifiedSourceCostRequestsAt(db, account.Domain, epoch, sourceRef, sourceGroup, previousHour)
	if err != nil {
		return "", 0, err
	}
	if !currentOK || !previousOK || currentRequests > math.MaxInt64-previousRequests {
		return "", 0, errors.New("倍率候选成本证据已变化")
	}
	var currentCost, previousCost ChannelUpstreamCostHourState
	if err := db.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, currentHour, channelCostEvidenceSemanticsVersion).First(&currentCost).Error; err != nil {
		return "", 0, err
	}
	if err := db.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, previousHour, channelCostEvidenceSemanticsVersion).First(&previousCost).Error; err != nil {
		return "", 0, err
	}
	var currentPricing, previousPricing ChannelUpstreamPricingHourState
	if err := db.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, currentHour, upstreamPricingSemanticsVersion).First(&currentPricing).Error; err != nil {
		return "", 0, err
	}
	if err := db.Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, previousHour, upstreamPricingSemanticsVersion).First(&previousPricing).Error; err != nil {
		return "", 0, err
	}
	requests := currentRequests + previousRequests
	digestParts := []string{
		account.Domain, epoch, sourceRef, sourceGroup, expectedValue,
		currentCost.ContentHash, previousCost.ContentHash, currentPricing.ContentHash, previousPricing.ContentHash,
		strconv.FormatInt(currentRequests, 10), strconv.FormatInt(previousRequests, 10),
		strconv.Itoa(upstreamPricingSemanticsVersion), strconv.Itoa(channelCostEvidenceSemanticsVersion), hmacKeyID,
	}
	digest := sha256.Sum256([]byte(strings.Join(digestParts, "\x00")))
	return hex.EncodeToString(digest[:]), requests, nil
}

// detectNewAPIChannelPricingProposals requires two consecutive, verified,
// reconciled hours and at least twenty requests. It writes an advisory proposal
// and append-only event only; no ChannelFinance row is modified.
func (m *Monitor) detectNewAPIChannelPricingProposals(ctx context.Context, account ChannelUpstreamAccount, hourTs, now int64) error {
	if hourTs < 3600 {
		return nil
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var currentSources []struct {
		SourceRef   string
		SourceGroup string
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamCostHourEvidence{}).
		Select("source_ref, source_group").
		Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, epoch, hourTs, channelCostEvidenceSemanticsVersion).
		Group("source_ref, source_group").Scan(&currentSources).Error; err != nil {
		return err
	}
	for _, source := range currentSources {
		binding, err := m.costSourceBindingAt(ctx, account.Domain, epoch, source.SourceRef, hourTs)
		if err != nil || binding.AllocationMode != "allocated" || binding.LocalChannelID <= 0 {
			continue
		}
		currentValue, _, currentOK, err := uniqueNewAPIGroupRateAt(m.storeDB.WithContext(ctx), account.Domain, epoch, source.SourceGroup, hourTs)
		if err != nil {
			return err
		}
		previousValue, _, previousOK, err := uniqueNewAPIGroupRateAt(m.storeDB.WithContext(ctx), account.Domain, epoch, source.SourceGroup, hourTs-3600)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		currentRequests, currentSourceOK, err := verifiedSourceCostRequestsAt(m.storeDB.WithContext(ctx), account.Domain, epoch, source.SourceRef, source.SourceGroup, hourTs)
		if err != nil {
			return err
		}
		previousRequests, previousSourceOK, err := verifiedSourceCostRequestsAt(m.storeDB.WithContext(ctx), account.Domain, epoch, source.SourceRef, source.SourceGroup, hourTs-3600)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if currentRequests > math.MaxInt64-previousRequests {
			return errors.New("倍率候选请求数溢出")
		}
		evidenceRequests := currentRequests + previousRequests
		if !currentOK || !previousOK || !currentSourceOK || !previousSourceOK || currentValue != previousValue || evidenceRequests < 20 {
			continue
		}
		oldValue, err := currentChannelCostCanonical(m.storeDB.WithContext(ctx), binding.LocalChannelID, source.SourceGroup)
		if err != nil {
			return err
		}
		if oldValue == currentValue {
			continue
		}
		evidenceDigest, digestRequests, err := m.channelPricingProposalEvidenceDigest(ctx, account, source.SourceRef, source.SourceGroup, currentValue, hourTs-3600, hourTs)
		if err != nil || digestRequests != evidenceRequests {
			if err != nil {
				return err
			}
			return errors.New("倍率候选证据请求数不一致")
		}
		var latest ChannelFinanceVersion
		baseVersion := int64(0)
		lookup := m.storeDB.WithContext(ctx).Where("domain = ?", account.Domain).Order("version DESC").First(&latest)
		if lookup.Error != nil && !errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
			return lookup.Error
		}
		if lookup.Error == nil {
			baseVersion = latest.Version
		}
		proposal := ChannelPricingChangeProposal{
			Domain: account.Domain, AccountEpoch: epoch, Provider: account.Provider,
			LocalChannelID: binding.LocalChannelID, SourceRef: source.SourceRef, SourceGroup: source.SourceGroup,
			Scope: "channel_default", ValueKind: "effective_multiplier", OldValue: oldValue, NewValue: currentValue,
			EvidenceFromHour: hourTs - 3600, EvidenceToHour: hourTs + 3600,
			VerifiedHours: 2, EvidenceRequests: evidenceRequests,
			ReconcileStatus: "matched", EvidenceDigest: evidenceDigest,
			PricingSemantics: upstreamPricingSemanticsVersion, CostSemantics: channelCostEvidenceSemanticsVersion, HMACKeyID: m.cfg.ChannelCostHMACKeyID,
			BaseVersion: baseVersion, Status: "pending",
			Reason: "连续两个已闭合小时的上游日志显示计价倍率变化", CreatedAt: now, UpdatedAt: now,
		}
		proposal.ProposalKey = pricingProposalKey(proposal.Domain, proposal.AccountEpoch, proposal.SourceRef, proposal.LocalChannelID, proposal.SourceGroup, proposal.NewValue, proposal.Scope, proposal.BaseVersion)
		if err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			created := tx.Where("proposal_key = ?", proposal.ProposalKey).FirstOrCreate(&proposal)
			if created.Error != nil || created.RowsAffected == 0 {
				return created.Error
			}
			return tx.Create(&ChannelPricingProposalEvent{
				ProposalKey: proposal.ProposalKey, Event: "created", ToStatus: "pending",
				Actor: "monitor", Reason: proposal.Reason, CreatedAt: now,
			}).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

func validateCostSourceBinding(row ChannelCostSourceBinding) error {
	if strings.TrimSpace(row.Domain) == "" || len(row.AccountEpoch) != 64 || len(row.SourceRef) != 64 {
		return errors.New("来源映射身份无效")
	}
	if row.ValidFrom < 0 || row.ValidFrom%3600 != 0 || row.ValidTo < 0 || row.ValidTo%3600 != 0 || (row.ValidTo != 0 && row.ValidTo <= row.ValidFrom) {
		return errors.New("来源映射必须使用整点半开区间")
	}
	if row.Status != "draft" && row.Status != "confirmed" && row.Status != "retired" {
		return errors.New("来源映射状态无效")
	}
	if row.AllocationMode != "allocated" && row.AllocationMode != "shared" && row.AllocationMode != "unallocated" {
		return errors.New("来源映射归属模式无效")
	}
	if row.AllocationMode == "allocated" && row.LocalChannelID <= 0 {
		return errors.New("已归属来源必须指定本地渠道")
	}
	if row.AllocationMode != "allocated" && row.LocalChannelID != 0 {
		return errors.New("shared/unallocated 来源不得猜测本地渠道")
	}
	return nil
}

// saveCostSourceBinding appends one mapping version. Confirmed intervals for
// one source may touch but never overlap.
func (m *Monitor) saveCostSourceBinding(ctx context.Context, row ChannelCostSourceBinding) error {
	if err := validateCostSourceBinding(row); err != nil {
		return err
	}
	m.channelCostBindingMu.Lock()
	defer m.channelCostBindingMu.Unlock()
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if row.Status == "confirmed" {
			var count int64
			q := tx.Model(&ChannelCostSourceBinding{}).
				Where("domain = ? AND account_epoch = ? AND source_ref = ? AND status = 'confirmed'", row.Domain, row.AccountEpoch, row.SourceRef).
				Where("valid_from < ? AND (valid_to = 0 OR valid_to > ?)", costIntervalEnd(row.ValidTo), row.ValidFrom)
			if err := q.Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return errors.New("同一来源存在重叠的已确认映射")
			}
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		return m.enqueueEconomicsForBindingTx(tx, row)
	})
}

// replaceCostSourceBinding atomically closes the current open interval and
// appends its successor. expectedCurrentValidFrom is an optimistic-lock token
// returned by the source-list API; stale or concurrent writers fail closed.
func (m *Monitor) replaceCostSourceBinding(ctx context.Context, row ChannelCostSourceBinding, expectedCurrentValidFrom *int64, expectedCurrentSignature string) error {
	if err := validateCostSourceBinding(row); err != nil {
		return err
	}
	if row.Status != "confirmed" || row.ValidTo != 0 {
		return errors.New("映射切换只允许追加开放的已确认区间")
	}
	m.channelCostBindingMu.Lock()
	defer m.channelCostBindingMu.Unlock()
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current ChannelCostSourceBinding
		currentErr := tx.Where("domain = ? AND account_epoch = ? AND source_ref = ? AND status = 'confirmed' AND valid_to = 0", row.Domain, row.AccountEpoch, row.SourceRef).
			Order("valid_from DESC").First(&current).Error
		switch {
		case currentErr == nil:
			if expectedCurrentValidFrom == nil || *expectedCurrentValidFrom != current.ValidFrom || expectedCurrentSignature == "" || expectedCurrentSignature != channelCostBindingSignature(current) {
				return errors.New("当前来源映射已变化，请刷新后重试")
			}
			if row.ValidFrom == current.ValidFrom && current.ValidFrom > time.Now().Unix() {
				// A not-yet-effective mapping has no historical meaning and may be
				// corrected in place. The content signature is the CAS token; a stale
				// page cannot overwrite a correction that kept the same hour key.
				result := tx.Where("domain = ? AND account_epoch = ? AND source_ref = ? AND valid_from = ? AND status = 'confirmed' AND valid_to = 0", current.Domain, current.AccountEpoch, current.SourceRef, current.ValidFrom).Delete(&ChannelCostSourceBinding{})
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return errors.New("当前来源映射并发变化，请刷新后重试")
				}
			} else if row.ValidFrom <= current.ValidFrom {
				return errors.New("新映射生效时间必须晚于当前版本")
			} else {
				result := tx.Model(&ChannelCostSourceBinding{}).
					Where("domain = ? AND account_epoch = ? AND source_ref = ? AND valid_from = ? AND status = 'confirmed' AND valid_to = 0", current.Domain, current.AccountEpoch, current.SourceRef, current.ValidFrom).
					Update("valid_to", row.ValidFrom)
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return errors.New("当前来源映射并发变化，请刷新后重试")
				}
			}
		case errors.Is(currentErr, gorm.ErrRecordNotFound):
			if (expectedCurrentValidFrom != nil && *expectedCurrentValidFrom != 0) || expectedCurrentSignature != "" {
				return errors.New("当前来源映射已变化，请刷新后重试")
			}
		default:
			return currentErr
		}

		var overlaps int64
		if err := tx.Model(&ChannelCostSourceBinding{}).
			Where("domain = ? AND account_epoch = ? AND source_ref = ? AND status = 'confirmed'", row.Domain, row.AccountEpoch, row.SourceRef).
			Where("valid_from < ? AND (valid_to = 0 OR valid_to > ?)", math.MaxInt64, row.ValidFrom).Count(&overlaps).Error; err != nil {
			return err
		}
		if overlaps > 0 {
			return errors.New("新映射与已有历史区间重叠")
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		return m.enqueueEconomicsForBindingTx(tx, row)
	})
}

func (m *Monitor) enqueueEconomicsForBindingTx(tx *gorm.DB, row ChannelCostSourceBinding) error {
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, row.Domain) {
		return nil
	}
	var affected []int64
	if err := tx.Model(&ChannelUpstreamCostHourEvidence{}).Distinct("hour_ts").
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND hour_ts >= ?", row.Domain, row.AccountEpoch, row.SourceRef, row.ValidFrom).
		Where("? = 0 OR hour_ts < ?", row.ValidTo, row.ValidTo).Order("hour_ts").Scan(&affected).Error; err != nil {
		return err
	}
	now := time.Now().Unix()
	for _, hourTs := range affected {
		dirty := ChannelEconomicsDirtyHour{Domain: row.Domain, AccountEpoch: row.AccountEpoch, HourTs: hourTs, Reason: "mapping_changed", Generation: 1, Status: "pending", NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
		if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
			return err
		}
	}
	return nil
}

func costIntervalEnd(validTo int64) int64 {
	if validTo == 0 {
		return math.MaxInt64
	}
	return validTo
}

func (m *Monitor) costSourceBindingAt(ctx context.Context, domain, epoch, sourceRef string, hourTs int64) (ChannelCostSourceBinding, error) {
	var row ChannelCostSourceBinding
	err := m.storeDB.WithContext(ctx).
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND status = 'confirmed' AND valid_from <= ? AND (valid_to = 0 OR valid_to > ?)", domain, epoch, sourceRef, hourTs, hourTs).
		Order("valid_from DESC").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return row, fmt.Errorf("来源在该小时没有已确认映射: %w", err)
	}
	return row, err
}
