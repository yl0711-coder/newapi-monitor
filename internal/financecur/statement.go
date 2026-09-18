package financecur

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"
)

const StatementSchemaVersion = 1

const (
	StatementStatusDraft       = "draft"
	StatementStatusPublishable = "publishable"
)

// Statement is a compact, immutable bridge between a CUR source file and a
// future Monitor finance import. It contains no AWS credentials, local paths,
// or mutable display state. Draft statements are useful for review, but must
// never be treated as a complete infrastructure cost.
type Statement struct {
	SchemaVersion        int    `json:"schema_version"`
	StatementID          string `json:"statement_id"`
	Status               string `json:"status"`
	AllocationComplete   bool   `json:"allocation_complete"`
	BillingFinalized     bool   `json:"billing_finalized"`
	SourceSHA256         string `json:"source_sha256"`
	PolicySHA256         string `json:"policy_sha256"`
	TimelineSHA256       string `json:"timeline_sha256"`
	Currency             string `json:"currency"`
	TimeZone             string `json:"time_zone"`
	BillingPeriodStart   int64  `json:"billing_period_start_unix"`
	BillingPeriodEnd     int64  `json:"billing_period_end_unix"`
	FirstUsageUnix       int64  `json:"first_usage_unix"`
	LastUsageThroughUnix int64  `json:"last_usage_through_unix"`
	Rows                 int64  `json:"rows"`
	SourceNanoUSD        int64  `json:"source_nano_usd"`
	NexusAPINanoUSD      int64  `json:"nexusapi_nano_usd"`
	ExcludedNanoUSD      int64  `json:"excluded_nano_usd"`
	UnallocatedNanoUSD   int64  `json:"unallocated_nano_usd"`
	ConflictNanoUSD      int64  `json:"conflict_nano_usd"`
	TotalAbsNanoUSD      uint64 `json:"total_abs_nano_usd"`
	AllocatedAbsNanoUSD  uint64 `json:"allocated_abs_nano_usd"`
	CoveragePPM          int64  `json:"coverage_ppm"`
	IssueCount           int64  `json:"issue_count"`
}

type statementPayload Statement

// BuildStatement binds parsed source evidence to one explicit allocation
// policy. The returned identifier is deterministic for identical evidence.
func BuildStatement(audit Audit, report AllocationReport, sourceSHA256, policySHA256, timeZone string, billingFinalized bool) (Statement, Timeline, error) {
	timeline, err := Periodize(report, timeZone)
	if err != nil {
		return Statement{}, Timeline{}, err
	}
	timelineHash, err := TimelineHash(timeline)
	if err != nil {
		return Statement{}, Timeline{}, err
	}
	statement := Statement{
		SchemaVersion:      StatementSchemaVersion,
		Status:             StatementStatusDraft,
		AllocationComplete: report.Publishable,
		BillingFinalized:   billingFinalized,
		SourceSHA256:       strings.ToLower(strings.TrimSpace(sourceSHA256)),
		PolicySHA256:       strings.ToLower(strings.TrimSpace(policySHA256)),
		TimelineSHA256:     timelineHash,
		Currency:           strings.ToUpper(strings.TrimSpace(audit.Currency)),
		TimeZone:           timeline.TimeZone,

		BillingPeriodStart:   audit.BillingPeriodStart,
		BillingPeriodEnd:     audit.BillingPeriodEnd,
		FirstUsageUnix:       audit.FirstUsageUnix,
		LastUsageThroughUnix: audit.LastUsageThroughUnix,
		Rows:                 audit.Rows,
		SourceNanoUSD:        audit.TotalNanoUSD,
		NexusAPINanoUSD:      report.NexusAPINanoUSD,
		ExcludedNanoUSD:      report.ExcludedNanoUSD,
		UnallocatedNanoUSD:   report.UnallocatedNanoUSD,
		ConflictNanoUSD:      report.ConflictNanoUSD,
		TotalAbsNanoUSD:      report.TotalAbsNanoUSD,
		AllocatedAbsNanoUSD:  report.AllocatedAbsNanoUSD,
		IssueCount:           report.IssueCount,
	}
	if report.Publishable && billingFinalized {
		statement.Status = StatementStatusPublishable
	}
	statement.CoveragePPM = coveragePPM(statement.AllocatedAbsNanoUSD, statement.TotalAbsNanoUSD)
	if err := validateStatementEvidence(statement, audit, report); err != nil {
		return Statement{}, Timeline{}, err
	}
	id, err := statementDigest(statement)
	if err != nil {
		return Statement{}, Timeline{}, err
	}
	statement.StatementID = id
	return statement, timeline, nil
}

// VerifyStatement detects accidental or unauthorized changes to a persisted
// statement. It does not authenticate the original policy author.
func VerifyStatement(statement Statement, timeline Timeline) error {
	if err := validateStatementFields(statement); err != nil {
		return err
	}
	timelineHash, err := TimelineHash(timeline)
	if err != nil {
		return err
	}
	if statement.TimeZone != timeline.TimeZone || statement.TimelineSHA256 != timelineHash {
		return errors.New("CUR statement timeline digest mismatch")
	}
	expected, err := statementDigest(statement)
	if err != nil {
		return err
	}
	if statement.StatementID != expected {
		return errors.New("CUR statement digest mismatch")
	}
	return nil
}

func validateStatementEvidence(statement Statement, audit Audit, report AllocationReport) error {
	if statement.Rows != report.Rows || statement.Rows <= 0 {
		return errors.New("CUR statement row coverage does not match allocation report")
	}
	if statement.SourceNanoUSD != report.TotalNanoUSD {
		return errors.New("CUR statement source cost does not match allocation report")
	}
	if report.Entries <= 0 {
		return errors.New("CUR statement has no allocation entries")
	}
	if report.Publishable != (report.IssueCount == 0 && report.AllocatedAbsNanoUSD == report.TotalAbsNanoUSD) {
		return errors.New("CUR allocation publishable state is inconsistent")
	}
	return validateStatementFields(statement)
}

func validateStatementFields(statement Statement) error {
	if statement.SchemaVersion != StatementSchemaVersion {
		return fmt.Errorf("unsupported CUR statement schema version %d", statement.SchemaVersion)
	}
	if !validSHA256(statement.SourceSHA256) || !validSHA256(statement.PolicySHA256) || !validSHA256(statement.TimelineSHA256) {
		return errors.New("CUR statement requires lowercase SHA-256 provenance")
	}
	if strings.TrimSpace(statement.TimeZone) == "" {
		return errors.New("CUR statement requires an accounting timezone")
	}
	if statement.Currency != "USD" {
		return fmt.Errorf("unsupported CUR statement currency %q", statement.Currency)
	}
	if statement.FirstUsageUnix < 0 || statement.LastUsageThroughUnix <= statement.FirstUsageUnix {
		return errors.New("CUR statement has invalid usage coverage")
	}
	if statement.BillingPeriodStart <= 0 || statement.BillingPeriodEnd <= statement.BillingPeriodStart ||
		statement.FirstUsageUnix < statement.BillingPeriodStart || statement.LastUsageThroughUnix > statement.BillingPeriodEnd {
		return errors.New("CUR statement has invalid billing period")
	}
	if statement.Rows <= 0 || statement.IssueCount < 0 {
		return errors.New("CUR statement has invalid row or issue count")
	}
	if statement.AllocatedAbsNanoUSD > statement.TotalAbsNanoUSD {
		return errors.New("CUR statement allocated coverage exceeds source coverage")
	}
	if statement.CoveragePPM != coveragePPM(statement.AllocatedAbsNanoUSD, statement.TotalAbsNanoUSD) {
		return errors.New("CUR statement coverage ratio is inconsistent")
	}
	total, err := sumNanoUSD(statement.NexusAPINanoUSD, statement.ExcludedNanoUSD, statement.UnallocatedNanoUSD, statement.ConflictNanoUSD)
	if err != nil || total != statement.SourceNanoUSD {
		return errors.New("CUR statement cost components do not reconcile")
	}
	switch statement.Status {
	case StatementStatusPublishable:
		if !statement.AllocationComplete || !statement.BillingFinalized || statement.IssueCount != 0 || statement.AllocatedAbsNanoUSD != statement.TotalAbsNanoUSD || statement.CoveragePPM != AllocationPPM {
			return errors.New("publishable CUR statement has incomplete coverage")
		}
	case StatementStatusDraft:
		if statement.AllocationComplete != (statement.IssueCount == 0 && statement.AllocatedAbsNanoUSD == statement.TotalAbsNanoUSD) {
			return errors.New("draft CUR statement allocation state is inconsistent")
		}
		if statement.AllocationComplete && statement.BillingFinalized {
			return errors.New("fully verified CUR statement cannot remain draft")
		}
	default:
		return fmt.Errorf("unsupported CUR statement status %q", statement.Status)
	}
	return nil
}

func statementDigest(statement Statement) (string, error) {
	payload := statementPayload(statement)
	payload.StatementID = ""
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode CUR statement: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func coveragePPM(allocated, total uint64) int64 {
	if total == 0 {
		return AllocationPPM
	}
	if allocated == total {
		return AllocationPPM
	}
	// Mul64/Div64 preserves the exact floor of allocated*1e6/total without
	// overflowing when absolute CUR totals approach uint64's limit.
	high, low := bits.Mul64(allocated, uint64(AllocationPPM))
	quotient, _ := bits.Div64(high, low, total)
	return int64(quotient)
}

func sumNanoUSD(values ...int64) (int64, error) {
	var total int64
	for _, value := range values {
		if value > 0 && total > math.MaxInt64-value || value < 0 && total < math.MinInt64-value {
			return 0, errors.New("CUR statement cost exceeds integer safety range")
		}
		total += value
	}
	return total, nil
}
