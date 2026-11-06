package monthlysource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Self-contained original observations for a pinned generation source. The
// source planner checks private capture receipts before assembling this file.
// Its digest freezes bytes, not the truth of historical financial attestations.
const TokenUsageArchiveLimit = 65 << 20

type TokenUsageMonthArchive struct {
	Version         int                    `json:"version"`
	Basis           string                 `json:"basis"`
	SelectionSHA256 string                 `json:"selection_sha256"`
	SourceIdentity  string                 `json:"source_identity"`
	Rules           FundingRules           `json:"rules"`
	UserID          int64                  `json:"user_id"`
	Month           string                 `json:"month"`
	Pairs           []TokenUsageWindowPair `json:"pairs"`
}

const TokenUsageArchiveBasis = "retained_token_observations_not_bill_approval"

// Rebuild the report from original groups every time. A saved or supplied
// report/hash alone is not accepted as token attribution evidence.
func DecodeTokenUsageMonthArchive(parent context.Context, raw []byte, now time.Time) (TokenUsageMonthArchive, TokenUsageMonthReport, error) {
	var archive TokenUsageMonthArchive
	var empty TokenUsageMonthReport
	if parent == nil || len(raw) == 0 || len(raw) > TokenUsageArchiveLimit {
		return archive, empty, errors.New("invalid retained token archive size or context")
	}
	if err := parent.Err(); err != nil {
		return archive, empty, err
	}
	if err := json.Unmarshal(raw, &archive); err != nil {
		return TokenUsageMonthArchive{}, empty, err
	}
	canonical, err := json.Marshal(archive)
	if err != nil || !bytes.Equal(raw, canonical) || archive.Version != 1 || archive.Basis != TokenUsageArchiveBasis || !validDigest(archive.SelectionSHA256) {
		return TokenUsageMonthArchive{}, empty, errors.New("invalid or noncanonical retained token archive")
	}
	request := FundingMonthRequest{Month: archive.Month, SourceIdentity: archive.SourceIdentity, Rules: archive.Rules, UserIDs: []int64{archive.UserID}}
	report, err := SummarizeTokenUsageMonth(parent, request, archive.Pairs, now)
	if err != nil {
		return TokenUsageMonthArchive{}, empty, err
	}
	return archive, report, parent.Err()
}
