package monthlysource

// Byte-preserving reindexing only. These digests prove identity/completeness
// of the supplied index, NOT source authenticity, amounts or bill approval.
import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

const capturedUsagePartMode = "partitioned_usage_reindex_requires_original_audit"
const capturedUsagePartLimit = 2 << 20
const capturedUsagePartsBudget = 4 << 20

type capturedUsagePartRef struct {
	Ordinal    int    `json:"ordinal"`
	FromMember int    `json:"from_member"`
	ToMember   int    `json:"to_member_exclusive"`
	Pairs      int    `json:"pairs"`
	SHA256     string `json:"sha256"`
}
type capturedUsagePartIndex struct {
	Version        int                    `json:"version"`
	Mode           string                 `json:"mode"`
	OriginalSHA256 string                 `json:"original_sha256"`
	Header         CapturedMonthSelection `json:"header"`
	Parts          []capturedUsagePartRef `json:"parts"`
}
type capturedUsagePart struct {
	Version        int                      `json:"version"`
	OriginalSHA256 string                   `json:"original_sha256"`
	FromMember     int                      `json:"from_member"`
	Tokens         []CapturedTokenSelection `json:"tokens"`
}
type PartitionedCapturedUsage struct {
	Index []byte
	Parts map[string][]byte
}

func canonicalCapturedUsageIndex(raw []byte, now time.Time) (CapturedMonthSelection, error) {
	var s CapturedMonthSelection
	if len(raw) == 0 || len(raw) > 2<<20 || json.Unmarshal(raw, &s) != nil {
		return s, ErrTokenMonthCaptureIntegrity
	}
	canonical, _ := json.Marshal(s)
	if !bytes.Equal(raw, canonical) || !validDigest(s.Key) || !validDigest(s.PlanSHA256) || (s.Version != 1 || s.Mode != CapturedMonthSelectionMode) && (s.Version != 2 || s.Mode != RecoveredCapturedMonthSelectionMode) {
		return s, ErrTokenMonthCaptureIntegrity
	}
	if _, err := canonicalFundingCaptureScope(s.Scope, now); err != nil {
		return s, err
	}
	if len(s.Tokens) != len(s.Scope.Funding.UserIDs) {
		return s, ErrTokenMonthCaptureIntegrity
	}
	start, _ := time.ParseInLocation("2006-01", s.Scope.Funding.Month, fundingMonthZone)
	days := int(start.AddDate(0, 1, 0).Sub(start) / (24 * time.Hour))
	parents, usage := map[string]bool{}, map[string]bool{}
	for i, t := range s.Tokens {
		if t.UserID != s.Scope.Funding.UserIDs[i] || len(t.Usage) < days || len(t.Usage) > days*16 || len(t.Parents) != 2*len(t.Usage) || s.Version == 1 && len(t.Usage) != days {
			return s, ErrTokenMonthCaptureIntegrity
		}
		for _, id := range t.Parents {
			if !validDigest(id) || parents[id] {
				return s, ErrTokenMonthCaptureIntegrity
			}
			parents[id] = true
		}
		for _, pair := range t.Usage {
			for _, id := range []string{pair.OlderID, pair.NewerID} {
				if !validDigest(id) || usage[id] {
					return s, ErrTokenMonthCaptureIntegrity
				}
				usage[id] = true
			}
		}
	}
	return s, nil
}

// PartitionCapturedUsageIndex groups whole accounts without changing the
// original organization header, raw-parent IDs, user-projection IDs or order.
// It performs no I/O, source query, financial audit or implicit format upgrade.
func PartitionCapturedUsageIndex(ctx context.Context, raw []byte, now time.Time) (PartitionedCapturedUsage, error) {
	var empty PartitionedCapturedUsage
	if ctx == nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	s, err := canonicalCapturedUsageIndex(raw, now)
	if err != nil {
		return empty, err
	}
	header := s
	header.Tokens = nil
	index := capturedUsagePartIndex{Version: 1, Mode: capturedUsagePartMode, OriginalSHA256: digest(raw), Header: header, Parts: []capturedUsagePartRef{}}
	out := PartitionedCapturedUsage{Parts: map[string][]byte{}}
	used := 0
	for from := 0; from < len(s.Tokens); {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		to, pairs := from, 0
		for to < len(s.Tokens) && pairs+len(s.Tokens[to].Usage) <= 4096 {
			pairs += len(s.Tokens[to].Usage)
			to++
		}
		if to == from {
			return empty, ErrTokenMonthCaptureBudget
		}
		body, err := json.Marshal(capturedUsagePart{1, index.OriginalSHA256, from, s.Tokens[from:to]})
		if err != nil {
			return empty, err
		}
		if len(body) > capturedUsagePartLimit {
			return empty, ErrTokenMonthCaptureBudget
		}
		used += len(body)
		if used > capturedUsagePartsBudget {
			return empty, ErrTokenMonthCaptureBudget
		}
		sha := digest(body)
		out.Parts[sha] = body
		index.Parts = append(index.Parts, capturedUsagePartRef{len(index.Parts), from, to, pairs, sha})
		from = to
	}
	out.Index, err = json.Marshal(index)
	if err != nil {
		return empty, err
	}
	if len(out.Index) > 128<<10 || used+len(out.Index) > capturedUsagePartsBudget {
		return empty, ErrTokenMonthCaptureBudget
	}
	return out, nil
}

// ReassembleCapturedUsageIndex requires every declared original part, with no
// extras, gaps or reordered members. The returned bytes must match the pinned
// ORIGINAL SHA256. Consumers must still call the unchanged source/amount audit;
// successful reassembly must never be used as a replacement approval flag.
func ReassembleCapturedUsageIndex(ctx context.Context, archive PartitionedCapturedUsage, now time.Time) ([]byte, error) {
	if ctx == nil {
		return nil, ErrTokenMonthCaptureIntegrity
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var index capturedUsagePartIndex
	if len(archive.Index) == 0 || len(archive.Index) > 128<<10 || json.Unmarshal(archive.Index, &index) != nil {
		return nil, ErrTokenMonthCaptureIntegrity
	}
	canonical, _ := json.Marshal(index)
	if !bytes.Equal(canonical, archive.Index) || index.Version != 1 || index.Mode != capturedUsagePartMode || !validDigest(index.OriginalSHA256) || index.Header.Tokens != nil || len(index.Parts) == 0 || len(index.Parts) > WalletBatchLimit || len(archive.Parts) != len(index.Parts) {
		return nil, ErrTokenMonthCaptureIntegrity
	}
	result := index.Header
	result.Tokens = []CapturedTokenSelection{}
	used := len(archive.Index)
	seen := map[string]bool{}
	for i, ref := range index.Parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		body, ok := archive.Parts[ref.SHA256]
		if !ok || !validDigest(ref.SHA256) || seen[ref.SHA256] || len(body) == 0 || len(body) > capturedUsagePartLimit || ref.Ordinal != i || ref.FromMember != len(result.Tokens) || ref.ToMember <= ref.FromMember || ref.ToMember > WalletBatchLimit || ref.Pairs < 1 || ref.Pairs > 4096 {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		seen[ref.SHA256] = true
		used += len(body)
		if used > capturedUsagePartsBudget {
			return nil, ErrTokenMonthCaptureBudget
		}
		var part capturedUsagePart
		if digest(body) != ref.SHA256 || json.Unmarshal(body, &part) != nil {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		canonical, _ := json.Marshal(part)
		if !bytes.Equal(body, canonical) || part.Version != 1 || part.OriginalSHA256 != index.OriginalSHA256 || part.FromMember != ref.FromMember || len(part.Tokens) != ref.ToMember-ref.FromMember {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		count := 0
		for _, t := range part.Tokens {
			count += len(t.Usage)
		}
		if count != ref.Pairs {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		result.Tokens = append(result.Tokens, part.Tokens...)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if digest(raw) != index.OriginalSHA256 {
		return nil, ErrTokenMonthCaptureIntegrity
	}
	if _, err := canonicalCapturedUsageIndex(raw, now); err != nil {
		return nil, err
	}
	return raw, nil
}
