package monitor

import (
	"math/big"
	"strconv"
)

// Interpretation is deliberately separate from raw evidence. No original token
// count or charged quota is mutated, and no current pricing config is consulted.
type portalCacheEvidence struct {
	State portalEvidenceState
	// Total may be explicitly stored or derived only from the recorded split.
	Total       portalEvidenceField
	Unsplit     portalEvidenceField
	TotalSource string // recorded / split_only / unknown
	Issue       string
}

// Exact decimal text avoids JS's 2^53 limit. Source distinguishes a recorded
// total from a subtotal of available splits; neither is a recalculated charge.
type portalCacheDisplay struct {
	State  portalEvidenceState `json:"state"`
	Value  string              `json:"value,omitempty"`
	Source string              `json:"source,omitempty"`
}

func portalCacheDisplays(e portalLogEvidence) (portalCacheDisplay, portalCacheDisplay) {
	if e.State != portalEvidenceValid {
		return portalCacheDisplay{State: e.State}, portalCacheDisplay{State: e.State}
	}
	read := e.Numbers["cache_tokens"]
	r := portalCacheDisplay{State: read.State}
	if read.State == portalEvidenceValid {
		n, _ := new(big.Rat).SetString(read.Text)
		r.Value, r.Source = n.Num().String(), "recorded"
	}
	cache := portalCacheWriteEvidence(e)
	w := portalCacheDisplay{State: cache.State, Source: cache.TotalSource}
	if cache.State == portalEvidenceValid {
		n, _ := new(big.Rat).SetString(cache.Total.Text)
		w.Value = n.Num().String()
	}
	return r, w
}

func portalCacheDisplayInt(c portalCacheDisplay) int64 {
	if c.State != portalEvidenceValid {
		return 0
	}
	n, err := strconv.ParseInt(c.Value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func portalCacheWriteEvidence(e portalLogEvidence) portalCacheEvidence {
	r := portalCacheEvidence{State: portalEvidenceMissing, Total: portalEvidenceField{State: portalEvidenceMissing}, Unsplit: portalEvidenceField{State: portalEvidenceMissing}, TotalSource: "unknown"}
	if e.State != portalEvidenceValid {
		r.State = e.State
		return r
	}
	total := e.Numbers["cache_creation_tokens"]
	five := e.Numbers["cache_creation_tokens_5m"]
	hour := e.Numbers["cache_creation_tokens_1h"]
	for _, f := range []portalEvidenceField{total, five, hour} {
		if f.State == portalEvidenceInvalid || f.State == portalEvidenceNull {
			r.State, r.Issue = portalEvidenceInvalid, "cache_write_field_invalid"
			return r
		}
	}
	if total.State == portalEvidenceMissing && five.State == portalEvidenceMissing && hour.State == portalEvidenceMissing {
		return r
	}
	sum := new(big.Int)
	for _, f := range []portalEvidenceField{five, hour} {
		if f.State == portalEvidenceValid {
			v, _ := new(big.Rat).SetString(f.Text)
			sum.Add(sum, v.Num()) // integer field can be written as 1e2 / 1.0
		}
	}
	if !sum.IsInt64() {
		r.State, r.Issue = portalEvidenceInvalid, "cache_write_sum_overflow"
		return r
	}
	if total.State == portalEvidenceMissing {
		// A visible subtotal, NOT proof that there were no unspecified writes.
		r.Total = portalEvidenceField{State: portalEvidenceValid, Text: sum.String()}
		r.State, r.TotalSource, r.Issue = portalEvidenceValid, "split_only", "cache_write_total_not_recorded"
		return r
	}
	v, _ := new(big.Rat).SetString(total.Text)
	n := v.Num().Int64()
	if n < sum.Int64() {
		r.State, r.Issue = portalEvidenceInvalid, "cache_write_split_exceeds_total"
		return r
	}
	r.State, r.Total, r.TotalSource = portalEvidenceValid, total, "recorded"
	r.Unsplit = portalEvidenceField{State: portalEvidenceValid, Text: strconv.FormatInt(n-sum.Int64(), 10)}
	return r
}
