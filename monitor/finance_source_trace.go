package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sort"
	"strings"
)

type financeSourceTraceKey struct{}
type financeSourceTrace map[string]string

// Diagnostics share the existing read; no extra SQL and no changed cache
// fingerprint. Only section names, never hashes or financial values, are logged.
type financeSourceHasher struct {
	hash.Hash
	section string
	parts   map[string]hash.Hash
	trace   financeSourceTrace
}

func newFinanceSourceHasher(ctx context.Context) *financeSourceHasher {
	trace, _ := ctx.Value(financeSourceTraceKey{}).(financeSourceTrace)
	return &financeSourceHasher{Hash: sha256.New(), section: "upstream-accounts", parts: make(map[string]hash.Hash), trace: trace}
}

func (h *financeSourceHasher) Write(p []byte) (int, error) {
	if h.trace != nil {
		part := h.parts[h.section]
		if part == nil {
			part = sha256.New()
			h.parts[h.section] = part
		}
		_, _ = part.Write(p)
	}
	return h.Hash.Write(p)
}

func (h *financeSourceHasher) Sum(b []byte) []byte {
	for key, part := range h.parts {
		h.trace[key] = hex.EncodeToString(part.Sum(nil))
	}
	return h.Hash.Sum(b)
}

func financeChangedSources(before, after financeSourceTrace) string {
	if len(before) == 0 || len(after) == 0 {
		return "source-fingerprint"
	}
	changed := make(map[string]bool)
	for key, value := range before {
		if after[key] != value {
			changed[key] = true
		}
	}
	for key, value := range after {
		if before[key] != value {
			changed[key] = true
		}
	}
	keys := make([]string, 0, len(changed))
	for key := range changed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
