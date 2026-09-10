package ecsarchive

import (
	"bytes"
	"encoding/json"
	"errors"
)

// FinalBoundary describes retained, append-only source files, not the success
// of a model request or completeness of an unrelated database/report interval.
type FinalBoundary struct {
	ObservedAt int64       `json:"observed_at"`
	Files      []FinalFile `json:"files"`
	Contract   string      `json:"contract,omitempty"`
}

type FinalFile struct {
	Name   string `json:"name"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
	SHA256 string `json:"sha256"`
}

type closurePayload struct {
	Node     string         `json:"node"`
	BatchID  string         `json:"batch_id"`
	Boundary *FinalBoundary `json:"boundary,omitempty"`
}

func BoundaryBody(node string, boundary *FinalBoundary) []byte {
	b, _ := json.Marshal(closurePayload{node, "collector-closure-v2", boundary})
	return b
}

func ReadClosure(body []byte, node, lane string) (*FinalBoundary, error) {
	var p closurePayload
	if json.Unmarshal(body, &p) != nil || p.Node != node || p.BatchID != "collector-closure-v2" || !bytes.Equal(body, BoundaryBody(node, p.Boundary)) {
		return nil, errors.New("invalid canonical closure")
	}
	if p.Boundary == nil {
		return nil, nil
	}
	b := p.Boundary
	if b.ObservedAt <= 0 || ValidateFinalFiles(b.Contract, lane, b.Files) != nil {
		return nil, errors.New("unsupported final source inventory")
	}
	return b, nil
}
