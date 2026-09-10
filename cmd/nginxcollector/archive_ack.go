package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// This receipt is accepted exclusively over the private ECS Unix transport.
// It releases local queue ownership to the durable external archive; it does
// not claim that Monitor has received facts or verified source coverage.
func validateArchiveReceipt(c config, lane, node, batchID string, body, data []byte) error {
	var ack struct {
		Version int    `json:"version"`
		State   string `json:"state"`
		Node    string `json:"node"`
		Lane    string `json:"lane"`
		BatchID string `json:"batch_id"`
		Hash    string `json:"body_sha256"`
	}
	sum := sha256.Sum256(body)
	if c.ecsSocket == "" || len(data) > 4096 || json.Unmarshal(data, &ack) != nil || ack.Version != 1 || ack.State != "durably_archived" || ack.Node != node || ack.Lane != lane || ack.BatchID != batchID || ack.Hash != hex.EncodeToString(sum[:]) {
		return errors.New("external archive receipt mismatch; retain local batch")
	}
	return nil
}
