package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const evidenceHeartbeatStateMaxBytes = 64 << 10

// Namespace is a durable batch namespace, NOT a trusted task identity. The
// receiver's node authorization and source-continuity rules remain unchanged.
// Keep this file separate from the event outbox: it must never be replayed as
// an event batch or removed by outbox retention.
type evidenceHeartbeatState struct {
	Version     int            `json:"version"`
	Node        string         `json:"node"`
	Namespace   string         `json:"namespace"`
	Next        uint64         `json:"next"`
	Pending     *evidenceBatch `json:"pending,omitempty"`
	PendingHash string         `json:"pending_hash,omitempty"`
}

func deliverEvidenceHeartbeat(ctx context.Context, c config, now time.Time) error {
	if c.evidenceFrozenHeartbeat {
		return deliverFrozenEvidenceHeartbeat(ctx, c, now)
	}
	current, err := loadCursor(c.cursorPath)
	if err != nil {
		return err
	}
	heartbeat, err := evidenceHeartbeatBatch(c, now, current)
	if err != nil {
		return err
	}
	_, _, err = postEvidence(ctx, c, heartbeat)
	return err
}

func deliverFrozenEvidenceHeartbeat(ctx context.Context, c config, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := c.cursorPath + ".evidence-heartbeat.json"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	// Two processes must not overwrite each other's prepared sequence. Never
	// block the minute/event workers while waiting for another process.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("evidence heartbeat state is already in use: %w", err)
	}
	defer func() {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
			// The deferred Close still releases the process's file lock. Do
			// not turn a confirmed ACK into a retry because cleanup failed.
			log.Printf("evidence heartbeat unlock failed (file close will release lock): %v", err)
		}
	}()
	state, err := loadEvidenceHeartbeatState(path, c.node)
	if err != nil {
		return err
	}
	if state.Pending == nil {
		if err := prepareEvidenceHeartbeat(c, now, &state); err != nil {
			return err
		}
		// Freeze before network I/O. Restart after lost ACK must resend exactly
		// this payload, including telemetry; never generate a new ID on 409.
		if err := saveEvidenceHeartbeatState(path, state); err != nil {
			return err
		}
	}
	if _, _, err := postEvidence(ctx, c, *state.Pending); err != nil {
		return err
	}
	state.Pending, state.PendingHash = nil, ""
	return saveEvidenceHeartbeatState(path, state)
}

func prepareEvidenceHeartbeat(c config, now time.Time, state *evidenceHeartbeatState) error {
	if state.Next == math.MaxUint64 {
		return errors.New("evidence heartbeat sequence exhausted")
	}
	current, err := loadCursor(c.cursorPath)
	if err != nil {
		return err
	}
	payload, err := evidenceHeartbeatBatch(c, now, current)
	if err != nil {
		return err
	}
	payload.BatchID = evidenceHeartbeatID(state.Node, state.Namespace, state.Next)
	payload.PayloadHash = evidencePayloadHash(payload)
	state.Next++
	state.Pending, state.PendingHash = &payload, evidenceHeartbeatWireHash(payload)
	return nil
}

func evidenceHeartbeatID(node, namespace string, sequence uint64) string {
	encoded, _ := json.Marshal([]any{"evidence-heartbeat-v2", node, namespace, sequence})
	digest := sha256.Sum256(encoded)
	return "ehb2_" + hex.EncodeToString(digest[:24]) // 53 chars, within the receiver's 64-char limit.
}

func evidenceHeartbeatWireHash(payload evidenceBatch) string {
	encoded, _ := json.Marshal(payload)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func saveEvidenceHeartbeatState(path string, state evidenceHeartbeatState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeAtomic(path, encoded, 0o600)
}

func loadEvidenceHeartbeatState(path, node string) (evidenceHeartbeatState, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		var namespace [16]byte
		if _, err := rand.Read(namespace[:]); err != nil {
			return evidenceHeartbeatState{}, err
		}
		return evidenceHeartbeatState{Version: 1, Node: node, Namespace: hex.EncodeToString(namespace[:]), Next: 1}, nil
	}
	if err != nil {
		return evidenceHeartbeatState{}, err
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, evidenceHeartbeatStateMaxBytes+1))
	if err != nil {
		return evidenceHeartbeatState{}, err
	}
	var state evidenceHeartbeatState
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var trailing any
	if len(encoded) > evidenceHeartbeatStateMaxBytes || decoder.Decode(&state) != nil || !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return state, errors.New("invalid evidence heartbeat state; original retained")
	}
	namespace, hexErr := hex.DecodeString(state.Namespace)
	if state.Version != 1 || state.Node != node || state.Next < 1 || hexErr != nil || len(namespace) != 16 {
		return state, errors.New("evidence heartbeat identity mismatch; original retained")
	}
	if state.Pending == nil {
		if state.PendingHash != "" {
			return state, errors.New("invalid evidence heartbeat pending state")
		}
		return state, nil
	}
	p := state.Pending
	if state.Next < 2 || p.Node != node || len(p.Events) != 0 || p.Source.StartOffset != p.Source.EndOffset ||
		p.Source.Kind != "access" || p.Source.Protocol != 0 || p.SchemaVersion != evidenceSchemaVersion ||
		p.BatchID != evidenceHeartbeatID(node, state.Namespace, state.Next-1) ||
		p.PayloadHash != evidencePayloadHash(*p) || state.PendingHash != evidenceHeartbeatWireHash(*p) {
		return state, errors.New("invalid frozen evidence heartbeat; original retained")
	}
	return state, nil
}
