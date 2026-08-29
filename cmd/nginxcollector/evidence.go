package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const evidenceSchemaVersion = 1

const maxEvidenceBatchFileBytes int64 = 4 << 20

type evidenceSourceRange struct {
	Kind                         string `json:"kind"`
	FileID                       string `json:"file_id"`
	StartOffset                  int64  `json:"start_offset"`
	EndOffset                    int64  `json:"end_offset"`
	FirstEventMS                 int64  `json:"first_event_ms"`
	LastEventMS                  int64  `json:"last_event_ms"`
	CursorDiscontinuities        int64  `json:"cursor_discontinuities"`
	LastCursorDiscontinuity      int64  `json:"last_cursor_discontinuity_at"`
	DiscardedLines               int64  `json:"discarded_lines"`
	LastDiscardedAt              int64  `json:"last_discarded_at"`
	EvidenceEligible             int64  `json:"evidence_eligible"`
	EvidenceParseRejected        int64  `json:"evidence_parse_rejected"`
	LastEvidenceParseRejectedAt  int64  `json:"last_evidence_parse_rejected_at"`
	EvidencePersistFailures      int64  `json:"evidence_persist_failures"`
	EvidenceDroppedEvents        int64  `json:"evidence_dropped_events"`
	LastEvidencePersistFailureAt int64  `json:"last_evidence_persist_failure_at"`
}

type evidenceTelemetry struct {
	OutboxBytes           int64 `json:"outbox_bytes"`
	OutboxBatches         int64 `json:"outbox_batches"`
	RejectedBytes         int64 `json:"rejected_bytes"`
	RejectedBatches       int64 `json:"rejected_batches"`
	DroppedEvents         int64 `json:"dropped_events"`
	UnknownDroppedBatches int64 `json:"unknown_dropped_batches"`
	GapCount              int64 `json:"gap_count"`
	LastGapFromMS         int64 `json:"last_gap_from_ms"`
	LastGapToMS           int64 `json:"last_gap_to_ms"`
}

type evidenceEvent struct {
	EventID          string `json:"event_id"`
	EventMS          int64  `json:"event_ms"`
	Route            string `json:"route"`
	Method           string `json:"method"`
	Status           int    `json:"status"`
	UpstreamStatus   int    `json:"upstream_status"`
	UpstreamAttempts int    `json:"upstream_attempts"`
	UpstreamStatuses []int  `json:"upstream_statuses,omitempty"`
	RequestMS        int64  `json:"request_ms"`
	UpstreamMS       int64  `json:"upstream_ms"`
	UpstreamPresent  bool   `json:"upstream_present"`
	ConnectMS        int64  `json:"connect_ms"`
	HeaderMS         int64  `json:"header_ms"`
	BytesSent        int64  `json:"bytes_sent"`
	Completion       string `json:"completion"`
	NginxIDHMAC      string `json:"nginx_id_hmac,omitempty"`
	OneAPIIDHMAC     string `json:"oneapi_id_hmac,omitempty"`
}

type evidenceBatch struct {
	SchemaVersion int                 `json:"schema_version"`
	Node          string              `json:"node"`
	BatchID       string              `json:"batch_id"`
	PayloadHash   string              `json:"payload_hash"`
	LogSchema     int                 `json:"log_schema"`
	HMACKeyID     string              `json:"hmac_key_id"`
	Source        evidenceSourceRange `json:"source"`
	Events        []evidenceEvent     `json:"events"`
	Telemetry     evidenceTelemetry   `json:"telemetry"`
}

type evidenceQueueState struct {
	NextSequence int64 `json:"next_sequence"`
}

type evidenceGapState struct {
	DroppedEvents         int64            `json:"dropped_events"`
	GapCount              int64            `json:"gap_count"`
	LastGapFromMS         int64            `json:"last_gap_from_ms"`
	LastGapToMS           int64            `json:"last_gap_to_ms"`
	LastBatchID           string           `json:"last_batch_id,omitempty"`
	LastPayloadHash       string           `json:"last_payload_hash,omitempty"`
	UnknownDroppedBatches int64            `json:"unknown_dropped_batches,omitempty"`
	Recent                []evidenceGapRef `json:"recent,omitempty"`
}

type evidenceGapRef struct {
	BatchID     string `json:"batch_id"`
	PayloadHash string `json:"payload_hash,omitempty"`
}

type rawEvidenceLine struct {
	LogSchema         flexNumber       `json:"log_schema"`
	UpstreamStatus    upstreamSequence `json:"upstream_status"`
	NginxRequestID    string           `json:"nginx_request_id"`
	OneAPIRequestID   string           `json:"oneapi_request_id"`
	RequestCompletion string           `json:"request_completion"`
	UpstreamConnect   upstreamNumber   `json:"upstream_connect_time"`
	UpstreamHeader    upstreamNumber   `json:"upstream_header_time"`
}

type upstreamSequence struct {
	Present bool
	Values  []float64
}

func (n *upstreamSequence) UnmarshalJSON(data []byte) error {
	var parsed upstreamNumber
	text := strings.TrimSpace(string(data))
	if text == "" || text == "null" || text == `""` || text == `"-"` {
		*n = upstreamSequence{}
		return nil
	}
	if !strings.HasPrefix(text, `"`) {
		if err := parsed.UnmarshalJSON(data); err != nil {
			return err
		}
		*n = upstreamSequence{Present: true, Values: []float64{parsed.Value}}
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ':' })
	if len(parts) < 1 || len(parts) > 8 {
		return errors.New("invalid upstream sequence")
	}
	values := make([]float64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "-" {
			continue
		}
		if part == "" {
			return errors.New("invalid upstream sequence")
		}
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return err
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		*n = upstreamSequence{}
		return nil
	}
	*n = upstreamSequence{Present: true, Values: values}
	return nil
}

type evidenceAck struct {
	OK            bool   `json:"ok"`
	SchemaVersion int    `json:"schema_version"`
	BatchID       string `json:"batch_id"`
	PayloadHash   string `json:"payload_hash"`
	Accepted      int    `json:"accepted"`
	Rejected      int    `json:"rejected"`
}

type evidenceErrorResponse struct {
	Error string `json:"error"`
}

// evidenceResponseReason only exposes Monitor-owned, fixed protocol errors.
// Never echo an arbitrary response body here: an intermediary or a future
// upstream could otherwise make collector logs contain request-derived data.
func evidenceResponseReason(data []byte) string {
	var response evidenceErrorResponse
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&response) != nil {
		return ""
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ""
	}
	switch response.Error {
	case "nginx evidence disabled",
		"evidence store capacity unavailable",
		"evidence store size limit reached",
		"invalid evidence payload",
		"invalid evidence envelope",
		"payload hash mismatch",
		"evidence source range mismatch",
		"batch id conflict",
		"source cursor discontinuity",
		"evidence store unavailable":
		return response.Error
	default:
		return ""
	}
}

func validEvidenceKeyID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for i, r := range value {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		digit := r >= '0' && r <= '9'
		if letter || digit || i > 0 && (r == '.' || r == '_' || r == '-') {
			continue
		}
		return false
	}
	return true
}

func validEvidenceID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f {
			return false
		}
	}
	return true
}

func evidenceHMAC(key []byte, domain, raw string) string {
	if !validEvidenceID(raw) {
		return ""
	}
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(h.Sum(nil))
}

func evidenceStatuses(raw upstreamSequence) ([]int, bool) {
	if !raw.Present {
		return nil, true
	}
	if len(raw.Values) < 1 || len(raw.Values) > 8 {
		return nil, false
	}
	out := make([]int, 0, len(raw.Values))
	for _, value := range raw.Values {
		status := int(value)
		if value != float64(status) || status < 100 || status > 599 {
			return nil, false
		}
		out = append(out, status)
	}
	return out, true
}

func evidenceCompletion(raw string) string {
	switch strings.TrimSpace(raw) {
	case "OK":
		return "complete_at_edge"
	case "", "-":
		return "incomplete_at_edge"
	default:
		return "unknown"
	}
}

func isEvidenceRoute(route string) bool {
	switch route {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/*":
		return true
	default:
		return false
	}
}

func durationMS(raw upstreamNumber) int64 {
	if !raw.Present || raw.Value < 0 || raw.Value > 86400 {
		return 0
	}
	return int64(raw.Value*1000 + 0.5)
}

func makeEvidenceEvent(c config, data []byte, raw rawLine, row sample, inode uint64, offset int64, lineDigest [sha256.Size]byte) (evidenceEvent, bool) {
	if c.evidenceMode == "off" || raw.Method == "" && raw.RequestMethod == "" || row.Method != "POST" || !isEvidenceRoute(row.Route) {
		return evidenceEvent{}, false
	}
	var extra rawEvidenceLine
	if json.Unmarshal(data, &extra) != nil || int(extra.LogSchema) != 2 {
		return evidenceEvent{}, false
	}
	statuses, ok := evidenceStatuses(extra.UpstreamStatus)
	if !ok {
		return evidenceEvent{}, false
	}
	identity := sha256.New()
	_, _ = fmt.Fprintf(identity, "%s:%d:%d:", c.node, inode, offset)
	_, _ = identity.Write(lineDigest[:])
	eventID := hex.EncodeToString(identity.Sum(nil))
	return evidenceEvent{
		EventID: eventID, EventMS: rawEventMS(raw), Route: row.Route, Method: row.Method, Status: row.Status,
		UpstreamStatus: row.UpstreamStatus, UpstreamAttempts: len(statuses), UpstreamStatuses: statuses,
		RequestMS: row.RequestTimeSumMS, UpstreamMS: row.UpstreamTimeSumMS, UpstreamPresent: row.UpstreamTimeCount == 1,
		ConnectMS: durationMS(extra.UpstreamConnect), HeaderMS: durationMS(extra.UpstreamHeader), BytesSent: row.BytesSent,
		Completion:   evidenceCompletion(extra.RequestCompletion),
		NginxIDHMAC:  evidenceHMAC(c.evidenceHMACKey, "nginx-request-id", extra.NginxRequestID),
		OneAPIIDHMAC: evidenceHMAC(c.evidenceHMACKey, "oneapi-request-id", extra.OneAPIRequestID),
	}, true
}

func evidenceCandidate(data []byte, raw rawLine, row sample) bool {
	if raw.Method == "" && raw.RequestMethod == "" || row.Method != "POST" || !isEvidenceRoute(row.Route) {
		return false
	}
	var schema struct {
		LogSchema flexNumber `json:"log_schema"`
	}
	return json.Unmarshal(data, &schema) == nil && int(schema.LogSchema) == 2
}

func evidencePayloadHash(in evidenceBatch) string {
	copyIn := in
	copyIn.PayloadHash = ""
	copyIn.Telemetry = evidenceTelemetry{}
	copyIn.Events = append([]evidenceEvent(nil), in.Events...)
	sort.Slice(copyIn.Events, func(i, j int) bool { return copyIn.Events[i].EventID < copyIn.Events[j].EventID })
	payload, _ := json.Marshal(copyIn)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func newEvidenceBatch(c config, batchID string, inode uint64, start, end, firstMS, lastMS int64, events []evidenceEvent, value cursor) *evidenceBatch {
	payload := &evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: c.node, BatchID: batchID, LogSchema: value.LastLogSchema, HMACKeyID: c.evidenceHMACKeyID,
		Source: evidenceSourceRange{Kind: "access", FileID: strconv.FormatUint(inode, 10), StartOffset: start, EndOffset: end, FirstEventMS: firstMS, LastEventMS: lastMS,
			CursorDiscontinuities: value.Discontinuities, LastCursorDiscontinuity: value.LastDiscontinuityAt, DiscardedLines: value.DiscardedLines, LastDiscardedAt: value.LastDiscardedAt,
			EvidenceEligible: value.EvidenceEligible, EvidenceParseRejected: value.EvidenceParseRejected, LastEvidenceParseRejectedAt: value.LastEvidenceParseRejectedAt,
			EvidencePersistFailures: value.EvidencePersistFailures, EvidenceDroppedEvents: value.EvidenceDroppedEvents,
			LastEvidencePersistFailureAt: value.LastEvidencePersistFailureAt}, Events: events}
	payload.PayloadHash = evidencePayloadHash(*payload)
	return payload
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func evidenceOutboxStats(path string) (bytes int64, batches int64, err error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || entry.Name() == "gap-state.json" || entry.Name() == "queue-state.json" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return 0, 0, infoErr
		}
		bytes += info.Size()
		batches++
	}
	return bytes, batches, nil
}

func evidenceRejectedStats(path string) (bytes int64, batches int64, err error) {
	entries, err := os.ReadDir(filepath.Join(path, "rejected"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return 0, 0, infoErr
		}
		bytes += info.Size()
		batches++
	}
	return bytes, batches, nil
}

func loadGapState(path string) (evidenceGapState, error) {
	data, err := os.ReadFile(filepath.Join(path, "gap-state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return evidenceGapState{}, nil
	}
	if err != nil {
		return evidenceGapState{}, err
	}
	var state evidenceGapState
	if json.Unmarshal(data, &state) != nil || state.DroppedEvents < 0 || state.GapCount < 0 ||
		(state.LastBatchID != "" && !validEvidenceID(state.LastBatchID)) || (state.LastPayloadHash != "" && len(state.LastPayloadHash) != 64) {
		return evidenceGapState{}, errors.New("invalid evidence gap state")
	}
	if len(state.Recent) > 256 {
		return evidenceGapState{}, errors.New("invalid evidence gap history")
	}
	for _, ref := range state.Recent {
		if !validEvidenceID(ref.BatchID) || ref.PayloadHash != "" && len(ref.PayloadHash) != 64 {
			return evidenceGapState{}, errors.New("invalid evidence gap history")
		}
	}
	if len(state.Recent) == 0 && state.LastBatchID != "" {
		state.Recent = []evidenceGapRef{{BatchID: state.LastBatchID, PayloadHash: state.LastPayloadHash}}
	}
	return state, nil
}

func recordEvidenceGapLocked(c config, payload evidenceBatch, dropped int) error {
	state, err := loadGapState(c.evidenceOutboxPath)
	if err != nil {
		return err
	}
	for _, ref := range state.Recent {
		if ref.BatchID != payload.BatchID {
			continue
		}
		if ref.PayloadHash != payload.PayloadHash {
			return errors.New("evidence gap batch collision")
		}
		return nil
	}
	state.DroppedEvents += int64(dropped)
	state.GapCount++
	state.LastBatchID = payload.BatchID
	state.LastPayloadHash = payload.PayloadHash
	state.Recent = append(state.Recent, evidenceGapRef{BatchID: payload.BatchID, PayloadHash: payload.PayloadHash})
	if len(state.Recent) > 256 {
		state.Recent = state.Recent[len(state.Recent)-256:]
	}
	if state.LastGapFromMS == 0 || payload.Source.FirstEventMS < state.LastGapFromMS {
		state.LastGapFromMS = payload.Source.FirstEventMS
	}
	if payload.Source.LastEventMS > state.LastGapToMS {
		state.LastGapToMS = payload.Source.LastEventMS
	}
	data, _ := json.Marshal(state)
	return writeAtomic(filepath.Join(c.evidenceOutboxPath, "gap-state.json"), data, 0o600)
}

func recordEvidenceGap(c config, payload evidenceBatch, dropped int) error {
	if c.evidenceFSMu != nil {
		c.evidenceFSMu.Lock()
		defer c.evidenceFSMu.Unlock()
	}
	return recordEvidenceGapLocked(c, payload, dropped)
}

func recordUnknownEvidenceGapLocked(c config, batchID string) error {
	state, err := loadGapState(c.evidenceOutboxPath)
	if err != nil {
		return err
	}
	for _, ref := range state.Recent {
		if ref.BatchID == batchID {
			return nil
		}
	}
	state.UnknownDroppedBatches++
	state.GapCount++
	state.LastBatchID = batchID
	state.LastPayloadHash = ""
	state.Recent = append(state.Recent, evidenceGapRef{BatchID: batchID})
	if len(state.Recent) > 256 {
		state.Recent = state.Recent[len(state.Recent)-256:]
	}
	data, _ := json.Marshal(state)
	return writeAtomic(filepath.Join(c.evidenceOutboxPath, "gap-state.json"), data, 0o600)
}

func spoolEvidence(c config, payload evidenceBatch) error {
	if len(payload.Events) == 0 && payload.Source.EndOffset == payload.Source.StartOffset {
		return nil
	}
	if c.evidenceFSMu != nil {
		c.evidenceFSMu.Lock()
		defer c.evidenceFSMu.Unlock()
	}
	if err := os.MkdirAll(c.evidenceOutboxPath, 0o700); err != nil {
		return err
	}
	path, findErr := findEvidenceBatchLocked(c.evidenceOutboxPath, payload.BatchID)
	if findErr != nil {
		return findErr
	}
	if path != "" {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.Size() > maxEvidenceBatchFileBytes {
			return errors.New("existing evidence outbox batch is oversized")
		}
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var prior evidenceBatch
		if json.Unmarshal(existing, &prior) != nil || prior.PayloadHash != payload.PayloadHash {
			return errors.New("evidence outbox batch collision")
		}
		return nil
	}
	used, batches, err := evidenceOutboxStats(c.evidenceOutboxPath)
	if err != nil {
		return err
	}
	gap, err := loadGapState(c.evidenceOutboxPath)
	if err != nil {
		return err
	}
	rejectedBytes, rejectedBatches, err := evidenceRejectedStats(c.evidenceOutboxPath)
	if err != nil {
		return err
	}
	payload.Telemetry = evidenceTelemetry{OutboxBytes: used, OutboxBatches: batches, RejectedBytes: rejectedBytes, RejectedBatches: rejectedBatches,
		DroppedEvents: gap.DroppedEvents, UnknownDroppedBatches: gap.UnknownDroppedBatches, GapCount: gap.GapCount, LastGapFromMS: gap.LastGapFromMS, LastGapToMS: gap.LastGapToMS}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if used+rejectedBytes+int64(len(data)) > c.evidenceOutboxMax || batches >= 10_000 {
		return recordEvidenceGapLocked(c, payload, len(payload.Events))
	}
	sequence, err := allocateEvidenceSequenceLocked(c.evidenceOutboxPath)
	if err != nil {
		return err
	}
	path = filepath.Join(c.evidenceOutboxPath, fmt.Sprintf("%020d-%s.json", sequence, payload.BatchID))
	return writeAtomic(path, data, 0o600)
}

func findEvidenceBatchLocked(path, batchID string) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || entry.Name() == "gap-state.json" || entry.Name() == "queue-state.json" {
			continue
		}
		if evidenceBatchIDFromFilename(entry.Name()) == batchID {
			return filepath.Join(path, entry.Name()), nil
		}
	}
	return "", nil
}

func loadEvidenceQueueState(path string) (evidenceQueueState, error) {
	data, err := os.ReadFile(filepath.Join(path, "queue-state.json"))
	if errors.Is(err, os.ErrNotExist) {
		var maxSequence int64
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return evidenceQueueState{}, readErr
		}
		for _, entry := range entries {
			if sequence, ok := evidenceSequenceFromFilename(entry.Name()); ok && sequence > maxSequence {
				maxSequence = sequence
			}
		}
		if maxSequence == math.MaxInt64 {
			return evidenceQueueState{}, errors.New("evidence enqueue sequence exhausted")
		}
		return evidenceQueueState{NextSequence: maxSequence + 1}, nil
	}
	if err != nil {
		return evidenceQueueState{}, err
	}
	var state evidenceQueueState
	if json.Unmarshal(data, &state) != nil || state.NextSequence <= 0 {
		return evidenceQueueState{}, errors.New("invalid evidence queue state")
	}
	return state, nil
}

func allocateEvidenceSequenceLocked(path string) (int64, error) {
	state, err := loadEvidenceQueueState(path)
	if err != nil {
		return 0, err
	}
	if state.NextSequence == math.MaxInt64 {
		return 0, errors.New("evidence enqueue sequence exhausted")
	}
	sequence := state.NextSequence
	state.NextSequence++
	data, _ := json.Marshal(state)
	if err := writeAtomic(filepath.Join(path, "queue-state.json"), data, 0o600); err != nil {
		return 0, err
	}
	return sequence, nil
}

func listEvidenceOutbox(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	type item struct {
		path      string
		mod       time.Time
		sequence  int64
		sequenced bool
	}
	items := make([]item, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || entry.Name() == "gap-state.json" || entry.Name() == "queue-state.json" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil, infoErr
		}
		sequence, sequenced := evidenceSequenceFromFilename(entry.Name())
		items = append(items, item{filepath.Join(path, entry.Name()), info.ModTime(), sequence, sequenced})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].sequenced != items[j].sequenced {
			// Legacy queued files predate the durable sequence and must drain first.
			return !items[i].sequenced
		}
		if items[i].sequenced && items[i].sequence != items[j].sequence {
			return items[i].sequence < items[j].sequence
		}
		if !items[i].mod.Equal(items[j].mod) {
			return items[i].mod.Before(items[j].mod)
		}
		return items[i].path < items[j].path
	})
	out := make([]string, len(items))
	for i := range items {
		out[i] = items[i].path
	}
	return out, nil
}

func evidenceSequenceFromFilename(name string) (int64, bool) {
	if len(name) <= 21 || name[20] != '-' {
		return 0, false
	}
	sequence, err := strconv.ParseInt(name[:20], 10, 64)
	return sequence, err == nil && sequence >= 0
}

func evidenceBatchIDFromFilename(name string) string {
	base := strings.TrimSuffix(name, ".json")
	if _, ok := evidenceSequenceFromFilename(name); ok {
		return base[21:]
	}
	return base
}

func quarantineCorruptEvidenceLocked(c config, path string) error {
	batchID := evidenceBatchIDFromFilename(filepath.Base(path))
	if err := recordUnknownEvidenceGapLocked(c, batchID); err != nil {
		return err
	}
	return retainRejectedEvidenceLocked(c, path)
}

func retainRejectedEvidenceLocked(c config, path string) error {
	rejectedDir := filepath.Join(c.evidenceOutboxPath, "rejected")
	if err := os.MkdirAll(rejectedDir, 0o700); err != nil {
		return err
	}
	target := filepath.Join(rejectedDir, filepath.Base(path)+".rejected")
	if _, err := os.Stat(target); err == nil {
		target = filepath.Join(rejectedDir, fmt.Sprintf("%d-%s.rejected", time.Now().UnixNano(), filepath.Base(path)))
	}
	if err := os.Rename(path, target); err != nil {
		return err
	}
	entries, err := os.ReadDir(rejectedDir)
	if err != nil {
		return err
	}
	type rejectedItem struct {
		path string
		mod  time.Time
		size int64
	}
	items := make([]rejectedItem, 0, len(entries))
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		items = append(items, rejectedItem{filepath.Join(rejectedDir, entry.Name()), info.ModTime(), info.Size()})
		total += info.Size()
	}
	sort.Slice(items, func(i, j int) bool { return items[i].mod.Before(items[j].mod) })
	limit := min(c.evidenceOutboxMax/4, int64(16<<20))
	for len(items) > 100 || total > limit {
		item := items[0]
		items = items[1:]
		if err := os.Remove(item.path); err != nil {
			return err
		}
		total -= item.size
	}
	if err := syncDirectory(rejectedDir); err != nil {
		return err
	}
	return syncDirectory(c.evidenceOutboxPath)
}

func postEvidence(ctx context.Context, c config, payload evidenceBatch) (evidenceAck, int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return evidenceAck{}, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.evidenceSinkURL, bytes.NewReader(body))
	if err != nil {
		return evidenceAck{}, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return errors.New("evidence redirect refused") }}
	resp, err := client.Do(req)
	if err != nil {
		return evidenceAck{}, 0, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		return evidenceAck{}, resp.StatusCode, readErr
	}
	if resp.StatusCode != http.StatusOK {
		if reason := evidenceResponseReason(data); reason != "" {
			return evidenceAck{}, resp.StatusCode, fmt.Errorf("monitor evidence returned HTTP %d: %s", resp.StatusCode, reason)
		}
		return evidenceAck{}, resp.StatusCode, fmt.Errorf("monitor evidence returned HTTP %d", resp.StatusCode)
	}
	var ack evidenceAck
	if json.Unmarshal(data, &ack) != nil || !ack.OK || ack.SchemaVersion != evidenceSchemaVersion || ack.BatchID != payload.BatchID || ack.PayloadHash != payload.PayloadHash || ack.Accepted+ack.Rejected != len(payload.Events) {
		return evidenceAck{}, resp.StatusCode, errors.New("evidence acknowledgement mismatch")
	}
	return ack, resp.StatusCode, nil
}

func drainEvidenceOnce(ctx context.Context, c config) error {
	if c.evidenceFSMu != nil {
		c.evidenceFSMu.Lock()
	}
	paths, err := listEvidenceOutbox(c.evidenceOutboxPath)
	if err != nil || len(paths) == 0 {
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		return err
	}
	path := paths[0]
	info, statErr := os.Stat(path)
	if statErr != nil {
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		return statErr
	}
	if info.Size() > maxEvidenceBatchFileBytes {
		err = quarantineCorruptEvidenceLocked(c, path)
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		if err != nil {
			return err
		}
		return errors.New("oversized evidence outbox payload quarantined")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		return err
	}
	var payload evidenceBatch
	if json.Unmarshal(data, &payload) != nil || payload.PayloadHash == "" || evidencePayloadHash(payload) != payload.PayloadHash {
		err = quarantineCorruptEvidenceLocked(c, path)
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		if err != nil {
			return err
		}
		return errors.New("invalid evidence outbox payload quarantined")
	}
	// Telemetry is explicitly outside the immutable payload hash. Refresh it at
	// delivery time and report the state expected after this exact batch is
	// acknowledged, so a quiet node does not leave a stale non-zero backlog.
	used, batches, err := evidenceOutboxStats(c.evidenceOutboxPath)
	if err != nil {
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		return err
	}
	gap, err := loadGapState(c.evidenceOutboxPath)
	if err != nil {
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		return err
	}
	rejectedBytes, rejectedBatches, err := evidenceRejectedStats(c.evidenceOutboxPath)
	if err != nil {
		if c.evidenceFSMu != nil {
			c.evidenceFSMu.Unlock()
		}
		return err
	}
	remainingBytes := used - int64(len(data))
	if remainingBytes < 0 {
		remainingBytes = 0
	}
	payload.Telemetry = evidenceTelemetry{OutboxBytes: remainingBytes, OutboxBatches: max(batches-1, int64(0)), RejectedBytes: rejectedBytes, RejectedBatches: rejectedBatches,
		DroppedEvents: gap.DroppedEvents, UnknownDroppedBatches: gap.UnknownDroppedBatches, GapCount: gap.GapCount, LastGapFromMS: gap.LastGapFromMS, LastGapToMS: gap.LastGapToMS}
	if c.evidenceFSMu != nil {
		c.evidenceFSMu.Unlock()
	}
	ack, status, err := postEvidence(ctx, c, payload)
	if err != nil {
		if status == http.StatusBadRequest || status == http.StatusConflict || status == http.StatusUnprocessableEntity {
			if gapErr := recordEvidenceGap(c, payload, len(payload.Events)); gapErr != nil {
				return gapErr
			}
			if c.evidenceFSMu != nil {
				c.evidenceFSMu.Lock()
				defer c.evidenceFSMu.Unlock()
			}
			return retainRejectedEvidenceLocked(c, path)
		}
		return err
	}
	if ack.Rejected > 0 {
		if err := recordEvidenceGap(c, payload, ack.Rejected); err != nil {
			return err
		}
	}
	if c.evidenceFSMu != nil {
		c.evidenceFSMu.Lock()
		defer c.evidenceFSMu.Unlock()
	}
	if err := os.Remove(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(c.evidenceOutboxPath)
}

func evidenceHeartbeatBatch(c config, now time.Time, value cursor) (evidenceBatch, error) {
	minute := now.Unix() / 60
	digest := sha256.Sum256([]byte(fmt.Sprintf("evidence-heartbeat:%s:%d", c.node, minute)))
	fileID := strconv.FormatUint(value.Inode, 10)
	payload := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: c.node,
		BatchID: "ehb_" + hex.EncodeToString(digest[:16]), LogSchema: value.LastLogSchema, HMACKeyID: c.evidenceHMACKeyID,
		Source: evidenceSourceRange{Kind: "access", FileID: fileID, StartOffset: value.Offset, EndOffset: value.Offset,
			CursorDiscontinuities: value.Discontinuities, LastCursorDiscontinuity: value.LastDiscontinuityAt, DiscardedLines: value.DiscardedLines, LastDiscardedAt: value.LastDiscardedAt,
			EvidenceEligible: value.EvidenceEligible, EvidenceParseRejected: value.EvidenceParseRejected, LastEvidenceParseRejectedAt: value.LastEvidenceParseRejectedAt,
			EvidencePersistFailures: value.EvidencePersistFailures, EvidenceDroppedEvents: value.EvidenceDroppedEvents,
			LastEvidencePersistFailureAt: value.LastEvidencePersistFailureAt}, Events: []evidenceEvent{}}
	if c.evidenceFSMu != nil {
		c.evidenceFSMu.Lock()
		defer c.evidenceFSMu.Unlock()
	}
	used, batches, err := evidenceOutboxStats(c.evidenceOutboxPath)
	if err != nil {
		return evidenceBatch{}, err
	}
	rejectedBytes, rejectedBatches, err := evidenceRejectedStats(c.evidenceOutboxPath)
	if err != nil {
		return evidenceBatch{}, err
	}
	gap, err := loadGapState(c.evidenceOutboxPath)
	if err != nil {
		return evidenceBatch{}, err
	}
	payload.Telemetry = evidenceTelemetry{OutboxBytes: used, OutboxBatches: batches, RejectedBytes: rejectedBytes, RejectedBatches: rejectedBatches,
		DroppedEvents: gap.DroppedEvents, UnknownDroppedBatches: gap.UnknownDroppedBatches, GapCount: gap.GapCount, LastGapFromMS: gap.LastGapFromMS, LastGapToMS: gap.LastGapToMS}
	payload.PayloadHash = evidencePayloadHash(payload)
	return payload, nil
}

func runEvidenceWorker(ctx context.Context, c config) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var nextDrain, nextHeartbeat time.Time
	failureStreak := 0
	for {
		now := time.Now()
		if !now.Before(nextDrain) {
			if err := drainEvidenceOnce(ctx, c); err != nil {
				failureStreak++
				logEvidenceDeliveryError(err)
				nextDrain = now.Add(evidenceRetryDelay(c.node, failureStreak))
			} else {
				failureStreak = 0
				nextDrain = now.Add(time.Second)
			}
		}
		if !now.Before(nextHeartbeat) {
			current, err := loadCursor(c.cursorPath)
			if err == nil {
				var heartbeat evidenceBatch
				heartbeat, err = evidenceHeartbeatBatch(c, now, current)
				if err == nil {
					_, _, err = postEvidence(ctx, c, heartbeat)
				}
			}
			if err != nil {
				logEvidenceDeliveryError(err)
			}
			nextHeartbeat = now.Add(time.Minute)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func evidenceRetryDelay(node string, streak int) time.Duration {
	if streak < 1 {
		return time.Second
	}
	if streak > 6 {
		streak = 6
	}
	delay := 5 * time.Second * time.Duration(1<<uint(streak-1))
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	sum := sha256.Sum256([]byte(node))
	jitter := time.Duration(sum[0]%21) * delay / 100
	return delay + jitter
}

var evidenceLogMu sync.Mutex
var evidenceLastLog time.Time

func logEvidenceDeliveryError(err error) {
	evidenceLogMu.Lock()
	defer evidenceLogMu.Unlock()
	if time.Since(evidenceLastLog) < time.Minute {
		return
	}
	evidenceLastLog = time.Now()
	fmt.Fprintf(os.Stderr, "nginxcollector: evidence lane degraded; minute lane continues: %v\n", err)
}
