package main

// runner_v2.go adapts the established bounded parsers to the exact byte-range
// protocol. It remains behind explicit lane configuration; no default path
// invokes it.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type sourceRangeV2 struct {
	Protocol      int    `json:"protocol"`
	Kind          string `json:"kind"`
	SourceEpoch   string `json:"source_epoch"`
	FileID        string `json:"file_id"`
	StartOffset   int64  `json:"start_offset"`
	EndOffset     int64  `json:"end_offset"`
	ContentSHA256 string `json:"content_sha256"`
}

type sourceCommitAckV2 struct {
	Protocol      int    `json:"protocol"`
	Kind          string `json:"kind"`
	SourceEpoch   string `json:"source_epoch"`
	FileID        string `json:"file_id"`
	StartOffset   int64  `json:"start_offset"`
	EndOffset     int64  `json:"end_offset"`
	NextOffset    int64  `json:"next_offset"`
	ContentSHA256 string `json:"content_sha256"`
	BatchID       string `json:"batch_id"`
}

type sourceV2HTTPClient struct {
	baseURL       string
	token         string
	client        *http.Client
	capabilityMu  sync.Mutex
	verifiedKinds map[string]bool
}

func (h *sourceV2HTTPClient) requireCapabilities(ctx context.Context, kind string) error {
	h.capabilityMu.Lock()
	defer h.capabilityMu.Unlock()
	if h.verifiedKinds[kind] {
		return nil
	}
	var response struct {
		Protocol         int      `json:"protocol"`
		Kinds            []string `json:"kinds"`
		Manifest         bool     `json:"manifest"`
		RangeCAS         bool     `json:"range_cas"`
		SourceEpoch      bool     `json:"source_epoch"`
		ACKEcho          bool     `json:"ack_echo"`
		ACKConfirm       bool     `json:"ack_confirm"`
		Heartbeat        bool     `json:"heartbeat"`
		MaxRangeBytes    int64    `json:"max_range_bytes"`
		MaxPendingBytes  int64    `json:"max_unconfirmed_bytes"`
		MaxPendingRanges int64    `json:"max_unconfirmed_ranges"`
		ServerTime       int64    `json:"server_time"`
	}
	if err := h.doJSONMode(ctx, http.MethodGet, "/internal/nginx-source/v2/capabilities", nil, &response, false); err != nil {
		return fmt.Errorf("verify source v2 capabilities: %w", err)
	}
	kindSupported := false
	for _, supported := range response.Kinds {
		if supported == kind {
			kindSupported = true
			break
		}
	}
	if response.Protocol != sourceCursorVersionV2 || !kindSupported || !response.Manifest || !response.RangeCAS || !response.SourceEpoch || !response.ACKEcho || !response.ACKConfirm || !response.Heartbeat || response.MaxRangeBytes < maxSourceRangeBytesV2 || response.MaxPendingBytes <= 0 || response.MaxPendingRanges <= 0 || response.ServerTime <= 0 {
		return errors.New("monitor does not advertise the required source v2 capabilities")
	}
	h.verifiedKinds[kind] = true
	return nil
}

func newSourceV2HTTPClient(baseURL, token string, allowHTTP bool) (*sourceV2HTTPClient, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if token == "" || baseURL == "" {
		return nil, errors.New("source v2 endpoint and token are required")
	}
	if err := validateSinkURL(baseURL, allowHTTP); err != nil {
		return nil, err
	}
	return &sourceV2HTTPClient{baseURL: baseURL, token: token, verifiedKinds: make(map[string]bool), client: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return errors.New("source v2 redirect refused")
	}}}, nil
}

func (h *sourceV2HTTPClient) doJSON(ctx context.Context, method, path string, in, out any) error {
	return h.doJSONMode(ctx, method, path, in, out, true)
}

func (h *sourceV2HTTPClient) doJSONMode(ctx context.Context, method, path string, in, out any, strict bool) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, 64<<10)
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(limited, 4<<10))
		return fmt.Errorf("monitor source v2 returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, limited)
		return nil
	}
	decoder := json.NewDecoder(limited)
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("monitor source v2 response contains trailing data")
	}
	return nil
}

func (h *sourceV2HTTPClient) registerManifest(ctx context.Context, node string, manifest sourceManifestV2) (string, error) {
	var response struct {
		OK             bool   `json:"ok"`
		ManifestSHA256 string `json:"manifest_sha256"`
		Revision       uint64 `json:"manifest_revision"`
		Duplicate      bool   `json:"duplicate"`
	}
	err := h.doJSON(ctx, http.MethodPost, "/internal/nginx-source/v2/manifest", map[string]any{"node": node, "manifest": manifest}, &response)
	if err != nil {
		return "", err
	}
	if !response.OK || response.Revision != 1 || !nginxHashPatternV2(response.ManifestSHA256) {
		return "", errors.New("invalid source manifest acknowledgement")
	}
	return response.ManifestSHA256, nil
}

func (h *sourceV2HTTPClient) registerFiles(ctx context.Context, node string, registration sourceFileRegistrationV2) (uint64, string, error) {
	var response struct {
		OK             bool   `json:"ok"`
		ManifestSHA256 string `json:"manifest_sha256"`
		Revision       uint64 `json:"manifest_revision"`
		Duplicate      bool   `json:"duplicate"`
	}
	err := h.doJSON(ctx, http.MethodPost, "/internal/nginx-source/v2/files", map[string]any{"node": node, "registration": registration}, &response)
	if err != nil {
		return 0, "", err
	}
	if !response.OK || response.Revision == 0 || !nginxHashPatternV2(response.ManifestSHA256) {
		return 0, "", errors.New("invalid source file registration acknowledgement")
	}
	return response.Revision, response.ManifestSHA256, nil
}

func (h *sourceV2HTTPClient) recover(ctx context.Context, node, kind, epoch, fileID string, offset int64) (sourceAckRecoveryV2, error) {
	query := url.Values{"node": {node}, "kind": {kind}, "source_epoch": {epoch}, "file_id": {fileID}, "client_offset": {fmt.Sprint(offset)}}
	var response sourceAckRecoveryV2
	err := h.doJSON(ctx, http.MethodGet, "/internal/nginx-source/v2/ack?"+query.Encode(), nil, &response)
	return response, err
}

func (h *sourceV2HTTPClient) confirm(ctx context.Context, node, kind, epoch, fileID string, offset int64) error {
	var response struct {
		OK              bool  `json:"ok"`
		ConfirmedOffset int64 `json:"confirmed_offset"`
		Duplicate       bool  `json:"duplicate"`
	}
	err := h.doJSON(ctx, http.MethodPost, "/internal/nginx-source/v2/confirm", map[string]any{
		"node": node, "kind": kind, "source_epoch": epoch, "file_id": fileID, "confirmed_offset": offset,
	}, &response)
	if err != nil {
		return err
	}
	if !response.OK || response.ConfirmedOffset != offset {
		return errors.New("invalid source acknowledgement confirmation")
	}
	return nil
}

func (h *sourceV2HTTPClient) heartbeat(ctx context.Context, payload map[string]any, epoch, fileID string, offset int64) error {
	var response struct {
		OK              bool   `json:"ok"`
		SourceEpoch     string `json:"source_epoch"`
		FileID          string `json:"file_id"`
		ConfirmedOffset int64  `json:"confirmed_offset"`
	}
	if err := h.doJSON(ctx, http.MethodPost, "/internal/nginx-source/v2/heartbeat", payload, &response); err != nil {
		return err
	}
	if !response.OK || response.SourceEpoch != epoch || response.FileID != fileID || response.ConfirmedOffset != offset {
		return errors.New("invalid source heartbeat acknowledgement")
	}
	return nil
}

func (h *sourceV2HTTPClient) postSourceBatch(ctx context.Context, path string, payload any, batchID string, source sourceRangeV2) error {
	var response struct {
		OK          bool               `json:"ok"`
		Duplicate   bool               `json:"duplicate"`
		Stored      int                `json:"stored"`
		SourceAckV2 *sourceCommitAckV2 `json:"source_ack_v2"`
	}
	if err := h.doJSON(ctx, http.MethodPost, path, payload, &response); err != nil {
		return err
	}
	ack := response.SourceAckV2
	if !response.OK || ack == nil || ack.Protocol != source.Protocol || ack.Kind != source.Kind || ack.SourceEpoch != source.SourceEpoch || ack.FileID != source.FileID ||
		ack.StartOffset != source.StartOffset || ack.EndOffset != source.EndOffset || ack.NextOffset != source.EndOffset || ack.ContentSHA256 != source.ContentSHA256 || ack.BatchID != batchID {
		return errors.New("monitor source v2 commit acknowledgement mismatch")
	}
	return nil
}

func v2ParserCursor(state sourceCursorV2, file fileWatermark) cursor {
	t := state.Telemetry
	return cursor{Version: cursorVersion, Device: file.Device, Inode: file.Inode, Offset: file.AckedOffset,
		Discontinuities: t.Discontinuities, LastDiscontinuityAt: t.LastDiscontinuityAt,
		DiscardedLines: t.DiscardedLines, LastDiscardedAt: t.LastDiscardedAt, LastLogSchema: t.LastLogSchema,
		EvidenceEligible: t.EvidenceEligible, EvidenceParseRejected: t.EvidenceParseRejected, LastEvidenceParseRejectedAt: t.LastEvidenceParseRejectedAt,
		EvidencePersistFailures: t.EvidencePersistFailures, EvidenceDroppedEvents: t.EvidenceDroppedEvents, LastEvidencePersistFailureAt: t.LastEvidencePersistFailureAt}
}

func updateV2Telemetry(state *sourceCursorV2, value cursor) {
	state.Telemetry.Discontinuities, state.Telemetry.LastDiscontinuityAt = value.Discontinuities, value.LastDiscontinuityAt
	state.Telemetry.DiscardedLines, state.Telemetry.LastDiscardedAt = value.DiscardedLines, value.LastDiscardedAt
	state.Telemetry.LastLogSchema = value.LastLogSchema
	state.Telemetry.EvidenceEligible, state.Telemetry.EvidenceParseRejected = value.EvidenceEligible, value.EvidenceParseRejected
	state.Telemetry.LastEvidenceParseRejectedAt = value.LastEvidenceParseRejectedAt
	state.Telemetry.EvidencePersistFailures, state.Telemetry.EvidenceDroppedEvents = value.EvidencePersistFailures, value.EvidenceDroppedEvents
	state.Telemetry.LastEvidencePersistFailureAt = value.LastEvidencePersistFailureAt
}

func v2BatchID(node string, source sourceRangeV2) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("v2:%s:%s:%s:%s:%d:%d:%s", node, source.Kind, source.SourceEpoch, source.FileID, source.StartOffset, source.EndOffset, source.ContentSHA256)))
	return hex.EncodeToString(digest[:])
}

func setV2RangeObservation(state *sourceCursorV2, index int, end, now int64) error {
	if index < 0 || index >= len(state.Files) || end <= state.Files[index].AckedOffset {
		return errors.New("invalid source v2 parser range")
	}
	file := &state.Files[index]
	if end > file.LastObservedSize {
		file.LastObservedSize, file.LastGrowthAt = end, now
	}
	file.LastSeenAt, file.State = now, "active"
	return validateSourceCursorV2(*state)
}

func sourceRangeForV2(logPath, kind string, state sourceCursorV2, file fileWatermark, end int64) (sourceRangeV2, int64, string, error) {
	handle, err := openPinnedSourceV2(logPath, file)
	if err != nil {
		return sourceRangeV2{}, 0, "", err
	}
	defer handle.Close()
	hash, err := hashPinnedSourceRangeV2(handle, file.AckedOffset, end, maxSourceRangeBytesV2)
	if err != nil {
		return sourceRangeV2{}, 0, "", err
	}
	anchorStart := max(file.AckedOffset, end-4096)
	anchor, err := hashPinnedSourceRangeV2(handle, anchorStart, end, 4096)
	if err != nil {
		return sourceRangeV2{}, 0, "", err
	}
	return sourceRangeV2{Protocol: sourceCursorVersionV2, Kind: kind, SourceEpoch: state.SourceEpoch, FileID: file.FileID, StartOffset: file.AckedOffset, EndOffset: end, ContentSHA256: hash}, anchorStart, anchor, nil
}

func v2BacklogIntoAccess(state sourceCursorV2, payload *batch) {
	payload.BacklogBytes, payload.BacklogKnown = sourceBacklogV2(state)
}

func v2BacklogIntoError(state sourceCursorV2, payload *errorBatch) {
	payload.BacklogBytes, payload.BacklogKnown = sourceBacklogV2(state)
}

func nowUnixV2() int64 { return time.Now().Unix() }

func ensureSourceProtocolV2(ctx context.Context, c config, logPath, legacyCursorPath, v2CursorPath, kind string, httpClient *sourceV2HTTPClient, now int64) (sourceCursorV2, error) {
	if err := httpClient.requireCapabilities(ctx, kind); err != nil {
		return sourceCursorV2{}, err
	}
	// The legacy cursor is a one-time migration input. Once the v2 cursor has
	// been fsynced, its epoch and per-file watermarks are authoritative and the
	// old cursor may be removed without stopping the lane.
	var legacy cursor
	if _, err := loadSourceCursorV2(v2CursorPath); errors.Is(err, os.ErrNotExist) {
		legacy, err = loadCursor(legacyCursorPath)
		if err != nil {
			return sourceCursorV2{}, err
		}
	} else if err != nil {
		return sourceCursorV2{}, fmt.Errorf("load source v2 cursor: %w", err)
	}
	state, err := bootstrapSourceProtocolV2(logPath, v2CursorPath, kind, legacy, now, saveSourceCursorV2, func(manifest sourceManifestV2) (string, error) {
		return httpClient.registerManifest(ctx, c.node, manifest)
	})
	if err != nil {
		return sourceCursorV2{}, err
	}
	state, err = resumeSourceFileRegistrationV2(logPath, v2CursorPath, kind, now, saveSourceCursorV2, func(registration sourceFileRegistrationV2) (uint64, string, error) {
		return httpClient.registerFiles(ctx, c.node, registration)
	})
	if err != nil {
		return sourceCursorV2{}, err
	}
	// Rebind names before proof verification; an accepted file may have been
	// renamed by logrotate while the HTTP acknowledgement was in flight.
	candidates, err := listSourceCandidatesV2(logPath)
	if err != nil {
		return sourceCursorV2{}, err
	}
	state, err = rebindPendingSourcePathsV2(state, candidates)
	if err != nil {
		return sourceCursorV2{}, err
	}
	for i := range state.Files {
		file := state.Files[i]
		if !file.Registered || !file.RecoveryPending || file.State == "retired" || file.State == "lost" {
			continue
		}
		proof, err := httpClient.recover(ctx, c.node, kind, state.SourceEpoch, file.FileID, file.AckedOffset)
		if err != nil {
			return sourceCursorV2{}, err
		}
		if proof.NextOffset > file.AckedOffset {
			state, err = recoverSourceAcknowledgementsV2(state, logPath, proof, now)
			if err != nil {
				return sourceCursorV2{}, err
			}
			if err := saveSourceCursorV2(v2CursorPath, state); err != nil {
				return sourceCursorV2{}, fmt.Errorf("persist recovered source acknowledgement: %w", err)
			}
		}
		for _, updated := range state.Files {
			if updated.FileID == file.FileID {
				if err := httpClient.confirm(ctx, c.node, kind, state.SourceEpoch, file.FileID, updated.AckedOffset); err != nil {
					return sourceCursorV2{}, err
				}
				if err := setSourceRecoveryPendingV2(&state, file.FileID, false); err != nil {
					return sourceCursorV2{}, err
				}
				break
			}
		}
	}
	state, _, err = applyWriterReleaseProofV2(state, logPath, defaultWriterReleaseProofPathV2(logPath), now)
	if err != nil {
		return sourceCursorV2{}, fmt.Errorf("verify source writer release: %w", err)
	}
	if err := saveSourceCursorV2(v2CursorPath, state); err != nil {
		return sourceCursorV2{}, err
	}
	return state, nil
}

func recoverAcceptedSourceRangeV2(ctx context.Context, c config, httpClient *sourceV2HTTPClient, logPath, cursorPath, kind string, state sourceCursorV2, file fileWatermark, postErr error, now int64) error {
	proof, err := httpClient.recover(ctx, c.node, kind, state.SourceEpoch, file.FileID, file.AckedOffset)
	if err != nil || proof.NextOffset == file.AckedOffset {
		return postErr
	}
	candidates, err := listSourceCandidatesV2(logPath)
	if err != nil {
		return err
	}
	state, err = rebindPendingSourcePathsV2(state, candidates)
	if err != nil {
		return err
	}
	state, err = recoverSourceAcknowledgementsV2(state, logPath, proof, now)
	if err != nil {
		return err
	}
	if err := saveSourceCursorV2(cursorPath, state); err != nil {
		return err
	}
	var offset int64
	for _, updated := range state.Files {
		if updated.FileID == file.FileID {
			offset = updated.AckedOffset
			break
		}
	}
	if err := httpClient.confirm(ctx, c.node, kind, state.SourceEpoch, file.FileID, offset); err != nil {
		return err
	}
	if err := setSourceRecoveryPendingV2(&state, file.FileID, false); err != nil {
		return err
	}
	return saveSourceCursorV2(cursorPath, state)
}

func runAccessSourceV2Once(ctx context.Context, c config, v2CursorPath string, httpClient *sourceV2HTTPClient) error {
	lock, err := acquireSourceCursorLockV2(v2CursorPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	now := nowUnixV2()
	state, err := ensureSourceProtocolV2(ctx, c, c.logPath, c.cursorPath, v2CursorPath, "access", httpClient, now)
	if err != nil {
		return err
	}
	index, ok := selectReadableWatermarkV2(&state)
	if !ok {
		return saveSourceCursorV2(v2CursorPath, state)
	}
	file := state.Files[index]
	parserConfig := c
	parserConfig.sourceV2Epoch, parserConfig.sourceV2FileID = state.SourceEpoch, file.FileID
	payload, next, parsed, err := readBatch(parserConfig, v2ParserCursor(state, file))
	if err != nil {
		return err
	}
	end := file.AckedOffset
	if parsed {
		end = next.Offset
	} else if (next.Device != file.Device || next.Inode != file.Inode) && !file.Current {
		end = file.LastObservedSize
	}
	if end <= file.AckedOffset {
		return saveSourceCursorV2(v2CursorPath, state)
	}
	if end-file.AckedOffset > maxSourceRangeBytesV2 {
		return errors.New("parsed source range exceeds v2 byte limit")
	}
	updateV2Telemetry(&state, next)
	if err := setV2RangeObservation(&state, index, end, now); err != nil {
		return err
	}
	source, anchorStart, anchor, err := sourceRangeForV2(c.logPath, "access", state, file, end)
	if err != nil {
		return err
	}
	payload.Node, payload.SourceBoundary, payload.SourceRangeV2 = c.node, nil, &source
	payload.BatchID = v2BatchID(c.node, source)
	payload.CursorDiscontinuities, payload.LastCursorDiscontinuityAt = state.Telemetry.Discontinuities, state.Telemetry.LastDiscontinuityAt
	payload.DiscardedLines, payload.LastDiscardedAt = state.Telemetry.DiscardedLines, state.Telemetry.LastDiscardedAt
	v2BacklogIntoAccess(state, &payload)
	if payload.Evidence != nil {
		payload.Evidence.BatchID = payload.BatchID
		payload.Evidence.Source.ContentSHA256 = source.ContentSHA256
		payload.Evidence.PayloadHash = evidencePayloadHash(*payload.Evidence)
		if err := spoolEvidence(c, *payload.Evidence); err != nil {
			payload.EvidencePersistFailures = 1
			payload.EvidenceDroppedEvents = int64(len(payload.Evidence.Events))
			state.Telemetry.EvidencePersistFailures++
			state.Telemetry.EvidenceDroppedEvents += int64(len(payload.Evidence.Events))
			state.Telemetry.LastEvidencePersistFailureAt = now
			logEvidenceDeliveryError(fmt.Errorf("persist evidence outbox: %w", err))
		}
	}
	if err := setSourceRecoveryPendingV2(&state, file.FileID, true); err != nil {
		return err
	}
	if err := saveSourceCursorV2(v2CursorPath, state); err != nil {
		return fmt.Errorf("persist access recovery marker before POST: %w", err)
	}
	if err := httpClient.postSourceBatch(ctx, "/internal/nginx", payload, payload.BatchID, source); err != nil {
		return recoverAcceptedSourceRangeV2(ctx, c, httpClient, c.logPath, v2CursorPath, "access", state, file, err, now)
	}
	if err := acknowledgeSourceRangeV2(&state, file.FileID, file.AckedOffset, end, anchorStart, anchor, now); err != nil {
		return err
	}
	if err := saveSourceCursorV2(v2CursorPath, state); err != nil {
		return err
	}
	if err := httpClient.confirm(ctx, c.node, "access", state.SourceEpoch, file.FileID, end); err != nil {
		return err
	}
	if err := setSourceRecoveryPendingV2(&state, file.FileID, false); err != nil {
		return err
	}
	return saveSourceCursorV2(v2CursorPath, state)
}

func runErrorSourceV2Once(ctx context.Context, c config, v2CursorPath string, httpClient *sourceV2HTTPClient) error {
	lock, err := acquireSourceCursorLockV2(v2CursorPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	now := nowUnixV2()
	state, err := ensureSourceProtocolV2(ctx, c, c.errorLogPath, c.errorCursorPath, v2CursorPath, "error", httpClient, now)
	if err != nil {
		return err
	}
	index, ok := selectReadableWatermarkV2(&state)
	if !ok {
		return saveSourceCursorV2(v2CursorPath, state)
	}
	file := state.Files[index]
	payload, next, parsed, err := readErrorBatch(c, v2ParserCursor(state, file))
	if err != nil {
		return err
	}
	end := file.AckedOffset
	if parsed {
		end = next.Offset
	} else if (next.Device != file.Device || next.Inode != file.Inode) && !file.Current {
		end = file.LastObservedSize
	}
	if end <= file.AckedOffset {
		return saveSourceCursorV2(v2CursorPath, state)
	}
	if end-file.AckedOffset > maxSourceRangeBytesV2 {
		return errors.New("parsed error source range exceeds v2 byte limit")
	}
	updateV2Telemetry(&state, next)
	if err := setV2RangeObservation(&state, index, end, now); err != nil {
		return err
	}
	source, anchorStart, anchor, err := sourceRangeForV2(c.errorLogPath, "error", state, file, end)
	if err != nil {
		return err
	}
	payload.Node, payload.SourceBoundary, payload.SourceRangeV2 = c.node, nil, &source
	payload.BatchID = v2BatchID(c.node, source)
	payload.CursorDiscontinuities, payload.LastCursorDiscontinuityAt = state.Telemetry.Discontinuities, state.Telemetry.LastDiscontinuityAt
	payload.DiscardedLines, payload.LastDiscardedAt = state.Telemetry.DiscardedLines, state.Telemetry.LastDiscardedAt
	v2BacklogIntoError(state, &payload)
	if err := setSourceRecoveryPendingV2(&state, file.FileID, true); err != nil {
		return err
	}
	if err := saveSourceCursorV2(v2CursorPath, state); err != nil {
		return fmt.Errorf("persist error recovery marker before POST: %w", err)
	}
	if err := httpClient.postSourceBatch(ctx, "/internal/nginx-errors", payload, payload.BatchID, source); err != nil {
		return recoverAcceptedSourceRangeV2(ctx, c, httpClient, c.errorLogPath, v2CursorPath, "error", state, file, err, now)
	}
	if err := acknowledgeSourceRangeV2(&state, file.FileID, file.AckedOffset, end, anchorStart, anchor, now); err != nil {
		return err
	}
	if err := saveSourceCursorV2(v2CursorPath, state); err != nil {
		return err
	}
	if err := httpClient.confirm(ctx, c.node, "error", state.SourceEpoch, file.FileID, end); err != nil {
		return err
	}
	if err := setSourceRecoveryPendingV2(&state, file.FileID, false); err != nil {
		return err
	}
	return saveSourceCursorV2(v2CursorPath, state)
}

func runSourceV2Heartbeat(ctx context.Context, c config, logPath, legacyCursorPath, v2CursorPath, kind string, httpClient *sourceV2HTTPClient) error {
	lock, err := acquireSourceCursorLockV2(v2CursorPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	now := nowUnixV2()
	state, err := ensureSourceProtocolV2(ctx, c, logPath, legacyCursorPath, v2CursorPath, kind, httpClient, now)
	if err != nil {
		return err
	}
	var current *fileWatermark
	for i := range state.Files {
		if state.Files[i].Current && state.Files[i].Registered && state.Files[i].State != "lost" && state.Files[i].State != "missing" && state.Files[i].State != "retired" {
			current = &state.Files[i]
			break
		}
	}
	if current == nil {
		return errors.New("source v2 heartbeat has no registered current file")
	}
	backlog, known := sourceBacklogV2(state)
	payload := map[string]any{
		"node": c.node, "kind": kind, "source_epoch": state.SourceEpoch, "file_id": current.FileID, "confirmed_offset": current.AckedOffset,
		"backlog_bytes": backlog, "backlog_known": known,
		"cursor_discontinuities": state.Telemetry.Discontinuities, "last_cursor_discontinuity_at": state.Telemetry.LastDiscontinuityAt,
		"discarded_lines": state.Telemetry.DiscardedLines, "last_discarded_at": state.Telemetry.LastDiscardedAt,
	}
	if kind == "access" {
		payload["evidence_persist_failures"] = state.Telemetry.EvidencePersistFailures
		payload["evidence_dropped_events"] = state.Telemetry.EvidenceDroppedEvents
		payload["last_evidence_persist_failure_at"] = state.Telemetry.LastEvidencePersistFailureAt
	}
	return httpClient.heartbeat(ctx, payload, state.SourceEpoch, current.FileID, current.AckedOffset)
}
