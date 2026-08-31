package monitor

// This file contains the isolated source-continuity protocol for explicitly
// allowlisted Nginx collector lanes. Default production ingest remains on v1;
// source ranges, ordinary batches, aggregates and evidence are joined through
// durable manifest/range transactions after a one-way per-lane cutover.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const nginxSourceProtocolV2 = 2
const maxNginxSourceRangeBytesV2 = int64(128 << 20)
const maxNginxSourceUnconfirmedBytesV2 = int64(1 << 30)
const maxNginxSourceUnconfirmedRangesV2 = int64(64)

var (
	nginxSourceEpochV2Pattern  = regexp.MustCompile(`^[0-9a-f]{32}$`)
	nginxSourceFileV2Pattern   = regexp.MustCompile(`^[0-9a-f]{32}-[0-9a-f]{16}$`)
	nginxSourceHashV2Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	errNginxSourceOverlap      = errors.New("nginx source range overlaps accepted bytes")
	errNginxSourceGap          = errors.New("nginx source range has a gap")
	errNginxSourceConflict     = errors.New("nginx source range was reused with conflicting identity")
	errNginxSourceEpoch        = errors.New("nginx source epoch changed after protocol cutover")
	errNginxSourceUnregistered = errors.New("nginx source file is not registered in the manifest")
	errNginxLegacyAfterV2      = errors.New("legacy nginx ingest is forbidden after protocol v2 cutover")
	errNginxSourceBackpressure = errors.New("nginx source unconfirmed range limit reached")
)

// nginxSourceBoundaryV1 binds a legacy aggregate batch to the exact local
// file position that produced it. Older collectors may omit it and continue
// on v1, but a v2 cutover is refused until an acknowledged boundary exists.
type nginxSourceBoundaryV1 struct {
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Checkpoint  bool   `json:"checkpoint,omitempty"`
}

type nginxSourceBoundaryAckV1 struct {
	Protocol    int    `json:"protocol"`
	BatchID     string `json:"batch_id"`
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Checkpoint  bool   `json:"checkpoint,omitempty"`
}

func nginxSourceBoundaryAckForV1(boundary *nginxSourceBoundaryV1, batchID string) *nginxSourceBoundaryAckV1 {
	if boundary == nil {
		return nil
	}
	return &nginxSourceBoundaryAckV1{Protocol: 1, BatchID: batchID, Device: boundary.Device, Inode: boundary.Inode,
		StartOffset: boundary.StartOffset, EndOffset: boundary.EndOffset, Checkpoint: boundary.Checkpoint}
}

func validateNginxSourceBoundaryV1(boundary *nginxSourceBoundaryV1) error {
	if boundary == nil {
		return nil
	}
	if boundary.Device == 0 || boundary.Inode == 0 || boundary.StartOffset < 0 || boundary.EndOffset < boundary.StartOffset || boundary.EndOffset > 1<<50 {
		return errors.New("invalid nginx v1 source boundary")
	}
	if boundary.Checkpoint {
		if boundary.StartOffset != boundary.EndOffset {
			return errors.New("invalid nginx v1 source checkpoint")
		}
		return nil
	}
	if boundary.EndOffset == boundary.StartOffset || boundary.EndOffset-boundary.StartOffset > maxNginxSourceRangeBytesV2 {
		return errors.New("invalid nginx v1 source range")
	}
	return nil
}

type nginxSourceRangeV2 struct {
	Protocol      int    `json:"protocol"`
	Kind          string `json:"kind"`
	SourceEpoch   string `json:"source_epoch"`
	FileID        string `json:"file_id"`
	StartOffset   int64  `json:"start_offset"`
	EndOffset     int64  `json:"end_offset"`
	ContentSHA256 string `json:"content_sha256"`
}

type nginxSourceManifestFileV2 struct {
	FileID     string `json:"file_id"`
	Generation uint64 `json:"generation"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	BaseOffset int64  `json:"base_offset"`
	Current    bool   `json:"current"`
}

type nginxSourceManifestV2 struct {
	Protocol           int                         `json:"protocol"`
	Kind               string                      `json:"kind"`
	SourceEpoch        string                      `json:"source_epoch"`
	CutoverFromV1      bool                        `json:"cutover_from_v1"`
	LegacyCursorInode  uint64                      `json:"legacy_cursor_inode,omitempty"`
	LegacyCursorDevice uint64                      `json:"legacy_cursor_device,omitempty"`
	LegacyCursorOffset int64                       `json:"legacy_cursor_offset,omitempty"`
	LegacyAckedBatchID string                      `json:"legacy_acked_batch_id,omitempty"`
	Files              []nginxSourceManifestFileV2 `json:"files"`
}

type nginxSourceFileRegistrationV2 struct {
	Protocol             int                         `json:"protocol"`
	Kind                 string                      `json:"kind"`
	SourceEpoch          string                      `json:"source_epoch"`
	ExpectedRevision     uint64                      `json:"expected_revision"`
	PreviousManifestHash string                      `json:"previous_manifest_hash"`
	Files                []nginxSourceManifestFileV2 `json:"files"`
}

type nginxSourceManifestRequestV2 struct {
	Node     string                `json:"node"`
	Manifest nginxSourceManifestV2 `json:"manifest"`
}

type nginxSourceFileRegistrationRequestV2 struct {
	Node         string                        `json:"node"`
	Registration nginxSourceFileRegistrationV2 `json:"registration"`
}

type nginxSourceAckConfirmationRequestV2 struct {
	Node            string `json:"node"`
	Kind            string `json:"kind"`
	SourceEpoch     string `json:"source_epoch"`
	FileID          string `json:"file_id"`
	ConfirmedOffset int64  `json:"confirmed_offset"`
}

type nginxSourceHeartbeatRequestV2 struct {
	Node                         string `json:"node"`
	Kind                         string `json:"kind"`
	SourceEpoch                  string `json:"source_epoch"`
	FileID                       string `json:"file_id"`
	ConfirmedOffset              int64  `json:"confirmed_offset"`
	BacklogBytes                 int64  `json:"backlog_bytes"`
	BacklogKnown                 bool   `json:"backlog_known"`
	CursorDiscontinuities        int64  `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt    int64  `json:"last_cursor_discontinuity_at"`
	DiscardedLines               int64  `json:"discarded_lines"`
	LastDiscardedAt              int64  `json:"last_discarded_at"`
	EvidencePersistFailures      int64  `json:"evidence_persist_failures,omitempty"`
	EvidenceDroppedEvents        int64  `json:"evidence_dropped_events,omitempty"`
	LastEvidencePersistFailureAt int64  `json:"last_evidence_persist_failure_at,omitempty"`
}

type NginxCollectorProtocolState struct {
	Node             string `gorm:"primaryKey;size:64"`
	Kind             string `gorm:"primaryKey;size:8"`
	Protocol         int
	SourceEpoch      string `gorm:"size:32;not null"`
	ManifestSHA256   string `gorm:"size:64;not null"`
	ManifestRevision uint64
	CutoverAt        int64
	UpdatedAt        int64
	RecoveryRequired bool
	ContinuityBroken bool
	LastRecoveryAt   int64
}

// NginxSourceFileWatermark is compact continuity state: one row per observed
// physical file. Fine-grained range rows are retained only until the collector
// confirms its matching cursor was durably fsynced.
type NginxSourceFileWatermark struct {
	Node            string `gorm:"primaryKey;size:64;uniqueIndex:idx_nginx_source_physical,priority:1"`
	Kind            string `gorm:"primaryKey;size:8;uniqueIndex:idx_nginx_source_physical,priority:2"`
	SourceEpoch     string `gorm:"primaryKey;size:32;uniqueIndex:idx_nginx_source_physical,priority:3"`
	FileID          string `gorm:"primaryKey;size:49"`
	Generation      uint64
	Device          uint64 `gorm:"uniqueIndex:idx_nginx_source_physical,priority:4"`
	Inode           uint64 `gorm:"uniqueIndex:idx_nginx_source_physical,priority:5"`
	BaseOffset      int64
	NextOffset      int64
	ConfirmedOffset int64
	Batches         int64
	LastContentHash string `gorm:"size:64"`
	RegisteredAt    int64
	UpdatedAt       int64
	State           string `gorm:"size:16"`
	Current         bool
}

type NginxSourceRangeBatch struct {
	Node        string `gorm:"primaryKey;size:64;uniqueIndex:idx_nginx_source_batch_identity,priority:1"`
	Kind        string `gorm:"primaryKey;size:8;uniqueIndex:idx_nginx_source_batch_identity,priority:2"`
	SourceEpoch string `gorm:"primaryKey;size:32"`
	FileID      string `gorm:"primaryKey;size:49"`
	StartOffset int64  `gorm:"primaryKey;autoIncrement:false"`
	EndOffset   int64  `gorm:"primaryKey;autoIncrement:false"`
	BatchID     string `gorm:"size:64;not null;uniqueIndex:idx_nginx_source_batch_identity,priority:3"`
	PayloadHash string `gorm:"size:64;not null"`
	ContentHash string `gorm:"size:64;not null"`
	ReceivedAt  int64  `gorm:"index"`
}

// NginxSourceBoundaryBatchV1 stores cutover preparation outside the existing
// aggregate/idempotency tables. This lets a default-off Monitor upgrade leave
// all deployed Nginx tables byte-for-byte schema compatible.
type NginxSourceBoundaryBatchV1 struct {
	Node        string `gorm:"primaryKey;size:64"`
	Kind        string `gorm:"primaryKey;size:8"`
	BatchID     string `gorm:"primaryKey;size:64"`
	PayloadHash string `gorm:"size:64;not null"`
	Device      uint64
	Inode       uint64
	StartOffset int64
	EndOffset   int64
	Checkpoint  bool
	ReceivedAt  int64 `gorm:"index"`
}

type NginxSourceBoundaryStateV1 struct {
	Node          string `gorm:"primaryKey;size:64"`
	Kind          string `gorm:"primaryKey;size:8"`
	LastBatchID   string `gorm:"size:64;not null"`
	Device        uint64
	Inode         uint64
	LastOffset    int64
	LastUpdatedAt int64
}

// NginxSourceCommitV2 is the compact permanent join between an accepted
// aggregate batch and its exact source bytes. Recovery ranges may be pruned
// after collector fsync; evidence delivery can still arrive later and prove
// it belongs to the same committed batch.
type NginxSourceCommitV2 struct {
	Node        string `gorm:"primaryKey;size:64"`
	Kind        string `gorm:"primaryKey;size:8"`
	BatchID     string `gorm:"primaryKey;size:64"`
	SourceEpoch string `gorm:"size:32;not null"`
	FileID      string `gorm:"size:49;not null"`
	StartOffset int64
	EndOffset   int64
	ContentHash string `gorm:"size:64;not null"`
	PayloadHash string `gorm:"size:64;not null"`
	ReceivedAt  int64  `gorm:"index"`
}

type nginxSourceAckProofV2 struct {
	StartOffset   int64  `json:"start_offset"`
	EndOffset     int64  `json:"end_offset"`
	ContentSHA256 string `json:"content_sha256"`
}

type nginxSourceAckRecoveryV2 struct {
	SourceEpoch string                  `json:"source_epoch"`
	FileID      string                  `json:"file_id"`
	NextOffset  int64                   `json:"next_offset"`
	Proofs      []nginxSourceAckProofV2 `json:"proofs"`
}

type nginxSourceCommitAckV2 struct {
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

func nginxSourceCommitAckForV2(source *nginxSourceRangeV2, batchID string) *nginxSourceCommitAckV2 {
	if source == nil {
		return nil
	}
	return &nginxSourceCommitAckV2{Protocol: source.Protocol, Kind: source.Kind, SourceEpoch: source.SourceEpoch, FileID: source.FileID,
		StartOffset: source.StartOffset, EndOffset: source.EndOffset, NextOffset: source.EndOffset,
		ContentSHA256: source.ContentSHA256, BatchID: batchID}
}

// NginxCollectorEpochRecovery is append-only audit for an operator-authorized
// recovery. Old epoch watermarks and ranges are retained.
type NginxCollectorEpochRecovery struct {
	ID              uint   `gorm:"primaryKey"`
	Node            string `gorm:"size:64;index"`
	Kind            string `gorm:"size:8;index"`
	OldSourceEpoch  string `gorm:"size:32"`
	NewSourceEpoch  string `gorm:"size:32"`
	OldManifestHash string `gorm:"size:64"`
	NewManifestHash string `gorm:"size:64"`
	Actor           string `gorm:"size:128"`
	Reason          string `gorm:"size:512"`
	RecoveredAt     int64  `gorm:"index"`
}

type nginxSourceV2TableSpec struct {
	table         string
	columns       []string
	primaryKey    []string
	uniqueIndexes map[string][]string
}

var nginxSourceV2SchemaSpecs = []nginxSourceV2TableSpec{
	{table: "nginx_source_boundary_batch_v1", columns: []string{"node", "kind", "batch_id", "payload_hash", "device", "inode", "start_offset", "end_offset", "checkpoint", "received_at"}, primaryKey: []string{"node", "kind", "batch_id"}},
	{table: "nginx_source_boundary_state_v1", columns: []string{"node", "kind", "last_batch_id", "device", "inode", "last_offset", "last_updated_at"}, primaryKey: []string{"node", "kind"}},
	{table: "nginx_collector_protocol_states", columns: []string{"node", "kind", "protocol", "source_epoch", "manifest_sha256", "manifest_revision", "cutover_at", "updated_at", "recovery_required", "continuity_broken", "last_recovery_at"}, primaryKey: []string{"node", "kind"}},
	{table: "nginx_source_file_watermarks", columns: []string{"node", "kind", "source_epoch", "file_id", "generation", "device", "inode", "base_offset", "next_offset", "confirmed_offset", "batches", "last_content_hash", "registered_at", "updated_at", "state", "current"}, primaryKey: []string{"node", "kind", "source_epoch", "file_id"}, uniqueIndexes: map[string][]string{"idx_nginx_source_physical": {"node", "kind", "source_epoch", "device", "inode"}}},
	{table: "nginx_source_range_batches", columns: []string{"node", "kind", "source_epoch", "file_id", "start_offset", "end_offset", "batch_id", "payload_hash", "content_hash", "received_at"}, primaryKey: []string{"node", "kind", "source_epoch", "file_id", "start_offset", "end_offset"}, uniqueIndexes: map[string][]string{"idx_nginx_source_batch_identity": {"node", "kind", "batch_id"}}},
	{table: "nginx_source_commit_v2", columns: []string{"node", "kind", "batch_id", "source_epoch", "file_id", "start_offset", "end_offset", "content_hash", "payload_hash", "received_at"}, primaryKey: []string{"node", "kind", "batch_id"}},
	{table: "nginx_collector_epoch_recoveries", columns: []string{"id", "node", "kind", "old_source_epoch", "new_source_epoch", "old_manifest_hash", "new_manifest_hash", "actor", "reason", "recovered_at"}, primaryKey: []string{"id"}},
}

// inspectNginxSourceV2Schema verifies the v22 contract, not merely the seven
// table names. An interrupted AutoMigrate can leave all names present while a
// required column, primary key or CAS uniqueness constraint is missing; that
// state must fail closed before handlers accept an irreversible cutover.
func inspectNginxSourceV2Schema(ctx context.Context, q sqliteQueryer) (bool, error) {
	present := 0
	for _, spec := range nginxSourceV2SchemaSpecs {
		var count int
		if err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", spec.table).Scan(&count); err != nil {
			return false, err
		}
		if count == 1 {
			present++
		}
	}
	if present == 0 {
		return false, nil
	}
	if present != len(nginxSourceV2SchemaSpecs) {
		return false, fmt.Errorf("nginx source v2 schema is incomplete: %d/%d tables", present, len(nginxSourceV2SchemaSpecs))
	}
	for _, spec := range nginxSourceV2SchemaSpecs {
		if err := inspectNginxSourceV2Table(ctx, q, spec); err != nil {
			return false, err
		}
	}
	return true, nil
}

func inspectNginxSourceV2Table(ctx context.Context, q sqliteQueryer, spec nginxSourceV2TableSpec) error {
	rows, err := q.QueryContext(ctx, "PRAGMA table_info("+quoteSQLiteIdentifier(spec.table)+")")
	if err != nil {
		return err
	}
	columns := map[string]bool{}
	primary := map[int]string{}
	for rows.Next() {
		var cid, notNull, primaryPosition int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryPosition); err != nil {
			_ = rows.Close()
			return err
		}
		columns[name] = true
		if primaryPosition > 0 {
			primary[primaryPosition] = name
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, column := range spec.columns {
		if !columns[column] {
			return fmt.Errorf("nginx source v2 table %s is missing required column %s", spec.table, column)
		}
	}
	if len(primary) != len(spec.primaryKey) {
		return fmt.Errorf("nginx source v2 table %s has an invalid primary key", spec.table)
	}
	for i, column := range spec.primaryKey {
		if primary[i+1] != column {
			return fmt.Errorf("nginx source v2 table %s has an invalid primary key", spec.table)
		}
	}
	for index, columns := range spec.uniqueIndexes {
		if err := inspectNginxSourceV2UniqueIndex(ctx, q, spec.table, index, columns); err != nil {
			return err
		}
	}
	return nil
}

func inspectNginxSourceV2UniqueIndex(ctx context.Context, q sqliteQueryer, table, index string, expected []string) error {
	rows, err := q.QueryContext(ctx, "PRAGMA index_list("+quoteSQLiteIdentifier(table)+")")
	if err != nil {
		return err
	}
	found, unique, partial := false, false, false
	for rows.Next() {
		var seq, uniqueValue, partialValue int
		var name, origin string
		if err := rows.Scan(&seq, &name, &uniqueValue, &origin, &partialValue); err != nil {
			_ = rows.Close()
			return err
		}
		if name == index {
			found, unique, partial = true, uniqueValue == 1, partialValue != 0
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !found || !unique || partial {
		return fmt.Errorf("nginx source v2 table %s is missing unique index %s", table, index)
	}
	rows, err = q.QueryContext(ctx, "PRAGMA index_info("+quoteSQLiteIdentifier(index)+")")
	if err != nil {
		return err
	}
	actual := make([]string, 0, len(expected))
	for rows.Next() {
		var seq, cid int
		var name string
		if err := rows.Scan(&seq, &cid, &name); err != nil {
			_ = rows.Close()
			return err
		}
		actual = append(actual, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("nginx source v2 index %s has invalid columns", index)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return fmt.Errorf("nginx source v2 index %s has invalid columns", index)
		}
	}
	return nil
}

// migrateNginxSourceV2Schema creates only the isolated protocol-v2 ledger.
// Callers must invoke it during startup only after the explicit v2 feature
// flag has been validated; disabled installations must not acquire these
// tables merely by upgrading Monitor.
func migrateNginxSourceV2Schema(db *gorm.DB) error {
	if db == nil {
		return errors.New("nginx source v2 store is unavailable")
	}
	return db.AutoMigrate(
		&NginxSourceBoundaryBatchV1{},
		&NginxSourceBoundaryStateV1{},
		&NginxCollectorProtocolState{},
		&NginxSourceFileWatermark{},
		&NginxSourceRangeBatch{},
		&NginxSourceCommitV2{},
		&NginxCollectorEpochRecovery{},
	)
}

func detectNginxSourceV2Schema(db *gorm.DB) (bool, error) {
	if db == nil {
		return false, errors.New("nginx source v2 store is unavailable")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return false, err
	}
	return inspectNginxSourceV2Schema(context.Background(), sqlDB)
}

func applyNginxSourceBoundaryV1(tx *gorm.DB, node, kind, batchID, payloadHash string, boundary nginxSourceBoundaryV1, ordinaryBatchDuplicate bool, now int64) error {
	if tx == nil || !nginxNodeNamePattern.MatchString(node) || (kind != "access" && kind != "error") || !validIngestBatchID(batchID) ||
		!nginxSourceHashV2Pattern.MatchString(payloadHash) || validateNginxSourceBoundaryV1(&boundary) != nil || now <= 0 {
		return errors.New("invalid nginx v1 source boundary envelope")
	}
	var existing NginxSourceBoundaryBatchV1
	err := tx.First(&existing, "node = ? AND kind = ? AND batch_id = ?", node, kind, batchID).Error
	if err == nil {
		if existing.PayloadHash != payloadHash || existing.Device != boundary.Device || existing.Inode != boundary.Inode ||
			existing.StartOffset != boundary.StartOffset || existing.EndOffset != boundary.EndOffset || existing.Checkpoint != boundary.Checkpoint {
			return errNginxBatchConflict
		}
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if ordinaryBatchDuplicate {
		latestBatchID := ""
		if kind == "access" {
			var state NginxSourceState
			if err := tx.First(&state, "node = ?", node).Error; err != nil {
				return errNginxBatchConflict
			}
			latestBatchID = state.LastBatchID
		} else {
			var state NginxErrorSourceState
			if err := tx.First(&state, "node = ?", node).Error; err != nil {
				return errNginxBatchConflict
			}
			latestBatchID = state.LastBatchID
		}
		if latestBatchID != batchID {
			return errNginxBatchConflict
		}
	}
	row := NginxSourceBoundaryBatchV1{Node: node, Kind: kind, BatchID: batchID, PayloadHash: payloadHash, Device: boundary.Device, Inode: boundary.Inode,
		StartOffset: boundary.StartOffset, EndOffset: boundary.EndOffset, Checkpoint: boundary.Checkpoint, ReceivedAt: now}
	if err := tx.Create(&row).Error; err != nil {
		return err
	}
	state := NginxSourceBoundaryStateV1{Node: node, Kind: kind, LastBatchID: batchID, Device: boundary.Device, Inode: boundary.Inode, LastOffset: boundary.EndOffset, LastUpdatedAt: now}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}, {Name: "kind"}}, DoUpdates: clause.Assignments(map[string]any{
		"last_batch_id": batchID, "device": boundary.Device, "inode": boundary.Inode, "last_offset": boundary.EndOffset, "last_updated_at": now,
	})}).Create(&state).Error
}

func validNginxSourceRangeV2(source nginxSourceRangeV2, expectedKind string) bool {
	return source.Protocol == nginxSourceProtocolV2 && source.Kind == expectedKind && (source.Kind == "access" || source.Kind == "error") &&
		nginxSourceEpochV2Pattern.MatchString(source.SourceEpoch) && nginxSourceFileV2Pattern.MatchString(source.FileID) &&
		strings.HasPrefix(source.FileID, source.SourceEpoch+"-") && nginxSourceHashV2Pattern.MatchString(source.ContentSHA256) &&
		source.StartOffset >= 0 && source.EndOffset > source.StartOffset && source.EndOffset-source.StartOffset <= maxNginxSourceRangeBytesV2 && source.EndOffset <= 1<<50
}

func sourceFileIDV2(epoch string, generation uint64) string {
	return fmt.Sprintf("%s-%016x", epoch, generation)
}

func validateNginxSourceManifestV2(manifest nginxSourceManifestV2, expectedKind string) error {
	return validateNginxSourceManifestForRecoveryV2(manifest, expectedKind, false)
}

func validateNginxSourceManifestForRecoveryV2(manifest nginxSourceManifestV2, expectedKind string, recovery bool) error {
	if manifest.Protocol != nginxSourceProtocolV2 || manifest.Kind != expectedKind || (manifest.Kind != "access" && manifest.Kind != "error") ||
		!nginxSourceEpochV2Pattern.MatchString(manifest.SourceEpoch) || len(manifest.Files) == 0 || len(manifest.Files) > 256 || manifest.LegacyCursorOffset < 0 {
		return errors.New("invalid nginx v2 source manifest")
	}
	if manifest.CutoverFromV1 != (manifest.LegacyCursorInode != 0) || manifest.CutoverFromV1 != (manifest.LegacyCursorDevice != 0) || manifest.CutoverFromV1 != (manifest.LegacyAckedBatchID != "") ||
		manifest.LegacyCursorInode == 0 && manifest.LegacyCursorOffset != 0 || manifest.LegacyAckedBatchID != "" && !safeBatchID(manifest.LegacyAckedBatchID) {
		return errors.New("invalid nginx v1 cutover identity")
	}
	seenID, seenGeneration, seenPhysical, current := map[string]bool{}, map[uint64]bool{}, map[string]bool{}, 0
	legacyBoundaryMatches := 0
	lastGeneration := uint64(0)
	for _, file := range manifest.Files {
		physical := fmt.Sprintf("%d:%d", file.Device, file.Inode)
		if file.Generation == 0 || file.Generation <= lastGeneration || file.FileID != sourceFileIDV2(manifest.SourceEpoch, file.Generation) ||
			file.Device == 0 || file.Inode == 0 || file.BaseOffset < 0 || file.BaseOffset > 1<<50 || seenID[file.FileID] || seenGeneration[file.Generation] || seenPhysical[physical] {
			return errors.New("invalid nginx v2 source manifest file")
		}
		if file.Current {
			current++
		}
		if recovery {
			// A recovery base is an explicit, audited discontinuity. It may start
			// at the current durable size so old bytes are not counted twice.
		} else if manifest.CutoverFromV1 && file.Device == manifest.LegacyCursorDevice && file.Inode == manifest.LegacyCursorInode && file.BaseOffset == manifest.LegacyCursorOffset {
			legacyBoundaryMatches++
		} else if file.BaseOffset != 0 {
			return errors.New("nginx v2 source manifest has an unproven nonzero base")
		}
		seenID[file.FileID], seenGeneration[file.Generation], seenPhysical[physical] = true, true, true
		lastGeneration = file.Generation
	}
	if current != 1 || (manifest.CutoverFromV1 && legacyBoundaryMatches != 1) {
		return errors.New("nginx v2 source manifest must have exactly one current file")
	}
	return nil
}

func nginxSourceManifestHashV2(manifest nginxSourceManifestV2) (string, error) {
	copyManifest := manifest
	copyManifest.Files = append([]nginxSourceManifestFileV2(nil), manifest.Files...)
	sort.Slice(copyManifest.Files, func(i, j int) bool { return copyManifest.Files[i].Generation < copyManifest.Files[j].Generation })
	data, err := json.Marshal(copyManifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func nginxSourceFileRegistrationHashV2(registration nginxSourceFileRegistrationV2) (string, error) {
	data, err := json.Marshal(registration)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func legacyNginxSourceAllowed(tx *gorm.DB, node, kind string) (bool, error) {
	var state NginxCollectorProtocolState
	err := tx.First(&state, "node = ? AND kind = ?", node, kind).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return state.Protocol < nginxSourceProtocolV2, nil
}

func detectActiveNginxSourceV2Protocol(db *gorm.DB) (bool, error) {
	if db == nil {
		return false, nil
	}
	var count int64
	if err := db.Model(&NginxCollectorProtocolState{}).Where("protocol = ?", nginxSourceProtocolV2).Limit(1).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func nginxSourceV2LaneIsActive(db *gorm.DB, node, kind string) (bool, error) {
	if db == nil || !nginxNodeNamePattern.MatchString(node) || (kind != "access" && kind != "error") {
		return false, nil
	}
	var count int64
	if err := db.Model(&NginxCollectorProtocolState{}).
		Where("node = ? AND kind = ? AND protocol = ?", node, kind, nginxSourceProtocolV2).
		Limit(1).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func nginxSourceV2RuntimeConfigMatches(db *gorm.DB, cfg Settings) (bool, error) {
	if db == nil {
		return true, nil
	}
	var states []NginxCollectorProtocolState
	if err := db.Where("protocol = ?", nginxSourceProtocolV2).Find(&states).Error; err != nil {
		return false, err
	}
	if len(states) == 0 {
		return true, nil
	}
	if !cfg.NginxEnabled || !cfg.NginxSourceV2Enabled {
		return false, nil
	}
	nodes := make(map[string]struct{}, len(cfg.NginxSourceV2AllowedNodes))
	for _, node := range cfg.NginxSourceV2AllowedNodes {
		nodes[strings.TrimSpace(node)] = struct{}{}
	}
	lanes := make(map[string]struct{}, len(cfg.NginxSourceV2AllowedLanes))
	for _, lane := range cfg.NginxSourceV2AllowedLanes {
		lanes[strings.TrimSpace(lane)] = struct{}{}
	}
	for _, state := range states {
		if _, ok := nodes[state.Node]; !ok {
			return false, nil
		}
		if _, ok := lanes[state.Node+":"+state.Kind]; !ok {
			return false, nil
		}
	}
	return true, nil
}

type nginxLegacyLaneBoundaryV1 struct {
	Exists  bool
	BatchID string
	Device  uint64
	Inode   uint64
	Offset  int64
}

func legacyNginxLaneStateV2(tx *gorm.DB, node, kind string) (nginxLegacyLaneBoundaryV1, error) {
	latestBatchID := ""
	exists := false
	if kind == "access" {
		var state NginxSourceState
		err := tx.First(&state, "node = ?", node).Error
		if err == nil {
			exists, latestBatchID = true, state.LastBatchID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nginxLegacyLaneBoundaryV1{}, err
		}
	} else if kind == "error" {
		var state NginxErrorSourceState
		err := tx.First(&state, "node = ?", node).Error
		if err == nil {
			exists, latestBatchID = true, state.LastBatchID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nginxLegacyLaneBoundaryV1{}, err
		}
	} else {
		return nginxLegacyLaneBoundaryV1{}, errors.New("invalid nginx source kind")
	}
	if !exists {
		return nginxLegacyLaneBoundaryV1{}, nil
	}
	var boundary NginxSourceBoundaryStateV1
	err := tx.First(&boundary, "node = ? AND kind = ?", node, kind).Error
	if errors.Is(err, gorm.ErrRecordNotFound) || err == nil && boundary.LastBatchID != latestBatchID {
		// Existing v1 data without an exact boundary intentionally blocks cutover.
		return nginxLegacyLaneBoundaryV1{Exists: true}, nil
	}
	if err != nil {
		return nginxLegacyLaneBoundaryV1{}, err
	}
	return nginxLegacyLaneBoundaryV1{Exists: true, BatchID: boundary.LastBatchID, Device: boundary.Device, Inode: boundary.Inode, Offset: boundary.LastOffset}, nil
}

// registerNginxSourceManifestV2 is a zero-byte-capable cutover. It must commit
// before the collector sends any range. expectedLegacyLastBatchID protects
// against a stale collector racing the active v1 writer.
func registerNginxSourceManifestV2(tx *gorm.DB, node, expectedLegacyLastBatchID string, manifest nginxSourceManifestV2, now int64) (bool, string, error) {
	if !nginxNodeNamePattern.MatchString(node) || now <= 0 {
		return false, "", errors.New("invalid nginx v2 manifest envelope")
	}
	if err := validateNginxSourceManifestV2(manifest, manifest.Kind); err != nil {
		return false, "", err
	}
	hash, err := nginxSourceManifestHashV2(manifest)
	if err != nil {
		return false, "", err
	}
	var existing NginxCollectorProtocolState
	err = tx.First(&existing, "node = ? AND kind = ?", node, manifest.Kind).Error
	if err == nil {
		if existing.Protocol == nginxSourceProtocolV2 && existing.SourceEpoch == manifest.SourceEpoch && existing.ManifestSHA256 == hash {
			return true, hash, nil
		}
		return false, "", errNginxSourceEpoch
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, "", err
	}
	legacy, err := legacyNginxLaneStateV2(tx, node, manifest.Kind)
	if err != nil {
		return false, "", err
	}
	if legacy.Exists {
		if !manifest.CutoverFromV1 || expectedLegacyLastBatchID == "" || expectedLegacyLastBatchID != manifest.LegacyAckedBatchID ||
			manifest.LegacyAckedBatchID != legacy.BatchID || manifest.LegacyCursorDevice != legacy.Device || manifest.LegacyCursorInode != legacy.Inode || manifest.LegacyCursorOffset != legacy.Offset {
			return false, "", errors.New("legacy nginx lane requires an exact explicit v1 cutover")
		}
	} else if expectedLegacyLastBatchID != "" || manifest.CutoverFromV1 {
		return false, "", errors.New("unexpected legacy nginx cutover precondition")
	}
	protocol := NginxCollectorProtocolState{Node: node, Kind: manifest.Kind, Protocol: nginxSourceProtocolV2, SourceEpoch: manifest.SourceEpoch, ManifestSHA256: hash, ManifestRevision: 1, CutoverAt: now, UpdatedAt: now}
	if err := tx.Create(&protocol).Error; err != nil {
		return false, "", err
	}
	for _, file := range manifest.Files {
		row := NginxSourceFileWatermark{Node: node, Kind: manifest.Kind, SourceEpoch: manifest.SourceEpoch, FileID: file.FileID, Generation: file.Generation, Device: file.Device, Inode: file.Inode, BaseOffset: file.BaseOffset, NextOffset: file.BaseOffset, ConfirmedOffset: file.BaseOffset, RegisteredAt: now, UpdatedAt: now, State: "active", Current: file.Current}
		if err := tx.Create(&row).Error; err != nil {
			return false, "", err
		}
	}
	return false, hash, nil
}

// registerNginxSourceFileV2 extends an existing manifest one generation at a
// time. It is a hash-chained CAS and must complete before any bytes from the
// newly created current inode are sent.
func registerNginxSourceFileV2(tx *gorm.DB, node string, registration nginxSourceFileRegistrationV2, now int64) (bool, uint64, string, error) {
	if !nginxNodeNamePattern.MatchString(node) || now <= 0 || registration.Protocol != nginxSourceProtocolV2 || (registration.Kind != "access" && registration.Kind != "error") ||
		!nginxSourceEpochV2Pattern.MatchString(registration.SourceEpoch) || registration.ExpectedRevision == 0 || !nginxSourceHashV2Pattern.MatchString(registration.PreviousManifestHash) ||
		len(registration.Files) == 0 || len(registration.Files) > 256 {
		return false, 0, "", errors.New("invalid nginx v2 source file registration")
	}
	for i, file := range registration.Files {
		if file.FileID != sourceFileIDV2(registration.SourceEpoch, file.Generation) || file.Generation == 0 || file.Device == 0 || file.Inode == 0 || file.BaseOffset != 0 ||
			file.Current != (i == len(registration.Files)-1) || (i > 0 && file.Generation != registration.Files[i-1].Generation+1) {
			return false, 0, "", errors.New("invalid nginx v2 source file registration suffix")
		}
	}
	hash, err := nginxSourceFileRegistrationHashV2(registration)
	if err != nil {
		return false, 0, "", err
	}
	var protocol NginxCollectorProtocolState
	if err := tx.First(&protocol, "node = ? AND kind = ?", node, registration.Kind).Error; err != nil {
		return false, 0, "", err
	}
	if protocol.Protocol != nginxSourceProtocolV2 || protocol.SourceEpoch != registration.SourceEpoch || protocol.RecoveryRequired {
		return false, 0, "", errNginxSourceEpoch
	}
	var existingCount int64
	fileIDs := make([]string, 0, len(registration.Files))
	for _, file := range registration.Files {
		fileIDs = append(fileIDs, file.FileID)
	}
	err = tx.Model(&NginxSourceFileWatermark{}).Where("node = ? AND kind = ? AND source_epoch = ? AND file_id IN ?", node, registration.Kind, registration.SourceEpoch, fileIDs).Count(&existingCount).Error
	if err != nil {
		return false, 0, "", err
	}
	if existingCount > 0 {
		if existingCount == int64(len(registration.Files)) && protocol.ManifestRevision == registration.ExpectedRevision+1 && protocol.ManifestSHA256 == hash {
			var existingFiles []NginxSourceFileWatermark
			if err := tx.Where("node = ? AND kind = ? AND source_epoch = ? AND file_id IN ?", node, registration.Kind, registration.SourceEpoch, fileIDs).Order("generation").Find(&existingFiles).Error; err != nil {
				return false, 0, "", err
			}
			if len(existingFiles) == len(registration.Files) {
				exact := true
				for i, file := range registration.Files {
					stored := existingFiles[i]
					if stored.FileID != file.FileID || stored.Generation != file.Generation || stored.Device != file.Device || stored.Inode != file.Inode || stored.BaseOffset != file.BaseOffset || stored.Current != file.Current {
						exact = false
						break
					}
				}
				if exact {
					return true, protocol.ManifestRevision, hash, nil
				}
			}
		}
		return false, 0, "", errNginxSourceConflict
	}
	if protocol.ManifestRevision != registration.ExpectedRevision || protocol.ManifestSHA256 != registration.PreviousManifestHash {
		return false, 0, "", errNginxSourceConflict
	}
	var maxGeneration uint64
	if err := tx.Model(&NginxSourceFileWatermark{}).Where("node = ? AND kind = ? AND source_epoch = ?", node, registration.Kind, registration.SourceEpoch).Select("COALESCE(MAX(generation),0)").Scan(&maxGeneration).Error; err != nil {
		return false, 0, "", err
	}
	if registration.Files[0].Generation != maxGeneration+1 {
		return false, 0, "", errors.New("nginx source generation is not contiguous")
	}
	if err := tx.Model(&NginxSourceFileWatermark{}).Where("node = ? AND kind = ? AND source_epoch = ? AND current = ?", node, registration.Kind, registration.SourceEpoch, true).Update("current", false).Error; err != nil {
		return false, 0, "", err
	}
	for _, file := range registration.Files {
		row := NginxSourceFileWatermark{Node: node, Kind: registration.Kind, SourceEpoch: registration.SourceEpoch, FileID: file.FileID, Generation: file.Generation, Device: file.Device, Inode: file.Inode, RegisteredAt: now, UpdatedAt: now, State: "active", Current: file.Current}
		if err := tx.Create(&row).Error; err != nil {
			return false, 0, "", err
		}
	}
	nextRevision := registration.ExpectedRevision + 1
	updated := tx.Model(&NginxCollectorProtocolState{}).Where("node = ? AND kind = ? AND protocol = ? AND source_epoch = ? AND manifest_revision = ? AND manifest_sha256 = ? AND recovery_required = ?", node, registration.Kind, nginxSourceProtocolV2, registration.SourceEpoch, registration.ExpectedRevision, registration.PreviousManifestHash, false).
		Updates(map[string]any{"manifest_revision": nextRevision, "manifest_sha256": hash, "updated_at": now})
	if updated.Error != nil {
		return false, 0, "", updated.Error
	}
	if updated.RowsAffected != 1 {
		return false, 0, "", errNginxSourceConflict
	}
	return false, nextRevision, hash, nil
}

func markNginxSourceRecoveryRequiredV2(tx *gorm.DB, node, kind, expectedEpoch string, now int64) error {
	if !nginxNodeNamePattern.MatchString(node) || (kind != "access" && kind != "error") || !nginxSourceEpochV2Pattern.MatchString(expectedEpoch) || now <= 0 {
		return errors.New("invalid nginx source recovery marker")
	}
	updated := tx.Model(&NginxCollectorProtocolState{}).Where("node = ? AND kind = ? AND protocol = ? AND source_epoch = ? AND recovery_required = ?", node, kind, nginxSourceProtocolV2, expectedEpoch, false).
		Updates(map[string]any{"recovery_required": true, "updated_at": now})
	if updated.Error != nil {
		return updated.Error
	}
	if updated.RowsAffected != 1 {
		return errNginxSourceEpoch
	}
	return nil
}

// recoverNginxSourceEpochV2 is deliberately not exposed as a normal ingest
// route. A future root-only operator API must authenticate, audit and call it
// with an expected-old-epoch CAS. Recovery preserves old watermarks and marks
// continuity permanently broken for status/reporting.
func recoverNginxSourceEpochV2(tx *gorm.DB, node, expectedOldEpoch, actor, reason string, manifest nginxSourceManifestV2, now int64) (string, error) {
	actor, reason = strings.TrimSpace(actor), strings.TrimSpace(reason)
	if !nginxNodeNamePattern.MatchString(node) || !nginxSourceEpochV2Pattern.MatchString(expectedOldEpoch) || manifest.SourceEpoch == expectedOldEpoch ||
		len(actor) < 3 || len(actor) > 128 || len(reason) < 8 || len(reason) > 512 || now <= 0 || manifest.CutoverFromV1 {
		return "", errors.New("invalid nginx source epoch recovery")
	}
	if err := validateNginxSourceManifestForRecoveryV2(manifest, manifest.Kind, true); err != nil {
		return "", err
	}
	newHash, err := nginxSourceManifestHashV2(manifest)
	if err != nil {
		return "", err
	}
	var protocol NginxCollectorProtocolState
	if err := tx.First(&protocol, "node = ? AND kind = ?", node, manifest.Kind).Error; err != nil {
		return "", err
	}
	if protocol.Protocol != nginxSourceProtocolV2 || protocol.SourceEpoch != expectedOldEpoch || !protocol.RecoveryRequired {
		return "", errNginxSourceEpoch
	}
	updated := tx.Model(&NginxCollectorProtocolState{}).Where("node = ? AND kind = ? AND source_epoch = ? AND recovery_required = ?", node, manifest.Kind, expectedOldEpoch, true).
		Updates(map[string]any{"source_epoch": manifest.SourceEpoch, "manifest_sha256": newHash, "manifest_revision": 1, "cutover_at": now, "updated_at": now, "recovery_required": false, "continuity_broken": true, "last_recovery_at": now})
	if updated.Error != nil {
		return "", updated.Error
	}
	if updated.RowsAffected != 1 {
		return "", errNginxSourceEpoch
	}
	for _, file := range manifest.Files {
		row := NginxSourceFileWatermark{Node: node, Kind: manifest.Kind, SourceEpoch: manifest.SourceEpoch, FileID: file.FileID, Generation: file.Generation, Device: file.Device, Inode: file.Inode, BaseOffset: file.BaseOffset, NextOffset: file.BaseOffset, ConfirmedOffset: file.BaseOffset, RegisteredAt: now, UpdatedAt: now, State: "active", Current: file.Current}
		if err := tx.Create(&row).Error; err != nil {
			return "", err
		}
	}
	audit := NginxCollectorEpochRecovery{Node: node, Kind: manifest.Kind, OldSourceEpoch: expectedOldEpoch, NewSourceEpoch: manifest.SourceEpoch, OldManifestHash: protocol.ManifestSHA256, NewManifestHash: newHash, Actor: actor, Reason: reason, RecoveredAt: now}
	if err := tx.Create(&audit).Error; err != nil {
		return "", err
	}
	return newHash, nil
}

// applyNginxSourceRangeV2 only advances a file registered by the manifest.
// The caller must use the same transaction for batch, aggregate and state.
func applyNginxSourceRangeV2(tx *gorm.DB, node, kind, batchID, payloadHash string, source nginxSourceRangeV2, now int64) (bool, error) {
	if !nginxNodeNamePattern.MatchString(node) || !validIngestBatchID(batchID) || !nginxSourceHashV2Pattern.MatchString(payloadHash) || !validNginxSourceRangeV2(source, kind) || now <= 0 {
		return false, errors.New("invalid nginx v2 source envelope")
	}
	var protocol NginxCollectorProtocolState
	if err := tx.First(&protocol, "node = ? AND kind = ?", node, kind).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, errNginxSourceUnregistered
		}
		return false, err
	}
	if protocol.Protocol != nginxSourceProtocolV2 || protocol.SourceEpoch != source.SourceEpoch || protocol.RecoveryRequired {
		return false, errNginxSourceEpoch
	}
	var watermark NginxSourceFileWatermark
	if err := tx.First(&watermark, "node = ? AND kind = ? AND source_epoch = ? AND file_id = ?", node, kind, source.SourceEpoch, source.FileID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, errNginxSourceUnregistered
		}
		return false, err
	}
	var exact NginxSourceRangeBatch
	err := tx.First(&exact, "node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND start_offset = ? AND end_offset = ?", node, kind, source.SourceEpoch, source.FileID, source.StartOffset, source.EndOffset).Error
	if err == nil {
		if exact.BatchID != batchID || exact.PayloadHash != payloadHash || exact.ContentHash != source.ContentSHA256 {
			return false, errNginxSourceConflict
		}
		if watermark.NextOffset < exact.EndOffset || watermark.BaseOffset > exact.StartOffset {
			return false, fmt.Errorf("%w: exact range has inconsistent permanent watermark", errNginxSourceConflict)
		}
		if err := ensureNginxSourceCommitV2(tx, node, kind, batchID, payloadHash, source, now); err != nil {
			return false, err
		}
		return true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	if source.StartOffset < watermark.NextOffset {
		return false, errNginxSourceOverlap
	}
	if source.StartOffset > watermark.NextOffset {
		return false, errNginxSourceGap
	}
	if source.EndOffset-watermark.ConfirmedOffset > maxNginxSourceUnconfirmedBytesV2 {
		return false, errNginxSourceBackpressure
	}
	var unconfirmedRanges int64
	if err := tx.Model(&NginxSourceRangeBatch{}).
		Where("node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND end_offset > ?", node, kind, source.SourceEpoch, source.FileID, watermark.ConfirmedOffset).
		Count(&unconfirmedRanges).Error; err != nil {
		return false, err
	}
	if unconfirmedRanges >= maxNginxSourceUnconfirmedRangesV2 {
		return false, errNginxSourceBackpressure
	}
	rangeRow := NginxSourceRangeBatch{Node: node, Kind: kind, SourceEpoch: source.SourceEpoch, FileID: source.FileID, StartOffset: source.StartOffset, EndOffset: source.EndOffset, BatchID: batchID, PayloadHash: payloadHash, ContentHash: source.ContentSHA256, ReceivedAt: now}
	if err := tx.Create(&rangeRow).Error; err != nil {
		return false, err
	}
	updated := tx.Model(&NginxSourceFileWatermark{}).Where("node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND next_offset = ?", node, kind, source.SourceEpoch, source.FileID, source.StartOffset).Updates(map[string]any{"next_offset": source.EndOffset, "batches": gorm.Expr("batches + 1"), "last_content_hash": source.ContentSHA256, "updated_at": now})
	if updated.Error != nil {
		return false, updated.Error
	}
	if updated.RowsAffected != 1 {
		return false, fmt.Errorf("%w: source watermark CAS failed", errNginxSourceConflict)
	}
	if err := ensureNginxSourceCommitV2(tx, node, kind, batchID, payloadHash, source, now); err != nil {
		return false, err
	}
	return false, nil
}

func ensureNginxSourceCommitV2(tx *gorm.DB, node, kind, batchID, payloadHash string, source nginxSourceRangeV2, now int64) error {
	row := NginxSourceCommitV2{Node: node, Kind: kind, BatchID: batchID, SourceEpoch: source.SourceEpoch, FileID: source.FileID,
		StartOffset: source.StartOffset, EndOffset: source.EndOffset, ContentHash: source.ContentSHA256, PayloadHash: payloadHash, ReceivedAt: now}
	created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if created.Error != nil {
		return created.Error
	}
	if created.RowsAffected == 1 {
		return nil
	}
	var existing NginxSourceCommitV2
	if err := tx.First(&existing, "node = ? AND kind = ? AND batch_id = ?", node, kind, batchID).Error; err != nil {
		return err
	}
	if existing.SourceEpoch != source.SourceEpoch || existing.FileID != source.FileID || existing.StartOffset != source.StartOffset || existing.EndOffset != source.EndOffset || existing.ContentHash != source.ContentSHA256 || existing.PayloadHash != payloadHash {
		return errNginxSourceConflict
	}
	return nil
}

// confirmNginxSourceAckV2 is sent only after the collector fsyncs its local
// cursor. Ranges at or below that durable offset are no longer needed for ACK
// recovery and can be pruned without allowing a future overlap to reapply.
func confirmNginxSourceAckV2(tx *gorm.DB, in nginxSourceAckConfirmationRequestV2, now int64) (bool, int64, error) {
	if !nginxNodeNamePattern.MatchString(in.Node) || (in.Kind != "access" && in.Kind != "error") ||
		!nginxSourceEpochV2Pattern.MatchString(in.SourceEpoch) || !nginxSourceFileV2Pattern.MatchString(in.FileID) ||
		!strings.HasPrefix(in.FileID, in.SourceEpoch+"-") || in.ConfirmedOffset < 0 || in.ConfirmedOffset > 1<<50 || now <= 0 {
		return false, 0, errors.New("invalid nginx source acknowledgement confirmation")
	}
	var protocol NginxCollectorProtocolState
	if err := tx.First(&protocol, "node = ? AND kind = ?", in.Node, in.Kind).Error; err != nil {
		return false, 0, err
	}
	if protocol.Protocol != nginxSourceProtocolV2 || protocol.SourceEpoch != in.SourceEpoch || protocol.RecoveryRequired {
		return false, 0, errNginxSourceEpoch
	}
	var watermark NginxSourceFileWatermark
	if err := tx.First(&watermark, "node = ? AND kind = ? AND source_epoch = ? AND file_id = ?", in.Node, in.Kind, in.SourceEpoch, in.FileID).Error; err != nil {
		return false, 0, errNginxSourceUnregistered
	}
	if in.ConfirmedOffset == watermark.ConfirmedOffset {
		return true, watermark.ConfirmedOffset, nil
	}
	if in.ConfirmedOffset < watermark.ConfirmedOffset || in.ConfirmedOffset > watermark.NextOffset || in.ConfirmedOffset < watermark.BaseOffset {
		return false, watermark.ConfirmedOffset, errNginxSourceConflict
	}
	if in.ConfirmedOffset != watermark.BaseOffset {
		var count int64
		if err := tx.Model(&NginxSourceRangeBatch{}).Where("node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND end_offset = ?", in.Node, in.Kind, in.SourceEpoch, in.FileID, in.ConfirmedOffset).Count(&count).Error; err != nil {
			return false, watermark.ConfirmedOffset, err
		}
		if count != 1 {
			return false, watermark.ConfirmedOffset, errNginxSourceConflict
		}
	}
	updated := tx.Model(&NginxSourceFileWatermark{}).
		Where("node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND confirmed_offset = ?", in.Node, in.Kind, in.SourceEpoch, in.FileID, watermark.ConfirmedOffset).
		Updates(map[string]any{"confirmed_offset": in.ConfirmedOffset, "updated_at": now})
	if updated.Error != nil {
		return false, watermark.ConfirmedOffset, updated.Error
	}
	if updated.RowsAffected != 1 {
		return false, watermark.ConfirmedOffset, errNginxSourceConflict
	}
	if err := tx.Where("node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND end_offset <= ?", in.Node, in.Kind, in.SourceEpoch, in.FileID, in.ConfirmedOffset).Delete(&NginxSourceRangeBatch{}).Error; err != nil {
		return false, watermark.ConfirmedOffset, err
	}
	return false, in.ConfirmedOffset, nil
}

// nginxSourceAckRecoveryProofV2 returns the complete accepted byte chain from
// clientOffset. A missing/pruned range fails closed instead of asking the
// collector to skip bytes it cannot verify locally.
func nginxSourceAckRecoveryProofV2(tx *gorm.DB, node, kind, epoch, fileID string, clientOffset int64) (nginxSourceAckRecoveryV2, error) {
	if !nginxNodeNamePattern.MatchString(node) || (kind != "access" && kind != "error") || !nginxSourceEpochV2Pattern.MatchString(epoch) ||
		fileID == "" || !strings.HasPrefix(fileID, epoch+"-") || clientOffset < 0 {
		return nginxSourceAckRecoveryV2{}, errors.New("invalid nginx v2 acknowledgement recovery query")
	}
	var watermark NginxSourceFileWatermark
	if err := tx.First(&watermark, "node = ? AND kind = ? AND source_epoch = ? AND file_id = ?", node, kind, epoch, fileID).Error; err != nil {
		return nginxSourceAckRecoveryV2{}, err
	}
	if clientOffset < watermark.BaseOffset || clientOffset > watermark.NextOffset {
		return nginxSourceAckRecoveryV2{}, errors.New("client offset is outside the registered source range")
	}
	if clientOffset < watermark.ConfirmedOffset {
		return nginxSourceAckRecoveryV2{}, errors.New("requested nginx acknowledgement proof was already durably confirmed and pruned")
	}
	result := nginxSourceAckRecoveryV2{SourceEpoch: epoch, FileID: fileID, NextOffset: watermark.NextOffset}
	if clientOffset == watermark.NextOffset {
		return result, nil
	}
	var rows []NginxSourceRangeBatch
	if err := tx.Where("node = ? AND kind = ? AND source_epoch = ? AND file_id = ? AND end_offset > ?", node, kind, epoch, fileID, clientOffset).
		Order("start_offset ASC").Limit(4096).Find(&rows).Error; err != nil {
		return nginxSourceAckRecoveryV2{}, err
	}
	next := clientOffset
	for _, row := range rows {
		if row.StartOffset != next || row.EndOffset <= row.StartOffset || !nginxSourceHashV2Pattern.MatchString(row.ContentHash) {
			return nginxSourceAckRecoveryV2{}, errors.New("accepted nginx source proof chain is incomplete")
		}
		result.Proofs = append(result.Proofs, nginxSourceAckProofV2{StartOffset: row.StartOffset, EndOffset: row.EndOffset, ContentSHA256: row.ContentHash})
		next = row.EndOffset
		if next == watermark.NextOffset {
			break
		}
	}
	if next != watermark.NextOffset {
		return nginxSourceAckRecoveryV2{}, errors.New("accepted nginx source proof chain exceeds recovery limit or was pruned")
	}
	return result, nil
}

func nginxSourceV2Capabilities() map[string]any {
	return map[string]any{"protocol": nginxSourceProtocolV2, "kinds": []string{"access", "error"}, "manifest": true, "range_cas": true, "source_epoch": true, "ack_echo": true, "ack_confirm": true, "heartbeat": true, "max_range_bytes": maxNginxSourceRangeBytesV2, "max_unconfirmed_bytes": maxNginxSourceUnconfirmedBytesV2, "max_unconfirmed_ranges": maxNginxSourceUnconfirmedRangesV2, "server_time": time.Now().Unix()}
}

func validateNginxSourceHeartbeatV2(in nginxSourceHeartbeatRequestV2, now int64) error {
	if (in.Kind != "access" && in.Kind != "error") || !nginxSourceEpochV2Pattern.MatchString(in.SourceEpoch) || !nginxSourceFileV2Pattern.MatchString(in.FileID) || in.ConfirmedOffset < 0 || in.ConfirmedOffset > 1<<50 ||
		in.BacklogBytes < 0 || in.BacklogBytes > 1<<50 || !in.BacklogKnown && in.BacklogBytes != 0 ||
		in.CursorDiscontinuities < 0 || in.CursorDiscontinuities > 1_000_000_000 || in.LastCursorDiscontinuityAt < 0 || in.LastCursorDiscontinuityAt > now+300 || (in.CursorDiscontinuities == 0) != (in.LastCursorDiscontinuityAt == 0) ||
		in.DiscardedLines < 0 || in.DiscardedLines > 1_000_000_000_000 || in.LastDiscardedAt < 0 || in.LastDiscardedAt > now+300 || (in.DiscardedLines == 0) != (in.LastDiscardedAt == 0) ||
		in.EvidencePersistFailures < 0 || in.EvidenceDroppedEvents < 0 || in.LastEvidencePersistFailureAt < 0 || in.LastEvidencePersistFailureAt > now+300 ||
		(in.EvidencePersistFailures == 0) != (in.LastEvidencePersistFailureAt == 0) || in.EvidencePersistFailures == 0 && in.EvidenceDroppedEvents != 0 {
		return errors.New("invalid nginx source heartbeat")
	}
	return nil
}

func applyNginxSourceHeartbeatV2(tx *gorm.DB, in nginxSourceHeartbeatRequestV2, now int64) error {
	if err := validateNginxSourceHeartbeatV2(in, now); err != nil {
		return err
	}
	var protocol NginxCollectorProtocolState
	if err := tx.First(&protocol, "node = ? AND kind = ?", in.Node, in.Kind).Error; err != nil {
		return errNginxSourceEpoch
	}
	if protocol.Protocol != nginxSourceProtocolV2 || protocol.SourceEpoch != in.SourceEpoch || protocol.RecoveryRequired || protocol.ContinuityBroken {
		return errNginxSourceEpoch
	}
	var watermark NginxSourceFileWatermark
	if err := tx.First(&watermark, "node = ? AND kind = ? AND source_epoch = ? AND file_id = ?", in.Node, in.Kind, in.SourceEpoch, in.FileID).Error; err != nil {
		return errNginxSourceUnregistered
	}
	if !watermark.Current || watermark.ConfirmedOffset != in.ConfirmedOffset {
		return errNginxSourceConflict
	}
	if in.Kind == "access" {
		state := NginxSourceState{Node: in.Node, LastIngestTs: now, BacklogBytes: in.BacklogBytes, BacklogKnown: in.BacklogKnown,
			CursorDiscontinuities: in.CursorDiscontinuities, LastCursorDiscontinuityAt: in.LastCursorDiscontinuityAt,
			DiscardedLines: in.DiscardedLines, LastDiscardedAt: in.LastDiscardedAt,
			EvidencePersistFailures: in.EvidencePersistFailures, EvidenceDroppedEvents: in.EvidenceDroppedEvents, LastEvidencePersistFailureAt: in.LastEvidencePersistFailureAt}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}}, DoUpdates: clause.Assignments(map[string]any{
			"last_ingest_ts": now, "backlog_bytes": in.BacklogBytes, "backlog_known": in.BacklogKnown,
			"cursor_discontinuities":       gorm.Expr("MAX(COALESCE(cursor_discontinuities, 0), excluded.cursor_discontinuities)"),
			"last_cursor_discontinuity_at": gorm.Expr("MAX(COALESCE(last_cursor_discontinuity_at, 0), excluded.last_cursor_discontinuity_at)"),
			"discarded_lines":              gorm.Expr("MAX(COALESCE(discarded_lines, 0), excluded.discarded_lines)"), "last_discarded_at": gorm.Expr("MAX(COALESCE(last_discarded_at, 0), excluded.last_discarded_at)"),
			"evidence_persist_failures":        gorm.Expr("MAX(COALESCE(evidence_persist_failures, 0), excluded.evidence_persist_failures)"),
			"evidence_dropped_events":          gorm.Expr("MAX(COALESCE(evidence_dropped_events, 0), excluded.evidence_dropped_events)"),
			"last_evidence_persist_failure_at": gorm.Expr("MAX(COALESCE(last_evidence_persist_failure_at, 0), excluded.last_evidence_persist_failure_at)"),
		})}).Create(&state).Error
	}
	state := NginxErrorSourceState{Node: in.Node, LastIngestTs: now, BacklogBytes: in.BacklogBytes, BacklogKnown: in.BacklogKnown,
		CursorDiscontinuities: in.CursorDiscontinuities, LastCursorDiscontinuityAt: in.LastCursorDiscontinuityAt,
		DiscardedLines: in.DiscardedLines, LastDiscardedAt: in.LastDiscardedAt}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}}, DoUpdates: clause.Assignments(map[string]any{
		"last_ingest_ts": now, "backlog_bytes": in.BacklogBytes, "backlog_known": in.BacklogKnown,
		"cursor_discontinuities": gorm.Expr("MAX(COALESCE(cursor_discontinuities, 0), excluded.cursor_discontinuities)"), "last_cursor_discontinuity_at": gorm.Expr("MAX(COALESCE(last_cursor_discontinuity_at, 0), excluded.last_cursor_discontinuity_at)"),
		"discarded_lines": gorm.Expr("MAX(COALESCE(discarded_lines, 0), excluded.discarded_lines)"), "last_discarded_at": gorm.Expr("MAX(COALESCE(last_discarded_at, 0), excluded.last_discarded_at)"),
	})}).Create(&state).Error
}

func (m *Monitor) beginNginxSourceV2Request(c *gin.Context) bool {
	if !m.cfg.NginxEnabled || !m.cfg.NginxSourceV2Enabled || (!m.cfg.NginxSourceV2CutoverEnabled && !m.nginxSourceV2Active.Load()) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx ingest disabled"})
		return false
	}
	if !m.checkIngest(c) {
		return false
	}
	return true
}

func (m *Monitor) validateNginxSourceV2Lane(c *gin.Context, node, kind string) bool {
	if !nginxNodeNamePattern.MatchString(node) || !m.nginxNodeAllowed(node) || !m.nginxSourceV2LaneAllowed(node, kind) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source lane not allowed"})
		return false
	}
	return true
}

func writeNginxSourceV2Error(c *gin.Context, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, errNginxSourceBackpressure) {
		status = http.StatusTooManyRequests
	} else if errors.Is(err, errNginxSourceConflict) || errors.Is(err, errNginxSourceGap) || errors.Is(err, errNginxSourceOverlap) || errors.Is(err, errNginxSourceEpoch) || errors.Is(err, errNginxSourceUnregistered) {
		status = http.StatusConflict
	} else if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "requires an exact") || strings.Contains(err.Error(), "unexpected legacy") || strings.Contains(err.Error(), "not contiguous") {
		status = http.StatusBadRequest
	}
	c.JSON(status, gin.H{"error": err.Error()})
}

func (m *Monitor) nginxSourceCapabilitiesV2(c *gin.Context) {
	if !m.beginNginxSourceV2Request(c) {
		return
	}
	c.JSON(http.StatusOK, nginxSourceV2Capabilities())
}

func (m *Monitor) registerNginxSourceManifestHTTPV2(c *gin.Context) {
	if !m.beginNginxSourceV2Request(c) {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	var in nginxSourceManifestRequestV2
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	in.Node = strings.TrimSpace(in.Node)
	if !m.validateNginxSourceV2Lane(c, in.Node, in.Manifest.Kind) {
		return
	}
	active, err := nginxSourceV2LaneIsActive(m.storeDB, in.Node, in.Manifest.Kind)
	if err != nil {
		writeNginxSourceV2Error(c, err)
		return
	}
	if !active && !m.cfg.NginxSourceV2CutoverEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "new nginx source v2 cutover is disabled"})
		return
	}
	var duplicate bool
	var hash string
	err = m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		duplicate, hash, err = registerNginxSourceManifestV2(tx, in.Node, in.Manifest.LegacyAckedBatchID, in.Manifest, time.Now().Unix())
		return err
	})
	if err != nil {
		writeNginxSourceV2Error(c, err)
		return
	}
	m.nginxSourceV2Active.Store(true)
	c.JSON(http.StatusOK, gin.H{"ok": true, "duplicate": duplicate, "manifest_sha256": hash, "manifest_revision": 1})
}

func (m *Monitor) registerNginxSourceFileHTTPV2(c *gin.Context) {
	if !m.beginNginxSourceV2Request(c) {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	var in nginxSourceFileRegistrationRequestV2
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	in.Node = strings.TrimSpace(in.Node)
	if !m.validateNginxSourceV2Lane(c, in.Node, in.Registration.Kind) {
		return
	}
	var duplicate bool
	var revision uint64
	var hash string
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		duplicate, revision, hash, err = registerNginxSourceFileV2(tx, in.Node, in.Registration, time.Now().Unix())
		return err
	})
	if err != nil {
		writeNginxSourceV2Error(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "duplicate": duplicate, "manifest_sha256": hash, "manifest_revision": revision})
}

func (m *Monitor) recoverNginxSourceAckHTTPV2(c *gin.Context) {
	if !m.beginNginxSourceV2Request(c) {
		return
	}
	node, kind := strings.TrimSpace(c.Query("node")), strings.TrimSpace(c.Query("kind"))
	if !m.validateNginxSourceV2Lane(c, node, kind) {
		return
	}
	offset, err := strconv.ParseInt(c.Query("client_offset"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid client_offset"})
		return
	}
	proof, err := nginxSourceAckRecoveryProofV2(m.storeDB, node, kind, strings.TrimSpace(c.Query("source_epoch")), strings.TrimSpace(c.Query("file_id")), offset)
	if err != nil {
		writeNginxSourceV2Error(c, err)
		return
	}
	c.JSON(http.StatusOK, proof)
}

func (m *Monitor) confirmNginxSourceAckHTTPV2(c *gin.Context) {
	if !m.beginNginxSourceV2Request(c) {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	var in nginxSourceAckConfirmationRequestV2
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	in.Node, in.Kind = strings.TrimSpace(in.Node), strings.TrimSpace(in.Kind)
	if !m.validateNginxSourceV2Lane(c, in.Node, in.Kind) {
		return
	}
	var duplicate bool
	var confirmed int64
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		duplicate, confirmed, err = confirmNginxSourceAckV2(tx, in, time.Now().Unix())
		return err
	})
	if err != nil {
		writeNginxSourceV2Error(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "duplicate": duplicate, "confirmed_offset": confirmed})
}

func (m *Monitor) nginxSourceHeartbeatHTTPV2(c *gin.Context) {
	if !m.beginNginxSourceV2Request(c) {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	var in nginxSourceHeartbeatRequestV2
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	in.Node, in.Kind, in.SourceEpoch, in.FileID = strings.TrimSpace(in.Node), strings.TrimSpace(in.Kind), strings.TrimSpace(in.SourceEpoch), strings.TrimSpace(in.FileID)
	if !m.validateNginxSourceV2Lane(c, in.Node, in.Kind) {
		return
	}
	now := time.Now().Unix()
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error { return applyNginxSourceHeartbeatV2(tx, in, now) }); err != nil {
		writeNginxSourceV2Error(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "source_epoch": in.SourceEpoch, "file_id": in.FileID, "confirmed_offset": in.ConfirmedOffset})
}
