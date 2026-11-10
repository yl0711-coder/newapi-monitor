package monitor

// TokenMeteringFact is one consumption identity, including normal requests.
// SourceFacts remains a bounded whitelist of usage operands.  Request and
// provenance identifiers live in their own bounded columns so the page can
// show the exact source identity without retaining the unbounded source
// payload.  Historical rows that predate these columns remain explicitly
// missing; they are never filled from the current channel configuration.
type TokenMeteringFact struct {
	EvidenceSnapshotAt      int64  `json:"evidence_snapshot_at"`
	SourceEpoch             string `gorm:"primaryKey;size:64;index:idx_token_reference_input,priority:1;index:idx_token_detection_work_fact,priority:1;index:idx_token_detection_fact_identity,priority:1" json:"source_epoch"`
	LogID                   int64  `gorm:"primaryKey;autoIncrement:false;index:idx_token_detection_work_fact,priority:3;index:idx_token_detection_fact_identity,priority:2" json:"log_id"`
	CreatedAt               int64  `gorm:"index:idx_token_fact_time;index:idx_token_reference_input,priority:4;index:idx_token_detection_work_fact,priority:2;index:idx_token_detection_fact_identity,priority:3" json:"created_at"`
	RequestID               string `gorm:"size:256;index:idx_token_fact_request_id" json:"request_id"`
	UpstreamRequestID       string `gorm:"size:256" json:"upstream_request_id"`
	UserID                  int64  `gorm:"index:idx_token_reference_input,priority:2" json:"user_id"`
	ChannelID               int64  `json:"channel_id"`
	Model                   string `gorm:"size:256;index:idx_token_reference_input,priority:3" json:"model"`
	UpstreamModelName       string `gorm:"size:256" json:"upstream_model_name"`
	UserGroup               string `gorm:"size:128" json:"user_group"`
	RequestMethod           string `gorm:"size:16" json:"request_method"`
	RequestPath             string `gorm:"size:256" json:"request_path"`
	SourceEventID           string `gorm:"size:256" json:"source_event_id"`
	EntryEventID            string `gorm:"size:64;index:idx_token_fact_entry_event" json:"entry_event_id"`
	EntryNode               string `gorm:"size:64" json:"entry_node"`
	EntryRoute              string `gorm:"size:256" json:"entry_route"`
	EntryMethod             string `gorm:"size:16" json:"entry_method"`
	EntryStatus             int    `json:"entry_status"`
	EntryObservedAt         int64  `json:"entry_observed_at"`
	SourceFacts             string `json:"-"`
	MissingFields           string `gorm:"type:text" json:"missing_fields"`
	FactHash                string `gorm:"size:64" json:"-"`
	Revision                int64  `gorm:"index:idx_token_detection_work_fact,priority:4;index:idx_token_detection_fact_identity,priority:4" json:"revision"`
	Semantic                string `gorm:"size:32;index" json:"semantic"`
	SemanticReason          string `gorm:"size:64" json:"semantic_reason"`
	ParserVersion           string `gorm:"size:32" json:"parser_version"`
	StoredBytes             int64  `json:"-"`
	FirstSeenAt             int64  `json:"first_seen_at"`
	LastSeenAt              int64  `json:"last_seen_at"`
	LinkState               string `gorm:"size:24;index" json:"link_state"`
	LinkReason              string `gorm:"size:64" json:"link_reason"`
	LinkAttempts            int    `json:"link_attempts"`
	LinkNextAt              int64  `gorm:"index:idx_token_link_due" json:"link_next_at"`
	LinkDeadline            int64  `json:"link_deadline"`
	LinkCheckedAt           int64  `json:"link_checked_at"`
	LinkRevision            int64  `gorm:"index:idx_token_detection_work_fact,priority:5;index:idx_token_detection_fact_identity,priority:5" json:"link_revision"`
	EvidenceJSON            string `json:"-"`
	EvidenceLevel           string `gorm:"size:16" json:"evidence_level"`
	RequestLength           *int64 `json:"request_length"`
	LengthState             string `gorm:"size:24;index" json:"length_state"`
	RequestLengthVersion    string `gorm:"size:32" json:"request_length_version"`
	RequestLengthMeaning    string `gorm:"size:64" json:"request_length_meaning"`
	RequestLengthObservedAt int64  `json:"request_length_observed_at"`
	PricingSnapshot         string `gorm:"type:text" json:"pricing_snapshot"`
	PricingEffectiveAt      int64  `json:"pricing_effective_at"`
	PricingAdapter          string `gorm:"size:64" json:"pricing_adapter"`
	RouteSnapshot           string `gorm:"size:256" json:"route_snapshot"`
	BasePathSnapshot        string `gorm:"size:256" json:"base_path_snapshot"`
	ModelMappingSnapshot    string `gorm:"type:text" json:"model_mapping_snapshot"`
	CustomerNameSnapshot    string `gorm:"size:256" json:"customer_name_snapshot"`
	CustomerNameSource      string `gorm:"size:64" json:"customer_name_source"`
	CustomerNameObservedAt  int64  `json:"customer_name_observed_at"`
}

// TokenMeteringRef stores only domain/versioned HMAC identifiers.
type TokenMeteringRef struct {
	SourceEpoch string `gorm:"primaryKey;size:64"`
	LogID       int64  `gorm:"primaryKey;autoIncrement:false"`
	KeyID       string `gorm:"primaryKey;size:32;index:idx_token_ref_hash,priority:1"`
	Hash        string `gorm:"size:64;index:idx_token_ref_hash,priority:2"`
	Version     string `gorm:"size:32"`
}

// TokenMeteringState owns an independent checkpoint and fenced worker lease.
type TokenMeteringState struct {
	HMACProfiles         string `json:"-"`
	SourceEpoch          string `gorm:"primaryKey;size:64" json:"source_epoch"`
	StartAt              int64  `json:"start_at"`
	ScanThrough          int64  `json:"scan_through"`
	CompleteThrough      int64  `json:"complete_through"`
	SourceMode           string `gorm:"size:16" json:"source_mode"`
	LeaseToken           string `gorm:"size:64" json:"-"`
	LeaseUntil           int64  `json:"-"`
	LastSuccessAt        int64  `json:"last_success_at"`
	LastFailureAt        int64  `json:"last_failure_at"`
	LastReason           string `gorm:"size:64" json:"last_reason"`
	LastFailureStage     string `gorm:"size:32" json:"last_failure_stage"`
	LastReadWaitMS       int64  `json:"last_read_wait_ms"`
	LastReadQueryMS      int64  `json:"last_read_query_ms"`
	LastReadConnectionMS int64  `json:"last_read_connection_ms"`
	LastReadExecuteMS    int64  `json:"last_read_execute_ms"`
	LastReadReceiveMS    int64  `json:"last_read_receive_ms"`
	LastReadDecodeMS     int64  `json:"last_read_decode_ms"`
	LastWriterWaitMS     int64  `json:"last_writer_wait_ms"`
	LastCommitMS         int64  `json:"last_commit_ms"`
	LastReadAt           int64  `json:"last_read_at"`
	FactCount            int64  `json:"fact_count"`
	FactBytes            int64  `json:"fact_bytes"`
	ExpiredGapWindows    int64  `json:"expired_gap_windows"`
	ExpiredBefore        int64  `json:"expired_before"`
	LastCleanupAt        int64  `json:"last_cleanup_at"`
	// CleanupCursor* is a durable keyset position for the retention lane. It
	// advances only with a committed delete prefix and is reset after a pass
	// reaches the end, so protected rows can become eligible on a later pass.
	CleanupCursorAt int64 `json:"-"`
	CleanupCursorID int64 `json:"-"`
	// A maintenance reconciliation records the exact backed-up deletion
	// boundary. Startup and ordinary retention cannot authorize their own
	// destructive pass. New/changed facts after the backup stay protected.
	CleanupBackupID     string `gorm:"size:256" json:"cleanup_backup_id"`
	CleanupVerifiedAt   int64  `json:"cleanup_verified_at"`
	CleanupSafeBefore   int64  `json:"cleanup_safe_before"`
	CleanupStableSince  int64  `json:"cleanup_stable_since"`
	CleanupReason       string `gorm:"size:64" json:"cleanup_reason"`
	CleanupNextAt       int64  `json:"cleanup_next_at"`
	CleanupDeletedRows  int64  `json:"cleanup_deleted_rows"`
	CleanupDeletedBytes int64  `json:"cleanup_deleted_bytes"`
	ReadPageRows        int    `json:"read_page_rows"`
	MigrationVersion    string `gorm:"size:64" json:"migration_version"`
	MigrationCursor     int64  `json:"migration_cursor"`
}

// TokenMeteringWindow keeps both forward and replay pagination crash safe.
type TokenMeteringWindow struct {
	SourceEpoch string `gorm:"primaryKey;size:64" json:"source_epoch"`
	StartAt     int64  `gorm:"primaryKey;autoIncrement:false" json:"start_at"`
	EndAt       int64  `json:"end_at"`
	// Lane is durable scheduling provenance.  Replay is kept for backwards
	// compatibility with the first cursor schema; new rows use one of the
	// explicit lane names below and are never inferred from a current config.
	Lane           string `gorm:"size:24;index:idx_token_metering_window_lane" json:"lane"`
	CursorAt       int64  `json:"cursor_at"`
	CursorID       int64  `json:"cursor_id"`
	Done           bool   `gorm:"index" json:"done"`
	SourceComplete bool   `json:"source_complete"`
	Replay         bool   `json:"replay"`
	Attempts       int    `json:"attempts"`
	NextAt         int64  `gorm:"index" json:"next_at"`
	LastScannedAt  int64  `gorm:"index" json:"last_scanned_at"`
	// Open EOF is an observation, not proof that this event-time window is complete.
	OpenObservedAt   int64  `gorm:"not null;default:0" json:"open_observed_at"`
	SealStarted      bool   `gorm:"not null;default:0" json:"seal_started"`
	LastReason       string `gorm:"size:64" json:"last_reason"`
	ReadPageRows     int    `json:"read_page_rows"`
	ReadSuccessPages int    `gorm:"not null;default:0" json:"read_success_pages"`
	ReadWorstMS      int64  `gorm:"not null;default:0" json:"read_worst_ms"`
	RowsRead         int64  `json:"rows_read"`
	RowsCommitted    int64  `json:"rows_committed"`
	UpdatedAt        int64  `gorm:"index;autoUpdateTime:false" json:"updated_at"`
	LastFailureStage string `gorm:"size:32" json:"last_failure_stage"`
	ReadWaitMS       int64  `json:"read_wait_ms"`
	ReadQueryMS      int64  `json:"read_query_ms"`
	ReadConnectionMS int64  `json:"read_connection_ms"`
	ReadExecuteMS    int64  `json:"read_execute_ms"`
	ReadReceiveMS    int64  `json:"read_receive_ms"`
	ReadDecodeMS     int64  `json:"read_decode_ms"`
	WriterWaitMS     int64  `json:"writer_wait_ms"`
	CommitMS         int64  `json:"commit_ms"`
}

// TokenMeteringLaneState is the durable health and scheduling projection for
// the three independent collection lanes.  It is deliberately separate from
// the source window cursor: a lane can yield or wait for a writer while the
// window cursor remains unchanged and therefore resumable after restart.
type TokenMeteringLaneState struct {
	SourceEpoch             string  `gorm:"primaryKey;size:64" json:"source_epoch"`
	Lane                    string  `gorm:"primaryKey;size:24" json:"lane"`
	WindowStart             int64   `json:"window_start"`
	WindowEnd               int64   `json:"window_end"`
	CursorAt                int64   `json:"cursor_at"`
	CursorID                int64   `json:"cursor_id"`
	DiscoveryAt             int64   `gorm:"not null;default:0" json:"discovery_at"`
	Attempt                 int     `json:"attempt"`
	NextAt                  int64   `gorm:"index" json:"next_at"`
	LeaseToken              string  `gorm:"size:64" json:"-"`
	LeaseUntil              int64   `json:"lease_until"`
	LastReason              string  `gorm:"size:64" json:"last_reason"`
	LastFailureStage        string  `gorm:"size:32" json:"last_failure_stage"`
	RowsRead                int64   `json:"rows_read"`
	RowsCommitted           int64   `json:"rows_committed"`
	Queued                  int64   `json:"queued"`
	QueueUnit               string  `gorm:"-" json:"queue_unit"`
	ReadBudgetMS            int64   `gorm:"-" json:"read_budget_ms"`
	WriterWaitBudgetMS      int64   `gorm:"-" json:"writer_wait_budget_ms"`
	BatchRows               int     `gorm:"-" json:"batch_rows"`
	MaxInFlightBatches      int     `gorm:"-" json:"max_in_flight_batches"`
	Running                 int64   `json:"running"`
	Processed               int64   `json:"processed"`
	Failed                  int64   `json:"failed"`
	Deferred                int64   `json:"deferred"`
	LastQueuedAt            int64   `json:"last_queued_at"`
	LastStartedAt           int64   `json:"last_started_at"`
	LastSuccessAt           int64   `json:"last_success_at"`
	LastFailureAt           int64   `json:"last_failure_at"`
	LastReadMS              int64   `json:"last_read_ms"`
	LastDecodeMS            int64   `json:"last_decode_ms"`
	LastWriterWaitMS        int64   `json:"last_writer_wait_ms"`
	LastCommitMS            int64   `json:"last_commit_ms"`
	OldestPendingAt         int64   `json:"oldest_pending_at"`
	ThroughputRowsPerSecond float64 `json:"throughput_rows_per_second"`
	EstimatedCatchupSeconds *int64  `json:"estimated_catchup_seconds"`
	UpdatedAt               int64   `gorm:"index" json:"updated_at"`
}
