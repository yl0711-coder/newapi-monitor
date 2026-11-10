package monitor

import "gorm.io/gorm"

// SQLite otherwise prefers the unique request identity index and fetches a
// large result_json row for every queue candidate. Read-only legacy snapshots
// can lack this additive covering index and must retain their working fallback.
func tokenDetectionHeadJoin(db *gorm.DB) string {
	return "LEFT JOIN " + tokenDetectionHeadTable(db) + " ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id"
}

func tokenDetectionHeadTable(db *gorm.DB) string {
	table := "token_detection_currents AS c"
	if db.Migrator().HasIndex(&TokenDetectionCurrent{}, "idx_token_detection_work_head_v2") {
		table += " INDEXED BY idx_token_detection_work_head_v2"
	} else if db.Migrator().HasIndex(&TokenDetectionCurrent{}, "idx_token_detection_work_head") {
		table += " INDEXED BY idx_token_detection_work_head"
	}
	return table
}

// GORM does not rebuild an existing named index when another tagged column is
// added. Version the covering definitions explicitly for retained stores.
func initTokenDetectionStateIndexes(db *gorm.DB) error {
	for _, statement := range []string{
		`CREATE INDEX IF NOT EXISTS idx_token_detection_work_head_v2 ON token_detection_currents(source_epoch,log_id,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,next_at,detection_state)`,
		`CREATE INDEX IF NOT EXISTS idx_token_detection_epoch_state_v1 ON token_detection_currents(source_epoch,detection_state)`,
		`CREATE INDEX IF NOT EXISTS idx_token_detection_epoch_retry_reason_v1 ON token_detection_retries(source_epoch,reason)`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}

// TokenDetectionState fences the local worker independently of ingestion.
type TokenDetectionState struct {
	SourceEpoch   string `gorm:"primaryKey;size:64" json:"source_epoch"`
	LeaseToken    string `json:"-"`
	LeaseUntil    int64  `json:"-"`
	PolicyHash    string `json:"policy_hash"`
	LastSuccessAt int64  `json:"last_success_at"`
	LastReason    string `json:"last_reason"`
	// The latest bounded round is observable independently from the durable
	// lease/retry reason.  A row-budget or time-budget handoff is a yielded
	// round, not a failed round; keeping these counters on the checkpoint lets
	// health expose that distinction without scanning the queue again.
	LastProcessed      int64  `gorm:"not null;default:0" json:"last_processed"`
	LastYielded        int64  `gorm:"not null;default:0" json:"last_yielded"`
	LastFailed         int64  `gorm:"not null;default:0" json:"last_failed"`
	LastDeferred       int64  `gorm:"not null;default:0" json:"last_deferred"`
	FreshWindowAt      int64  `gorm:"not null;default:0" json:"-"`
	CorrectionWindowAt int64  `gorm:"not null;default:0" json:"-"`
	HistoryScanAt      int64  `gorm:"not null;default:0" json:"historical_scan_at"`
	HistoryScanID      int64  `gorm:"not null;default:0" json:"historical_scan_id"`
	RecentScanAt       int64  `gorm:"not null;default:0" json:"recent_scan_at"`
	RecentScanID       int64  `gorm:"not null;default:0" json:"recent_scan_id"`
	ScanPolicyHash     string `gorm:"not null;default:''" json:"-"`
}

// TokenDetectionCurrent has one statistical contribution per request.
type TokenDetectionCurrent struct {
	ReferenceBucket     string `gorm:"index;index:idx_token_detection_work_head,priority:7;index:idx_token_detection_due_head,priority:8" json:"reference_bucket"`
	ReferenceGeneration int64  `gorm:"index:idx_token_detection_work_head,priority:6;index:idx_token_detection_due_head,priority:9" json:"reference_generation"`
	SourceEpoch         string `gorm:"primaryKey;size:64;index:idx_token_detection_report_range,priority:1;index:idx_token_detection_report_status,priority:1;index:idx_token_detection_work_head,priority:1;index:idx_token_detection_cache_signal,priority:1;index:idx_token_detection_due_head,priority:1" json:"source_epoch"`
	LogID               int64  `gorm:"primaryKey;autoIncrement:false;index:idx_token_detection_report_range,priority:3;index:idx_token_detection_report_status,priority:4;index:idx_token_detection_work_head,priority:2;index:idx_token_detection_due_head,priority:4" json:"log_id"`
	CreatedAt           int64  `gorm:"index:idx_token_detection_time;index:idx_token_detection_report_range,priority:2;index:idx_token_detection_report_status,priority:3;index:idx_token_detection_cache_signal,priority:5;index:idx_token_detection_due_head,priority:3" json:"created_at"`
	UserID              int64  `json:"user_id"`
	ChannelID           int64  `gorm:"index:idx_token_detection_cache_signal,priority:2" json:"channel_id"`
	Model               string `json:"model"`
	UserGroup           string `json:"user_group"`
	FactRevision        int64  `gorm:"index:idx_token_detection_work_head,priority:3;index:idx_token_detection_due_head,priority:5" json:"fact_revision"`
	LinkRevision        int64  `gorm:"index:idx_token_detection_work_head,priority:4;index:idx_token_detection_due_head,priority:6" json:"link_revision"`
	PolicyHash          string `gorm:"index:idx_token_detection_work_head,priority:5;index:idx_token_detection_due_head,priority:7" json:"policy_hash"`
	Revision            int64  `json:"revision"`
	DecisionHash        string `json:"decision_hash"`
	SourceComplete      bool   `gorm:"index:idx_token_detection_work_head,priority:8;index:idx_token_detection_due_head,priority:10" json:"source_complete"`
	Bucket              string `gorm:"index;index:idx_token_detection_cache_signal,priority:3" json:"bucket"`
	CacheSignal         bool   `gorm:"index:idx_token_detection_cache_signal,priority:4" json:"cache_signal"`
	Candidate           bool   `gorm:"index;index:idx_token_detection_report_status,priority:2" json:"candidate"`
	Quarantined         bool   `json:"quarantined"`
	// DetectionState is a durable display/retention classification. Empty is
	// legacy or not yet evaluated and must remain pending until reprocessed.
	DetectionState   string `gorm:"size:32;index:idx_token_detection_state;index:idx_token_detection_work_head,priority:10" json:"detection_state"`
	Stage            int    `json:"stage"`
	NextAt           int64  `gorm:"index;index:idx_token_detection_work_head,priority:9;index:idx_token_detection_due_head,priority:2" json:"next_at"`
	FirstEvaluatedAt int64  `json:"first_evaluated_at"`
	EvaluatedAt      int64  `json:"evaluated_at"`
	ExpiresAt        int64  `gorm:"index" json:"expires_at"`
	ResultJSON       string `json:"-"`
}

// TokenDetectionRevision freezes operands independently of expiring facts.
type TokenDetectionRevision struct {
	CompactStorage bool   `gorm:"-" json:"-"`
	SourceEpoch    string `gorm:"primaryKey;size:64" json:"source_epoch"`
	LogID          int64  `gorm:"primaryKey;autoIncrement:false" json:"log_id"`
	Revision       int64  `gorm:"primaryKey;autoIncrement:false" json:"revision"`
	DecisionHash   string `json:"decision_hash"`
	RecordedAt     int64  `json:"recorded_at"`
	ExpiresAt      int64  `gorm:"index" json:"expires_at"`
	FrozenJSON     string `gorm:"serializer:token_storage_json_v1" json:"frozen"`
	StoredBytes    int64  `json:"-"`
}

// TokenDetectionReference is an immutable statistical anchor with a time bound.
type TokenDetectionReference struct {
	InvalidReason string `json:"invalid_reason,omitempty"`
	InvalidatedAt int64  `json:"invalidated_at,omitempty"`
	ID            string `gorm:"primaryKey;size:64" json:"id"`
	Bucket        string `gorm:"index:idx_token_reference_bucket,priority:1" json:"bucket"`
	Through       int64  `gorm:"index:idx_token_reference_bucket,priority:2" json:"through"`
	RecordedAt    int64  `json:"recorded_at"`
	ExpiresAt     int64  `gorm:"index" json:"expires_at"`
	SnapshotJSON  string `json:"snapshot"`
	StoredBytes   int64  `json:"-"`
}

// TokenDetectionBucket invalidates comparable results when local facts change.
// It is bounded independently; no source or Incident cursor is involved.
type TokenDetectionBucket struct {
	ID                string `gorm:"primaryKey;size:64" json:"id"`
	Generation        int64  `json:"generation"`
	ChangedAt         int64  `json:"changed_at"`
	ParentBucket      string `gorm:"not null;default:'';index:idx_token_detection_bucket_window,priority:1" json:"-"`
	WindowStart       int64  `gorm:"not null;default:0;index:idx_token_detection_bucket_window,priority:2" json:"-"`
	ChannelID         int64  `gorm:"not null;default:0" json:"-"`
	LengthMin         int64  `gorm:"not null;default:0" json:"-"`
	LengthMax         int64  `gorm:"not null;default:0" json:"-"`
	LengthBoundsKnown bool   `gorm:"not null;default:0" json:"-"`
}

// TokenDetectionRetry defers one capacity-limited job without consuming its head.
// Source changes bypass the backoff. Rows are bounded and cleared on success.
type TokenDetectionRetry struct {
	SourceEpoch  string `gorm:"primaryKey;size:64" json:"source_epoch"`
	LogID        int64  `gorm:"primaryKey;autoIncrement:false" json:"log_id"`
	FactRevision int64  `json:"fact_revision"`
	LinkRevision int64  `json:"link_revision"`
	PolicyHash   string `json:"policy_hash"`
	Attempts     int    `json:"attempts"`
	NextAt       int64  `gorm:"index" json:"next_at"`
	ExpiresAt    int64  `gorm:"index" json:"expires_at"`
	Reason       string `json:"reason"`
}
