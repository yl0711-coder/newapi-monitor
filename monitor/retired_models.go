package monitor

// Retired feature models.
//
// The features below were removed from the code, but their tables are kept
// and still created by AutoMigrate so that the database schema, the
// pre-migration plan ID and the rollback snapshots stay exactly as they were.
// Dropping these tables is a separate, later release that bumps the plan ID.
// Do not change these definitions: any change alters the schema hash and is
// caught by TestSchemaChangeRequiresPlanBump.

// CloudWatch Shadow reconciliation (retired in v1.18.69; never enabled in
// production, both tables are empty).

// CloudWatchShadowReconciliationRun is the durable, low-cardinality header
// for one bounded comparison window.  It intentionally contains no raw
// Request ID, customer IP, log body, model, group or user identity.
type CloudWatchShadowReconciliationRun struct {
	ID             string `gorm:"primaryKey;size:64"`
	WindowFrom     int64  `gorm:"index:idx_cw_shadow_run_window,priority:1;not null"`
	WindowTo       int64  `gorm:"index:idx_cw_shadow_run_window,priority:2;not null"`
	Status         string `gorm:"size:24;index;not null"`
	CreatedAtUnix  int64  `gorm:"index;not null"`
	StartedAtUnix  int64  `gorm:"not null"`
	FinishedAtUnix int64  `gorm:"not null"`
	QueryCount     int    `gorm:"not null"`
	BytesScanned   int64  `gorm:"not null"`

	NginxStatus     string `gorm:"size:24;not null"`
	RejectionStatus string `gorm:"size:24;not null"`
	NginxOldCount   int64  `gorm:"not null"`
	NginxNewCount   int64  `gorm:"not null"`
	RejectOldCount  int64  `gorm:"not null"`
	RejectNewCount  int64  `gorm:"not null"`
	DifferenceCount int    `gorm:"not null"`
	BlindSpots      string `gorm:"size:2048;not null"`
}

// CloudWatchShadowReconciliationDiff stores only a closed metric set and an
// HMAC of the comparison dimension.  DimensionHMAC lets the operator group
// repeated differences without putting a route/model/group/user value into
// the durable audit store.
type CloudWatchShadowReconciliationDiff struct {
	ID               uint   `gorm:"primaryKey"`
	RunID            string `gorm:"size:64;index:idx_cw_shadow_diff_run,priority:1;not null"`
	Lane             string `gorm:"size:24;index:idx_cw_shadow_diff_run,priority:2;not null"`
	BucketTs         int64  `gorm:"index:idx_cw_shadow_diff_bucket;not null"`
	DimensionClass   string `gorm:"size:48;not null"`
	DimensionHMAC    string `gorm:"size:64;index;not null"`
	OldCount         int64  `gorm:"not null"`
	NewCount         int64  `gorm:"not null"`
	Delta            int64  `gorm:"not null"`
	OldRequestSumMS  int64  `gorm:"not null"`
	NewRequestSumMS  int64  `gorm:"not null"`
	OldRequestMaxMS  int64  `gorm:"not null"`
	NewRequestMaxMS  int64  `gorm:"not null"`
	OldUpstreamSumMS int64  `gorm:"not null"`
	NewUpstreamSumMS int64  `gorm:"not null"`
	OldRequestIDs    int64  `gorm:"not null"`
	NewRequestIDs    int64  `gorm:"not null"`
	DifferenceMask   string `gorm:"size:160;not null"`
	CreatedAtUnix    int64  `gorm:"index;not null"`
}
