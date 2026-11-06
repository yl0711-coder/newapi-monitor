package monthlysource

// MonthlyObservationMaterial is a PRIVATE, versioned file handoff, not a
// GenerationPlan or proof of settled balances/historical organization scope.
// A consumer must pin its canonical file digest, verify every referenced file
// and re-audit the retained observations before using it to prepare a bill.
type MonthlyObservationMaterial struct {
	Version        int                         `json:"version"`
	Mode           string                      `json:"mode"`
	GroupID        int64                       `json:"group_id"`
	FundingVersion int                         `json:"funding_version"`
	Funding        FundingMonthRequest         `json:"funding"`
	FundingWindows []FundingMonthWindowVersion `json:"funding_windows"`
	UsageWindows   []UsageWindowVersion        `json:"usage_windows"`
	// Version 2 explicitly references the unchanged complete captured index.
	// Readers derive partitions and re-audit originals, never cached totals.
	UsageSelection *MonthlyObservationFile       `json:"usage_selection,omitempty"`
	WalletIDs      []string                      `json:"wallet_ids"`
	Database       MonthlyObservationFile        `json:"observation_database"`
	Tokens         []MonthlyObservationTokenFile `json:"token_files"`
	// Optional source-preserving alternative to WalletIDs; never a DB mutation.
	WalletReuse *MonthlyObservationFile `json:"wallet_reuse,omitempty"`
}

type MonthlyObservationFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type MonthlyObservationTokenFile struct {
	UserID int64                  `json:"user_id"`
	File   MonthlyObservationFile `json:"file"`
}

const MonthlyObservationMaterialMode = "sealed_observation_material_not_bill_approval"
const PartitionedObservationMaterialMode = "sealed_partitioned_capture_material_not_bill_approval"
