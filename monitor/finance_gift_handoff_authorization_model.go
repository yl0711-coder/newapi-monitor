package monitor

// Approval metadata lives in Monitor's main store, not in the facts store.
// Its table is only migrated when the separate approval flag is enabled.
// No worker consumes these rows yet. Never treat their existence as execution.
type financeGiftHandoffAuthorization struct {
	ID          string `gorm:"primaryKey;size:64"`
	Payload     string
	PayloadHash string
	RevokedAt   int64
	RevokedBy   string
}

func (financeGiftHandoffAuthorization) TableName() string {
	return "finance_gift_handoff_authorizations"
}
