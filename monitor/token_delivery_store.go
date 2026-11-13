package monitor

// TokenEvidenceExport is an immutable, owner-bound, expiring redacted package.
type TokenEvidenceExport struct {
	ID          string `gorm:"primaryKey;size:64" json:"id"`
	Owner       string `gorm:"index" json:"-"`
	RequestHash string `json:"-"`
	TargetID    string `json:"target_id"`
	PolicyHash  string `json:"policy_hash"`
	CreatedAt   int64  `gorm:"index" json:"created_at"`
	ExpiresAt   int64  `gorm:"index" json:"expires_at"`
	Count       int    `json:"count"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
	Manifest    string `json:"-"`
	Payload     []byte `json:"-"`
}

// TokenDeliveryAudit contains no source payload, credentials or arbitrary errors.
type TokenDeliveryAudit struct {
	ID       uint64 `gorm:"primaryKey" json:"id"`
	At       int64  `gorm:"index" json:"at"`
	Actor    string `json:"-"`
	Action   string `json:"action"`
	ObjectID string `gorm:"index" json:"object_id"`
	Reason   string `json:"reason"`
}

// TokenNotifyOutbox keeps a frozen notification and a fenced send attempt.
type TokenNotifyOutbox struct {
	ID          string `gorm:"primaryKey;size:64" json:"id"`
	TargetKey   string `gorm:"index" json:"target_key"`
	PolicyHash  string `json:"policy_hash"`
	Fingerprint string `json:"fingerprint"`
	Severity    string `json:"severity"`
	Evidence    string `json:"evidence_level"`
	Count       int64  `json:"request_count"`
	State       string `gorm:"index:idx_token_notify_due,priority:1" json:"state"`
	Reason      string `json:"reason"`
	CreatedAt   int64  `json:"created_at"`
	ExpiresAt   int64  `gorm:"index" json:"expires_at"`
	NextAt      int64  `gorm:"index:idx_token_notify_due,priority:2" json:"next_at"`
	Attempts    int    `json:"attempts"`
	LeaseToken  string `json:"-"`
	LeaseUntil  int64  `json:"lease_until"`
	SentAt      int64  `gorm:"index" json:"sent_at"`
	Payload     string `json:"-"`
}
type tokenNotifyTarget struct {
	ID          string `gorm:"primaryKey;size:64"`
	Fingerprint string
	LastSentAt  int64
	Severity    string
	Evidence    string
	SentCount   int64
	UpdatedAt   int64 `gorm:"index"`
}
type tokenDeliveryState struct {
	ID              string `gorm:"primaryKey;size:32" json:"-"`
	LeaseToken      string `json:"-"`
	LeaseUntil      int64  `json:"-"`
	LastEvaluatedAt int64  `json:"last_evaluated_at"`
	LastReason      string `json:"last_reason"`
	BudgetDay       int64  `json:"budget_day"`
	BudgetUsed      int    `json:"budget_used"`
}

func tokenDeliveryTables() []any {
	return []any{&TokenEvidenceExport{}, &TokenDeliveryAudit{}, &TokenNotifyOutbox{}, &tokenNotifyTarget{}, &tokenDeliveryState{}}
}
