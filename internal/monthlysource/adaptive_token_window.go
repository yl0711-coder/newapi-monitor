package monthlysource

// An explicit, private producer for ONE failed source window. It does not
// replace the old month slots, invent a parent observation, select a financial
// input or publish a bill. Month scheduler/sealer integration is separate.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

var ErrAdaptiveTokenIntegrity = errors.New("adaptive token evidence inconsistent")
var ErrAdaptiveTokenBudget = errors.New("adaptive token capture budget reached")
var ErrAdaptiveTokenStopped = errors.New("adaptive token capture stopped after source failures")

const adaptiveTokenJobs = 128
const adaptiveTokenSplits = 15
const adaptiveTokenBytes = 64 << 20

type AdaptiveTokenWindowProgress struct {
	Mode           string `json:"mode"`
	Leaves         int    `json:"leaves"`
	Observations   int    `json:"observations"`
	MatchingPairs  int    `json:"matching_pairs"`
	NextEligibleMS int64  `json:"next_eligible_unix_ms,omitempty"`
	Captured       bool   `json:"captured"`
	Split          bool   `json:"split"`
	Complete       bool   `json:"complete"`
}

type adaptiveTokenRoot struct {
	Version int                `json:"version"`
	JobKey  string             `json:"job_key"`
	Request UsageWindowRequest `json:"request"`
}
type adaptiveTokenNode struct{ from, to int64 }
type adaptiveTokenSlot struct {
	adaptiveTokenNode
	phase int
}
type adaptiveTokenRecord struct {
	sha, usage         string
	started, completed int64
}
type adaptiveTokenState struct {
	root          adaptiveTokenRoot
	id            string
	leaves        []UsageWindowRequest
	records       map[adaptiveTokenSlot]adaptiveTokenRecord
	nextAt        int64
	created       int64
	faults        []adaptiveTokenFault
	epoch         int
	resumeHead    string
	resumeEnabled bool
}

type adaptiveTokenReader interface {
	walletRowReader
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Explicit setup of a separate private capture SQLite only; never called by
// constructors, HTTP or production startup. Existing monthly slots are intact.
func InitAdaptiveTokenCaptureStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrAdaptiveTokenIntegrity
	}
	if err := InitUsageWindowStore(parent, db); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS monthly_adaptive_token_roots(id TEXT PRIMARY KEY CHECK(length(id)=64),payload BLOB NOT NULL CHECK(length(payload)<=2048),created_at_ms INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS monthly_adaptive_token_splits(root_id TEXT NOT NULL,window_from INTEGER NOT NULL,window_to INTEGER NOT NULL,phase INTEGER NOT NULL CHECK(phase IN (0,1)),PRIMARY KEY(root_id,window_from,window_to))`,
		`CREATE TABLE IF NOT EXISTS monthly_adaptive_token_observations(root_id TEXT NOT NULL,window_from INTEGER NOT NULL,window_to INTEGER NOT NULL,phase INTEGER NOT NULL CHECK(phase IN (0,1)),sha TEXT NOT NULL CHECK(length(sha)=64),usage_id TEXT NOT NULL CHECK(length(usage_id)=64),started_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL,payload BLOB NOT NULL CHECK(length(payload)<=2097152),projection BLOB NOT NULL CHECK(length(projection)<=2097152),PRIMARY KEY(root_id,window_from,window_to,phase))`,
		`CREATE TABLE IF NOT EXISTS monthly_adaptive_token_faults(root_id TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 2),code TEXT NOT NULL,at_ms INTEGER NOT NULL,terminal INTEGER NOT NULL CHECK(terminal IN (0,1)),PRIMARY KEY(root_id,ordinal))`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	for _, table := range []string{"roots", "splits", "observations", "faults"} {
		for _, action := range []string{"UPDATE", "DELETE"} {
			q := `CREATE TRIGGER IF NOT EXISTS adaptive_token_` + table + `_no_` + action + ` BEFORE ` + action + ` ON monthly_adaptive_token_` + table + ` BEGIN SELECT RAISE(ABORT,'adaptive token records are immutable'); END`
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func adaptiveTokenIdentity(job string, request UsageWindowRequest) (adaptiveTokenRoot, []byte, string, error) {
	r := adaptiveTokenRoot{Version: 1, JobKey: job, Request: request}
	if !validDigest(job) || !validUsageWindowRequest(request) || request.To > time.Now().Unix() {
		return r, nil, "", ErrAdaptiveTokenIntegrity
	}
	raw, err := json.Marshal(r)
	return r, raw, digest(raw), err
}

func ensureAdaptiveTokenRoot(ctx context.Context, db interface {
	walletRowReader
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, job string, req UsageWindowRequest, now time.Time) error {
	_, raw, id, err := adaptiveTokenIdentity(job, req)
	if err != nil {
		return err
	}
	var existing []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM monthly_adaptive_token_roots WHERE id=?`, id).Scan(&existing); err == nil {
		if !bytes.Equal(raw, existing) {
			return ErrAdaptiveTokenIntegrity
		}
		return nil // No no-op writer lock during five-minute waits.
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO monthly_adaptive_token_roots(id,payload,created_at_ms) SELECT ?,?,? WHERE (SELECT count(*) FROM monthly_adaptive_token_roots)<? ON CONFLICT(id) DO NOTHING`, id, raw, now.UnixMilli(), adaptiveTokenJobs)
	if err != nil {
		return err
	}
	var stored []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM monthly_adaptive_token_roots WHERE id=?`, id).Scan(&stored); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAdaptiveTokenBudget
		}
		return err
	}
	if !bytes.Equal(raw, stored) {
		return ErrAdaptiveTokenIntegrity
	}
	return nil
}

func loadAdaptiveTokenState(ctx context.Context, db adaptiveTokenReader, job string, req UsageWindowRequest, now time.Time) (adaptiveTokenState, error) {
	return loadAdaptiveTokenStateMode(ctx, db, job, req, now, false)
}

func loadAdaptiveTokenStateMode(ctx context.Context, db adaptiveTokenReader, job string, req UsageWindowRequest, now time.Time, inspectStopped bool) (adaptiveTokenState, error) {
	root, raw, id, err := adaptiveTokenIdentity(job, req)
	s := adaptiveTokenState{root: root, id: id, records: map[adaptiveTokenSlot]adaptiveTokenRecord{}}
	if err != nil {
		return s, err
	}
	var stored []byte
	var created int64
	if err := db.QueryRowContext(ctx, `SELECT payload,created_at_ms FROM monthly_adaptive_token_roots WHERE id=?`, id).Scan(&stored, &created); err != nil {
		return s, err
	}
	if !bytes.Equal(raw, stored) || created <= 0 || created > now.UnixMilli() {
		return s, ErrAdaptiveTokenIntegrity
	}
	s.created = created
	splits, err := adaptiveTokenSplitNodes(ctx, db, id)
	if err != nil {
		return s, err
	}
	nodes := map[adaptiveTokenNode]bool{}
	used := 0
	var walk func(UsageWindowRequest) error
	walk = func(r UsageWindowRequest) error {
		node := adaptiveTokenNode{r.From, r.To}
		nodes[node] = true
		if !splits[node] {
			s.leaves = append(s.leaves, r)
			return nil
		}
		used++
		children, err := SplitUsageWindow(r)
		if err != nil {
			return ErrAdaptiveTokenIntegrity
		}
		for _, c := range children {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(req); err != nil || used != len(splits) || len(s.leaves) > 16 {
		return s, ErrAdaptiveTokenIntegrity
	}
	rows, err := db.QueryContext(ctx, `SELECT window_from,window_to,phase,sha,usage_id,started_at_ms,completed_at_ms,length(payload),length(projection) FROM monthly_adaptive_token_observations WHERE root_id=? LIMIT 63`, id)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var key adaptiveTokenSlot
		var record adaptiveTokenRecord
		var size, projection int
		if err := rows.Scan(&key.from, &key.to, &key.phase, &record.sha, &record.usage, &record.started, &record.completed, &size, &projection); err != nil {
			_ = rows.Close()
			return s, err
		}
		if !nodes[key.adaptiveTokenNode] || key.phase < 0 || key.phase > 1 || !validDigest(record.sha) || !validDigest(record.usage) || record.started <= 0 || record.completed < record.started || record.completed > now.UnixMilli() || size <= 0 || size > TokenUsageObservationLimit || projection <= 0 || projection > TokenUsageObservationLimit || len(s.records) >= 62 {
			_ = rows.Close()
			return s, ErrAdaptiveTokenIntegrity
		}
		if _, exists := s.records[key]; exists {
			_ = rows.Close()
			return s, ErrAdaptiveTokenIntegrity
		}
		s.records[key] = record
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return s, err
	}
	if err := loadAdaptiveTokenFaultCursor(ctx, db, &s, now); err != nil {
		return s, err
	}
	terminal := false
	for _, f := range s.faults {
		terminal = terminal || f.Terminal
		s.nextAt = max(s.nextAt, f.AtMS+30000)
	}
	if !inspectStopped && (terminal || len(s.faults) >= 3) {
		return s, ErrAdaptiveTokenStopped
	}
	return s, nil
}

func adaptiveTokenSplitNodes(ctx context.Context, db adaptiveTokenReader, id string) (map[adaptiveTokenNode]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT window_from,window_to,phase FROM monthly_adaptive_token_splits WHERE root_id=? LIMIT 16`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[adaptiveTokenNode]bool{}
	for rows.Next() {
		var node adaptiveTokenNode
		var phase int
		if err := rows.Scan(&node.from, &node.to, &phase); err != nil {
			return nil, err
		}
		if len(out) >= adaptiveTokenSplits || out[node] || phase < 0 || phase > 1 {
			return nil, ErrAdaptiveTokenIntegrity
		}
		out[node] = true
	}
	return out, rows.Err()
}

func adaptiveTokenFaultCode(code string) bool {
	switch code {
	case "transport", "retryable_transport", "retryable_source", "source", "minimum_window", "byte_budget", "pair_changed":
		return true
	}
	return false
}

func saveAdaptiveTokenFault(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, s adaptiveTokenState, code string, terminal bool, now time.Time) error {
	if !adaptiveTokenFaultCode(code) {
		return ErrAdaptiveTokenIntegrity
	}
	stop := 0
	if terminal {
		stop = 1
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	query := `INSERT INTO monthly_adaptive_token_faults(root_id,ordinal,code,at_ms,terminal) SELECT ?,count(*),?,?,? FROM monthly_adaptive_token_faults WHERE root_id=? HAVING count(*)=? AND count(*)<3`
	args := []any{s.id, code, now.UnixMilli(), stop, s.id, len(s.faults)}
	if s.epoch > 0 {
		query = `INSERT INTO monthly_adaptive_token_resume_faults(root_id,epoch,ordinal,code,at_ms,terminal) SELECT ?,?,count(*),?,?,? FROM monthly_adaptive_token_resume_faults WHERE root_id=? AND epoch=? HAVING count(*)=? AND count(*)<3`
		args = []any{s.id, s.epoch, code, now.UnixMilli(), stop, s.id, s.epoch, len(s.faults)}
	}
	if s.resumeEnabled {
		query += ` AND (SELECT COALESCE(max(ordinal),0) FROM monthly_adaptive_token_resumptions WHERE root_id=?)=?`
		args = append(args, s.id, s.epoch)
	}
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return ErrAdaptiveTokenIntegrity
	}
	return err
}

func adaptiveTokenProgress(s adaptiveTokenState, now time.Time) (AdaptiveTokenWindowProgress, UsageWindowRequest, int) {
	p := AdaptiveTokenWindowProgress{Mode: "independent_child_observations_not_parent_or_bill", Leaves: len(s.leaves)}
	var next UsageWindowRequest
	phase := -1
	for _, leaf := range s.leaves {
		key := adaptiveTokenSlot{adaptiveTokenNode{leaf.From, leaf.To}, 0}
		a, old := s.records[key]
		key.phase = 1
		b, newer := s.records[key]
		if old {
			p.Observations++
		}
		if newer {
			p.Observations++
		}
		if newer && !old || newer && b.started-a.completed < 300000 {
			return p, UsageWindowRequest{}, -2
		}
		if old && newer {
			p.MatchingPairs++ // actual originals are revalidated before Complete
		} else if !old {
			if phase != 0 {
				next, phase = leaf, 0
			}
		} else if eligible := a.completed + 300000; eligible > now.UnixMilli() {
			if p.NextEligibleMS == 0 || eligible < p.NextEligibleMS {
				p.NextEligibleMS = eligible
			}
		} else if phase == -1 {
			next, phase = leaf, 1
		}
	}
	p.Complete = p.MatchingPairs == p.Leaves
	if s.nextAt > now.UnixMilli() {
		p.NextEligibleMS, next, phase = s.nextAt, UsageWindowRequest{}, -1
	} else if phase >= 0 {
		p.NextEligibleMS = 0 // useful older/ready work before waiting
	}
	return p, next, phase
}
