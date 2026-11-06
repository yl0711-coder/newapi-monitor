package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

var (
	ErrGenerationSelectionMissing  = errors.New("generation source selection unavailable")
	ErrGenerationSelectionConflict = errors.New("generation source selection changed; reload before selecting")
)

const generationSelectionLimit = 1024
const generationSelectionMaxRecords = 2 * generationSelectionLimit // Reserve room for withdrawal at capacity.
const generationSelectionBytes = 2048

// GenerationSelection is INTERNAL control metadata, not a customer DTO. A nil
// Request is an explicit withdrawal, not a missing or incomplete database read.
type GenerationSelection struct {
	Head    string
	Request *monthlytasks.Request
}

type generationSelectionRecord struct {
	Version  int                   `json:"version"`
	GroupID  int64                 `json:"group_id"`
	Month    string                `json:"month"`
	Previous string                `json:"previous"`
	Request  *monthlytasks.Request `json:"request"`
}

// InitGenerationSelections is an explicit LOCAL opt-in on an already initialized
// task receiver. It neither selects an input nor starts workers or migrations in
// production. The bounded append-only ledger cannot delete revocation history.
func InitGenerationSelections(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrGenerationInput
	}
	return generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		for _, query := range []string{
			`CREATE TABLE IF NOT EXISTS portal_monthly_generation_selections (
sequence INTEGER PRIMARY KEY AUTOINCREMENT,
id TEXT NOT NULL UNIQUE CHECK(length(id)=64),group_id INTEGER NOT NULL CHECK(group_id>0),
month TEXT NOT NULL CHECK(length(month)=7),payload BLOB NOT NULL CHECK(length(payload)<=2048))`,
			`CREATE INDEX IF NOT EXISTS idx_monthly_generation_selection_scope ON portal_monthly_generation_selections(group_id,month,sequence)`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_selection_no_update BEFORE UPDATE ON portal_monthly_generation_selections BEGIN SELECT RAISE(ABORT,'selection history is immutable'); END`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_selection_no_delete BEFORE DELETE ON portal_monthly_generation_selections BEGIN SELECT RAISE(ABORT,'selection history is immutable'); END`,
		} {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
		return nil
	})
}

func validSelectionScope(gid int64, month string) bool {
	m, err := time.Parse("2006-01", month)
	return gid > 0 && err == nil && m.Format("2006-01") == month && m.Year() >= 2020
}

func validSelectionRequest(r monthlytasks.Request, gid int64, month string) bool {
	return r.Owner.GroupID == gid && r.Month == month && r.Owner.AuthVersion >= 0 &&
		generationHex(r.Owner.MemberFingerprint, 16) && generationHex(r.Owner.MemberManifest, 32) &&
		generationHex(r.SourceIdentity, 32) && generationHex(r.InputSHA256, 32) && generationHex(r.RulesSHA256, 32)
}

type selectionQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Recheck the whole bounded per-scope chain, not merely the newest row. This
// catches missing predecessors, bad descriptors and reactivation of old inputs.
func readGenerationSelection(ctx context.Context, q selectionQuerier, gid int64, month string) (GenerationSelection, map[monthlytasks.Request]bool, error) {
	var out GenerationSelection
	seen := make(map[monthlytasks.Request]bool)
	rows, err := q.QueryContext(ctx, `SELECT id,CASE WHEN length(CAST(payload AS BLOB))<=2048 THEN payload END
FROM portal_monthly_generation_selections WHERE group_id=? AND month=? ORDER BY sequence LIMIT 2049`, gid, month)
	if err != nil {
		return out, nil, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return GenerationSelection{}, nil, err
		}
		var record generationSelectionRecord
		if count > generationSelectionMaxRecords || !generationHex(id, 32) || generationHash(raw) != id || json.Unmarshal(raw, &record) != nil {
			return GenerationSelection{}, nil, ErrGenerationIntegrity
		}
		canonical, err := json.Marshal(record)
		if err != nil || !bytes.Equal(canonical, raw) || record.Version != 1 || record.GroupID != gid || record.Month != month || record.Previous != out.Head {
			return GenerationSelection{}, nil, ErrGenerationIntegrity
		}
		if record.Request != nil {
			if !validSelectionRequest(*record.Request, gid, month) || seen[*record.Request] {
				return GenerationSelection{}, nil, ErrGenerationIntegrity
			}
			seen[*record.Request] = true
		} else if out.Head == "" || out.Request == nil {
			return GenerationSelection{}, nil, ErrGenerationIntegrity
		}
		out = GenerationSelection{Head: id, Request: record.Request}
	}
	if err := rows.Err(); err != nil {
		return GenerationSelection{}, nil, err
	}
	return out, seen, nil
}

// ReadGenerationSelection never initializes/repairs a missing registry. A blank
// Head means no selection; a nonblank Head and nil Request means withdrawn.
func ReadGenerationSelection(parent context.Context, db *sql.DB, gid int64, month string) (GenerationSelection, error) {
	if db == nil || !validSelectionScope(gid, month) {
		return GenerationSelection{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	out, _, err := readGenerationSelection(ctx, db, gid, month)
	if err != nil {
		return GenerationSelection{}, err
	}
	if err := ctx.Err(); err != nil {
		return GenerationSelection{}, err
	}
	return out, nil
}

// GenerationSelector supplies the real persisted selection to the Usage guard.
// Missing/corrupt/unavailable records do not revoke a task; explicit withdrawal
// does. The handle should be read-only and remains owned by the caller.
func GenerationSelector(db *sql.DB) (SelectedGenerationRequest, error) {
	if db == nil {
		return nil, ErrGenerationInput
	}
	return func(ctx context.Context, gid int64, month string) (monthlytasks.Request, error) {
		out, err := ReadGenerationSelection(ctx, db, gid, month)
		if err != nil {
			return monthlytasks.Request{}, err
		}
		if out.Head == "" {
			return monthlytasks.Request{}, ErrGenerationSelectionMissing
		}
		if out.Request == nil {
			return monthlytasks.Request{}, ErrGenerationChanged
		}
		return *out.Request, nil
	}, nil
}

// SelectGenerationVersion registers a canonical descriptor supplied by TRUSTED
// server-side orchestration, never customer input. It validates shape/binding,
// not financial truth or source freshness: generation still audits those inputs.
// expectedHead is mandatory compare-and-swap (empty only for the first version).
// This stores no amounts, plans or evidence copies and cannot publish a bill.
func SelectGenerationVersion(ctx context.Context, db *sql.DB, plan GenerationPlan, preparation []byte, expectedHead string) (GenerationSelection, error) {
	p, raw, _, err := canonicalGeneration(plan, preparation, time.Now())
	if err != nil {
		return GenerationSelection{}, err
	}
	req := generationRequest(p, raw)
	return changeGenerationSelection(ctx, db, p.Owner.GroupID, p.Funding.Month, &req, expectedHead)
}

// WithdrawGenerationSelection is explicit and permanent for the selected input
// descriptor. Re-enabling requires a NEW validated input version; old task keys
// must not become current again (including tasks not read during withdrawal).
func WithdrawGenerationSelection(ctx context.Context, db *sql.DB, gid int64, month, expectedHead string) (GenerationSelection, error) {
	return changeGenerationSelection(ctx, db, gid, month, nil, expectedHead)
}

func changeGenerationSelection(ctx context.Context, db *sql.DB, gid int64, month string, request *monthlytasks.Request, expectedHead string) (GenerationSelection, error) {
	return changeGenerationSelectionPrepared(ctx, db, gid, month, request, expectedHead, nil)
}

// prepare is trusted receiver-only storage work in the SAME transaction. It is
// never invoked on rejected selection conflicts or historical reactivation.
func changeGenerationSelectionPrepared(ctx context.Context, db *sql.DB, gid int64, month string, request *monthlytasks.Request, expectedHead string, prepare func(context.Context, *sql.Tx, GenerationSelection) error) (GenerationSelection, error) {
	var result GenerationSelection
	if db == nil || !validSelectionScope(gid, month) || (expectedHead != "" && !generationHex(expectedHead, 32)) {
		return result, ErrGenerationInput
	}
	err := generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		current, seen, err := readGenerationSelection(ctx, tx, gid, month)
		if err != nil {
			return err
		}
		if current.Head != expectedHead {
			return ErrGenerationSelectionConflict
		}
		if request == nil && current.Head == "" {
			return ErrGenerationSelectionMissing
		}
		same := (request == nil && current.Request == nil) || (request != nil && current.Request != nil && *request == *current.Request)
		if same {
			if prepare != nil {
				if err := prepare(ctx, tx, current); err != nil {
					return err
				}
			}
			result = current
			return nil
		}
		if request != nil && seen[*request] {
			return ErrGenerationChanged
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_generation_selections`).Scan(&count); err != nil {
			return err
		}
		limit := generationSelectionLimit
		if request == nil {
			limit = generationSelectionMaxRecords
		}
		if count >= limit {
			return ErrGenerationCapacity
		}
		if prepare != nil {
			if err := prepare(ctx, tx, current); err != nil {
				return err
			}
		}
		raw, err := json.Marshal(generationSelectionRecord{Version: 1, GroupID: gid, Month: month, Previous: current.Head, Request: request})
		if err != nil || len(raw) > generationSelectionBytes {
			return ErrGenerationIntegrity
		}
		id := generationHash(raw)
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_selections(id,group_id,month,payload) VALUES(?,?,?,?)`, id, gid, month, raw); err != nil {
			return err
		}
		result = GenerationSelection{Head: id, Request: request}
		return nil
	})
	if err != nil {
		return GenerationSelection{}, err
	}
	return result, nil
}
