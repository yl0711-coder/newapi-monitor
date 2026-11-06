package monthlytasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func readTestObservations(t *testing.T, db *sql.DB) ([]TaskObservation, error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	return InspectReceiverTasks(context.Background(), tx)
}

func TestTaskInspectionValidatesMetadataAndHidesLeaseToken(t *testing.T) {
	f := newFixture(t)
	j := submit(t, f.store, requestFixture())
	lease := claim(t, f.store, j)
	before, err := readTestObservations(t, f.db)
	if err != nil || len(before) != 1 || before[0].Task.State != Running || before[0].Retryable {
		t.Fatal("running observation", before, err)
	}
	raw, err := json.Marshal(before)
	if err != nil || strings.Contains(string(raw), lease.token) {
		t.Fatal("observation leaked capability", err)
	}
	f.clock.Add(time.Second.Milliseconds())
	if err := f.store.Renew(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	after, err := readTestObservations(t, f.db)
	if err != nil || len(after) != 1 || before[0].StorageSHA256 == after[0].StorageSHA256 {
		t.Fatal("lease renewal not bound by observation", err)
	}
	if err := f.store.Fail(context.Background(), lease, "internal_failure"); err != nil {
		t.Fatal(err)
	}
	failed, err := readTestObservations(t, f.db)
	if err != nil || !failed[0].Retryable {
		t.Fatal("retryable failure lost", err)
	}
	if _, err := f.db.Exec(`UPDATE portal_monthly_draft_tasks SET descriptor='{}' WHERE id=?`, j.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := readTestObservations(t, f.db); !errors.Is(err, ErrCorrupt) || got != nil {
		t.Fatal("corrupt descriptor accepted or partially returned", got, err)
	}
}

func TestTaskInspectionUnknownSchemaAndCanceledContext(t *testing.T) {
	f := newFixture(t)
	if got, err := InspectReceiverTasks(context.Background(), nil); !errors.Is(err, ErrInvalid) || got != nil {
		t.Fatal("nil snapshot accepted", err)
	}
	tx, err := f.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := InspectReceiverTasks(ctx, tx); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatal("canceled inspection returned data", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec(`UPDATE portal_monthly_draft_task_control SET version=99`); err != nil {
		t.Fatal(err)
	}
	if got, err := readTestObservations(t, f.db); !errors.Is(err, ErrCorrupt) || got != nil {
		t.Fatal("unknown schema was accepted", err)
	}
}

func TestTaskInspectionRejectsOversizeMetadataInventory(t *testing.T) {
	f := newFixture(t)
	if _, err := f.db.Exec(`WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<257)
INSERT INTO portal_monthly_draft_tasks
(id,task_key,group_id,descriptor,state,attempts,created_at,updated_at,started_at,lease_until,lease_token,retry_at,result_sha256,failure_code)
SELECT printf('%032x',n),printf('%064x',n),n,X'7b7d','invalidated',0,1,1,0,0,'',0,'','inputs_changed' FROM numbers`); err != nil {
		t.Fatal(err)
	}
	if got, err := readTestObservations(t, f.db); !errors.Is(err, ErrCapacity) || got != nil {
		t.Fatal("oversize task inventory truncated or returned data", got, err)
	}
}
