package monthlydraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

const retentionInspectionPolicy = "local-copy-candidates-v1"
const retentionReviewAge = 30 * 24 * time.Hour // LOCAL proposal, not a production purge policy.

var retentionInspectionSlots = make(chan struct{}, 1)

// retentionInspectionTimeout bounds one receiver inspection snapshot. A
// variable only so race-detector tests of the maximum record bound can widen it.
var retentionInspectionTimeout = 5 * time.Second

// RetentionInspection is an INTERNAL read-only inventory. It contains no money,
// usernames, paths, raw logs or lease tokens. A copy candidate is NOT permission
// to delete, publish or restore anything. Source evidence is not re-audited.
type RetentionInspection struct {
	Mode                  string          `json:"mode"`
	Policy                string          `json:"policy"`
	AsOfMS                int64           `json:"as_of_ms"`
	SnapshotSHA256        string          `json:"snapshot_sha256"`
	TaskCount             int             `json:"task_count"`
	InputCount            int             `json:"input_count"`
	RetainedPayloadBytes  int64           `json:"retained_payload_bytes"`
	ActiveTasks           int             `json:"active_tasks"`
	RetryableTasks        int             `json:"retryable_tasks"`
	ArchiveCopyCandidates int             `json:"archive_copy_candidates"`
	ArchivedInputs        int             `json:"archived_inputs"`
	ReviewRequired        int             `json:"review_required"`
	DeleteAllowed         bool            `json:"delete_allowed"`
	SourceEvidenceChecked bool            `json:"source_evidence_checked"`
	Entries               []RetentionItem `json:"entries"`
}

type RetentionItem struct {
	InputSHA256      string   `json:"input_sha256"`
	TaskIDs          []string `json:"task_ids"`
	GroupID          int64    `json:"group_id"`
	Month            string   `json:"month"`
	Disposition      string   `json:"disposition"`
	Reason           string   `json:"reason"`
	Integrity        string   `json:"integrity"`
	LastActivityMS   int64    `json:"last_activity_ms"`
	PayloadBytes     int64    `json:"payload_bytes"`
	KeepTaskMetadata bool     `json:"keep_task_metadata"`
	KeepInvalidation bool     `json:"keep_invalidation"`
}

type retainedInputCheck struct {
	id, digest string
	size       int64
	request    monthlytasks.Request
	err        error
}

// InspectGenerationRetention reads only this feature's receiver, in one bounded
// snapshot. Open db mode=ro for additional enforcement. It does not inspect live
// authorization, source files, funding scans or formal publications; it cannot
// certify a customer bill or authorize subsequent destructive maintenance.
func InspectGenerationRetention(parent context.Context, db *sql.DB, asOf time.Time) (RetentionInspection, error) {
	if db == nil || asOf.UnixMilli() <= 0 {
		return RetentionInspection{}, ErrGenerationInput
	}
	select {
	case retentionInspectionSlots <- struct{}{}:
		defer func() { <-retentionInspectionSlots }()
	default:
		return RetentionInspection{}, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, retentionInspectionTimeout)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return RetentionInspection{}, err
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return RetentionInspection{}, err
	}
	closed := false
	defer func() {
		if !closed {
			if err := tx.Rollback(); err != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
	}()
	out, err := inspectRetentionSnapshot(ctx, tx, asOf)
	if err != nil {
		return RetentionInspection{}, err
	}
	if err := ctx.Err(); err != nil {
		return RetentionInspection{}, err
	}
	if err := tx.Rollback(); err != nil { // End the read snapshot without a write.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return RetentionInspection{}, err
	}
	closed = true
	return out, nil
}

func inspectRetentionSnapshot(ctx context.Context, tx *sql.Tx, asOf time.Time) (RetentionInspection, error) {
	tasks, err := monthlytasks.InspectReceiverTasks(ctx, tx)
	if err != nil {
		return RetentionInspection{}, err
	}
	out := RetentionInspection{Mode: "local_retention_inspection_only", Policy: retentionInspectionPolicy,
		AsOfMS: asOf.UnixMilli(), TaskCount: len(tasks), Entries: []RetentionItem{}}
	var maxPlan, maxInput, maxArtifact int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(length(CAST(plan AS BLOB))+length(CAST(preparation AS BLOB))+COALESCE(length(CAST(artifact AS BLOB)),0)),0),
COALESCE(MAX(length(CAST(plan AS BLOB))),0),COALESCE(MAX(length(CAST(preparation AS BLOB))),0),COALESCE(MAX(length(CAST(artifact AS BLOB))),0)
FROM portal_monthly_generation_inputs`).Scan(&out.InputCount, &out.RetainedPayloadBytes, &maxPlan, &maxInput, &maxArtifact); err != nil {
		return RetentionInspection{}, err
	}
	if out.InputCount > generationRecordLimit || out.RetainedPayloadBytes > generationReceiverLimit ||
		maxPlan > generationPlanLimit || maxInput > 8<<20 || maxArtifact > generationArtifactLimit {
		return RetentionInspection{}, ErrGenerationCapacity
	}
	refs := map[string][]monthlytasks.TaskObservation{}
	latest := map[string]monthlytasks.Task{}
	for _, observed := range tasks {
		t := observed.Task
		refs[t.Request.InputSHA256] = append(refs[t.Request.InputSHA256], observed)
		if t.State == monthlytasks.Queued || t.State == monthlytasks.Running {
			out.ActiveTasks++
		}
		if observed.Retryable {
			out.RetryableTasks++
		}
	}
	digest := sha256.New()
	encoder := json.NewEncoder(digest)
	if err := encoder.Encode(tasks); err != nil {
		return RetentionInspection{}, err
	}
	publications, err := inspectPublicationRetention(ctx, tx, tasks, encoder)
	if err != nil {
		return RetentionInspection{}, err
	}
	for _, r := range publications {
		if r.state == publicationRunning {
			out.ActiveTasks++
		} else if r.state == publicationRetry {
			out.RetryableTasks++
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,
CASE WHEN length(CAST(plan AS BLOB))<=2097152 THEN plan END,
CASE WHEN length(CAST(preparation AS BLOB))<=8388608 THEN preparation END,
CASE WHEN length(CAST(artifact AS BLOB))<=8388608 THEN artifact END,artifact_hash,
length(CAST(plan AS BLOB))+length(CAST(preparation AS BLOB))+COALESCE(length(CAST(artifact AS BLOB)),0)
FROM portal_monthly_generation_inputs ORDER BY id LIMIT ?`, generationRecordLimit+1)
	if err != nil {
		return RetentionInspection{}, err
	}
	defer rows.Close()
	seen := 0
	checks := make([]retainedInputCheck, 0, out.InputCount)
	for rows.Next() {
		var id string
		var size int64
		var saved generationRetained
		if err := rows.Scan(&id, &saved.planRaw, &saved.inputRaw, &saved.artifact, &saved.digest, &size); err != nil {
			return RetentionInspection{}, err
		}
		if !generationHex(id, 32) {
			return RetentionInspection{}, ErrGenerationIntegrity
		}
		if err := encoder.Encode([]any{id, generationHash(saved.planRaw), generationHash(saved.inputRaw),
			generationHash(saved.artifact), saved.artifact != nil, saved.digest, size}); err != nil {
			return RetentionInspection{}, err
		}
		request, checkErr := inspectRetainedInput(id, &saved, asOf)
		checks = append(checks, retainedInputCheck{id, saved.digest, size, request, checkErr})
		seen++
	}
	if err := rows.Err(); err != nil {
		return RetentionInspection{}, err
	}
	if seen != out.InputCount {
		return RetentionInspection{}, ErrGenerationIntegrity
	}
	if err := rows.Close(); err != nil {
		return RetentionInspection{}, err
	}
	// A corrupt or missing newer draft must NOT unpin the last intact one.
	// This is receiver-integrity checking, not independent financial attestation.
	for _, c := range checks {
		precheck := classifyRetainedInput(c.id, c.size, c.digest, c.request, c.err, refs[c.id], nil, asOf)
		if precheck.Disposition == "manual_review" {
			continue
		}
		for _, observed := range refs[c.id] {
			t := observed.Task
			if t.State != monthlytasks.DraftReady {
				continue
			}
			key := retentionMonthKey(t.Request)
			prior := latest[key]
			if prior.ID == "" || t.UpdatedAtMS > prior.UpdatedAtMS || (t.UpdatedAtMS == prior.UpdatedAtMS && t.ID > prior.ID) {
				latest[key] = t
			}
		}
	}
	for _, c := range checks {
		item := classifyRetainedInput(c.id, c.size, c.digest, c.request, c.err, refs[c.id], latest, asOf)
		protectPublicationInput(&item, publications, asOf)
		out.Entries = append(out.Entries, item)
		delete(refs, c.id)
	}
	for id, missing := range refs {
		item := classifyRetainedInput(id, 0, "", monthlytasks.Request{}, sql.ErrNoRows, missing, latest, asOf)
		item.Reason, item.Integrity = "missing_retained_input", "missing"
		reference, err := archiveReference(ctx, tx, id)
		if err != nil {
			return RetentionInspection{}, err
		}
		if reference != nil {
			// A retained reference explains missing hot payload, not proof that
			// the file is still present. Never perform filesystem I/O in inventory.
			item.Reason, item.Integrity = "archive_reference_requires_terminal_tasks", "archive_not_rechecked"
			terminal := true
			for _, t := range missing {
				terminal = terminal && (t.Task.State == monthlytasks.Invalidated || t.Task.State == monthlytasks.DraftReady || (t.Task.State == monthlytasks.Failed && t.Task.Attempts == 3))
				if _, pending := publications[t.Task.ID]; pending {
					terminal = false
				}
			}
			if terminal {
				item.Disposition, item.Reason = "archived_payload", "archive_receipt_retained_file_not_rechecked"
				item.GroupID, item.Month = missing[0].Task.Request.Owner.GroupID, missing[0].Task.Request.Month
				out.ArchivedInputs++
			}
		}
		protectPublicationInput(&item, publications, asOf)
		out.Entries = append(out.Entries, item)
	}
	// Maps above are unordered: bind archive references in stable ID order.
	missingIDs := make([]string, 0, len(refs))
	for id := range refs {
		missingIDs = append(missingIDs, id)
	}
	sort.Strings(missingIDs)
	for _, id := range missingIDs {
		ref, err := archiveReference(ctx, tx, id)
		if err != nil {
			return RetentionInspection{}, err
		}
		if ref != nil {
			if err := encoder.Encode([]any{id, generationHash(ref)}); err != nil {
				return RetentionInspection{}, err
			}
		}
	}
	sort.Slice(out.Entries, func(i, j int) bool { return out.Entries[i].InputSHA256 < out.Entries[j].InputSHA256 })
	for _, item := range out.Entries {
		switch item.Disposition {
		case "archive_copy_candidate":
			out.ArchiveCopyCandidates++
		case "manual_review":
			out.ReviewRequired++
		}
	}
	out.SnapshotSHA256 = hex.EncodeToString(digest.Sum(nil))
	if err := ctx.Err(); err != nil {
		return RetentionInspection{}, err
	}
	return out, nil
}

// This verifies retained structure/hashes only. It does NOT re-read independent
// evidence, re-run arithmetic, or convert a successful draft into a publication.
func inspectRetainedInput(id string, s *generationRetained, asOf time.Time) (monthlytasks.Request, error) {
	if generationHash(s.planRaw) != id || json.Unmarshal(s.planRaw, &s.plan) != nil {
		return monthlytasks.Request{}, ErrGenerationIntegrity
	}
	p, raw, _, err := canonicalGeneration(s.plan, s.inputRaw, asOf)
	if err != nil || !bytes.Equal(raw, s.planRaw) || (len(s.artifact) == 0) != (s.digest == "") ||
		(s.artifact != nil && len(s.artifact) == 0) {
		return monthlytasks.Request{}, ErrGenerationIntegrity
	}
	if len(s.artifact) > 0 {
		if generationHash(s.artifact) != s.digest {
			return monthlytasks.Request{}, ErrGenerationIntegrity
		}
		if _, err := decodeGenerationArtifact(*s); err != nil {
			return monthlytasks.Request{}, err
		}
	}
	return generationRequest(p, raw), nil
}

func retentionMonthKey(r monthlytasks.Request) string {
	// JSON keeps group/month identity unambiguous without human names.
	b, _ := json.Marshal([]any{r.Owner.GroupID, r.Month})
	return string(b)
}

func classifyRetainedInput(id string, size int64, artifactHash string, request monthlytasks.Request, checkErr error,
	refs []monthlytasks.TaskObservation, latest map[string]monthlytasks.Task, asOf time.Time) RetentionItem {
	item := RetentionItem{InputSHA256: id, TaskIDs: []string{}, PayloadBytes: size,
		KeepTaskMetadata: true, Integrity: "structure_and_digests_only", GroupID: request.Owner.GroupID, Month: request.Month}
	active, retryable, pinned := false, false, false
	for _, observed := range refs {
		t := observed.Task
		item.TaskIDs = append(item.TaskIDs, t.ID)
		if t.UpdatedAtMS > item.LastActivityMS {
			item.LastActivityMS = t.UpdatedAtMS
		}
		if t.State == monthlytasks.Invalidated {
			item.KeepInvalidation = true
		}
		if t.Request != request || (t.State == monthlytasks.DraftReady && (artifactHash == "" || artifactHash != t.ResultSHA256)) {
			checkErr = errors.Join(checkErr, ErrGenerationIntegrity)
		}
		active = active || t.State == monthlytasks.Queued || t.State == monthlytasks.Running
		retryable = retryable || observed.Retryable
		pinned = pinned || (t.State == monthlytasks.DraftReady && latest[retentionMonthKey(t.Request)].ID == t.ID)
	}
	sort.Strings(item.TaskIDs)
	switch {
	case checkErr != nil:
		item.Disposition, item.Reason, item.Integrity = "manual_review", "input_or_task_binding_invalid", "not_verified"
	case len(refs) == 0:
		item.Disposition, item.Reason = "manual_review", "orphan_without_reliable_age"
	case item.LastActivityMS > asOf.UnixMilli():
		item.Disposition, item.Reason = "manual_review", "activity_is_in_the_future"
	case active:
		item.Disposition, item.Reason = "keep_active", "queued_or_running_even_if_lease_expired"
	case retryable:
		item.Disposition, item.Reason = "keep_retryable", "attempt_budget_remaining"
	case pinned:
		item.Disposition, item.Reason = "keep_latest_ready", "latest_draft_for_organization_month"
	case asOf.UnixMilli()-item.LastActivityMS < retentionReviewAge.Milliseconds():
		item.Disposition, item.Reason = "keep_recent", "terminal_younger_than_30_days"
	default:
		item.Disposition, item.Reason = "archive_copy_candidate", "old_terminal_copy_only_keep_task_metadata"
	}
	return item
}
