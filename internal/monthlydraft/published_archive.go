package monthlydraft

// Verified maintenance primitives for a successfully published latest draft.
// Only an explicit maintenance owner may orchestrate them. These primitives do
// not schedule work, expose HTTP, activate tasks or delete original sources.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

const publishedArchiveMode = "saved_publication_payload_copy_not_bill_or_source_mutation"
const publishedArchivePolicy = "verified-saved-publication-v2"

type publishedArchiveBinding struct {
	TaskID                string `json:"task_id"`
	TaskKey               string `json:"task_key"`
	ResultSHA256          string `json:"result_sha256"`
	JournalSHA256         string `json:"journal_sha256"`
	JournalEvidenceSHA256 string `json:"journal_evidence_sha256"`
	PublicationSHA256     string `json:"publication_sha256"`
	PayloadSHA256         string `json:"payload_sha256"`
	EvidenceSHA256        string `json:"evidence_sha256"`
	MemberFingerprint     string `json:"member_fingerprint"`
	MemberManifest        string `json:"member_manifest"`
	PublishedAt           int64  `json:"published_at"`
}

func publishedBindingHash(b publishedArchiveBinding) string {
	raw, _ := json.Marshal(b)
	return generationHash(raw)
}

func publishedJournalHash(r publicationRecord) string {
	// publicationRecord has private fields; marshaling the struct would hash {}.
	raw, _ := json.Marshal([]any{r.id, r.key, r.result, r.state, r.attempts, r.updated, r.until, r.retry, r.evidence})
	return generationHash(raw)
}

func publishedRecordHash(r monitor.PortalMonthlyBill) string {
	raw, _ := json.Marshal(r)
	return generationHash(raw)
}

func validatePublishedArchiveBinding(b publishedArchiveBinding, tasks []monthlytasks.TaskObservation, req monthlytasks.Request, result string, asOf int64) error {
	if len(tasks) != 1 {
		return ErrGenerationIntegrity
	}
	o := tasks[0]
	t := o.Task
	if o.Retryable || !generationHex(o.StorageSHA256, 32) || t.Request != req || t.State != monthlytasks.DraftReady ||
		t.ID != b.TaskID || t.Key != b.TaskKey || t.ResultSHA256 != result || result != b.ResultSHA256 ||
		t.UpdatedAtMS > asOf || b.PublishedAt <= 0 || b.PublishedAt > asOf/1000 ||
		b.MemberFingerprint != req.Owner.MemberFingerprint || b.MemberManifest != req.Owner.MemberManifest {
		return ErrGenerationIntegrity
	}
	for _, h := range []string{b.TaskKey, b.ResultSHA256, b.JournalSHA256, b.JournalEvidenceSHA256, b.PublicationSHA256, b.PayloadSHA256, b.EvidenceSHA256} {
		if !generationHex(h, 32) {
			return ErrGenerationIntegrity
		}
	}
	return nil
}

func publishedArchiveItem(id string, size int64, task monthlytasks.TaskObservation) RetentionItem {
	return RetentionItem{InputSHA256: id, TaskIDs: []string{task.Task.ID}, GroupID: task.Task.Request.Owner.GroupID,
		Month: task.Task.Request.Month, Disposition: "published_copy_candidate", Reason: "verified_saved_publication_copy_only",
		Integrity: "structure_and_hashes_verified", LastActivityMS: task.Task.UpdatedAtMS, PayloadBytes: size, KeepTaskMetadata: true}
}

// readPublishedRecord is bounded BEFORE allocating payloads. It never obtains a
// writer, and refuses aliases of this feature's receiver or observation store.
func (g *Generation) readPublishedRecord(ctx context.Context, main *gorm.DB, req monthlytasks.Request) (monitor.PortalMonthlyBill, error) {
	var out monitor.PortalMonthlyBill
	if main == nil {
		return out, ErrGenerationInput
	}
	db, err := main.DB()
	if err != nil {
		return out, err
	}
	info, err := generationDatabaseFile(db)
	if err != nil {
		return out, err
	}
	for _, other := range []*sql.DB{g.receiver, g.funding} {
		otherInfo, err := generationDatabaseFile(other)
		if err != nil || os.SameFile(info, otherInfo) {
			return out, ErrGenerationInput
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = db.QueryRowContext(readCtx, `SELECT group_id,month,member_fingerprint,member_manifest,
CASE WHEN length(CAST(payload AS BLOB))<=2097152 THEN payload END,payload_hash,
CASE WHEN length(CAST(evidence AS BLOB))<=2097152 THEN evidence END,evidence_hash,published_at,publication_id
FROM portal_monthly_bills WHERE group_id=? AND month=?`, req.Owner.GroupID, req.Month).
		Scan(&out.GroupID, &out.Month, &out.MemberFingerprint, &out.MemberManifest, &out.Payload, &out.PayloadHash, &out.Evidence, &out.EvidenceHash, &out.PublishedAt, &out.PublicationID)
	if err != nil {
		return monitor.PortalMonthlyBill{}, err
	}
	if out.MemberFingerprint != req.Owner.MemberFingerprint || out.MemberManifest != req.Owner.MemberManifest ||
		!strings.HasPrefix(out.PublicationID, "cents-chain-v1:") || monitor.ValidateMonthlyBillPublicationRecord(out, time.Now()) != nil {
		return monitor.PortalMonthlyBill{}, ErrGenerationIntegrity
	}
	return out, nil
}

func inspectPublishedTask(ctx context.Context, tx *sql.Tx, req monthlytasks.Request, binding *publishedArchiveBinding) ([]monthlytasks.TaskObservation, error) {
	all, err := monthlytasks.InspectReceiverTasks(ctx, tx)
	if err != nil {
		return nil, err
	}
	var tasks []monthlytasks.TaskObservation
	for _, o := range all {
		if o.Task.Request.InputSHA256 == req.InputSHA256 {
			tasks = append(tasks, o)
		}
	}
	if len(tasks) != 1 || tasks[0].Task.Request != req || tasks[0].Task.State != monthlytasks.DraftReady {
		return nil, ErrGenerationIntegrity
	}
	t := tasks[0].Task
	r, err := loadPublication(ctx, tx, t.ID)
	if err != nil {
		return nil, err
	}
	if r.state != publicationSaved || r.key != t.Key || r.result != t.ResultSHA256 {
		return nil, ErrGenerationInput
	}
	b := publishedArchiveBinding{TaskID: t.ID, TaskKey: t.Key, ResultSHA256: t.ResultSHA256, JournalSHA256: publishedJournalHash(r), JournalEvidenceSHA256: r.evidence}
	if binding != nil && (binding.TaskID != b.TaskID || binding.TaskKey != b.TaskKey || binding.ResultSHA256 != b.ResultSHA256 || binding.JournalSHA256 != b.JournalSHA256 || binding.JournalEvidenceSHA256 != b.JournalEvidenceSHA256) {
		return nil, ErrGenerationChanged
	}
	return tasks, nil
}

// No filesystem work or source audit is performed while holding this snapshot.
func (g *Generation) publishedTaskSnapshot(ctx context.Context, req monthlytasks.Request) ([]monthlytasks.TaskObservation, publishedArchiveBinding, error) {
	tx, err := g.receiver.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, publishedArchiveBinding{}, err
	}
	defer func() { _ = tx.Rollback() }()
	tasks, err := inspectPublishedTask(ctx, tx, req, nil)
	if err != nil {
		return nil, publishedArchiveBinding{}, err
	}
	t := tasks[0].Task
	r, err := loadPublication(ctx, tx, t.ID)
	if err != nil {
		return nil, publishedArchiveBinding{}, err
	}
	return tasks, publishedArchiveBinding{TaskID: t.ID, TaskKey: t.Key, ResultSHA256: t.ResultSHA256, JournalSHA256: publishedJournalHash(r), JournalEvidenceSHA256: r.evidence}, nil
}

func (g *Generation) auditPublishedArchive(ctx context.Context, main *gorm.DB, owner monthlytasks.Owner, taskID string) (generationRetained, archiveEntry, publishedArchiveBinding, error) {
	var empty generationRetained
	var entry archiveEntry
	var binding publishedArchiveBinding
	if g == nil || g.receiver == nil || g.tasks == nil || g.readOnly {
		return empty, entry, binding, ErrGenerationInput
	}
	// Inspection must not invalidate/claim a task if authorization or state
	// changes while Run re-audits a ready artifact. Copy the handle, not its DB.
	inspection := *g
	inspection.readOnly = true
	g = &inspection
	task, err := g.tasks.Get(ctx, owner, taskID)
	if err != nil || task.State != monthlytasks.DraftReady {
		return empty, entry, binding, errors.Join(ErrGenerationInput, err)
	}
	tasks, binding, err := g.publishedTaskSnapshot(ctx, task.Request)
	if err != nil {
		return empty, entry, binding, err
	}
	record, err := g.readPublishedRecord(ctx, main, task.Request)
	if err != nil {
		return empty, entry, binding, err
	}
	journalEvidence, err := monitor.MonthlyBillSavedGenerationBinding(record, task.Request.SourceIdentity, time.Now())
	if err != nil || journalEvidence != binding.JournalEvidenceSHA256 {
		return empty, entry, binding, errors.Join(ErrGenerationIntegrity, err)
	}
	// Validate the artifact too, not just the arithmetic in the published bill.
	if _, err := g.Run(ctx, owner, taskID); err != nil {
		return empty, entry, binding, err
	}
	var evidence monitor.MonthlyBillEvidence
	if err := json.Unmarshal([]byte(record.Evidence), &evidence); err != nil {
		return empty, entry, binding, err
	}
	proof, err := g.auditAutomaticDisplaySource(ctx, task.Request, evidence.ReviewedAt)
	if err != nil || !sameAutomaticDisplayEvidence(proof.Evidence, []byte(record.Evidence)) {
		return empty, entry, binding, errors.Join(ErrGenerationEvidence, err)
	}
	saved, err := g.loadRetainedGeneration(ctx, task.Request, time.Now())
	if err != nil {
		return empty, entry, binding, err
	}
	binding.PublicationSHA256, binding.PayloadSHA256, binding.EvidenceSHA256 = publishedRecordHash(record), record.PayloadHash, record.EvidenceHash
	binding.MemberFingerprint, binding.MemberManifest, binding.PublishedAt = record.MemberFingerprint, record.MemberManifest, record.PublishedAt
	entry = archiveEntry{Item: publishedArchiveItem(task.Request.InputSHA256, int64(len(saved.planRaw)+len(saved.inputRaw)+len(saved.artifact)), tasks[0]),
		Tasks: tasks, PreparationSHA256: generationHash(saved.inputRaw), ArtifactSHA256: saved.digest}
	if err := validatePublishedArchiveBinding(binding, tasks, task.Request, saved.digest, time.Now().UnixMilli()); err != nil {
		return empty, archiveEntry{}, publishedArchiveBinding{}, err
	}
	return saved, entry, binding, nil
}

// CopyPublishedGenerationArchive writes ONE verified completed task into an
// explicitly supplied EMPTY private directory. Originals remain untouched. A
// complete copy is replayable; partial output is never overwritten/resumed.
// It does not initialize retention or grant permission to delete hot payloads.
func (g *Generation) CopyPublishedGenerationArchive(parent context.Context, main *gorm.DB, owner monthlytasks.Owner, taskID, directory string) (ArchiveReceipt, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	saved, entry, binding, err := g.auditPublishedArchive(ctx, main, owner, taskID)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	select {
	case archiveSlots <- struct{}{}:
		defer func() { <-archiveSlots }()
	default:
		return ArchiveReceipt{}, ErrGenerationBusy
	}
	root, lock, err := openCapturedOutput(directory)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	defer root.Close()
	defer lock.Close()
	listing, err := root.Open(".")
	if err != nil {
		return ArchiveReceipt{}, err
	}
	names, readErr := listing.Readdirnames(6)
	closeErr := listing.Close()
	if (readErr != nil && !errors.Is(readErr, io.EOF)) || closeErr != nil {
		return ArchiveReceipt{}, errors.Join(readErr, closeErr)
	}
	if len(names) != 0 {
		if len(names) != 5 { // three payloads plus the linked pending/final manifest
			return ArchiveReceipt{}, ErrGenerationIntegrity
		}
		raw, err := readArchiveFile(ctx, root, "manifest.json", archiveManifestLimit)
		if err != nil {
			return ArchiveReceipt{}, err
		}
		var m archiveManifest
		if json.Unmarshal(raw, &m) != nil || m.Published == nil || *m.Published != binding || !reflect.DeepEqual(m.Entries, []archiveEntry{entry}) {
			return ArchiveReceipt{}, ErrGenerationChanged
		}
		receipt, err := verifyArchive(ctx, root, "manifest.json", generationHash(raw))
		if err != nil {
			return ArchiveReceipt{}, err
		}
		pending, err := readArchiveFile(ctx, root, "manifest.pending", archiveManifestLimit)
		if err != nil || !reflect.DeepEqual(raw, pending) {
			return ArchiveReceipt{}, errors.Join(ErrGenerationIntegrity, err)
		}
		receipt.Directory = directory
		return receipt, nil
	}
	m := archiveManifest{Version: 2, Mode: publishedArchiveMode, Policy: publishedArchivePolicy, SnapshotSHA256: publishedBindingHash(binding), AsOfMS: time.Now().UnixMilli(), Entries: []archiveEntry{entry}, Published: &binding}
	id := entry.Item.InputSHA256
	for _, p := range []struct {
		suffix string
		raw    []byte
	}{{"plan", saved.planRaw}, {"preparation", saved.inputRaw}, {"artifact", saved.artifact}} {
		if err := writeArchiveFile(ctx, root, id+"."+p.suffix, p.raw); err != nil {
			return ArchiveReceipt{}, err
		}
	}
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > archiveManifestLimit {
		return ArchiveReceipt{}, ErrGenerationCapacity
	}
	if err := writeArchiveFile(ctx, root, "manifest.pending", raw); err != nil {
		return ArchiveReceipt{}, err
	}
	receipt, err := verifyArchive(ctx, root, "manifest.pending", generationHash(raw))
	if err != nil {
		return ArchiveReceipt{}, err
	}
	if err := root.Link("manifest.pending", "manifest.json"); err != nil {
		return ArchiveReceipt{}, err
	}
	if err := syncArchiveDirectory(root); err != nil {
		return ArchiveReceipt{}, err
	}
	receipt.Directory = directory
	return receipt, nil
}
