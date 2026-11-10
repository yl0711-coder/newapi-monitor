package monthlydraft

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

const archiveMode = "local_receiver_payload_copy_not_publication"
const archiveManifestLimit = 1 << 20

var archiveSlots = make(chan struct{}, 1)

// ArchiveReceipt must be retained separately from the copy. Its digest detects
// changed manifests; neither the receipt nor a copy authorizes deletion/restore.
// Copies contain PRIVATE financial inputs. They are not customer downloads.
type ArchiveReceipt struct {
	Directory      string `json:"directory"`
	ManifestSHA256 string `json:"manifest_sha256"`
	InputCount     int    `json:"input_count"`
	PayloadBytes   int64  `json:"payload_bytes"`
}

type archiveManifest struct {
	Version        int                      `json:"version"`
	Mode           string                   `json:"mode"`
	Policy         string                   `json:"policy"`
	SnapshotSHA256 string                   `json:"snapshot_sha256"`
	AsOfMS         int64                    `json:"as_of_ms"`
	Entries        []archiveEntry           `json:"entries"`
	Published      *publishedArchiveBinding `json:"published,omitempty"`
}

type archiveEntry struct {
	Item              RetentionItem                  `json:"item"`
	Tasks             []monthlytasks.TaskObservation `json:"tasks"`
	PreparationSHA256 string                         `json:"preparation_sha256"`
	ArtifactSHA256    string                         `json:"artifact_sha256"`
}

// CopyGenerationArchive re-inspects the receiver in a single read snapshot.
// expectedSnapshot comes from a prior inspection; stale plans fail closed. It
// copies only old terminal candidates, never source evidence or publications.
// No SQL writes, eviction, activation, or production hooks are performed.
// Failed copies remain private and incomplete; retries create fresh directories
// rather than overwrite/resume files of uncertain provenance.
// archiveOperationTimeout bounds one archive copy or verification. A variable
// only so race-detector tests of the maximum record bound can widen it.
var archiveOperationTimeout = 30 * time.Second

func CopyGenerationArchive(parent context.Context, db *sql.DB, directory, expectedSnapshot string, asOf time.Time) (ArchiveReceipt, error) {
	return copyGenerationArchive(parent, db, directory, expectedSnapshot, asOf, writeArchiveFile)
}

// The private writer seam permits deterministic disk-failure/interruption
// tests without exposing a configurable writer through the public API.
func copyGenerationArchive(parent context.Context, db *sql.DB, directory, expectedSnapshot string, asOf time.Time,
	writeFile func(context.Context, *os.Root, string, []byte) error) (ArchiveReceipt, error) {
	if db == nil || !generationHex(expectedSnapshot, 32) || asOf.UnixMilli() <= 0 {
		return ArchiveReceipt{}, ErrGenerationInput
	}
	select {
	case archiveSlots <- struct{}{}:
		defer func() { <-archiveSlots }()
	default:
		return ArchiveReceipt{}, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, archiveOperationTimeout)
	defer cancel()
	root, err := openArchiveRoot(directory)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	defer root.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return ArchiveReceipt{}, err
	}
	closed := false
	defer func() {
		if !closed {
			if err := tx.Rollback(); err != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
	}()
	report, err := inspectRetentionSnapshot(ctx, tx, asOf)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	if report.SnapshotSHA256 != expectedSnapshot {
		return ArchiveReceipt{}, ErrGenerationChanged
	}
	if report.ArchiveCopyCandidates == 0 {
		return ArchiveReceipt{}, ErrGenerationInput
	}
	tasks, err := monthlytasks.InspectReceiverTasks(ctx, tx)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	name := "copy-" + rand.Text()
	if err := root.Mkdir(name, 0700); err != nil {
		return ArchiveReceipt{}, err
	}
	copyRoot, err := root.OpenRoot(name)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	defer copyRoot.Close()
	m := archiveManifest{Version: 1, Mode: archiveMode, Policy: retentionInspectionPolicy,
		SnapshotSHA256: report.SnapshotSHA256, AsOfMS: report.AsOfMS, Entries: []archiveEntry{}}
	for _, item := range report.Entries {
		if item.Disposition != "archive_copy_candidate" {
			continue
		}
		var saved generationRetained
		if err := tx.QueryRowContext(ctx, `SELECT plan,preparation,artifact,artifact_hash FROM portal_monthly_generation_inputs WHERE id=?`, item.InputSHA256).
			Scan(&saved.planRaw, &saved.inputRaw, &saved.artifact, &saved.digest); err != nil {
			return ArchiveReceipt{}, err
		}
		entry := archiveEntry{Item: item, PreparationSHA256: generationHash(saved.inputRaw), ArtifactSHA256: saved.digest}
		for _, t := range tasks {
			if t.Task.Request.InputSHA256 == item.InputSHA256 {
				entry.Tasks = append(entry.Tasks, t)
			}
		}
		for _, part := range []struct {
			suffix string
			raw    []byte
		}{{"plan", saved.planRaw}, {"preparation", saved.inputRaw}, {"artifact", saved.artifact}} {
			if part.raw == nil {
				continue
			}
			if err := writeFile(ctx, copyRoot, item.InputSHA256+"."+part.suffix, part.raw); err != nil {
				return ArchiveReceipt{}, err
			}
		}
		m.Entries = append(m.Entries, entry)
	}
	if err := tx.Rollback(); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return ArchiveReceipt{}, err
	}
	closed = true // Release the SQLite snapshot before file verification.
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > archiveManifestLimit {
		return ArchiveReceipt{}, ErrGenerationCapacity
	}
	if err := writeFile(ctx, copyRoot, "manifest.pending", raw); err != nil {
		return ArchiveReceipt{}, err
	}
	receipt, err := verifyArchive(ctx, copyRoot, "manifest.pending", generationHash(raw))
	if err != nil {
		return ArchiveReceipt{}, err
	}
	// Link refuses an existing target. The completion marker appears only after
	// every copied byte was read back. A pending file alone is never accepted.
	if err := copyRoot.Link("manifest.pending", "manifest.json"); err != nil {
		return ArchiveReceipt{}, err
	}
	if err := syncArchiveDirectory(copyRoot); err != nil {
		return ArchiveReceipt{}, err
	}
	if err := syncArchiveDirectory(root); err != nil {
		return ArchiveReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArchiveReceipt{}, err
	}
	receipt.Directory = filepath.Join(directory, name)
	return receipt, nil
}

// VerifyGenerationArchive is an offline read-back drill, NOT database recovery.
// It checks exact payloads, task bindings and invalidation metadata. Independent
// source files, authorization and financial truth must still be revalidated
// before any future recovery or publication. No receiver is opened or written.
func VerifyGenerationArchive(parent context.Context, receipt ArchiveReceipt) error {
	select {
	case archiveSlots <- struct{}{}:
		defer func() { <-archiveSlots }()
	default:
		return ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, archiveOperationTimeout)
	defer cancel()
	root, err := openArchiveRoot(receipt.Directory)
	if err != nil {
		return err
	}
	defer root.Close()
	got, err := verifyArchive(ctx, root, "manifest.json", receipt.ManifestSHA256)
	if err != nil {
		return err
	}
	if got.InputCount != receipt.InputCount || got.PayloadBytes != receipt.PayloadBytes {
		return ErrGenerationIntegrity
	}
	return nil
}

func verifyArchive(ctx context.Context, root *os.Root, manifestName, digest string) (ArchiveReceipt, error) {
	if !generationHex(digest, 32) {
		return ArchiveReceipt{}, ErrGenerationInput
	}
	raw, err := readArchiveFile(ctx, root, manifestName, archiveManifestLimit)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	var m archiveManifest
	if generationHash(raw) != digest || json.Unmarshal(raw, &m) != nil {
		return ArchiveReceipt{}, ErrGenerationIntegrity
	}
	canonical, err := json.Marshal(m)
	legacy := m.Version == 1 && m.Mode == archiveMode && m.Policy == retentionInspectionPolicy && m.Published == nil
	published := m.Version == 2 && m.Mode == publishedArchiveMode && m.Policy == publishedArchivePolicy && m.Published != nil && len(m.Entries) == 1
	if err != nil || !bytes.Equal(canonical, raw) || (!legacy && !published) ||
		!generationHex(m.SnapshotSHA256, 32) || m.AsOfMS <= 0 || len(m.Entries) == 0 || len(m.Entries) > generationRecordLimit {
		return ArchiveReceipt{}, ErrGenerationIntegrity
	}
	out := ArchiveReceipt{ManifestSHA256: digest, InputCount: len(m.Entries)}
	previous := ""
	for _, entry := range m.Entries {
		id := entry.Item.InputSHA256
		if !generationHex(id, 32) || id <= previous || len(entry.Tasks) == 0 || len(entry.Tasks) > 256 ||
			!generationHex(entry.PreparationSHA256, 32) || (entry.ArtifactSHA256 != "" && !generationHex(entry.ArtifactSHA256, 32)) {
			return ArchiveReceipt{}, ErrGenerationIntegrity
		}
		previous = id
		s := generationRetained{digest: entry.ArtifactSHA256}
		for _, part := range []struct {
			suffix string
			limit  int64
			out    *[]byte
		}{{"plan", generationPlanLimit, &s.planRaw}, {"preparation", 8 << 20, &s.inputRaw}, {"artifact", generationArtifactLimit, &s.artifact}} {
			if part.suffix == "artifact" && s.digest == "" {
				if _, err := root.Lstat(id + ".artifact"); !os.IsNotExist(err) {
					return ArchiveReceipt{}, ErrGenerationIntegrity
				}
				continue
			}
			*part.out, err = readArchiveFile(ctx, root, id+"."+part.suffix, part.limit)
			if err != nil {
				return ArchiveReceipt{}, err
			}
		}
		size := int64(len(s.planRaw) + len(s.inputRaw) + len(s.artifact))
		out.PayloadBytes += size
		if out.PayloadBytes > generationReceiverLimit || generationHash(s.inputRaw) != entry.PreparationSHA256 {
			return ArchiveReceipt{}, ErrGenerationIntegrity
		}
		request, checkErr := inspectRetainedInput(id, &s, time.UnixMilli(m.AsOfMS))
		for _, t := range entry.Tasks {
			if !generationHex(t.StorageSHA256, 32) || !generationHex(t.Task.ID, 16) ||
				(t.Task.State != monthlytasks.Invalidated && t.Task.State != monthlytasks.DraftReady && t.Task.State != monthlytasks.Failed) ||
				(t.Task.State == monthlytasks.Failed && t.Task.Attempts != 3) || t.Retryable {
				return ArchiveReceipt{}, ErrGenerationIntegrity
			}
		}
		item := classifyRetainedInput(id, size, s.digest, request, checkErr, entry.Tasks, nil, time.UnixMilli(m.AsOfMS))
		if published {
			if checkErr != nil || validatePublishedArchiveBinding(*m.Published, entry.Tasks, request, s.digest, m.AsOfMS) != nil || m.SnapshotSHA256 != publishedBindingHash(*m.Published) {
				return ArchiveReceipt{}, ErrGenerationIntegrity
			}
			item = publishedArchiveItem(id, size, entry.Tasks[0])
		}
		if (legacy && item.Disposition != "archive_copy_candidate") || !reflect.DeepEqual(item, entry.Item) {
			return ArchiveReceipt{}, ErrGenerationIntegrity
		}
	}
	return out, ctx.Err()
}

func openArchiveRoot(path string) (*os.Root, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, ErrGenerationInput
	}
	st, err := os.Lstat(path)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return nil, ErrGenerationInput
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(st, opened) {
		_ = root.Close()
		return nil, ErrGenerationInput
	}
	return root, nil
}

func writeArchiveFile(ctx context.Context, root *os.Root, name string, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, bytes.NewReader(raw)); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return ctx.Err()
}

func readArchiveFile(ctx context.Context, root *os.Root, name string, limit int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// os.Root may resolve links while enforcing its directory boundary; do not
	// rely on O_NOFOLLOW alone to reject a final symlink on every platform.
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0077 != 0 || before.Size() > limit {
		return nil, ErrGenerationIntegrity
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !os.SameFile(before, st) || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > limit {
		return nil, ErrGenerationIntegrity
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) != st.Size() {
		return nil, ErrGenerationIntegrity
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	after, err := root.Lstat(name)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(st, after) || after.Mode().Perm()&0077 != 0 || after.Size() != st.Size() {
		return nil, ErrGenerationIntegrity
	}
	return raw, nil
}

func syncArchiveDirectory(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
