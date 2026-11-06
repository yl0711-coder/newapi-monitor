package monthlydraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type CapturedSourceFiles struct {
	Mode      string                               `json:"mode"`
	Material  monthlysource.MonthlyObservationFile `json:"material"`
	Selection monthlysource.MonthlyObservationFile `json:"selection"`
}

const capturedSourceFilesMode = "sealed_captured_sources_requires_history_and_bill_validation"

// SealCapturedSources is a background-only LOCAL adapter. directory is an
// existing private output directory supplied by server composition, NEVER by
// the customer. A complete matching directory is revalidated/reused; an
// incomplete directory is retained and rejected, not overwritten or repaired.
// No input selection, historical balance or customer bill is created here.
func (p *WalletSourcePreparation) SealCapturedSources(ctx context.Context, req SourcePreparationRequest, directory string) (CapturedSourceFiles, error) {
	return p.sealCapturedSources(ctx, req, directory, writeArchiveFile)
}

func (p *WalletSourcePreparation) sealCapturedSources(parent context.Context, req SourcePreparationRequest, directory string,
	writeFile func(context.Context, *os.Root, string, []byte) error) (CapturedSourceFiles, error) {
	return p.sealCapturedSourcesBounded(parent, req, directory, generationEvidenceLimit, writeFile)
}

func (p *WalletSourcePreparation) sealCapturedSourcesBounded(parent context.Context, req SourcePreparationRequest, directory string, available int64,
	writeFile func(context.Context, *os.Root, string, []byte) error) (CapturedSourceFiles, error) {
	var empty CapturedSourceFiles
	if parent == nil || p == nil || p.funding == nil || writeFile == nil {
		return empty, ErrGenerationInput
	}
	if available <= 0 || available > generationEvidenceLimit {
		return empty, ErrGenerationCapacity
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	assembled, err := p.AssembleCapturedSources(ctx, req)
	if err != nil {
		return empty, err
	}
	var selected monthlysource.CapturedMonthSelection
	if json.Unmarshal(assembled.Selection, &selected) != nil {
		return empty, ErrGenerationIntegrity
	}
	// Do not emit a handoff the existing generation reader would reject. Keep
	// its 4096 combined-window limit rather than silently widening a budget.
	windows := 0
	for _, a := range selected.Tokens {
		windows += len(a.Usage)
	}
	if windows == 0 || !p.funding.partitionUsage && windows > 4096 {
		return empty, ErrGenerationCapacity
	}
	root, lock, err := openCapturedOutput(directory)
	if err != nil {
		return empty, err
	}
	defer root.Close()
	defer lock.Close()
	if _, err := root.Lstat("materials.json"); err == nil {
		out, err := verifyCapturedSourceFiles(ctx, root, directory, assembled, p.funding.partitionUsage, p.funding.registeredOrigins, p.funding.walletTails)
		if err != nil {
			return empty, err
		}
		if _, err := p.funding.scope(ctx, req); err != nil {
			return empty, err
		}
		return out, nil
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	if err := automaticPreparationOutputEmpty(root); err != nil {
		return empty, err
	}
	databaseLimit, err := capturedDatabaseBudget(assembled, available)
	if err != nil {
		return empty, err
	}
	if err := writeFile(ctx, root, "selection.json", assembled.Selection); err != nil {
		return empty, err
	}
	if err := automaticPreparationRootCurrent(root, directory); err != nil {
		return empty, err
	}
	file, err := root.OpenFile("observations.sqlite", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return empty, err
	}
	reserved, statErr := file.Stat()
	if err := errors.Join(statErr, file.Close()); err != nil {
		return empty, err
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(directory, "observations.sqlite")}
	u.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(500)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return empty, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var copy monthlysource.CapturedMonthAssembly
	var copyErr error
	if p.funding.partitionUsage {
		copy, copyErr = monthlysource.CopyPartitionedCapturedMonthObservationsBounded(ctx, p.funding.capture, db, req.Key(), selected.Scope, databaseLimit, p.funding.tokenRecovery)
	} else if p.funding.tokenRecovery {
		copy, copyErr = monthlysource.CopyRecoveredCapturedMonthObservationsBounded(ctx, p.funding.capture, db, req.Key(), selected.Scope, databaseLimit)
	} else {
		copy, copyErr = monthlysource.CopyCapturedMonthObservationsBounded(ctx, p.funding.capture, db, req.Key(), selected.Scope, databaseLimit)
	}
	if copyErr == nil && p.funding.registeredOrigins {
		copyErr = monthlysource.CopySelectedRegisteredWallet(ctx, p.funding.capture, db, assembled.Selection)
	}
	if copyErr == nil && p.funding.walletTails {
		copyErr = monthlysource.CopySelectedWalletTails(ctx, p.funding.capture, db, assembled.Selection)
	}
	if errors.Is(copyErr, monthlysource.ErrTokenMonthCaptureBudget) {
		copyErr = errors.Join(ErrGenerationCapacity, copyErr)
	}
	if err := errors.Join(copyErr, db.Close()); err != nil {
		return empty, err
	}
	if !bytes.Equal(copy.Selection, assembled.Selection) || len(copy.Tokens) != len(assembled.Tokens) {
		return empty, ErrGenerationChanged
	}
	current, err := root.Lstat("observations.sqlite")
	if err != nil || !os.SameFile(reserved, current) {
		return empty, capturedFilesError(ctx, err)
	}
	for i, token := range assembled.Tokens {
		if token.UserID != copy.Tokens[i].UserID || !bytes.Equal(token.Raw, copy.Tokens[i].Raw) {
			return empty, ErrGenerationChanged
		}
		if err := writeFile(ctx, root, capturedTokenName(token.UserID), token.Raw); err != nil {
			return empty, err
		}
	}
	material, err := capturedMaterial(ctx, root, directory, assembled, p.funding.partitionUsage, p.funding.registeredOrigins, p.funding.walletTails)
	if err != nil {
		return empty, err
	}
	var total int64
	entries, err := root.Open(".")
	if err != nil {
		return empty, err
	}
	files, readErr := entries.ReadDir(205)
	if err := errors.Join(readErr, entries.Close()); err != nil {
		return empty, err
	}
	for _, file := range files {
		info, err := root.Lstat(file.Name())
		if err != nil {
			return empty, err
		}
		total += info.Size()
	}
	materialBytes, err := json.Marshal(material)
	if err != nil {
		return empty, err
	}
	if total+int64(len(materialBytes)) > available {
		return empty, ErrGenerationCapacity
	}
	// No source network access: confirm original captures and CURRENT authority
	// again after disk I/O, before any completion marker is written.
	again, err := p.AssembleCapturedSources(ctx, req)
	if err != nil {
		return empty, err
	}
	if !bytes.Equal(again.Selection, assembled.Selection) {
		return empty, ErrGenerationChanged
	}
	if err := automaticPreparationRootCurrent(root, directory); err != nil {
		return empty, err
	}
	if err := syncArchiveDirectory(root); err != nil {
		return empty, err
	}
	raw, err := json.Marshal(material)
	if err != nil {
		return empty, err
	}
	// Exclusive write, last. Even an interrupted marker is not usable: every
	// consumer checks canonical bytes/digest and the fully audited source set.
	if err := writeFile(ctx, root, "materials.json", raw); err != nil {
		return empty, err
	}
	if err := syncArchiveDirectory(root); err != nil {
		return empty, err
	}
	out, err := verifyCapturedSourceFiles(ctx, root, directory, assembled, p.funding.partitionUsage, p.funding.registeredOrigins, p.funding.walletTails)
	if err != nil {
		return empty, err
	}
	if _, err := p.funding.scope(ctx, req); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return out, nil
}

// Reserve JSON handoff space and bound both final bytes and SQLite's transient
// DELETE rollback journal while the (yet unaccompanied) database is written.
// Existing bytes and failed attempts are accounted by the spool before calling.
func capturedDatabaseBudget(a monthlysource.CapturedMonthAssembly, available int64) (int64, error) {
	var tokens int64
	for _, t := range a.Tokens {
		tokens += int64(len(t.Raw))
	}
	// Up to 2 MiB material plus bounded per-file allocation/directory overhead.
	metadata := int64(len(a.Selection)) + (4 << 20)
	budget := min(int64(64<<20), available-tokens-metadata, (available-metadata)/2)
	budget = budget / 4096 * 4096
	if available <= 0 || available > generationEvidenceLimit || budget < 128<<10 {
		return 0, ErrGenerationCapacity
	}
	return budget, nil
}

func capturedTokenName(id int64) string { return "tokens-" + strconv.FormatInt(id, 10) + ".json" }

func openCapturedOutput(directory string) (*os.Root, *os.File, error) {
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil || resolved != directory {
		return nil, nil, ErrGenerationInput
	}
	root, err := openArchiveRoot(directory)
	if err != nil {
		return nil, nil, err
	}
	lock, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	st, err := lock.Stat()
	var stat *syscall.Stat_t
	if err == nil {
		stat, _ = st.Sys().(*syscall.Stat_t)
	}
	if err != nil || stat == nil || stat.Uid != uint32(os.Geteuid()) {
		_ = lock.Close()
		_ = root.Close()
		return nil, nil, ErrGenerationInput
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, nil, ErrGenerationBusy
	}
	return root, lock, nil
}

// Hash a closed source without loading a 64 MiB SQLite into memory. Reject
// aliases, sidecars, modified identities, public permissions and excess bytes.
func capturedFileDigest(ctx context.Context, root *os.Root, name string, limit int64) (string, int64, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return "", 0, err
	}
	if err := capturedFilePrivate(before, limit); err != nil {
		return "", 0, err
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return "", 0, ErrGenerationIntegrity
	}
	h := sha256.New()
	buf := make([]byte, 64<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		n, readErr := f.Read(buf)
		total += int64(n)
		if total > limit {
			return "", 0, ErrGenerationCapacity
		}
		_, _ = h.Write(buf[:n])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", 0, readErr
		}
	}
	after, err := root.Lstat(name)
	if err != nil || !os.SameFile(opened, after) || total != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return "", 0, capturedFilesError(ctx, err)
	}
	if err := capturedFilePrivate(after, limit); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), total, ctx.Err()
}

func capturedFilePrivate(info os.FileInfo, limit int64) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() <= 0 {
		return ErrGenerationIntegrity
	}
	if info.Size() > limit {
		return ErrGenerationCapacity
	}
	return nil
}

func capturedFilesError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return ErrGenerationIntegrity
}

func capturedMaterial(ctx context.Context, root *os.Root, directory string, assembled monthlysource.CapturedMonthAssembly, partitioned, registered, tails bool) (monthlysource.MonthlyObservationMaterial, error) {
	var empty monthlysource.MonthlyObservationMaterial
	selection, err := readArchiveFile(ctx, root, "selection.json", 2<<20)
	if err != nil || !bytes.Equal(selection, assembled.Selection) {
		return empty, capturedFilesError(ctx, err)
	}
	selectionHash, _, err := capturedFileDigest(ctx, root, "selection.json", 2<<20)
	if err != nil || selectionHash != generationHash(selection) {
		return empty, capturedFilesError(ctx, err)
	}
	var s monthlysource.CapturedMonthSelection
	if json.Unmarshal(selection, &s) != nil {
		return empty, ErrGenerationIntegrity
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := root.Lstat("observations.sqlite" + suffix); !os.IsNotExist(err) {
			return empty, ErrGenerationIntegrity
		}
	}
	sha, size, err := capturedFileDigest(ctx, root, "observations.sqlite", 64<<20)
	if err != nil {
		return empty, err
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(directory, "observations.sqlite")}
	u.RawQuery = url.Values{"mode": {"ro"}, "immutable": {"1"}, "_pragma": {"query_only(1)", "busy_timeout(500)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return empty, err
	}
	db.SetMaxOpenConns(1)
	var auditErr error
	if partitioned {
		parts, err := monthlysource.PartitionCapturedUsageIndex(ctx, selection, time.Now())
		if err != nil {
			_ = db.Close()
			return empty, err
		}
		auditErr = monthlysource.VerifyPartitionedCapturedObservationSelection(ctx, db, generationHash(selection), s.Scope, parts)
	} else {
		auditErr = monthlysource.VerifyCapturedObservationSelection(ctx, db, selection)
	}
	if auditErr == nil && registered {
		_, _, auditErr = monthlysource.ReadSelectedRegisteredWallet(ctx, db, selection)
	}
	if auditErr == nil && tails {
		_, auditErr = monthlysource.ReadSelectedWalletTails(ctx, db, selection)
	}
	if err := errors.Join(auditErr, db.Close()); err != nil {
		return empty, err
	}
	after, _, err := capturedFileDigest(ctx, root, "observations.sqlite", 64<<20)
	if err != nil || after != sha {
		return empty, capturedFilesError(ctx, err)
	}
	m := monthlysource.MonthlyObservationMaterial{Version: 1, Mode: monthlysource.MonthlyObservationMaterialMode, GroupID: s.Scope.GroupID, FundingVersion: 2, Funding: s.Scope.Funding, FundingWindows: s.Funding, WalletIDs: s.WalletIDs, Database: monthlysource.MonthlyObservationFile{Path: u.Path, SHA256: sha}, UsageWindows: []monthlysource.UsageWindowVersion{}, Tokens: []monthlysource.MonthlyObservationTokenFile{}}
	if partitioned {
		m.Version, m.Mode = 2, monthlysource.PartitionedObservationMaterialMode
		m.UsageSelection = &monthlysource.MonthlyObservationFile{Path: filepath.Join(directory, "selection.json"), SHA256: selectionHash}
	}
	for i, token := range assembled.Tokens {
		if i >= len(s.Tokens) || token.UserID != s.Tokens[i].UserID {
			return empty, ErrGenerationIntegrity
		}
		name := capturedTokenName(token.UserID)
		raw, err := readArchiveFile(ctx, root, name, monthlysource.TokenUsageArchiveLimit)
		if err != nil || !bytes.Equal(raw, token.Raw) {
			return empty, capturedFilesError(ctx, err)
		}
		digest, n, err := capturedFileDigest(ctx, root, name, monthlysource.TokenUsageArchiveLimit)
		if err != nil || digest != generationHash(raw) {
			return empty, capturedFilesError(ctx, err)
		}
		size += n
		if size+int64(len(selection)) > generationEvidenceLimit {
			return empty, ErrGenerationCapacity
		}
		archive, _, err := monthlysource.DecodeTokenUsageMonthArchive(ctx, raw, time.Now())
		if err != nil || archive.UserID != token.UserID || archive.SelectionSHA256 != generationHash(selection) {
			return empty, capturedFilesError(ctx, err)
		}
		m.Tokens = append(m.Tokens, monthlysource.MonthlyObservationTokenFile{UserID: token.UserID, File: monthlysource.MonthlyObservationFile{Path: filepath.Join(directory, name), SHA256: digest}})
		if !partitioned {
			m.UsageWindows = append(m.UsageWindows, s.Tokens[i].Usage...)
		}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return empty, err
	}
	if size+int64(len(selection))+int64(len(raw)) > generationEvidenceLimit {
		return empty, ErrGenerationCapacity
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return m, nil
}

func verifyCapturedSourceFiles(ctx context.Context, root *os.Root, directory string, assembled monthlysource.CapturedMonthAssembly, partitioned, registered, tails bool) (CapturedSourceFiles, error) {
	var empty CapturedSourceFiles
	m, err := capturedMaterial(ctx, root, directory, assembled, partitioned, registered, tails)
	if err != nil {
		return empty, err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return empty, err
	}
	stored, err := readArchiveFile(ctx, root, "materials.json", 2<<20)
	if err != nil || !bytes.Equal(raw, stored) {
		return empty, capturedFilesError(ctx, err)
	}
	sha, _, err := capturedFileDigest(ctx, root, "materials.json", 2<<20)
	if err != nil || sha != generationHash(raw) {
		return empty, capturedFilesError(ctx, err)
	}
	f, err := root.Open(".")
	if err != nil {
		return empty, err
	}
	entries, readErr := f.ReadDir(205 + capturedHistoryScopeVersions)
	if err := errors.Join(readErr, f.Close()); err != nil {
		return empty, err
	}
	allowed := map[string]bool{"selection.json": true, "materials.json": true, "observations.sqlite": true}
	for _, token := range assembled.Tokens {
		allowed[capturedTokenName(token.UserID)] = true
	}
	if len(entries) < len(allowed) || len(entries) > len(allowed)+capturedHistoryScopeVersions {
		return empty, ErrGenerationIntegrity
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] {
			// Added scope envelopes are independent preparation outputs, not
			// part of the raw seal. Only bounded, content-addressed private
			// envelopes may coexist; a damaged one never triggers resealing.
			want := capturedHistoryScopeDigest(entry.Name())
			if want == "" {
				return empty, ErrGenerationIntegrity
			}
			got, _, err := capturedFileDigest(ctx, root, entry.Name(), monitor.MonthlyPreparationInputLimit)
			if err != nil || got != want {
				return empty, ErrGenerationIntegrity
			}
		} else {
			delete(allowed, entry.Name())
		}
	}
	if len(allowed) != 0 {
		return empty, ErrGenerationIntegrity
	}
	if err := automaticPreparationRootCurrent(root, directory); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return CapturedSourceFiles{Mode: capturedSourceFilesMode, Material: monthlysource.MonthlyObservationFile{Path: filepath.Join(directory, "materials.json"), SHA256: sha}, Selection: monthlysource.MonthlyObservationFile{Path: filepath.Join(directory, "selection.json"), SHA256: generationHash(assembled.Selection)}}, nil
}
