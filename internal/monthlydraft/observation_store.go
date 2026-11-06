package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// bindObservationStore returns a call-local view, never mutating a shared
// Generation or taking ownership of its default DB/root/receiver. New plans
// select ONLY a hash-pinned retained source, not an HTTP-supplied database path.
// The caller releases the scoped handle after all nested audits complete.
func (g *Generation) bindObservationStore(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (*Generation, func(), error) {
	noop := func() {}
	if ctx == nil || g == nil || g.root == nil || g.funding == nil {
		return nil, noop, ErrGenerationInput
	}
	if err := ctx.Err(); err != nil {
		return nil, noop, err
	}
	if p.ObservationStoreSourceID == "" {
		if g.observationSource != nil {
			return nil, noop, ErrGenerationIntegrity
		}
		return g, noop, nil // Preserve retained legacy inputs byte-for-byte.
	}
	var selected monitor.MonthlyPreparationSource
	for _, source := range input.Sources {
		if source.ID == p.ObservationStoreSourceID {
			if selected.ID != "" {
				return nil, noop, ErrGenerationInput
			}
			selected = source
		}
	}
	if !validScopeSourceID(selected.ID) || !generationHex(selected.SHA256, 32) ||
		(p.WalletObservations != nil && p.WalletObservations.SourceID != selected.ID) {
		return nil, noop, ErrGenerationInput
	}
	if g.observationSource != nil {
		if *g.observationSource != selected {
			return nil, noop, ErrGenerationIntegrity
		}
		return g, noop, nil // Nested normalizers share the same scoped handle.
	}
	before, err := g.sealedObservationFile(selected)
	if err != nil {
		return nil, noop, err
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return nil, noop, err
	}
	// immutable avoids journal creation or recovery. Only sealed private copies
	// without ANY SQLite sidecar qualify; never apply this to a live DB/WAL.
	u := url.URL{Scheme: "file", Path: selected.Path}
	u.RawQuery = url.Values{"mode": {"ro"}, "immutable": {"1"}, "_pragma": {"query_only(1)", "busy_timeout(500)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, noop, ErrGenerationIntegrity
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	release := func() { _ = db.Close() }
	if err := db.PingContext(ctx); err != nil {
		release()
		return nil, noop, errors.Join(ErrGenerationIntegrity, ctx.Err())
	}
	scoped := *g
	scoped.funding, scoped.observationSource, scoped.observationFile = db, &selected, before
	after, err := scoped.sealedObservationFile(selected)
	if err == nil && (!os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime())) {
		err = ErrGenerationIntegrity
	}
	if err == nil {
		err = scoped.sealedUsageSource(ctx, selected)
	}
	if err != nil {
		release()
		return nil, noop, err
	}
	return &scoped, release, nil
}

func (g *Generation) requireObservationStore(p GenerationPlan) error {
	if p.ObservationStoreSourceID == "" {
		if g.observationSource == nil {
			return nil
		}
	} else if g.observationSource != nil && g.observationSource.ID == p.ObservationStoreSourceID {
		return nil
	}
	return ErrGenerationIntegrity // Never silently fall back to the default DB.
}

// Recheck the seal after file hashing without streaming the same files again.
// Scope packaging also uses this after reserving budget for its new file.
func (g *Generation) verifyBoundObservationStore(ctx context.Context, sources []monitor.MonthlyPreparationSource) error {
	if g.observationSource == nil {
		return nil
	}
	found := false
	for _, source := range sources {
		found = found || source == *g.observationSource
	}
	if !found {
		return ErrGenerationIntegrity
	}
	info, err := g.sealedObservationFile(*g.observationSource)
	if err != nil {
		return err
	}
	if g.observationFile == nil || !os.SameFile(info, g.observationFile) || info.Size() != g.observationFile.Size() ||
		!info.ModTime().Equal(g.observationFile.ModTime()) {
		return ErrGenerationIntegrity
	}
	return g.sealedUsageSource(ctx, *g.observationSource)
}

func (g *Generation) sealedObservationFile(source monitor.MonthlyPreparationSource) (os.FileInfo, error) {
	rel, err := g.relativeSource(source.Path)
	if err != nil {
		return nil, ErrGenerationIntegrity
	}
	// Do not let a symlinked parent redirect sqlite's absolute-path open outside
	// the anchored root. Parent directories inside the root must also be private.
	for parent := filepath.Dir(rel); parent != "."; parent = filepath.Dir(parent) {
		info, err := g.root.Lstat(parent)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return nil, ErrGenerationIntegrity
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(os.Geteuid()) {
			return nil, ErrGenerationIntegrity
		}
	}
	info, err := g.root.Lstat(rel)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, ErrGenerationIntegrity
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return nil, ErrGenerationIntegrity
	}
	if info.Size() <= 0 || info.Size() > generationEvidenceLimit {
		return nil, ErrGenerationCapacity
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := g.root.Lstat(rel + suffix); !os.IsNotExist(err) {
			return nil, ErrGenerationIntegrity
		}
	}
	receiver, err := generationDatabaseFile(g.receiver)
	if err != nil || os.SameFile(receiver, info) {
		return nil, ErrGenerationIntegrity
	}
	return info, nil
}
