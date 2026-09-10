package ecslogagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

// Isolated acceptance has a hard budget, not unbounded task disk growth. At
// capacity stop safely; do not prune dependency proofs before final manifests.
const archiveJournalEntries = 8192
const archiveJournalBytes = 2 << 20

type archiveJournal struct {
	Binding    string
	Heads      map[string]string
	Previous   map[string]string
	Closed     map[string]string                    `json:",omitempty"`
	Boundaries map[string]*ecsarchive.FinalBoundary `json:",omitempty"`
}

func (a *Agent) archiveJournalPath() string { return filepath.Join(a.dir, "archive-chain.json") }
func (a *Agent) readArchiveJournal() (archiveJournal, error) {
	var state archiveJournal
	f, err := os.OpenFile(a.archiveJournalPath(), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return state, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return state, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > archiveJournalBytes {
		return state, errors.New("invalid private archive journal")
	}
	raw, err := io.ReadAll(io.LimitReader(f, archiveJournalBytes+1))
	if err != nil {
		return state, err
	}
	if len(raw) > archiveJournalBytes || json.Unmarshal(raw, &state) != nil || state.Heads == nil || state.Previous == nil || len(state.Heads) > 4 || len(state.Previous) > archiveJournalEntries {
		return state, errors.New("invalid bounded archive journal")
	}
	if len(state.Closed) > 4 || len(state.Boundaries) > 4 {
		return state, errors.New("invalid archive closure journal")
	}
	for lane, boundary := range state.Boundaries {
		if boundary == nil || state.Closed[lane] == "" {
			return state, errors.New("orphan final boundary")
		}
		if _, err := ecsarchive.ReadClosure(ecsarchive.BoundaryBody(a.node, boundary), a.node, lane); err != nil {
			return state, err
		}
	}
	for lane, hash := range state.Closed {
		_, linked := state.Previous[lane+"/"+hash]
		expected := sha256.Sum256(ecsarchive.BoundaryBody(a.node, state.Boundaries[lane]))
		if !a.laneAllowed(lane) || !linked || state.Heads[lane] != hash || hash != hex.EncodeToString(expected[:]) {
			return state, errors.New("archive closure journal lost its boundary")
		}
	}
	return state, nil
}
func (a *Agent) saveArchiveJournal(state archiveJournal) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(raw) > archiveJournalBytes {
		return errors.New("archive journal capacity reached")
	}
	return writeIdentity(a.archiveJournalPath(), raw)
}
func (a *Agent) checkArchiveJournal(binding string) error {
	if binding == "" {
		return errors.New("archive store identity required")
	}
	sum := sha256.Sum256([]byte(binding))
	binding = hex.EncodeToString(sum[:])
	state, err := a.readArchiveJournal()
	if errors.Is(err, os.ErrNotExist) {
		marker := filepath.Join(a.dir, "archive-enabled")
		if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
			return errors.New("archive journal missing after activation; recovery required")
		}
		// Marker first: a crash between these writes fails closed on restart.
		if err := writeIdentity(marker, []byte(binding)); err != nil {
			return err
		}
		return a.saveArchiveJournal(archiveJournal{Binding: binding, Heads: map[string]string{}, Previous: map[string]string{}})
	}
	if err != nil {
		return err
	}
	if state.Binding != binding {
		return errors.New("archive destination changed; preserve journal for recovery")
	}
	return nil
}
func (a *Agent) freezeArchiveEnvelope(lane string, body []byte) (ecsarchive.Envelope, error) {
	e, err := ecsarchive.Seal(a.cfg.Audience, a.node, lane, body, a.key)
	if err != nil {
		return e, err
	}
	state, err := a.readArchiveJournal()
	if err != nil {
		return e, err
	}
	id := lane + "/" + e.Hash
	previous, exists := state.Previous[id]
	if !exists {
		if state.Closed[lane] != "" {
			return e, errors.New("archive lane already closed; preserve unexpected batch")
		}
		if len(state.Previous) >= archiveJournalEntries {
			return e, errors.New("archive journal full; retain local batch")
		}
		previous = state.Heads[lane]
		state.Previous[id], state.Heads[lane] = previous, e.Hash
		if err := a.saveArchiveJournal(state); err != nil {
			return e, err
		}
	}
	return ecsarchive.SealAfter(a.cfg.Audience, a.node, lane, body, previous, a.key)
}
