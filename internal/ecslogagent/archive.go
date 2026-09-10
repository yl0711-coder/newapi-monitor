package ecslogagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

const (
	archiveWriteTimeout = 8 * time.Second
	archiveLeaseMargin  = 20 * time.Second
)

// Must be configured before RunChild. The owner is STS UserId, not a task's
// self-asserted path. IAM confines writes/reads to ${aws:userid} under Prefix.
func (a *Agent) ConfigureArchive(store ecsarchive.Writer, prefix, owner string) error {
	if store == nil || ecsarchive.OwnerTask(owner) != a.meta.TaskARN[strings.LastIndex(a.meta.TaskARN, "/")+1:] {
		return errors.New("archive writer identity is not this task")
	}
	probe, err := ecsarchive.Seal(a.cfg.Audience, a.node, a.cfg.lanes()[0], []byte(`{"node":"`+a.node+`","batch_id":"archive-config-probe"}`), a.key)
	if err != nil {
		return err
	}
	if _, err := ecsarchive.ObjectKey(prefix, owner, probe); err != nil {
		return err
	}
	if err := a.checkArchiveJournal(store.Binding() + "/" + prefix + "/" + owner); err != nil {
		return err
	}
	a.archive, a.archivePrefix, a.archiveOwner = store, prefix, owner
	return nil
}

func (a *Agent) archiveFrozen(ctx context.Context, lane string, body []byte) error {
	if a.archive == nil {
		return nil
	}
	// A registered source can archive during its short lease even if Monitor's
	// data endpoint is offline. After expiry, retain locally and await renewal.
	a.archiveMu.Lock()
	defer a.archiveMu.Unlock()
	// Leave time for the bounded write while the authorization is valid.
	// Check after taking the lock, not before waiting behind another lane.
	if a.lease(lane) <= a.now().Add(archiveLeaseMargin).Unix() {
		return errors.New("source lease unavailable for archive")
	}
	ctx, cancel := context.WithTimeout(ctx, archiveWriteTimeout)
	defer cancel()
	envelope, err := a.freezeArchiveEnvelope(lane, body)
	if err != nil {
		return err
	}
	return a.putArchiveEnvelope(ctx, lane, envelope)
}

// Caller holds archiveMu and supplies a bounded context.
func (a *Agent) putArchiveEnvelope(ctx context.Context, lane string, envelope ecsarchive.Envelope) error {
	key, err := ecsarchive.ObjectKey(a.archivePrefix, a.archiveOwner, envelope)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	err = a.archive.Put(ctx, key, raw)
	if errors.Is(err, ecsarchive.ErrExists) {
		previous, readErr := a.archive.Get(ctx, key)
		if readErr != nil {
			return readErr
		}
		if previous.Key != key || !bytes.Equal(previous.Body, raw) {
			return errors.New("immutable archive object conflict; retain batch")
		}
		if a.lease(lane) <= a.now().Unix() {
			return errors.New("source lease expired before archive confirmation")
		}
		return nil
	}
	if err == nil && a.lease(lane) <= a.now().Unix() {
		return errors.New("source lease expired before archive confirmation")
	}
	return err
}
