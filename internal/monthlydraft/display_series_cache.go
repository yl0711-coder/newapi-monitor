package monthlydraft

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const automaticDisplayCacheLimit int64 = 4 << 20

type automaticDisplayCache struct {
	Version    int                                     `json:"version"`
	Request    monthlytasks.Request                    `json:"request"`
	ReviewedAt int64                                   `json:"reviewed_at"`
	Source     monitor.MonthlyDisplayPublicationSource `json:"source"`
	SHA256     string                                  `json:"sha256"`
}

func automaticDisplayCacheDigest(c automaticDisplayCache) (string, error) {
	c.SHA256 = ""
	raw, err := json.Marshal(c)
	if err != nil || int64(len(raw)) > automaticDisplayCacheLimit {
		return "", errors.Join(ErrGenerationCapacity, err)
	}
	return generationHash(raw), nil
}

// One immutable private cache per exact source request. A per-file OS lock
// coordinates both processes and independent receiver handles sharing a root.
// Completed temp data is fsynced before atomic rename; a crash before rename
// leaves no usable partial cache. No runtime database/schema is changed.
func (g *Generation) automaticDisplaySource(ctx context.Context, request monthlytasks.Request) (monitor.MonthlyDisplayPublicationSource, error) {
	var empty monitor.MonthlyDisplayPublicationSource
	if ctx == nil {
		return empty, ErrGenerationInput
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	raw, err := json.Marshal(request)
	if err != nil || !validSelectionRequest(request, request.Owner.GroupID, request.Month) {
		return empty, ErrGenerationInput
	}
	name := "display-source-" + generationHash(raw)
	lock, err := g.root.OpenFile(name+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	info, err := lock.Stat()
	if err != nil || !privateAutomaticDisplayFile(info) {
		return empty, ErrGenerationIntegrity
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return empty, ErrGenerationBusy
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	file := name + ".json"
	var cached automaticDisplayCache
	before, err := g.root.Lstat(file)
	exists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return empty, err
	}
	if exists {
		if !privateAutomaticDisplayFile(before) {
			return empty, ErrGenerationIntegrity
		}
		raw, err := readArchiveFile(ctx, g.root, file, automaticDisplayCacheLimit)
		if err != nil || json.Unmarshal(raw, &cached) != nil {
			return empty, errors.Join(ErrGenerationIntegrity, err)
		}
		canonical, err := json.Marshal(cached)
		digest, digestErr := automaticDisplayCacheDigest(cached)
		after, statErr := g.root.Lstat(file)
		if err != nil || digestErr != nil || statErr != nil || !privateAutomaticDisplayFile(after) || !os.SameFile(before, after) ||
			!bytes.Equal(raw, canonical) || cached.Version != 1 || cached.Request != request || cached.SHA256 != digest || cached.ReviewedAt <= 0 || cached.ReviewedAt > time.Now().Unix() {
			return empty, ErrGenerationIntegrity
		}
	} else {
		cached = automaticDisplayCache{Version: 1, Request: request, ReviewedAt: time.Now().Unix()}
	}
	source, err := g.auditAutomaticDisplaySource(ctx, request, cached.ReviewedAt)
	if err != nil {
		return empty, err
	}
	if exists {
		want, _ := json.Marshal(cached.Source)
		got, err := json.Marshal(source)
		if err != nil || !bytes.Equal(want, got) {
			return empty, ErrGenerationIntegrity
		}
		return source, ctx.Err()
	}
	cached.Source = source
	cached.SHA256, err = automaticDisplayCacheDigest(cached)
	if err != nil {
		return empty, err
	}
	encoded, err := json.Marshal(cached)
	if err != nil || int64(len(encoded)) > automaticDisplayCacheLimit {
		return empty, ErrGenerationCapacity
	}
	temp := name + ".pending-" + rand.Text()
	defer func() { _ = g.root.Remove(temp) }() // only this call's exclusively created temp name
	if err := writeArchiveFile(ctx, g.root, temp, encoded); err != nil {
		return empty, err
	}
	if err := g.current(ctx, request); err != nil {
		return empty, err
	}
	if err := g.commitAutomaticDisplayCache(ctx, temp, file, int64(len(encoded))); err != nil {
		return empty, err
	}
	return source, ctx.Err()
}

// The immutable evidence cache is bounded independently of the SQL receiver.
// Never evict anchors or source evidence automatically to make room. Existing
// cache hits remain usable at capacity; new versions require reviewed retention.
func (g *Generation) commitAutomaticDisplayCache(ctx context.Context, temp, file string, added int64) error {
	lock, err := g.root.OpenFile("display-cache-capacity.lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	info, err := lock.Stat()
	if err != nil || !privateAutomaticDisplayFile(info) {
		return ErrGenerationIntegrity
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return ErrGenerationBusy
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	directory, err := g.root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	count, visited := 0, 0
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := directory.ReadDir(128)
		for _, entry := range entries {
			visited++
			if visited > 8192 {
				return ErrGenerationCapacity
			}
			name := entry.Name()
			key := strings.TrimSuffix(strings.TrimPrefix(name, "display-source-"), ".json")
			if name != "display-source-"+key+".json" || !generationHex(key, 32) {
				continue
			}
			info, err := entry.Info()
			if err != nil || !privateAutomaticDisplayFile(info) || info.Size() < 0 {
				return ErrGenerationIntegrity
			}
			count++
			size += info.Size()
			if count >= 256 || size > generationReceiverLimit-added {
				return ErrGenerationCapacity
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	if _, err := g.root.Lstat(file); !errors.Is(err, os.ErrNotExist) {
		return ErrGenerationIntegrity
	}
	if err := g.root.Rename(temp, file); err != nil {
		return err
	}
	return directory.Sync()
}

func privateAutomaticDisplayFile(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1 && stat.Uid == uint32(os.Geteuid())
}
