package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// ECS error batches need the same durable retry boundary as access batches.
// Otherwise an ACK loss followed by new log lines would reread an overlapping
// range with a new batch ID and archive both versions. Lightsail is unchanged.
type ecsErrorInflight struct {
	Version int
	LogPath string
	Current cursor
	Next    cursor
	Payload errorBatch
	Hash    string
}

func ecsErrorInflightPath(c config) string { return c.errorCursorPath + ".ecs-inflight.json" }

func (p ecsErrorInflight) digest() string {
	p.Hash = ""
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func loadECSErrorInflight(c config) (ecsErrorInflight, bool, error) {
	var p ecsErrorInflight
	f, err := os.Open(ecsErrorInflightPath(c))
	if errors.Is(err, os.ErrNotExist) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, accessInflightMaxBytes+1))
	if err != nil {
		return p, false, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var trailing any
	if len(raw) > accessInflightMaxBytes || d.Decode(&p) != nil || !errors.Is(d.Decode(&trailing), io.EOF) ||
		p.Version != 1 || p.LogPath != c.errorLogPath || p.Payload.Node != c.node || len(p.Payload.BatchID) != sha256.Size*2 ||
		p.Payload.SourceBoundary != nil || p.Next.Inode == 0 || p.Next.Device != 0 || p.Next.Offset <= 0 || p.Current == p.Next || p.Hash != p.digest() {
		return p, false, errors.New("invalid frozen ECS error batch; preserve journal and cursor")
	}
	return p, true, nil
}

func removeECSErrorInflight(c config) error {
	path := ecsErrorInflightPath(c)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func runECSErrorOnce(ctx context.Context, c config) error {
	if c.sourceV2Prepare {
		return errors.New("ECS error archive does not acknowledge source V2 boundaries")
	}
	current, err := loadCursor(c.errorCursorPath)
	if err != nil {
		return err
	}
	p, exists, err := loadECSErrorInflight(c)
	if err != nil {
		return err
	}
	if exists {
		if current == p.Next {
			return removeECSErrorInflight(c)
		}
		if current != p.Current {
			return errors.New("ECS error cursor differs from frozen batch; recovery required")
		}
	} else {
		payload, next, ok, err := readErrorBatch(c, current)
		if err != nil {
			return err
		}
		next.Device = 0 // Match the existing v1 cursor format.
		if !ok {
			if next != current {
				return saveCursor(c.errorCursorPath, next)
			}
			return nil
		}
		p = ecsErrorInflight{Version: 1, LogPath: c.errorLogPath, Current: current, Next: next, Payload: payload}
		p.Hash = p.digest()
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if len(raw) > accessInflightMaxBytes {
			return errors.New("ECS error inflight journal exceeds budget")
		}
		if err := writeAtomic(ecsErrorInflightPath(c), raw, 0600); err != nil {
			return err
		}
	}
	if err := postErrorBatch(ctx, c, p.Payload); err != nil {
		return err
	}
	if err := saveCursor(c.errorCursorPath, p.Next); err != nil {
		return err
	}
	return removeECSErrorInflight(c)
}
