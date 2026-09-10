package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// Synthetic fixed dates exercise a multi-file contract without depending on
// midnight during a short test. Log record timestamps remain current UTC.
var rejectFixtureNames = []string{"oneapi-20000101.log", "oneapi-20000102.log", "oneapi-20000103.log"}

func splitLayout(kind string) (string, []string, error) {
	switch kind {
	case "nginx":
		return "/logs", []string{"nexusapi_access.jsonl", "error.log"}, nil
	case "new-api":
		return "/app/logs", append([]string(nil), rejectFixtureNames[:2]...), nil
	default:
		return "", nil, fmt.Errorf("unknown synthetic producer kind")
	}
}

func runSplit(kind string, args []string) error {
	root, names, err := splitLayout(kind)
	if err != nil {
		return err
	}
	if len(args) == 1 && args[0] == "--health" {
		for _, name := range names {
			info, err := os.Lstat(filepath.Join(root, name))
			if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
				return fmt.Errorf("synthetic initial logs not ready")
			}
		}
		return nil
	}
	if len(args) != 0 {
		return fmt.Errorf("only --health is supported")
	}
	// Install TERM handling BEFORE initial files become visible to health checks.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	return produceSplit(ctx, kind, root, os.Stdout, time.Second, 70*time.Minute)
}

type splitWriter struct {
	kind, root, instance string
	files                map[string]*os.File
	counts               map[string]int
	sequence             int
	output               io.Writer
}

func newSplitWriter(kind, root string, output io.Writer) (*splitWriter, error) {
	_, names, err := splitLayout(kind)
	if err != nil {
		return nil, err
	}
	var id [8]byte
	if _, err = rand.Read(id[:]); err != nil {
		return nil, err
	}
	w := &splitWriter{kind: kind, root: root, instance: hex.EncodeToString(id[:]),
		files: make(map[string]*os.File), counts: make(map[string]int), output: output}
	for _, name := range names {
		if err := w.open(name); err != nil {
			w.close()
			return nil, err
		}
	}
	return w, nil
}

func (w *splitWriter) open(name string) error {
	// Never overwrite/reuse a previous producer's file or follow a file symlink.
	f, err := os.OpenFile(filepath.Join(w.root, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.files[name], w.counts[name] = f, 0
	return nil
}

func (w *splitWriter) close() {
	for _, f := range w.files {
		_ = f.Close()
	}
}

func (w *splitWriter) write(final bool) error {
	if final && w.kind == "new-api" {
		if err := w.open(rejectFixtureNames[2]); err != nil {
			return err
		}
	}
	w.sequence++
	now := time.Now().UTC()
	id := fmt.Sprintf("split-%s-%d", w.instance, w.sequence)
	for name, f := range w.files {
		var line string
		switch name {
		case "nexusapi_access.jsonl":
			line = fmt.Sprintf(`{"log_schema":2,"msec":"%d.250","request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.350","upstream_status":"200","upstream_response_time":"0.300","upstream_connect_time":"0.025","upstream_header_time":"0.125","bytes_sent":"1024","nginx_request_id":%q,"oneapi_request_id":%q,"request_completion":"OK"}`, now.Unix(), id, id)
		case "error.log":
			line = now.Format("2006/01/02 15:04:05") + " [error] 1#1: *1 upstream timed out (110: Connection timed out) while reading response header from upstream"
		default:
			line = "[ERR] " + now.Format("2006/01/02 - 15:04:05") + " | " + id + " | user 7 | No available channel for model synthetic under group synthetic-group (distributor)"
		}
		if _, err := f.WriteString(line + "\n"); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		w.counts[name]++
	}
	if final {
		for _, f := range w.files {
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	if final || w.sequence == 1 || w.sequence%10 == 0 {
		// Distinct prefix/schema: the old single-producer audit must not silently
		// treat these per-file counts as its old equal-three-files oracle.
		data, err := json.Marshal(map[string]any{"schema": "split-v1", "kind": w.kind,
			"instance": w.instance, "files": w.counts, "final": final})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(w.output, "SPLIT_SYNTHETIC_ORACLE %s\n", data)
		return err
	}
	return nil
}

func produceSplit(ctx context.Context, kind, root string, output io.Writer, interval, lifetime time.Duration) error {
	w, err := newSplitWriter(kind, root, output)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.write(false); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.NewTimer(lifetime)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return w.write(true)
		case <-deadline.C:
			return w.write(true)
		case <-ticker.C:
			if err := w.write(false); err != nil {
				return err
			}
		}
	}
}
