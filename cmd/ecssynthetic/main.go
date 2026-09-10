// ecssynthetic generates bounded, non-business log fixtures for isolated ECS.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "synthetic generator failed:", err)
		os.Exit(1)
	}
}

func run() error {
	if os.Getenv("ECSLOG_SCOPE") != "isolated" {
		return fmt.Errorf("isolated scope required")
	}
	if kind := os.Getenv("ECSLOG_SYNTHETIC_KIND"); kind != "" {
		return runSplit(kind, os.Args[1:])
	}
	if len(os.Args) != 1 {
		return fmt.Errorf("legacy synthetic generator accepts no arguments")
	}
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	instance := hex.EncodeToString(id[:])
	files := make([]*os.File, 0, 3)
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	for _, name := range []string{"access.jsonl", "error.log", "new-api.log"} {
		f, err := os.OpenFile(filepath.Join("/logs", name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		files = append(files, f)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.NewTimer(70 * time.Minute)
	defer deadline.Stop()
	count := 0
	write := func(final bool) error {
		now := time.Now().UTC()
		next := count + 1
		requestID := fmt.Sprintf("synthetic-%s-%d", instance, next)
		access, err := json.Marshal(map[string]any{
			"log_schema": 2, "msec": fmt.Sprintf("%d.250", now.Unix()), "request_method": "POST",
			"uri": "/v1/responses", "status": "200", "request_time": "0.350", "upstream_status": "200",
			"upstream_response_time": "0.300", "upstream_connect_time": "0.025", "upstream_header_time": "0.125",
			"bytes_sent": "1024", "nginx_request_id": requestID, "oneapi_request_id": requestID, "request_completion": "OK",
		})
		if err != nil {
			return err
		}
		lines := []string{string(access) + "\n",
			now.Format("2006/01/02 15:04:05") + " [error] 1#1: *1 upstream timed out (110: Connection timed out) while reading response header from upstream\n",
			"[ERR] " + now.Format("2006/01/02 - 15:04:05") + " | " + requestID + " | user 7 | No available channel for model synthetic under group synthetic-group (distributor)\n"}
		for i, line := range lines {
			if _, err := files[i].WriteString(line); err != nil {
				return err
			}
			if err := files[i].Sync(); err != nil {
				return err
			}
		}
		count = next
		if final || count%10 == 0 {
			// CloudWatch stdout is an independent producer oracle, not derived
			// from collector output. All three files have exactly this many lines.
			fmt.Printf("SYNTHETIC_ORACLE {\"instance\":%q,\"count\":%d,\"final\":%t}\n", instance, count, final)
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return write(true)
		case <-deadline.C:
			return write(true)
		case <-ticker.C:
			if err := write(false); err != nil {
				return err
			}
		}
	}
}
