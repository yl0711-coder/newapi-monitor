package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestECSDrainDoesNotDeclarePartialOrMissingFileComplete(t *testing.T) {
	for _, scenario := range []string{"missing", "partial"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			c := config{node: "ecs-fixture", ecsSocket: filepath.Join(dir, "unavailable.sock"), logPath: filepath.Join(dir, "access.jsonl"), cursorPath: filepath.Join(dir, "cursor.json"), evidenceMode: "off", maxLines: 10, retentionDays: 7}
			if scenario == "partial" {
				if err := os.WriteFile(c.logPath, []byte(`{"log_schema":2`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := drainECSCollector(ctx, c); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("incomplete source reported drained: %v", err)
			}
		})
	}
}

func TestECSObservedEOFRejectsRecordedGaps(t *testing.T) {
	dir := t.TempDir()
	path, checkpoint := filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "cursor.json")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveCursor(checkpoint, cursor{Inode: fileInode(info), Discontinuities: 1, LastDiscontinuityAt: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := ecsObservedEOF(path, checkpoint); err == nil {
		t.Fatal("EOF hid known source gap")
	}
}

func TestECSDrainRejectsUnadaptedSourceV2(t *testing.T) {
	if err := drainECSCollector(context.Background(), config{ecsSocket: "/fixture.sock", sourceV2Prepare: true}); err == nil {
		t.Fatal("unadapted source V2 accepted")
	}
}
