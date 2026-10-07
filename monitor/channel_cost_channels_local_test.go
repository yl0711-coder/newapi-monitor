//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// This opt-in acceptance reads an existing sealed local fixture. No worker,
// migration, external client or writable database is opened. CI needs no data.
func TestChannelCostHistoricalDirectorySealedLocalAcceptance(t *testing.T) {
	path := os.Getenv("MONITOR_CHANNEL_HISTORY_ACCEPTANCE_DB")
	if path == "" {
		t.Skip("requires a sealed local snapshot; never uses a running database")
	}
	if giftLocalFileHash(t, path) != attributionLocalSHA256 {
		t.Fatal("sealed local source hash mismatch")
	}
	backup, err := openGiftRelevantBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	gin.SetMode(gin.TestMode)
	m := &Monitor{storeDB: backup.db.WithContext(ctx), cfg: Settings{LocalSnapshotOnly: true}}
	response := getChannelCostDirectory(t, m, attributionLocalDomain)
	if response.Code != http.StatusOK {
		t.Fatal("local source directory unavailable", response.Code)
	}
	var payload struct {
		Epoch     string                             `json:"account_epoch"`
		Sources   []channelCostSourceView            `json:"sources"`
		Channels  []channelCostHistoricalChannelView `json:"historical_channels"`
		Truncated bool                               `json:"historical_channels_truncated"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	available := make(map[int]bool)
	for _, channel := range payload.Channels {
		available[channel.ID] = channel.Current
	}
	if payload.Truncated || !available[64] || !available[69] {
		t.Fatal("snapshot candidates must be selectable; neither is deleted in this fixture")
	}
	var candidate *channelCostSourceView
	for i := range payload.Sources {
		if payload.Sources[i].SourceRef == attributionLocalSource {
			candidate = &payload.Sources[i]
		}
	}
	if candidate == nil || candidate.Requests != 43 || candidate.CurrentBinding != nil || candidate.HistoricalBindingCount != 0 {
		t.Fatal("source is not the inspected unbound 43-request candidate")
	}
	history := channelCostHistoricalBindingInput{Domain: attributionLocalDomain, AccountEpoch: payload.Epoch,
		SourceRef: attributionLocalSource, LocalChannelID: attributionLocalChannel, Reason: "READ-ONLY LOCAL ACCEPTANCE; no attribution authorized"}
	previewResponse := previewChannelCostHistoricalBinding(t, m, history)
	var preview channelCostHistoricalBindingPlan
	if previewResponse.Code != http.StatusOK || json.Unmarshal(previewResponse.Body.Bytes(), &preview) != nil {
		t.Fatal("sealed historical preview failed", previewResponse.Code)
	}
	if preview.EvidenceHours != 11 || preview.EvidenceRequests != 43 || preview.LocalRequests != 43 || preview.LocalTestRequests != 0 || preview.WillQueueHours != 11 {
		t.Fatal("preview evidence differs from the inspected candidate")
	}
	// Even a mistakenly enabled rollout must not allow a snapshot write.
	m.cfg.ChannelCostClosureEnabled = true
	m.cfg.ChannelCostClosureDomains = []string{attributionLocalDomain}
	future := channelCostBindingInput{Domain: attributionLocalDomain, AccountEpoch: payload.Epoch, SourceRef: attributionLocalSource,
		SourceRefKind: candidate.SourceRefKind, HMACKeyID: candidate.HMACKeyID, LocalChannelID: attributionLocalChannel,
		AllocationMode: "allocated", Reason: history.Reason}
	if got := postChannelCostBinding(t, m, future); got.Code != http.StatusServiceUnavailable || !strings.Contains(got.Body.String(), "本地快照模式禁止写入未来来源映射") {
		t.Fatal("local future write was not refused before touching storage", got.Code)
	}
	if got := postChannelCostHistoricalBinding(t, m, history); got.Code != http.StatusServiceUnavailable || !strings.Contains(got.Body.String(), "本地快照模式禁止写入历史来源映射") {
		t.Fatal("local historical write was not refused before touching storage", got.Code)
	}
	assertNoCostDirectoryWrites(t, m)
	if err := backup.unchanged(); err != nil || giftLocalFileHash(t, path) != attributionLocalSHA256 {
		t.Fatal("sealed source was changed", err)
	}
	if output := os.Getenv("MONITOR_CHANNEL_HISTORY_ACCEPTANCE_RECEIPT"); output != "" {
		parent, err := os.Lstat(filepath.Dir(output))
		if err != nil || !filepath.IsAbs(output) || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 || parent.Mode().Perm()&0077 != 0 {
			t.Fatal("receipt needs an existing private directory and an absolute path")
		}
		// Only the selected source and its safe directory are needed by the UI
		// contract check; never export account credentials or the full database.
		payload.Sources = []channelCostSourceView{*candidate}
		receipt := map[string]any{"mode": "readonly_local_history_directory", "source_sha256": attributionLocalSHA256,
			"source_payload": payload, "preview": preview, "original_unchanged": true, "both_writes_refused": true}
		encoded, err := json.MarshalIndent(receipt, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, encoded); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("PASS: #64/#69 selectable; 11-hour/43-request preview; both writes refused; sealed source unchanged")
}
