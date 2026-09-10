package ecslogagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

// 202 confirms ONLY external durability, never Monitor fact ingestion. A
// collector must explicitly understand this contract before advancing a cursor.
func (a *Agent) writeArchiveReceipt(w http.ResponseWriter, lane string, body []byte) {
	var payload struct {
		BatchID string `json:"batch_id"`
	}
	if json.Unmarshal(body, &payload) != nil || payload.BatchID == "" {
		http.Error(w, "invalid archive identity", 400)
		return
	}
	sum := sha256.Sum256(body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(struct {
		Version  int    `json:"version"`
		State    string `json:"state"`
		Node     string `json:"node"`
		Lane     string `json:"lane"`
		BatchID  string `json:"batch_id"`
		BodyHash string `json:"body_sha256"`
	}{1, "durably_archived", a.node, lane, payload.BatchID, hex.EncodeToString(sum[:])})
}
