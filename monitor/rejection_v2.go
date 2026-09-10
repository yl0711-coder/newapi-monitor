package monitor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const rejectionV2MaxBody = 1 << 20
const rejectionV2ReplaySeconds int64 = 24 * 60 * 60
const rejectionReceiptMinimumSeconds int64 = 48 * 60 * 60

type rejectionV2Request struct {
	Node      string `json:"node"`
	BatchID   string `json:"batch_id"`
	CreatedAt int64  `json:"created_at"`
	Samples   []struct {
		BucketTs int64  `json:"bucket_ts"`
		Reason   string `json:"reason"`
		Model    string `json:"model"`
		Group    string `json:"group"`
		Count    int64  `json:"count"`
	} `json:"samples"`
}

func validateRejectionV2(in rejectionV2Request, now int64, retentionDays int) ([]RejectionSample, bool) {
	if !nginxNodeNamePattern.MatchString(in.Node) || !validIngestBatchID(in.BatchID) || in.CreatedAt <= 0 || in.CreatedAt < now-rejectionV2ReplaySeconds || in.CreatedAt > now+60 || len(in.Samples) == 0 || len(in.Samples) > 1000 {
		return nil, false
	}
	// The receipt ledger has a 48h minimum, longer than the 24h replay window
	// plus clock skew. Older data needs repair, not unbounded automatic replay.
	if retentionDays < 1 {
		retentionDays = 7
	}
	oldest := now - int64(retentionDays)*86400
	rows := make([]RejectionSample, 0, len(in.Samples))
	for _, s := range in.Samples {
		if s.BucketTs < oldest || s.BucketTs > now+60 || s.BucketTs%60 != 0 || s.Reason == "" || len(s.Reason) > 64 || s.Model == "" || len(s.Model) > 128 || len(s.Group) > 64 || s.Count < 1 || s.Count > 1000000 {
			return nil, false // Never silently drop/clip fields and ACK success.
		}
		rows = append(rows, RejectionSample{Node: in.Node, BucketTs: s.BucketTs, Reason: s.Reason, Model: s.Model, Grp: s.Group, Count: s.Count})
	}
	return rows, true
}

func (m *Monitor) ingestRejectionsV2(c *gin.Context) {
	if !m.checkIngest(c) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, rejectionV2MaxBody+1))
	if err != nil || len(body) > rejectionV2MaxBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "reject body unreadable or too large"})
		return
	}
	var in rejectionV2Request
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var trailing any
	if dec.Decode(&in) != nil || !errors.Is(dec.Decode(&trailing), io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reject v2 payload"})
		return
	}
	now := time.Now().Unix()
	if ecsLogNodePattern.MatchString(in.Node) {
		source, ok := verifiedECSLog(c)
		if !ok || source.Node != in.Node || source.Lane != "reject" {
			c.JSON(403, gin.H{"error": "ECS sources require signed ECS ingestion"})
			return
		}
	}
	rows, ok := validateRejectionV2(in, now, m.cfg.RetentionDays)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reject batch invalid or replay window expired; keep checkpoint for review"})
		return
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	duplicate, err := m.ingestRejectionBatchWithHash(in.Node, in.BatchID, rows, now, hash)
	if errors.Is(err, errRejectionBatchConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": "batch_id payload conflict"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reject store unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": 2, "node": in.Node, "batch_id": in.BatchID, "payload_hash": hash, "accepted": len(rows), "rejected": 0, "duplicate": duplicate})
}
