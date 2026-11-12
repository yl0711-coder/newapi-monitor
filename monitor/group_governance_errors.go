package monitor

// Only the existing bounded minute collector writes this projection. Pages do
// not collect errors. No raw request body, credential or customer name is kept.
import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const groupGovernanceErrorVersion = 1

type GroupGovernanceErrorMinute struct {
	BucketTs    int64  `gorm:"primaryKey;autoIncrement:false"`
	ChannelID   int    `gorm:"primaryKey;autoIncrement:false"`
	Grp         string `gorm:"primaryKey;size:64"`
	Model       string `gorm:"primaryKey;size:128"`
	Failed      int64
	Anomaly     int64
	FailedRead  int64
	AnomalyRead int64
	Version     int
	CapturedAt  int64
	ReasonsJSON string `gorm:"type:text"`
}

type ggErrorSample struct {
	LogID     int64  `json:"log_id"`
	RequestID string `json:"request_id"`
	CreatedAt int64  `json:"created_at"`
	Model     string `json:"model,omitempty"`
}
type ggErrorReason struct {
	RuleID         string          `json:"rule_id"`
	Code           string          `json:"code"`
	Reason         string          `json:"reason"`
	Responsibility string          `json:"responsibility"`
	Count          int64           `json:"count"`
	Attributed     bool            `json:"attributed"`
	Pending        bool            `json:"pending"`
	Unavailable    bool            `json:"unavailable"`
	Capacity       bool            `json:"capacity"`
	Samples        []ggErrorSample `json:"samples"`
}
type ggMetricKey struct {
	Group, Model string
	Channel      int
}
type ggMinuteKey struct {
	ggMetricKey
	At int64
}
type ggErrorProjection struct {
	rows    map[ggMinuteKey]*GroupGovernanceErrorMinute
	reasons map[ggMinuteKey]map[string]*ggErrorReason
}

func newGGErrorProjection(batch []CapacityUserMinuteSample) *ggErrorProjection {
	p := &ggErrorProjection{rows: map[ggMinuteKey]*GroupGovernanceErrorMinute{}, reasons: map[ggMinuteKey]map[string]*ggErrorReason{}}
	for _, r := range batch {
		k := ggMinuteKey{ggMetricKey{strings.TrimSpace(r.Grp), strings.TrimSpace(r.ModelName), r.ChannelID}, r.BucketTs}
		if p.rows[k] == nil {
			p.rows[k] = &GroupGovernanceErrorMinute{BucketTs: k.At, Grp: k.Group, Model: k.Model, ChannelID: k.Channel, Version: groupGovernanceErrorVersion}
		}
		p.rows[k].Failed += r.Failed
		p.rows[k].Anomaly += r.Anomaly
	}
	return p
}

func (p *ggErrorProjection) observe(r LogChainRow, f logChainFault) {
	k := ggMinuteKey{ggMetricKey{strings.TrimSpace(r.Group), strings.TrimSpace(r.ModelName), int(r.ChannelID)}, r.CreatedAt / 60 * 60}
	row := p.rows[k]
	if row == nil {
		return
	}
	if r.Type == 5 {
		row.FailedRead++
	} else {
		row.AnomalyRead++
	}
	if p.reasons[k] == nil {
		p.reasons[k] = map[string]*ggErrorReason{}
	}
	d := f.Diagnosis
	reason := ggErrorReason{Code: r.UpstreamErrorCode, Reason: f.Reason, Responsibility: f.Fault, Pending: true}
	if reason.Reason == "" {
		reason.Reason = f.Why
	}
	// Keep the shared diagnostic verdict. Only a known, exclusively
	// channel/upstream cause can enter the confirmed fault numerator.
	if d != nil {
		reason.RuleID, reason.Reason, reason.Responsibility = d.RuleID, d.Reason, d.Responsibility
		exclusive := len(d.ResponsibleParties) > 0
		for _, party := range d.ResponsibleParties {
			if party != faultUpstream && party != faultOurs {
				exclusive = false
			}
		}
		// Platform resource/billing errors affect the platform, not necessarily
		// this channel. Only upstream or diagnosed channel account/route errors
		// qualify; other known causes stay visible without charging the channel.
		channel := strings.Contains(f.Fault, faultUpstream) || d.Profile == "account" || d.Profile == "route" || d.RuleID == "LC-089"
		reason.Attributed = d.CauseKnown && exclusive && channel && f.Confidence == faultConfHigh
		reason.Pending = !d.CauseKnown || f.Confidence != faultConfHigh || strings.Contains(f.Fault, faultUnknown) || (strings.Contains(f.Fault, faultDownstream) && f.Fault != faultDownstream)
		if reason.Attributed {
			switch d.RuleID {
			case "LC-082", "LC-089", "LC-122", "LC-129", "LC-145", "LC-146", "LC-156", "LC-173":
				reason.Unavailable = true
			case "LC-063", "LC-077", "LC-108", "LC-110", "LC-115", "LC-120", "LC-155":
				reason.Capacity = true
			}
		}
	} else if f.Fault == faultDownstream && f.Confidence == faultConfHigh {
		reason.Pending = false
	}
	if reason.Reason == "" {
		reason.Reason = "分钟统计将此尝试计为交付异常；同次请求的客户排障规则未给出故障原因，需核对请求路径、流结束状态和交付证据"
	}
	if reason.Responsibility == "" {
		reason.Responsibility = "待查"
	}
	key := reason.RuleID + "\x00" + reason.Code + "\x00" + reason.Responsibility + "\x00" + reason.Reason
	v := p.reasons[k][key]
	if v == nil {
		v = &reason
		p.reasons[k][key] = v
	}
	v.Count++
	for _, sample := range v.Samples {
		if r.RequestID != "" && sample.RequestID == r.RequestID {
			return
		}
	}
	if len(v.Samples) < 3 {
		v.Samples = append(v.Samples, ggErrorSample{LogID: r.ID, RequestID: logChainDiagnosticSafeText(r.RequestID, 192), CreatedAt: r.CreatedAt})
	}
}

// Delivery details are optional and use the SAME source slot and deadline as
// the minute collector. A failure never invalidates customer-health or FRT.
// The predicate is the same as the minute denominator, not a broad 4xx/5xx list.
func (p *ggErrorProjection) collectDelivery(ctx context.Context, db *sql.DB, from, to int64) {
	var expected int64
	for _, r := range p.rows {
		expected += r.Anomaly
	}
	if expected == 0 || expected > customerHealthClassificationRows {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	query := `SELECT /*+ MAX_EXECUTION_TIME(1500) */ id,created_at,user_id,COALESCE(channel_id,0),COALESCE(model_name,''),COALESCE(` + "`group`" + `,''),
 COALESCE(request_id,''),SUBSTR(COALESCE(content,''),1,16385),SUBSTR(COALESCE(other,''),1,32769),
 COALESCE(prompt_tokens,0),COALESCE(completion_tokens,0),COALESCE(quota,0),COALESCE(is_stream,0)
 FROM logs WHERE created_at>=? AND created_at<? AND type=2 AND NOT (` + channelTestLogPredicateSQL() + `) AND ` + deliveryAnomalySQL() + ` ORDER BY created_at,id LIMIT 2049`
	rows, err := db.QueryContext(ctx, query, from, to)
	if err != nil {
		return
	}
	defer rows.Close()
	type observation struct {
		r LogChainRow
		f logChainFault
	}
	var observations []observation
	bytes := 0
	for rows.Next() {
		var r LogChainRow
		var other string
		var quota int64
		r.Type = 2
		if rows.Scan(&r.ID, &r.CreatedAt, &r.UserID, &r.ChannelID, &r.ModelName, &r.Group, &r.RequestID, &r.Content, &other, &r.PromptTokens, &r.CompletionTokens, &quota, &r.IsStream) != nil {
			return
		}
		bytes += len(r.Content) + len(other)
		if bytes > customerHealthClassificationBytes || len(observations) >= customerHealthClassificationRows || len(r.Content) > 16384 || len(other) > 32768 {
			return
		}
		r = customerHealthDiagnosticRow(r, other)
		if o := parseLogOther(other); o != nil {
			r.EndReason = o.StreamStatus.EndReason
			r.StreamErrorCount = o.StreamStatus.ErrorCount
			r.FirstByteMs = int64(o.FRT)
			r.BillingSource = o.BillingSource
		}
		r.AnomalyTags = logChainAnomalyTags(r, quota)
		observations = append(observations, observation{r, logChainAttributeFaultWithEvidence(r, r.AnomalyTags)})
	}
	if rows.Err() != nil || int64(len(observations)) != expected {
		return
	}
	counts := map[ggMinuteKey]int64{}
	for _, o := range observations {
		k := ggMinuteKey{ggMetricKey{strings.TrimSpace(o.r.Group), strings.TrimSpace(o.r.ModelName), int(o.r.ChannelID)}, o.r.CreatedAt / 60 * 60}
		counts[k]++
	}
	for k, n := range counts {
		if p.rows[k] == nil || p.rows[k].Anomaly != n {
			return
		}
	}
	for _, o := range observations {
		p.observe(o.r, o.f)
	}
}

func (p *ggErrorProjection) publish(tx *gorm.DB, now int64) error {
	if len(p.rows) > 6000 {
		return fmt.Errorf("governance error minute batch exceeds 6000 rows")
	}
	rows := make([]GroupGovernanceErrorMinute, 0, len(p.rows))
	for k, r := range p.rows {
		// A concurrent source append or classification budget failure cannot
		// certify a partial numerator. Known complete populations are atomic.
		if r.FailedRead != r.Failed || r.AnomalyRead > r.Anomaly {
			continue
		}
		var reasons []ggErrorReason
		for _, v := range p.reasons[k] {
			reasons = append(reasons, *v)
		}
		sort.Slice(reasons, func(i, j int) bool { return reasons[i].RuleID+reasons[i].Reason < reasons[j].RuleID+reasons[j].Reason })
		b, err := json.Marshal(reasons)
		if err != nil {
			return err
		}
		if len(b) > 256*1024 {
			return fmt.Errorf("governance reason projection exceeds bound")
		}
		r.ReasonsJSON = string(b)
		r.CapturedAt = now
		rows = append(rows, *r)
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(rows, 100).Error
}
