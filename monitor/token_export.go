package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type tokenExportSelection struct {
	LogID        int64  `json:"log_id"`
	Revision     int64  `json:"revision"`
	DecisionHash string `json:"decision_hash"`
}
type tokenExportRequest struct {
	RequestID              string                 `json:"request_id"`
	TargetID               string                 `json:"target_id"`
	SourceEpoch            string                 `json:"source_epoch"`
	From                   int64                  `json:"from"`
	To                     int64                  `json:"to"`
	IncludeCustomerPricing bool                   `json:"include_customer_pricing"`
	Selection              []tokenExportSelection `json:"selection"`
}

func tokenDeliveryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 5*time.Second)
}
func tokenExportRequestValid(q tokenExportRequest) bool {
	if !tokenMeteringName.MatchString(q.RequestID) || !tokenMeteringName.MatchString(q.TargetID) || !tokenMeteringName.MatchString(q.SourceEpoch) || q.From <= 0 || q.To <= q.From || q.To-q.From > 7*86400 || len(q.Selection) == 0 || len(q.Selection) > tokenExportMaxRequests {
		return false
	}
	seen := map[int64]bool{}
	for _, s := range q.Selection {
		if s.LogID <= 0 || s.Revision <= 0 || len(s.DecisionHash) != 64 || seen[s.LogID] {
			return false
		}
		if _, err := hex.DecodeString(s.DecisionHash); err != nil {
			return false
		}
		seen[s.LogID] = true
	}
	return true
}
func tokenExportTargetFor(p tokenExportPolicy, q tokenExportRequest) (tokenExportTarget, error) {
	for _, t := range p.Targets {
		if t.ID == q.TargetID && t.SourceEpoch == q.SourceEpoch && q.From >= t.From && q.To <= t.To && (!q.IncludeCustomerPricing || t.AllowCustomerPricing) {
			return t, nil
		}
	}
	return tokenExportTarget{}, errTokenExportScope
}
func tokenExportContains(t tokenExportTarget, id int64) bool {
	for _, v := range t.ChannelIDs {
		if v == id {
			return true
		}
	}
	return false
}
func tokenExportAlias(salt []byte, kind string, v any) string {
	h := hmac.New(sha256.New, salt)
	_, _ = fmt.Fprintf(h, "%s:%v", kind, v)
	return kind + "-" + hex.EncodeToString(h.Sum(nil))[:16]
}
func tokenExportCell(s string) string {
	if strings.ContainsAny(strings.TrimLeft(s, " \t\r\n"), "\x00") {
		return "[invalid]"
	}
	trimmed := strings.TrimLeft(s, " \t\r\n")
	if trimmed != "" && strings.ContainsAny(trimmed[:1], "=+-@") {
		return "'" + s
	}
	if strings.ContainsAny(s, "\t\r\n") {
		return "'" + s
	}
	return s
}
func tokenExportCSV(rows [][]string) ([]byte, error) {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	for _, row := range rows {
		out := make([]string, len(row))
		for i, v := range row {
			out[i] = tokenExportCell(v)
		}
		if err := w.Write(out); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return b.Bytes(), w.Error()
}
func tokenExportUsage(u tokenMeteringParsed, pricing bool) map[string]any {
	fields := map[string]any{}
	keys := []string{"prompt_tokens", "completion_tokens", "cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h", "is_stream"}
	if pricing {
		keys = append(keys, "quota", "billing_mode", "billing_source", "currency", "quota_per_unit", "rounding_mode", "model_price", "model_ratio", "effective_model_ratio", "effective_group_ratio", "completion_ratio", "cache_ratio", "cache_creation_ratio", "cache_creation_ratio_5m", "cache_creation_ratio_1h", "expr_b64", "tiered_expr", "image", "is_task", "request_rules")
	}
	for _, key := range keys {
		if f, ok := u.Fields[key]; ok {
			fields[key] = f
		}
	}
	return map[string]any{"parser_version": u.Version, "semantic": u.Semantic, "semantic_reason": u.SemanticReason, "adapter": u.Adapter, "total_input": u.TotalInput, "fields": fields}
}
func tokenExportEvidence(f tokenDetectionFrozen, s tokenExportSelection, salt []byte, pricing bool) map[string]any {
	in, r := f.Input, f.Result
	refs := []map[string]any{}
	for _, v := range r.Reference.Samples {
		refs = append(refs, map[string]any{"sample": tokenExportAlias(salt, "sample", fmt.Sprint(v.Epoch, ":", v.LogID)), "channel": tokenExportAlias(salt, "comparison-channel", v.ChannelID), "created_at": v.CreatedAt, "request_length": v.Length, "input": v.Input, "read_share": v.ReadShare})
	}
	out := map[string]any{"request": tokenExportAlias(salt, "request", fmt.Sprint(in.Fact.SourceEpoch, ":", in.Fact.LogID)), "customer": tokenExportAlias(salt, "customer", in.Fact.UserID), "channel_id": in.Fact.ChannelID, "created_at": in.Fact.CreatedAt, "model": in.Fact.Model, "revision": s.Revision, "decision_hash": s.DecisionHash, "upstream_request_id": nil, "upstream_request_id_reason": "not_collected_cannot_reverse_hmac", "usage": tokenExportUsage(in.Usage, pricing), "entry": map[string]any{"request_length": in.Fact.RequestLength, "length_state": in.Fact.LengthState, "meaning": "whole_http_request_not_body", "link_state": in.Fact.LinkState, "link_reason": in.Fact.LinkReason, "evidence_level": in.Fact.EvidenceLevel}, "reference": map[string]any{"status": r.Reference.Status, "reason": r.Reference.Reason, "from": r.Reference.From, "through": r.Reference.Through, "upper": r.Reference.Upper, "sample_count": r.Reference.SampleCount, "samples": refs, "gaps": r.Reference.Gaps, "truncated": r.Reference.Truncated}, "rules": r.Rules, "policy": f.Policy, "candidate": r.Candidate, "source_complete": r.SourceComplete, "gaps": r.Gaps, "pricing_disclosure": pricing, "conclusion": "candidate_or_logged_difference_not_confirmed_upstream_overcharge"}
	if pricing {
		out["price"] = r.Price
		out["reference_price"] = r.ReferencePrice
		out["actual_quota"] = r.ActualQuota
		out["formula_difference_quota"] = r.FormulaDifference
		out["estimated_loss_quota"] = r.EstimatedLoss
		out["estimate_reason"] = r.EstimateReason
	} else {
		// Rule operands are administrator diagnostics. Keep the pre-existing
		// supplier export boundary when customer pricing is not authorized.
		rules := append([]tokenDetectionRule(nil), r.Rules...)
		for i := range rules {
			rules[i].Operands, rules[i].Missing = nil, nil
		}
		out["rules"] = rules
	}
	return out
}
func tokenExportPackage(q tokenExportRequest, p tokenExportPolicy, frozen []tokenDetectionFrozen, state TokenMeteringState) ([]byte, string, error) {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, "", err
	}
	evidence := []map[string]any{}
	heads := []TokenDetectionCurrent{}
	details := [][]string{{"request", "customer", "utc", "beijing", "channel_id", "model", "revision", "candidate", "request_length_whole_http", "actual_quota", "formula_difference_quota", "estimated_loss_quota", "pricing_note"}}
	for i, f := range frozen {
		s := q.Selection[i]
		e := tokenExportEvidence(f, s, salt, q.IncludeCustomerPricing)
		evidence = append(evidence, e)
		h := f.Input.Fact
		heads = append(heads, TokenDetectionCurrent{UserID: h.UserID, ChannelID: h.ChannelID, Model: h.Model, UserGroup: h.UserGroup, ResultJSON: tokenDetectionJSON(f.Result)})
		length := "unavailable"
		if h.RequestLength != nil {
			length = strconv.FormatInt(*h.RequestLength, 10)
		}
		money := []string{"withheld", "withheld", "withheld", "customer pricing withheld"}
		if q.IncludeCustomerPricing {
			money = []string{"unavailable", "unavailable", "unavailable", "customer wallet; not upstream loss"}
			for j, v := range []*string{f.Result.ActualQuota, f.Result.FormulaDifference, f.Result.EstimatedLoss} {
				if v != nil {
					money[j] = *v
				}
			}
		}
		at := time.Unix(h.CreatedAt, 0)
		details = append(details, append([]string{e["request"].(string), e["customer"].(string), at.UTC().Format(time.RFC3339), at.In(time.FixedZone("CST", 8*3600)).Format(time.RFC3339), strconv.FormatInt(h.ChannelID, 10), h.Model, strconv.FormatInt(s.Revision, 10), strconv.FormatBool(f.Result.Candidate), length}, money...))
	}
	summary, err := tokenDetectionSummarize(heads)
	if err != nil {
		return nil, "", err
	}
	sums := [][]string{{"metric", "count", "subject", "unit", "quota_per_unit", "method", "quota", "amount_fraction"}}
	sums = append(sums, []string{"selected_requests", strconv.Itoa(len(frozen)), "", "", "", "", "", ""}, []string{"candidate_requests", strconv.FormatInt(summary.Candidates, 10), "", "", "", "", "", ""})
	if q.IncludeCustomerPricing {
		for _, a := range summary.Amounts {
			amount := "unavailable"
			if a.Amount != nil {
				amount = *a.Amount
			}
			sums = append(sums, []string{"amount", strconv.FormatInt(a.Count, 10), a.Subject, a.Unit, a.QuotaPerUnit, a.Method, a.Quota, amount})
		}
		for reason, count := range summary.Unavailable {
			sums = append(sums, []string{"unavailable", strconv.FormatInt(count, 10), "", "", "", reason, "", ""})
		}
	} else {
		sums = append(sums, []string{"pricing_withheld", strconv.Itoa(len(frozen)), "", "", "", "", "", ""})
	}
	d, err := tokenExportCSV(details)
	if err != nil {
		return nil, "", err
	}
	s, err := tokenExportCSV(sums)
	if err != nil {
		return nil, "", err
	}
	ev, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return nil, "", err
	}
	files := map[string][]byte{"details.csv": d, "summary.csv": s, "evidence.json": ev, "README.txt": []byte("Token metering review evidence. This package covers only the explicitly selected frozen revisions and authorized supplier scope, not every request in the time range. Candidates and statistical references do not confirm overcharging. request_length is the entire HTTP request, not body size. Customer wallet pricing, if explicitly authorized, is not supplier wallet cost. Formula differences and reference estimates must not be added. Missing operands and evidence remain unavailable. Supplier request identifiers are not collected; internal HMAC cannot be reversed. No source scans or external delivery were performed.\n")}
	checks := map[string]string{}
	raw := 0
	for name, data := range files {
		h := sha256.Sum256(data)
		checks[name] = hex.EncodeToString(h[:])
		raw += len(data)
	}
	manifest := map[string]any{"version": tokenDeliveryVersion, "target_id": q.TargetID, "policy_version": p.Version, "policy_hash": tokenDetectionHash(p), "from": q.From, "to_exclusive": q.To, "selected_requests": len(frozen), "scope": "explicit_selected_revisions_only", "source_complete_through": state.CompleteThrough, "source_range_complete": state.SourceMode == "complete" && state.StartAt <= q.From && state.ExpiredBefore <= q.From && state.CompleteThrough >= q.To, "redaction": "package_scoped_customer_and_comparison_aliases_no_source_ids", "customer_pricing_included": q.IncludeCustomerPricing, "files_sha256": checks}
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, "", err
	}
	files["manifest.json"] = mb
	raw += len(mb)
	if raw > tokenExportMaxRawBytes {
		return nil, "", errTokenDeliveryCapacity
	}
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	for _, name := range []string{"README.txt", "details.csv", "summary.csv", "evidence.json", "manifest.json"} {
		w, e := zw.Create(name)
		if e != nil {
			return nil, "", e
		}
		if _, e = w.Write(files[name]); e != nil {
			return nil, "", e
		}
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}
	if out.Len() > tokenExportMaxBytes {
		return nil, "", errTokenDeliveryCapacity
	}
	return out.Bytes(), string(mb), nil
}
func (m *Monitor) createTokenExport(ctx context.Context, actor string, q tokenExportRequest, now int64) (TokenEvidenceExport, error) {
	var out TokenEvidenceExport
	if !m.cfg.TokenExportsEnabled || m.tokenStore() == nil || !tokenExportRequestValid(q) {
		return out, errTokenExportScope
	}
	p, err := parseTokenExportPolicy(m.cfg.TokenExportPolicy)
	if err != nil {
		return out, errTokenExportScope
	}
	target, err := tokenExportTargetFor(p, q)
	if err != nil {
		return out, err
	}
	id := tokenDetectionHash([]string{"export", actor, q.RequestID})
	hash := tokenDetectionHash(q)
	policyHash := tokenDetectionHash(p)
	err = m.tokenStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("id = ?", id).Limit(1).Find(&out).Error; err != nil {
			return err
		}
		if out.ID != "" {
			if out.Owner != actor || out.RequestHash != hash || out.PolicyHash != policyHash || out.ExpiresAt <= now {
				return errTokenExportConflict
			}
			return nil
		}
		var count, active, size int64
		if err := tx.Model(&TokenEvidenceExport{}).Where("owner = ? AND created_at > ?", actor, now-3600).Count(&count).Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenEvidenceExport{}).Where("expires_at > ?", now).Count(&active).Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenEvidenceExport{}).Select("COALESCE(SUM(length(payload)),0)").Scan(&size).Error; err != nil {
			return err
		}
		if count >= 10 || active >= 25 || size >= tokenExportMaxStoredBytes {
			return errTokenDeliveryCapacity
		}
		frozen := []tokenDetectionFrozen{}
		rawBytes := 0
		for _, sel := range q.Selection {
			var row TokenDetectionRevision
			if err := tx.Where("source_epoch = ? AND log_id = ? AND revision = ?", q.SourceEpoch, sel.LogID, sel.Revision).First(&row).Error; err != nil {
				return err
			}
			if row.DecisionHash != sel.DecisionHash || row.ExpiresAt <= now {
				return errTokenExportConflict
			}
			rawBytes += len(row.FrozenJSON)
			if rawBytes > tokenExportMaxRawBytes {
				return errTokenDeliveryCapacity
			}
			var f tokenDetectionFrozen
			if err := json.Unmarshal([]byte(row.FrozenJSON), &f); err != nil {
				return err
			}
			h := f.Input.Fact
			if h.SourceEpoch != q.SourceEpoch || h.LogID != sel.LogID || h.CreatedAt < q.From || h.CreatedAt >= q.To || !tokenExportContains(target, h.ChannelID) {
				return errTokenExportScope
			}
			frozen = append(frozen, f)
		}
		var state TokenMeteringState
		if err := tx.Where("source_epoch = ?", q.SourceEpoch).Limit(1).Find(&state).Error; err != nil {
			return err
		}
		if state.SourceEpoch != "" {
			if err := recomputeTokenMeteringComplete(tx, &state); err != nil {
				return err
			}
		}
		payload, manifest, err := tokenExportPackage(q, p, frozen, state)
		if err != nil {
			return err
		}
		if size+int64(len(payload)) > tokenExportMaxStoredBytes {
			return errTokenDeliveryCapacity
		}
		digest := sha256.Sum256(payload)
		out = TokenEvidenceExport{ID: id, Owner: actor, RequestHash: hash, TargetID: q.TargetID, PolicyHash: policyHash, CreatedAt: now, ExpiresAt: now + tokenExportTTL, Count: len(frozen), Bytes: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]), Manifest: manifest, Payload: payload}
		if err := tx.Create(&out).Error; err != nil {
			return err
		}
		return tokenDeliveryAudit(tx, actor, "export_created", id, "explicit_scope_and_revisions", now)
	})
	return out, err
}
func (m *Monitor) serveTokenExportCreate(c *gin.Context) {
	if !m.tokenDeliveryWrite(c) {
		return
	}
	var q tokenExportRequest
	if !tokenDeliveryBody(c, &q, 32<<10) {
		return
	}
	if !tokenExportRequestValid(q) {
		c.JSON(400, gin.H{"error": "需指定供应商、时间与最多 100 条冻结修订"})
		return
	}
	ctx, cancel := tokenDeliveryContext(c.Request.Context())
	defer cancel()
	actor := c.GetString("uname")
	out, err := m.createTokenExport(ctx, actor, q, time.Now().Unix())
	if err != nil {
		if m.tokenStore() != nil {
			if auditErr := m.tokenStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				return tokenDeliveryAudit(tx, actor, "export_rejected", tokenDetectionHash(q), "scope_version_or_capacity_rejected", time.Now().Unix())
			}); auditErr != nil {
				err = auditErr
			}
		}
		tokenDeliveryError(c, err)
		return
	}
	c.JSON(200, out)
}
func (m *Monitor) readTokenExport(ctx context.Context, actor, id string, now int64) (TokenEvidenceExport, error) {
	if m.tokenStore() == nil {
		return TokenEvidenceExport{}, errTokenExportScope
	}
	return m.readTokenExportDB(m.tokenStore().WithContext(ctx), actor, id, now)
}
func (m *Monitor) readTokenExportDB(db *gorm.DB, actor, id string, now int64) (TokenEvidenceExport, error) {
	var row TokenEvidenceExport
	if !m.cfg.TokenExportsEnabled || len(id) != 64 || m.tokenStore() == nil {
		return row, errTokenExportScope
	}
	if err := db.First(&row, "id = ? AND owner = ?", id, actor).Error; err != nil {
		return row, err
	}
	p, err := parseTokenExportPolicy(m.cfg.TokenExportPolicy)
	if err != nil || row.PolicyHash != tokenDetectionHash(p) {
		return row, errTokenExportScope
	}
	if row.ExpiresAt <= now || len(row.Payload) == 0 {
		return row, errTokenExportConflict
	}
	return row, nil
}
func (m *Monitor) serveTokenExportStatus(c *gin.Context) {
	ctx, cancel := tokenDeliveryContext(c.Request.Context())
	defer cancel()
	out, err := m.readTokenExport(ctx, c.GetString("uname"), c.Param("id"), time.Now().Unix())
	if err != nil {
		tokenDeliveryError(c, err)
		return
	}
	c.JSON(200, out)
}
func (m *Monitor) serveTokenExportDownload(c *gin.Context) {
	if !m.tokenDeliveryWrite(c) {
		return
	}
	var q struct{}
	if !tokenDeliveryBody(c, &q, 128) {
		return
	}
	ctx, cancel := tokenDeliveryContext(c.Request.Context())
	defer cancel()
	actor := c.GetString("uname")
	now := time.Now().Unix()
	out, err := m.downloadTokenExport(ctx, actor, c.Param("id"), now)
	if err != nil {
		tokenDeliveryError(c, err)
		return
	}
	c.Header("Content-Disposition", "attachment; filename=token-evidence-"+out.ID[:16]+".zip")
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("X-Content-SHA256", out.SHA256)
	c.Data(200, "application/zip", out.Payload)
}

func (m *Monitor) downloadTokenExport(ctx context.Context, actor, id string, now int64) (TokenEvidenceExport, error) {
	var out TokenEvidenceExport
	if m.tokenStore() == nil {
		return out, errTokenExportScope
	}
	err := m.tokenStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		out, err = m.readTokenExportDB(tx, actor, id, now)
		if err != nil {
			return err
		}
		var count int64
		if err = tx.Model(&TokenDeliveryAudit{}).Where("actor = ? AND action = ? AND at > ?", actor, "export_downloaded", now-60).Count(&count).Error; err != nil {
			return err
		}
		if count >= 10 {
			return errTokenDeliveryCapacity
		}
		return tokenDeliveryAudit(tx, actor, "export_downloaded", out.ID, "download_response_started", now)
	})
	return out, err
}
