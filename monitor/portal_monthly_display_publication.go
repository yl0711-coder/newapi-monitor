package monitor

// Private transaction adapter for the fixed-source series. Not an HTTP input,
// not a source collector, and deliberately not enabled by the automatic worker
// until that worker can retain/re-audit the entire supported source range.
import (
	"context"
	"crypto/rand"
	"encoding/json"
	"strings"
	"time"

	"gorm.io/gorm"
)

const monthlySeriesPublicationPrefix = monthlyFrozenPublicationPrefix + "series-v1:"

type monthlyDisplayPublicationSource struct {
	Evidence []byte
	Account  string
	Names    map[int64]string // independently retained names, not current user input
}

// Keep prepared facts and evidence digests private. A checksum alone is not
// evidence review: only the trusted source-preparation path may construct this.
type monthlyPreparedDisplayPublication struct {
	scope    monthlyDisplaySeriesScope
	sources  []monthlyDisplaySeriesSource
	evidence map[string]string
	plan     monthlyDisplaySeriesPlan
	cent     *monthlyBillCentSeries
}

func monthlyPrepareDisplayPublication(ctx context.Context, scope monthlyDisplaySeriesScope, input []monthlyDisplayPublicationSource, now time.Time) (*monthlyPreparedDisplayPublication, error) {
	prepared, err := monthlyPrepareDisplaySources(ctx, scope, input, now)
	if err != nil {
		return nil, err
	}
	plan, err := monthlyPlanDisplaySeries(ctx, scope, prepared.sources, nil)
	if err != nil {
		return nil, err
	}
	prepared.plan = plan
	return prepared, nil
}

func monthlyPrepareDisplaySources(ctx context.Context, scope monthlyDisplaySeriesScope, input []monthlyDisplayPublicationSource, now time.Time) (*monthlyPreparedDisplayPublication, error) {
	if ctx == nil {
		return nil, errMonthlyDisplayChainInvalid
	}
	if len(input) > monthlyDisplaySeriesMonthLimit {
		return nil, errMonthlyDisplaySeriesCapacity
	}
	prepared := &monthlyPreparedDisplayPublication{scope: scope, evidence: map[string]string{}}
	size := 0
	for _, source := range input {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		size += len(source.Evidence)
		if size > MonthlyPreparationInputLimit {
			return nil, errMonthlyDisplaySeriesCapacity
		}
		e, err := decodeMonthlyBillEvidence(source.Evidence)
		if err != nil {
			return nil, err
		}
		p, ids, err := buildPublishedMonthlyBill(e, now)
		if err != nil {
			return nil, err
		}
		if e.GroupID != scope.GroupID || len(source.Names) != len(ids) {
			return nil, errMonthlyDisplayChainInvalid
		}
		for _, id := range ids {
			if _, exists := source.Names[id]; !exists {
				return nil, errMonthlyDisplayChainInvalid
			}
		}
		p.Account = source.Account
		for i := range p.Members {
			p.Members[i].Username = source.Names[p.Members[i].UserID]
		}
		for i := range p.Credits.Rows {
			p.Credits.Rows[i].Username = source.Names[p.Credits.Rows[i].UserID]
		}
		raw, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		prepared.evidence[e.Month] = monthlyDigest(raw)
		prepared.sources = append(prepared.sources, monthlyDisplaySeriesSource{GroupID: scope.GroupID, SourceIdentity: scope.SourceIdentity, Statement: p})
	}
	return prepared, nil
}

func monthlySeriesPublicationID(p *monthlyPreparedDisplayPublication) string {
	if p.cent != nil {
		return monthlyCentBillPublicationPrefix + p.scope.From + ":" + p.scope.SourceIdentity + ":" + monthlyCentSeriesAnchor(*p.cent) + ":" + rand.Text()
	}
	return monthlySeriesPublicationPrefix + p.scope.From + ":" + p.scope.SourceIdentity + ":" + p.plan.AnchorSHA256 + ":" + rand.Text()
}

func monthlySeriesRecordAnchor(record PortalMonthlyBill, p *monthlyPreparedDisplayPublication) (string, error) {
	if !strings.HasPrefix(record.PublicationID, monthlySeriesPublicationPrefix) {
		return "", nil // compatible legacy must still match every source/cent below
	}
	parts := strings.Split(strings.TrimPrefix(record.PublicationID, monthlySeriesPublicationPrefix), ":")
	if len(parts) != 4 || parts[0] != p.scope.From || parts[1] != p.scope.SourceIdentity || parts[2] != p.plan.AnchorSHA256 || parts[3] == "" {
		return "", errMonthlyDisplaySeriesFrozen
	}
	return parts[2], nil
}

// Names are part of a retained source snapshot, not its authorization. A live
// rename must neither move the first source digest nor prevent an older month
// from being generated later. Current IDs/revisions are still checked by the
// enclosing transaction; only presentation labels come from the audited source.
func monthlyRetainPreparedDisplayNames(prepared *monthlyPreparedDisplayPublication, groupID int64, evidence string, target *monthlyStatement) error {
	if prepared == nil || target == nil || prepared.scope.GroupID != groupID || prepared.evidence[target.Month] != evidence {
		return errMonthlyDisplayChainInvalid
	}
	for _, source := range prepared.sources {
		if source.Statement.Month != target.Month {
			continue
		}
		names := map[int64]string{}
		for _, member := range source.Statement.Members {
			names[member.UserID] = member.Username
		}
		if len(names) != len(target.Members) {
			return errMonthlyDisplayChainInvalid
		}
		for i := range target.Members {
			name, exists := names[target.Members[i].UserID]
			if !exists {
				return errMonthlyDisplayChainInvalid
			}
			target.Members[i].Username = name
		}
		for i := range target.Credits.Rows {
			name, exists := names[target.Credits.Rows[i].UserID]
			if !exists {
				return errMonthlyDisplayChainInvalid
			}
			target.Credits.Rows[i].Username = name
		}
		target.Account = source.Statement.Account
		return nil
	}
	return errMonthlyDisplaySeriesIncomplete
}

// Called INSIDE the existing authorization + immutable publication transaction.
// Recheck ALL issued months, including disconnected endpoints, and canonical
// private evidence. Do not trust a preflight snapshot taken before the write.
// No schema change, source write, or rewrite of an earlier publication occurs.
func monthlyApplyPreparedDisplayPublication(ctx context.Context, tx *gorm.DB, groupID int64, targetEvidence string, target *monthlyStatement, prepared *monthlyPreparedDisplayPublication) error {
	if prepared == nil || target == nil || groupID != prepared.scope.GroupID || prepared.evidence[target.Month] != targetEvidence {
		return errMonthlyDisplayChainInvalid
	}
	if prepared.cent != nil {
		_, err := monthlyApplyPreparedCentPublication(ctx, tx, groupID, targetEvidence, target, prepared)
		return err
	}
	digest, err := monthlyDisplaySeriesDigest(prepared.plan)
	if err != nil || digest != prepared.plan.PlanSHA256 {
		return errMonthlyDisplayChainInvalid
	}
	rows, err := tx.Model(&PortalMonthlyBill{}).Where("group_id = ?", groupID).Order("month").Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	var frozen []monthlyDisplaySeriesFrozen
	size := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var record PortalMonthlyBill
		if err := tx.ScanRows(rows, &record); err != nil {
			return err
		}
		if strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) {
			return errMonthlyDisplaySeriesFrozen
		}
		size += len(record.Payload) + len(record.Evidence)
		if size > MonthlyPreparationInputLimit || len(frozen) >= monthlyDisplaySeriesMonthLimit {
			return errMonthlyDisplaySeriesCapacity
		}
		if prepared.evidence[record.Month] != record.EvidenceHash {
			return errMonthlyDisplaySeriesFrozen
		}
		if _, err := readStoredMonthlyEvidence(record); err != nil {
			return err
		}
		p, err := readPublishedMonthlyBill(record)
		if err != nil {
			return err
		}
		anchor, err := monthlySeriesRecordAnchor(record, prepared)
		if err != nil {
			return err
		}
		link, err := monthlyDisplayLinkFromBill(groupID, p)
		if err != nil {
			return err
		}
		frozen = append(frozen, monthlyDisplaySeriesFrozen{AnchorSHA256: anchor, Link: link})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	plan, err := monthlyPlanDisplaySeries(ctx, prepared.scope, prepared.sources, frozen)
	if err != nil {
		return err
	}
	if plan.PlanSHA256 != prepared.plan.PlanSHA256 {
		return errMonthlyDisplayChainInvalid
	}
	current := *target
	current.DisplayBalances = nil // only this transaction's already-built DTO
	display, err := monthlySelectDisplaySeries(ctx, plan, monthlyDisplaySeriesSource{GroupID: groupID, SourceIdentity: prepared.scope.SourceIdentity, Statement: current})
	if err != nil {
		return err
	}
	target.DisplayBalances = display
	return nil
}
