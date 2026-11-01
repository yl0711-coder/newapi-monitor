package monitor

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
)

func monthlyCentSeriesDigest(series monthlyBillCentSeries) string {
	series.Digest = ""
	raw, _ := json.Marshal(series)
	return monthlyDigest(raw)
}

func monthlyCentSeriesAnchor(series monthlyBillCentSeries) string {
	if len(series.Months) == 0 {
		return ""
	}
	raw, _ := json.Marshal(struct {
		Policy, Origin, From, First string
		GroupID                     int64
	}{
		monthlyCentBillFormat, series.SourceIdentity, series.From, series.Months[0].Digest, series.GroupID})
	return monthlyDigest(raw)
}

// PrepareMonthlyRegisteredDisplayPublication selects a format using only the
// local authority store. Existing series retain their policy; complete token
// evidence in a new series uses the new cent format. Malformed modern evidence
// never falls back to legacy or permits a mixed-format series.
func PrepareMonthlyRegisteredDisplayPublication(ctx context.Context, db *gorm.DB, scope MonthlyDisplaySeriesScope, input []MonthlyDisplayPublicationSource, now time.Time) (*PreparedMonthlyDisplayPublication, error) {
	return prepareMonthlyRegisteredDisplayPublication(ctx, db, scope, input, now, "", "")
}

// PrepareMonthlyRegisteredDisplayPublicationForBinding preserves a previously
// bound, unfinished server task across an upgrade. The hash is from the private
// recovery journal, never customer input; it must match a fully rebuilt plan.
func PrepareMonthlyRegisteredDisplayPublicationForBinding(ctx context.Context, db *gorm.DB, scope MonthlyDisplaySeriesScope, input []MonthlyDisplayPublicationSource, now time.Time, month, binding string) (*PreparedMonthlyDisplayPublication, error) {
	if !validMonthlyPublication(binding) {
		return nil, errMonthlyDisplayChainInvalid
	}
	return prepareMonthlyRegisteredDisplayPublication(ctx, db, scope, input, now, month, binding)
}

func prepareMonthlyRegisteredDisplayPublication(ctx context.Context, db *gorm.DB, scope MonthlyDisplaySeriesScope, input []MonthlyDisplayPublicationSource, now time.Time, month, binding string) (*PreparedMonthlyDisplayPublication, error) {
	if ctx == nil || db == nil {
		return nil, errMonthlyDisplayChainInvalid
	}
	p, err := monthlyPrepareDisplaySources(ctx, scope, input, now)
	if err != nil {
		return nil, err
	}
	var records []PortalMonthlyBill
	if err := db.WithContext(ctx).Select("publication_id").Where("group_id = ?", scope.GroupID).Limit(monthlyDisplaySeriesMonthLimit + 1).Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) > monthlyDisplaySeriesMonthLimit {
		return nil, errMonthlyDisplaySeriesCapacity
	}
	legacy, modern := false, false
	for _, record := range records {
		if strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) {
			modern = true
		} else {
			legacy = true
		}
	}
	tokens := true
	for _, source := range p.sources {
		tokens = tokens && source.Statement.TokenDetailsVersion == 1
	}
	if legacy && modern || modern && !tokens {
		return nil, errMonthlyDisplaySeriesFrozen
	}
	if binding != "" && !legacy && !modern && tokens {
		// An old task may have frozen a v1 display before the first write.
		// Try that exact binding, not a guessed policy or a reset retry budget.
		old, err := monthlyPlanDisplaySeries(ctx, scope, p.sources, nil)
		if err == nil {
			p.plan = old
			candidate := &PreparedMonthlyDisplayPublication{value: p}
			if got, err := monthlyPreparedSavedBinding(candidate, month); err == nil && got == binding {
				return candidate, nil
			}
			p.plan = monthlyDisplaySeriesPlan{}
		}
	}
	if legacy || !tokens {
		p.plan, err = monthlyPlanDisplaySeries(ctx, scope, p.sources, nil)
	} else {
		var series monthlyBillCentSeries
		series, err = monthlyPlanBillCentSeries(ctx, scope, p.sources, nil)
		p.cent = &series
	}
	if err != nil {
		return nil, err
	}
	result := &PreparedMonthlyDisplayPublication{value: p}
	if binding != "" {
		got, err := monthlyPreparedSavedBinding(result, month)
		if err != nil || got != binding {
			return nil, errMonthlyDisplaySeriesFrozen
		}
	}
	return result, nil
}

func monthlyPreparedSavedBinding(p *PreparedMonthlyDisplayPublication, month string) (string, error) {
	display, err := p.BindingSHA256(month)
	if err != nil {
		return "", err
	}
	raw, _ := json.Marshal(struct{ Mode, EvidenceSHA256, DisplaySHA256 string }{
		"fixed-origin-publication-v1", p.value.evidence[month], display})
	return monthlyDigest(raw), nil
}

// Validate a private prepared result without solving its allocation again.
// All source facts, cents equations and chronological boundaries are checked.
func monthlyVerifyPreparedCentSeries(ctx context.Context, p *monthlyPreparedDisplayPublication) error {
	if ctx == nil || p == nil || p.cent == nil {
		return errMonthlyDisplayChainInvalid
	}
	s := p.cent
	if s.Version != 1 || s.GroupID != p.scope.GroupID || s.SourceIdentity != p.scope.SourceIdentity ||
		s.From != p.scope.From || s.Through != p.scope.Through || s.Digest != monthlyCentSeriesDigest(*s) ||
		len(s.Months) == 0 || len(s.Months) > monthlyDisplaySeriesMonthLimit || len(p.sources) != len(s.Months) || len(p.evidence) != len(s.Months) {
		return errMonthlyDisplayChainInvalid
	}
	from, fok := monthlyDisplayChainMonth(s.From)
	through, tok := monthlyDisplayChainMonth(s.Through)
	if !fok || !tok || !validMonthlyPublication(s.SourceIdentity) || s.GroupID <= 0 || through.Before(from) ||
		(through.Year()-from.Year())*12+int(through.Month()-from.Month())+1 != len(s.Months) {
		return errMonthlyDisplayChainInvalid
	}
	sources := make(map[string]monthlyStatement, len(p.sources))
	for _, source := range p.sources {
		if source.GroupID != s.GroupID || source.SourceIdentity != s.SourceIdentity {
			return errMonthlyDisplayChainInvalid
		}
		if _, duplicate := sources[source.Statement.Month]; duplicate {
			return errMonthlyDisplayChainInvalid
		}
		sources[source.Statement.Month] = source.Statement
	}
	for i, month := range s.Months {
		if err := ctx.Err(); err != nil {
			return err
		}
		source, exists := sources[month.Month]
		if !exists || month.Month != from.AddDate(0, i, 0).Format("2006-01") || month.GroupID != s.GroupID || month.SourceIdentity != s.SourceIdentity ||
			!validMonthlyPublication(p.evidence[month.Month]) || !monthlyBillCentMatchesSource(source, month) {
			return errMonthlyDisplayChainInvalid
		}
		if i == 0 {
			opening, _ := monthlyRoundedDisplayUnits(*source.MonthOpeningBalance, 2)
			if monthlyDisplayCentsMoney(opening) != month.Organization.Opening {
				return errMonthlyDisplayChainInvalid
			}
			for _, member := range month.Members {
				raw, err := monthlyCentParse(member.Wallet.SourceOpening)
				if err != nil {
					return err
				}
				if _, err := monthlyCentTarget(raw, member.Wallet.Opening); err != nil {
					return err
				}
			}
		} else {
			checked := month
			checked.Members = append([]monthlyBillCentMember(nil), month.Members...)
			if err := monthlyBillCentOpenings(&checked, nil, &s.Months[i-1]); err != nil {
				return err
			}
			if checked.Organization.Opening != month.Organization.Opening {
				return errMonthlyDisplayChainConflict
			}
			for j, member := range month.Members {
				if member.Wallet.Opening != checked.Members[j].Wallet.Opening {
					return errMonthlyDisplayChainConflict
				}
			}
		}
	}
	return ctx.Err()
}

func monthlyApplyPreparedCentPublication(ctx context.Context, tx *gorm.DB, gid int64, evidence string, target *monthlyStatement, p *monthlyPreparedDisplayPublication) (monthlyBillCentMonth, error) {
	var empty monthlyBillCentMonth
	if p == nil || target == nil || p.scope.GroupID != gid || p.evidence[target.Month] != evidence {
		return empty, errMonthlyDisplayChainInvalid
	}
	if err := monthlyVerifyPreparedCentSeries(ctx, p); err != nil {
		return empty, err
	}
	byMonth := make(map[string]monthlyBillCentMonth, len(p.cent.Months))
	for _, month := range p.cent.Months {
		byMonth[month.Month] = month
	}
	rows, err := tx.Model(&PortalMonthlyBill{}).Where("group_id = ?", gid).Order("month").Rows()
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	size, count := 0, 0
	anchor := monthlyCentSeriesAnchor(*p.cent)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		var record PortalMonthlyBill
		if err := tx.ScanRows(rows, &record); err != nil {
			return empty, err
		}
		size += len(record.Payload) + len(record.Evidence)
		count++
		if size > MonthlyPreparationInputLimit || count > monthlyDisplaySeriesMonthLimit {
			return empty, errMonthlyDisplaySeriesCapacity
		}
		parts := strings.Split(strings.TrimPrefix(record.PublicationID, monthlyCentBillPublicationPrefix), ":")
		if !strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) || len(parts) != 4 ||
			parts[0] != p.scope.From || parts[1] != p.scope.SourceIdentity || parts[2] != anchor || parts[3] == "" ||
			p.evidence[record.Month] != record.EvidenceHash {
			return empty, errMonthlyDisplaySeriesFrozen
		}
		if _, err := readStoredMonthlyEvidence(record); err != nil {
			return empty, err
		}
		stored, err := monthlyReadCentBill(record)
		if err != nil {
			return empty, err
		}
		wanted, exists := byMonth[record.Month]
		if !exists || !reflect.DeepEqual(wanted, stored.Plan) {
			return empty, errMonthlyDisplaySeriesFrozen
		}
	}
	if err := rows.Err(); err != nil {
		return empty, err
	}
	if err := rows.Close(); err != nil {
		return empty, err
	}
	selected, exists := byMonth[target.Month]
	if !exists || !monthlyBillCentMatchesSource(*target, selected) {
		return empty, errMonthlyDisplayChainInvalid
	}
	return selected, nil
}
