package monitor

// New-version preparation, separate from legacy v1 planning. The publication
// adapter freezes this exact plan, rechecks source/authorization, and refuses
// to overwrite or mix an issued v1 bill.
import (
	"context"
	"encoding/json"
	"reflect"
)

type monthlyBillCentSeries struct {
	Version        int                    `json:"version"`
	GroupID        int64                  `json:"group_id"`
	SourceIdentity string                 `json:"source_identity"`
	From           string                 `json:"from"`
	Through        string                 `json:"through"`
	Months         []monthlyBillCentMonth `json:"months"`
	Digest         string                 `json:"digest"`
}

func monthlyPlanBillCentSeries(ctx context.Context, scope monthlyDisplaySeriesScope, sources []monthlyDisplaySeriesSource, frozen []monthlyBillCentMonth) (monthlyBillCentSeries, error) {
	var empty monthlyBillCentSeries
	if ctx == nil {
		return empty, errMonthlyCentAllocation
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	from, fromOK := monthlyDisplayChainMonth(scope.From)
	through, throughOK := monthlyDisplayChainMonth(scope.Through)
	if scope.GroupID <= 0 || !validMonthlyPublication(scope.SourceIdentity) || !fromOK || !throughOK || through.Before(from) {
		return empty, errMonthlyCentAllocation
	}
	count := (through.Year()-from.Year())*12 + int(through.Month()-from.Month()) + 1
	if count > monthlyDisplaySeriesMonthLimit || len(sources) > monthlyDisplaySeriesMonthLimit || len(frozen) > monthlyDisplaySeriesMonthLimit {
		return empty, errMonthlyDisplaySeriesCapacity
	}
	if len(sources) != count {
		return empty, errMonthlyDisplaySeriesIncomplete
	}
	byMonth := make(map[string]monthlyStatement, count)
	inputSize := 0
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		p := source.Statement
		date, ok := monthlyDisplayChainMonth(p.Month)
		if source.GroupID != scope.GroupID || source.SourceIdentity != scope.SourceIdentity || !ok || date.Before(from) || date.After(through) {
			return empty, errMonthlyCentAllocation
		}
		if _, duplicate := byMonth[p.Month]; duplicate {
			return empty, errMonthlyCentAllocation
		}
		raw, err := json.Marshal(p)
		if err != nil {
			return empty, err
		}
		inputSize += len(raw)
		if len(raw) > 2<<20 || inputSize > MonthlyPreparationInputLimit {
			return empty, errMonthlyDisplaySeriesCapacity
		}
		byMonth[p.Month] = p
	}
	result := monthlyBillCentSeries{Version: 1, GroupID: scope.GroupID, SourceIdentity: scope.SourceIdentity,
		From: scope.From, Through: scope.Through, Months: make([]monthlyBillCentMonth, 0, count)}
	outputSize := 0
	for month := from; !month.After(through); month = month.AddDate(0, 1, 0) {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		p, exists := byMonth[month.Format("2006-01")]
		if !exists {
			return empty, errMonthlyDisplaySeriesIncomplete
		}
		var previous *monthlyBillCentMonth
		if len(result.Months) > 0 {
			previous = &result.Months[len(result.Months)-1]
		}
		plan, err := monthlyPlanBillCentMonth(scope.GroupID, scope.SourceIdentity, p, previous)
		if err != nil {
			return empty, err
		}
		raw, err := json.Marshal(plan)
		if err != nil {
			return empty, err
		}
		outputSize += len(raw)
		if outputSize > MonthlyPreparationInputLimit {
			return empty, errMonthlyDisplaySeriesCapacity
		}
		result.Months = append(result.Months, plan)
	}
	seen := make(map[string]bool, len(frozen))
	for _, issued := range frozen {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		date, ok := monthlyDisplayChainMonth(issued.Month)
		if !ok || date.Before(from) || date.After(through) || seen[issued.Month] {
			return empty, errMonthlyDisplaySeriesFrozen
		}
		index := (date.Year()-from.Year())*12 + int(date.Month()-from.Month())
		if !reflect.DeepEqual(issued, result.Months[index]) {
			return empty, errMonthlyDisplaySeriesFrozen
		}
		seen[issued.Month] = true
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return empty, err
	}
	if len(raw) > MonthlyPreparationInputLimit {
		return empty, errMonthlyDisplaySeriesCapacity
	}
	result.Digest = monthlyDigest(raw)
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}
