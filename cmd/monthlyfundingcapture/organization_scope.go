package main

import (
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Mutually exclusive with manually selected member IDs. This only pins current
// collection scope; generation STILL rereads authority and historical evidence.
func organizationInitScope(path, expected string, group int64, month, source, users string) (int64, monthlysource.FundingMonthRequest, error) {
	if path == "" && expected == "" {
		ids, err := parseIDs(users)
		return group, monthlysource.FundingMonthRequest{Month: month, SourceIdentity: source, UserIDs: ids,
			Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}}, err
	}
	if path == "" || !validSHA(expected) || group != 0 || month != "" || source != "" || users != "" {
		return 0, monthlysource.FundingMonthRequest{}, errInput
	}
	raw, err := privateRead(path, monthlysource.OrganizationMonthScopeLimit)
	if err != nil || hash(raw) != expected {
		return 0, monthlysource.FundingMonthRequest{}, errInput
	}
	scope, err := monthlysource.DecodeOrganizationMonthScope(raw, time.Now())
	if err != nil {
		return 0, monthlysource.FundingMonthRequest{}, errInput
	}
	return scope.GroupID, scope.Funding, nil
}
