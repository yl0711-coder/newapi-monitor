package monthlydraft

import "time"

// Only fixes STATUS precedence after a trusted source selection already exists.
// Not permission to resume a failed queue, renew its age/work/retry budget,
// repair missing inputs, activate roots, submit or publish a bill. Integrity,
// money, descriptor and authority failures keep their existing hard stop.
func selectedHandoffFailure(s preparationFailedSnapshot, now time.Time) bool {
	if s.State != "failed" || s.CreatedMS <= 0 || s.CreatedMS > now.UnixMilli() ||
		s.NextMS < s.CreatedMS || s.NextMS > now.UnixMilli()+30000 ||
		s.Steps < 0 || s.Steps > 4096 || s.Failures < 0 || s.Failures > 3 || s.Lease != "" || s.LeaseUntilMS != 0 {
		return false
	}
	switch s.Code {
	case "temporarily_busy", "temporarily_unavailable", "lease_expired":
		return s.Failures == 3 && s.Steps >= 3
	case "budget_exhausted":
		return s.Steps == 4096 || now.UnixMilli()-s.CreatedMS >= (24*time.Hour).Milliseconds()
	default:
		return false
	}
}
