package monitor

import "strings"

// Pre-route rejection classification shared by the CloudWatch pre-route
// rejection lane, the CloudWatch Nginx source and the rejection alert query.
// Moved here unchanged from the retired CloudWatch Shadow reconciliation.

func canonicalRejectionReason(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	lower = strings.NewReplacer("-", "_", " ", "_", ".", "_").Replace(lower)
	switch lower {
	case "no_channel", "no_available_channel", "route_no_channel", "noavailablechannel":
		return "route_no_channel"
	case "invalid_token", "token_invalid":
		return "invalid_token"
	case "token_disabled", "disabled_token":
		return "token_disabled"
	case "model_forbidden", "token_model_forbidden":
		return "model_forbidden"
	case "model_not_found", "unknown_model":
		return "model_not_found"
	case "quota_account", "quota_insufficient", "insufficient_quota", "user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed":
		return "quota_account"
	case "rate_limited", "rate_limit", "too_many_requests":
		return "rate_limited"
	default:
		return lower
	}
}

func isPreRouteRejectionCategory(category string) bool {
	switch canonicalRejectionReason(category) {
	case "route_no_channel", "invalid_token", "token_disabled", "model_forbidden", "model_not_found", "quota_account", "rate_limited":
		return true
	default:
		return false
	}
}

// lastUpstreamStatus returns the final upstream status of a retried request.
func lastUpstreamStatus(values []int) int {
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}
