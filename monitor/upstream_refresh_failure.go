package monitor

import (
	"errors"
	"net/http"
)

// A refresh request is still an upstream request. A timeout, rate limit or
// server error does not prove that the stored credential is invalid. Preserve
// the original typed error (including RetryAt) for the shared retry policy.
// Unknown 4xx/response-shape errors are not authentication evidence either.
func upstreamRefreshFailure(err error) error {
	if err == nil {
		return nil
	}
	var status *upstreamHTTPError
	if errors.As(err, &status) && (status.Status == http.StatusUnauthorized || status.Status == http.StatusForbidden) {
		return &upstreamAuthError{err: err}
	}
	return err
}
