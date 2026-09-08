// Package trafficclass owns the persisted user/internal-test classification
// version shared by the private monitor and the isolated public status package.
package trafficclass

// Current is bumped whenever an older local aggregate can no longer be safely
// interpreted with the current user-traffic classification rules.
// v4 removed the dependency on a modified producer protocol. v5 makes the
// success marker NULL-safe: SQL three-valued logic previously turned
// NOT(marker) into NULL when token_name/content was NULL and silently dropped
// legitimate type=2 traffic. Delivery/anomaly rules have an independent
// Monitor-local version: changing them must never invalidate user usage facts.
const Current = 5

// DeliveryCurrent versions model/stability delivery outcomes. It is separate
// from Current because anomaly and zero-output rules may change without
// changing which source rows belong to real users.
const DeliveryCurrent = 6

// SourceExclusionPredicateSQL is the portable SQL boundary for traffic that
// an unmodified NewAPI emits while testing channels internally. Both the
// production facts reader and the isolated read-only parity tool use this
// exact predicate so a control check cannot accidentally compare different
// traffic classes. Keep it valid in production MySQL and the SQLite fake
// source used by acceptance tests.
//
// The two legacy text markers must both match for successful tests. Failed
// legacy tests use the fixed synthetic root/no-token/no-request-id shape.
const SourceExclusionPredicateSQL = `((type=2 AND COALESCE(token_name,'')='模型测试' AND COALESCE(content,'')='模型测试') OR ` +
	`(type=5 AND user_id=1 AND COALESCE(token_id,0)=0 AND COALESCE(token_name,'')='' AND COALESCE(request_id,'')=''))`
