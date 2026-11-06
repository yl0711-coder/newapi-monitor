package monthlysource

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Source-reviewed rc26 controller/channel.go, controller/option.go and the
// corresponding middleware fallback. Exact actions and parameter shapes only;
// no channel.* prefix, textual money heuristics or generic-route catch-all.
func fundingResourceAudit(row FundingLog, envelope map[string]json.RawMessage, action string, raw json.RawMessage) bool {
	// rc26 model.buildOpField omits params when the map is empty. Absence is
	// accepted only for this reviewed no-argument route, never for generic or
	// wallet operations; explicit null still goes through strict rejection.
	if action == "option.payment_compliance" && len(raw) == 0 {
		return fundingResourceRouteAudit(row, envelope, "POST", "/api/option/payment_compliance", false)
	}
	p, ok := fundingObject(string(raw), false)
	if !ok {
		return false
	}
	switch action {
	case "channel.status_update":
		var changed bool
		_, id := fundingID(p["id"])
		status, validStatus := fundingID(p["status"])
		return fundingAuditFields(p, "id", "status", "changed") && id && validStatus && (status == 1 || status == 2) &&
			string(p["changed"]) != "null" && json.Unmarshal(p["changed"], &changed) == nil
	case "channel.update":
		_, id := fundingID(p["id"])
		_, name := fundingString(p["name"])
		var fields []string
		if !fundingAuditFields(p, "id", "name", "changed_fields") || !id || !name || string(p["changed_fields"]) == "null" || json.Unmarshal(p["changed_fields"], &fields) != nil || len(fields) > 5 {
			return false
		}
		seen := map[string]bool{}
		for _, field := range fields {
			switch field {
			case "models", "group", "type", "base_url", "key":
			default:
				return false
			}
			if seen[field] {
				return false
			}
			seen[field] = true
		}
		return true
	case "channel.create":
		_, name := fundingString(p["name"])
		_, kind := fundingID(p["type"])
		_, count := fundingID(p["count"])
		return fundingAuditFields(p, "name", "type", "count") && name && kind && count
	case "channel.copy":
		from, sourceOK := fundingID(p["sourceId"])
		to, targetOK := fundingID(p["id"])
		_, nameOK := fundingString(p["name"])
		// Copy/reset acts on a Channel's balance/used_quota, not a User wallet.
		return fundingAuditFields(p, "sourceId", "id", "name") && sourceOK && targetOK && from != to && nameOK
	case "channel.delete":
		_, idOK := fundingID(p["id"])
		_, nameOK := fundingString(p["name"])
		return fundingAuditFields(p, "id", "name") && idOK && nameOK
	case "option.update":
		key, ok := fundingString(p["key"])
		return fundingAuditFields(p, "key") && ok && strings.TrimSpace(key) != ""
	case "option.payment_compliance":
		// The reviewed handler only writes confirmation options. The middleware
		// maps this static route to an empty-params action, not to a recharge.
		return fundingAuditFields(p) && fundingResourceRouteAudit(row, envelope, "POST", "/api/option/payment_compliance", false)
	case "generic":
		method, mok := fundingString(p["method"])
		route, rok := fundingString(p["route"])
		if !fundingAuditFields(p, "method", "route") || !mok || !rok {
			return false
		}
		switch method + " " + route {
		case "PUT /api/channel/", "POST /api/channel/fetch_models", "POST /api/channel/upstream_updates/detect":
			return fundingResourceRouteAudit(row, envelope, method, route, false)
		case "POST /api/channel/:id/key":
			return fundingResourceRouteAudit(row, envelope, method, route, true)
		case "POST /api/subscription/admin/users/:id/subscriptions", "DELETE /api/subscription/admin/user_subscriptions/:id":
			return fundingSubscriptionControlAudit(row, envelope, method, route)
		}
	}
	return false
}

// Reviewed against rc26 8f6961c675932f406260ff0c218bc2aa0603e9b2:
// AdminCreateUserSubscription -> AdminBindSubscription ->
// CreateUserSubscriptionFromPlanTx, and AdminDeleteUserSubscription. These
// handlers grant/delete a separate subscription and may change users.group;
// neither mutates users.quota or users.used_quota. This is NOT an exemption
// for subscription purchases, balance/pay, arbitrary subscription routes or
// subscription consumption. Keep their existing review/ledger handling.
func fundingSubscriptionControlAudit(row FundingLog, envelope map[string]json.RawMessage, method, route string) bool {
	if row.Content != method+" "+route {
		return false
	}
	for key := range envelope {
		if key != "op" && key != "audit_info" && key != "admin_info" {
			return false
		}
	}
	audit, ok := fundingObject(string(envelope["audit_info"]), false)
	if !ok || !fundingAuditFields(audit, "method", "route", "path", "params", "status", "success") {
		return false
	}
	params, ok := fundingObject(string(audit["params"]), false)
	id, validID := fundingID(params["id"])
	if !ok || !fundingAuditFields(params, "id") || !validID {
		return false
	}
	var actualPath string
	switch method + " " + route {
	case "POST /api/subscription/admin/users/:id/subscriptions":
		actualPath = "/api/subscription/admin/users/" + strconv.FormatInt(id, 10) + "/subscriptions"
	case "DELETE /api/subscription/admin/user_subscriptions/:id":
		actualPath = "/api/subscription/admin/user_subscriptions/" + strconv.FormatInt(id, 10)
	default:
		return false
	}
	m, mok := fundingString(audit["method"])
	r, rok := fundingString(audit["route"])
	p, pok := fundingString(audit["path"])
	status, sok := fundingID(audit["status"])
	var success bool
	return mok && rok && pok && m == method && r == route && p == actualPath && sok && status >= 100 && status <= 599 &&
		string(audit["success"]) != "null" && json.Unmarshal(audit["success"], &success) == nil
}

// Source-reviewed fallback routes only. No substring or resource-prefix match.
// A dynamic key-view audit must also bind its canonical numeric ID to the path;
// malformed/extra params or any future wallet endpoint retain review status.
func fundingResourceRouteAudit(row FundingLog, envelope map[string]json.RawMessage, method, route string, dynamicID bool) bool {
	if row.Content != method+" "+route {
		return false
	}
	audit, ok := fundingObject(string(envelope["audit_info"]), false)
	if !ok {
		return false
	}
	keys := []string{"method", "route", "path", "status", "success"}
	actualPath := route
	if dynamicID {
		keys = append(keys, "params")
		params, pok := fundingObject(string(audit["params"]), false)
		id, iok := fundingID(params["id"])
		if !pok || !fundingAuditFields(params, "id") || !iok {
			return false
		}
		actualPath = "/api/channel/" + strconv.FormatInt(id, 10) + "/key"
	}
	if !fundingAuditFields(audit, keys...) {
		return false
	}
	m, mok := fundingString(audit["method"])
	r, rok := fundingString(audit["route"])
	p, pok := fundingString(audit["path"])
	status, sok := fundingID(audit["status"])
	var success bool
	return mok && rok && pok && m == method && r == route && p == actualPath && sok && status >= 100 && status <= 599 && string(audit["success"]) != "null" && json.Unmarshal(audit["success"], &success) == nil
}

func fundingAuditFields(p map[string]json.RawMessage, keys ...string) bool {
	if len(p) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := p[key]; !ok {
			return false
		}
	}
	return true
}

// The source's recordManageAuditFor supplies target_user_id for the newly
// created user, not the operator. Preserve a review item for that beneficiary;
// this does not infer an initial balance or claim creation has no money effect.
func fundingCreatedUserTarget(raw json.RawMessage) (int64, bool) {
	p, ok := fundingObject(string(raw), false)
	if !ok || !fundingAuditFields(p, "username", "role", "target_user_id") {
		return 0, false
	}
	name, nok := fundingString(p["username"])
	_, rok := fundingID(p["role"])
	id, iok := fundingID(p["target_user_id"])
	return id, nok && strings.TrimSpace(name) != "" && rok && iok
}
