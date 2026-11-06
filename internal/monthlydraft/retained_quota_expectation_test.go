package monthlydraft

// Test-only oracle for a real historical month with no previously issued bill.
// Expected integer quotas are independently checked against retained originals
// by the local fixture producer. They are NOT passed to the product generator.
import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

type retainedQuotaToken struct {
	TokenID      int64  `json:"token_id"`
	Name         string `json:"name"`
	ConsumeQuota string `json:"consume_quota"`
	RefundQuota  string `json:"refund_quota"`
}
type retainedQuotaMember struct {
	UserID          int64                `json:"user_id"`
	Username        string               `json:"username"`
	OpeningQuota    string               `json:"opening_quota"`
	RechargeQuota   string               `json:"recharge_quota"`
	GiftQuota       string               `json:"gift_quota"`
	AdjustmentQuota string               `json:"adjustment_quota"`
	ConsumeQuota    string               `json:"consume_quota"`
	RefundQuota     string               `json:"refund_quota"`
	CorrectionQuota string               `json:"correction_quota"`
	ClosingQuota    string               `json:"closing_quota"`
	Requests        int64                `json:"requests"`
	Tokens          int64                `json:"tokens"`
	TokenUsage      []retainedQuotaToken `json:"token_consumption"`
}
type retainedQuotaExpectation struct {
	Version       int                   `json:"version"`
	GroupID       int64                 `json:"group_id"`
	Account       string                `json:"account"`
	Month         string                `json:"month"`
	RechargeCount int                   `json:"recharge_count"`
	Members       []retainedQuotaMember `json:"members"`
}

func retainedQuotaReference(t *testing.T, raw []byte, ownerID int64, month, account string) []byte {
	t.Helper()
	var e retainedQuotaExpectation
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&e) != nil {
		t.Fatal("invalid independent quota expectation")
	}
	canonical, err := json.Marshal(e)
	if err != nil || !bytes.Equal(raw, canonical) || e.Version != 1 || e.GroupID != ownerID || e.Month != month || e.Account != account || len(e.Members) == 0 || e.RechargeCount < 0 {
		t.Fatal("independent quota expectation scope/canonical form differs")
	}
	q := func(s string) *big.Int {
		t.Helper()
		n, ok := new(big.Int).SetString(s, 10)
		if !ok || !n.IsInt64() || n.String() != s {
			t.Fatal("expected money must be exact canonical int64 quota")
		}
		return n
	}
	money := func(n *big.Int) string {
		return new(big.Rat).SetFrac(new(big.Int).Set(n), big.NewInt(500000)).FloatString(6)
	}
	fields := []string{"opening", "recharge", "gift", "adjustment", "consume", "refund", "correction", "closing"}
	totals := map[string]*big.Int{}
	for _, name := range fields {
		totals[name] = new(big.Int)
	}
	var requests, tokens int64
	members := []any{}
	var previous int64
	for _, m := range e.Members {
		if m.UserID <= previous || m.Username == "" || m.Requests < 0 || m.Tokens < 0 || m.TokenUsage == nil {
			t.Fatal("invalid/duplicate expected account or unknown counts")
		}
		previous = m.UserID
		amounts := []string{m.OpeningQuota, m.RechargeQuota, m.GiftQuota, m.AdjustmentQuota, m.ConsumeQuota, m.RefundQuota, m.CorrectionQuota, m.ClosingQuota}
		v := map[string]*big.Int{}
		for i, s := range amounts {
			v[fields[i]] = q(s)
			totals[fields[i]].Add(totals[fields[i]], v[fields[i]])
		}
		for _, f := range []string{"recharge", "gift", "consume", "refund", "correction"} {
			if v[f].Sign() < 0 {
				t.Fatal("invalid expected money sign")
			}
		}
		balance := new(big.Int).Set(v["opening"])
		for _, f := range []string{"recharge", "gift", "adjustment", "refund", "correction"} {
			balance.Add(balance, v[f])
		}
		balance.Sub(balance, v["consume"])
		if balance.Cmp(v["closing"]) != 0 {
			t.Fatal("independent raw wallet equation does not close")
		}
		credit := new(big.Int).Add(v["recharge"], v["gift"])
		net := new(big.Int).Sub(v["consume"], v["refund"])
		tokenRows := []any{}
		gross, refund := new(big.Int), new(big.Int)
		last := int64(-1)
		for _, tok := range m.TokenUsage {
			if tok.TokenID <= last || tok.Name == "" {
				t.Fatal("invalid expected token identity/order")
			}
			last = tok.TokenID
			c, r := q(tok.ConsumeQuota), q(tok.RefundQuota)
			if c.Sign() < 0 || r.Sign() < 0 {
				t.Fatal("negative expected token gross/refund")
			}
			gross.Add(gross, c)
			refund.Add(refund, r)
			// The public DTO deliberately groups unknown historical token IDs
			// under a stable label, not an apparently attributable old log name.
			// Preserve ID and every exact amount; this is presentation only.
			label := tok.Name
			if tok.TokenID == 0 {
				label = "历史未记录令牌"
			}
			tokenRows = append(tokenRows, map[string]any{"token_id": tok.TokenID, "name": label, "amount": money(c), "refund": money(r), "net": money(new(big.Int).Sub(c, r))})
		}
		// Independently encode the established public order: known IDs first,
		// followed by the explicitly retained unknown-ID bucket. Do not relax
		// the response comparison to an unordered or partially checked list.
		sort.Slice(tokenRows, func(i, j int) bool {
			a := tokenRows[i].(map[string]any)["token_id"].(int64)
			b := tokenRows[j].(map[string]any)["token_id"].(int64)
			return a != 0 && (b == 0 || a < b)
		})
		if gross.Cmp(v["consume"]) != 0 || refund.Cmp(v["refund"]) != 0 {
			t.Fatal("independent token sums do not close to account")
		}
		if m.Requests > 9007199254740991-requests || m.Tokens > 9007199254740991-tokens {
			t.Fatal("expected counts exceed customer JSON range")
		}
		requests += m.Requests
		tokens += m.Tokens
		members = append(members, map[string]any{"user_id": m.UserID, "username": m.Username, "opening": money(v["opening"]), "closing": money(v["closing"]),
			"credit_total": money(credit), "adjustment": money(v["adjustment"]),
			"consumption": map[string]any{"requests": m.Requests, "tokens": m.Tokens, "amount": money(v["consume"]), "refund": money(v["refund"]), "net": money(net)}, "token_consumption": tokenRows})
		// Existing payload omits a zero correction; nonzero must be checked.
		if v["correction"].Sign() != 0 {
			members[len(members)-1].(map[string]any)["balance_correction"] = money(v["correction"])
		}
	}
	credits := map[string]any{"known_total": money(new(big.Int).Add(totals["recharge"], totals["gift"])), "adjustment": money(totals["adjustment"]),
		"recharge_count": e.RechargeCount, "recharge_total": money(totals["recharge"]), "gift_total": money(totals["gift"]), "available": true, "unknown": 0, "message": ""}
	if totals["correction"].Sign() != 0 {
		credits["balance_correction"] = money(totals["correction"])
	}
	out, err := json.Marshal(map[string]any{"ok": true, "data": map[string]any{"account": e.Account, "month": e.Month, "published": true, "current_month": false,
		"month_opening_balance": money(totals["opening"]), "month_end_balance": money(totals["closing"]), "credits": credits, "members": members,
		"consumption":            map[string]any{"requests": requests, "tokens": tokens, "amount": money(totals["consume"]), "refund": money(totals["refund"]), "net": money(new(big.Int).Sub(totals["consume"], totals["refund"]))},
		"member_details_version": 1, "token_details_version": 1, "current_balance": nil}})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func retainedQuotaResponseError(expected, response []byte) error {
	decode := func(raw []byte) (any, error) {
		var v any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		err := d.Decode(&v)
		return v, err
	}
	var check func(any, any, string) error
	check = func(want, got any, at string) error {
		switch w := want.(type) {
		case map[string]any:
			g, ok := got.(map[string]any)
			if !ok {
				return fmt.Errorf("missing expected customer object at %s", at)
			}
			if correction, extra := g["balance_correction"]; extra && w["balance_correction"] == nil && correction != "0.000000" {
				return fmt.Errorf("unexpected customer balance correction at %s", at)
			}
			for k, v := range w {
				gval, present := g[k]
				if !present {
					// Public DTO uses omitempty for a source-proven empty token
					// list. Only that explicit empty expectation may be absent;
					// nonempty/unknown token evidence must never be skipped.
					if rows, empty := v.([]any); k == "token_consumption" && empty && len(rows) == 0 {
						continue
					}
					return fmt.Errorf("missing expected customer field at %s/%s", at, k)
				}
				if err := check(v, gval, at+"/"+k); err != nil {
					return err
				}
			}
		case []any:
			g, ok := got.([]any)
			if !ok || len(g) != len(w) {
				return fmt.Errorf("customer account/token count differs at %s", at)
			}
			for i := range w {
				if err := check(w[i], g[i], at+"/"+strconv.Itoa(i)); err != nil {
					return err
				}
			}
		default:
			if !reflect.DeepEqual(want, got) {
				return fmt.Errorf("customer figure/identity differs from independent original quota at %s", at)
			}
		}
		return nil
	}
	want, err := decode(expected)
	if err != nil {
		return err
	}
	got, err := decode(response)
	if err != nil {
		return err
	}
	return check(want, got, "")
}

func retainedQuotaResponseMatches(t *testing.T, expected, response []byte) {
	t.Helper()
	if err := retainedQuotaResponseError(expected, response); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedQuotaReferencePreservesExactMoneyCountsAndTokens(t *testing.T) {
	e := retainedQuotaExpectation{Version: 1, GroupID: 10, Account: "synthetic", Month: "2026-08", RechargeCount: 1,
		Members: []retainedQuotaMember{{UserID: 20, Username: "synthetic-user", OpeningQuota: "0", RechargeQuota: "99500000", GiftQuota: "500000", AdjustmentQuota: "0", ConsumeQuota: "3988151", RefundQuota: "0", CorrectionQuota: "0", ClosingQuota: "96011849", Requests: 140, Tokens: 8077291,
			TokenUsage: []retainedQuotaToken{{TokenID: 348, Name: "a", ConsumeQuota: "3949112", RefundQuota: "0"}, {TokenID: 350, Name: "b", ConsumeQuota: "39039", RefundQuota: "0"}}}}}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	want := retainedQuotaReference(t, raw, 10, "2026-08", "synthetic")
	if !bytes.Contains(want, []byte(`"month_end_balance":"192.023698"`)) || !bytes.Contains(want, []byte(`"amount":"7.898224"`)) {
		t.Fatal("independent quota arithmetic drifted")
	}
	retainedQuotaResponseMatches(t, want, want)
	for _, pair := range [][2]string{{`"month_end_balance":"192.023698"`, `"month_end_balance":"192.023699"`}, {`"requests":140`, `"requests":141`}, {`"token_id":350`, `"token_id":351`}, {`"account":"synthetic"`, `"account":"another-account"`}, {`"gift_total":"1.000000"`, `"gift_total":"0.000000"`}} {
		changed := bytes.Replace(want, []byte(pair[0]), []byte(pair[1]), 1)
		if bytes.Equal(changed, want) || retainedQuotaResponseError(want, changed) == nil {
			t.Fatal("independent oracle accepted a changed figure or identity")
		}
	}
	changed := bytes.Replace(want, []byte(`"credits":{`), []byte(`"credits":{"balance_correction":"0.000002",`), 1)
	if retainedQuotaResponseError(want, changed) == nil {
		t.Fatal("independent oracle accepted an extra balance correction")
	}
}

func TestRetainedQuotaReferenceAcceptsOnlyProvenEmptyTokenOmission(t *testing.T) {
	e := retainedQuotaExpectation{Version: 1, GroupID: 10, Account: "synthetic", Month: "2026-08",
		Members: []retainedQuotaMember{{UserID: 20, Username: "later-registered", OpeningQuota: "0", RechargeQuota: "0", GiftQuota: "0", AdjustmentQuota: "0", ConsumeQuota: "0", RefundQuota: "0", CorrectionQuota: "0", ClosingQuota: "0", TokenUsage: []retainedQuotaToken{}}}}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	want := retainedQuotaReference(t, raw, 10, e.Month, e.Account)
	omitted := bytes.Replace(want, []byte(`"token_consumption":[],`), nil, 1)
	if bytes.Equal(want, omitted) || retainedQuotaResponseError(want, omitted) != nil {
		t.Fatal("source-proven empty list did not match the public omitempty contract")
	}
	unexpected := bytes.Replace(want, []byte(`"token_consumption":[]`), []byte(`"token_consumption":[{"token_id":1}]`), 1)
	if retainedQuotaResponseError(want, unexpected) == nil {
		t.Fatal("an extra token was accepted for a proven empty account")
	}
	e.Members[0].ConsumeQuota, e.Members[0].ClosingQuota = "1", "-1"
	e.Members[0].TokenUsage = []retainedQuotaToken{{TokenID: 1, Name: "synthetic", ConsumeQuota: "1", RefundQuota: "0"}}
	raw, err = json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	want = retainedQuotaReference(t, raw, 10, e.Month, e.Account)
	var response map[string]any
	if json.Unmarshal(want, &response) != nil {
		t.Fatal("invalid oracle JSON")
	}
	member := response["data"].(map[string]any)["members"].([]any)[0].(map[string]any)
	delete(member, "token_consumption")
	missing, err := json.Marshal(response)
	if err != nil || retainedQuotaResponseError(want, missing) == nil {
		t.Fatal("missing nonempty token details were accepted")
	}
}

func TestRetainedQuotaReferenceHistoricalUnknownTokenPresentation(t *testing.T) {
	e := retainedQuotaExpectation{Version: 1, GroupID: 10, Account: "synthetic", Month: "2026-06",
		Members: []retainedQuotaMember{{UserID: 20, Username: "synthetic-user", OpeningQuota: "0", RechargeQuota: "100", GiftQuota: "0", AdjustmentQuota: "0", ConsumeQuota: "60", RefundQuota: "0", CorrectionQuota: "0", ClosingQuota: "40", Requests: 3, Tokens: 100,
			TokenUsage: []retainedQuotaToken{{TokenID: 0, Name: "old-log-name-is-not-proof-of-identity", ConsumeQuota: "10", RefundQuota: "0"}, {TokenID: 3, Name: "a", ConsumeQuota: "20", RefundQuota: "0"}, {TokenID: 4, Name: "b", ConsumeQuota: "30", RefundQuota: "0"}}}}}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	want := retainedQuotaReference(t, raw, e.GroupID, e.Month, e.Account)
	var response map[string]any
	if json.Unmarshal(want, &response) != nil {
		t.Fatal("invalid independent reference")
	}
	member := response["data"].(map[string]any)["members"].([]any)[0].(map[string]any)
	rows := member["token_consumption"].([]any)
	for i, id := range []float64{3, 4, 0} {
		if rows[i].(map[string]any)["token_id"] != id {
			t.Fatal("known IDs must precede the explicitly unknown-ID bucket")
		}
	}
	if rows[2].(map[string]any)["name"] != "历史未记录令牌" || rows[2].(map[string]any)["amount"] != "0.000020" {
		t.Fatal("unknown historical identity was guessed or its amount changed")
	}
	retainedQuotaResponseMatches(t, want, want)
	for _, pair := range [][2]string{{`"token_id":0`, `"token_id":99`}, {`"name":"历史未记录令牌"`, `"name":"guessed-token"`}, {`"amount":"0.000020"`, `"amount":"0.000021"`}} {
		changed := bytes.Replace(want, []byte(pair[0]), []byte(pair[1]), 1)
		if bytes.Equal(changed, want) || retainedQuotaResponseError(want, changed) == nil {
			t.Fatal("unknown-ID label compatibility concealed an identity/amount change")
		}
	}
	member["token_consumption"] = []any{rows[2], rows[0], rows[1]}
	reordered, err := json.Marshal(response)
	if err != nil || retainedQuotaResponseError(want, reordered) == nil {
		t.Fatal("incorrect public token order was accepted")
	}
	member["token_consumption"] = rows[:2]
	missing, err := json.Marshal(response)
	if err != nil || retainedQuotaResponseError(want, missing) == nil {
		t.Fatal("missing historical unknown-ID money was accepted")
	}
}
