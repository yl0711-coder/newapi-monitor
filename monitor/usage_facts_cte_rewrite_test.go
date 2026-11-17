package monitor

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// legacyUsageFactsCTEForTest is the pre-v1.18.64 usageFactsCTE, kept verbatim
// to prove the rewritten visibility predicate selects the same rows.
func legacyUsageFactsCTEForTest(ids []int64, fromTs, toTs, tokenID int64) (string, []any, error) {
	if len(ids) == 0 {
		return "", nil, nil
	}
	if toTs <= fromTs {
		return "WITH facts AS (SELECT 0 AS bucket_ts,0 AS user_id,0 AS channel_id,'' AS grp,'' AS model_name,0 AS token_id,'' AS token_name,0 AS requests,0 AS refund_records,0 AS prompt_tokens,0 AS completion_tokens,0 AS consume_quota,0 AS refund_quota WHERE 1=0) ", nil, nil
	}
	// Daily rows are whole-day versions. A published watermark inside the current
	// day must therefore use hours for the tail; selecting that day's candidate
	// daily row would expose future/unverified hours in one step.
	fullDayThrough := usageFactDayStart(toTs)
	dailyIn, dailyArgs := usageIn("d.user_id", ids)
	hourIn, hourArgs := usageIn("h.user_id", ids)
	dailyWhere := "d.date_ts >= ? AND d.date_ts < ? AND " + dailyIn
	hourWhere := "h.hour_ts >= ? AND h.hour_ts < ? AND " + hourIn
	dailyParams := append([]any{fromTs, fullDayThrough}, dailyArgs...)
	hourParams := append([]any{fromTs, toTs}, hourArgs...)
	if tokenID > 0 {
		dailyWhere += " AND d.token_id = ?"
		dailyParams = append(dailyParams, tokenID)
		hourWhere += " AND h.token_id = ?"
		hourParams = append(hourParams, tokenID)
	}
	q := `WITH facts AS (
 SELECT d.date_ts AS bucket_ts,d.user_id,d.channel_id,d.grp,d.model_name,d.token_id,d.token_name,
  d.requests,d.refund_records,d.prompt_tokens,d.completion_tokens,d.consume_quota,d.refund_quota
 FROM usage_daily_facts d
 LEFT JOIN usage_fact_published_members pm ON pm.user_id=d.user_id
 WHERE ` + dailyWhere + `
  AND ((pm.user_id IS NOT NULL AND (pm.source_floor_hour<=0 OR d.date_ts>=pm.source_floor_hour))
       OR (pm.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM usage_fact_published_members LIMIT 1)))
 UNION ALL
 SELECT h.day_ts AS bucket_ts,h.user_id,h.channel_id,h.grp,h.model_name,h.token_id,h.token_name,
  h.requests,h.refund_records,h.prompt_tokens,h.completion_tokens,h.consume_quota,h.refund_quota
	 FROM usage_hour_facts h
	 LEFT JOIN usage_fact_published_members pm ON pm.user_id=h.user_id
	 WHERE ` + hourWhere + `
	  AND ((pm.user_id IS NOT NULL AND (pm.source_floor_hour<=0 OR h.hour_ts>=pm.source_floor_hour))
	       OR (pm.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM usage_fact_published_members LIMIT 1)))
	  AND NOT EXISTS (
	   SELECT 1 FROM usage_fact_member_day_states p
	   WHERE p.date_ts = h.day_ts AND p.user_id = h.user_id AND p.date_ts < ` + strconv.FormatInt(fullDayThrough, 10) + ` AND p.content_hash <> ''
	  )
	  AND NOT EXISTS (
	   SELECT 1 FROM usage_daily_facts d
	   WHERE d.date_ts = h.day_ts AND d.user_id = h.user_id AND d.date_ts < ` + strconv.FormatInt(fullDayThrough, 10) + `
	  )
) `
	return q, append(dailyParams, hourParams...), nil
}

func TestUsageFactsCTEEqualityRewriteMatchesLegacy(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	db := m.usageFactsStore()
	rng := rand.New(rand.NewSource(11))
	day0 := usageFactDayStart(1794931200) - 20*usageFactDaySeconds
	users := []int64{31, 32, 33, 34}
	var daily []UsageDailyFact
	var hours []UsageHourFact
	var states []UsageFactMemberDayState
	for _, uid := range users {
		for d := int64(0); d < 21; d++ {
			day := day0 + d*usageFactDaySeconds
			hasDaily := rng.Intn(3) > 0 && d < 20
			if hasDaily {
				for k := 0; k < 3; k++ {
					daily = append(daily, UsageDailyFact{DateTs: day, UserID: uid, ChannelID: int64(k), Grp: "g", ModelName: fmt.Sprintf("m%d", k), TokenID: int64(k % 2),
						Requests: int64(rng.Intn(9) + 1), ConsumeQuota: int64(rng.Intn(1000)), RefundQuota: int64(rng.Intn(50)), RefundRecords: int64(rng.Intn(2)),
						PromptTokens: int64(rng.Intn(100)), CompletionTokens: int64(rng.Intn(100))})
				}
			}
			switch rng.Intn(3) {
			case 0:
				states = append(states, UsageFactMemberDayState{UserID: uid, DateTs: day, ContentHash: "h"})
			case 1:
				states = append(states, UsageFactMemberDayState{UserID: uid, DateTs: day, ContentHash: ""})
			}
			for h := int64(0); h < 24; h += int64(rng.Intn(5) + 1) {
				hours = append(hours, UsageHourFact{HourTs: day + h*usageFactHourSeconds, DayTs: day, UserID: uid, ChannelID: h % 3, Grp: "g", ModelName: "m", TokenID: h % 2,
					Requests: int64(rng.Intn(5) + 1), ConsumeQuota: int64(rng.Intn(500)), RefundQuota: int64(rng.Intn(20)),
					PromptTokens: int64(rng.Intn(50)), CompletionTokens: int64(rng.Intn(50))})
			}
		}
	}
	for _, rows := range []any{daily, hours, states} {
		if err := db.CreateInBatches(rows, 200).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, pm := range []UsageFactPublishedMember{{UserID: 31, SourceFloorHour: 0}, {UserID: 32, SourceFloorHour: day0 + 3*usageFactDaySeconds}, {UserID: 33, SourceFloorHour: day0 + 5*usageFactDaySeconds + 7*usageFactHourSeconds}} {
		if err := db.Create(&pm).Error; err != nil {
			t.Fatal(err)
		}
	}
	run := func(cte string, args []any) string {
		rows, err := db.Raw(cte+`SELECT bucket_ts,user_id,channel_id,grp,model_name,token_id,requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota FROM facts`, args...).Rows()
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v [12]any
			ptrs := make([]any, 12)
			for i := range v {
				ptrs[i] = &v[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(v))
		}
		sort.Strings(out)
		return strings.Join(out, "\n")
	}
	ranges := [][2]int64{
		{day0, day0 + 21*usageFactDaySeconds},
		{day0 + 2*usageFactDaySeconds, day0 + 20*usageFactDaySeconds + 13*usageFactHourSeconds},
		{day0 + 6*usageFactDaySeconds, day0 + 9*usageFactDaySeconds},
		{day0 + 19*usageFactDaySeconds + 5*usageFactHourSeconds, day0 + 20*usageFactDaySeconds + 23*usageFactHourSeconds},
	}
	for _, idset := range [][]int64{users, {32}, {33, 34}} {
		for _, r := range ranges {
			for _, tok := range []int64{0, 1} {
				oldCTE, oldArgs, err := legacyUsageFactsCTEForTest(idset, r[0], r[1], tok)
				if err != nil {
					t.Fatal(err)
				}
				newCTE, newArgs, err := usageFactsCTE(idset, r[0], r[1], tok)
				if err != nil {
					t.Fatal(err)
				}
				want, got := run(oldCTE, oldArgs), run(newCTE, newArgs)
				if want != got {
					t.Fatalf("ids=%v range=%v token=%d: rewritten CTE differs\nwant %d bytes, got %d bytes", idset, r, tok, len(want), len(got))
				}
				if want == "" && r[1]-r[0] > usageFactDaySeconds {
					t.Fatalf("ids=%v range=%v token=%d: fixture selected nothing", idset, r, tok)
				}
			}
		}
	}
	// The daily-version check must be an equality lookup, not a per-user range scan.
	cte, args, _ := usageFactsCTE(users, ranges[0][0], ranges[0][1], 0)
	var plan []struct {
		ID, Parent, Notused int
		Detail              string
	}
	if err := db.Raw("EXPLAIN QUERY PLAN "+cte+"SELECT COUNT(*) FROM facts", args...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	for _, p := range plan {
		if strings.Contains(p.Detail, "SEARCH d USING") && strings.Contains(p.Detail, "date_ts<?") && !strings.Contains(p.Detail, "date_ts>?") {
			t.Fatalf("daily-version check scans a range: %s", p.Detail)
		}
	}
}
