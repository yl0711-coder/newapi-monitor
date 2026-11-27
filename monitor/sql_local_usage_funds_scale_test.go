package monitor

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

func TestSQLAuditFundEventProjectionScale(t *testing.T) {
	db, _ := sqlScaleDB(t, &ChannelUpstreamFundEvent{}, &ChannelUpstreamAccount{}, &UpstreamFundSyncState{})
	// About 8MiB of synthetic retained evidence, not copied production data.
	if err := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<501)
	INSERT INTO channel_upstream_fund_events(domain,account_epoch,event_key,occurred_at,kind,direction,
	amount_usd,amount_known,observed_count,content,raw_json,reparse_error)
	SELECT 'selected','epoch',printf('%06d',n),100+n,'topup','credit',1,1,1,'display content',
	'{'||'"synthetic":"'||replace(hex(zeroblob(8192)),'0','x')||'"}', '' FROM seq;
	INSERT INTO channel_upstream_fund_events(domain,account_epoch,event_key,occurred_at,kind,direction,raw_json)
	VALUES('other','epoch','other',200,'topup','credit','other-account');`).Error; err != nil {
		t.Fatal(err)
	}
	base := func() *gorm.DB {
		return db.Where("domain = ? AND account_epoch = ? AND occurred_at >= ? AND occurred_at < ?", "selected", "epoch", 100, 700).
			Order("occurred_at DESC,event_key DESC").Limit(upstreamFundEventQueryLimit)
	}
	type observation struct {
		Variant        string
		US             []int64
		LoadedRawBytes int
		Plan           []string
	}
	results := []observation{{Variant: "legacy_full_row"}, {Variant: "omit_private_raw_evidence"}}
	var want []byte
	for round := 0; round < 7; round++ {
		for j := range results {
			i := j
			if round%2 != 0 {
				i = len(results) - 1 - j
			}
			q := base()
			if i == 1 {
				q = q.Omit("raw_json")
			}
			var rows []ChannelUpstreamFundEvent
			at := time.Now()
			err := q.Find(&rows).Error
			us := time.Since(at).Microseconds()
			if err != nil || len(rows) != upstreamFundEventQueryLimit {
				t.Fatal("event query changed", len(rows), err)
			}
			rawBytes := 0
			for _, row := range rows {
				rawBytes += len(row.RawJSON)
			}
			rendered, err := json.Marshal(rows)
			if err != nil {
				t.Fatal(err)
			}
			if want == nil {
				want = rendered
			} else if !bytes.Equal(want, rendered) {
				t.Fatal("omission changed visible JSON")
			}
			results[i].US = append(results[i].US, us)
			results[i].LoadedRawBytes = rawBytes
			if round == 0 {
				stmt := q.Session(&gorm.Session{DryRun: true}).Find(&[]ChannelUpstreamFundEvent{})
				results[i].Plan = sqlScalePlan(t, db, stmt)
			}
		}
	}
	if results[0].LoadedRawBytes < 7<<20 || results[1].LoadedRawBytes != 0 {
		t.Fatal("projection fixture did not test retained evidence width")
	}
	var retained int64
	if err := db.Model(&ChannelUpstreamFundEvent{}).Select("SUM(length(raw_json))").Scan(&retained).Error; err != nil || retained <= int64(results[0].LoadedRawBytes) {
		t.Fatal("retained evidence changed", retained, err)
	}
	account := ChannelUpstreamAccount{Domain: "selected", Provider: upstreamProviderNewAPI, BaseURL: "https://selected.invalid", UserID: 7, Enabled: true, UsageSyncEnabled: true}
	epoch := newAPIUpstreamAccountEpoch(account)
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelUpstreamFundEvent{}).Where("domain=?", "selected").Update("account_epoch", epoch).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&UpstreamFundSyncState{Domain: "selected", AccountEpoch: epoch, CoverageFrom: 100, TailSyncedUntil: 700, BackfillDone: true}).Error; err != nil {
		t.Fatal(err)
	}
	seenProjection := false
	if err := db.Callback().Query().Before("gorm:query").Register("test:fund-visible-projection", func(tx *gorm.DB) {
		if _, ok := tx.Statement.Dest.(*[]ChannelUpstreamFundEvent); !ok {
			return
		}
		if tx.Statement.SQL.Len() == 0 {
			callbacks.BuildQuerySQL(tx)
		}
		seenProjection = true
		if strings.Contains(tx.Statement.SQL.String(), "raw_json") {
			t.Error("actual handler still selected private raw evidence")
		}
	}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{UpstreamFundsSyncEnabled: true, UpstreamFundsDomains: []string{"selected"}}}
	attachSQLAuditFixture(t, m)
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/channels/upstream/funds?domain=selected&from=100&to=700", nil)
	m.getChannelUpstreamFundsHandler(c)
	var response struct {
		Events  []ChannelUpstreamFundEvent `json:"events"`
		Summary upstreamFundSummary        `json:"summary"`
		Limited bool                       `json:"limited"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || !seenProjection {
		t.Fatal("actual handler failed", w.Code, err)
	}
	rendered, err := json.Marshal(response.Events)
	if err != nil || !bytes.Equal(rendered, want) || !response.Limited || response.Summary.CreditedUSD != 501 || response.Summary.EventOccurrences != 501 {
		t.Fatal("actual handler changed visible events or truncated totals", err)
	}
	var after int64
	if err := db.Model(&ChannelUpstreamFundEvent{}).Select("SUM(length(raw_json))").Scan(&after).Error; err != nil || after != retained {
		t.Fatal("page read changed originals", after, retained, err)
	}
	source, err := os.ReadFile("channel_upstream_funds.go")
	if err != nil {
		t.Fatal(err)
	}
	sqlScaleWrite(t, "fund-event-projection-scale-20261127.json", struct {
		Warning, SourceSHA256 string
		Results               []observation
	}{"501 synthetic retained events, 7 alternating memory-SQLite read samples. Identical complete visible JSON; raw evidence remains stored. Timings exclude HTTP and are not production latency.", fmt.Sprintf("%x", sha256.Sum256(source)), results})
}

func TestSQLAuditUsageHourAccessScale(t *testing.T) {
	db, _ := sqlScaleDB(t, &UsageDailyFact{}, &UsageHourFact{}, &UsageFactPublishedMember{}, &UsageFactMemberDayState{})
	for _, q := range []string{
		"CREATE INDEX idx_usage_hour_fact_user_day ON usage_hour_facts(user_id,day_ts)",
		"CREATE INDEX idx_usage_daily_fact_user_date ON usage_daily_facts(user_id,date_ts)",
		"CREATE INDEX idx_usage_daily_fact_user_token_date ON usage_daily_facts(user_id,token_id,date_ts)",
	} {
		if err := db.Exec(q).Error; err != nil {
			t.Fatal(err)
		}
	}
	from := time.Date(2026, 11, 19, 0, 0, 0, 0, usageCST).Unix()
	if err := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<120000)
	INSERT INTO usage_hour_facts(hour_ts,user_id,channel_id,grp,model_name,token_id,day_ts,token_name,
	requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota)
	SELECT ?+(n%192)*3600,1+n%80,n,'g','m',(n/80)%5,?+(n%192/24)*86400,'token',1,n%2,2,3,n%17,n%3 FROM seq;`, from, from).Error; err != nil {
		t.Fatal(err)
	}
	type sample struct {
		Schema, Case, Variant string
		US                    []int64
		Rows                  int
		Plan                  []string
	}
	var results []sample
	all := make([]int64, 80)
	for i := range all {
		all[i] = int64(i + 1)
	}
	for _, schema := range []string{"existing", "experimental_user_hour"} {
		if schema == "experimental_user_hour" {
			// Experimental only; never install this index in product migrations.
			if err := db.Exec("CREATE INDEX audit_usage_hour_user_hour ON usage_hour_facts(user_id,hour_ts)").Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Exec("DELETE FROM sqlite_stat1; ANALYZE sqlite_schema").Error; err != nil {
				t.Fatal(err)
			}
		}
		for _, stats := range []bool{false, true} {
			if stats {
				if err := db.Exec("ANALYZE").Error; err != nil {
					t.Fatal(err)
				}
			}
			for _, c := range []struct {
				name  string
				ids   []int64
				span  int64
				token int64
			}{{"one_short", []int64{1}, 12 * 3600, 0}, {"one_full", []int64{1}, 8 * 86400, 0}, {"two_short", []int64{1, 2}, 12 * 3600, 0}, {"all_short", all, 12 * 3600, 0}, {"all_full", all, 8 * 86400, 0}, {"token_short", []int64{1}, 12 * 3600, 1}, {"absent", []int64{999}, 12 * 3600, 0}} {
				cte, args, err := usageFactsCTE(c.ids, from, from+c.span, c.token)
				if err != nil {
					t.Fatal(err)
				}
				original := cte + "SELECT user_id,bucket_ts,SUM(requests) requests,SUM(consume_quota) consume FROM facts GROUP BY user_id,bucket_ts ORDER BY user_id,bucket_ts"
				forced := strings.Replace(original, "FROM usage_hour_facts h", "FROM usage_hour_facts h INDEXED BY idx_usage_hour_fact_hour_user", 1)
				start := len(results)
				variants := []struct{ name, q string }{{"native", original}, {"force_existing_hour", forced}}
				var want []struct{ UserID, BucketTs, Requests, Consume int64 }
				if err := db.Raw(original, args...).Scan(&want).Error; err != nil {
					t.Fatal(err)
				}
				for _, v := range variants {
					dry := db.Session(&gorm.Session{DryRun: true}).Raw(v.q, args...).Scan(&[]struct{}{})
					results = append(results, sample{Schema: fmt.Sprintf("%s/analyzed=%v", schema, stats), Case: c.name, Variant: v.name, Rows: len(want), Plan: sqlScalePlan(t, db, dry)})
				}
				for round := 0; round < 5; round++ {
					for j := range variants {
						i := j
						if round%2 != 0 {
							i = len(variants) - 1 - j
						}
						var got []struct{ UserID, BucketTs, Requests, Consume int64 }
						at := time.Now()
						err := db.Raw(variants[i].q, args...).Scan(&got).Error
						us := time.Since(at).Microseconds()
						if err != nil || !reflect.DeepEqual(got, want) {
							t.Fatal("access path changed values", err)
						}
						results[start+i].US = append(results[start+i].US, us)
					}
				}
			}
		}
	}
	source, err := os.ReadFile("usage_facts.go")
	if err != nil {
		t.Fatal(err)
	}
	sqlScaleWrite(t, "usage-hour-access-scale-20261127.json", struct {
		Warning, SourceSHA256 string
		Results               []sample
	}{"120000 synthetic fact rows, current CTE and existing indexes versus test-only experimental index; 5 alternating memory-SQLite samples. No product index, extra filter or statistics change. Not production latency.", fmt.Sprintf("%x", sha256.Sum256(source)), results})
}
