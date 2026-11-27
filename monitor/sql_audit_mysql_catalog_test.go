package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type mysqlAuditStatement struct {
	ID      string `json:"id"`
	Owner   string `json:"owner"`
	Variant string `json:"variant"`
	SQL     string `json:"sql"`
	Args    []any  `json:"args"`
	Origin  string `json:"origin"`
}
type mysqlAuditRecorder struct {
	mu             sync.Mutex
	owner, variant string
	items          []mysqlAuditStatement
}

func (r *mysqlAuditRecorder) add(owner, variant, q string, args []any, origin string) {
	h := sha256.Sum256([]byte(owner + "|" + variant + "|" + q))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, mysqlAuditStatement{fmt.Sprintf("%x", h[:12]), owner, variant, q, append([]any{}, args...), origin})
}

// This driver cannot connect anywhere. Empty rows plus explicitly synthetic
// user/subscription scaffolding render branches; they are not source evidence.
type explainCatalogMySQLDriver struct{ r *mysqlAuditRecorder }
type explainCatalogConn struct{ r *mysqlAuditRecorder }
type explainCatalogRows struct {
	columns []string
	values  [][]driver.Value
	at      int
}
type explainCatalogTx struct{}

func (d *explainCatalogMySQLDriver) Open(string) (driver.Conn, error) {
	return &explainCatalogConn{d.r}, nil
}
func (c *explainCatalogConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("catalog: prepare unsupported")
}
func (c *explainCatalogConn) Close() error              { return nil }
func (c *explainCatalogConn) Begin() (driver.Tx, error) { return &explainCatalogTx{}, nil }
func (c *explainCatalogConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &explainCatalogTx{}, nil
}
func (c *explainCatalogConn) QueryContext(_ context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	args := make([]any, len(a))
	for i, v := range a {
		args[i] = v.Value
	}
	c.r.add(c.r.owner, c.r.variant, q, args, "actual renderer, network-impossible recorder")
	if strings.Contains(q, "id, quota, created_at FROM users") {
		return &explainCatalogRows{columns: []string{"id", "quota", "created_at"}, values: [][]driver.Value{{int64(168), int64(0), int64(1795590000)}}}, nil
	}
	if strings.Contains(q, "id, COALESCE(created_at,0) FROM users") || strings.Contains(q, "id, created_at FROM users") {
		return &explainCatalogRows{columns: []string{"id", "created_at"}, values: [][]driver.Value{{int64(168), int64(1795590000)}}}, nil
	}
	if strings.Contains(q, "FROM user_subscriptions WHERE user_id IN") {
		return &explainCatalogRows{columns: []string{"id", "user_id", "plan_id", "start_time", "end_time", "status", "source"}, values: [][]driver.Value{{int64(1), int64(168), int64(1), int64(1795683600), int64(1795687200), "active", "audit-fixture"}}}, nil
	}
	return &explainCatalogRows{}, nil
}
func (r *explainCatalogRows) Columns() []string { return r.columns }
func (r *explainCatalogRows) Close() error      { return nil }
func (r *explainCatalogRows) Next(dst []driver.Value) error {
	if r.at >= len(r.values) {
		return io.EOF
	}
	copy(dst, r.values[r.at])
	r.at++
	return nil
}
func (t *explainCatalogTx) Commit() error   { return nil }
func (t *explainCatalogTx) Rollback() error { return nil }

func TestSQLAuditExportMySQLCatalog(t *testing.T) {
	path := os.Getenv("MONITOR_SQL_MYSQL_CATALOG")
	if path == "" {
		t.Skip("explicit SQL-only export path required")
	}
	abs, err := filepath.Abs(path)
	root, _ := filepath.Abs("../docs/sql-audit-20261126")
	if err != nil || !strings.HasPrefix(abs, root+string(os.PathSeparator)) {
		t.Fatal("invalid catalog destination")
	}
	r := &mysqlAuditRecorder{}
	name := fmt.Sprintf("explain-catalog-%d", usageCountingDriverID.Add(1))
	sql.Register(name, &explainCatalogMySQLDriver{r})
	db, err := sql.Open(name, "cannot-connect")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	m := newTestMonitor(t)
	m.prodDB = db
	from, to := int64(1795683600), int64(1795687200) // fixed closed hour, not current-window execution
	uid := int64(168)
	ids := []int64{uid}
	predicate := channelTestLogPredicateSQL()
	add := func(owner, variant, q string, args ...any) { r.add(owner, variant, q, args, "application SQL builder") }
	for _, v := range []struct{ owner, q string }{
		{"sampleRangeSlice/basic", sampleWindowSQL()}, {"sampleRangeSlice/user", sampleWindowUserSQL()},
		{"sampleRangeSlice/health", sampleCustomerHealthWindowSQL()}, {"sampleTokensRangeWithPriority", sampleTokenSQL()},
		{"recomputeFRTSumSlice", frtRecomputeWindowSQL()}, {"stabilityRange", stabilityRangeSQL()},
	} {
		add(v.owner, "closed-hour", v.q, from, to)
	}
	day := usageFactDayStart(from)
	add("fetchUsageFactHistoryRange/detail", "day-member", usageFactHistoryDetailSQL(m.dayExpr(), "user_id IN (?)", predicate), day, day+86400, uid, usageFactHistoryMaxRows+1)
	add("fetchUsageFactHistoryControls", "day-member", usageFactHistoryControlSQL(m.dayExpr(), "user_id IN (?)", predicate), day, day+86400, uid, 2)
	add("fetchUsageFactControlSlice", "closed-hour", usageFactHistoryControlSQL(m.dayExpr(), "user_id = ?", predicate), from, to, uid, 2)
	add("financeUserHourSQL", "closed-hour", financeUserHourSQL(), from, to)
	add("financeCreditHourSQL", "closed-hour", financeCreditHourSQL(), from, to)
	q, e := financeGiftBoundarySQL(1)
	if e != nil {
		t.Fatal(e)
	}
	add("financeGiftBoundarySQL", "closed-hour", q, from, to, uid)
	q, a := financeInternalAccountHourSQL(ids)
	add("financeInternalAccountHourSQL", "closed-hour", q, append([]any{from, to}, a...)...)
	for _, v := range []struct {
		name string
		f    usageLogFilter
	}{
		{"default", usageLogFilter{}}, {"username", usageLogFilter{Username: "GT_Infra001"}},
		{"username_like", usageLogFilter{Username: "%Infra%"}}, {"channel", usageLogFilter{Channel: 269}},
		{"username_channel", usageLogFilter{Username: "GT_Infra001", Channel: 269}},
		{"model", usageLogFilter{ModelName: "gpt-6.1-sol"}}, {"model_like", usageLogFilter{ModelName: "gpt%"}},
		{"token", usageLogFilter{TokenName: "lsgpt-test"}}, {"group", usageLogFilter{Group: "codex-1.2x"}},
		{"error", usageLogFilter{Type: 5}}, {"request", usageLogFilter{RequestID: "audit-absent-request"}},
		{"upstream_request", usageLogFilter{UpstreamRequestID: "audit-absent-upstream"}},
	} {
		f := v.f
		f.Start, f.End = from, to
		w, err := buildUsageLogWhere(f, usageLogWhereList)
		if err != nil {
			t.Fatal(err)
		}
		for _, page := range []struct {
			name string
			p    int
			c    *usageLogCursor
		}{{"first", 1, nil}, {"offset", 3, nil}, {"cursor", 2, &usageLogCursor{CreatedAt: to - 1, ID: 15889972}}} {
			if f.RequestID != "" || f.UpstreamRequestID != "" {
				q, a = usageLogsIDPageSQL(w, page.c, page.p, 10)
			} else {
				q, a = usageLogsPageSQL(w, "logs", page.p, 10, page.c, true)
			}
			add("serveUsageLogsList", v.name+"/"+page.name, q, a...)
		}
		add("queryUsageLogsCount/native", v.name, usageLogsSelect+"COUNT(*) FROM logs"+w.sql(), w.args...)
		if usageLogsUseCountSpan(f) {
			p, pa := usageLogsAggregateProbeSQL(f, w, "COUNT(*)")
			add("queryUsageLogsCount/planning", v.name, p, pa...)
			add("queryUsageLogsCount/time", v.name, usageLogsSelect+"COUNT(*) FROM logs FORCE INDEX (idx_created_at_type)"+w.sql(), w.args...)
			q, a = usageLogsAggregateSpanSQL(f, w, "COUNT(*)")
			add("queryUsageLogsCount/span", v.name, q, a...)
		}
		w, _ = buildUsageLogWhere(f, usageLogWhereStat)
		add("queryUsageLogsQuota/native", v.name, usageLogsSelect+"COALESCE(SUM(quota),0) FROM logs"+w.sql(), w.args...)
		if usageLogsUseQuotaSpan(f) {
			planning := f
			planning.Type = 0
			p, pa := usageLogsAggregateProbeSQL(planning, w, "COALESCE(SUM(quota),0)")
			add("queryUsageLogsQuota/planning", v.name, p, pa...)
			add("queryUsageLogsQuota/time", v.name, usageLogsSelect+"COALESCE(SUM(quota),0) FROM logs FORCE INDEX (idx_created_at_type)"+w.sql(), w.args...)
			q, a = usageLogsQuotaSpanSQL(f, w)
			add("queryUsageLogsQuota/span", v.name, q, a...)
		}
		w, _ = buildUsageLogWhere(f, usageLogWhereRate)
		w.add("created_at >= ?", to-60)
		add("serveUsageLogsStat/rate", v.name, usageLogsSelect+"COUNT(*), COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0) FROM logs"+w.sql(), w.args...)
	}
	// Exercise original methods: no copied SQL, no network-capable driver.
	render := func(owner string, fn func(context.Context)) {
		r.owner, r.variant = owner, "synthetic-catalog-inputs"
		m.backgroundSourceScheduleMu.Lock()
		m.stabilitySourceNotBefore = time.Time{}
		m.backgroundSourceScheduleMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		fn(ctx)
	}
	render("loadChannelCostLocalKeyOwners", func(c context.Context) { _, _ = m.loadChannelCostLocalKeyOwners(c, "example.invalid") })
	render("resolveNewAPIUser", func(c context.Context) { _, _ = m.resolveNewAPIUser(c, "168") })
	render("refreshTrackedLabels", func(c context.Context) { _, _, _ = m.refreshTrackedLabels(c, []TrackedUser{{UserID: uid}}) })
	render("fetchGroupGovernanceOptions", func(c context.Context) { _, _ = m.fetchGroupGovernanceOptions(c) })
	render("fetchGroupGovernanceUsers", func(c context.Context) { _, _ = m.fetchGroupGovernanceUsers(c) })
	render("fetchGroupGovernanceTokens", func(c context.Context) { _, _ = m.fetchGroupGovernanceTokens(c, to) })
	render("fetchGroupGovernanceSubscriptions", func(c context.Context) { _, _, _ = m.fetchGroupGovernanceSubscriptions(c) })
	render("refreshChannelsContext", func(c context.Context) { _ = m.refreshChannelsContext(c) })
	render("mainSiteSMTP", func(context.Context) { _, _ = m.mainSiteSMTP() })
	render("computeUsageStats", func(c context.Context) { _, _ = m.computeUsageStats(c, ids, from, to, 0) })
	render("computeUsageStats/token", func(c context.Context) { _, _ = m.computeUsageStats(c, ids, from, to, 1) })
	render("computeUsageMatrix", func(c context.Context) { _, _ = m.computeUsageMatrix(c, ids, from, to) })
	render("userLiveUsage", func(c context.Context) { _, _, _ = m.userLiveUsage(c, uid) })
	render("tokenMetaOf", func(c context.Context) { _ = m.tokenMetaOf(c, uid, 1) })
	render("computeUserTokenAggregates", func(c context.Context) { _, _ = m.computeUserTokenAggregates(c, uid, from, to) })
	render("hydrateUserTokenUsage", func(c context.Context) {
		_, _ = m.hydrateUserTokenUsage(c, uid, []tokenUsageAggregate{{TokenID: 1}}, nil)
	})
	render("sumUsageLiveQuotas", func(c context.Context) { _, _ = m.sumUsageLiveQuotas(c, ids) })
	render("fetchUsageFactHourWithPriority", func(c context.Context) { _, _ = m.fetchUsageFactHour(c, from, ids) })
	render("fetchUsageFactRawPage", func(c context.Context) { _, _ = m.fetchUsageFactRawPage(c, uid, from, to, from, 2, 0, 100, false) })
	render("discoverUsageFactSourceBoundaries", func(c context.Context) { _, _ = m.discoverUsageFactSourceBoundaries(c, ids) })
	render("readUserDirectory", func(c context.Context) { _, _ = m.readUserDirectory(c) })
	render("fetchConfiguredWebsiteGroups", func(c context.Context) { _, _ = m.fetchConfiguredWebsiteGroups(c) })
	render("fetchEffectiveWebsiteGroups", func(c context.Context) { _, _ = m.fetchEffectiveWebsiteGroups(c) })
	render("fetchWebsiteGroupSources", func(c context.Context) { _, _, _ = m.fetchWebsiteGroupSources(c) })
	render("loadUsageLogsOptions", func(c context.Context) { _, _ = m.loadUsageLogsOptions(c, db) })
	render("usageOwnerLabel", func(c context.Context) { _ = m.usageOwnerLabel(c, uid) })
	if err := m.storeDB.Create(&TrackedUser{UserID: uid}).Error; err != nil {
		t.Fatal(err)
	}
	render("syncUsageProfiles", func(c context.Context) { _ = m.syncUsageProfiles(c, time.Unix(to, 0)) })
	render("financeCreditUserCreatedAt", func(c context.Context) { _, _ = financeCreditUserCreatedAt(c, db, map[int64]struct{}{uid: {}}) })
	render("classifyCustomerHealthErrors", func(c context.Context) {
		_ = classifyCustomerHealthErrors(c, db, from, to, []CapacityUserMinuteSample{{BucketTs: from, UserID: uid, Failed: 1}})
	})
	render("ggErrorProjection.collectDelivery", func(c context.Context) {
		p := ggErrorProjection{rows: map[ggMinuteKey]*GroupGovernanceErrorMinute{{At: from}: {Anomaly: 1}}, reasons: map[ggMinuteKey]map[string]*ggErrorReason{}}
		p.collectDelivery(c, db, from, to)
	})
	render("readTokenMeteringPageWithBudgets", func(c context.Context) {
		_, _, _ = m.readTokenMeteringPage(c, TokenMeteringWindow{StartAt: from, EndAt: to, CursorAt: from}, 10)
	})
	render("readPortalLogColumns", func(c context.Context) { _, _ = readPortalLogColumns(c, db) })
	for i, query := range sourcePreflightQueriesForSettings(Settings{UsageFactsEnabled: true, UsageFactsFullHistoryEnabled: true}) {
		add("probeSource", fmt.Sprintf("schema-%d", i), query)
	}
	for i, query := range sourcePreflightQueriesForSettings(Settings{LogChainOnlySource: true}) {
		add("probeSource/logchain", fmt.Sprintf("schema-%d", i), query)
	}
	// Additional source paths, rendered through their original methods.
	m.portalLogColumns.db = db
	m.portalLogColumns.expires = time.Now().Add(time.Hour)
	m.portalLogColumns.columns = portalLogColumnSet{ip: true, upstreamID: true}
	for _, filter := range []struct{ name, model, group, token, detail, request string }{
		{name: "plain"}, {name: "model", model: "gpt-6.1-sol"}, {name: "group", group: "codex-1.2x"}, {name: "token", token: "audit"}, {name: "detail", detail: "timeout"}, {name: "request", request: "audit-absent-request"},
	} {
		render("queryGroupLogs/"+filter.name, func(c context.Context) {
			_, _ = m.queryGroupLogs(c, ids, from, to, uid, 0, filter.model, filter.group, filter.token, filter.detail, filter.request, 0, 10)
		})
		render("countGroupLogs/"+filter.name, func(c context.Context) {
			_, _ = m.countGroupLogs(c, ids, from, to, uid, 0, filter.model, filter.group, filter.token, filter.detail, filter.request)
		})
	}
	render("countGroupLogsSnapshot", func(c context.Context) {
		_, _, _ = m.countGroupLogsSnapshot(c, ids, from, to, uid, 0, "", "", "", "", "")
	})
	render("continuePagedProblemIngest", func(c context.Context) {
		_, _ = m.continuePagedProblemIngest(c, &StabilityProblemIngestState{BucketTs: from})
	})
	source := newSimpleReplicaSource(db)
	render("simpleReplicaSource.Registrations", func(c context.Context) { _, _ = source.Registrations(c, ids) })
	render("simpleReplicaSource.Snapshot", func(c context.Context) { _, _ = source.Snapshot(c, ids, from, to) })
	for _, scope := range []struct {
		name string
		s    logChainScope
	}{
		{"plain", logChainScope{}}, {"user", logChainScope{UserID: uid}}, {"channel", logChainScope{ChannelID: 269}}, {"model", logChainScope{Model: "gpt-6.1-sol"}},
		{"token", logChainScope{TokenName: "audit"}}, {"request", logChainScope{RequestID: "audit-absent-request"}},
		{"errors", logChainScope{ErrorOnly: true}}, {"stream", logChainScope{Anomaly: anomalyStream}}, {"combined_anomaly", logChainScope{Anomaly: anomalyAll}},
	} {
		s := scope.s
		s.FromTs, s.ToTs, s.Limit = from, to, 10
		q, args := m.logChainPageQuery(s, nil)
		add("logChainPageQuery", scope.name, q, args...)
	}
	data, err := json.MarshalIndent(struct {
		Warning string                `json:"warning"`
		Cases   []mysqlAuditStatement `json:"cases"`
	}{"Rendered templates and explicit structural variants, not all possible input combinations. Network-impossible recorder with minimal synthetic user/subscription scaffolding; other scalar-dependent branches remain gaps. Not yet EXPLAINed.", r.items}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(abs, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("rendered MySQL catalog cases=%d bytes=%d", len(r.items), len(data))
}
