package monitor

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// A channel addressed by its supplier's origin IP is attributed to the
// supplier's configured domain once aliased; NewAPI keeps routing to the IP.
func TestChannelDomainAliasAttributesOriginIPChannel(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("CREATE TABLE channels (id INTEGER PRIMARY KEY,name TEXT,type INTEGER,status INTEGER,`group` TEXT,models TEXT,base_url TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO channels (id,name,type,status,`group`,models,base_url) VALUES (250,'rkshadow-claude',60,1,'claude','claude-opus','http://148.163.65.162')"); err != nil {
		t.Fatal(err)
	}
	m.refreshChannels()
	snap := func() ChannelSnap {
		t.Helper()
		var s ChannelSnap
		if err := m.storeDB.First(&s, "id = ?", 250).Error; err != nil {
			t.Fatal(err)
		}
		return s
	}
	if got := snap(); got.BaseDomain != "148.163.65.162" {
		t.Fatalf("unaliased domain=%q", got.BaseDomain)
	}
	router := gin.New()
	router.POST("/channels/domain-alias", m.requireRole(roleRoot), m.saveChannelDomainAliasHandler)
	post := func(alias, domain string) (int, string) {
		w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/domain-alias", channelDomainAliasInput{Alias: alias, Domain: domain})
		return w.Code, w.Body.String()
	}
	if code, body := post("148.163.65.162", "rkshadow.com"); code == http.StatusOK || !strings.Contains(body, "尚未配置上游账户") {
		t.Fatalf("target without an upstream account must be refused: %d %s", code, body)
	}
	if err := m.storeDB.Create(&ChannelUpstreamAccount{Domain: "rkshadow.com", Provider: upstreamProviderSub2API, BaseURL: "https://sub.rkshadow.com", Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	if code, body := post("unknown.example", "rkshadow.com"); code == http.StatusOK || !strings.Contains(body, "不是当前任何渠道") {
		t.Fatalf("an address no channel uses must be refused: %d %s", code, body)
	}
	if code, body := post("148.163.65.162", "rkshadow.com"); code != http.StatusOK {
		t.Fatalf("alias refused: %d %s", code, body)
	}
	if code, body := post("rkshadow.com", "other.example"); code == http.StatusOK {
		t.Fatalf("chained alias must be refused: %d %s", code, body)
	}
	m.refreshChannels()
	if got := snap(); got.BaseDomain != "rkshadow.com" || got.BaseHost != "148.163.65.162" {
		t.Fatalf("aliased channel=%+v", got)
	}
	hour := time.Date(2026, 9, 18, 10, 0, 0, 0, cstLocation).Unix()
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+7200)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range report.Domains {
		if d.Domain == "rkshadow.com" && len(d.Aliases) == 1 && d.Aliases[0] == "148.163.65.162" {
			found = true
		}
		if d.Domain == "148.163.65.162" {
			t.Fatal("aliased address must not remain a separate supplier")
		}
	}
	if !found {
		t.Fatalf("target domain must list its alias: %+v", report.Domains)
	}
	if code, body := post("148.163.65.162", ""); code != http.StatusOK {
		t.Fatalf("remove alias: %d %s", code, body)
	}
	m.refreshChannels()
	if got := snap(); got.BaseDomain != "148.163.65.162" {
		t.Fatalf("removed alias must restore the raw address: %q", got.BaseDomain)
	}
}

// An address that has its own upstream account cannot be aliased: the same
// upstream spend would be booked twice.
func TestChannelDomainAliasRefusesAccountedAddress(t *testing.T) {
	m := newStabilityTestMonitor(t)
	for _, row := range []ChannelSnap{{ID: 1, BaseDomain: "a.example", BaseHost: "a.example", Status: 1}, {ID: 2, BaseDomain: "b.example", BaseHost: "b.example", Status: 1}} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, domain := range []string{"a.example", "b.example"} {
		if err := m.storeDB.Create(&ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: "https://" + domain, Enabled: true}).Error; err != nil {
			t.Fatal(err)
		}
	}
	router := gin.New()
	router.POST("/channels/domain-alias", m.requireRole(roleRoot), m.saveChannelDomainAliasHandler)
	w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/domain-alias", channelDomainAliasInput{Alias: "a.example", Domain: "b.example"})
	if w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "重复计算") {
		t.Fatalf("accounted address alias must be refused: %d %s", w.Code, w.Body.String())
	}
	if err := m.storeDB.Model(&ChannelDomainAlias{}).Count(new(int64)).Error; err != nil {
		t.Fatal(err)
	}
}
