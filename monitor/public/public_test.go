package public

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 测试用的表结构(列名与生产一致),避免 import monitor 造成循环。
type metricSample struct {
	BucketTs   int64  `gorm:"primaryKey;autoIncrement:false"`
	ChannelID  int    `gorm:"primaryKey;autoIncrement:false"`
	ModelName  string `gorm:"primaryKey;size:128"`
	Grp        string `gorm:"primaryKey;size:64;column:grp"`
	Success    int64
	Anomaly    int64
	Failed     int64
	MaxUseTime int
	Err4xx     int64 `gorm:"column:err_4xx"`
	Lat1       int64 `gorm:"column:lat_1"`
	Lat2       int64 `gorm:"column:lat_2"`
	Lat5       int64 `gorm:"column:lat_5"`
	Lat10      int64 `gorm:"column:lat_10"`
	Lat30      int64 `gorm:"column:lat_30"`
	Lat60      int64 `gorm:"column:lat_60"`
	LatInf     int64 `gorm:"column:lat_inf"`
}

func (metricSample) TableName() string { return "metric_samples" }

type channelSnap struct {
	ID           int `gorm:"primaryKey;autoIncrement:false"`
	Status       int
	Groups       string
	Models       string
	EnabledSince int64
	UpdatedAt    int64
}

func (channelSnap) TableName() string { return "channel_snaps" }

func testDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.AutoMigrate(&metricSample{}, &channelSnap{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestComputeTopologyAndTraffic(t *testing.T) {
	db := testDB(t)
	now := int64(1_900_000_000)
	bt := now - 3600 // 窗口内

	// 渠道:m_ok、m_warn 各有 1 条启用;m_down 只在 1 条禁用渠道上(→ 不可选,应隐藏)
	db.Create(&[]channelSnap{
		{ID: 1, Status: 1, Groups: "g1", Models: "m_ok,m_warn", UpdatedAt: now},
		{ID: 2, Status: 3, Groups: "g1", Models: "m_down", UpdatedAt: now}, // 自动禁用 → m_down 不可选
	})
	// 流量:m_ok 全成功;m_warn 96%(降级)
	db.Create(&[]metricSample{
		{BucketTs: bt, ChannelID: 1, ModelName: "m_ok", Grp: "g1", Success: 30, Lat2: 30, MaxUseTime: 2},
		{BucketTs: bt, ChannelID: 1, ModelName: "m_warn", Grp: "g1", Success: 24, Failed: 1, Lat5: 24, MaxUseTime: 5},
	})

	h := &handler{db: db, cfg: Config{}} // 无 NewAPIBaseURL → 可见分组退回"有流量分组"
	snap := h.compute(now)

	if snap.Overall != stWarn {
		t.Fatalf("overall = %d, want stWarn(有正常模型 m_ok + 部分降级 → 线路降级)", snap.Overall)
	}
	if len(snap.Groups) != 1 || snap.Groups[0].Name != "g1" {
		t.Fatalf("groups = %+v, want 单个 g1", snap.Groups)
	}
	// 只显示可选模型:m_down 只在禁用渠道上 → 隐藏,只剩 m_ok、m_warn
	if len(snap.Groups[0].Models) != 2 {
		t.Fatalf("应只显示 2 个可选模型(m_down 不可选,隐藏),得 %d", len(snap.Groups[0].Models))
	}
	got := map[string]int{}
	for _, m := range snap.Groups[0].Models {
		got[m.Name] = m.Status
	}
	if _, ok := got["m_down"]; ok {
		t.Error("m_down 只在禁用渠道上(不可选),不该出现在看板")
	}
	if got["m_ok"] != stUp {
		t.Errorf("m_ok = %d, want stUp", got["m_ok"])
	}
	if got["m_warn"] != stWarn {
		t.Errorf("m_warn = %d, want stWarn(96%%)", got["m_warn"])
	}
	// 心跳条长度
	for _, m := range snap.Groups[0].Models {
		if len(m.Beats) != beatCount {
			t.Errorf("%s beats=%d, want %d", m.Name, len(m.Beats), beatCount)
		}
	}
}

// 脱敏:输出 JSON 不得包含任何内部/敏感字段。
func TestPublicSnapshotSanitized(t *testing.T) {
	db := testDB(t)
	now := int64(1_900_000_000)
	db.Create(&channelSnap{ID: 1, Status: 1, Groups: "g1", Models: "claude-opus-4-8", UpdatedAt: now})
	db.Create(&metricSample{BucketTs: now - 3600, ChannelID: 7, ModelName: "claude-opus-4-8", Grp: "g1", Success: 50, Lat2: 50, MaxUseTime: 2})

	h := &handler{db: db, cfg: Config{}}
	b, _ := json.Marshal(h.compute(now))
	s := strings.ToLower(string(b))
	for _, forbidden := range []string{"channel", "cost", "quota", "token", "ip", "qps", "use_time", "err_", "content"} {
		if strings.Contains(s, forbidden) {
			t.Errorf("公开 JSON 含敏感字段 %q: %s", forbidden, b)
		}
	}
	if !strings.Contains(s, "claude-opus-4-8") { // 模型名应在
		t.Errorf("缺模型名: %s", b)
	}
}

func TestEndpoints(t *testing.T) {
	db := testDB(t)
	db.Create(&channelSnap{ID: 1, Status: 1, Groups: "g1", Models: "gpt-5.5", UpdatedAt: 1})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Register(r, db, Config{})

	// /status 出 HTML
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "服务状态") {
		t.Fatalf("/status code=%d 未含页面", w.Code)
	}
	// /public/status 出 JSON
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/public/status", nil))
	if w.Code != 200 {
		t.Fatalf("/public/status code=%d", w.Code)
	}
	var snap Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
		t.Fatalf("JSON 解析失败: %v", err)
	}
}

func TestProvider(t *testing.T) {
	cases := map[string]string{
		"claude-opus-4-8": "anthropic", "gpt-5.5": "openai", "gpt-5.3-codex": "openai",
		"gemini-2.5-pro": "google", "deepseek-v3": "deepseek", "glm-5": "zhipu", "o3": "openai", "weird-model": "other",
	}
	for in, want := range cases {
		if got := provider(in); got != want {
			t.Errorf("provider(%q)=%q want %q", in, got, want)
		}
	}
}

func TestPretty(t *testing.T) {
	if got := pretty("claude-haiku-4-5-20251001"); got != "claude-haiku-4-5" {
		t.Errorf("pretty 去日期后缀失败: %q", got)
	}
	if got := pretty("gpt-5.5"); got != "gpt-5.5" {
		t.Errorf("pretty 不该改无后缀名: %q", got)
	}
}

func TestBandAndWorse(t *testing.T) {
	// 阈值:≥99 正常 / 50–99 降级 / <50 不可用
	if band(0.999) != stUp || band(0.97) != stWarn || band(0.60) != stWarn || band(0.40) != stDown {
		t.Error("band 阈值错")
	}
	if worse(stUp, stDown) != stDown || worse(stWarn, stUp) != stWarn || worse(stDown, stWarn) != stDown {
		t.Error("worse 严重度比较错")
	}
}

// 陈旧数据不应判死:有健康渠道、但只有几天前的失败流量 → 当下应正常、可用率显空。
func TestStaleNotMarkedDown(t *testing.T) {
	db := testDB(t)
	now := int64(1_900_000_000)
	old := now - 4*86400 // 4 天前(>48h 陈旧,且不在 24h 近期窗口)
	db.Create(&channelSnap{ID: 1, Status: 1, Groups: "g1", Models: "m_stale", UpdatedAt: now})
	// 旧流量:26 次里 25 失败(若按 7 天窗口判定会是"不可用")
	db.Create(&metricSample{BucketTs: old, ChannelID: 1, ModelName: "m_stale", Grp: "g1", Success: 1, Failed: 25})

	h := &handler{db: db, cfg: Config{}}
	snap := h.compute(now)
	var got *Model
	for i := range snap.Groups[0].Models {
		if snap.Groups[0].Models[i].Name == "m_stale" {
			got = &snap.Groups[0].Models[i]
		}
	}
	if got == nil {
		t.Fatal("没找到 m_stale")
	}
	if got.Status != stUp {
		t.Errorf("陈旧+健康渠道应为 stUp(正常),得 %d", got.Status)
	}
	if got.Uptime != nil {
		t.Errorf("陈旧数据不应展示可用率,得 %v", *got.Uptime)
	}
}

func TestP50ms(t *testing.T) {
	// 全部落在 (1,2] 档 → P50 ≈ 2s 上界以内,返回毫秒 > 1000
	a := agg{Lat: [7]int64{0, 10, 0, 0, 0, 0, 0}, Max: 2}
	if ms := p50ms(a); ms < 1000 || ms > 2000 {
		t.Errorf("p50ms=%d, want 1000~2000", ms)
	}
	if p50ms(agg{}) != 0 {
		t.Error("空直方图应返回 0")
	}
}

// 禁用渠道的失败不该拖低模型:同模型 m1 由 1 个启用渠道(全成功)+ 1 个手动禁用渠道(全挂)提供,
// 看板应判 m1 正常(stUp),而不是被禁用渠道的失败拖到不可用。这正是线上报的那个 bug。
func TestDisabledChannelExcludedFromBoard(t *testing.T) {
	db := testDB(t)
	now := int64(1_900_000_000)
	bt := now - 1800 // 近窗口内
	db.Create(&[]channelSnap{
		{ID: 1, Status: 1, Groups: "g1", Models: "m1", EnabledSince: now - 86400, UpdatedAt: now}, // 启用
		{ID: 2, Status: 2, Groups: "g1", Models: "m1", EnabledSince: 0, UpdatedAt: now},           // 手动禁用
	})
	db.Create(&[]metricSample{
		{BucketTs: bt, ChannelID: 1, ModelName: "m1", Grp: "g1", Success: 50, Lat2: 50, MaxUseTime: 2}, // 启用渠道:全成功
		{BucketTs: bt, ChannelID: 2, ModelName: "m1", Grp: "g1", Failed: 80},                           // 禁用渠道:全挂 → 应排除
	})
	st := -99
	for _, g := range (&handler{db: db, cfg: Config{}}).compute(now).Groups {
		for _, m := range g.Models {
			if m.Name == "m1" {
				st = m.Status
			}
		}
	}
	if st != stUp {
		t.Fatalf("m1 应为 stUp(禁用渠道的80失败被排除、启用渠道全成功),得 %d", st)
	}
}

// 分组状态:不取最差模型,体现"线路还能不能用"(诚实但不夸大)。
func TestGroupStatus(t *testing.T) {
	cases := []struct {
		name string
		in   []int
		want int
	}{
		{"全正常 → 正常", []int{stUp, stUp}, stUp},
		{"有正常+部分降级/不可用 → 降级(codex-1.2x 场景)", []int{stUp, stUp, stWarn, stDown}, stWarn},
		{"无任何正常模型 → 不可用", []int{stDown, stWarn}, stDown},
		{"全数据不足 → 正常(忽略)", []int{stNoData, stNoData}, stUp},
	}
	for _, c := range cases {
		var models []Model
		for _, s := range c.in {
			models = append(models, Model{Status: s})
		}
		if got := groupStatus(models); got != c.want {
			t.Errorf("%s: got %d want %d", c.name, got, c.want)
		}
	}
}
