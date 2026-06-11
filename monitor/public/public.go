// Package public 是对外公开的「服务状态看板」,与内部监控同进程、同本地库,但【强隔离】:
//   - 独立包,绝不 import 内部 monitor 的任何结构(Snapshot/Row/TokenRow…),
//     输出全部用本包自己的脱敏结构体从零拼,编译层杜绝内部数据外泄;
//   - 只读本地采样库(metric_samples 流量 + channel_snaps 渠道健康),不碰生产库;
//   - 维度 = 线路(分组)× 模型,渠道对用户透明;可见分组取自 new-api /api/pricing 的 usable_group。
//
// 公开面【绝不输出】:渠道名/ID/IP、成本/配额、令牌/用户、请求量/QPS、错误详情。
package public

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

//go:embed status.html
var statusHTML []byte

const (
	windowDays      = 7 // 可用率/状态条窗口
	windowSec       = int64(windowDays * 86400)
	beatCount       = 50            // 心跳条桶数
	beatMinSample   = 5             // 单个心跳桶请求数低于此 → 不画红/黄(几条里挂1个不代表服务差,公开页避免被噪声误导)
	minSample       = 20            // 窗口内请求数低于此 → 不据流量判故障
	recentWindowSec = int64(86400)  // 判定【当下】状态的近期窗口(24h):状态看当下,不被旧数据钉死
	staleAfterSec   = int64(172800) // 最新流量早于此(48h)→ 可用率/延迟视为陈旧,显 — 不展示
	snapTTL         = 30            // 看板快照缓存(秒)
	groupsTTL       = 300           // 可见分组缓存(秒)
)

// 状态枚举(对齐 Uptime Kuma 习惯):1 正常 / 2 降级 / 0 不可用 / 3 维护 / -1 数据不足。
const (
	stDown   = 0
	stUp     = 1
	stWarn   = 2
	stMaint  = 3
	stNoData = -1
)

// Config 是看板所需的最小配置(由 monitor 注入)。
type Config struct {
	NewAPIBaseURL string // 用于匿名拉取 /api/pricing 的可见分组;为空则退回"有流量的分组"
	SiteName      string // 站点名:部署时从主站 system_name 同步;为空则前端显通用名(不硬编码)
	Logo          string // 站点 logo 绝对 URL:从主站同步,供前端做 favicon;可空
}

// ---- 对外脱敏数据结构(独立,绝不复用内部结构)----

// Model 是某线路下某模型对外可见的状态:仅状态/可用率/延迟/心跳条,无任何内部明细。
type Model struct {
	Provider  string   `json:"provider"`   // anthropic/openai/google/deepseek/other
	Name      string   `json:"name"`       // 模型友好名(去日期后缀)
	Status    int      `json:"status"`     // 见状态枚举
	Uptime    *float64 `json:"uptime"`     // 近7天可用率 0-1;数据不足为 null
	LatencyMs *int     `json:"latency_ms"` // P50 延迟(ms);无则 null
	Beats     []int    `json:"beats"`      // 近7天逐桶状态(老→新),仅状态枚举,无任何明细
}

// Group 是一条线路(= 用户建令牌所选分组)及其下模型的对外状态。
type Group struct {
	Name   string  `json:"name"` // 线路显示名(= 分组 key)
	Status int     `json:"status"`
	Models []Model `json:"models"`
}

// Snapshot 是一次完整的对外看板快照:站点品牌 + 整体状态 + 各线路 × 模型。
type Snapshot struct {
	Site      string  `json:"site"`           // 站点名(从主站同步;空=前端用通用名)
	Logo      string  `json:"logo,omitempty"` // 站点 logo URL(favicon)
	UpdatedAt string  `json:"updated_at"`
	Overall   int     `json:"overall"`
	Groups    []Group `json:"groups"`
}

// ---- handler ----

type handler struct {
	db  *gorm.DB
	cfg Config

	snapMu sync.Mutex
	snap   *Snapshot
	snapAt int64

	grpMu  sync.Mutex
	groups []vgroup
	grpAt  int64
}

type vgroup struct{ Key, Name string }

// Register 把看板挂到给定引擎:GET /status(页面) + GET /public/status(脱敏 JSON)。均【无鉴权】。
func Register(r *gin.Engine, db *gorm.DB, cfg Config) {
	h := &handler{db: db, cfg: cfg} // SiteName 可空——前端回退到通用名,不硬编码品牌
	r.GET("/status", h.page)
	r.GET("/public/status", h.data)
}

func (h *handler) page(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=300")
	c.Data(http.StatusOK, "text/html; charset=utf-8", statusHTML)
}

func (h *handler) data(c *gin.Context) {
	now := time.Now().Unix()
	h.snapMu.Lock()
	if h.snap != nil && now-h.snapAt < snapTTL {
		snap := h.snap
		h.snapMu.Unlock()
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, snap)
		return
	}
	h.snapMu.Unlock()

	snap := h.compute(now)

	h.snapMu.Lock()
	h.snap, h.snapAt = snap, now
	h.snapMu.Unlock()
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, snap)
}

// ---- 聚合 ----

type agg struct {
	Success, Anomaly, Failed, Err4xx int64
	Lat                              [7]int64
	Max                              int64
}

func (h *handler) compute(now int64) *Snapshot {
	since := now - windowSec

	// 1) 渠道健康快照 → 每(分组,模型)的"在售"与"健康渠道数"
	offered, enabled := h.channelMaps()

	// 2) 近7天每(分组×模型)汇总
	totals := h.totals(since)

	// 3) 近7天每(分组×模型)分桶序列 → 心跳条
	series := h.series(since)

	// 4) 可见分组(令牌可选的)
	vgs := h.visibleGroups(now, totals)

	var groups []Group
	overall := stUp
	for _, vg := range vgs {
		models := mergedModels(offered[vg.Key], totals, vg.Key)
		if len(models) == 0 {
			continue
		}
		var pms []Model
		for _, mdl := range models {
			pms = append(pms, h.buildModel(vg.Key, mdl, enabled, totals, series, now, since))
		}
		sort.Slice(pms, func(i, j int) bool {
			if pms[i].Provider != pms[j].Provider {
				return pms[i].Provider < pms[j].Provider
			}
			return pms[i].Name < pms[j].Name
		})
		gStatus := groupStatus(pms)
		groups = append(groups, Group{Name: vg.Name, Status: gStatus, Models: pms})
		overall = worse(overall, gStatus)
	}

	return &Snapshot{
		Site:      h.cfg.SiteName,
		Logo:      h.cfg.Logo,
		UpdatedAt: time.Unix(now, 0).UTC().Format(time.RFC3339),
		Overall:   overall,
		Groups:    groups,
	}
}

func (h *handler) buildModel(grp, mdl string, enabled map[string]map[string]int, totals map[string]agg, series map[string][]seriesPt, now, since int64) Model {
	key := grp + "\x00" + mdl
	a := totals[key]
	pts := series[key]
	en := 0
	if mm := enabled[grp]; mm != nil {
		en = mm[mdl]
	}

	pm := Model{Provider: provider(mdl), Name: pretty(mdl), Beats: buildBeats(pts, now, since)}

	// 拓扑优先:配置在册但 0 健康渠道 → 无可用渠道(不可用),不靠流量猜。
	if en == 0 {
		pm.Status = stDown
		if n := len(pm.Beats); n > 0 {
			pm.Beats[n-1] = stDown // 当下不可用,末桶标红
		}
		return pm
	}

	// 从分桶序列取「最新流量时间」与「近期窗口」流量,用于判定【当下】状态(不被一周前的旧数据钉死)。
	var latest, recUp, recFail int64
	for _, p := range pts {
		if p.Ts > latest {
			latest = p.Ts
		}
		if p.Ts >= now-recentWindowSec {
			recUp += p.Up
			recFail += p.Fail
		}
	}

	// 状态(颜色)= 当下:近期流量太少 → 有健康渠道即视为正常,不拿陈旧数据判死;否则按近期可用率定档。
	if recUp+recFail < minSample {
		pm.Status = stUp
	} else {
		pm.Status = band(float64(recUp) / float64(recUp+recFail))
	}

	// 可用率%/延迟 = 近7天,但仅当数据【足够且不陈旧】才展示,否则显 —(避免拿几天前的旧值误导)。
	total := a.Success + a.Anomaly + a.Failed
	fresh := latest >= now-staleAfterSec
	if fresh && total >= minSample {
		if denom := total - a.Err4xx; denom > 0 { // 排除用户侧 4xx
			r := float64(a.Success+a.Anomaly) / float64(denom)
			pm.Uptime = &r
		}
	}
	if fresh {
		if lat := p50ms(a); lat > 0 {
			pm.LatencyMs = &lat
		}
	}
	return pm
}

// ---- 本地库查询 ----

func (h *handler) channelMaps() (offered map[string]map[string]bool, enabled map[string]map[string]int) {
	offered = map[string]map[string]bool{}
	enabled = map[string]map[string]int{}
	var rows []struct {
		Status int    `gorm:"column:status"`
		Groups string `gorm:"column:groups"`
		Models string `gorm:"column:models"`
	}
	if err := h.db.Raw("SELECT status, groups, models FROM channel_snaps").Scan(&rows).Error; err != nil {
		slog.Warn("看板:渠道快照查询失败(降级为空)", "err", err)
	}
	for _, r := range rows {
		gs := splitList(r.Groups)
		ms := splitList(r.Models)
		for _, g := range gs {
			for _, m := range ms {
				if offered[g] == nil {
					offered[g] = map[string]bool{}
				}
				offered[g][m] = true
				if r.Status == 1 {
					if enabled[g] == nil {
						enabled[g] = map[string]int{}
					}
					enabled[g][m]++
				}
			}
		}
	}
	return
}

func (h *handler) totals(since int64) map[string]agg {
	var rows []struct {
		Grp     string `gorm:"column:grp"`
		Model   string `gorm:"column:model"`
		Success int64  `gorm:"column:success"`
		Anomaly int64  `gorm:"column:anomaly"`
		Failed  int64  `gorm:"column:failed"`
		Err4xx  int64  `gorm:"column:err4xx"`
		L1      int64  `gorm:"column:l1"`
		L2      int64  `gorm:"column:l2"`
		L5      int64  `gorm:"column:l5"`
		L10     int64  `gorm:"column:l10"`
		L30     int64  `gorm:"column:l30"`
		L60     int64  `gorm:"column:l60"`
		LInf    int64  `gorm:"column:linf"`
		Mx      int64  `gorm:"column:mx"`
	}
	if err := h.db.Raw(`SELECT grp, model_name AS model,
		COALESCE(SUM(success),0) success, COALESCE(SUM(anomaly),0) anomaly, COALESCE(SUM(failed),0) failed,
		COALESCE(SUM(err_4xx),0) err4xx,
		COALESCE(SUM(lat_1),0) l1, COALESCE(SUM(lat_2),0) l2, COALESCE(SUM(lat_5),0) l5, COALESCE(SUM(lat_10),0) l10,
		COALESCE(SUM(lat_30),0) l30, COALESCE(SUM(lat_60),0) l60, COALESCE(SUM(lat_inf),0) linf,
		COALESCE(MAX(max_use_time),0) mx
		FROM metric_samples WHERE bucket_ts >= ?`+enabledChanFilter+` GROUP BY grp, model_name`, since).Scan(&rows).Error; err != nil {
		slog.Warn("看板:分组×模型汇总查询失败(降级为空)", "err", err)
	}
	out := make(map[string]agg, len(rows))
	for _, r := range rows {
		out[r.Grp+"\x00"+r.Model] = agg{
			Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed, Err4xx: r.Err4xx,
			Lat: [7]int64{r.L1, r.L2, r.L5, r.L10, r.L30, r.L60, r.LInf}, Max: r.Mx,
		}
	}
	return out
}

// enabledChanFilter:看板稳定性排除"已知被禁用 / 在其启用时刻之前"的渠道流量,与内部监控一致。
// 用 NOT EXISTS(反向):没有渠道快照的流量默认保留(fail-open),避免新部署空窗全空。
// 详见 monitor 包 store.go 同名常量说明。
const enabledChanFilter = ` AND NOT EXISTS (SELECT 1 FROM channel_snaps c ` +
	`WHERE c.id = metric_samples.channel_id AND (c.status <> 1 OR metric_samples.bucket_ts < c.enabled_since))`

type seriesPt struct {
	Ts   int64
	Up   int64
	Fail int64
}

func (h *handler) series(since int64) map[string][]seriesPt {
	var rows []struct {
		Grp      string `gorm:"column:grp"`
		Model    string `gorm:"column:model"`
		BucketTs int64  `gorm:"column:bucket_ts"`
		Up       int64  `gorm:"column:up"`
		Fail     int64  `gorm:"column:fail"`
	}
	if err := h.db.Raw(`SELECT grp, model_name AS model, bucket_ts,
		COALESCE(SUM(success+anomaly),0) up, COALESCE(SUM(failed),0) fail
		FROM metric_samples WHERE bucket_ts >= ?`+enabledChanFilter+` GROUP BY grp, model_name, bucket_ts ORDER BY bucket_ts`, since).Scan(&rows).Error; err != nil {
		slog.Warn("看板:分桶序列查询失败(降级为空)", "err", err)
	}
	out := map[string][]seriesPt{}
	for _, r := range rows {
		k := r.Grp + "\x00" + r.Model
		out[k] = append(out[k], seriesPt{Ts: r.BucketTs, Up: r.Up, Fail: r.Fail})
	}
	return out
}

// visibleGroups 拉取 new-api 可见分组(令牌可选);失败则退回"近窗有流量的分组"。带缓存。
func (h *handler) visibleGroups(now int64, totals map[string]agg) []vgroup {
	h.grpMu.Lock()
	if h.groups != nil && now-h.grpAt < groupsTTL {
		g := h.groups
		h.grpMu.Unlock()
		return g
	}
	h.grpMu.Unlock()

	vgs := h.fetchUsableGroups()
	if len(vgs) == 0 { // 兜底:有流量的分组
		seen := map[string]bool{}
		for k := range totals {
			g := strings.SplitN(k, "\x00", 2)[0]
			if g != "" && !seen[g] {
				seen[g] = true
				vgs = append(vgs, vgroup{Key: g, Name: g})
			}
		}
	}
	sort.Slice(vgs, func(i, j int) bool { return vgs[i].Key < vgs[j].Key })

	h.grpMu.Lock()
	h.groups, h.grpAt = vgs, now
	h.grpMu.Unlock()
	return vgs
}

func (h *handler) fetchUsableGroups() []vgroup {
	if h.cfg.NewAPIBaseURL == "" {
		return nil
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get(strings.TrimRight(h.cfg.NewAPIBaseURL, "/") + "/api/pricing")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var body struct {
		UsableGroup map[string]string `json:"usable_group"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil
	}
	var vgs []vgroup
	for k := range body.UsableGroup {
		if k == "" {
			continue
		}
		// 显示名直接用分组 key(= 用户建令牌所选,如 codex-1.2x);
		// usable_group 的描述(desc)可能含"逆向/openclaw/折扣"等敏感字样,不对外。
		vgs = append(vgs, vgroup{Key: k, Name: k})
	}
	return vgs
}

// ---- 纯函数辅助 ----

// mergedModels 合并"配置在售"与"近窗有流量"的模型集合,得到该线路完整服务面。
func mergedModels(offeredG map[string]bool, totals map[string]agg, grp string) []string {
	set := map[string]bool{}
	for m := range offeredG {
		if m != "" && m != "*" {
			set[m] = true
		}
	}
	prefix := grp + "\x00"
	for k := range totals {
		if strings.HasPrefix(k, prefix) {
			if m := strings.TrimPrefix(k, prefix); m != "" {
				set[m] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	return out
}

// buildBeats 把 7 天序列压成 beatCount 个状态桶(老→新)。无流量桶视为正常。
func buildBeats(pts []seriesPt, now, since int64) []int {
	beats := make([]int, beatCount)
	sums := make([]struct{ up, fail int64 }, beatCount)
	slice := float64(now-since) / float64(beatCount)
	for _, p := range pts {
		idx := int(float64(p.Ts-since) / slice)
		if idx < 0 {
			idx = 0
		}
		if idx >= beatCount {
			idx = beatCount - 1
		}
		sums[idx].up += p.Up
		sums[idx].fail += p.Fail
	}
	for i := range beats {
		t := sums[i].up + sums[i].fail
		if t < beatMinSample { // 样本太少(含空桶)不画红/黄:避免少量请求里的偶发失败在公开页呈现为"差"
			beats[i] = stUp
			continue
		}
		beats[i] = band(float64(sums[i].up) / float64(t))
	}
	return beats
}

// band 把成功率映射成状态:"不可用"只留给真·调不通(<50%,基本全挂);
// 在服务但有失败(50–99%)算"降级",如实标但不夸大成不可用;≥99% 正常。
func band(rate float64) int {
	switch {
	case rate >= 0.99:
		return stUp
	case rate >= 0.50:
		return stWarn
	default:
		return stDown
	}
}

// groupStatus 按"线路还能不能用"判分组状态,不取最差模型——避免个别模型降级
// 就把整条线标成"不可用"(对外夸大)。有正常模型就最多算"降级";
// 没有任何正常模型才"不可用";没有任何问题则"正常"(忽略维护/数据不足)。
func groupStatus(models []Model) int {
	up, problem := 0, 0
	for _, m := range models {
		switch m.Status {
		case stUp:
			up++
		case stWarn, stDown:
			problem++
		}
	}
	switch {
	case problem == 0:
		return stUp
	case up == 0:
		return stDown
	default:
		return stWarn
	}
}

// worse 返回两个状态里"更严重"的。严重度:不可用 > 降级 > 数据不足 > 维护 > 正常。
func worse(a, b int) int {
	if sev(b) > sev(a) {
		return b
	}
	return a
}

func sev(s int) int {
	switch s {
	case stDown:
		return 4
	case stWarn:
		return 3
	case stNoData:
		return 2
	case stMaint:
		return 1
	default: // stUp
		return 0
	}
}

func provider(model string) string {
	m := strings.ToLower(model)
	switch {
	case strings.HasPrefix(m, "claude"):
		return "anthropic"
	case strings.HasPrefix(m, "gemini"):
		return "google"
	case strings.HasPrefix(m, "deepseek"):
		return "deepseek"
	case strings.HasPrefix(m, "glm"):
		return "zhipu"
	case strings.HasPrefix(m, "gpt"), strings.HasPrefix(m, "o1"), strings.HasPrefix(m, "o3"),
		strings.HasPrefix(m, "o4"), strings.HasPrefix(m, "chatgpt"), strings.HasPrefix(m, "dall"),
		strings.Contains(m, "codex"):
		return "openai"
	default:
		return "other"
	}
}

var dateSuffix = regexp.MustCompile(`-\d{8}$`)

func pretty(model string) string { return dateSuffix.ReplaceAllString(model, "") }

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

var latEdges = []int64{1, 2, 5, 10, 30, 60}

// p50ms 由总延迟直方图近似 P50(秒),返回毫秒。
func p50ms(a agg) int {
	var total int64
	for _, c := range a.Lat {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := float64(total) * 0.5
	var cum, lower float64
	for i, c := range a.Lat {
		upper := float64(a.Max)
		if i < len(latEdges) {
			upper = float64(latEdges[i])
		}
		if upper < lower {
			upper = lower
		}
		if cum+float64(c) >= target {
			if c == 0 {
				return int(upper * 1000)
			}
			return int((lower + (target-cum)/float64(c)*(upper-lower)) * 1000)
		}
		cum += float64(c)
		lower = upper
	}
	return int(lower * 1000)
}
