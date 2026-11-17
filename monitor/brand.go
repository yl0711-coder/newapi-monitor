package monitor

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// brand.go:站点显示名(品牌)。默认从 new-api 的 /api/status 取一次 system_name 存下来,
// 超管可在报警设置页改。公开库由此保持通用——各部署自动显示各自的 new-api 名称。

// brandName 返回站点显示名:已存则用存的;未存则从 new-api 取一次 system_name 并存下。
// 取不到则返回空,由前端回退到通用默认名。
func (m *Monitor) brandName() string {
	c := m.loadAlertConfig()
	if c.SiteName != "" {
		return c.SiteName
	}
	name := m.fetchSystemName()
	if name != "" {
		c.SiteName = name
		_ = m.saveAlertConfig(c) // 取到则落库,下次直接用存的
	}
	return name
}

// fetchSystemName 从 new-api 读取 system_name(公开接口)。
func (m *Monitor) fetchSystemName() string { n, _ := m.fetchBrand(); return n }

// fetchBrand 从 new-api 的 /api/status 读取站点名(system_name)与 logo(公开接口,无需鉴权)。
// logo 若为相对路径,补全为基于 new-api 地址的绝对 URL,供跨域看板做 favicon。
func (m *Monitor) fetchBrand() (name, logo string) {
	base := strings.TrimRight(m.cfg.NewAPIBaseURL, "/")
	if base == "" {
		return "", ""
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get(base + "/api/status")
	if err != nil {
		return "", ""
	}
	defer resp.Body.Close()
	var sr struct {
		Data struct {
			SystemName string `json:"system_name"`
			Logo       string `json:"logo"`
		} `json:"data"`
	}
	if json.NewDecoder(resp.Body).Decode(&sr) != nil {
		return "", ""
	}
	name = strings.TrimSpace(sr.Data.SystemName)
	logo = strings.TrimSpace(sr.Data.Logo)
	if logo != "" && !strings.HasPrefix(logo, "http") && !strings.HasPrefix(logo, "data:") {
		logo = base + "/" + strings.TrimLeft(logo, "/")
	}
	return name, logo
}

// brandLogo 返回 new-api 当前配置的 logo 地址(与主站一致,主站换 logo 后自动跟随)。
// 结果按 new-api 地址缓存:成功缓存 30 分钟,失败 5 分钟内不再重试,避免每次打开页面都请求 new-api。
func (m *Monitor) brandLogo() string {
	base := strings.TrimRight(m.cfg.NewAPIBaseURL, "/")
	if base == "" {
		return ""
	}
	now := time.Now()
	brandLogoCache.Lock()
	entry, ok := brandLogoCache.entries[base]
	brandLogoCache.Unlock()
	if ok && now.Before(entry.until) {
		return entry.logo
	}
	_, logo := m.fetchBrand()
	logo = safeBrandLogo(logo)
	ttl := 30 * time.Minute
	if logo == "" {
		ttl = 5 * time.Minute
	}
	brandLogoCache.Lock()
	brandLogoCache.entries[base] = brandLogoEntry{logo: logo, until: now.Add(ttl)}
	brandLogoCache.Unlock()
	return logo
}

type brandLogoEntry struct {
	logo  string
	until time.Time
}

var brandLogoCache = struct {
	sync.Mutex
	entries map[string]brandLogoEntry
}{entries: map[string]brandLogoEntry{}}

// safeBrandLogo 只接受 http(s) 图片地址或 data:image,其余(如 javascript:)一律丢弃。
func safeBrandLogo(logo string) string {
	lower := strings.ToLower(logo)
	if strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "data:image/") {
		return logo
	}
	return ""
}

// brandHandler 公开返回站点名与 logo,供前端设置页面标题、品牌文字和图标。
func (m *Monitor) brandHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"name": m.brandName(), "logo": m.brandLogo()})
}
