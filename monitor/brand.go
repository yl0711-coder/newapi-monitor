package monitor

import (
	"encoding/json"
	"net/http"
	"strings"
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

// brandHandler 公开返回站点名,供前端设置页面标题与品牌文字。
func (m *Monitor) brandHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"name": m.brandName()})
}
