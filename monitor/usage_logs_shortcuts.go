package monitor

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// UsageLogShortcut is one admin's quick filter on the usage-logs page: a
// label that, when clicked, searches logs for one new-api username. Each
// admin (keyed by the main-site login name) keeps a private list; rows never
// touch the new-api database.
type UsageLogShortcut struct {
	ID        int64  `gorm:"primaryKey" json:"id"`
	Owner     string `gorm:"size:64;not null;uniqueIndex:idx_usage_log_shortcut_owner_user,priority:1" json:"-"`
	Username  string `gorm:"size:64;not null;uniqueIndex:idx_usage_log_shortcut_owner_user,priority:2" json:"username"`
	Label     string `gorm:"size:64;not null" json:"label"`
	Sort      int    `gorm:"not null;default:0" json:"sort"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

const (
	usageLogShortcutMax        = 20
	usageLogShortcutLabelRunes = 16
	usageLogShortcutUserRunes  = 64
)

var errUsageLogShortcutDuplicate = errors.New("该用户已在快捷标签中")

type usageLogShortcutInput struct {
	Label    string `json:"label"`
	Username string `json:"username"`
}

// normalize validates and trims an input. An empty label falls back to the
// username so a shortcut can be added by username alone.
func (in usageLogShortcutInput) normalize() (label, username string, err error) {
	username = strings.TrimSpace(in.Username)
	label = strings.TrimSpace(in.Label)
	if username == "" {
		return "", "", errors.New("请填写用户名")
	}
	if strings.Contains(username, "%") {
		return "", "", errors.New("快捷标签的用户名需填写完整用户名，不支持 % 通配符")
	}
	if utf8.RuneCountInString(username) > usageLogShortcutUserRunes {
		return "", "", errors.New("用户名过长")
	}
	if label == "" {
		label = username
	}
	if utf8.RuneCountInString(label) > usageLogShortcutLabelRunes {
		r := []rune(label)
		label = string(r[:usageLogShortcutLabelRunes])
	}
	return label, username, nil
}

func usageLogShortcutOwner(c *gin.Context) (string, bool) {
	owner := strings.TrimSpace(c.GetString("uname"))
	if owner == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"success": false, "message": "未登录"})
		return "", false
	}
	return clip(owner, 64), true
}

func usageLogShortcutReply(c *gin.Context, status int, err error) {
	c.JSON(status, gin.H{"success": false, "message": err.Error()})
}

func (m *Monitor) usageLogShortcutList(owner string) ([]UsageLogShortcut, error) {
	var rows []UsageLogShortcut
	err := m.storeDB.Where("owner = ?", owner).Order("sort ASC, id ASC").Find(&rows).Error
	return rows, err
}

func (m *Monitor) serveUsageLogShortcutsReply(c *gin.Context, owner string) {
	rows, err := m.usageLogShortcutList(owner)
	if err != nil {
		usageLogShortcutReply(c, http.StatusInternalServerError, errors.New("读取快捷标签失败"))
		return
	}
	if rows == nil {
		rows = []UsageLogShortcut{}
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "", "data": gin.H{"items": rows, "max": usageLogShortcutMax}})
}

// GET /usage-logs/shortcuts
func (m *Monitor) serveUsageLogShortcuts(c *gin.Context) {
	owner, ok := usageLogShortcutOwner(c)
	if !ok {
		return
	}
	m.serveUsageLogShortcutsReply(c, owner)
}

// POST /usage-logs/shortcuts
func (m *Monitor) createUsageLogShortcut(c *gin.Context) {
	owner, ok := usageLogShortcutOwner(c)
	if !ok {
		return
	}
	var in usageLogShortcutInput
	if err := c.ShouldBindJSON(&in); err != nil {
		usageLogShortcutReply(c, http.StatusBadRequest, errors.New("请求格式错误"))
		return
	}
	label, username, err := in.normalize()
	if err != nil {
		usageLogShortcutReply(c, http.StatusBadRequest, err)
		return
	}
	now := time.Now().Unix()
	err = m.storeDB.Transaction(func(tx *gorm.DB) error {
		var n int64
		if err := tx.Model(&UsageLogShortcut{}).Where("owner = ?", owner).Count(&n).Error; err != nil {
			return err
		}
		if n >= usageLogShortcutMax {
			return errors.New("快捷标签最多 " + strconv.Itoa(usageLogShortcutMax) + " 个，请先删除不用的")
		}
		var dup int64
		if err := tx.Model(&UsageLogShortcut{}).Where("owner = ? AND username = ?", owner, username).Count(&dup).Error; err != nil {
			return err
		}
		if dup > 0 {
			return errUsageLogShortcutDuplicate
		}
		var maxSort struct{ S int }
		if err := tx.Model(&UsageLogShortcut{}).Select("COALESCE(MAX(sort),0) AS s").Where("owner = ?", owner).Scan(&maxSort).Error; err != nil {
			return err
		}
		return tx.Create(&UsageLogShortcut{Owner: owner, Username: username, Label: label, Sort: maxSort.S + 1, CreatedAt: now, UpdatedAt: now}).Error
	})
	if err != nil {
		usageLogShortcutReply(c, http.StatusBadRequest, usageLogShortcutErr(err))
		return
	}
	m.serveUsageLogShortcutsReply(c, owner)
}

// POST /usage-logs/shortcuts/:id
func (m *Monitor) updateUsageLogShortcut(c *gin.Context) {
	owner, ok := usageLogShortcutOwner(c)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		usageLogShortcutReply(c, http.StatusBadRequest, errors.New("标签不存在"))
		return
	}
	var in usageLogShortcutInput
	if err := c.ShouldBindJSON(&in); err != nil {
		usageLogShortcutReply(c, http.StatusBadRequest, errors.New("请求格式错误"))
		return
	}
	label, username, err := in.normalize()
	if err != nil {
		usageLogShortcutReply(c, http.StatusBadRequest, err)
		return
	}
	err = m.storeDB.Transaction(func(tx *gorm.DB) error {
		var dup int64
		if err := tx.Model(&UsageLogShortcut{}).Where("owner = ? AND username = ? AND id <> ?", owner, username, id).Count(&dup).Error; err != nil {
			return err
		}
		if dup > 0 {
			return errUsageLogShortcutDuplicate
		}
		res := tx.Model(&UsageLogShortcut{}).Where("id = ? AND owner = ?", id, owner).
			Updates(map[string]any{"label": label, "username": username, "updated_at": time.Now().Unix()})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
	if err != nil {
		usageLogShortcutReply(c, http.StatusBadRequest, usageLogShortcutErr(err))
		return
	}
	m.serveUsageLogShortcutsReply(c, owner)
}

// POST /usage-logs/shortcuts/:id/delete
func (m *Monitor) deleteUsageLogShortcut(c *gin.Context) {
	owner, ok := usageLogShortcutOwner(c)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		usageLogShortcutReply(c, http.StatusBadRequest, errors.New("标签不存在"))
		return
	}
	// Deleting an already-removed (or someone else's) id is a no-op: the
	// reply is simply the caller's current list.
	if err := m.storeDB.Where("id = ? AND owner = ?", id, owner).Delete(&UsageLogShortcut{}).Error; err != nil {
		usageLogShortcutReply(c, http.StatusInternalServerError, errors.New("删除快捷标签失败"))
		return
	}
	m.serveUsageLogShortcutsReply(c, owner)
}

// POST /usage-logs/shortcuts/order  {"ids":[3,1,2]}
// Ids not owned by the caller are ignored; owned ids missing from the list
// keep their relative order after the listed ones.
func (m *Monitor) reorderUsageLogShortcuts(c *gin.Context) {
	owner, ok := usageLogShortcutOwner(c)
	if !ok {
		return
	}
	var in struct {
		IDs []int64 `json:"ids"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || len(in.IDs) > usageLogShortcutMax*2 {
		usageLogShortcutReply(c, http.StatusBadRequest, errors.New("请求格式错误"))
		return
	}
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var rows []UsageLogShortcut
		if err := tx.Where("owner = ?", owner).Order("sort ASC, id ASC").Find(&rows).Error; err != nil {
			return err
		}
		owned := make(map[int64]bool, len(rows))
		for _, r := range rows {
			owned[r.ID] = true
		}
		order := make([]int64, 0, len(rows))
		seen := make(map[int64]bool, len(rows))
		for _, id := range in.IDs {
			if owned[id] && !seen[id] {
				order = append(order, id)
				seen[id] = true
			}
		}
		for _, r := range rows {
			if !seen[r.ID] {
				order = append(order, r.ID)
			}
		}
		for i, id := range order {
			if err := tx.Model(&UsageLogShortcut{}).Where("id = ? AND owner = ?", id, owner).Update("sort", i+1).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		usageLogShortcutReply(c, http.StatusInternalServerError, errors.New("保存顺序失败"))
		return
	}
	m.serveUsageLogShortcutsReply(c, owner)
}

func usageLogShortcutErr(err error) error {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return errors.New("标签不存在或已被删除")
	case errors.Is(err, errUsageLogShortcutDuplicate):
		return err
	case strings.Contains(strings.ToLower(err.Error()), "unique"):
		return errUsageLogShortcutDuplicate
	}
	if strings.HasPrefix(err.Error(), "快捷标签最多") {
		return err
	}
	return errors.New("保存快捷标签失败")
}
