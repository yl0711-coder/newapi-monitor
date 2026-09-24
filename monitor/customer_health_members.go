package monitor

// Independent customer-health membership storage and HTTP CRUD.  The usage
// list (TrackedUser/CustomerGroup) is intentionally not touched by these
// handlers; customer maintenance has its own lifecycle and can now be
// changed without changing usage, portal, or follow-up behaviour.

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const customerHealthMigrationVersion = 1
const maxCustomerHealthMembers = 500

var errCustomerHealthMemberDifferentGroup = errors.New("customer health member belongs to another company")

func customerHealthConflict(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(message, "unique constraint") || strings.Contains(message, "duplicate")
}

// migrateLegacyCustomerHealthTables copies the old customer-maintenance
// projection exactly once.  IDs are retained so links/bookmarks and existing
// report facts keep their meaning.  The marker is written in the same
// transaction as the copy, so a failed migration is safe to retry.
func migrateLegacyCustomerHealthTables(db *gorm.DB) error {
	if db == nil {
		return errors.New("customer health store is nil")
	}
	var state CustomerHealthMigrationState
	err := db.First(&state, 1).Error
	if err == nil && state.Version >= customerHealthMigrationVersion {
		return nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var groups []CustomerGroup
		if err := tx.Order("id ASC").Find(&groups).Error; err != nil {
			return err
		}
		groupIDs := make(map[int64]int64, len(groups))
		for _, old := range groups {
			row := CustomerHealthGroup{ID: old.ID, Name: old.Name, Note: old.Note, CreatedAt: old.CreatedAt}
			var existing CustomerHealthGroup
			// Match by name first.  A health-only group may already have taken
			// the legacy numeric ID while still representing this same company.
			err := tx.Where("name = ?", old.Name).First(&existing).Error
			switch {
			case err == nil:
				groupIDs[old.ID] = existing.ID
			case errors.Is(err, gorm.ErrRecordNotFound):
				idErr := tx.First(&existing, old.ID).Error
				switch {
				case errors.Is(idErr, gorm.ErrRecordNotFound):
					if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
						return err
					}
					groupIDs[old.ID] = old.ID
				case idErr == nil && existing.Name == old.Name:
					groupIDs[old.ID] = existing.ID
				case idErr == nil:
					// The numeric ID is occupied by an unrelated independent
					// row; let SQLite allocate a fresh ID for the legacy copy.
					row.ID = 0
					if err := tx.Create(&row).Error; err != nil {
						return err
					}
					groupIDs[old.ID] = row.ID
				default:
					return idErr
				}
			default:
				return err
			}
		}
		var users []TrackedUser
		if err := tx.Where("group_id > 0").Order("user_id ASC").Find(&users).Error; err != nil {
			return err
		}
		for _, old := range users {
			groupID := groupIDs[old.GroupID]
			if groupID <= 0 {
				// A legacy member can refer to a group that disappeared before
				// migration. Keep the copy fail-closed rather than creating an
				// orphaned health member row.
				continue
			}
			member := CustomerHealthMember{
				UserID: old.UserID, GroupID: groupID, Username: old.Username,
				Email: old.Email, Note: old.Note, AddedAt: old.AddedAt,
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&member).Error; err != nil {
				return err
			}
		}
		return tx.Save(&CustomerHealthMigrationState{ID: 1, Version: customerHealthMigrationVersion, MigratedAt: time.Now().Unix()}).Error
	})
}

// listCustomerHealthGroups is the read side used by the maintenance page.
func (m *Monitor) listCustomerHealthGroups(c *gin.Context) {
	var groups []CustomerHealthGroup
	if err := m.storeDB.Order("created_at ASC, id ASC").Find(&groups).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取客户维护公司失败"})
		return
	}
	type row struct {
		CustomerHealthGroup
		Members int64 `json:"members"`
	}
	out := make([]row, 0, len(groups))
	for _, group := range groups {
		var count int64
		if err := m.storeDB.Model(&CustomerHealthMember{}).Where("group_id = ?", group.ID).Count(&count).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "读取客户维护成员失败"})
			return
		}
		out = append(out, row{CustomerHealthGroup: group, Members: count})
	}
	c.JSON(http.StatusOK, gin.H{"groups": out})
}

func (m *Monitor) listCustomerHealthMembers(c *gin.Context) {
	var members []CustomerHealthMember
	query := m.storeDB.Order("group_id ASC, user_id ASC")
	if raw := strings.TrimSpace(c.Query("group_id")); raw != "" {
		var groupID int64
		if _, err := fmt.Sscan(raw, &groupID); err != nil || groupID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group_id"})
			return
		}
		query = query.Where("group_id = ?", groupID)
	}
	if err := query.Find(&members).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取客户维护成员失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"members": members})
}

func customerHealthGroupInput(c *gin.Context) (string, string, bool) {
	var in struct {
		Name string `json:"name"`
		Note string `json:"note"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return "", "", false
	}
	name, note, err := normalizeGroupInput(in.Name, in.Note)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return "", "", false
	}
	return name, note, true
}

func (m *Monitor) createCustomerHealthGroup(c *gin.Context) {
	name, note, ok := customerHealthGroupInput(c)
	if !ok {
		return
	}
	var count int64
	if err := m.storeDB.Model(&CustomerHealthGroup{}).Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取客户维护公司失败"})
		return
	}
	if count >= maxCustomerGroups {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("客户维护公司已达上限 %d 个", maxCustomerGroups)})
		return
	}
	group := CustomerHealthGroup{Name: name, Note: note, CreatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&group).Error; err != nil {
		if customerHealthConflict(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "创建失败：公司名可能已存在"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建客户维护公司失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "group": group})
}

func (m *Monitor) updateCustomerHealthGroup(c *gin.Context) {
	var in struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
		Note string `json:"note"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	name, note, err := normalizeGroupInput(in.Name, in.Note)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res := m.storeDB.Model(&CustomerHealthGroup{}).Where("id = ?", in.ID).Updates(map[string]any{"name": name, "note": note})
	if res.Error != nil {
		if customerHealthConflict(res.Error) {
			c.JSON(http.StatusConflict, gin.H{"error": "保存失败：公司名可能已存在"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "保存客户维护公司失败"})
		}
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "公司不存在"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (m *Monitor) deleteCustomerHealthGroup(c *gin.Context) {
	id := customerHealthGroupID(c)
	if id <= 0 {
		return
	}
	var count int64
	if err := m.storeDB.Model(&CustomerHealthMember{}).Where("group_id = ?", id).Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取成员失败"})
		return
	}
	if count > 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "公司仍有成员，请先删除成员或使用解散"})
		return
	}
	res := m.storeDB.Delete(&CustomerHealthGroup{}, id)
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除公司失败"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "公司不存在"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func customerHealthGroupID(c *gin.Context) int64 {
	var in struct {
		ID int64 `json:"id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return 0
	}
	return in.ID
}

func (m *Monitor) dissolveCustomerHealthGroup(c *gin.Context) {
	id := customerHealthGroupID(c)
	if id <= 0 {
		return
	}
	// The page keeps the idempotency key when a network response is lost.  A
	// retry after the first successful dissolve must therefore be a harmless
	// success, not a misleading 404.
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var group CustomerHealthGroup
		if err := tx.First(&group, id).Error; err != nil {
			return err
		}
		if err := tx.Where("group_id = ?", id).Delete(&CustomerHealthMember{}).Error; err != nil {
			return err
		}
		return tx.Delete(&group).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusOK, gin.H{"ok": true, "replayed": true})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "解散公司失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (m *Monitor) resolveCustomerHealthInput(c *gin.Context, input string, userID int64) (CustomerHealthMember, error) {
	// 客户维护必须在 logchain-only 最小权限部署中也能新增成员。这里
	// 不能再调用 resolveNewAPIUser：那个旧入口会查询生产 users 表，
	// 而正式只读账号不应拥有 users 权限。数字 ID 直接接受；名称只
	// 使用本地 user_directory（同步失败时宁可要求填 ID，也不回查生产）。
	input = strings.TrimSpace(input)
	if userID <= 0 && input != "" {
		if parsed, err := strconv.ParseInt(input, 10, 64); err == nil && parsed > 0 {
			userID = parsed
		}
	}
	if userID > 0 {
		member := CustomerHealthMember{UserID: userID, AddedAt: time.Now().Unix()}
		var directory UserDirectoryEntry
		if err := m.storeDB.First(&directory, "user_id = ?", userID).Error; err == nil {
			member.Username = strings.TrimSpace(directory.Username)
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return CustomerHealthMember{}, fmt.Errorf("读取本地用户目录失败")
		}
		return member, nil
	}
	if input == "" {
		return CustomerHealthMember{}, errors.New("请输入用户ID，或已同步到本地目录的用户名")
	}
	resolved, err := lookupUsersByName(m.storeDB, input)
	if err != nil {
		return CustomerHealthMember{}, errors.New("读取本地用户目录失败")
	}
	switch len(resolved) {
	case 0:
		return CustomerHealthMember{}, fmt.Errorf("本地目录没有找到该用户，请改用用户ID添加(%s)", input)
	case 1:
		return CustomerHealthMember{UserID: resolved[0].UserID, Username: resolved[0].Username, AddedAt: time.Now().Unix()}, nil
	default:
		return CustomerHealthMember{}, errors.New("该用户名对应多个用户，请改用用户ID添加")
	}
}

func (m *Monitor) addCustomerHealthMember(c *gin.Context) {
	var in struct {
		GroupID int64  `json:"group_id"`
		UserID  int64  `json:"user_id"`
		Input   string `json:"input"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.GroupID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group_id required"})
		return
	}
	member, err := m.resolveCustomerHealthInput(c, in.Input, in.UserID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	member.GroupID = in.GroupID
	var group CustomerHealthGroup
	if err := m.storeDB.First(&group, in.GroupID).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "公司不存在"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取公司失败"})
		return
	}
	var existing CustomerHealthMember
	if err := m.storeDB.First(&existing, member.UserID).Error; err == nil {
		if existing.GroupID == in.GroupID {
			c.JSON(http.StatusOK, gin.H{"ok": true, "member": existing, "replayed": true})
		} else {
			c.JSON(http.StatusConflict, gin.H{"error": "用户已属于另一家公司"})
		}
		return
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取成员失败"})
		return
	}
	var memberCount int64
	if err := m.storeDB.Model(&CustomerHealthMember{}).Count(&memberCount).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取客户维护成员失败"})
		return
	}
	if memberCount >= maxCustomerHealthMembers {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("客户维护成员已达上限 %d 个", maxCustomerHealthMembers)})
		return
	}
	if err := m.storeDB.Create(&member).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "用户已在客户维护名单中，或已属于另一家公司"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "member": member})
}

func (m *Monitor) removeCustomerHealthMember(c *gin.Context) {
	var in struct {
		UserID  int64 `json:"user_id"`
		GroupID int64 `json:"group_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	q := m.storeDB.Where("user_id = ?", in.UserID)
	if in.GroupID > 0 {
		q = q.Where("group_id = ?", in.GroupID)
	}
	res := q.Delete(&CustomerHealthMember{})
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除成员失败"})
		return
	}
	if res.RowsAffected == 0 {
		// A lost response followed by a retry reaches this branch after the
		// first request already removed the member.  Treat it as an idempotent
		// replay so the UI does not report a false failure.
		c.JSON(http.StatusOK, gin.H{"ok": true, "replayed": true})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (m *Monitor) updateCustomerHealthMember(c *gin.Context) {
	var in struct {
		UserID  int64  `json:"user_id"`
		GroupID int64  `json:"group_id"`
		Note    string `json:"note"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	note := strings.TrimSpace(in.Note)
	if len(note) > 200 {
		note = note[:200]
	}
	updates := map[string]any{"note": note}
	if in.GroupID > 0 {
		var group CustomerHealthGroup
		if err := m.storeDB.First(&group, in.GroupID).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "公司不存在"})
			return
		} else if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "读取公司失败"})
			return
		}
		updates["group_id"] = in.GroupID
	}
	res := m.storeDB.Model(&CustomerHealthMember{}).Where("user_id = ?", in.UserID).Updates(updates)
	if res.Error != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "保存成员失败"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "成员不在客户维护名单中"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// createCustomerHealthCustomer atomically creates/reuses a company and adds
// its first member.  It mirrors the old /usage/customers convenience API but
// writes only the independent customer-health tables.
func (m *Monitor) createCustomerHealthCustomer(c *gin.Context) {
	var in struct {
		Company string `json:"company"`
		Input   string `json:"input"`
		UserID  int64  `json:"user_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	name, _, err := normalizeGroupInput(in.Company, "")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	member, err := m.resolveCustomerHealthInput(c, in.Input, in.UserID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var group CustomerHealthGroup
	err = m.storeDB.Transaction(func(tx *gorm.DB) error {
		findErr := tx.Where("name = ?", name).First(&group).Error
		switch {
		case findErr == nil:
		case errors.Is(findErr, gorm.ErrRecordNotFound):
			var count int64
			if err := tx.Model(&CustomerHealthGroup{}).Count(&count).Error; err != nil {
				return err
			}
			if count >= maxCustomerGroups {
				return fmt.Errorf("客户维护公司已达上限 %d 个", maxCustomerGroups)
			}
			group = CustomerHealthGroup{Name: name, CreatedAt: time.Now().Unix()}
			if err := tx.Create(&group).Error; err != nil {
				return err
			}
		default:
			return findErr
		}
		member.GroupID = group.ID
		var existing CustomerHealthMember
		err := tx.First(&existing, member.UserID).Error
		if err == nil {
			if existing.GroupID != group.ID {
				return errCustomerHealthMemberDifferentGroup
			}
			member = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var memberCount int64
		if err := tx.Model(&CustomerHealthMember{}).Count(&memberCount).Error; err != nil {
			return err
		}
		if memberCount >= maxCustomerHealthMembers {
			return fmt.Errorf("客户维护成员已达上限 %d 个", maxCustomerHealthMembers)
		}
		return tx.Create(&member).Error
	})
	if err != nil {
		if errors.Is(err, errCustomerHealthMemberDifferentGroup) {
			c.JSON(http.StatusConflict, gin.H{"error": "用户已属于另一家公司"})
			return
		}
		if customerHealthConflict(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "用户已在客户维护名单中，或公司名已存在"})
			return
		}
		if strings.Contains(err.Error(), "客户维护成员已达上限") {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if strings.Contains(err.Error(), "客户维护公司已达上限") {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建客户失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "group": group, "member": member})
}
