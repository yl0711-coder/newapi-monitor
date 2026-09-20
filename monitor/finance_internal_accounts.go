package monitor

// 经营核算内部账号配置。配置只保存不变的 NewAPI user_id；
// username 只是便于人工核对的快照，不参与身份判断。原始日志、
// 上游账单和稳定性统计均不会因该配置被删除或改写。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const financeInternalAccountMax = 100

type FinanceInternalAccount struct {
	UserID    int64  `gorm:"primaryKey;autoIncrement:false;column:user_id" json:"user_id"`
	Username  string `gorm:"size:128;column:username" json:"username"`
	CreatedAt int64  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt int64  `gorm:"column:updated_at;index" json:"updated_at"`
	UpdatedBy string `gorm:"size:128;column:updated_by" json:"updated_by,omitempty"`
}

type FinanceInternalAccountAudit struct {
	ID         int64  `gorm:"primaryKey;column:id" json:"id"`
	Action     string `gorm:"size:32;column:action" json:"action"`
	BeforeHash string `gorm:"size:64;column:before_hash" json:"before_hash"`
	AfterHash  string `gorm:"size:64;column:after_hash" json:"after_hash"`
	AccountIDs string `gorm:"type:text;column:account_ids" json:"account_ids"`
	Actor      string `gorm:"size:128;column:actor" json:"actor"`
	CreatedAt  int64  `gorm:"column:created_at;index" json:"created_at"`
}

type financeInternalAccountInput struct {
	Accounts string `json:"accounts"`
}

func financeInternalAccountHash(rows []FinanceInternalAccount) string {
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		if row.UserID > 0 {
			ids = append(ids, row.UserID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write([]byte(strconv.FormatInt(id, 10)))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func financeInternalAccountIDText(rows []FinanceInternalAccount) string {
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.UserID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	return strings.Join(parts, ",")
}

func (m *Monitor) financeReportConfigurationHash(ctx context.Context) (string, error) {
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		return "", err
	}
	policies, err := loadChannelBusinessGroupPolicies(ctx, m.storeDB)
	if err != nil {
		return "", err
	}
	groups := make([]string, 0, len(policies))
	for group := range policies {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "finance-semantics-v4|%t|%t|%t|%s|%s\n",
		m.cfg.FinanceEnabled, m.cfg.ChannelEconomicsReportEnabled, m.cfg.FinanceCURArtifactEnabled,
		m.cfg.FinanceStartDate, m.cfg.FinanceCURArtifactPath)
	_, _ = h.Write([]byte(financeInternalAccountHash(accounts)))
	for _, group := range groups {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(group))
		if policies[group] {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{2})
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (m *Monitor) loadFinanceInternalAccounts(ctx context.Context) ([]FinanceInternalAccount, error) {
	if m == nil || m.storeDB == nil {
		return nil, errors.New("经营核算配置库不可用")
	}
	var rows []FinanceInternalAccount
	if err := m.storeDB.WithContext(ctx).Order("username ASC,user_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// resolveFinanceInternalAccounts 只读 Monitor 本地用户目录。用户名必须
// 精确且唯一；同名时必须由管理员改填 user_id，绝不自动选第一个。
func (m *Monitor) resolveFinanceInternalAccounts(ctx context.Context, raw string) ([]FinanceInternalAccount, error) {
	tokens := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ';' || r == '，' || r == '；'
	})
	if len(tokens) > financeInternalAccountMax {
		return nil, fmt.Errorf("内部账号最多配置 %d 个", financeInternalAccountMax)
	}
	byID := make(map[int64]FinanceInternalAccount, len(tokens))
	now := time.Now().Unix()
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		var candidates []UserDirectoryEntry
		if id, err := strconv.ParseInt(strings.TrimPrefix(token, "#"), 10, 64); err == nil {
			if id <= 0 {
				return nil, fmt.Errorf("无效用户 ID: %s", token)
			}
			var row UserDirectoryEntry
			err := m.storeDB.WithContext(ctx).First(&row, "user_id = ?", id).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 数字 ID 是 NewAPI 不变的身份键。离线快照或用户目录
				// 同步滞后时仍允许管理员显式填入；只把用户名留空，
				// 不会根据相似名称猜测账号。
				row = UserDirectoryEntry{UserID: id}
				err = nil
			}
			if err != nil {
				return nil, err
			}
			candidates = []UserDirectoryEntry{row}
		} else {
			rows, lookupErr := lookupUsersByName(m.storeDB.WithContext(ctx), token)
			if lookupErr != nil {
				return nil, lookupErr
			}
			candidates = rows
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("本地用户目录中找不到账号 %q", token)
		}
		if len(candidates) > 1 {
			ids := make([]string, 0, len(candidates))
			for _, row := range candidates {
				ids = append(ids, strconv.FormatInt(row.UserID, 10))
			}
			return nil, fmt.Errorf("账号 %q 对应多个用户 ID（%s），请直接填写 ID", token, strings.Join(ids, "、"))
		}
		row := candidates[0]
		if row.UserID <= 0 {
			return nil, fmt.Errorf("账号 %q 的用户 ID 无效", token)
		}
		byID[row.UserID] = FinanceInternalAccount{UserID: row.UserID, Username: clip(strings.TrimSpace(row.Username), 128), CreatedAt: now, UpdatedAt: now}
	}
	rows := make([]FinanceInternalAccount, 0, len(byID))
	for _, row := range byID {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].UserID < rows[j].UserID })
	return rows, nil
}

func (m *Monitor) serveFinanceInternalAccounts(c *gin.Context) {
	c.Header("Cache-Control", "private, no-store")
	rows, err := m.loadFinanceInternalAccounts(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	status, statusErr := m.financeInternalFactSyncStatus(c.Request.Context())
	if statusErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": statusErr.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"accounts": rows, "config_hash": financeInternalAccountHash(rows), "max_accounts": financeInternalAccountMax, "sync": status})
}

func (m *Monitor) saveFinanceInternalAccounts(c *gin.Context) {
	c.Header("Cache-Control", "private, no-store")
	var in financeInternalAccountInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请填写用户名或用户 ID，多个账号用换行或逗号分隔"})
		return
	}
	rows, err := m.resolveFinanceInternalAccounts(c.Request.Context(), in.Accounts)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	before, err := m.loadFinanceInternalAccounts(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	actor := strings.TrimSpace(c.GetString("uname"))
	if actor == "" {
		actor = "root"
	}
	now := time.Now().Unix()
	beforeByID := make(map[int64]FinanceInternalAccount, len(before))
	for _, row := range before {
		beforeByID[row.UserID] = row
	}
	for i := range rows {
		rows[i].UpdatedAt = now
		rows[i].UpdatedBy = actor
		if old, ok := beforeByID[rows[i].UserID]; ok {
			rows[i].CreatedAt = old.CreatedAt
		}
	}
	audit := FinanceInternalAccountAudit{Action: "replace", BeforeHash: financeInternalAccountHash(before), AfterHash: financeInternalAccountHash(rows), AccountIDs: financeInternalAccountIDText(rows), Actor: actor, CreatedAt: now}
	if audit.BeforeHash == audit.AfterHash {
		c.JSON(http.StatusOK, gin.H{"ok": true, "unchanged": true, "accounts": before, "config_hash": audit.AfterHash})
		return
	}
	err = m.storeDB.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&FinanceInternalAccount{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := tx.Create(&rows).Error; err != nil {
				return err
			}
		}
		return tx.Create(&audit).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存内部账号失败", "detail": err.Error()})
		return
	}
	// 账号名单会改变经营口径，不应等待常规的分钟级轮询。
	// 唤醒信号是有界的，不会增加生产库并发查询数。
	m.financeFactsPreferInternal.Store(true)
	m.notifyFinanceFactsSync()
	// 配置改变后旧报表缓存不得继续命中。缓存键另含配置
	// 哈希，这里无需删除原始事实或重启 Monitor。
	c.JSON(http.StatusOK, gin.H{"ok": true, "accounts": rows, "config_hash": audit.AfterHash})
}
