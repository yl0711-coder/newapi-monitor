package monitor

// user_directory.go：user_id → 用户名的本地只读缓存。
//
// 为什么要这张表：问题预警要像客户排障那样显示客户名字，但那一页只读 Monitor
// 本地库——页面刷新不许查生产源。而既有的 tracked_users 是「客户管理名单」，
// 只覆盖人工加进去的 ID（customers.go 里按 idsOf(tracked) 查），
// 被前置拒绝的用户多半不在名单里，拿它翻名字会大面积显示不出来。
//
// 成本可忽略：生产 users 表实测只有 150 行，整表同步一次就是一条
// SELECT id, username, group。同步在后台采样周期里做，不在页面路径上。
//
// 这张表是纯派生缓存：删掉它只会让页面退回只显示 ID，不影响任何统计。

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// UserDirectoryEntry 一个 new-api 用户的展示用身份。
// 只存展示必需的字段：不存邮箱、余额、令牌——那些各有自己的权限边界，
// 这张表会被问题预警这类聚合页读取，多存一列就是多一处泄漏面。
type UserDirectoryEntry struct {
	UserID   int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	Username string `gorm:"size:128"`
	// Grp 是用户在主站的分组。前置拒绝日志里多数错误不带分组，
	// 这里的值可作为「该客户平时属于哪个分组」的参考，不当作请求时刻的事实。
	Grp      string `gorm:"size:64;column:grp"`
	SyncedAt int64  `gorm:"column:synced_at"`
}

// userDirectorySyncMax 是一次同步接受的上限。生产实测 150 行；
// 留出余量但仍设上限，避免主站用户量意外暴涨时把整表拉进本地。
const userDirectorySyncMax = 5000

// startUserDirectorySync 把展示缓存放到低优先来源槽异步执行。它不能阻塞主采样，
// 也不能因连续 ticker 重叠创建多个等待者去挤占 stability/facts 等既有任务。
func (m *Monitor) startUserDirectorySync(ctx context.Context) {
	if !m.userDirectorySyncing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.userDirectorySyncing.Store(false)
		if err := m.syncUserDirectory(ctx); err != nil {
			slog.Warn("用户名缓存同步失败(沿用旧值,页面退回只显示 ID)", "err", err)
		}
	}()
}

// syncUserDirectory 全量刷新用户名缓存。只读 SELECT，一次往返。
// 失败时保留上一次的值——名字显示旧一点没关系，显示不出来才是问题。
func (m *Monitor) syncUserDirectory(ctx context.Context) error {
	if m.prodDB == nil {
		return nil // 未连生产库（快照环境）：沿用本地已有缓存。
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	release, err := m.acquireBackgroundSourceLow(cctx)
	if err != nil {
		return err
	}
	defer release()
	rows, err := m.prodDB.QueryContext(cctx,
		"SELECT id, COALESCE(username,''), COALESCE(`group`,'') FROM users ORDER BY id LIMIT ?",
		userDirectorySyncMax+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	now := time.Now().Unix()
	out := make([]UserDirectoryEntry, 0, 256)
	for rows.Next() {
		var e UserDirectoryEntry
		if err := rows.Scan(&e.UserID, &e.Username, &e.Grp); err != nil {
			return err
		}
		e.Username = clip(strings.TrimSpace(e.Username), 128)
		e.Grp = clip(strings.TrimSpace(e.Grp), 64)
		e.SyncedAt = now
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(out) > userDirectorySyncMax {
		// 超限只警告并截断，不静默丢弃：宁可名字缓存不全，也不要把本地库撑爆。
		slog.Warn("主站用户数超过用户名缓存上限，已截断", "limit", userDirectorySyncMax, "got", len(out))
		out = out[:userDirectorySyncMax]
	}
	if len(out) == 0 {
		return nil // 一行都没读到：当作本次未获得新值，不清空既有缓存。
	}
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"username", "grp", "synced_at"}),
	}).CreateInBatches(out, 200).Error
}

// lookupUsersByName 按用户名精确查本地缓存里的客户 ID 候选。
//
// 客户排障用它回答「这个名字是不是唯一一个客户」。同名不是假想：主站不保证
// username 唯一，按名字直查 logs 会把多个客户的请求混成一份，而页面上看起来
// 就像“这一个客户的问题”。返回全部候选（按 ID 升序）交由调用方要求人工选择，
// 绝不自动取第一个。
//
// 只读本地缓存：查不到候选时调用方必须按“无法确认身份”处理，而不是当成唯一客户。
func lookupUsersByName(db *gorm.DB, username string) ([]UserDirectoryEntry, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, nil
	}
	var rows []UserDirectoryEntry
	if err := db.Where("username = ?", username).Order("user_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// lookupUserNames 批量取用户名。只读本地缓存，不碰生产库。
// 返回 map 里没有的 ID 表示缓存里没有——调用方必须显示 ID 本身，不能显示空白。
func lookupUserNames(db *gorm.DB, ids []int64) map[int64]string {
	out := map[int64]string{}
	if len(ids) == 0 {
		return out
	}
	var rows []UserDirectoryEntry
	if err := db.Where("user_id IN ?", ids).Find(&rows).Error; err != nil {
		slog.Warn("读取用户名缓存失败，页面将只显示用户 ID", "err", err)
		return out
	}
	for _, r := range rows {
		if r.Username != "" {
			out[r.UserID] = r.Username
		}
	}
	return out
}
