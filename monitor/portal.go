package monitor

// portal.go:客户端「用量报表」——给客户看自己分组用量的独立页面。
//
// 隔离铁律(与对外 API 平台同一条):
//   - 独立监听端口(MONITOR_PORTAL_ADDR,默认关):客户域名只指到这个端口,
//     该端口上【不存在】任何管理端路由/页面/资源——物理隔离,零 monitor 痕迹;
//   - 会话独立:自己的 cookie 名 + 独立 HMAC 密钥域,管理端会话在这里无效,反之亦然;
//   - 数据强隔离:group_id 只从会话取,服务端强制只查该组成员;客户传什么参数都越不了权。
//
// 账号模型:一组一账号(portal_email),双密码并存——我方配置密码(PortalPwAdmin,永久有效)
// 和客户自改密码(PortalPwUser)任一匹配即可登录;均只存 bcrypt 哈希,后台不可见只能重置。
//
// 容量设计:昂贵聚合结果走 Redis(可选)+有界本机应急缓存+
// singleflight(cache.go)。Redis 不可用时自动降级；缓存键带服务端成员指纹，成员变化不会串组。

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

//go:embed portal.html
var portalHTML string

//go:embed portal_login.html
var portalLoginHTML string

const (
	portalSessionCookie    = "report_session" // 中性命名,不带 monitor 字样
	portalSessionTTL       = 12 * time.Hour
	portalLoginWindow      = 10 * time.Minute // 登录限流窗口
	portalLogPageSize      = 50               // 日志查看每页条数
	portalExportCap        = 50000            // 单次 CSV 导出封顶行数(超出弹确认导最新这么多)
	portalExportPageSize   = 500              // CSV 分页读取，避免 5 万行及详情同时堆入内存
	portalExportWindow     = 5 * time.Minute  // 导出限流:每组织账号该窗口内 1 次(仅计成功下载)
	portalExportPrepWindow = 5 * time.Second  // 预检 COUNT 防连点/并发；不改变成功导出的 5 分钟限额
	portalExportTicketTTL  = 5 * time.Minute  // 预检结果短期有效；下载凭票跳过第二次 COUNT
	// CSV 必须完整落盘并通过末端权限复核后才发给客户。
	// 上限避免单个导出吃满 /data；保留水位保护 SQLite WAL/备份。
	portalExportMaxBytes         int64 = 128 << 20
	portalExportDiskReserveBytes int64 = 256 << 20
	portalExportStaleFileAge           = 6 * time.Hour
	portalExportTempDirName            = ".portal-export-tmp"
	portalExportTempFilePrefix         = "export-"
	portalLoginMaxFails                = 8    // 窗口内最多失败次数(按来源 IP)
	loginLimiterMaxKeys                = 4096 // 防大量伪造来源把限流表撑大
	// token_name/content 中间包含查询无法利用普通 B-tree 前缀索引。
	// 必须同时约束关键词长度和日期窗口，导出又比首页 LIMIT 51 更严。
	portalTokenFuzzyMaxRange   = 31 * 24 * time.Hour
	portalDetailFuzzyMaxRange  = 7 * 24 * time.Hour
	portalExportTokenMaxRange  = 7 * 24 * time.Hour
	portalExportDetailMaxRange = 24 * time.Hour
	portalTokenFuzzyMinRunes   = 2
	portalDetailFuzzyMinRunes  = 3
)

var (
	errPortalExportTooLarge            = errors.New("导出内容超过文件上限")
	errPortalExportInsufficientStorage = errors.New("导出临时存储空间不足")
	errPortalExportStorageBusy         = errors.New("已有客户导出正在生成")
	portalExportProcessGate            = make(chan struct{}, 1)
)

// portalExportLimitedWriter 在写入前拒绝超限的整块数据。临时文件
// 可能已含前缀，但客户响应尚未开始，任何失败都会删除整个文件。
type portalExportLimitedWriter struct {
	dst     io.Writer
	written int64
	max     int64
}

type portalExportStorageLease struct {
	lockFile *os.File
	once     sync.Once
}

func (lease *portalExportStorageLease) release() {
	if lease == nil {
		return
	}
	lease.once.Do(func() {
		if lease.lockFile != nil {
			_ = syscall.Flock(int(lease.lockFile.Fd()), syscall.LOCK_UN)
			_ = lease.lockFile.Close()
		}
		<-portalExportProcessGate
	})
}

func (w *portalExportLimitedWriter) Write(payload []byte) (int, error) {
	if w.max <= 0 || int64(len(payload)) > w.max-w.written {
		return 0, errPortalExportTooLarge
	}
	n, err := w.dst.Write(payload)
	w.written += int64(n)
	return n, err
}

// ---- 密码(bcrypt) ----

func hashPassword(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

func checkPassword(hash, pw string) bool {
	if hash == "" || pw == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}

// ---- 管理端:配置分组客户账号(仅超管;路由挂在管理端口) ----

// setGroupPortal POST /usage/groups/portal:开通/更新分组的客户端账号。
// {id, email, password?, clear?, reset_user_pw?}
//   - clear=true:关闭账号(清邮箱+双密码)
//   - password 留空=不改我方配置密码(但首次开通必须设);reset_user_pw=true 清客户自改密码
func (m *Monitor) setGroupPortal(c *gin.Context) {
	var in struct {
		ID          int64  `json:"id"`
		Email       string `json:"email"`
		Password    string `json:"password"`
		Clear       bool   `json:"clear"`
		ResetUserPw bool   `json:"reset_user_pw"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	var g CustomerGroup
	if err := m.storeDB.First(&g, in.ID).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "分组不存在"})
		return
	}
	if in.Clear {
		if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).
			Updates(map[string]any{"portal_email": "", "portal_pw_admin": "", "portal_pw_user": "", "portal_auth_ver": gorm.Expr("portal_auth_ver + 1")}).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	// 登录账号:用户名/邮箱都行,不校验格式(用户要求);仅去空格+统一小写(登录不区分大小写)
	email := strings.ToLower(strings.TrimSpace(in.Email))
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请填写登录账号"})
		return
	}
	// 账号跨组唯一(登录按账号找组,撞了就乱)
	var dup int64
	m.storeDB.Model(&CustomerGroup{}).Where("portal_email = ? AND id <> ?", email, in.ID).Count(&dup)
	if dup > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该账号已被其他分组使用"})
		return
	}
	upd := map[string]any{"portal_email": email}
	if in.Password != "" {
		if len(in.Password) < 8 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "密码至少 8 位"})
			return
		}
		h, err := hashPassword(in.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "密码处理失败"})
			return
		}
		upd["portal_pw_admin"] = h
	} else if g.PortalPwAdmin == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "首次开通必须设置密码"})
		return
	}
	if in.ResetUserPw {
		upd["portal_pw_user"] = ""
	}
	if email != g.PortalEmail || in.Password != "" || in.ResetUserPw {
		upd["portal_auth_ver"] = gorm.Expr("portal_auth_ver + 1")
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).Updates(upd).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---- 客户端会话(独立密钥域,与管理端互不相认) ----

func (m *Monitor) portalMACKey() []byte { return []byte(m.cfg.SessionSecret + "|portal") }

func (m *Monitor) signPortalSession(gid, authVer, nowUnix int64) string {
	p := fmt.Sprintf("%d|%d|%d", gid, authVer, nowUnix)
	enc := base64.RawURLEncoding.EncodeToString([]byte(p))
	mac := hmac.New(sha256.New, m.portalMACKey())
	mac.Write([]byte(enc))
	return enc + "." + hex.EncodeToString(mac.Sum(nil))
}

func (m *Monitor) verifyPortalSession(token string, nowUnix int64) (gid, authVer int64, ok bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	mac := hmac.New(sha256.New, m.portalMACKey())
	mac.Write([]byte(parts[0]))
	if !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(parts[1])) {
		return 0, 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, 0, false
	}
	f := strings.Split(string(raw), "|")
	if len(f) != 2 && len(f) != 3 {
		return 0, 0, false
	}
	gid, _ = strconv.ParseInt(f[0], 10, 64)
	var issued int64
	if len(f) == 2 {
		// 升级前会话没有版本字段，等价于版本 0；这样部署本身不强制全员掉线。
		// 一旦账号或密码发生变化，库内版本递增，旧会话仍会立即失效。
		authVer = 0
		issued, _ = strconv.ParseInt(f[1], 10, 64)
	} else {
		authVer, _ = strconv.ParseInt(f[1], 10, 64)
		issued, _ = strconv.ParseInt(f[2], 10, 64)
	}
	if gid <= 0 || nowUnix-issued > int64(portalSessionTTL.Seconds()) {
		return 0, 0, false
	}
	return gid, authVer, true
}

// portalExportClaim 是一次 CSV 原生下载的短期、签名预检结果。浏览器先做一次
// COUNT+MAX(id)，随后把票据交给原生下载；服务端凭票跳过第二次 COUNT，并从
// MaxID 快照开始分页，避免预检后新写入的日志让“是否超过 5 万行”结论漂移。
// 票据只含服务端已校验的筛选条件和成员指纹，不含密码、令牌密钥或日志内容。
type portalExportClaim struct {
	GID         int64  `json:"gid"`
	MemberFP    string `json:"member_fp"`
	MemberUID   int64  `json:"member_uid"`
	FromTs      int64  `json:"from_ts"`
	ToTs        int64  `json:"to_ts"`
	LogType     int    `json:"log_type"`
	Model       string `json:"model"`
	Group       string `json:"group"`
	TokenName   string `json:"token_name"`
	DetailKw    string `json:"detail_kw"`
	RequestID   string `json:"request_id"`
	Total       int64  `json:"total"`
	StartCursor int64  `json:"start_cursor"`
	ExpiresAt   int64  `json:"expires_at"`
}

func (m *Monitor) portalExportMACKey() []byte {
	return []byte(m.cfg.SessionSecret + "|portal-export")
}

func (m *Monitor) signPortalExportClaim(claim portalExportClaim) (string, error) {
	raw, err := json.Marshal(claim)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, m.portalExportMACKey())
	_, _ = mac.Write([]byte(enc))
	return enc + "." + hex.EncodeToString(mac.Sum(nil)), nil
}

func (m *Monitor) verifyPortalExportClaim(token string, nowUnix int64) (portalExportClaim, bool) {
	var claim portalExportClaim
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return claim, false
	}
	mac := hmac.New(sha256.New, m.portalExportMACKey())
	_, _ = mac.Write([]byte(parts[0]))
	if !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(parts[1])) {
		return claim, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(raw, &claim) != nil {
		return portalExportClaim{}, false
	}
	if claim.GID <= 0 || claim.MemberFP == "" || claim.FromTs <= 0 || claim.ToTs <= claim.FromTs || claim.Total < 0 || claim.StartCursor < 0 || claim.ExpiresAt <= nowUnix {
		return portalExportClaim{}, false
	}
	return claim, true
}

// ---- 登录限流(来源 IP,窗口内失败次数封顶) ----

type portalLimiter struct {
	mu sync.Mutex
	m  map[string][]int64
}

func (l *portalLimiter) tooMany(key string, now int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts, exists := l.m[key]
	if !exists {
		return false
	}
	cut := now - int64(portalLoginWindow.Seconds())
	kept := ts[:0]
	for _, t := range ts {
		if t > cut {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(l.m, key)
		return false
	}
	l.m[key] = kept
	return len(kept) >= portalLoginMaxFails
}

func (l *portalLimiter) fail(key string, now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.m[key]; !exists && len(l.m) >= loginLimiterMaxKeys {
		l.pruneLocked(now)
		if len(l.m) >= loginLimiterMaxKeys {
			return // 入口受保护优先，宁可不记录新的来源也不能让限流表无界增长。
		}
	}
	l.m[key] = append(l.m[key], now)
}

// prune 清掉窗口内已无失败记录的键,防止攻击者用大量不同 IP/邮箱刷 /login 使 map 无界增长。
func (l *portalLimiter) prune(now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(now)
}

func (l *portalLimiter) pruneLocked(now int64) {
	cut := now - int64(portalLoginWindow.Seconds())
	for k, ts := range l.m {
		kept := ts[:0]
		for _, t := range ts {
			if t > cut {
				kept = append(kept, t)
			}
		}
		if len(kept) == 0 {
			delete(l.m, k)
		} else {
			l.m[k] = kept
		}
	}
}

// ---- 导出限流(每组织账号 gid:窗口内最多 1 次,仅计成功下载) ----
// 原子预占语义:reserve 在检查通过的同一把锁内立即写入占位,并发请求不可能同时通过
// (check-then-act 分离会被并发绕过);探测/失败路径用 rollback 退回,保住"仅计成功下载"。

type exportLimiter struct {
	mu      sync.Mutex
	last    map[int64]int64 // gid -> 最近一次占位(成功导出/在途预占)unix
	prepare map[int64]int64 // gid -> 最近一次预检 unix；只保护 COUNT，不计入成功导出限额
}

// portalBufferedResponseWriter keeps a protected aggregate response off the
// socket until the current group membership and Portal credential version are
// checked again. JSON aggregate responses are already bounded; streaming CSV
// uses its separate ticket/fingerprint protocol and is never buffered here.
type portalBufferedResponseWriter struct {
	gin.ResponseWriter
	header http.Header
	body   bytes.Buffer
	status int
}

func newPortalBufferedResponseWriter(base gin.ResponseWriter) *portalBufferedResponseWriter {
	header := make(http.Header, len(base.Header()))
	for key, values := range base.Header() {
		header[key] = append([]string(nil), values...)
	}
	return &portalBufferedResponseWriter{ResponseWriter: base, header: header}
}

func (w *portalBufferedResponseWriter) Header() http.Header { return w.header }

func (w *portalBufferedResponseWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}

func (w *portalBufferedResponseWriter) WriteHeaderNow() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
}

func (w *portalBufferedResponseWriter) Write(payload []byte) (int, error) {
	w.WriteHeaderNow()
	return w.body.Write(payload)
}

func (w *portalBufferedResponseWriter) WriteString(payload string) (int, error) {
	w.WriteHeaderNow()
	return w.body.WriteString(payload)
}

func (w *portalBufferedResponseWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *portalBufferedResponseWriter) Size() int     { return w.body.Len() }
func (w *portalBufferedResponseWriter) Written() bool { return w.status != 0 }
func (w *portalBufferedResponseWriter) Flush()        { w.WriteHeaderNow() }

func (w *portalBufferedResponseWriter) commitTo(target gin.ResponseWriter) {
	for key := range target.Header() {
		delete(target.Header(), key)
	}
	for key, values := range w.header {
		target.Header()[key] = append([]string(nil), values...)
	}
	target.WriteHeader(w.Status())
	_, _ = target.Write(w.body.Bytes())
}

func (m *Monitor) portalAggregateAuthorizationGuard(next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		gid := c.GetInt64("portalGID")
		sessionAuthVersion := c.GetInt64("portalAuthVer")
		for attempt := 0; attempt < 2; attempt++ {
			before, err := m.loadPortalUsageAuthorizationSnapshot(c.Request.Context(), gid)
			if err != nil || before.AuthVersion != sessionAuthVersion {
				if errors.Is(err, errPortalAuthorizationInvalid) || (err == nil && before.AuthVersion != sessionAuthVersion) {
					c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
				} else {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": "成员权限状态暂不可用，请稍后重试"})
				}
				return
			}

			original := c.Writer
			buffered := newPortalBufferedResponseWriter(original)
			func() {
				c.Writer = buffered
				defer func() { c.Writer = original }()
				next(c)
			}()
			// Error responses contain no authorized aggregate payload. Preserve
			// their original status instead of turning ordinary validation into
			// a retry loop.
			if buffered.Status() < 200 || buffered.Status() >= 300 {
				buffered.commitTo(original)
				return
			}
			after, err := m.loadPortalUsageAuthorizationSnapshot(c.Request.Context(), gid)
			if err != nil {
				if errors.Is(err, errPortalAuthorizationInvalid) {
					c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
				} else {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": "成员权限状态暂不可用，请稍后重试"})
				}
				return
			}
			if before.equal(after) {
				buffered.commitTo(original)
				return
			}
			if attempt == 0 {
				continue
			}
			c.JSON(http.StatusConflict, gin.H{"error": "成员范围已变化，请重试"})
			return
		}
	}
}

// allowPrepare 把“成功导出窗口检查”和“预检防连点”放在同一临界区。
// 它只限制昂贵的 COUNT+MAX(id) 频率；真正下载仍由 reserve 原子限流。
func (l *exportLimiter) allowPrepare(gid, now, exportWindow, prepareWindow int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now-l.last[gid] < exportWindow {
		return false
	}
	if l.prepare == nil {
		l.prepare = make(map[int64]int64)
	}
	if now-l.prepare[gid] < prepareWindow {
		return false
	}
	l.prepare[gid] = now
	return true
}

// reserve 窗口内无占用则原子占位并返回旧值(供回退);否则 ok=false。
func (l *exportLimiter) reserve(gid, now, window int64) (prev int64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now-l.last[gid] < window {
		return 0, false
	}
	prev = l.last[gid]
	l.last[gid] = now
	return prev, true
}

// rollback 撤销 reserve 的占位(探测/出错未真正下载时调用);仅当占位仍是自己写的才回退。
func (l *exportLimiter) rollback(gid, prev, reservedAt int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last[gid] == reservedAt {
		l.last[gid] = prev
	}
}

func (l *exportLimiter) prune(now, window int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, t := range l.last {
		if now-t > window*2 {
			delete(l.last, k)
		}
	}
	for k, t := range l.prepare {
		if now-t > int64(portalExportPrepWindow.Seconds())*2 {
			delete(l.prepare, k)
		}
	}
}

func (m *Monitor) portalExportTempDir() (string, error) {
	storePath := strings.TrimSpace(m.cfg.StorePath)
	if !storeUsesFile(storePath) {
		return "", errors.New("未配置可持久化的 Monitor 数据卷")
	}
	return filepath.Join(filepath.Dir(storePath), portalExportTempDirName), nil
}

func preparePortalExportTempDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// 即使目录由旧版本或手工预先创建，也不允许其他用户读取客户日志。
	return os.Chmod(dir, 0o700)
}

func ensurePortalExportDiskCapacity(dir string, maxBytes int64) error {
	if maxBytes <= 0 {
		return errPortalExportTooLarge
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return err
	}
	blockSize := uint64(stat.Bsize)
	available := stat.Bavail * blockSize
	required := uint64(maxBytes + portalExportDiskReserveBytes)
	if available < required {
		return fmt.Errorf("%w: available=%d required=%d", errPortalExportInsufficientStorage, available, required)
	}
	return nil
}

func (m *Monitor) createPortalExportTempFile(maxBytes int64) (*os.File, *portalExportStorageLease, error) {
	select {
	case portalExportProcessGate <- struct{}{}:
	default:
		return nil, nil, errPortalExportStorageBusy
	}
	lease := &portalExportStorageLease{}
	fail := func(err error) (*os.File, *portalExportStorageLease, error) {
		lease.release()
		return nil, nil, err
	}
	dir, err := m.portalExportTempDir()
	if err != nil {
		return fail(err)
	}
	if err := preparePortalExportTempDir(dir); err != nil {
		return fail(err)
	}
	lockFile, err := os.OpenFile(filepath.Join(dir, ".export.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fail(err)
	}
	lease.lockFile = lockFile
	if err := lockFile.Chmod(0o600); err != nil {
		return fail(err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return fail(errPortalExportStorageBusy)
		}
		return fail(err)
	}
	if err := ensurePortalExportDiskCapacity(dir, maxBytes); err != nil {
		return fail(err)
	}
	tmp, err := os.CreateTemp(dir, portalExportTempFilePrefix+"*.csv")
	if err != nil {
		return fail(err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		name := tmp.Name()
		_ = tmp.Close()
		_ = os.Remove(name)
		return fail(err)
	}
	return tmp, lease, nil
}

// cleanupPortalExportTempFiles 只删除本功能前缀且超过最长正常导出时间的
// 普通文件，不跟随符号链接，也不触碰数据卷里的其他文件。
func (m *Monitor) cleanupPortalExportTempFiles(now time.Time) error {
	dir, err := m.portalExportTempDir()
	if err != nil {
		return err
	}
	if err := preparePortalExportTempDir(dir); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), portalExportTempFilePrefix) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, statErr := os.Lstat(path)
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				continue
			}
			return statErr
		}
		if !info.Mode().IsRegular() || now.Sub(info.ModTime()) < portalExportStaleFileAge {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func portalExportStorageError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errPortalExportTooLarge):
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "导出内容超过 128 MiB，请缩小日期范围或筛选条件"})
	case errors.Is(err, errPortalExportStorageBusy):
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "另一个客户导出正在生成，请稍后重试"})
	case errors.Is(err, errPortalExportInsufficientStorage), errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT):
		c.JSON(http.StatusInsufficientStorage, gin.H{"error": "导出临时存储空间不足，请联系管理员或稍后重试"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "准备下载失败,请稍后重试"})
	}
}

// ---- 路由注册(独立引擎,挂到独立端口) ----

func (m *Monitor) RegisterPortalRoutes(r *gin.Engine) {
	r.Use(requestBodyLimit(maxJSONRequestBody))
	if m.usageCache == nil {
		m.usageCache = newUsageResultCache(m.cfg)
	}
	if m.portalLim == nil {
		m.portalLim = &portalLimiter{m: map[string][]int64{}}
	}
	if m.exportLim == nil {
		m.exportLim = &exportLimiter{last: map[int64]int64{}}
	}
	if err := m.cleanupPortalExportTempFiles(time.Now()); err != nil {
		// 不因为清理失败隐式关闭 Portal；真正导出时还会严格检查
		// 目录权限和剩余空间，并在响应前明确返回 503/507。
		slog.Warn("客户 CSV 导出残留文件清理失败", "err", err)
	}
	// 限流表 GC:低频粗扫,防长期运行/被刷时缓慢增长。结果缓存自身有容量硬上限，
	// Redis 键又全部带 TTL，不需要后台全表扫描。
	m.portalGCOnce.Do(func() {
		go func() {
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					m.portalLim.prune(time.Now().Unix())
					m.exportLim.prune(time.Now().Unix(), int64(portalExportWindow.Seconds()))
				case <-m.shutdownSignal():
					return
				}
			}
		}()
	})

	r.GET("/echarts.js", func(c *gin.Context) { // 图表库自服务,不走 CDN(客户域名零外链)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", echartsJS)
	})
	r.GET("/flatpickr.js", func(c *gin.Context) { // 日期范围选择器,与管理端同一控件、自服务
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", flatpickrJS)
	})
	r.GET("/flatpickr.css", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "text/css; charset=utf-8", flatpickrCSS)
	})
	r.GET("/range-picker.js", func(c *gin.Context) { // 与管理端同源的中转站风格范围选择器
		// 这是会随 Monitor 功能调整的适配层，不做永久不可变缓存。
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", rangePickerJS)
	})
	r.GET("/react.js", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", reactJS)
	})
	r.GET("/react-dom.js", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", reactDOMJS)
	})
	r.GET("/semi-ui.js", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", semiUIJS)
	})
	r.GET("/semi-ui.css", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "text/css; charset=utf-8", semiUICSS)
	})
	r.GET("/login", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, portalLoginHTML)
	})
	r.POST("/login", m.portalLogin)
	r.GET("/logout", func(c *gin.Context) {
		c.SetCookie(portalSessionCookie, "", -1, "/", "", false, true)
		c.Redirect(http.StatusFound, "/login")
	})

	page := r.Group("/", m.requirePortal(false))
	page.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, portalHTML)
	})
	api := r.Group("/api", m.requirePortal(true))
	api.GET("/overview", m.portalAggregateAuthorizationGuard(m.portalOverview))
	api.GET("/breakdown", m.portalAggregateAuthorizationGuard(m.portalBreakdown)) // 整组按分组/按模型汇总
	api.GET("/user", m.portalAggregateAuthorizationGuard(m.portalUserDetail))
	api.GET("/logs", m.portalAggregateAuthorizationGuard(m.portalLogs))                             // 使用日志:游标分页查看
	api.GET("/logs/export/prepare", m.portalAggregateAuthorizationGuard(m.portalLogsExportPrepare)) // CSV 预检并签发短期下载票据
	api.GET("/logs/export", m.portalLogsExport)                                                     // 使用日志:CSV 原生下载(超5万确认/限流)
	api.POST("/password", m.portalChangePassword)
}

// requirePortal 客户会话门:apiMode=true 未登录回 401 JSON,否则 302 到 /login。
func (m *Monitor) requirePortal(apiMode bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok, _ := c.Cookie(portalSessionCookie)
		gid, authVer, ok := m.verifyPortalSession(tok, time.Now().Unix())
		if !ok {
			if apiMode {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			} else {
				c.Redirect(http.StatusFound, "/login")
				c.Abort()
			}
			return
		}
		// 会话有效但账号可能已被关闭:每次核一遍(代价=本地 sqlite 主键查,可忽略)
		var g CustomerGroup
		if err := m.storeDB.First(&g, gid).Error; err != nil || g.PortalEmail == "" || g.PortalAuthVer != authVer {
			if apiMode {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			} else {
				c.Redirect(http.StatusFound, "/login")
				c.Abort()
			}
			return
		}
		c.Set("portalGID", gid)
		c.Set("portalAuthVer", authVer)
		c.Set("portalGroupName", g.Name)
		c.Next()
	}
}

func (m *Monitor) portalLogin(c *gin.Context) {
	if !limitBodyForLogin(c) {
		return
	}
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请输入账号和密码"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(in.Email))
	now := time.Now().Unix()
	// 按来源 IP 限制，而不是 IP+账号；否则攻击者不断换账号即可绕过限流。
	// Gin 仅接受 MONITOR_TRUSTED_PROXIES 指定反代提供的转发头，避免来源 IP 被伪造。
	limKey := c.ClientIP()
	if m.portalLim.tooMany(limKey, now) {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "尝试次数过多,请稍后再试"})
		return
	}
	var g CustomerGroup
	err := m.storeDB.Where("portal_email = ? AND portal_email <> ''", email).First(&g).Error
	// 双密码:我方配置密码 / 客户自改密码,任一匹配即可
	if err != nil || (!checkPassword(g.PortalPwAdmin, in.Password) && !checkPassword(g.PortalPwUser, in.Password)) {
		m.portalLim.fail(limKey, now)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "账号或密码错误"})
		return
	}
	tok := m.signPortalSession(g.ID, g.PortalAuthVer, now)
	secure := c.Request.TLS != nil || c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(portalSessionCookie, tok, int(portalSessionTTL.Seconds()), "/", "", secure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true, "group_name": g.Name})
}

// portalChangePassword POST /api/password {old,new}:客户自改密码(写 PortalPwUser;我方配置密码始终有效)。
func (m *Monitor) portalChangePassword(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	var in struct {
		Old string `json:"old"`
		New string `json:"new"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || len(in.New) < 8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "新密码至少 8 位"})
		return
	}
	var g CustomerGroup
	if err := m.storeDB.First(&g, gid).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	if !checkPassword(g.PortalPwAdmin, in.Old) && !checkPassword(g.PortalPwUser, in.Old) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "当前密码不正确"})
		return
	}
	h, err := hashPassword(in.New)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "密码处理失败"})
		return
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", gid).Updates(map[string]any{
		"portal_pw_user":  h,
		"portal_auth_ver": gorm.Expr("portal_auth_ver + 1"),
	}).Error; err != nil {
		slog.Warn("客户改密码写库失败", "gid", gid, "err", err) // 细节进日志,不回显库内部结构给客户
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存失败,请稍后重试"})
		return
	}
	secure := c.Request.TLS != nil || c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(portalSessionCookie, "", -1, "/", "", secure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---- 组隔离数据接口(gid 只从会话取) ----

type portalOverviewPayload struct {
	GroupName        string            `json:"group_name"`
	From             string            `json:"from"`
	To               string            `json:"to"`
	RequestedFrom    string            `json:"requested_from,omitempty"`
	RequestedTo      string            `json:"requested_to,omitempty"`
	RangePartial     bool              `json:"range_partial"`
	RangeMessage     string            `json:"range_message,omitempty"`
	Days             []string          `json:"days"`
	Users            []UsageMatrixUser `json:"users"` // 复用矩阵行结构(note/group 字段对本组无泄露风险,前端不展示 note)
	Cells            []UsageMatrixCell `json:"cells"`
	DailyByModel     []UsageDailyModel `json:"daily_by_model"` // 供首页每日消费趋势按模型堆叠展示
	ByModel          []UsageDim        `json:"by_model"`       // 供堆叠图确定 top-N 模型
	ByModelTruncated bool              `json:"by_model_truncated"`
	TrendAvailable   bool              `json:"trend_available"`
	// TrendStale 表示趋势使用的是最近一次成功统计，或本地事实层尚未追平已闭合小时。
	// 它与 TrendAvailable 分离，避免把“可读但非最新”误报成“趋势不可用”。
	TrendStale   bool   `json:"trend_stale"`
	TrendMessage string `json:"trend_message,omitempty"`
	// MatrixAvailable 只描述“所选范围的每日消费矩阵”是否可读；成员资料、当前余额和
	// 累计消耗是独立资料域，即使这里为 false 也必须继续返回，不能把整页降成 500。
	MatrixAvailable bool   `json:"matrix_available"`
	MatrixStale     bool   `json:"matrix_stale"`
	MatrixMessage   string `json:"matrix_message,omitempty"`
}

// portalBreakdownPayload 是门户“按分组/按模型”的独立数据域。Available=false 时，
// 页面必须保留总览、余额和每日矩阵，不把暂时无法取得的细分聚合伪造成 0 或整页 500。
type portalBreakdownPayload struct {
	Available        bool       `json:"available"`
	Stale            bool       `json:"stale"`
	RangePartial     bool       `json:"range_partial"`
	RangeMessage     string     `json:"range_message,omitempty"`
	Message          string     `json:"message,omitempty"`
	ByGroup          []UsageDim `json:"by_group"`
	ByModel          []UsageDim `json:"by_model"`
	ByGroupTruncated bool       `json:"by_group_truncated"`
	ByModelTruncated bool       `json:"by_model_truncated"`
}

// portalMemberFingerprint 只使用服务端名单中的 user_id，排序后取 SHA-256 前 128 位。
// 它不是鉴权凭据，只用于让成员增删/移动后自然换键，避免 Redis 中旧权限域被重新命中。
func portalMemberFingerprint(tracked []TrackedUser) string {
	return portalMemberFingerprintFromIDs(idsOf(tracked))
}

func portalMemberFingerprintFromIDs(ids []int64) string {
	ids = append([]int64(nil), ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write([]byte(strconv.FormatInt(id, 10)))
		_, _ = h.Write([]byte{','})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func portalGroupAggregateKey(kind string, gid int64, memberFP string, fromTs, toTs int64) string {
	return fmt.Sprintf("agg:%s:portal:g:%d:m:%s:r:%d:%d:%s", usageAggregateKeyVersion, gid, memberFP, fromTs, toTs, kind)
}

func portalUserAggregateKey(kind string, gid int64, memberFP string, uid, tokenID, fromTs, toTs int64) string {
	return fmt.Sprintf("agg:%s:portal:g:%d:m:%s:r:%d:%d:u:%d:t:%d:%s", usageAggregateKeyVersion, gid, memberFP, fromTs, toTs, uid, tokenID, kind)
}

// loadPortalGroupMatrix 只缓存门户总览必需的每日矩阵。趋势统计是独立的数据域：
// 即使趋势查询暂时不可用，客户仍应能看到成员、余额和每日消费明细。
func (m *Monitor) loadPortalGroupMatrix(
	ctx context.Context,
	gid int64,
	memberFP string,
	ids []int64,
	fromTs, toTs int64,
) (*UsageMatrix, bool, error) {
	out := &UsageMatrix{}
	stale, err := m.loadUsageAggregateJSONStaleIfError(
		ctx,
		m.usageFactCacheKey(portalGroupAggregateKey("matrix", gid, memberFP, fromTs, toTs)),
		usageAggregateTTL(toTs, time.Now()),
		toTs,
		false,
		out,
		func() (any, error) {
			mx, err := m.computeUsageMatrixForRead(ctx, ids, fromTs, toTs)
			if err != nil {
				return nil, err
			}
			if mx == nil {
				return nil, errors.New("用量矩阵结果为空")
			}
			// 防御未来 computeUsageMatrix 改动：Redis 只允许稀疏消费格，
			// 用户名、邮箱、余额等资料不进入共享缓存，由当前读取模式独立提供。
			mx.Users = nil
			return mx, nil
		},
	)
	return out, stale, err
}

// loadPortalGroupStats 独立缓存按模型/按分组趋势统计。该数据域失败时，调用方
// 可以仅降级趋势或排行卡片，不能连带清空总览矩阵和资料快照。
func (m *Monitor) loadPortalGroupStats(
	ctx context.Context,
	gid int64,
	memberFP string,
	ids []int64,
	fromTs, toTs int64,
) (*UsageStats, bool, error) {
	out := &UsageStats{}
	stale, err := m.loadUsageAggregateJSONStaleIfError(
		ctx,
		m.usageFactCacheKey(portalGroupAggregateKey("stats", gid, memberFP, fromTs, toTs)),
		usageAggregateTTL(toTs, time.Now()),
		toTs,
		false,
		out,
		func() (any, error) {
			st, err := m.computeUsageStatsForRead(ctx, ids, fromTs, toTs, 0)
			if err != nil {
				return nil, err
			}
			if st == nil {
				return nil, errors.New("用量统计结果为空")
			}
			return st, nil
		},
	)
	return out, stale, err
}

func (m *Monitor) portalOverview(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	val, err := m.buildPortalOverview(c, gid, fromTs, toTs)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": val})
}

func (m *Monitor) buildPortalOverview(c *gin.Context, gid, fromTs, toTs int64) (*portalOverviewPayload, error) {
	var g CustomerGroup
	if err := m.storeDB.First(&g, gid).Error; err != nil {
		return nil, err
	}
	tracked, err := m.portalTrackedMembersForUsageRead(c.Request.Context(), gid)
	if err != nil {
		return nil, err
	}
	requestedRange := newUsageMatrixRange(fromTs, toTs)
	p := &portalOverviewPayload{
		GroupName: g.Name, TrendAvailable: true, MatrixAvailable: true,
		RequestedFrom: requestedRange.From, RequestedTo: requestedRange.To,
	}
	if len(tracked) == 0 {
		// 空分组不需要、也不应触碰事实库或主站 logs；日期轴可在本地纯计算得到。
		mx := requestedRange
		p.From, p.To, p.Days = mx.From, mx.To, mx.Days
		p.Users, p.Cells = []UsageMatrixUser{}, []UsageMatrixCell{}
		return p, nil
	}
	tracked, balances, usedTotals := m.refreshTrackedLabelsForRead(c.Request.Context(), tracked)
	if err := c.Request.Context().Err(); err != nil {
		return nil, err
	}
	ids := idsOf(tracked)
	memberFP := portalMemberFingerprint(tracked)
	readRange := m.resolveUsageAggregateReadRange(fromTs, toTs)
	fromTs, toTs = readRange.From, readRange.To
	p.RangePartial, p.RangeMessage = readRange.Partial, readRange.Message
	mx := requestedRange
	if readRange.Available {
		mx = newUsageMatrixRange(fromTs, toTs)
	}
	matrixSkippedForSize := readRange.Available && usageMatrixExceedsCellBudget(len(tracked), fromTs, toTs)
	var matrixStale bool
	var matrixErr error
	if !readRange.Available {
		matrixErr = errUsageFactsNotReady
	} else if matrixSkippedForSize {
		p.MatrixAvailable = false
		p.RangePartial = false
		p.MatrixMessage = usageMatrixCellBudgetMessage(len(tracked), fromTs, toTs)
	} else {
		mx, matrixStale, matrixErr = m.loadPortalGroupMatrix(c.Request.Context(), gid, memberFP, ids, fromTs, toTs)
	}
	if matrixErr != nil {
		if c.Request.Context().Err() != nil {
			return nil, matrixErr
		}
		// 每日消费矩阵是独立数据域。这里绝不能因为聚合暂时失败而吞掉已经成功取得的
		// 成员、余额和累计消耗；范围消费保持“不可用”而非伪造为 0。
		p.MatrixAvailable = false
		p.RangePartial = false
		p.MatrixMessage = readRange.Message
		if p.MatrixMessage == "" {
			p.MatrixMessage = "每日消费明细暂不可用，余额和累计消耗仍可查看"
		}
		mx = requestedRange
		slog.Warn("客户门户每日消费矩阵失败，保留资料快照", "gid", gid, "err", matrixErr)
	} else {
		matrixStale, matrixMessage := m.usageDataStaleness(matrixStale, fromTs, toTs, time.Now())
		if matrixStale {
			p.MatrixStale = true
			p.MatrixMessage = matrixMessage
		}
	}
	totals := map[int64]UsageBilling{}
	for _, cell := range mx.Cells {
		t := totals[cell.UserID]
		t.add(cell.UsageBilling)
		totals[cell.UserID] = t
	}
	for _, u := range tracked {
		t := totals[u.UserID]
		mu := UsageMatrixUser{UserID: u.UserID, Username: u.Username, Email: u.Email,
			ConsumeQuota: t.ConsumeQuota, RefundQuota: t.RefundQuota, NetQuota: t.NetQuota,
			TotalUSD: t.CostUSD, RefundUSD: t.RefundUSD, NetUSD: t.NetUSD}
		if b, ok := balances[u.UserID]; ok {
			bq := b
			bv := float64(bq) / quotaPerUSD
			mu.BalanceQuota, mu.BalanceUSD = &bq, &bv
		}
		if uq, ok := usedTotals[u.UserID]; ok {
			usedQ := uq
			uv := float64(usedQ) / quotaPerUSD
			mu.TotalUsedQuota, mu.TotalUsedUSD = &usedQ, &uv
		}
		p.Users = append(p.Users, mu)
	}
	sortPortalUsers(p.Users)
	p.From, p.To, p.Days, p.Cells = mx.From, mx.To, mx.Days, mx.Cells
	// 当核心每日矩阵已经明确无法读取时，不再在同一个页面请求中继续触发另一条
	// 依赖 logs 的趋势聚合。这样既保留用户资料/余额/累计消耗，也避免源库故障时
	// 一次打开门户连续发起两次无效扫描。矩阵正常时，趋势仍是独立数据域。
	if !p.MatrixAvailable && !matrixSkippedForSize {
		p.TrendAvailable = false
		p.TrendMessage = "趋势统计未加载：每日消费明细暂不可用"
		return p, nil
	}
	// 趋势和矩阵刻意分域：趋势失败时，保留已成功取得的矩阵和资料。
	st, statsStale, err := m.loadPortalGroupStats(c.Request.Context(), gid, memberFP, ids, fromTs, toTs)
	if err != nil {
		if c.Request.Context().Err() != nil {
			return nil, err
		}
		p.TrendAvailable = false
		p.TrendMessage = "趋势统计暂不可用，其他用量数据正常"
		slog.Warn("客户门户趋势统计失败，保留总览矩阵", "gid", gid, "err", err)
		return p, nil
	}
	p.DailyByModel = st.DailyByModel
	p.ByModel = st.ByModel
	p.ByModelTruncated = st.ByModelTruncated
	if stale, message := m.usageDataStaleness(statsStale, fromTs, toTs, time.Now()); stale {
		p.TrendStale = true
		p.TrendMessage = message
	}
	return p, nil
}

// sortPortalUsers 与管理端同规则:累计总消耗降序,稳定。
func sortPortalUsers(users []UsageMatrixUser) {
	usedOf := func(u UsageMatrixUser) int64 {
		if u.TotalUsedQuota != nil {
			return *u.TotalUsedQuota
		}
		return 0
	}
	sort.SliceStable(users, func(i, j int) bool {
		ui, uj := usedOf(users[i]), usedOf(users[j])
		if ui != uj {
			return ui > uj
		}
		return users[i].Username < users[j].Username
	})
}

// portalBreakdown GET /api/breakdown:整组(公司)按分组 + 按模型的汇总。
// 它与趋势共用 stats 数据域，但和总览矩阵刻意隔离，局部失败不影响首页核心数据。
func (m *Monitor) portalBreakdown(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	tracked, err := m.portalTrackedMembersForUsageRead(c.Request.Context(), gid)
	if err != nil {
		slog.Warn("查询客户组成员失败", "gid", gid, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	ids := idsOf(tracked)
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": portalBreakdownPayload{
			Available: true, ByGroup: []UsageDim{}, ByModel: []UsageDim{},
		}})
		return
	}
	readRange := m.resolveUsageAggregateReadRange(fromTs, toTs)
	fromTs, toTs = readRange.From, readRange.To
	if !readRange.Available {
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": portalBreakdownPayload{
			Available: false,
			Message:   readRange.Message,
			ByGroup:   []UsageDim{},
			ByModel:   []UsageDim{},
		}})
		return
	}
	st, statsStale, err := m.loadPortalGroupStats(
		c.Request.Context(), gid, portalMemberFingerprint(tracked), ids, fromTs, toTs,
	)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		// 细分排行属于独立、可降级数据域。这里返回明确的可用性状态而不是 500，
		// 让前端仅降级两个排行卡片，避免客户误以为整个用户用量页面不可使用。
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": portalBreakdownPayload{
			Available: false,
			Message:   "分组与模型统计暂不可用，其他用量数据正常",
			ByGroup:   []UsageDim{},
			ByModel:   []UsageDim{},
		}})
		return
	}
	dataStale, dataMessage := m.usageDataStaleness(statsStale, fromTs, toTs, time.Now())
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": portalBreakdownPayload{
		Available:        true,
		Stale:            dataStale,
		RangePartial:     readRange.Partial,
		RangeMessage:     readRange.Message,
		Message:          dataMessage,
		ByGroup:          st.ByGroup,
		ByModel:          st.ByModel,
		ByGroupTruncated: st.ByGroupTruncated,
		ByModelTruncated: st.ByModelTruncated,
	}})
}

func (m *Monitor) portalUserDetail(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	uid, _ := strconv.ParseInt(c.Query("uid"), 10, 64)
	if uid <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "uid required"})
		return
	}
	// 越权闸是“当前 active ∩ 已发布且 revision 匹配”。新增/重加
	// 成员在全历史签收前只对管理员可见，Portal 不返回其身份、余额或小计。
	tracked, err := m.portalTrackedMembersForUsageRead(c.Request.Context(), gid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	found := false
	var member TrackedUser
	for _, u := range tracked {
		if u.UserID == uid {
			found = true
			member = u
			break
		}
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	servingTracked := tracked
	statsMemberReady := true
	// 令牌下钻(可选):聚合强制 uid+token_id 双条件,别组令牌只会查出空,不做归属探测响应差异
	var tokenID int64
	if t := strings.TrimSpace(c.Query("token_id")); t != "" {
		tokenID, _ = strconv.ParseInt(t, 10, 64)
		if tokenID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "token_id 不合法"})
			return
		}
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	readRange := m.resolveUsageAggregateReadRange(fromTs, toTs)
	requestedRange := newUsageStatsRange(fromTs, toTs)
	requestedFrom, requestedTo := requestedRange.From, requestedRange.To
	fromTs, toTs = readRange.From, readRange.To
	memberFP := portalMemberFingerprint(servingTracked)
	cacheTTL := usageAggregateTTL(toTs, time.Now())
	st := requestedRange
	var dataStale bool
	if statsMemberReady && readRange.Available {
		st = newUsageStatsRange(fromTs, toTs)
		dataStale, err = m.loadUsageAggregateJSONStaleIfError(
			c.Request.Context(),
			m.usageFactCacheKey(portalUserAggregateKey("stats", gid, memberFP, uid, tokenID, fromTs, toTs)),
			cacheTTL,
			toTs,
			false,
			st,
			func() (any, error) {
				return m.computeUsageStatsForRead(c.Request.Context(), []int64{uid}, fromTs, toTs, tokenID)
			},
		)
	} else {
		err = errUsageFactsNotReady
	}
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		// 详情的历史用量与账户资料是独立数据域。范围聚合不可用时，仍返回
		// 成员身份、当前余额和累计总消耗；空 stats 只表示“未知”，不能显示为 0。
		slog.Warn("客户门户成员详情聚合失败，保留账户资料", "gid", gid, "user_id", uid, "err", err)
		st = requestedRange
	}
	statsAvailable := err == nil
	statsMessage := ""
	dataMessage := ""
	if statsAvailable {
		dataStale, dataMessage = m.usageDataStaleness(dataStale, fromTs, toTs, time.Now())
	} else {
		dataStale = false
		statsMessage = readRange.Message
		if statsMessage == "" {
			statsMessage = "范围统计暂不可用，余额和累计消耗仍可查看"
		}
	}
	if tokenID > 0 { // 令牌资料按当前读取模式读取；缓存中只有日志聚合数字。
		data := gin.H{
			"stats": st, "stats_available": statsAvailable, "stats_message": statsMessage,
			"token":          m.tokenMetaOfForRead(c.Request.Context(), uid, tokenID),
			"range_partial":  statsAvailable && readRange.Partial,
			"range_message":  readRange.Message,
			"requested_from": requestedFrom, "requested_to": requestedTo,
		}
		if dataStale {
			data["data_stale"] = true
			data["data_message"] = dataMessage
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": data})
		return
	}

	// 令牌明细是一个独立、可降级的数据域。即使该细分聚合或资料快照临时失败，
	// 已成功读取的用户汇总、余额、累计消耗仍应返回给客户，而不是整页报错。
	tokenDataAvailable := statsAvailable
	tokenDataMessage := ""
	tokenAggregates := make([]tokenUsageAggregate, 0)
	if !statsAvailable {
		tokenDataMessage = "令牌明细未加载：范围统计暂不可用"
	} else {
		err = m.usageCache.DoJSON(
			c.Request.Context(),
			m.usageFactCacheKey(portalUserAggregateKey("tokens", gid, memberFP, uid, 0, fromTs, toTs)),
			cacheTTL,
			&tokenAggregates,
			func() (any, error) {
				return m.computeUserTokenAggregatesForRead(c.Request.Context(), uid, fromTs, toTs)
			},
		)
		if err != nil {
			if abortCanceledUsageRequest(c, err) {
				return
			}
			slog.Warn("客户门户令牌用量聚合失败，保留用户总览", "gid", gid, "user_id", uid, "err", err)
			tokenDataAvailable = false
			tokenDataMessage = "令牌明细暂不可用，其他统计正常"
		}
	}
	// 先传名单展示名；下方按当前读取模式用资料快照/主站资料校准，避免
	// hydrateUserTokenUsage 内再发起一次必然会被后续结果覆盖的资料查询。
	ownerSnapshot := trackedLabel(member)
	toks := make([]TokenUsage, 0)
	if tokenDataAvailable {
		toks, err = m.hydrateUserTokenUsageForRead(c.Request.Context(), uid, tokenAggregates, &ownerSnapshot)
		if err != nil {
			if abortCanceledUsageRequest(c, err) {
				return
			}
			slog.Warn("客户门户令牌资料读取失败，保留用户总览", "gid", gid, "user_id", uid, "err", err)
			tokenDataAvailable = false
			tokenDataMessage = "令牌明细暂不可用，其他统计正常"
			toks = []TokenUsage{}
		}
	}
	// 资料与用量事实独立：普通模式读取主站，事实模式读取本地资料快照；
	// 均不进入 Redis。资料失败时沿用名单展示名，金额显示 —，不阻断历史统计。
	fresh, balances, usedTotals := m.refreshTrackedLabelsForRead(c.Request.Context(), []TrackedUser{member})
	if err := c.Request.Context().Err(); err != nil {
		abortCanceledUsageRequest(c, err)
		return
	}
	owner := trackedLabel(member)
	if len(fresh) == 1 {
		owner = trackedLabel(fresh[0])
	}
	for i := range toks {
		toks[i].Owner = owner
	}
	var balanceQ, usedQ *int64
	if v, ok := balances[uid]; ok {
		q := v
		balanceQ = &q
	}
	if v, ok := usedTotals[uid]; ok {
		q := v
		usedQ = &q
	}
	data := gin.H{
		"stats":                st,
		"stats_available":      statsAvailable,
		"stats_message":        statsMessage,
		"by_token":             toks, // key 只在资料读取阶段脱敏，缓存与事实表均不保存明文
		"token_data_available": tokenDataAvailable,
		"token_data_message":   tokenDataMessage,
		"balance_quota":        balanceQ,
		"balance_usd":          quotaUSDPtr(balanceQ),
		"total_used_quota":     usedQ,
		"total_used_usd":       quotaUSDPtr(usedQ),
		"range_partial":        statsAvailable && readRange.Partial,
		"range_message":        readRange.Message,
		"requested_from":       requestedFrom,
		"requested_to":         requestedTo,
	}
	if dataStale {
		data["data_stale"] = true
		data["data_message"] = dataMessage
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": data})
}

// ---- 使用日志(逐条明细):查看 + CSV 导出 ----

var (
	// errMemberNotInGroup:成员筛选值不属本组(越权探测)。对外统一装作 404,不暴露"组里有没有这个人"。
	errMemberNotInGroup = errors.New("member not in group")
	// errPortalMemberStore:本地客户名单读取失败。它不是参数错误，不能伪装成“暂无日志”。
	errPortalMemberStore = errors.New("portal member store unavailable")
)

// portalLogParamsError 统一处理 portalLogParams 的错误响应：越权探测→404，
// 本地名单库故障→500、事实发布切换/修复→503 通用文案，其余参数错误→400。
func portalLogParamsError(c *gin.Context, err error) {
	if errors.Is(err, errMemberNotInGroup) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	if errors.Is(err, errPortalMemberStore) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据读取失败,请稍后重试"})
		return
	}
	if errors.Is(err, errUsageFactsNotReady) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "用量明细暂不可用，请稍后重试"})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
}

func (m *Monitor) portalTrackedMembers(gid int64) ([]TrackedUser, error) {
	var tracked []TrackedUser
	if err := m.storeDB.Where("group_id = ?", gid).Find(&tracked).Error; err != nil {
		slog.Warn("客户日志成员名单读取失败", "gid", gid, "err", err)
		return nil, fmt.Errorf("%w: %w", errPortalMemberStore, err)
	}
	return tracked, nil
}

// portalLogParams 解析并校验日志相关的公共参数(组隔离:成员必属本组)。
func (m *Monitor) portalLogParams(c *gin.Context) (gid int64, ids []int64, memberUID, fromTs, toTs int64, logType int, model, group, tokenName, detailKw, requestID string, err error) {
	gid = c.GetInt64("portalGID")
	fromTs, toTs, err = parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		return
	}
	tracked, storeErr := m.portalTrackedMembersForUsageRead(c.Request.Context(), gid)
	if storeErr != nil {
		err = storeErr
		return
	}
	ids = idsOf(tracked)
	memberUID, _ = strconv.ParseInt(c.Query("member"), 10, 64)
	if memberUID > 0 { // 成员筛选值必须属本组(越权闸)
		in := false
		for _, id := range ids {
			if id == memberUID {
				in = true
				break
			}
		}
		if !in {
			err = errMemberNotInGroup
			return
		}
	}
	if t, e := strconv.Atoi(c.Query("type")); e == nil && t >= 1 && t <= 6 {
		logType = t // 具体类型(1-6 全部开放,同 new-api 官方客户端使用日志);其余(0/越界)= 全部
	}
	model = strings.TrimSpace(c.Query("model"))
	group = strings.TrimSpace(c.Query("group"))
	tokenName = strings.TrimSpace(c.Query("token"))
	if len(tokenName) > 64 { // 令牌名搜索限长:防超长 LIKE 模式拖慢生产库查询
		err = fmt.Errorf("令牌名搜索最长 64 字符")
		return
	}
	detailKw = strings.TrimSpace(c.Query("detail_kw"))
	if len(detailKw) > 64 { // 详情关键字搜索同限长,同一防御理由
		err = fmt.Errorf("详情关键字搜索最长 64 字符")
		return
	}
	requestID = strings.TrimSpace(c.Query("request_id"))
	if len(requestID) > 64 { // request_id 精确匹配,同一限长防御(new-api request_id 列本身是 varchar(64))
		err = fmt.Errorf("Request ID 最长 64 字符")
		return
	}
	err = validatePortalLogFuzzyRange(fromTs, toTs, tokenName, detailKw, false)
	return
}

// validatePortalLogFuzzyRange 在任何来源 SQL 之前拒绝高风险宽扫。
// request_id/model/group/member 都是精确匹配，不需要这个额外限制。
func validatePortalLogFuzzyRange(fromTs, toTs int64, tokenName, detailKw string, export bool) error {
	window := time.Duration(toTs-fromTs) * time.Second
	if tokenName != "" {
		if utf8.RuneCountInString(tokenName) < portalTokenFuzzyMinRunes {
			return fmt.Errorf("令牌名模糊搜索至少输入 %d 个字符", portalTokenFuzzyMinRunes)
		}
		limit := portalTokenFuzzyMaxRange
		if export {
			limit = portalExportTokenMaxRange
		}
		if window > limit {
			return fmt.Errorf("令牌名模糊搜索的时间范围最长 %d 天", int(limit/(24*time.Hour)))
		}
	}
	if detailKw != "" {
		if utf8.RuneCountInString(detailKw) < portalDetailFuzzyMinRunes {
			return fmt.Errorf("详情模糊搜索至少输入 %d 个字符", portalDetailFuzzyMinRunes)
		}
		limit := portalDetailFuzzyMaxRange
		if export {
			limit = portalExportDetailMaxRange
		}
		if window > limit {
			return fmt.Errorf("详情模糊搜索的时间范围最长 %d 天", int(limit/(24*time.Hour)))
		}
	}
	return nil
}

// portalLogs GET /api/logs:游标分页看本组消费日志(时间倒序)。
func (m *Monitor) portalLogs(c *gin.Context) {
	gid, ids, memberUID, fromTs, toTs, logType, model, group, tokenName, detailKw, requestID, err := m.portalLogParams(c)
	if err != nil {
		portalLogParamsError(c, err)
		return
	}
	_ = gid
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "rows": []LogRow{}, "has_more": false})
		return
	}
	beforeID, _ := strconv.ParseInt(c.Query("cursor"), 10, 64)
	rows, err := m.queryGroupLogs(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID, beforeID, portalLogPageSize+1)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	hasMore := len(rows) > portalLogPageSize
	if hasMore {
		rows = rows[:portalLogPageSize]
	}
	var next int64
	if len(rows) > 0 {
		next = rows[len(rows)-1].ID
	}
	// 游标 + has_more 已足够稳定翻页。不要为了一个总页数在首屏追加 COUNT(*)：
	// 大窗口/模糊筛选下，50 行本身可能瞬间返回，而全量计数却扫描数百万行，
	// 同时拖慢页面和来源库。精确总数不属于日志查看的正确性条件。
	c.JSON(http.StatusOK, gin.H{"ok": true, "rows": rows, "has_more": hasMore, "next_cursor": next})
}

// portalLogsExportPrepare 只做一次受统一闸门保护的 COUNT+MAX(id)，并签发短期
// 下载票据。前端随后交给浏览器原生下载，不再把最多 5 万行 CSV 堆进 JS Blob；
// 真正下载仍由 portalLogsExport 原子限流，预检不会占掉“成功下载”额度。
func (m *Monitor) portalLogsExportPrepare(c *gin.Context) {
	gid, ids, memberUID, fromTs, toTs, logType, model, group, tokenName, detailKw, requestID, err := m.portalLogParams(c)
	if err != nil {
		portalLogParamsError(c, err)
		return
	}
	if err := validatePortalLogFuzzyRange(fromTs, toTs, tokenName, detailKw, true); err != nil {
		portalLogParamsError(c, err)
		return
	}
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"error": "本组暂无成员日志"})
		return
	}
	now := time.Now().Unix()
	if !m.exportLim.allowPrepare(gid, now, int64(portalExportWindow.Seconds()), int64(portalExportPrepWindow.Seconds())) {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "导出准备过于频繁,请稍后再试"})
		return
	}
	total, startCursor, err := m.countGroupLogsSnapshot(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	claim := portalExportClaim{
		GID:         gid,
		MemberFP:    portalMemberFingerprintFromIDs(ids),
		MemberUID:   memberUID,
		FromTs:      fromTs,
		ToTs:        toTs,
		LogType:     logType,
		Model:       model,
		Group:       group,
		TokenName:   tokenName,
		DetailKw:    detailKw,
		RequestID:   requestID,
		Total:       total,
		StartCursor: startCursor,
		ExpiresAt:   now + int64(portalExportTicketTTL.Seconds()),
	}
	ticket, err := m.signPortalExportClaim(claim)
	if err != nil {
		slog.Warn("客户端 CSV 下载票据签发失败", "gid", gid, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "准备下载失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":           true,
		"ticket":       ticket,
		"total":        total,
		"total_exact":  total <= portalExportCap,
		"need_confirm": total > portalExportCap,
		"cap":          portalExportCap,
	})
}

// csvSafe 防 CSV 公式注入:文本以 = + - @ 制表符或回车开头时前置单引号,Excel/WPS 打开时按文本处理。
// 令牌名/详情等值最终来自用户可控输入(如用户把令牌起名 =HYPERLINK(...)),导出前必须消毒。
func csvSafe(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + s
	}
	return s
}

// portalLogsExport GET /api/logs/export:CSV 导出。新前端携带 prepare 签发的票据，
// 可跳过重复 COUNT 并按预检时的 MaxID 快照分页；旧前端不带票据时仍保留原协议，
// 但也在下载请求内取得 MaxID 快照。两条路径都执行原子限流、5 万行上限、
// 组成员指纹校验和只读流式分页。
func (m *Monitor) portalLogsExport(c *gin.Context) {
	m.portalLogsExportWithLimit(c, portalExportMaxBytes)
}

// portalLogsExportWithLimit 让文件上限成为可单测的边界；生产路由始终传
// portalExportMaxBytes，不接受客户参数覆盖。
func (m *Monitor) portalLogsExportWithLimit(c *gin.Context, maxBytes int64) {
	m.portalLogsExportWithWriter(c, maxBytes, nil)
}

// portalLogsExportWithWriter 的 writerWrapper 只用于确定性注入落盘中途失败。
// 生产调用固定为 nil，不存在配置或客户输入通道。
func (m *Monitor) portalLogsExportWithWriter(c *gin.Context, maxBytes int64, writerWrapper func(io.Writer) io.Writer) {
	var (
		gid, memberUID, fromTs, toTs, startCursor int64
		logType                                   int
		model, group, tokenName, detailKw         string
		requestID                                 string
		ids                                       []int64
		total                                     int64
		err                                       error
	)
	ticket := strings.TrimSpace(c.Query("ticket"))
	if ticket != "" {
		claim, ok := m.verifyPortalExportClaim(ticket, time.Now().Unix())
		gid = c.GetInt64("portalGID")
		if !ok || claim.GID != gid {
			c.JSON(http.StatusBadRequest, gin.H{"error": "下载凭证无效或已过期,请重新导出"})
			return
		}
		tracked, storeErr := m.portalTrackedMembersForUsageRead(c.Request.Context(), gid)
		if storeErr != nil {
			portalLogParamsError(c, storeErr)
			return
		}
		ids = idsOf(tracked)
		if portalMemberFingerprintFromIDs(ids) != claim.MemberFP {
			c.JSON(http.StatusConflict, gin.H{"error": "成员范围已变化,请重新导出"})
			return
		}
		memberUID, fromTs, toTs, logType = claim.MemberUID, claim.FromTs, claim.ToTs, claim.LogType
		model, group, tokenName, detailKw, requestID = claim.Model, claim.Group, claim.TokenName, claim.DetailKw, claim.RequestID
		total, startCursor = claim.Total, claim.StartCursor
	} else {
		gid, ids, memberUID, fromTs, toTs, logType, model, group, tokenName, detailKw, requestID, err = m.portalLogParams(c)
		if err != nil {
			portalLogParamsError(c, err)
			return
		}
	}
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"error": "本组暂无成员日志"})
		return
	}
	authorizationBefore, authErr := m.loadPortalUsageAuthorizationSnapshot(c.Request.Context(), gid)
	if authErr != nil {
		if errors.Is(authErr, errPortalAuthorizationInvalid) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "成员权限状态暂不可用，请稍后重试"})
		}
		return
	}
	if authorizationBefore.AuthVersion != c.GetInt64("portalAuthVer") ||
		authorizationBefore.ServingFingerprint != portalMemberFingerprintFromIDs(ids) {
		c.JSON(http.StatusConflict, gin.H{"error": "成员范围已变化，请重新导出"})
		return
	}
	if err := validatePortalLogFuzzyRange(fromTs, toTs, tokenName, detailKw, true); err != nil {
		portalLogParamsError(c, err)
		return
	}
	// 限流前置 + 原子预占:窗口内已有占用直接拒,重查询根本不执行;并发请求只有一个能占到位。
	// 探测(need_confirm)/查询失败走 rollback 不计次,仅成功下载保留占位(defer 统一处理)。
	now := time.Now().Unix()
	prev, ok := m.exportLim.reserve(gid, now, int64(portalExportWindow.Seconds()))
	if !ok {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "导出过于频繁,请 5 分钟后再试"})
		return
	}
	downloaded := false
	defer func() {
		if !downloaded {
			m.exportLim.rollback(gid, prev, now)
		}
	}()
	if ticket == "" {
		// 兼容旧页面：无预检票据时在下载请求内一次取得 COUNT+MaxID；新页面则复用
		// prepare 票据。旧路径也必须固定快照，否则计数后新到的日志会混入文件。
		total, startCursor, err = m.countGroupLogsSnapshot(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID)
		if err != nil {
			if abortCanceledUsageRequest(c, err) {
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
			return
		}
	}
	confirm := c.Query("confirm") == "1"
	if total > portalExportCap && !confirm { // 超上限、未确认:让前端弹确认框(不消耗限流、不拉行)
		c.JSON(http.StatusOK, gin.H{"need_confirm": true, "cap": portalExportCap})
		return
	}
	fname := fmt.Sprintf("usage-logs-%s_%s.csv",
		time.Unix(fromTs, 0).In(usageCST).Format("20060102"),
		time.Unix(toTs-1, 0).In(usageCST).Format("20060102"))
	// 先流式落到 0600 临时文件，再复核当前成员 revision/Portal AuthVer。
	// 权限变化时整份丢弃，一个字节都不发给旧组；内存仍只保留一页。
	tmp, storageLease, err := m.createPortalExportTempFile(maxBytes)
	if err != nil {
		portalExportStorageError(c, err)
		return
	}
	defer storageLease.release()
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	var exportWriter io.Writer = tmp
	if writerWrapper != nil {
		exportWriter = writerWrapper(exportWriter)
	}
	limited := &portalExportLimitedWriter{dst: exportWriter, max: maxBytes}
	if _, err := limited.Write([]byte("\xEF\xBB\xBF")); err != nil { // Excel UTF-8 BOM
		portalExportStorageError(c, err)
		return
	}
	w := csv.NewWriter(limited)
	if err := w.Write([]string{"时间", "成员", "令牌", "分组", "类型", "模型", "用时(秒)", "输入tokens", "输出tokens", "费用(美元)", "详情", "Request ID"}); err != nil {
		slog.Warn("客户端 CSV 表头写入失败", "gid", gid, "err", err)
		portalExportStorageError(c, err)
		return
	}
	w.Flush()
	if err := w.Error(); err != nil {
		slog.Warn("客户端 CSV 表头刷新失败", "gid", gid, "err", err)
		portalExportStorageError(c, err)
		return
	}

	beforeID := startCursor
	remaining := portalExportCap
	for remaining > 0 {
		limit := portalExportPageSize
		if remaining < limit {
			limit = remaining
		}
		rows, qerr := m.queryGroupLogsForExport(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID, beforeID, limit)
		if qerr != nil {
			if isCanceledUsageRequest(c, qerr) {
				return
			}
			// 临时文件阶段尚未开始 HTTP 响应，查询失败时整份丢弃，
			// 不会把看似正常的 CSV 前缀交给客户。
			slog.Warn("客户端 CSV 分页查询失败", "gid", gid, "err", qerr)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := w.Write(portalCSVRecord(row)); err != nil {
				slog.Warn("客户端 CSV 写入失败", "gid", gid, "err", err)
				portalExportStorageError(c, err)
				return
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			slog.Warn("客户端 CSV 刷新失败", "gid", gid, "err", err)
			portalExportStorageError(c, err)
			return
		}
		remaining -= len(rows)
		beforeID = rows[len(rows)-1].ID
		if len(rows) < limit {
			break
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		portalExportStorageError(c, err)
		return
	}
	authorizationAfter, authErr := m.loadPortalUsageAuthorizationSnapshot(c.Request.Context(), gid)
	if authErr != nil {
		if errors.Is(authErr, errPortalAuthorizationInvalid) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "成员权限状态暂不可用，请稍后重试"})
		}
		return
	}
	if !authorizationBefore.equal(authorizationAfter) || authorizationAfter.AuthVersion != c.GetInt64("portalAuthVer") {
		c.JSON(http.StatusConflict, gin.H{"error": "成员范围已变化，请重新导出"})
		return
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "准备下载失败,请稍后重试"})
		return
	}
	c.Header("Content-Disposition", "attachment; filename=\""+fname+"\"")
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Status(http.StatusOK)
	if _, err := io.Copy(c.Writer, tmp); err != nil {
		slog.Warn("客户端 CSV 发送失败", "gid", gid, "err", err)
		return
	}
	downloaded = true // 完整发送后才计入成功导出限额，断线允许重试。
}

// portalCSVRecord 把允许给客户导出的字段转成一行。详情保留原始内容；csvSafe
// 防 Excel/WPS 把用户可控文本当作公式执行。
func portalCSVRecord(r LogRow) []string {
	cost := ""
	if r.Type == 2 {
		cost = strconv.FormatFloat(r.CostUSD, 'f', 6, 64)
	}
	return []string{
		time.Unix(r.CreatedAt, 0).In(usageCST).Format("2006-01-02 15:04:05"),
		csvSafe(r.Member), csvSafe(r.TokenName), csvSafe(r.Group), logTypeName(r.Type), csvSafe(r.ModelName),
		strconv.FormatInt(r.UseTime, 10), strconv.FormatInt(r.PromptTokens, 10), strconv.FormatInt(r.CompletionTokens, 10),
		cost, csvSafe(r.Detail), csvSafe(r.RequestID),
	}
}

// invalidatePortalAggregates 管理端手动刷新矩阵后，精确删除同范围内各客户组的
// matrix/stats 两个独立聚合键。删除失败不影响响应；cache.go 会阻止失效旧值重新回流。
func (m *Monitor) invalidatePortalAggregates(tracked []TrackedUser, fromTs, toTs int64) {
	if m.usageCache == nil {
		return
	}
	byGroup := make(map[int64][]TrackedUser)
	for _, u := range tracked {
		if u.GroupID > 0 {
			byGroup[u.GroupID] = append(byGroup[u.GroupID], u)
		}
	}
	keys := make([]string, 0, len(byGroup)*2)
	for gid, members := range byGroup {
		memberFP := portalMemberFingerprint(members)
		keys = append(keys,
			m.usageFactCacheKey(portalGroupAggregateKey("matrix", gid, memberFP, fromTs, toTs)),
			m.usageFactCacheKey(portalGroupAggregateKey("stats", gid, memberFP, fromTs, toTs)),
		)
	}
	m.usageCache.Delete(context.Background(), keys...)
}

// invalidatePortalUserAggregates 在管理端主动刷新某用户/令牌后，精确删除该客户组的
// 对应下钻聚合。指纹必须使用整个客户组的当前成员，不能只用被查的单个用户。
func (m *Monitor) invalidatePortalUserAggregates(tracked []TrackedUser, uid, tokenID, fromTs, toTs int64) {
	if m.usageCache == nil || uid <= 0 {
		return
	}
	var gid int64
	for _, u := range tracked {
		if u.UserID == uid {
			gid = u.GroupID
			break
		}
	}
	if gid <= 0 {
		return // 未分组用户没有 Usage Portal 权限域。
	}
	members := make([]TrackedUser, 0)
	for _, u := range tracked {
		if u.GroupID == gid {
			members = append(members, u)
		}
	}
	memberFP := portalMemberFingerprint(members)
	keys := []string{m.usageFactCacheKey(portalUserAggregateKey("stats", gid, memberFP, uid, tokenID, fromTs, toTs))}
	if tokenID == 0 {
		keys = append(keys, m.usageFactCacheKey(portalUserAggregateKey("tokens", gid, memberFP, uid, 0, fromTs, toTs)))
	}
	m.usageCache.Delete(context.Background(), keys...)
}
