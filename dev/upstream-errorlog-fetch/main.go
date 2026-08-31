// upstream-errorlog-fetch 独立拉取上游错误日志并存成本地文件。
//
// ★ 为什么要有这个独立工具 ★
//
// monitor 里的采集层（monitor/channel_upstream_errorlog.go）已经写好，但它要
// 部署后才跑，而部署要先提交、bump plan ID、重建容器。为了在那之前就能
// 拿到真实上游数据核对字段名与串联键，需要一个不依赖部署、不依赖凭据库的工具。
//
// 它与 monitor 采集层的关系：**同一个端点、同一套参数、同一个 decoder 语义**，
// 但凭据从命令行给、结果存文件而非入库。所以它拉到的东西可以直接用来验证
// 采集层的解析是否正确。
//
// ★ 安全边界 ★
//   - 只发 GET，只读
//   - token 只从环境变量读，绝不打印、绝不写进输出文件
//   - 输出文件里是上游日志原文，可能含客户请求片段 → 存在仓库外（默认 ./out，已 gitignore）
//   - 默认 dry-run 打印计划，加 -run 才真发请求
//
// 用法：
//
//	export UPSTREAM_TOKEN='<访问令牌>'
//	go run ./dev/upstream-errorlog-fetch \
//	  -base https://4sapi.com -user 123 -days 1 -run
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// logTypeError 与我方 logs 表同一口径：2=消费 5=错误。本工具只拉 5。
	// ★ 这就是「普通消费日志」与「上游错误日志」的唯一区分点 ★
	logTypeError = 5

	// pageSize 与 monitor 采集层保持一致，便于对照分页行为。
	pageSize = 100

	// maxPages 单次运行的页数上限。防止误设时间窗把上游翻爆。
	maxPages = 50

	// requestInterval 相邻请求间隔。上游按账号限流，不能连打。
	requestInterval = 300 * time.Millisecond

	// httpTimeout 单请求超时。
	httpTimeout = 20 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "失败:", err)
		os.Exit(1)
	}
}

type config struct {
	baseURL string
	userID  int64
	days    int
	outDir  string
	doRun   bool
	logType int
}

func parseFlags() (config, error) {
	var c config
	flag.StringVar(&c.baseURL, "base", "", "上游站点根地址，如 https://4sapi.com")
	flag.Int64Var(&c.userID, "user", 0, "该上游账户的 user id（New-Api-User 头）")
	flag.IntVar(&c.days, "days", 1, "回看天数（按自然日边界，CST）")
	flag.StringVar(&c.outDir, "out", "out", "输出目录（存上游日志原文，勿入库勿提交）")
	flag.BoolVar(&c.doRun, "run", false, "真发请求；缺省只打印计划")
	flag.IntVar(&c.logType, "type", logTypeError, "日志类型：5=错误(默认) 2=消费")
	flag.Parse()

	if strings.TrimSpace(c.baseURL) == "" {
		return c, errors.New("缺 -base")
	}
	if !strings.HasPrefix(c.baseURL, "https://") && !strings.HasPrefix(c.baseURL, "http://") {
		return c, errors.New("-base 必须带 http:// 或 https://")
	}
	// -user 允许为 0：那表示**不发 New-Api-User 头**。
	//
	// monitor 采集层总是发这个头（它从账户配置里拿得到 user id），但本工具是
	// 手工核对用的，未必知道 user id。有些 new-api 版本能从 token 反查用户，
	// 所以先试不带头——通了就省一次去后台翻配置；401 再补。
	// 之所以不默认必填：让人为了一次探测去翻生产配置，会把「先看一眼」的
	// 成本抬到没人愿意做，那才是真正的阻碍。
	if c.days < 1 || c.days > 31 {
		return c, errors.New("-days 取值应在 1~31（与我方排障跨度上限一致）")
	}
	return c, nil
}

// cstWindow 按 CST 自然日算 [from, to)。与 monitor 的 parseLogChainScope 同口径：
// to 取「明天 00:00」，from 往前推 days 天。两侧口径一致，拉回来的窗口才能对照。
func cstWindow(days int, now time.Time) (from, to int64) {
	cst := time.FixedZone("CST", 8*3600)
	n := now.In(cst)
	midnightTomorrow := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, cst).AddDate(0, 0, 1)
	to = midnightTomorrow.Unix()
	from = to - int64(days)*86400
	return from, to
}

func run() error {
	cfg, err := parseFlags()
	if err != nil {
		flag.Usage()
		return err
	}
	token := strings.TrimSpace(os.Getenv("UPSTREAM_TOKEN"))
	from, to := cstWindow(cfg.days, time.Now())
	cst := time.FixedZone("CST", 8*3600)

	fmt.Printf("上游      : %s\n", cfg.baseURL)
	fmt.Printf("端点      : /api/log/self  (HTTP GET，非消息队列)\n")
	fmt.Printf("区分点    : type=%d  %s\n", cfg.logType, typeLabel(cfg.logType))
	fmt.Printf("窗口      : %s ~ %s (CST，左闭右开)\n",
		time.Unix(from, 0).In(cst).Format("2006-01-02 15:04:05"),
		time.Unix(to, 0).In(cst).Format("2006-01-02 15:04:05"))
	if cfg.userID > 0 {
		fmt.Printf("认证      : Authorization: Bearer <token>  +  New-Api-User: %d\n", cfg.userID)
	} else {
		fmt.Println("认证      : Authorization: Bearer <token>  （未给 -user，不发 New-Api-User 头）")
	}
	fmt.Printf("token     : %s\n", tokenStatus(token))

	if !cfg.doRun {
		fmt.Println("\n未加 -run，只打印计划，未发任何请求。")
		return nil
	}
	if token == "" {
		return errors.New("UPSTREAM_TOKEN 为空。请 export UPSTREAM_TOKEN='<访问令牌>' 后重试")
	}

	if err := os.MkdirAll(cfg.outDir, 0o700); err != nil {
		return fmt.Errorf("建输出目录失败: %w", err)
	}
	stamp := time.Now().In(cst).Format("20060102-150405")
	host := hostOf(cfg.baseURL)
	rawPath := filepath.Join(cfg.outDir,
		fmt.Sprintf("%s-type%d-%s.jsonl", host, cfg.logType, stamp))

	fmt.Printf("\n输出      : %s\n\n", rawPath)
	items, err := fetchAll(cfg, token, from, to, rawPath)
	if err != nil {
		return err
	}
	report(items, cfg, rawPath)
	return nil
}

func typeLabel(t int) string {
	switch t {
	case 2:
		return "（消费日志：渠道管理的用量同步用的就是这个）"
	case 5:
		return "（错误日志：本工具的目标）"
	}
	return "（非标准取值）"
}

// tokenStatus 只报长度与前后各 2 字符，绝不打印完整 token。
func tokenStatus(t string) string {
	if t == "" {
		return "未设置（UPSTREAM_TOKEN 为空）"
	}
	if len(t) <= 6 {
		return fmt.Sprintf("已设置，长度 %d（过短，疑似不完整）", len(t))
	}
	return fmt.Sprintf("已设置，长度 %d，形如 %s…%s", len(t), t[:2], t[len(t)-2:])
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "upstream"
	}
	return strings.ReplaceAll(u.Host, ":", "_")
}

// fetchAll 翻页拉完窗口，边拉边把每条原文按 JSONL 追加落盘。
//
// 边拉边写而不是全拉完再写：中途失败时已拉到的部分不丢——上游日志有保留期，
// 重拉可能已经拉不到了。
func fetchAll(cfg config, token string, from, to int64, rawPath string) ([]json.RawMessage, error) {
	f, err := os.OpenFile(rawPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("建输出文件失败: %w", err)
	}
	defer f.Close()

	client := &http.Client{Timeout: httpTimeout}
	var all []json.RawMessage
	seen := map[string]bool{}

	for page := 1; page <= maxPages; page++ {
		if page > 1 {
			time.Sleep(requestInterval)
		}
		items, total, err := fetchPage(client, cfg, token, from, to, page)
		if err != nil {
			// 已拉到的部分保留：报错但不删文件。
			return all, fmt.Errorf("第 %d 页失败（已落盘 %d 条）: %w", page, len(all), err)
		}
		fmt.Printf("第 %2d 页: %3d 条  (上游报告 total=%d)\n", page, len(items), total)
		if len(items) == 0 {
			break
		}
		for _, it := range items {
			// 用 id 去重：翻页期间上游有新日志写入时，同一条可能出现在两页。
			key := idOf(it)
			if key != "" && seen[key] {
				continue
			}
			seen[key] = true
			if _, err := f.Write(append([]byte(compact(it)), '\n')); err != nil {
				return all, fmt.Errorf("写文件失败: %w", err)
			}
			all = append(all, it)
		}
		if int64(len(seen)) >= total {
			break
		}
	}
	return all, nil
}

func fetchPage(client *http.Client, cfg config, token string, from, to int64, page int) ([]json.RawMessage, int64, error) {
	q := url.Values{}
	q.Set("p", strconv.Itoa(page))
	q.Set("page_size", strconv.Itoa(pageSize))
	q.Set("type", strconv.Itoa(cfg.logType))
	q.Set("start_timestamp", strconv.FormatInt(from, 10))
	// end_timestamp 是**闭区间**，故减 1 才等于我方的半开区间 [from,to)。
	// 与 monitor 采集层完全一致；差这 1 秒会让边界那一秒重复或漏掉。
	q.Set("end_timestamp", strconv.FormatInt(to-1, 10))

	endpoint := strings.TrimRight(cfg.baseURL, "/") + "/api/log/self?" + q.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	// userID 为 0 表示不发该头（见 parseFlags 的说明）。
	if cfg.userID > 0 {
		req.Header.Set("New-Api-User", strconv.FormatInt(cfg.userID, 10))
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		// 不打印 body 全文：可能含敏感信息。只给状态码与前 200 字符。
		return nil, 0, fmt.Errorf("HTTP %d: %.200s", resp.StatusCode, string(body))
	}

	// NewAPI 的信封：{"success":bool,"message":string,"data":{"items":[],"total":n}}
	// 也见过 data 直接是数组的旧形态，故两种都试。
	var env struct {
		Success bool            `json:"success"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, 0, fmt.Errorf("响应不是 JSON: %.200s", string(body))
	}
	if !env.Success && env.Message != "" {
		return nil, 0, fmt.Errorf("上游返回失败: %s", env.Message)
	}
	var page1 struct {
		Items []json.RawMessage `json:"items"`
		Total int64             `json:"total"`
	}
	if err := json.Unmarshal(env.Data, &page1); err == nil && page1.Items != nil {
		return page1.Items, page1.Total, nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(env.Data, &arr); err == nil {
		return arr, int64(len(arr)), nil
	}
	return nil, 0, errors.New("data 既不是 {items,total} 也不是数组")
}

func idOf(item json.RawMessage) string {
	var m struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(item, &m); err != nil {
		return ""
	}
	return strings.TrimSpace(string(m.ID))
}

// report 对拉回来的数据做校验并打印结论。
//
// ★ 这就是「验证方案」的自动化部分 ★
// 它回答三个问题，每个都对应 monitor 侧一处可能出错的地方：
//  1. 字段名对不对 → monitor/channel_upstream_errorlog.go 的 decodeUpstreamErrorLogItem
//  2. other 嵌套解得开吗 → 同文件的 applyUpstreamErrorOther（含转义字符串形态）
//  3. 串联键能抠出来吗 → 尚未实现的关联层（见 docs 第 19 节）
//
// 打印时**不输出字段值**，只输出字段名与命中率——错误原文可能含客户请求片段。
func report(items []json.RawMessage, cfg config, rawPath string) {
	fmt.Printf("\n════ 共 %d 条，已存 %s ════\n", len(items), rawPath)
	if len(items) == 0 {
		fmt.Println("窗口内没有错误日志。换更长的 -days，或确认该上游当期确实无错误。")
		return
	}

	topKeys := map[string]int{}
	otherKeys := map[string]int{}
	otherForm := map[string]int{}
	var withJoinKey, withOtherChannel int

	for _, it := range items {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(it, &m); err != nil {
			continue
		}
		for k := range m {
			topKeys[k]++
		}
		// other 的两种形态都试：对象 / 转义 JSON 字符串。
		if raw, ok := m["other"]; ok {
			form, inner := parseOther(raw)
			otherForm[form]++
			for k := range inner {
				otherKeys[k]++
			}
			if s := unquote(inner["channel_name"]); s != "" {
				withOtherChannel++
			}
		}
		if joinKeyFrom(unquote(m["content"])) != "" {
			withJoinKey++
		}
	}

	fmt.Println("\n── 顶层字段（名 × 出现条数）──")
	printKeys(topKeys, len(items))

	fmt.Println("\n── other 内字段 ──")
	if len(otherKeys) == 0 {
		fmt.Println("  （没有 other，或全都解不开——若是后者，monitor 侧那五个字段会全空）")
	} else {
		printKeys(otherKeys, len(items))
		fmt.Print("  other 形态: ")
		for form, n := range otherForm {
			fmt.Printf("%s=%d ", form, n)
		}
		fmt.Println()
	}

	fmt.Println("\n── 关键字段校验（对应 monitor 侧的解析）──")
	check("content（错误原文，表的存在理由）", topKeys["content"], len(items))
	check("id（主键，错了会互相覆盖）", topKeys["id"], len(items))
	check("created_at（时间轴锚点）", topKeys["created_at"], len(items))
	check("use_time（区分超时/立即拒绝）", topKeys["use_time"], len(items))
	check("other.channel_name（上游用了它自己哪个渠道）", withOtherChannel, len(items))
	check("other.status_code", otherKeys["status_code"], len(items))
	check("other.error_code（归因层用它判掉待判行）", otherKeys["error_code"], len(items))
	check("other.error_type", otherKeys["error_type"], len(items))

	fmt.Println("\n── 上下游串联键（关联层尚未实现，此处只测可行性）──")
	check("content 里可抠出串联键（即最深层模型商的 id）", withJoinKey, len(items))
	fmt.Printf("  ★ 串联键是「双方 content 里嵌的同一个 id」，不是上游的 request_id 字段 ★\n" +
		"  2026-08-28 实测（kpzhu.com，我方渠道 #66，跨 4 天）：\n" +
		"    上游 request_id 字段 vs 我方嵌的 id →   1 / 486 命中（≈0，原假设错）\n" +
		"    上游嵌的 id         vs 我方嵌的 id → 152 命中（我方带 key 的行几乎全中）\n" +
		"  原因是错误体逐层透传：真正的模型商生成 id P，kpzhu 记自己的 request_id K，\n" +
		"  但它的 content 里带 P；我方收到后记自己的 O，content 里还是 P。\n" +
		"  两侧能对上的是 P ↔ P；用 K 去对必然落空——K 是上游自己的流水号，我方看不到。\n")
}

func printKeys(counts map[string]int, total int) {
	names := make([]string, 0, len(counts))
	for k := range counts {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		n := counts[k]
		mark := ""
		if n < total {
			mark = fmt.Sprintf("  ← 只有 %d/%d 条有", n, total)
		}
		fmt.Printf("  %-28s %4d%s\n", k, n, mark)
	}
}

func check(label string, got, total int) {
	pct := 0
	if total > 0 {
		pct = got * 100 / total
	}
	status := "✓"
	switch {
	case got == 0:
		status = "★ 一条都没有"
	case pct < 50:
		status = "△ 覆盖偏低"
	}
	fmt.Printf("  %-46s %4d/%d (%3d%%) %s\n", label, got, total, pct, status)
}

// parseOther 返回形态标识与解开后的键值。两种形态都认——
// 生产库里是对象，而契约 fixture 里是转义 JSON 字符串，只认一种会静默丢字段。
func parseOther(raw json.RawMessage) (string, map[string]json.RawMessage) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		return "对象", obj
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		if err := json.Unmarshal([]byte(s), &obj); err == nil {
			return "转义字符串", obj
		}
	}
	return "解不开", nil
}

func unquote(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

// joinKeyFrom 从错误原文里抠上游 request id。
//
// 两种形态都要认（2026-08-28 生产实测都存在）：
//
//	(request id: 202608280208089288070118268d9d6WhLijC4B)   ← new-api 系上游
//	(request_id: req_1787652519221_1a03fa99)                ← 另一种上游
//
// 只认前者会漏掉后者那批，而那批恰恰是 bad_response_status_code 的主体。
var joinKeyRe = regexp.MustCompile(`\(request[ _]id:\s*([0-9A-Za-z_-]+)\)`)

func joinKeyFrom(content string) string {
	m := joinKeyRe.FindStringSubmatch(content)
	if m == nil {
		return ""
	}
	return m[1]
}

// compact 把一条 item 压成单行，JSONL 每行一条才好逐行处理。
func compact(item json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, item); err != nil {
		// 压不了就原样写：宁可格式差一点，也不能丢数据。
		return strings.ReplaceAll(string(item), "\n", " ")
	}
	return buf.String()
}
