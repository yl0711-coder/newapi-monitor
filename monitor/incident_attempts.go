package monitor

// 尝试次数口径。
//
// 复用 logchain.js:887-899 已有的归并与去重口径（按 Request ID 归组，
// 组内以 (channel_id, 秒级时间) 去重），但必须把它的局限一起带出来：
//
//  1. 这是「当前可见尝试数」，不是完整、精确的实际网络尝试数。
//  2. logchain.js:889 注明「取不到渠道时退化为按记录计数」——该退化必须显式标记，
//     不能因为撤掉了早先误设计的 attempt_count_is_rows 字段就把它隐藏掉。
//  3. 无 Request ID 的行不可关联（logchain.js:877 的 unlinkable），单独计并标记。
//
// 它也不能用来证明客户端重试循环：组内多次渠道尝试可能是 NewAPI 内部重试，
// 而 Nginx 的 upstream_attempts 属于 Nginx→NewAPI 那一跳。两者都不是
// 01.3 要求的「客户端直接证实」。

// IncidentAttemptRow 是参与尝试次数统计的一行日志的最小必要字段。
type IncidentAttemptRow struct {
	// ChannelID 为 0 或负数表示取不到渠道（退化为按记录计数的来源）。
	ChannelID int64
	// CreatedAt 秒级时间，与 ChannelID 组成去重键。
	CreatedAt int64
	// RequestID 为空表示该行无法按请求归并。
	RequestID string
}

// IncidentAttemptCount 是尝试次数及其完整性说明。
//
// 三个计数分列，不互相代替（用户要求：日志条数、尝试次数、独立请求数不能混用）。
type IncidentAttemptCount struct {
	// VisibleAttempts 当前可见尝试数。按渠道去重 + 无渠道行按记录计数。
	VisibleAttempts int64

	// LogRows 日志条数。
	LogRows int64

	// DistinctRequests 独立请求数（按 Request ID 去重，不含无 ID 的行）。
	DistinctRequests int64

	// RowsWithoutChannel 取不到渠道、退化为按记录计数的行数。
	// 大于 0 时 VisibleAttempts 的这部分不是真正的「尝试」去重结果。
	RowsWithoutChannel int64

	// UnlinkableRows 无 Request ID、无法按请求归并的行数。
	UnlinkableRows int64

	// Exact 为真表示本次统计没有任何退化或不可关联的行。
	// 即便 Exact 为真，它仍只是「当前可见」——见下方 CompletenessNote。
	Exact bool
}

// ComputeIncidentAttempts 按既有口径统计，并把退化情况一并带出。
//
// 去重**必须先按 Request ID 归组、再做组内去重**（logchain.js:869-884 先 groupByRequest
// 再对每组调 requestAttempts）。只用「渠道 + 秒级时间」会把两个不同请求在同一秒
// 命中同一渠道算成一次尝试——那是跨请求错误合并。
//
// 无 Request ID 的记录不可关联（logchain.js:877 每行单独成组），
// 因此**不得互相合并**，也不得与有 ID 的行合并：按记录各计一次。
func ComputeIncidentAttempts(rows []IncidentAttemptRow) IncidentAttemptCount {
	out := IncidentAttemptCount{LogRows: int64(len(rows))}

	// 先按 Request ID 归组；组内再以 (channel_id, 秒级时间) 去重。
	// 去重键带上 RequestID，保证不同请求的同渠道同秒不被合并。
	seenChannelAttempt := map[string]struct{}{}
	seenRequest := map[string]struct{}{}

	// perRecord 统计无法参与去重、只能按记录计数的行：
	// 缺渠道（判不出是否同一次尝试）或缺 Request ID（不可关联，不得合并）。
	var perRecord int64

	for _, r := range rows {
		linkable := r.RequestID != ""
		hasChannel := r.ChannelID > 0

		if !linkable {
			out.UnlinkableRows++
		} else {
			seenRequest[r.RequestID] = struct{}{}
		}
		if !hasChannel {
			// 退化为按记录计数（logchain.js:889、:896 的 extra++）。
			out.RowsWithoutChannel++
		}

		if linkable && hasChannel {
			// 组内去重键：请求 + 渠道 + 秒级时间。
			seenChannelAttempt[formatAttemptKey(r.RequestID, r.ChannelID, r.CreatedAt)] = struct{}{}
			continue
		}
		perRecord++
	}

	out.VisibleAttempts = int64(len(seenChannelAttempt)) + perRecord
	out.DistinctRequests = int64(len(seenRequest))
	out.Exact = out.RowsWithoutChannel == 0 && out.UnlinkableRows == 0
	return out
}

// formatAttemptKey 构造组内去重键。
//
// 带 RequestID 是关键：没有它，不同请求在同一秒命中同一渠道会被合并成一次。
func formatAttemptKey(requestID string, channelID, createdAt int64) string {
	return requestID + incidentKeySep + itoa64(channelID) + incidentKeySep + itoa64(createdAt)
}

// itoa64 避免为一个小转换引入 strconv 以外的依赖；与 incident_dedup.go 保持一致风格。
func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	var buf [20]byte
	i := len(buf)
	u := v
	if neg {
		u = -v
	}
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// CompletenessNote 返回必须随尝试次数一起展示的完整性说明。
//
// 即使 Exact 为真也会返回说明——因为「可见」与「实际」的差距始终存在：
// 客户端到 Nginx 之前的尝试 Monitor 永远看不到。
func (c IncidentAttemptCount) CompletenessNote() string {
	base := "当前可见尝试数（按渠道与秒级时间去重）；不含 Monitor 观察点之前的客户端尝试，不等于实际网络尝试数"
	if c.RowsWithoutChannel > 0 {
		base += "；其中 " + itoa64(c.RowsWithoutChannel) + " 行取不到渠道，已退化为按记录计数"
	}
	if c.UnlinkableRows > 0 {
		base += "；另有 " + itoa64(c.UnlinkableRows) + " 行无 Request ID，无法按请求归并"
	}
	return base
}

// ProvesClientRetryLoop 恒为 false。
//
// 保留这个函数是为了让「尝试次数不能证明客户端重试循环」这件事在代码里有一个
// 可被测试引用的落点，而不是只写在注释里。
//
// 组内多次渠道尝试可能是 NewAPI 内部重试；Nginx 的 upstream_attempts 属于
// Nginx→NewAPI 这一跳。01.3 第 76 行要求 client_retry_loop 是「客户端直接证实的
// 重试循环」，上述两者都不满足，补一个「重试几次」的阈值也不能解决。
func (c IncidentAttemptCount) ProvesClientRetryLoop() bool {
	return false
}
