package monitor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const modelFRTMaxLeaves = 10000

var errModelFRTLimit = errors.New("FRT leaf limit exceeded")

type ModelFRTFilter struct {
	ChannelID *int    `json:"channel_id"`
	Group     *string `json:"group"`
	Model     *string `json:"model"`
	// MeanGtMs 保留毫秒整数，避免把用户输入的秒数反复做浮点比较。
	// 它只过滤渠道层，不下推到 SQL：均值要等三级树合并完 sum/observed 才成立，
	// 在 SQL 里按渠道 HAVING 会算出与页面显示不同的值。
	MeanGtMs *int64 `json:"mean_gt_ms"`
}

type modelFRTInputError struct{ message string }

func (e *modelFRTInputError) Error() string { return e.message }

// ModelFRTMetrics uses null for unknown measurements; zero is only a proven
// zero sample count, never a latency. Coverage and sample availability differ.
type ModelFRTMetrics struct {
	MeanMs                *float64 `json:"mean_ms"`
	MeanCoverageComplete  bool     `json:"mean_coverage_complete"`
	MeanUnavailableReason string   `json:"mean_unavailable_reason"`
	Over3s                *int64   `json:"over_3s"`
	Over3sPct             *float64 `json:"over_3s_pct"`
	Observed              *int64   `json:"observed"`
	P50Ms                 *float64 `json:"p50_ms"`
	P95Ms                 *float64 `json:"p95_ms"`
	P99Ms                 *float64 `json:"p99_ms"`
	MaxMs                 *int64   `json:"max_ms"`
	CoverageComplete      bool     `json:"coverage_complete"`
	UnavailableReason     string   `json:"unavailable_reason"`
}

type ModelFRTModel struct {
	Model string          `json:"model"`
	FRT   ModelFRTMetrics `json:"frt"`
}
type ModelFRTGroup struct {
	Group  string          `json:"group"`
	FRT    ModelFRTMetrics `json:"frt"`
	Models []ModelFRTModel `json:"models"`
}
type ModelFRTChannel struct {
	ChannelID int             `json:"channel_id"`
	Name      string          `json:"name"`
	FRT       ModelFRTMetrics `json:"frt"`
	Groups    []ModelFRTGroup `json:"groups"`
}
type ModelFRTCoverage struct {
	Status   string     `json:"status"`
	Complete bool       `json:"complete"`
	From     *time.Time `json:"from"`
	Through  *time.Time `json:"through"`
	Note     string     `json:"note"`
}
type ModelFRTReport struct {
	Range  ModelReportRange `json:"range"`
	Filter ModelFRTFilter   `json:"filter"`
	Window string           `json:"window"`
	From   time.Time        `json:"from"`
	To     time.Time        `json:"to"`
	// ToTs is the requested comparison right edge; ThroughTs and
	// ActualThroughTs are the newest certified local-fact minute.  Keeping both
	// makes fixed-range comparisons stable while exposing a real freshness
	// watermark to the page and to reconciliation tools.
	ToTs             int64             `json:"to_ts"`
	ThroughTs        int64             `json:"through_ts"`
	ActualThroughTs  int64             `json:"actual_through_ts"`
	Target           time.Time         `json:"target"`
	GeneratedAt      time.Time         `json:"generated_at"`
	Timing           ReportTiming      `json:"timing"`
	LagSeconds       int64             `json:"lag_seconds"`
	CollectionFailed bool              `json:"collection_failed"`
	Coverage         ModelFRTCoverage  `json:"coverage"`
	Channels         []ModelFRTChannel `json:"channels"`
	// 以下两项仅在使用平均 FRT 阈值筛选时有意义：不可计算的渠道既不能算命中
	// 也不能算未命中,必须单独报数,否则页面会把"筛掉了"读成"没有这种渠道"。
	MeanFilterExcluded int64 `json:"mean_filter_excluded"`
	MeanFilterApplied  bool  `json:"mean_filter_applied"`
}

type modelFRTRow struct {
	FRTSumMs            int64
	FRTSumObserved      int64
	MeanUnsupportedRows int64
	MeanInvalidRows     int64
	Row                 modelStatisticsRow `gorm:"embedded"`
	HistoricalRows      int64              `gorm:"column:historical_rows"`
	InvalidRows         int64              `gorm:"column:invalid_rows"`
}
type modelFRTAggregate struct {
	sumMs, sumObserved           int64
	meanUnsupported, meanInvalid bool
	hist                         modelStatisticsTTFTAggregate
	historical, invalid          bool
}

func (a *modelFRTAggregate) merge(b modelFRTAggregate) {
	if b.sumMs < 0 || b.sumObserved < 0 || a.sumMs > math.MaxInt64-b.sumMs || a.sumObserved > math.MaxInt64-b.sumObserved {
		a.meanInvalid = true
	} else {
		a.sumMs += b.sumMs
		a.sumObserved += b.sumObserved
	}
	a.meanUnsupported = a.meanUnsupported || b.meanUnsupported
	a.meanInvalid = a.meanInvalid || b.meanInvalid
	mergeModelStatisticsTTFTAggregate(&a.hist, b.hist)
	a.historical = a.historical || b.historical
	a.invalid = a.invalid || b.invalid
}
func (a modelFRTAggregate) view(complete bool) ModelFRTMetrics {
	v := ModelFRTMetrics{CoverageComplete: complete}
	switch {
	case a.historical:
		v.UnavailableReason = "historical_unsupported"
	case a.invalid || a.hist.invalid:
		v.UnavailableReason = "invalid_histogram"
	default:
		n, p50, p95, p99, maximum, over3s, over3sPct := modelStatisticsTTFTViewDetailed(a.hist)
		if n > 0 {
			v.Over3s, v.Over3sPct = &over3s, &over3sPct
			v.Observed, v.P50Ms, v.P95Ms, v.P99Ms, v.MaxMs = &n, &p50, &p95, &p99, &maximum
			switch {
			case a.meanUnsupported:
				v.MeanUnavailableReason = "historical_sum_unavailable"
			case a.meanInvalid || a.sumObserved != n || a.sumMs <= 0:
				v.MeanUnavailableReason = "invalid_sum"
			default:
				mean := float64(a.sumMs) / float64(n)
				v.MeanMs, v.MeanCoverageComplete = &mean, complete
			}
			return v
		}
		if a.hist.observed != 0 {
			v.UnavailableReason = "invalid_histogram"
		} else {
			v.Observed = &n
			v.UnavailableReason = "no_valid_samples"
			v.MeanUnavailableReason = v.UnavailableReason
			return v
		}
	}
	v.MeanUnavailableReason = v.UnavailableReason
	v.CoverageComplete = false
	return v
}

func modelFRTTime(ts int64) *time.Time {
	if ts <= 0 {
		return nil
	}
	t := time.Unix(ts, 0).UTC()
	return &t
}

// The optional end pins comparison with the old page. Never consult rejection
// facts/cursors to choose this report's independent request/FRT window.
func (m *Monitor) buildModelFRTReport(ctx context.Context, key, rawEnd string, now time.Time) (ModelFRTReport, error) {
	return m.buildModelFRTReportFiltered(ctx, key, rawEnd, ModelFRTFilter{}, now)
}

func (m *Monitor) buildModelFRTReportFiltered(ctx context.Context, key, rawEnd string, filter ModelFRTFilter, now time.Time) (ModelFRTReport, error) {
	selection, err := modelReportSelectionFor(url.Values{"window": {key}, "to": {rawEnd}})
	if err != nil {
		return ModelFRTReport{}, err
	}
	return m.buildModelFRTSelected(ctx, selection, filter, now)
}

func (m *Monitor) buildModelFRTSelected(ctx context.Context, selection modelReportSelection, filter ModelFRTFilter, now time.Time) (ModelFRTReport, error) {
	report := ModelFRTReport{Channels: []ModelFRTChannel{}}
	if m == nil || m.storeDB == nil {
		return report, fmt.Errorf("local fact store unavailable")
	}
	retainedFrom := m.modelReportRetainedFrom(now)
	target := metricFinalizeTarget(now.Unix())
	if m.cfg.CustomerHealthSourceEnabled {
		target = customerHealthSourceFinalizedThrough(now)
	}
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		local := &Monitor{cfg: m.cfg, storeDB: tx}
		end, readErr := local.modelStatisticsAvailableEnd(ctx, target, false)
		if readErr != nil {
			return readErr
		}
		bounds, readErr := local.resolveModelReportBounds(selection, now, target, end)
		if readErr != nil {
			return readErr
		}
		from, end := bounds.From, bounds.To
		if !bounds.Range.HasWindow {
			report = ModelFRTReport{Range: bounds.Range, Filter: filter, Window: bounds.Key, From: time.Unix(from, 0).UTC(), To: time.Unix(end, 0).UTC(), ToTs: end, ThroughTs: bounds.ActualThrough, ActualThroughTs: bounds.ActualThrough, Target: time.Unix(bounds.Target, 0).UTC(), GeneratedAt: now.UTC(), LagSeconds: bounds.Lag, Channels: []ModelFRTChannel{}, Coverage: ModelFRTCoverage{Status: modelCoverageIncomplete, Note: bounds.Range.Note}}
			return ctx.Err()
		}
		coverage, readErr := local.modelFRTCoverage(ctx, from, end)
		if readErr != nil {
			return readErr
		}
		if bounds.Incomplete {
			coverage.Complete = false
			coverage.Status = modelCoverageIncomplete
			coverage.Note += " " + bounds.Range.Note
		}
		if from < retainedFrom {
			coverage.Complete = false
			coverage.Status = modelCoverageIncomplete
			coverage.Note = "实际窗口包含已超出本地分钟留存的区间；保留已知样本，覆盖不完整。"
		}
		rows, readErr := local.modelFRTRows(ctx, from, end, filter)
		if readErr != nil {
			return readErr
		}
		for _, row := range rows {
			if row.InvalidRows > 0 || row.HistoricalRows > 0 {
				coverage.Complete = false
				if coverage.Status != modelCoverageUnavailable {
					coverage.Status = modelCoverageIncomplete
				}
				coverage.Note += " 存在历史不支持或无效的 FRT 源行。"
				break
			}
		}
		var names []ChannelSnap
		if readErr = tx.Select("id, name").Find(&names).Error; readErr != nil {
			return readErr
		}
		nameMap := make(map[int]string, len(names))
		for _, row := range names {
			nameMap[row.ID] = strings.TrimSpace(row.Name)
		}
		channels := modelFRTTree(rows, nameMap, coverage.Complete)
		var excluded int64
		if filter.MeanGtMs != nil {
			channels, excluded = filterModelFRTChannelsByMean(channels, *filter.MeanGtMs)
		}
		report = ModelFRTReport{Range: bounds.Range, Filter: filter, Window: bounds.Key, From: time.Unix(from, 0).UTC(), To: time.Unix(end, 0).UTC(), ToTs: end, ThroughTs: bounds.ActualThrough, ActualThroughTs: bounds.ActualThrough, Target: time.Unix(bounds.Target, 0).UTC(), GeneratedAt: now.UTC(), LagSeconds: bounds.Lag, Coverage: coverage, Channels: channels,
			MeanFilterExcluded: excluded, MeanFilterApplied: filter.MeanGtMs != nil}
		return ctx.Err()
	})
	report.CollectionFailed = m.modelReportCollectionFailed(false)
	report.Timing = m.modelReportTiming(false)
	return report, err
}

func modelFRTTree(rows []modelFRTRow, names map[int]string, complete bool) []ModelFRTChannel {
	tree := map[int]map[string]map[string]*modelFRTAggregate{}
	for _, row := range rows {
		sample := row.Row
		group, model := strings.TrimSpace(sample.Group), strings.TrimSpace(sample.Model)
		if tree[sample.ChannelID] == nil {
			tree[sample.ChannelID] = map[string]map[string]*modelFRTAggregate{}
		}
		if tree[sample.ChannelID][group] == nil {
			tree[sample.ChannelID][group] = map[string]*modelFRTAggregate{}
		}
		if tree[sample.ChannelID][group][model] == nil {
			tree[sample.ChannelID][group][model] = &modelFRTAggregate{}
		}
		a := modelFRTAggregate{historical: row.HistoricalRows > 0, invalid: row.InvalidRows > 0,
			sumMs: row.FRTSumMs, sumObserved: row.FRTSumObserved, meanUnsupported: row.MeanUnsupportedRows > 0, meanInvalid: row.MeanInvalidRows > 0}
		mergeModelStatisticsTTFT(&a.hist, sample)
		tree[sample.ChannelID][group][model].merge(a)
	}
	channels := make([]ModelFRTChannel, 0, len(tree))
	for id, groups := range tree {
		channel := ModelFRTChannel{ChannelID: id, Name: names[id], Groups: []ModelFRTGroup{}}
		if channel.Name == "" {
			channel.Name = fmt.Sprintf("渠道 #%d", id)
		}
		if id <= 0 {
			channel.Name = "未记录渠道"
		}
		var channelHist modelFRTAggregate
		for group, models := range groups {
			g := ModelFRTGroup{Group: group, Models: []ModelFRTModel{}}
			var groupHist modelFRTAggregate
			for model, hist := range models {
				g.Models = append(g.Models, ModelFRTModel{Model: model, FRT: hist.view(complete)})
				groupHist.merge(*hist)
			}
			sort.Slice(g.Models, func(i, j int) bool {
				return modelFRTMeanLess(g.Models[i].FRT, g.Models[j].FRT, g.Models[i].Model < g.Models[j].Model)
			})
			g.FRT = groupHist.view(complete)
			channel.Groups = append(channel.Groups, g)
			channelHist.merge(groupHist)
		}
		sort.Slice(channel.Groups, func(i, j int) bool {
			return modelFRTMeanLess(channel.Groups[i].FRT, channel.Groups[j].FRT, channel.Groups[i].Group < channel.Groups[j].Group)
		})
		channel.FRT = channelHist.view(complete)
		channels = append(channels, channel)
	}
	sort.Slice(channels, func(i, j int) bool {
		return modelFRTMeanLess(channels[i].FRT, channels[j].FRT, channels[i].ChannelID < channels[j].ChannelID)
	})
	return channels
}

func (m *Monitor) serveModelFRTReport(c *gin.Context) {
	c.Header("Cache-Control", "private, no-store")
	ctx, cancel := context.WithTimeout(c.Request.Context(), modelStatisticsQueryTimeout)
	defer cancel()
	var filter ModelFRTFilter
	if raw := c.Query("channel_id"); raw != "" {
		id, err := strconv.Atoi(raw)
		if err != nil || id < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "channel_id 必须是非负整数"})
			return
		}
		filter.ChannelID = &id
	}
	for _, item := range []struct {
		key    string
		target **string
	}{{"group", &filter.Group}, {"model", &filter.Model}} {
		if c.Request.URL.Query().Has(item.key) {
			value := strings.TrimSpace(c.Query(item.key))
			if len(value) > 512 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "筛选值过长"})
				return
			}
			*item.target = &value
		}
	}
	// 阈值按秒输入(与页面显示单位一致),在此一次性转成毫秒整数。
	// 上界取 3600 秒:超过一小时的首字耗时不是筛选场景,只会是误输入。
	if raw := strings.TrimSpace(c.Query("mean_gt_s")); raw != "" {
		seconds, convErr := strconv.ParseFloat(raw, 64)
		if convErr != nil || math.IsNaN(seconds) || seconds < 0 || seconds > 3600 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "平均 FRT 阈值必须是 0 到 3600 之间的秒数"})
			return
		}
		ms := int64(math.Round(seconds * 1000))
		filter.MeanGtMs = &ms
	}
	selection, err := modelReportSelectionFor(c.Request.URL.Query())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	report, err := m.buildModelFRTSelected(ctx, selection, filter, time.Now())
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		var input *modelFRTInputError
		if errors.As(err, &input) {
			c.JSON(http.StatusBadRequest, gin.H{"error": input.Error()})
			return
		}
		message := "首字耗时本地事实暂时无法读取，请稍后重试"
		if errors.Is(err, errModelFRTLimit) {
			message = "所选范围的模型维度过多，请按渠道、分组或模型筛选后重试"
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": message})
		return
	}
	c.JSON(http.StatusOK, report)
}
