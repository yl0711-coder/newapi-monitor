package monitor

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Explicit ranges retain the largest existing query budget. Calendar queries
// keep their Beijing midnight start; exact comparisons never move either end.
const modelReportMaxDays = 7

type modelReportSelection struct {
	Window   modelStatisticsWindow
	Mode     string
	From, To int64
	RawEnd   string
}

type ModelReportRange struct {
	Mode             string    `json:"mode"`
	RequestedFrom    time.Time `json:"requested_from"`
	RequestedTo      time.Time `json:"requested_to"`
	AvailableThrough time.Time `json:"available_through"`
	RetainedFrom     time.Time `json:"retained_from"`
	MaxDays          int       `json:"max_days"`
	TodayOpen        bool      `json:"today_open"`
	HasWindow        bool      `json:"has_window"`
	Note             string    `json:"note"`
}

type modelReportBounds struct {
	From, To, ActualThrough, Target, Lag int64
	Key, Label                           string
	Range                                ModelReportRange
	Incomplete                           bool
}

func modelReportUTCMinute(raw string) (int64, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil || !strings.HasSuffix(raw, "Z") || t.Nanosecond() != 0 || t.Unix() <= 0 || t.Unix()%60 != 0 {
		return 0, &modelFRTInputError{"起止时间必须是 UTC 整分钟时间（以 Z 结尾）"}
	}
	return t.Unix(), nil
}

func modelReportSelectionFor(q url.Values) (modelReportSelection, error) {
	var s modelReportSelection
	for _, key := range []string{"window", "from", "to", "range"} {
		if len(q[key]) > 1 {
			return s, &modelFRTInputError{"时间参数不能重复"}
		}
	}
	if !q.Has("from") {
		if q.Has("range") {
			return s, &modelFRTInputError{"range 需要同时指定 from 和 to"}
		}
		w, err := modelStatisticsWindowFor(q.Get("window"))
		if err != nil {
			return s, &modelFRTInputError{err.Error()}
		}
		return modelReportSelection{Window: w, Mode: "rolling", RawEnd: q.Get("to")}, nil
	}
	if q.Has("window") || !q.Has("to") {
		return s, &modelFRTInputError{"from 和 to 必须成对传入，且不能与 window 混用"}
	}
	s.Mode = q.Get("range")
	if s.Mode == "" {
		s.Mode = "exact"
	}
	if s.Mode != "calendar" && s.Mode != "exact" {
		return s, &modelFRTInputError{"range 必须是 calendar 或 exact"}
	}
	var err error
	if s.From, err = modelReportUTCMinute(q.Get("from")); err != nil {
		return s, err
	}
	if s.To, err = modelReportUTCMinute(q.Get("to")); err != nil {
		return s, err
	}
	if s.To <= s.From {
		return s, &modelFRTInputError{"结束时间必须晚于开始时间；结束日期包含当天"}
	}
	if s.To-s.From > modelReportMaxDays*86400 {
		return s, &modelFRTInputError{"查询跨度最多 7 天"}
	}
	if s.Mode == "calendar" && (s.From != customerHealthDayStart(s.From) || s.To != customerHealthDayStart(s.To)) {
		return s, &modelFRTInputError{"自然日查询必须使用北京时间零点；结束日期转换为次日零点"}
	}
	return s, nil
}

func (m *Monitor) modelReportRetainedFrom(now time.Time) int64 {
	days := modelStatisticsRetentionDays(m.cfg)
	if m.cfg.CustomerHealthSourceEnabled {
		return customerHealthDayStart(now.Unix()) - max(customerHealthHistoryDays, int64(days))*86400
	}
	return metricMinuteRetentionCutoff(now.Unix(), days)
}

func (m *Monitor) resolveModelReportBounds(s modelReportSelection, now time.Time, target, available int64) (modelReportBounds, error) {
	var b modelReportBounds
	retained := m.modelReportRetainedFrom(now)
	b.Range = ModelReportRange{Mode: s.Mode, AvailableThrough: time.Unix(available, 0).UTC(), RetainedFrom: time.Unix(retained, 0).UTC(), MaxDays: min(modelReportMaxDays, modelStatisticsRetentionDays(m.cfg))}
	b.Target = target
	// Exact comparisons retain their requested right edge so the two pages
	// compare the same interval.  Expose the newest certified fact minute
	// separately instead of making callers infer it from a lag calculation.
	b.ActualThrough = minPositive(available, s.To)
	if s.Mode == "rolling" {
		if s.Window.Seconds > int64(modelStatisticsRetentionDays(m.cfg))*86400 {
			return b, &modelFRTInputError{"窗口超出当前分钟事实留存范围"}
		}
		end := available
		if s.RawEnd != "" {
			t, err := modelReportUTCMinute(s.RawEnd)
			if err != nil || t > available || t-s.Window.Seconds < retained {
				return b, &modelFRTInputError{"to 必须是留存范围内、不晚于可用水位的 UTC 整分钟时间"}
			}
			end = t
		}
		b.From, b.To, b.Key, b.Label = end-s.Window.Seconds, end, s.Window.Key, s.Window.Label
		b.ActualThrough = end
		if b.From <= 0 {
			return b, fmt.Errorf("本地统计水位尚未建立")
		}
		b.Lag = max(int64(0), target-end)
		b.Range.HasWindow = true
	} else {
		if s.From < retained || s.To-s.From > int64(b.Range.MaxDays)*86400 {
			return b, &modelFRTInputError{"所选日期超出当前分钟事实留存或查询跨度限制"}
		}
		today := customerHealthDayStart(now.Unix())
		if s.From > now.Unix() || s.From == now.Unix() && s.Mode != "calendar" || s.To > now.Unix() && (s.Mode != "calendar" || s.To != today+86400) {
			return b, &modelFRTInputError{"不能查询未来日期；今天仅统计到实际可用截止"}
		}
		b.From, b.To, b.Key, b.Label = s.From, s.To, s.Mode, "指定时间范围"
		b.Target = min(target, s.To)
		b.Lag = max(int64(0), b.Target-min(available, s.To))
		b.Range.TodayOpen = s.Mode == "calendar" && s.To > now.Unix()
		b.Range.HasWindow = true
		if s.Mode == "calendar" {
			b.To = max(s.From, min(s.To, available))
			b.ActualThrough = b.To
			b.Range.HasWindow = b.To > s.From
			b.Incomplete = !b.Range.HasWindow || b.To < s.To && !b.Range.TodayOpen
			b.Range.Note = "按北京时间自然日统计，结束日期包含当天。"
			if b.Range.TodayOpen {
				b.Range.Note += "今日尚未结束，仅展示实际可用截止前的数据。"
			}
			if !b.Range.HasWindow {
				b.Range.Note += "所选日期尚无可用的已定稿区间；未改用其他日期，不能据此确认零请求。"
			} else if b.To < s.To && !b.Range.TodayOpen {
				b.Range.Note += "所选日期尚未完整覆盖，当前仅有部分区间。"
			}
		} else {
			b.Incomplete = s.To > available
			b.Range.Note = "固定起止核对，统计区间不会随水位移动。"
			if b.Incomplete {
				b.Range.Note += "本页来源尚未覆盖固定区间，不能确认完整核对。"
			}
		}
		b.Range.RequestedFrom = time.Unix(s.From, 0).UTC()
		b.Range.RequestedTo = time.Unix(s.To, 0).UTC()
	}
	if s.Mode == "rolling" {
		b.Range.RequestedFrom = time.Unix(b.From, 0).UTC()
		b.Range.RequestedTo = time.Unix(b.To, 0).UTC()
	}
	return b, nil
}
