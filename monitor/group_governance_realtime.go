package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// A small durable publication, independent from the low-frequency config sync.
// Hit/baseline history survives restart and is never advanced by a page read.
type GroupGovernanceRealtimeState struct {
	ID            uint `gorm:"primaryKey;autoIncrement:false"`
	Generation    int64
	Through       int64
	UpdatedAt     int64
	LastAttemptAt int64
	LastError     string `gorm:"size:512"`
	ReportJSON    string `gorm:"type:text"`
	ControlJSON   string `gorm:"type:text"`
}

func (m *Monitor) runGroupGovernanceRealtime(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdownSignal():
			return
		case <-timer.C:
			if ctx.Err() != nil {
				return
			}
			if err := m.syncGroupGovernanceRealtime(ctx, time.Now()); err != nil && ctx.Err() == nil {
				slog.Warn("分组治理近期指标延期", "err", err)
				// Preserve the last publication and its real data cutoff.
				cctx, cancel := context.WithTimeout(ctx, time.Second)
				m.storeDB.WithContext(cctx).Model(&GroupGovernanceRealtimeState{}).Where("id=?", 1).Updates(map[string]any{"last_attempt_at": time.Now().Unix(), "last_error": "本轮本地计算未完成；保留上次结果，等待有界重试"})
				cancel()
			}
			timer.Reset(time.Duration(groupGovernancePolicy().PollSeconds) * time.Second)
		}
	}
}

func ggConfigurationKey(groups []GroupGovernanceGroup) string {
	// Snapshot revision/time changes are not configuration changes. Only the
	// membership used for operation scope resets continuity and normal history.
	type item struct {
		Group   string
		Current bool
	}
	values := make([]item, 0, len(groups))
	for _, g := range groups {
		values = append(values, item{g.Grp, g.Current})
	}
	b, _ := json.Marshal(values)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (m *Monitor) syncGroupGovernanceRealtime(parent context.Context, now time.Time) error {
	if !m.cfg.GroupGovernanceEnabled || m.storeDB == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, groupGovernanceReadBudget)
	defer cancel()
	var report ggRealtimeReport
	var prior GroupGovernanceRealtimeState
	var snapshot GroupGovernanceState
	var groups []GroupGovernanceGroup
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("id=?", 1).Limit(1).Find(&prior).Error; err != nil {
			return err
		}
		if err := tx.Where("id=?", 1).Limit(1).Find(&snapshot).Error; err != nil {
			return err
		}
		if err := tx.Select("grp,current").Order("grp").Limit(groupGovernanceMaxGroups + 1).Find(&groups).Error; err != nil {
			return err
		}
		local := &Monitor{cfg: m.cfg, storeDB: tx}
		var err error
		report, err = local.readGroupGovernanceMetrics(ctx, now)
		return err
	})
	if err != nil {
		return err
	}
	// The transactional reader has no runtime atomics. Carry the actual source
	// failure separately; a historical finalizer failure is not a live-tail failure.
	failed := m.sourceLastFailureAt.Load() > m.sourceLastSuccessAt.Load()
	if m.cfg.CustomerHealthSourceEnabled {
		failed = failed || m.customerHealthSourceLastFailure.Load() > m.customerHealthSourceLastSuccess.Load()
	}
	if failed {
		report.Status, report.Note = "stale", "最近来源读取失败；保留已提交指标与真实截止时间，暂不能确认调整"
		for i := range report.Metrics {
			report.Metrics[i].Stale = true
			report.Metrics[i].Status = "stale"
		}
	}
	if len(groups) > groupGovernanceMaxGroups {
		return fmt.Errorf("group configuration exceeds bound")
	}
	if err = ggValidateMetricLimit(report.Metrics); err != nil {
		return err
	}
	p := groupGovernancePolicy()
	m.applyGroupGovernanceConfig(&report, snapshot, now)
	var control ggControl
	_ = json.Unmarshal([]byte(prior.ControlJSON), &control)
	ggRecommend(&report, &control, ggConfigurationKey(groups), p)
	report.AdviceReady = report.Status == "ready" && report.ConfigReady && report.RequestsComplete
	b, err := json.Marshal(report)
	if err != nil {
		return err
	}
	c, err := json.Marshal(control)
	if err != nil {
		return err
	}
	if len(b)+len(c) > 16<<20 {
		return fmt.Errorf("governance publication exceeds 16 MiB bound")
	}
	row := GroupGovernanceRealtimeState{ID: 1, Generation: prior.Generation + 1, Through: report.Through, UpdatedAt: now.Unix(), LastAttemptAt: now.Unix(), ReportJSON: string(b), ControlJSON: string(c)}
	// The expensive reads and rule evaluation finish before the short writer
	// transaction. CAS prevents two instances from consuming a minute twice.
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if prior.ID == 0 {
			return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error
		}
		res := tx.Model(&GroupGovernanceRealtimeState{}).Where("id=1 AND generation=?", prior.Generation).Select("generation", "through", "updated_at", "last_attempt_at", "last_error", "report_json", "control_json").Updates(&row)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("governance publication superseded")
		}
		// This derived error cache is independent of retained request facts.
		// Bounded deletion never touches unfinished Token or source cursors.
		return tx.Exec("DELETE FROM group_governance_error_minutes WHERE rowid IN (SELECT rowid FROM group_governance_error_minutes WHERE bucket_ts < ? ORDER BY bucket_ts LIMIT 200)", now.Unix()-86400).Error
	})
}

func (m *Monitor) loadGroupGovernanceRealtime(ctx context.Context) (*ggRealtimeReport, error) {
	var state GroupGovernanceRealtimeState
	if err := m.storeDB.WithContext(ctx).Where("id=?", 1).Limit(1).Find(&state).Error; err != nil {
		return nil, err
	}
	if state.ID == 0 {
		return &ggRealtimeReport{Status: "pending", Note: "后台尚未生成近期指标；页面读取不会触发生产查询", Metrics: []ggMetrics{}, Actions: []ggAction{}}, nil
	}
	var report ggRealtimeReport
	if err := json.Unmarshal([]byte(state.ReportJSON), &report); err != nil {
		return nil, err
	}
	if report.Version != groupGovernancePolicy().Version {
		report.Status = "deferred"
		report.Note = "统计窗口规则已升级，等待后台按渠道重新计算；保留已有指标及其原统计范围。"
	}
	if report.Channels == nil {
		// An older publication contains mixed model windows. Never relabel it
		// as channel totals; the existing background worker will replace it.
		report.Status = "pending"
		report.Note = "渠道汇总等待后台按同一窗口更新；页面不会触发生产查询"
		report.Actions = []ggAction{}
		report.AdjustmentCount, report.PauseCount, report.IncreaseCount = 0, 0, 0
		report.AdviceReady = false
	}
	if state.LastError != "" {
		report.Status = "deferred"
		report.Note = state.LastError
	}
	if time.Now().Unix()-state.UpdatedAt > groupGovernancePolicy().StaleSeconds {
		report.Status = "stale"
		report.Note = "后台近期指标未更新；已知故障保留，旧建议暂停执行"
	}
	if report.Status == "stale" || report.Status == "deferred" {
		// Keep evidence but withdraw executable suggestions when the publication
		// itself is stale. No refresh can count as a recovery minute.
		report.Actions = []ggAction{}
		report.AdjustmentCount = 0
		report.PauseCount = 0
		report.IncreaseCount = 0
		report.AdviceReady = false
		for i := range report.Channels {
			report.Channels[i].Stale = true
			report.Channels[i].Status = "stale"
			report.Channels[i].Actions = nil
			ggExplainChannel(&report.Channels[i], &report)
		}
	}
	return &report, nil
}

func (m *Monitor) applyGroupGovernanceConfig(report *ggRealtimeReport, snapshot GroupGovernanceState, now time.Time) {
	report.ConfigAt = snapshot.LastSuccessAt
	// A failed attempt keeps the previous snapshot (only a success replaces it),
	// so the snapshot's age alone decides staleness. Treating any failed attempt
	// as stale paused advice for minutes after a single queue timeout although
	// the configuration was minutes old.
	configStale := snapshot.LastSuccessAt == 0 || now.Unix()-snapshot.LastSuccessAt > int64(groupGovernanceInterval(m.cfg.GroupGovernanceSyncMinutes).Seconds()*2)
	report.ChannelMaxAge = int64(m.groupGovernanceChannelMaxAge().Seconds())
	report.ConfigReady = !configStale && m.groupGovernanceChannelCurrent(report.ChannelConfigAt, now)
	if !report.ConfigReady {
		report.Status = "pending"
		report.Note += " 渠道配置尚未更新，暂不能确认调整范围；保留已有指标及其实际截止时间。"
	}
	failed := m.modelReportCollectionFailed(false)
	for i := range report.Metrics {
		v := &report.Metrics[i]
		v.Stale = v.Stale || failed
		v.ConfigCurrent = v.ConfigCurrent && report.ConfigReady
		if !v.ConfigCurrent {
			v.Missing = append(v.Missing, "配置待更新：渠道配置尚未更新，暂不能确认调整范围")
			if v.Status == "unchanged" {
				v.Status = "pending"
			}
		}
		if v.Stale {
			v.Status = "stale"
			report.Status = "stale"
		}
	}
}
