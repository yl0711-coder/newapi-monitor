package monitor

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FinanceChannelDomainPeriod pins hourly accounting attribution independently
// of the live channel directory. The initial row preserves the legacy snapshot;
// it is not proof of configuration changes before Monitor recorded them.
// An empty domain marks the entire unobserved change window as unattributed.
type FinanceChannelDomainPeriod struct {
	ChannelID  int    `gorm:"primaryKey;autoIncrement:false;column:channel_id"`
	FromHourTs int64  `gorm:"primaryKey;autoIncrement:false;column:from_hour_ts"`
	Domain     string `gorm:"size:253;column:domain"`
}

func (FinanceChannelDomainPeriod) TableName() string { return "finance_channel_domain_periods" }

const financeUnattributedDomain = "未配置/历史"

func financeDomain(domain string) string {
	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" {
		return financeUnattributedDomain
	}
	return domain
}

// Only successful authoritative directory refreshes call this, in the same
// transaction as their snapshot update. Rename/disable/delete never move money.
func recordFinanceChannelDomains(tx *gorm.DB, incoming []ChannelSnap, now int64) error {
	if len(incoming) == 0 {
		return nil
	}
	var previous []ChannelSnap
	if err := tx.Select("id,base_domain,updated_at").Find(&previous).Error; err != nil {
		return err
	}
	byID := make(map[int]ChannelSnap, len(previous))
	for _, snap := range previous {
		byID[snap.ID] = snap
	}
	baselines := make([]FinanceChannelDomainPeriod, 0, len(incoming))
	for _, snap := range incoming {
		domain := snap.BaseDomain
		if old, ok := byID[snap.ID]; ok {
			domain = old.BaseDomain
		}
		baselines = append(baselines, FinanceChannelDomainPeriod{ChannelID: snap.ID, Domain: strings.ToLower(strings.TrimSpace(domain))})
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(baselines, 200).Error; err != nil {
		return err
	}
	for _, snap := range incoming {
		old, exists := byID[snap.ID]
		if !exists || financeDomain(old.BaseDomain) == financeDomain(snap.BaseDomain) {
			continue
		}
		if now <= 0 {
			return fmt.Errorf("渠道 %d 归属变更时间无效", snap.ID)
		}
		// A directory observation is not the exact operator-change timestamp.
		// Do not split hourly money by a guessed boundary. Clock reversal widens
		// the unknown window rather than rewriting it with a confident owner.
		left, right := max(old.UpdatedAt, 0), now
		if left > right {
			left, right = right, left
		}
		unknownFrom := left / 3600 * 3600
		knownFrom := max((right+3599)/3600*3600, unknownFrom+3600)
		if err := tx.Where("channel_id=? AND from_hour_ts>=?", snap.ID, unknownFrom).Delete(&FinanceChannelDomainPeriod{}).Error; err != nil {
			return err
		}
		periods := []FinanceChannelDomainPeriod{
			{ChannelID: snap.ID, FromHourTs: unknownFrom},
			{ChannelID: snap.ID, FromHourTs: knownFrom, Domain: strings.ToLower(strings.TrimSpace(snap.BaseDomain))},
		}
		if err := tx.Create(&periods).Error; err != nil {
			return err
		}
	}
	return nil
}

func financeChannelDomainTableExists(db *gorm.DB) (bool, error) {
	// Old immutable offline snapshots have no new table. This check is read-only;
	// unlike HasTable, an I/O/context error must not become a silent fallback.
	var count int64
	err := db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='finance_channel_domain_periods'").Scan(&count).Error
	return count == 1, err
}

type financeChannelDomains struct {
	current map[int]string
	periods map[int][]FinanceChannelDomainPeriod
}

func loadFinanceChannelDomains(ctx context.Context, db *gorm.DB) (financeChannelDomains, error) {
	result := financeChannelDomains{current: make(map[int]string), periods: make(map[int][]FinanceChannelDomainPeriod)}
	var snaps []ChannelSnap
	if err := db.WithContext(ctx).Select("id,base_domain").Find(&snaps).Error; err != nil {
		return result, err
	}
	for _, snap := range snaps {
		result.current[snap.ID] = financeDomain(snap.BaseDomain)
	}
	exists, err := financeChannelDomainTableExists(db.WithContext(ctx))
	if err != nil || !exists {
		return result, err
	}
	var periods []FinanceChannelDomainPeriod
	if err := db.WithContext(ctx).Order("channel_id,from_hour_ts").Find(&periods).Error; err != nil {
		return result, err
	}
	for _, period := range periods {
		result.periods[period.ChannelID] = append(result.periods[period.ChannelID], period)
	}
	return result, nil
}

func (d financeChannelDomains) at(channelID int, hourTs int64) string {
	periods := d.periods[channelID]
	i := sort.Search(len(periods), func(i int) bool { return periods[i].FromHourTs > hourTs })
	if i > 0 {
		return financeDomain(periods[i-1].Domain)
	}
	return financeDomain(d.current[channelID])
}

func (d financeChannelDomains) varyingChannels(from, to int64) []int {
	var ids []int
	for channelID, periods := range d.periods {
		initial := d.at(channelID, from)
		for _, period := range periods {
			if period.FromHourTs > from && period.FromHourTs < to && financeDomain(period.Domain) != initial {
				ids = append(ids, channelID)
				break
			}
		}
	}
	sort.Ints(ids)
	return ids
}

// Only internal, constant SQL aliases/expressions may be passed here. The
// publisher and its dirty-hour detector must use the same historical owner as
// the report, or later republication could undo the report's attribution fix.
func financeChannelDomainSQL(db *gorm.DB, channelAlias, hourExpression string) (string, error) {
	exists, err := financeChannelDomainTableExists(db)
	if err != nil {
		return "", err
	}
	domain := channelAlias + ".base_domain"
	if exists {
		domain = fmt.Sprintf("COALESCE((SELECT d.domain FROM finance_channel_domain_periods d WHERE d.channel_id=%s.id AND d.from_hour_ts<=%s ORDER BY d.from_hour_ts DESC LIMIT 1),%s)", channelAlias, hourExpression, domain)
	}
	return "LOWER(COALESCE(NULLIF(TRIM(" + domain + "),''),'未配置/历史'))", nil
}
