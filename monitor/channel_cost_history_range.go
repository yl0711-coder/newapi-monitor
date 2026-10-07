package monitor

import (
	"errors"

	"gorm.io/gorm"
)

// Omitted bounds preserve legacy previews. Explicit ranges are closed-hour,
// half-open intervals. The returned plan narrows to observed evidence only;
// callers must echo that plan's bounds when saving after confirmation.
func validateChannelCostHistoricalRange(in channelCostHistoricalBindingInput, closedHour int64) error {
	if in.ValidFrom == nil && in.ValidTo == nil {
		return nil
	}
	if in.ValidFrom == nil || in.ValidTo == nil {
		return errors.New("历史回填必须同时指定开始和结束时间")
	}
	from, to := *in.ValidFrom, *in.ValidTo
	if from < 0 || to <= from || from%3600 != 0 || to%3600 != 0 {
		return errors.New("历史回填需使用递增的整小时时段，结束时间不包含在内")
	}
	if to-from > int64(channelCostHistoricalBindingMaxHours)*3600 {
		return errors.New("历史回填单次最多 90 天，请缩小时段分段预演")
	}
	if to > closedHour {
		return errors.New("历史回填结束时间不能晚于已闭合小时")
	}
	return nil
}

// column is a fixed, qualified column name supplied by the five internal
// evidence/activity queries, never HTTP input. All values are parameterized.
func channelCostHistoricalRangeScope(in channelCostHistoricalBindingInput, column string) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if in.ValidFrom == nil || in.ValidTo == nil {
			return db
		}
		return db.Where(column+" >= ? AND "+column+" < ?", *in.ValidFrom, *in.ValidTo)
	}
}
