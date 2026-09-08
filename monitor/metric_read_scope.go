package monitor

// metricReadScope separates historical accounting from operational routing
// policy. Changing today's channel configuration must never rewrite yesterday's
// dashboard, while existing alert queries retain their routing exclusions.
type metricReadScope uint8

const modelDimensionRowLimit = 200

// Read one sentinel row for dashboard truncation detection without a second
// aggregate/count query. Operational callers keep their existing row budget.
func (scope metricReadScope) dimensionReadLimit() int {
	if scope == metricRecordedTraffic {
		return modelDimensionRowLimit + 1
	}
	return modelDimensionRowLimit
}

type DimensionLimit struct {
	Limit     int  `json:"limit"`
	Truncated bool `json:"truncated"`
}

func limitModelDimension(rows []Row) ([]Row, DimensionLimit) {
	meta := DimensionLimit{Limit: modelDimensionRowLimit, Truncated: len(rows) > modelDimensionRowLimit}
	if meta.Truncated {
		rows = rows[:modelDimensionRowLimit]
	}
	return rows, meta
}

const (
	metricCurrentRoutes metricReadScope = iota
	metricRecordedTraffic
)

func (scope metricReadScope) filter(dim string) string {
	if scope == metricRecordedTraffic || dim == channelDim {
		return currentMetricTrafficFilter
	}
	return currentMetricTrafficFilter + enabledChanFilter + selectableFilter
}
