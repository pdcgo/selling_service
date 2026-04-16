package metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryShipmentProblemMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *selling_iface.TimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryShipmentProblemMetric{
		TimeType: trange.Type,
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryShipmentProblem{
			HistoryShipmentProblem: &result,
		},
	}, err
}
