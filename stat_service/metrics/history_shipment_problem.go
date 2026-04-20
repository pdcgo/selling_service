package metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryShipmentProblemMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *selling_iface.TimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryShipmentProblemMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.ShipmentProblemItem{},
	}

	query := db.
		Table("inv_item_problems iip").
		Joins("join inv_tx_items iti on iti.id = iip.tx_item_id").
		Joins("left join restock_costs rc on rc.inv_transaction_id = iip.tx_id").
		Joins("left join inv_transactions it on it.id = iip.tx_id").
		Where("iip.problem_type in ('lost_s', 'broken_s')").
		Where("iip.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	var selects []string

	switch trange.Type {
	case selling_iface.TimeType_TIME_TYPE_DAY:
		selects = append(selects, "date_trunc('day', iip.created) as t")
	case selling_iface.TimeType_TIME_TYPE_WEEK:
		selects = append(selects, "date_trunc('week', iip.created) as t")
	case selling_iface.TimeType_TIME_TYPE_MONTH:
		selects = append(selects, "date_trunc('month', iip.created) as t")
	case selling_iface.TimeType_TIME_TYPE_YEAR:
		selects = append(selects, "date_trunc('year', iip.created) as t")
	}

	selects = append(selects,
		"count(iip.tx_id) filter (where iip.problem_type = 'lost_s') as lost_transaction_count",
		"sum(iip.count) filter (where iip.problem_type = 'lost_s') as lost_piece_count",
		"sum(iip.count * (iti.price + coalesce(rc.per_piece_fee, 0))) filter (where iip.problem_type = 'lost_s') as lost_piece_amount",

		"count(iip.tx_id) filter (where iip.problem_type = 'broken_s') as damaged_transaction_count",
		"sum(iip.count) filter (where iip.problem_type = 'broken_s') as damaged_piece_count",
		"sum(iip.count * (iti.price + coalesce(rc.per_piece_fee, 0))) filter (where iip.problem_type = 'broken_s') as damaged_piece_amount",
	)

	err = query.
		Select(selects).
		Group("t").
		Order("t").
		Find(&result.Items).Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryShipmentProblem{
			HistoryShipmentProblem: &result,
		},
	}, err
}
