package metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryWarehouseProblemMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *selling_iface.TimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryWarehouseProblemMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.WarehouseProblemItem{},
	}

	query := db.
		Table("inv_item_problems iip").
		Joins("join inv_tx_items iti on iti.id = iip.tx_item_id").
		Joins("left join inv_transactions it on it.id = iip.tx_id").
		Where("iip.problem_type in ('lost_w', 'broken_w')").
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
		"sum(iip.count) filter (where iip.problem_type = 'lost_w') as lost_piece_count",
		"sum(iip.count * iti.price) filter (where iip.problem_type = 'lost_w') as lost_piece_amount",

		"sum(iip.count) filter (where iip.problem_type = 'broken_w') as damaged_piece_count",
		"sum(iip.count * iti.price) filter (where iip.problem_type = 'broken_w') as damaged_piece_amount",
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
		Data: &selling_iface.Metric_HistoryWarehouseProblem{
			HistoryWarehouseProblem: &result,
		},
	}, err
}
