package metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryRestockMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *selling_iface.TimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryRestockMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.HistoryRestockItem{},
	}

	var createdSelects, arrivedSelects []string
	switch trange.Type {
	case selling_iface.TimeType_TIME_TYPE_DAY:
		createdSelects = append(createdSelects,
			"date_trunc('day', it.created) as t",
		)
		arrivedSelects = append(arrivedSelects,
			"date_trunc('day', it.arrived) as t",
		)
	case selling_iface.TimeType_TIME_TYPE_WEEK:
		createdSelects = append(createdSelects,
			"date_trunc('week', it.created) as t",
		)
		arrivedSelects = append(arrivedSelects,
			"date_trunc('week', it.arrived) as t",
		)
	case selling_iface.TimeType_TIME_TYPE_MONTH:
		createdSelects = append(createdSelects,
			"date_trunc('month', it.created) as t",
		)
		arrivedSelects = append(arrivedSelects,
			"date_trunc('month', it.arrived) as t",
		)
	case selling_iface.TimeType_TIME_TYPE_YEAR:
		createdSelects = append(createdSelects,
			"date_trunc('year', it.created) as t",
		)
		arrivedSelects = append(arrivedSelects,
			"date_trunc('year', it.arrived) as t",
		)
	}

	createdSelects = append(createdSelects,
		"count(iti.inv_transaction_id) as created_transaction_count",
		"sum(iti.count) as created_piece_count",
		"sum(iti.total + (iti.count * coalesce(rc.per_piece_fee, 0))) as created_piece_amount",
	)
	arrivedSelects = append(arrivedSelects,
		"count(iti.inv_transaction_id) as arrived_transaction_count",
		"sum(iti.count) as arrived_piece_count",
		"sum(iti.total + (iti.count * coalesce(rc.per_piece_fee, 0))) as arrived_piece_amount",
	)

	query := db.
		Table("inv_transactions it").
		Joins("left join inv_tx_items iti on iti.inv_transaction_id = it.id").
		Joins("left join restock_costs rc on rc.inv_transaction_id  = it.id").
		Where("it.status != 'cancel'").
		Where("it.type = 'restock'").
		Where("it.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	dquery := db.
		Table("(?) c",
			query.
				Session(&gorm.Session{}).
				Select(createdSelects).
				Group("t"),
		).
		Joins("full join (?) a on a.t = c.t",
			query.
				Session(&gorm.Session{}).
				Where("it.arrived is not null").
				Select(arrivedSelects).
				Group("t"),
		).
		Select([]string{
			"coalesce(c.t, a.t) as t",
			"coalesce(c.created_transaction_count, 0) as created_transaction_count",
			"coalesce(c.created_piece_count, 0) as created_piece_count",
			"coalesce(c.created_piece_amount, 0) as created_piece_amount",
			"coalesce(a.arrived_transaction_count, 0) as arrived_transaction_count",
			"coalesce(a.arrived_piece_count, 0) as arrived_piece_count",
			"coalesce(a.arrived_piece_amount, 0) as arrived_piece_amount",
		})

	err = dquery.
		Order("t asc").
		Find(&result.Items).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryRestock{
			HistoryRestock: &result,
		},
	}, err
}

func NewHistoryStockResolutionMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *selling_iface.TimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryStockResolutionMetric{
		TimeType: trange.Type,
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryStockResolution{
			HistoryStockResolution: &result,
		},
	}, err
}

func NewHistoryOutboundMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *selling_iface.TimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryOutboundMetric{
		TimeType: trange.Type,
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryOutbound{
			HistoryOutbound: &result,
		},
	}, err
}
