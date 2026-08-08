package metrics

import (
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryStockOrderMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
	trange *common.StatTimeRange,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryStockOrderMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.HistoryStockOrderItem{},
	}

	var selects []string

	switch trange.Type {
	case common.StatTimeType_STAT_TIME_TYPE_DAY:
		selects = append(selects,
			"date_trunc('day', o.created_at) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_WEEK:
		selects = append(selects,
			"date_trunc('week', o.created_at) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_MONTH:
		selects = append(selects,
			"date_trunc('month', o.created_at) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_YEAR:
		selects = append(selects,
			"date_trunc('year', o.created_at) as t",
		)
	}

	selects = append(selects,
		"count(oi.order_id) as transaction_count",
		"sum(oi.count) as piece_count",
		"sum(oi.total) as piece_amount",

		"count(oi.order_id) filter (where p.team_id = o.team_id) as own_transaction_count",
		"sum(oi.count) filter (where p.team_id = o.team_id) as own_piece_count",
		"sum(oi.total) filter (where p.team_id = o.team_id) as own_piece_amount",

		"count(oi.order_id) filter (where p.team_id != o.team_id) as cross_transaction_count",
		"sum(oi.count) filter (where p.team_id != o.team_id) as cross_piece_count",
		"sum(oi.total) filter (where p.team_id != o.team_id) as cross_piece_amount",
	)

	query := db.
		Table("order_items oi").
		Joins("left join orders o on o.id = oi.order_id").
		Joins("left join products p on p.id = oi.product_id").
		// Where("o.status != 'cancel'").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.
			Joins("left join inv_transactions it on it.id = o.invertory_tx_id").
			Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("p.team_id = ?", filter.TeamId)
	}

	if filter.ProductFilter != nil {
		productFilter := filter.ProductFilter
		query = query.Where("p.id = ?", productFilter.ProductId)
	}

	err = query.
		Select(selects).
		Group("t").
		Order("t asc").
		Find(&result.Items).
		Error

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryStockOrder{
			HistoryStockOrder: &result,
		},
	}, err
}
