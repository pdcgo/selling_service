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
			"date_trunc('day', it.created) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_WEEK:
		selects = append(selects,
			"date_trunc('week', it.created) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_MONTH:
		selects = append(selects,
			"date_trunc('month', it.created) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_YEAR:
		selects = append(selects,
			"date_trunc('year', it.created) as t",
		)
	}

	selects = append(selects,
		"count(iti.inv_transaction_id) as transaction_count",
		"sum(iti.count) as piece_count",
		"sum(iti.total) as piece_amount",

		"count(iti.inv_transaction_id) filter (where it.team_id = s.team_id) as own_transaction_count",
		"sum(iti.count) filter (where it.team_id = s.team_id) as own_piece_count",
		"sum(iti.total) filter (where it.team_id = s.team_id) as own_piece_amount",

		"count(iti.inv_transaction_id) filter (where it.team_id != s.team_id) as cross_transaction_count",
		"sum(iti.count) filter (where it.team_id != s.team_id) as cross_piece_count",
		"sum(iti.total) filter (where it.team_id != s.team_id) as cross_piece_amount",
	)

	query := db.
		Table("inv_tx_items iti").
		Joins("left join inv_transactions it on it.id = iti.inv_transaction_id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.status != 'cancel'").
		Where("it.type = 'order'").
		Where("it.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("s.team_id = ?", filter.TeamId)
	}

	if filter.ProductFilter != nil {
		productFilter := filter.ProductFilter
		query = query.Where("s.product_id = ?", productFilter.ProductId)
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
