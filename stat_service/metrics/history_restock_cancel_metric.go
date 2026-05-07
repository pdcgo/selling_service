package metrics

import (
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryRestockCancelMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *common.StatTimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryRestockCancelMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.HistoryRestockCancelItem{},
	}

	var selects []string
	switch trange.Type {
	case common.StatTimeType_STAT_TIME_TYPE_DAY:
		selects = append(selects,
			"date_trunc('day', its.timestamp) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_WEEK:
		selects = append(selects,
			"date_trunc('week', its.timestamp) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_MONTH:
		selects = append(selects,
			"date_trunc('month', its.timestamp) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_YEAR:
		selects = append(selects,
			"date_trunc('year', its.timestamp) as t",
		)
	}

	selects = append(selects,
		"count(iti.inv_transaction_id) as cancel_transaction_count",
		"sum(iti.count) as cancel_piece_count",
		"sum(iti.total + (iti.count * coalesce(rc.per_piece_fee, 0))) as cancel_piece_amount",
	)

	query := db.
		Table("inv_transactions it").
		Joins("left join inv_tx_items iti on iti.inv_transaction_id = it.id").
		Joins("left join restock_costs rc on rc.inv_transaction_id  = it.id").
		Joins("left join inv_timestamps its on its.tx_id = it.id and its.status = 'cancel'").
		Where("it.type = 'restock'").
		Where("its.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	if filter.ProductFilter != nil {
		productFilter := filter.ProductFilter
		skuQuery := db.
			Table("skus s").
			Where("s.product_id = ?", productFilter.ProductId).
			Where("s.id = iti.sku_id").
			Select("1")
		query = query.Where("exists (?)", skuQuery)
	}

	err = query.
		Group("t").
		Order("t asc").
		Select(selects).
		Find(&result.Items).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryRestockCancel{
			HistoryRestockCancel: &result,
		},
	}, err
}
