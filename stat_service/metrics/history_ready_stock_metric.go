package metrics

import (
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryReadyStockMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *common.StatTimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryReadyStockMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.HistoryReadyStockItem{},
	}

	var selects []string = []string{
		"sum(dsh.end_stock_count) as piece_count",
		"sum(dsh.end_stock_amount) as piece_amount",
		"sum(dsh.diff_stock_count) as spent_count",
		"sum(dsh.diff_stock_amount) as spent_amount",
	}

	switch trange.Type {
	case common.StatTimeType_STAT_TIME_TYPE_DAY:
		selects = append(selects, "date_trunc('day', dsh.t) as t")
	case common.StatTimeType_STAT_TIME_TYPE_WEEK:
		selects = append(selects, "date_trunc('week', dsh.t) as t")
	case common.StatTimeType_STAT_TIME_TYPE_MONTH:
		selects = append(selects, "date_trunc('month', dsh.t) as t")
	case common.StatTimeType_STAT_TIME_TYPE_YEAR:
		selects = append(selects, "date_trunc('year', dsh.t) as t")
	}

	query := db.
		Table("daily_sku_histories dsh").
		Joins("left join skus s on s.id = dsh.sku_id").
		Where("dsh.t between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.TeamId != 0 {
		query = query.Where("s.team_id = ?", filter.TeamId)
	}

	if filter.WarehouseId != 0 {
		query = query.Where("dsh.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.ProductFilter != nil {
		prodFilter := filter.ProductFilter
		query = query.Where("s.product_id = ?", prodFilter.ProductId)
	}

	err = query.
		Select(selects).
		Group("t").
		Order("t asc").
		Find(&result.Items).
		Error

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryReadyStock{
			HistoryReadyStock: &result,
		},
	}, err
}
