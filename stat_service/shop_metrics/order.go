package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/shop_metric/v1"
	"gorm.io/gorm"
)

type shopOrderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [ShopMetricBase].
func (s *shopOrderMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	var err error

	result := shop_metric.ShopOrderMetric{
		Data: map[uint64]*shop_metric.ShopOrderItem{},
	}
	trange := filter.Range

	resultList := []*shop_metric.ShopOrderItem{}

	selects := []string{
		"o.order_mp_id as shop_id",
		"count(oi.order_id) as transaction_count",
		"sum(oi.count) as piece_count",
		"sum(oi.total) as piece_amount",
		"sum(oi.count)::numeric / nullif(count(oi.order_id), 0) as unit_per_transaction",
		"sum(coalesce(o.order_mp_total, 0)) as mp_total_amount",
		"sum(coalesce(o.total, 0)) as order_total_amount",
		"(sum(coalesce(o.total, 0))::double precision / nullif(count(oi.order_id), 0)) as average_transaction_value",
		"max(o.created_at) as last_order_created",
	}

	query := s.db.
		Table("order_items oi").
		Joins("left join orders o on o.id = oi.order_id").
		Joins("left join inv_transactions it on it.id = o.invertory_tx_id").
		// Where("o.status != 'cancel'").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.order_mp_id in (?)", ids).
		Select(selects)

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	err = query.
		Group("o.order_mp_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.ShopId] = item
	}

	return &selling_iface.ShopMetric{
		Data: &selling_iface.ShopMetric_ShopOrderMetric{
			ShopOrderMetric: &result,
		},
	}, err
}

// ProcessSort implements [ShopMetricBase].
func (s *shopOrderMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	trange := filter.Range

	query := s.db.
		Table("order_items oi").
		Joins("left join orders o on o.id = oi.order_id").
		Joins("left join inv_transactions it on it.id = o.invertory_tx_id").
		// Where("o.status != 'cancel'").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	switch sort.GetShopOrderMetricSort() {
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(oi.order_id) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(oi.count) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_PIECE_AMOUNT:
		sortField = "sum(oi.total) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_UNIT_PER_TRANSACTION:
		sortField = "sum(oi.count)::numeric / nullif(count(oi.order_id), 0) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_MP_TOTAL_AMOUNT:
		sortField = "sum(coalesce(o.order_mp_total, 0)) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_ORDER_TOTAL_AMOUNT:
		sortField = "sum(coalesce(o.total, 0)) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_AVERAGE_TRANSACTION_VALUE:
		sortField = "sum(coalesce(o.total, 0))::numeric / nullif(count(oi.order_id), 0) as sfield"
	case shop_metric.ShopOrderMetricSort_SHOP_ORDER_METRIC_SORT_LAST_ORDER_CREATED:
		sortField = "max(o.created_at) as sfield"
	}

	query = query.
		Select("o.order_mp_id", sortField).
		Group("o.order_mp_id")

	wrapquery := s.db.
		Table("(?) w", query).
		Select("order_mp_id")

	switch sort.SortType {
	case selling_iface.ShopMetricSortType_SHOP_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.ShopMetricSortType_SHOP_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(filter.Page)
	err = wrapquery.
		Limit(limit).
		Offset(offset).
		Find(&productIds).
		Error

	return productIds, err
}

func NewShopOrderMetric(db *gorm.DB) ShopMetricBase {
	return &shopOrderMetric{db}
}
