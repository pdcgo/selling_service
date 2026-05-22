package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/shop_metric/v1"
	"gorm.io/gorm"
)

type shopProductCostMetric struct {
	db *gorm.DB
}

// FetchMetric implements [ShopMetricBase].
func (s *shopProductCostMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	var err error

	result := shop_metric.ShopProductCostMetric{
		Data: map[uint64]*shop_metric.ShopProductCostItem{},
	}
	trange := filter.Range

	resultList := []*shop_metric.ShopProductCostItem{}

	selects := []string{
		"o.order_mp_id as shop_id",
		"count(oi.product_id) filter (where oi.owned = true) as own_product_count",
		"sum(oi.total) filter (where oi.owned = true) as own_product_amount",
		"count(oi.product_id) filter (where oi.owned = false) as cross_product_count",
		"sum(oi.total) filter (where oi.owned = false) as cross_product_amount",
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
		Data: &selling_iface.ShopMetric_ShopProductCostMetric{
			ShopProductCostMetric: &result,
		},
	}, err
}

// ProcessSort implements [ShopMetricBase].
func (s *shopProductCostMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
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

	switch sort.GetShopProductCostMetricSort() {
	case shop_metric.ShopProductCostMetricSort_SHOP_PRODUCT_COST_METRIC_SORT_OWN_PRODUCT_COUNT:
		sortField = "count(oi.product_id) filter (where oi.owned = true) as sfield"
	case shop_metric.ShopProductCostMetricSort_SHOP_PRODUCT_COST_METRIC_SORT_OWN_PRODUCT_AMOUNT:
		sortField = "sum(oi.total) filter (where oi.owned = true) as sfield"
	case shop_metric.ShopProductCostMetricSort_SHOP_PRODUCT_COST_METRIC_SORT_CROSS_PRODUCT_COUNT:
		sortField = "count(oi.product_id) filter (where oi.owned = false) as sfield"
	case shop_metric.ShopProductCostMetricSort_SHOP_PRODUCT_COST_METRIC_SORT_CROSS_PRODUCT_AMOUNT:
		sortField = "sum(oi.total) filter (where oi.owned = false) as sfield"
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

func NewShopProductCostMetric(db *gorm.DB) ShopMetricBase {
	return &shopProductCostMetric{db}
}
