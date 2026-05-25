package product_cross_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_cross_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type costProductMetric struct {
	db *gorm.DB
}

// FetchMetric implements [metric_base.ProductCrossMetricBase].
func (c *costProductMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductCrossStatMetricFilter) (*selling_iface.ProductCrossMetric, error) {
	var err error

	result := product_cross_metric.CostProductMetric{
		Data: map[uint64]*product_cross_metric.CostProductItem{},
	}

	resultList := []*product_cross_metric.CostProductItem{}

	selects := []string{
		"oi.product_id",
		"count(oi.order_id) as transaction_count",
		"sum(oi.count) as piece_count",
		"sum(oi.total) as cost_amount",
	}

	query := c.db.Table("order_items oi").
		Joins("left join orders o on o.id = oi.order_id").
		Where("o.status != ?", db_models.OrdCancel).
		Where("oi.product_id in ?", productIds)

	if pfilter.Range != nil {
		query = query.Where("o.created_at between ? and ?", pfilter.Range.Start.AsTime(), pfilter.Range.End.AsTime())
	}

	if pfilter.WarehouseId != 0 {
		txQuery := c.db.
			Table("inv_transactions it").
			Where("it.warehouse_id = ?", pfilter.WarehouseId).
			Where("it.id = o.invertory_tx_id").
			Select("1")

		query = query.Where("exists (?)", txQuery)
	}

	if pfilter.TeamId != 0 {
		subCross := c.db.
			Table("team_cross_products cp").
			Where("cp.team_id = ?", pfilter.TeamId).
			Where("cp.product_id = oi.product_id").
			Select("1")

		query = query.Where("exists (?)", subCross)
	}

	query = query.
		Select(selects).
		Group("oi.product_id")

	err = query.
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.ProductId] = item
	}

	return &selling_iface.ProductCrossMetric{
		Data: &selling_iface.ProductCrossMetric_CostProductMetric{
			CostProductMetric: &result,
		},
	}, err
}

// ProcessSort implements [metric_base.ProductCrossMetricBase].
func (c *costProductMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductCrossStatMetricFilter, psort *selling_iface.ProductCrossMetricSort) ([]uint64, error) {
	var err error
	var sortField string
	var productIds []uint64

	query := c.
		db.
		Table("order_items oi").
		Joins("left join orders o on o.id = oi.order_id").
		Where("o.status != ?", db_models.OrdCancel)

	if pfilter.Range != nil {
		query = query.Where("o.created_at between ? and ?", pfilter.Range.Start.AsTime(), pfilter.Range.End.AsTime())
	}

	if pfilter.WarehouseId != 0 {
		txQuery := c.db.
			Table("inv_transactions it").
			Where("it.warehouse_id = ?", pfilter.WarehouseId).
			Where("it.id = o.invertory_tx_id").
			Select("1")

		query = query.Where("exists (?)", txQuery)
	}

	if pfilter.TeamId != 0 {
		subCross := c.db.
			Table("team_cross_products cp").
			Where("cp.team_id = ?", pfilter.TeamId).
			Where("cp.product_id = oi.product_id").
			Select("1")

		query = query.Where("exists (?)", subCross)
	}

	switch psort.GetCostProductMetricSort() {
	case product_cross_metric.CostProductMetricSort_COST_PRODUCT_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(oi.order_id) as sfield"
	case product_cross_metric.CostProductMetricSort_COST_PRODUCT_METRIC_SORT_COST_AMOUNT:
		sortField = "sum(oi.total) as sfield"
	case product_cross_metric.CostProductMetricSort_COST_PRODUCT_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(oi.count) as sfield"
	}

	query = query.Select("oi.product_id", sortField).
		Group("oi.product_id")

	wrapquery := c.db.
		Table("(?) w", query).
		Select("product_id")

	switch psort.SortType {
	case selling_iface.ProductCrossMetricSortType_PRODUCT_CROSS_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.ProductCrossMetricSortType_PRODUCT_CROSS_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(pfilter.Page)
	err = wrapquery.
		Limit(limit).
		Offset(offset).
		Find(&productIds).
		Error

	return productIds, err
}

func NewCostProductMetric(db *gorm.DB) metric_base.ProductCrossMetricBase {
	return &costProductMetric{
		db: db,
	}
}
