package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"gorm.io/gorm"
)

type stockReadyMetric struct {
	db *gorm.DB
}

// FetchMetric implements [metric_base.ProductMetricBase].
func (s *stockReadyMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	var err error

	result := product_metric.StockReadyMetric{
		Data: map[uint64]*product_metric.StockReadyItem{},
	}

	selects := []string{
		"s.product_id",
		"sum(ih.count * -1) as stock_count",
		"sum(-1 * ih.count * (ih.price + coalesce(ih.ext_price, 0))) as stock_amount",
	}

	query := s.db.
		Table("invertory_histories ih").
		Joins("left join skus s on s.id = ih.sku_id").
		Where("ih.tx_id is null").
		Where("s.product_id in ?", productIds)

	if pfilter.WarehouseId != 0 {
		query = query.Where("s.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("s.team_id = ?", pfilter.TeamId)
	}

	resultList := []*product_metric.StockReadyItem{}
	err = query.
		Select(selects).
		Group("s.product_id").
		Find(&resultList).
		Error

	if err != nil {
		return nil, err
	}

	for _, item := range resultList {
		result.Data[item.ProductId] = item
	}

	return &selling_iface.ProductMetric{
		Data: &selling_iface.ProductMetric_StockReadyMetric{
			StockReadyMetric: &result,
		},
	}, err
}

// ProcessSort implements [metric_base.ProductMetricBase].
func (s *stockReadyMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	query := s.db.
		Table("invertory_histories ih").
		Joins("left join skus s on s.id = ih.sku_id").
		Where("ih.tx_id is null")

	if pfilter.WarehouseId != 0 {
		query = query.Where("s.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("s.team_id = ?", pfilter.TeamId)
	}

	switch psort.GetStockReadyMetricSort() {
	case product_metric.StockReadyMetricSort_STOCK_READY_METRIC_SORT_STOCK_AMOUNT:
		sortField = "sum(ih.count * -1) as sfield"
	case product_metric.StockReadyMetricSort_STOCK_READY_METRIC_SORT_STOCK_COUNT:
		sortField = "sum(-1 * ih.count * (ih.price + coalesce(ih.ext_price, 0))) as sfield"
	}

	query = query.
		Select("s.product_id", sortField).
		Group("s.product_id")

	wrapquery := s.db.
		Table("(?) w", query).
		Select("product_id")

	switch psort.SortType {
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_DESC:
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

func NewStockReadyMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &stockReadyMetric{db}
}
