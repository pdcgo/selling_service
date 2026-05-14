package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type stockOngoingMetric struct {
	db *gorm.DB
}

// FetchMetric implements [metric_base.ProductMetricBase].
func (s *stockOngoingMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	var err error

	result := product_metric.StockOngoingMetric{
		Data: map[uint64]*product_metric.StockOngoingItem{},
	}

	query := s.
		db.
		Table("inv_tx_items iti").
		Joins("left join inv_transactions it on it.id = iti.inv_transaction_id").
		Joins("left join restock_costs rc on rc.inv_transaction_id = iti.inv_transaction_id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.type in ?", []db_models.InvTxType{db_models.InvTxRestock, db_models.InvTxReturn}).
		Where("it.status = ?", db_models.InvTxOngoing).
		Where("s.product_id in ?", productIds).
		Select([]string{
			"s.product_id",
			"sum(iti.count) as stock_count",
			"sum(iti.count * (iti.price + coalesce(rc.per_piece_fee, 0))) as stock_amount",
		})

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	resultList := []*product_metric.StockOngoingItem{}

	err = query.
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
		Data: &selling_iface.ProductMetric_StockOngoingMetric{
			StockOngoingMetric: &result,
		},
	}, nil
}

// ProcessSort implements [metric_base.ProductMetricBase].
func (s *stockOngoingMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	query := s.
		db.
		Table("inv_tx_items iti").
		Joins("left join inv_transactions it on it.id = iti.inv_transaction_id").
		Joins("left join restock_costs rc on rc.inv_transaction_id = iti.inv_transaction_id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.type in ?", []db_models.InvTxType{db_models.InvTxRestock, db_models.InvTxReturn}).
		Where("it.status = ?", db_models.InvTxOngoing)

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	switch psort.GetStockOngoingMetricSort() {
	case product_metric.StockOngoingMetricSort_STOCK_ONGOING_METRIC_SORT_STOCK_COUNT:
		sortField = "sum(iti.count) as sfield"
	case product_metric.StockOngoingMetricSort_STOCK_ONGOING_METRIC_SORT_STOCK_AMOUNT:
		sortField = "sum(iti.count * (iti.price + coalesce(rc.per_piece_fee, 0))) as sfield"
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

func NewStockOngoingMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &stockOngoingMetric{db}
}
