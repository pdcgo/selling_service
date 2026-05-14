package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"gorm.io/gorm"
)

type restockAcceptedMetric struct {
	db *gorm.DB
}

func NewRestockAcceptedMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &restockAcceptedMetric{db: db}
}

func (m *restockAcceptedMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	trange := pfilter.Range

	query := m.db.
		Table("inv_transactions it").
		Joins("left join inv_tx_items iti on iti.inv_transaction_id = it.id").
		Joins("left join skus s on s.id = iti.sku_id").
		Joins("left join restock_costs rc on rc.inv_transaction_id  = it.id").
		// Where("it.status != 'cancel'").
		Where("it.type = 'restock'").
		Where("it.arrived between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	// sorting
	switch psort.GetRestockAcceptedMetricSort() {
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_LAST_ARRIVED:
		sortField = "max(it.arrived) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(iti.total + (iti.count * coalesce(rc.per_piece_fee, 0))) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(iti.count) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(iti.inv_transaction_id) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_COST_AMOUNT:
		sortField = "sum(iti.count * coalesce(rc.per_piece_fee, 0)) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_PRODUCT_AMOUNT:
		sortField = "sum(iti.total) as sfield"
	}

	query = query.
		Select("s.product_id", sortField).
		Group("s.product_id")

	wrapquery := m.db.
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

func (m *restockAcceptedMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	var err error

	result := product_metric.RestockAcceptedMetric{
		Data: map[uint64]*product_metric.RestockAcceptedItem{},
	}
	trange := pfilter.Range

	resultList := []*product_metric.RestockAcceptedItem{}

	selects := []string{
		"s.product_id",
		"count(iti.inv_transaction_id) as transaction_count",
		"sum(iti.count) as piece_count",
		"sum(iti.total + (iti.count * coalesce(rc.per_piece_fee, 0))) as total_amount",
		"sum(iti.total) as product_amount",
		"sum(iti.count * coalesce(rc.per_piece_fee, 0)) as cost_amount",
		"max(it.arrived) as last_arrived",
	}

	query := m.db.
		Table("inv_transactions it").
		Joins("left join inv_tx_items iti on iti.inv_transaction_id = it.id").
		Joins("left join skus s on s.id = iti.sku_id").
		Joins("left join restock_costs rc on rc.inv_transaction_id  = it.id").
		// Where("it.status != 'cancel'").
		Where("it.type = 'restock'").
		Where("it.arrived between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("s.product_id in (?)", productIds).
		Select(selects)

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	err = query.
		Group("s.product_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.ProductId] = item
	}

	return &selling_iface.ProductMetric{
		Data: &selling_iface.ProductMetric_RestockAcceptedMetric{
			RestockAcceptedMetric: &result,
		},
	}, err
}
