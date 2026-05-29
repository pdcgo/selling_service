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

func createRestockAcceptedQuery(db *gorm.DB, pfilter *selling_iface.ProductStatMetricFilter) *gorm.DB {
	trange := pfilter.Range
	perTxProduct := db.
		Table("inv_tx_items iti").
		Joins("join skus s on s.id = iti.sku_id").
		Select([]string{
			"s.product_id",
			"iti.inv_transaction_id",
			"sum(iti.total) as piece_amount",
			"sum(iti.count) as piece_count",
		}).
		Group("s.product_id, iti.inv_transaction_id")

	query := db.
		Table("(?) p", perTxProduct).
		Joins("join inv_transactions it on it.id = p.inv_transaction_id and it.type = 'restock' and not it.deleted").
		Joins("left join restock_costs rc on rc.inv_transaction_id = it.id").
		Where("it.arrived between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	return query
}

func NewRestockAcceptedMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &restockAcceptedMetric{db: db}
}

func (m *restockAcceptedMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	query := createRestockAcceptedQuery(m.db, pfilter)

	// sorting
	switch psort.GetRestockAcceptedMetricSort() {
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_LAST_ARRIVED:
		sortField = "max(it.arrived) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(p.piece_amount + (p.piece_count * coalesce(rc.per_piece_fee, 0))) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(p.piece_count) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(p.inv_transaction_id) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_COST_AMOUNT:
		sortField = "sum(p.piece_count * coalesce(rc.per_piece_fee, 0)) as sfield"
	case product_metric.RestockAcceptedMetricSort_RESTOCK_ACCEPTED_METRIC_SORT_PRODUCT_AMOUNT:
		sortField = "sum(p.piece_amount) as sfield"
	}

	query = query.
		Select("p.product_id", sortField).
		Group("p.product_id")

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

	resultList := []*product_metric.RestockAcceptedItem{}
	selects := []string{
		"p.product_id",
		"count(p.inv_transaction_id) as transaction_count",
		"sum(p.piece_count) as piece_count",
		"sum(p.piece_amount + (p.piece_count * coalesce(rc.per_piece_fee, 0))) as total_amount",
		"sum(p.piece_amount) as product_amount",
		"sum(p.piece_count * coalesce(rc.per_piece_fee, 0)) as cost_amount",
		"max(it.arrived) as last_arrived",
	}

	query := createRestockAcceptedQuery(m.db, pfilter).
		Where("p.product_id in (?)", productIds).
		Select(selects)

	err = query.
		Group("p.product_id").
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
