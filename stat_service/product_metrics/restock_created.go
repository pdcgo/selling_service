package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"gorm.io/gorm"
)

type restockCreatedMetric struct {
	db *gorm.DB
}

func createRestockCreatedQuery(db *gorm.DB, pfilter *selling_iface.ProductStatMetricFilter) *gorm.DB {
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
		Where("it.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	return query
}

func NewRestockCreatedMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &restockCreatedMetric{db: db}
}

func (m *restockCreatedMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	query := createRestockCreatedQuery(m.db, pfilter)

	// sorting
	switch psort.GetRestockCreatedMetricSort() {
	case product_metric.RestockCreatedMetricSort_RESTOCK_CREATED_METRIC_SORT_LAST_CREATED:
		sortField = "max(it.created) as sfield"
	case product_metric.RestockCreatedMetricSort_RESTOCK_CREATED_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(p.piece_amount + (p.piece_count * coalesce(rc.per_piece_fee, 0))) as sfield"
	case product_metric.RestockCreatedMetricSort_RESTOCK_CREATED_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(p.piece_count) as sfield"
	case product_metric.RestockCreatedMetricSort_RESTOCK_CREATED_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(p.inv_transaction_id) as sfield"
	case product_metric.RestockCreatedMetricSort_RESTOCK_CREATED_METRIC_SORT_TRANSACTION_AMOUNT:
		sortField = "sum(it.total) as sfield"
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

func (m *restockCreatedMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	var err error

	result := product_metric.RestockCreatedMetric{
		Data: map[uint64]*product_metric.RestockCreatedItem{},
	}

	resultList := []*product_metric.RestockCreatedItem{}
	selects := []string{
		"p.product_id",
		"count(p.inv_transaction_id) as transaction_count",
		"count(it.total) as transaction_amount",
		"sum(p.piece_count) as piece_count",
		"sum(p.piece_amount + (p.piece_count * coalesce(rc.per_piece_fee, 0))) as total_amount",
		"max(it.created) as last_created",
	}

	query := createRestockCreatedQuery(m.db, pfilter).
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
		Data: &selling_iface.ProductMetric_RestockCreatedMetric{
			RestockCreatedMetric: &result,
		},
	}, err
}
