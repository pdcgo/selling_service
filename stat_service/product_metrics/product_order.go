package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"gorm.io/gorm"
)

type productMetric struct {
	db *gorm.DB
}

func NewProductOrderMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &productMetric{db: db}
}

func (m *productMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var sortField string
	var productIds []uint64

	query := m.db.
		Table("inv_tx_items iti").
		Joins("left join inv_transactions it on it.id = iti.inv_transaction_id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.status != 'cancel'").
		Where("it.type = 'order'")

	// doing sort
	if pfilter.Range != nil {
		query = query.Where("it.created between ? and ?", pfilter.Range.Start.AsTime(), pfilter.Range.End.AsTime())
	}

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("s.team_id = ?", pfilter.TeamId)
	}

	// doing sorting
	switch psort.GetProductOrderMetricSort() {
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(iti.count) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_ORDER_COUNT:
		sortField = "count(iti.inv_transaction_id) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(iti.total) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_OWN_PIECE_COUNT:
		sortField = "sum(iti.count) filter (where it.team_id = s.team_id) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_OWN_ORDER_COUNT:
		sortField = "count(iti.inv_transaction_id) filter (where it.team_id = s.team_id) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_OWN_TOTAL_AMOUNT:
		sortField = "sum(iti.total) filter (where it.team_id = s.team_id) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_CROSS_PIECE_COUNT:
		sortField = "sum(iti.count) filter (where it.team_id != s.team_id) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_CROSS_ORDER_COUNT:
		sortField = "count(iti.inv_transaction_id) filter (where it.team_id != s.team_id) as sfield"
	case selling_iface.ProductOrderMetricSort_PRODUCT_ORDER_METRIC_SORT_CROSS_TOTAL_AMOUNT:
		sortField = "sum(iti.total) filter (where it.team_id != s.team_id) as sfield"
	}

	query = query.
		Select("s.product_id", sortField).
		Group("s.product_id")

	wrapquery := m.db.
		Table("(?) w", query).
		Select("product_id")

	switch psort.SortType {
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc")
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc")
	}

	err = wrapquery.
		Find(&productIds).
		Error

	return productIds, err
}

func (m *productMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	var err error

	result := selling_iface.ProductOrderMetric{
		Data: map[uint64]*selling_iface.ProductOrderMetricItem{},
	}

	resultList := []*selling_iface.ProductOrderMetricItem{}

	selects := []string{
		"s.product_id as product_id",
		"count(iti.inv_transaction_id) as transaction_count",
		"sum(iti.count) as piece_count",
		"sum(iti.total) as piece_amount",

		"count(iti.inv_transaction_id) filter (where it.team_id = s.team_id) as own_transaction_count",
		"sum(iti.count) filter (where it.team_id = s.team_id) as own_piece_count",
		"sum(iti.total) filter (where it.team_id = s.team_id) as own_piece_amount",

		"count(iti.inv_transaction_id) filter (where it.team_id != s.team_id) as cross_transaction_count",
		"sum(iti.count) filter (where it.team_id != s.team_id) as cross_piece_count",
		"sum(iti.total) filter (where it.team_id != s.team_id) as cross_piece_amount",
	}

	query := m.db.
		Table("inv_tx_items iti").
		Joins("left join inv_transactions it on it.id = iti.inv_transaction_id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.status != 'cancel'").
		Where("it.type = 'order'").
		Where("s.product_id in ?", productIds).
		Select(selects).
		Group("s.product_id")

	// doing sort
	if pfilter.Range != nil {
		query = query.Where("it.created between ? and ?", pfilter.Range.Start.AsTime(), pfilter.Range.End.AsTime())
	}

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("s.team_id = ?", pfilter.TeamId)
	}

	err = query.Find(&resultList).Error

	for _, item := range resultList {
		result.Data[item.ProductId] = item
	}

	return &selling_iface.ProductMetric{
		Data: &selling_iface.ProductMetric_OrderMetric{
			OrderMetric: &result,
		},
	}, err
}
