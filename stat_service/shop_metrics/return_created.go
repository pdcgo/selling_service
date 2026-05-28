package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/shop_metric/v1"
	"gorm.io/gorm"
)

type shopReturnCreatedMetric struct {
	db *gorm.DB
}

type ReturnCreatedQueryType int

const (
	RETURN_CREATED_QUERY_NO_AGGREGATE ReturnCreatedQueryType = iota
	RETURN_CREATED_QUERY_ONLY_PIECE_AGGREGATE
	RETURN_CREATED_QUERY_ONLY_FEE_AGGREGATE
	RETURN_CREATED_QUERY_ALL_AGGREGATE
)

func createReturnCreatedQuery(db *gorm.DB, filter *selling_iface.ShopStatMetricFilter, t ReturnCreatedQueryType) *gorm.DB {
	trange := filter.Range
	query := db.
		Table("orders o").
		Joins("join inv_transactions it on it.id = o.invertory_return_tx_id and not it.deleted")

	if t == RETURN_CREATED_QUERY_ONLY_PIECE_AGGREGATE || t == RETURN_CREATED_QUERY_ALL_AGGREGATE {
		pieceAgg := db.
			Table("inv_tx_items iti").
			Select([]string{
				"iti.inv_transaction_id as transaction_id",
				"sum(iti.count) as piece_count",
				"sum(iti.total) as piece_amount",
			}).
			Group("iti.inv_transaction_id")

		query = query.Joins("join (?) pieceAgg on pieceAgg.transaction_id = o.invertory_return_tx_id", pieceAgg)
	}

	if t == RETURN_CREATED_QUERY_ONLY_FEE_AGGREGATE || t == RETURN_CREATED_QUERY_ALL_AGGREGATE {
		feeAgg := db.
			Table("restock_costs rc").
			Select([]string{
				"rc.inv_transaction_id as transaction_id",
				"sum(rc.per_piece_fee) as per_piece_fee",
			}).
			Group("rc.inv_transaction_id")

		query = query.Joins("join (?) feeAgg on feeAgg.transaction_id = o.invertory_return_tx_id", feeAgg)
	}

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	return query.Where("it.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())
}

// FetchMetric implements [ShopMetricBase].
func (s *shopReturnCreatedMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	var err error

	result := shop_metric.ShopReturnMetric{
		Data: map[uint64]*shop_metric.ShopReturnItem{},
	}

	resultList := []*shop_metric.ShopReturnItem{}
	selects := []string{
		"o.order_mp_id as shop_id",
		"count(it.id) as transaction_count",
		"sum(it.total) as transaction_amount",
		"sum(pieceAgg.piece_count) as piece_count",
		"sum(pieceAgg.piece_amount + (pieceAgg.piece_count * coalesce(feeAgg.per_piece_fee, 0))) as total_amount",
		"max(it.created) as last_return",
	}

	query := createReturnCreatedQuery(s.db, filter, RETURN_CREATED_QUERY_ALL_AGGREGATE).
		Where("o.order_mp_id in (?)", ids).
		Select(selects)

	err = query.
		Group("o.order_mp_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.ShopId] = item
	}

	return &selling_iface.ShopMetric{
		Data: &selling_iface.ShopMetric_ShopReturnMetric{
			ShopReturnMetric: &result,
		},
	}, err
}

// ProcessSort implements [ShopMetricBase].
func (s *shopReturnCreatedMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string
	var queryType ReturnCreatedQueryType

	switch sort.GetShopReturnMetricSort() {
	case shop_metric.ShopReturnMetricSort_SHOP_RETURN_METRIC_SORT_LAST_RETURN:
		queryType = RETURN_CREATED_QUERY_NO_AGGREGATE
		sortField = "max(it.created) as sfield"
	case shop_metric.ShopReturnMetricSort_SHOP_RETURN_METRIC_SORT_TOTAL_AMOUNT:
		queryType = RETURN_CREATED_QUERY_ALL_AGGREGATE
		sortField = "sum(pieceAgg.piece_amount + (pieceAgg.piece_count * coalesce(feeAgg.per_piece_fee, 0))) as sfield"
	case shop_metric.ShopReturnMetricSort_SHOP_RETURN_METRIC_SORT_PIECE_COUNT:
		queryType = RETURN_CREATED_QUERY_ONLY_PIECE_AGGREGATE
		sortField = "sum(pieceAgg.piece_count) as sfield"
	case shop_metric.ShopReturnMetricSort_SHOP_RETURN_METRIC_SORT_TRANSACTION_COUNT:
		queryType = RETURN_CREATED_QUERY_NO_AGGREGATE
		sortField = "count(it.id) as sfield"
	case shop_metric.ShopReturnMetricSort_SHOP_RETURN_METRIC_SORT_TRANSACTION_AMOUNT:
		queryType = RETURN_CREATED_QUERY_NO_AGGREGATE
		sortField = "sum(it.total) as sfield"
	}

	query := createReturnCreatedQuery(s.db, filter, queryType)
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

func NewShopReturnCreatedMetric(db *gorm.DB) ShopMetricBase {
	return &shopReturnCreatedMetric{db}
}
