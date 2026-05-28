package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/shop_metric/v1"
	"gorm.io/gorm"
)

type shopOrderCancelledMetric struct {
	db *gorm.DB
}

type OrderCancelledQueryType int

const (
	ORDER_CANCELLED_QUERY_NO_AGGREGATE OrderCancelledQueryType = iota
	ORDER_CANCELLED_QUERY_ONLY_PIECE_AGGREGATE
	ORDER_CANCELLED_QUERY_ALL_AGGREGATE
)

func createOrderCancelledQuery(db *gorm.DB, filter *selling_iface.ShopStatMetricFilter, t OrderCancelledQueryType) *gorm.DB {
	trange := filter.Range
	query := db.
		Table("orders o").
		Joins("join inv_transactions it on it.id = o.invertory_tx_id and not it.deleted").
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'cancel'")

	if t == ORDER_CANCELLED_QUERY_ONLY_PIECE_AGGREGATE || t == ORDER_CANCELLED_QUERY_ALL_AGGREGATE {
		pieceAgg := db.
			Table("order_items oi").
			Select([]string{
				"oi.order_id",
				"sum(oi.count) as piece_count",
				"sum(oi.price * oi.count) as piece_amount",
			}).
			Group("oi.order_id")

		query = query.Joins("join (?) pieceAgg on pieceAgg.order_id = o.id", pieceAgg)
	}

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	return query.Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())
}

// FetchMetric implements [ShopMetricBase].
func (s *shopOrderCancelledMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	var err error

	result := shop_metric.ShopOrderCancelledMetric{
		Data: map[uint64]*shop_metric.ShopOrderCancelledItem{},
	}

	resultList := []*shop_metric.ShopOrderCancelledItem{}
	selects := []string{
		"o.order_mp_id as shop_id",
		"count(o.id) as transaction_count",
		"sum(pieceAgg.piece_count) as piece_count",
		"sum(pieceAgg.piece_amount) as piece_amount",
		"sum(pieceAgg.piece_count)::numeric / nullif(count(o.id), 0) as unit_per_transaction",
		"sum(coalesce(o.order_mp_total, 0)) as mp_total_amount",
		"sum(o.total) as order_total_amount",
		"(sum(coalesce(o.order_mp_total, 0))::double precision / nullif(count(o.id), 0)) as average_transaction_value",
		"max(ot.timestamp) as last_order_cancelled",
	}

	query := createOrderCancelledQuery(s.db, filter, ORDER_CANCELLED_QUERY_ALL_AGGREGATE).
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
		Data: &selling_iface.ShopMetric_ShopOrderCancelledMetric{
			ShopOrderCancelledMetric: &result,
		},
	}, err
}

// ProcessSort implements [ShopMetricBase].
func (s *shopOrderCancelledMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string
	var queryType OrderCancelledQueryType

	switch sort.GetShopOrderCancelledMetricSort() {
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_TRANSACTION_COUNT:
		queryType = ORDER_CANCELLED_QUERY_NO_AGGREGATE
		sortField = "count(o.id) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_PIECE_COUNT:
		queryType = ORDER_CANCELLED_QUERY_ONLY_PIECE_AGGREGATE
		sortField = "sum(pieceAgg.piece_count) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_PIECE_AMOUNT:
		queryType = ORDER_CANCELLED_QUERY_ONLY_PIECE_AGGREGATE
		sortField = "sum(pieceAgg.piece_amount) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_UNIT_PER_TRANSACTION:
		queryType = ORDER_CANCELLED_QUERY_ONLY_PIECE_AGGREGATE
		sortField = "sum(pieceAgg.piece_count)::numeric / nullif(count(o.id), 0) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_MP_TOTAL_AMOUNT:
		queryType = ORDER_CANCELLED_QUERY_NO_AGGREGATE
		sortField = "sum(coalesce(o.order_mp_total, 0)) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_ORDER_TOTAL_AMOUNT:
		queryType = ORDER_CANCELLED_QUERY_NO_AGGREGATE
		sortField = "sum(coalesce(o.total, 0)) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_AVERAGE_TRANSACTION_VALUE:
		queryType = ORDER_CANCELLED_QUERY_NO_AGGREGATE
		sortField = "sum(coalesce(o.total, 0))::numeric / nullif(count(oi.order_id), 0) as sfield"
	case shop_metric.ShopOrderCancelledMetricSort_SHOP_ORDER_CANCELLED_METRIC_SORT_LAST_ORDER_CANCELLED:
		queryType = ORDER_CANCELLED_QUERY_NO_AGGREGATE
		sortField = "max(ot.timestamp) as sfield"
	}

	query := createOrderCancelledQuery(s.db, filter, queryType)
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

func NewShopOrderCancelledMetric(db *gorm.DB) ShopMetricBase {
	return &shopOrderCancelledMetric{db}
}
