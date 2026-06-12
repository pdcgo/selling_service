package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type stockUserOrderCompleted struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *stockUserOrderCompleted) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	query := u.
		db.
		Table("order_items oi").
		Joins("join orders o on o.id = oi.order_id").
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'completed'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *stockUserOrderCompleted) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserStockOrderCompletedMetric{
		Data: map[uint64]*user_metric.UserStockOrderCompletedItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserStockOrderCompletedItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(oi.count) as piece_count",
			"sum(oi.count) filter (where oi.owned = true) as own_piece_count",
			"sum(oi.count) filter (where oi.owned = false) as cross_piece_count",
			"sum(oi.total) as piece_amount",
			"sum(oi.total) filter (where oi.owned = true) as own_piece_amount",
			"sum(oi.total) filter (where oi.owned = false) as cross_piece_amount",
		}).
		Group("o.created_by_id").
		Find(&resultList).
		Error

	if err != nil {
		return nil, err
	}

	for _, item := range resultList {
		result.Data[item.UserId] = item
	}

	return &selling_iface.UserMetric{
		Data: &selling_iface.UserMetric_UserStockOrderCompletedMetric{
			UserStockOrderCompletedMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *stockUserOrderCompleted) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserStockOrderCompletedMetricSort() {
	case user_metric.UserStockOrderCompletedMetricSort_USER_STOCK_ORDER_COMPLETED_METRIC_SORT_PIECE_COUNT:
		sortfield = "sum(oi.count) as sfield"
	case user_metric.UserStockOrderCompletedMetricSort_USER_STOCK_ORDER_COMPLETED_METRIC_SORT_OWN_PIECE_COUNT:
		sortfield = "sum(oi.count) filter (where oi.owned = true) as sfield"
	case user_metric.UserStockOrderCompletedMetricSort_USER_STOCK_ORDER_COMPLETED_METRIC_SORT_CROSS_PIECE_COUNT:
		sortfield = "sum(oi.count) filter (where oi.owned = false) as sfield"
	case user_metric.UserStockOrderCompletedMetricSort_USER_STOCK_ORDER_COMPLETED_METRIC_SORT_PIECE_AMOUNT:
		sortfield = "sum(oi.total) as sfield"
	case user_metric.UserStockOrderCompletedMetricSort_USER_STOCK_ORDER_COMPLETED_METRIC_SORT_OWN_PIECE_AMOUNT:
		sortfield = "sum(oi.total) filter (where oi.owned = true) as sfield"
	case user_metric.UserStockOrderCompletedMetricSort_USER_STOCK_ORDER_COMPLETED_METRIC_SORT_CROSS_PIECE_AMOUNT:
		sortfield = "sum(oi.total) filter (where oi.owned = false) as sfield"
	}

	query = query.
		Select([]string{"o.created_by_id", sortfield}).
		Group("o.created_by_id")

	limit, offset := getLimitOffset(ufilter.Page)
	wquery := u.
		db.
		Table("(?) w", query).
		Select("w.created_by_id")

	switch usort.SortType {
	case selling_iface.UserMetricSortType_USER_METRIC_SORT_TYPE_ASC:
		wquery = wquery.Order("w.sfield asc nulls last")
	case selling_iface.UserMetricSortType_USER_METRIC_SORT_TYPE_DESC:
		wquery = wquery.Order("w.sfield desc nulls last")

	}

	err = wquery.
		Limit(limit).
		Offset(offset).
		Find(&ids).
		Error

	return ids, err

}

func NewUserStockOrderCompletedMetric(db *gorm.DB) UserMetricBase {
	return &stockUserOrderCompleted{db: db}
}
