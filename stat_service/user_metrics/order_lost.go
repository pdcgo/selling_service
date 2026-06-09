package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userOrderLost struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userOrderLost) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	query := u.
		db.
		Table("orders o").
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return_problem'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userOrderLost) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserOrderLostMetric{
		Data: map[uint64]*user_metric.UserOrderLostItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserOrderLostItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(o.total) as total_amount",
			"sum(o.order_mp_total) as mp_total_amount",
			"count(o.id) as transaction_count",
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
		Data: &selling_iface.UserMetric_UserOrderLostMetric{
			UserOrderLostMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userOrderLost) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserOrderLostMetricSort() {
	case user_metric.UserOrderLostMetricSort_USER_ORDER_LOST_METRIC_SORT_TOTAL_AMOUNT:
		sortfield = "sum(o.total) as sfield"
	case user_metric.UserOrderLostMetricSort_USER_ORDER_LOST_METRIC_SORT_MP_TOTAL_AMOUNT:
		sortfield = "sum(o.order_mp_total) as sfield"
	case user_metric.UserOrderLostMetricSort_USER_ORDER_LOST_METRIC_SORT_TRANSACTION_COUNT:
		sortfield = "count(o.id) as sfield"
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

func NewUserOrderLostMetric(db *gorm.DB) UserMetricBase {
	return &userOrderLost{
		db: db,
	}
}
