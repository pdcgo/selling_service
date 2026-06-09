package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userOrderWithdrawal struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userOrderWithdrawal) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	query := u.
		db.
		Table("orders o").
		Joins("join order_adjustments oa on oa.order_id = o.id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userOrderWithdrawal) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserOrderWithdrawalMetric{
		Data: map[uint64]*user_metric.UserOrderWithdrawalItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserOrderWithdrawalItem{}
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
		Data: &selling_iface.UserMetric_UserOrderWithdrawalMetric{
			UserOrderWithdrawalMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userOrderWithdrawal) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserOrderWithdrawalMetricSort() {
	case user_metric.UserOrderWithdrawalMetricSort_USER_ORDER_WITHDRAWAL_METRIC_SORT_TOTAL_AMOUNT:
		sortfield = "sum(o.total) as sfield"
	case user_metric.UserOrderWithdrawalMetricSort_USER_ORDER_WITHDRAWAL_METRIC_SORT_MP_TOTAL_AMOUNT:
		sortfield = "sum(o.order_mp_total) as sfield"
	case user_metric.UserOrderWithdrawalMetricSort_USER_ORDER_WITHDRAWAL_METRIC_SORT_TRANSACTION_COUNT:
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

func NewUserOrderWithdrawalMetric(db *gorm.DB) UserMetricBase {
	return &userOrderWithdrawal{
		db: db,
	}
}
