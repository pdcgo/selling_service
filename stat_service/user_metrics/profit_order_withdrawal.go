package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userProfitOrderWithdrawal struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userProfitOrderWithdrawal) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	withdrawals := u.
		db.
		Table("order_adjustments oa").
		Joins("join orders o on o.id = oa.order_id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("oa.order_id, sum(oa.amount) as amount")

	if ufilter.TeamId != 0 {
		withdrawals = withdrawals.Where("o.team_id = ?", ufilter.TeamId)
	}

	query := u.
		db.
		Table("(?) w", withdrawals.Group("oa.order_id")).
		Joins("join orders o on o.id = w.order_id")

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userProfitOrderWithdrawal) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserProfitOrderWithdrawalMetric{
		Data: map[uint64]*user_metric.UserProfitOrderWithdrawalItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserProfitOrderWithdrawalItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(w.amount - o.total) as profit_withdrawal_amount",
			"((sum(w.amount - o.total) / sum(o.total)) * 100) as profit_withdrawal_percent",
			"sum(w.amount - o.order_mp_total) as profit_withdrawal_adjustment_amount",
			"((sum(w.amount - o.order_mp_total) / sum(o.order_mp_total)) * 100) as profit_withdrawal_adjustment_percent",
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
		Data: &selling_iface.UserMetric_UserProfitOrderWithdrawalMetric{
			UserProfitOrderWithdrawalMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userProfitOrderWithdrawal) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserProfitOrderWithdrawalMetricSort() {
	case user_metric.UserProfitOrderWithdrawalMetricSort_USER_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_AMOUNT:
		sortfield = "sum(w.amount - o.total) as sfield"
	case user_metric.UserProfitOrderWithdrawalMetricSort_USER_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_PERCENT:
		sortfield = "((sum(w.amount - o.total) / sum(o.total)) * 100) as sfield"
	case user_metric.UserProfitOrderWithdrawalMetricSort_USER_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_ADJUSTMENT_AMOUNT:
		sortfield = "sum(w.amount - o.order_mp_total) as sfield"
	case user_metric.UserProfitOrderWithdrawalMetricSort_USER_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_ADJUSTMENT_PERCENT:
		sortfield = "((sum(w.amount - o.order_mp_total) / sum(o.order_mp_total)) * 100) as sfield"
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

func NewUserProfitOrderWithdrawalMetric(db *gorm.DB) UserMetricBase {
	return &userProfitOrderWithdrawal{
		db: db,
	}
}
