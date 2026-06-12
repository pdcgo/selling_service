package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userWithdrawalBreakdown struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userWithdrawalBreakdown) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range

	query := u.
		db.
		Table("order_adjustments oa").
		Joins("join orders o on oa.order_id = o.id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userWithdrawalBreakdown) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserWithdrawalBreakdownMetric{
		Data: map[uint64]*user_metric.UserWithdrawalBreakdownItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserWithdrawalBreakdownItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(oa.amount) filter (where oa.type = 'aff_commission') as aff_commission_amount",
			"sum(oa.amount) filter (where oa.type = 'premi') as premi_amount",
			"sum(oa.amount) filter (where oa.type = 'packaging') as packaging_amount",
			"sum(oa.amount) filter (where oa.type = 'return_adj') as return_adj_amount",
			"sum(oa.amount) filter (where oa.type = 'shipping_adj') as shipping_adj_amount",
			"sum(oa.amount) filter (where oa.type = 'compensation') as compensation_amount",
			"sum(oa.amount) filter (where oa.type = 'lost_compensation') as lost_compensation_amount",
			"sum(oa.amount) filter (where oa.type = 'unknown') as unknown_amount",
			"sum(oa.amount) filter (where oa.type = 'order_fund') as order_fund_amount",
			"sum(oa.amount) filter (where oa.type = 'unknown_adj') as unknown_adj_amount",
			"sum(oa.amount) as total_amount",
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
		Data: &selling_iface.UserMetric_UserWithdrawalBreakdownMetric{
			UserWithdrawalBreakdownMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userWithdrawalBreakdown) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserWithdrawalBreakdownMetricSort() {
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_AFF_COMMISSION_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'aff_commission') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_PREMI_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'premi') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_PACKAGING_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'packaging') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_RETURN_ADJ_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'return_adj') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_SHIPPING_ADJ_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'shipping_adj') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_COMPENSATION_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'compensation') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_LOST_COMPENSATION_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'lost_compensation') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_UNKNOWN_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'unknown') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_ORDER_FUND_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'order_fund') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_UNKNOWN_ADJ_AMOUNT:
		sortfield = "sum(oa.amount) filter (where oa.type = 'unknown_adj') as sfield"
	case user_metric.UserWithdrawalBreakdownMetricSort_USER_WITHDRAWAL_BREAKDOWN_METRIC_SORT_TOTAL_AMOUNT:
		sortfield = "sum(oa.amount) as sfield"
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

func NewUserWithdrawalBreakdownMetric(db *gorm.DB) UserMetricBase {
	return &userWithdrawalBreakdown{db: db}
}
