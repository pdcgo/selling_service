package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userRevenueOrder struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userRevenueOrder) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	query := u.
		db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.status != 'cancel'")

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userRevenueOrder) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserRevenueOrderMetric{
		Data: map[uint64]*user_metric.UserRevenueOrderItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserRevenueOrderItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(o.order_mp_total) as revenue_amount",
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
		Data: &selling_iface.UserMetric_UserRevenueOrderMetric{
			UserRevenueOrderMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userRevenueOrder) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserRevenueOrderMetricSort() {
	case user_metric.UserRevenueOrderMetricSort_USER_REVENUE_ORDER_METRIC_SORT_REVENUE_AMOUNT:
		sortfield = "sum(o.order_mp_total) as sfield"
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

func NewUserRevenueOrderMetric(db *gorm.DB) UserMetricBase {
	return &userRevenueOrder{db: db}
}
