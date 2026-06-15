package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userRevenueProductAdjustment struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userRevenueProductAdjustment) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	query := u.
		db.
		Table("invoices inv").
		Joins("join orders o on o.id = inv.order_id").
		Where("inv.type = 'prod_adjust'").
		Where("inv.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userRevenueProductAdjustment) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserRevenueProductAdjustmentMetric{
		Data: map[uint64]*user_metric.UserRevenueProductAdjustmentItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserRevenueProductAdjustmentItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(inv.amount) as product_adjustment_amount",
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
		Data: &selling_iface.UserMetric_UserRevenueProductAdjustmentMetric{
			UserRevenueProductAdjustmentMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userRevenueProductAdjustment) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserRevenueProductAdjustmentMetricSort() {
	case user_metric.UserRevenueProductAdjustmentMetricSort_USER_REVENUE_PRODUCT_ADJUSTMENT_METRIC_SORT_PRODUCT_ADJUSTMENT_AMOUNT:
		sortfield = "sum(inv.amount) as sfield"
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

func NewUserRevenueProductAdjustmentMetric(db *gorm.DB) UserMetricBase {
	return &userRevenueProductAdjustment{db: db}
}
