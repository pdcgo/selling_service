package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userProfitOrderCreated struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userProfitOrderCreated) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	query := u.
		db.
		Table("orders o").
		Where("o.status != 'cancel'").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userProfitOrderCreated) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserProfitOrderCreatedMetric{
		Data: map[uint64]*user_metric.UserProfitOrderCreatedItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserProfitOrderCreatedItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(o.order_mp_total - o.total) as profit_created_amount",
			"((sum(o.order_mp_total - o.total) / sum(o.order_mp_total)) * 100) as profit_created_percent",
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
		Data: &selling_iface.UserMetric_UserProfitOrderCreatedMetric{
			UserProfitOrderCreatedMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userProfitOrderCreated) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserProfitOrderCreatedMetricSort() {
	case user_metric.UserProfitOrderCreatedMetricSort_USER_PROFIT_ORDER_CREATED_METRIC_SORT_PROFIT_CREATED_AMOUNT:
		sortfield = "sum(o.order_mp_total - o.total) as sfield"
	case user_metric.UserProfitOrderCreatedMetricSort_USER_PROFIT_ORDER_CREATED_METRIC_SORT_PROFIT_CREATED_PERCENT:
		sortfield = "((sum(o.order_mp_total - o.total) / sum(o.total)) * 100) as sfield"
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

func NewUserProfitOrderCreatedMetric(db *gorm.DB) UserMetricBase {
	return &userProfitOrderCreated{
		db: db,
	}
}
