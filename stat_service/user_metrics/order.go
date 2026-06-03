package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userOrder struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userOrder) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserOrderMetric{
		Data: map[uint64]*user_metric.UserOrderItem{},
	}

	resultList := []*user_metric.UserOrderItem{}
	trange := ufilter.Range
	query := u.
		db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.created_by_id IN ?", userIds)

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	err = query.
		Select([]string{
			"o.created_by_id as user_id",
			"sum(o.total) as total_amount",
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
		Data: &selling_iface.UserMetric_UserOrderMetric{
			UserOrderMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userOrder) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	trange := ufilter.Range

	query := u.
		db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	switch usort.GetUserOrderMetricSort() {
	case user_metric.UserOrderMetricSort_USER_ORDER_METRIC_SORT_TOTAL_AMOUNT:
		sortfield = "sum(o.total) as sfield"
	case user_metric.UserOrderMetricSort_USER_ORDER_METRIC_SORT_TRANSACTION_COUNT:
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
		wquery = wquery.Order("w.created_by_id asc nulls last")
	case selling_iface.UserMetricSortType_USER_METRIC_SORT_TYPE_DESC:
		wquery = wquery.Order("w.created_by_id desc nulls last")

	}

	err = wquery.
		Limit(limit).
		Offset(offset).
		Find(&ids).
		Error

	return ids, err

}

func NewUserOrderMetric(db *gorm.DB) UserMetricBase {
	return &userOrder{db: db}
}
