package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userLostProfitOrder struct {
	db *gorm.DB
}

var lostProfitStatus = []string{"return", "return_completed", "problem", "return_problem"}

// FetchMetric implements [UserMetricBase].
func (u *userLostProfitOrder) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range
	firstTimestamp := u.db.
		Table("order_timestamps ot").
		Joins("join orders o on o.id = ot.order_id").
		Where("order_status in (?)", lostProfitStatus).
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("order_id, min(timestamp) as timestamp").
		Group("order_id")

	if ufilter.TeamId != 0 {
		firstTimestamp = firstTimestamp.Where("o.team_id = ?", ufilter.TeamId)
	}

	query := u.
		db.
		Table("orders o").
		Joins("join (?) ot on ot.order_id = o.id", firstTimestamp).
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select([]string{
			"sum(o.total) as total",
			"sum(o.order_mp_total) as mp_total",
		})

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userLostProfitOrder) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserLostProfitOrderMetric{
		Data: map[uint64]*user_metric.UserLostProfitOrderItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserLostProfitOrderItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(o.order_mp_total - o.total) as lost_profit_amount",
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
		Data: &selling_iface.UserMetric_UserLostProfitOrderMetric{
			UserLostProfitOrderMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userLostProfitOrder) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserLostProfitOrderMetricSort() {
	case user_metric.UserLostProfitOrderMetricSort_USER_LOST_PROFIT_ORDER_METRIC_SORT_LOST_PROFIT_AMOUNT:
		sortfield = "sum(o.order_mp_total - o.total) as sfield"
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

func NewUserLostProfitOrderMetric(db *gorm.DB) UserMetricBase {
	return &userLostProfitOrder{
		db: db,
	}
}
