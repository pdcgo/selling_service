package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type avgUserOrderReturn struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *avgUserOrderReturn) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range

	pieces := u.
		db.
		Table("order_items oi").
		Joins("join orders o on o.id = oi.order_id").
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("oi.order_id", "sum(oi.count) as units").
		Group("oi.order_id")

	if ufilter.TeamId != 0 {
		pieces = pieces.Where("o.team_id = ?", ufilter.TeamId)
	}

	query := u.
		db.
		Table("orders o").
		Joins("join (?) pieces on pieces.order_id = o.id", pieces).
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *avgUserOrderReturn) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserAvgOrderReturnMetric{
		Data: map[uint64]*user_metric.UserAvgOrderReturnItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserAvgOrderReturnItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(o.total)::numeric / nullif(count(o.id), 0) as total_per_transaction",
			"sum(coalesce(pieces.units,0))::numeric / nullif(count(o.id), 0) as piece_per_transaction",
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
		Data: &selling_iface.UserMetric_UserAvgOrderReturnMetric{
			UserAvgOrderReturnMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *avgUserOrderReturn) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserAvgOrderReturnMetricSort() {
	case user_metric.UserAvgOrderReturnMetricSort_USER_AVG_ORDER_RETURN_METRIC_SORT_TOTAL_PER_TRANSACTION:
		sortfield = "sum(o.total)::numeric / nullif(count(o.id), 0) as sfield"
	case user_metric.UserAvgOrderReturnMetricSort_USER_AVG_ORDER_RETURN_METRIC_SORT_PIECE_PER_TRANSACTION:
		sortfield = "sum(coalesce(pieces.units,0))::numeric / nullif(count(o.id), 0) as sfield"
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

func NewUserAvgOrderReturnMetric(db *gorm.DB) UserMetricBase {
	return &avgUserOrderReturn{db: db}
}
