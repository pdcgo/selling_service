package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type costUserOrderLost struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *costUserOrderLost) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range

	pieces := u.
		db.
		Table("order_items oi").
		Joins("join orders o on o.id = oi.order_id").
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return_problem'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select([]string{
			"oi.order_id",
			"sum(oi.total) as amount",
			"sum(oi.total) filter (where oi.owned = true) as own_amount",
			"sum(oi.total) filter (where oi.owned = false) as cross_amount",
		}).
		Group("oi.order_id")

	if ufilter.TeamId != 0 {
		pieces = pieces.Where("o.team_id = ?", ufilter.TeamId)
	}

	query := u.
		db.
		Table("orders o").
		Joins("join (?) pieces on pieces.order_id = o.id", pieces).
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return_problem'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("o.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *costUserOrderLost) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserCostOrderLostMetric{
		Data: map[uint64]*user_metric.UserCostOrderLostItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserCostOrderLostItem{}
	err = query.
		Where("o.created_by_id IN ?", userIds).
		Select([]string{
			"o.created_by_id as user_id",
			"sum(pieces.amount) as product_amount",
			"sum(pieces.own_amount) as own_product_amount",
			"sum(pieces.cross_amount) as cross_product_amount",
			"sum(o.warehouse_fee) as warehouse_amount",
			"sum(o.total) as total_amount",
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
		Data: &selling_iface.UserMetric_UserCostOrderLostMetric{
			UserCostOrderLostMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *costUserOrderLost) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserCostOrderLostMetricSort() {
	case user_metric.UserCostOrderLostMetricSort_USER_COST_ORDER_LOST_METRIC_SORT_PRODUCT_AMOUNT:
		sortfield = "sum(pieces.amount) as sfield"
	case user_metric.UserCostOrderLostMetricSort_USER_COST_ORDER_LOST_METRIC_SORT_OWN_PRODUCT_AMOUNT:
		sortfield = "sum(pieces.own_amount) as sfield"
	case user_metric.UserCostOrderLostMetricSort_USER_COST_ORDER_LOST_METRIC_SORT_CROSS_PRODUCT_AMOUNT:
		sortfield = "sum(pieces.cross_amount) as sfield"
	case user_metric.UserCostOrderLostMetricSort_USER_COST_ORDER_LOST_METRIC_SORT_WAREHOUSE_AMOUNT:
		sortfield = "sum(o.warehouse_fee) as sfield"
	case user_metric.UserCostOrderLostMetricSort_USER_COST_ORDER_LOST_METRIC_SORT_TOTAL_AMOUNT:
		sortfield = "sum(o.total) as sfield"
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

func NewUserCostOrderLostMetric(db *gorm.DB) UserMetricBase {
	return &costUserOrderLost{db: db}
}
