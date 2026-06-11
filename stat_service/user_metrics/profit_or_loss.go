package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type userProfitOrLoss struct {
	db        *gorm.DB
	metricMap MetricMap
}

func (u *userProfitOrLoss) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	ocpQuery, err := u.metricMap.GetQuery(selling_iface.UserMetricType_USER_METRIC_TYPE_PROFIT_ORDER_CREATED, ctx, ufilter)
	if err != nil {
		return nil, err
	}
	ocpQuery = ocpQuery.
		Select([]string{"sum(o.order_mp_total - o.total) as profit_created_amount", "o.created_by_id"}).
		Group("o.created_by_id")

	olQuery, err := u.metricMap.GetQuery(selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_LOST, ctx, ufilter)
	if err != nil {
		return nil, err
	}
	olQuery = olQuery.
		Select([]string{"sum(o.total) as lost_amount", "o.created_by_id"}).
		Group("o.created_by_id")

	wdQuery, err := u.metricMap.GetQuery(selling_iface.UserMetricType_USER_METRIC_TYPE_WITHDRAWAL, ctx, ufilter)
	if err != nil {
		return nil, err
	}
	wdQuery = wdQuery.
		Select([]string{"sum(oa.amount) filter (where oa.amount < 0) as adjustment_amount", "o.created_by_id"}).
		Group("o.created_by_id")

	aeQuery, err := u.metricMap.GetQuery(selling_iface.UserMetricType_USER_METRIC_TYPE_ADS_EXPENSE, ctx, ufilter)
	if err != nil {
		return nil, err
	}
	aeQuery = aeQuery.
		Select([]string{"sum(aeh.amount) as ads_amount", "aeh.created_by_id"}).
		Group("aeh.created_by_id")

	var query = u.
		db.
		Table("(?) ocp", ocpQuery).
		Joins("full join (?) ol on ol.created_by_id = ocp.created_by_id", olQuery).
		Joins("full join (?) wd on wd.created_by_id = ocp.created_by_id", wdQuery).
		Joins("full join (?) ae on ae.created_by_id = ocp.created_by_id", aeQuery)

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *userProfitOrLoss) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserProfitOrLossMetric{
		Data: map[uint64]*user_metric.UserProfitOrLossItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserProfitOrLossItem{}
	err = query.
		Where("coalesce(ocp.created_by_id, ol.created_by_id, wd.created_by_id, ae.created_by_id)  IN ?", userIds).
		Select([]string{
			"coalesce(ocp.created_by_id, ol.created_by_id, wd.created_by_id, ae.created_by_id) as user_id",
			"coalesce(ocp.profit_created_amount, 0) - coalesce(ol.lost_amount, 0) + coalesce(wd.adjustment_amount, 0) - coalesce(ae.ads_amount, 0) as profit_or_loss_amount",
		}).
		Find(&resultList).
		Error

	if err != nil {
		return nil, err
	}

	for _, item := range resultList {
		result.Data[item.UserId] = item
	}

	return &selling_iface.UserMetric{
		Data: &selling_iface.UserMetric_UserProfitOrLossMetric{
			UserProfitOrLossMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *userProfitOrLoss) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserProfitOrLossMetricSort() {
	case user_metric.UserProfitOrLossMetricSort_USER_PROFIT_OR_LOSS_METRIC_SORT_PROFIT_OR_LOSS_AMOUNT:
		sortfield = "coalesce(ocp.profit_created_amount, 0) - coalesce(ol.lost_amount, 0) + coalesce(wd.adjustment_amount, 0) - coalesce(ae.ads_amount, 0) as sfield"
	}

	query = query.
		Select([]string{
			"coalesce(ocp.created_by_id, ol.created_by_id, wd.created_by_id, ae.created_by_id) as created_by_id", sortfield,
		})

	limit, offset := getLimitOffset(ufilter.Page)
	wquery := u.
		db.
		Table("(?) w", query).
		Where("w.created_by_id > 0").
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

func NewUserProfitOrLossMetric(db *gorm.DB, metricMap MetricMap) UserMetricBase {
	return &userProfitOrLoss{
		db:        db,
		metricMap: metricMap,
	}
}
