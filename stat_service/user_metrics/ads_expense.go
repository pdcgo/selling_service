package user_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/user_metric/v1"
	"gorm.io/gorm"
)

type adsExpense struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *adsExpense) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {

	trange := ufilter.Range

	query := u.
		db.
		Table("ads_expense_histories aeh").
		Where("aeh.at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if ufilter.TeamId != 0 {
		query = query.Where("aeh.team_id = ?", ufilter.TeamId)
	}

	return query, nil
}

// FetchMetric implements [UserMetricBase].
func (u *adsExpense) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	var err error

	result := &user_metric.UserAdsExpenseMetric{
		Data: map[uint64]*user_metric.UserAdsExpenseItem{},
	}

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	resultList := []*user_metric.UserAdsExpenseItem{}
	err = query.
		Where("aeh.created_by_id IN ?", userIds).
		Select([]string{
			"aeh.created_by_id as user_id",
			"sum(aeh.amount) as ads_amount",
		}).
		Group("aeh.created_by_id").
		Find(&resultList).
		Error

	if err != nil {
		return nil, err
	}

	for _, item := range resultList {
		result.Data[item.UserId] = item
	}

	return &selling_iface.UserMetric{
		Data: &selling_iface.UserMetric_UserAdsExpenseMetric{
			UserAdsExpenseMetric: result,
		},
	}, err
}

// ProcessSort implements [UserMetricBase].
func (u *adsExpense) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := u.Query(ctx, ufilter)
	if err != nil {
		return nil, err
	}

	switch usort.GetUserAdsExpenseMetricSort() {
	case user_metric.UserAdsExpenseMetricSort_USER_ADS_EXPNESE_METRIC_SORT_ADS_AMOUNT:
		sortfield = "sum(aeh.amount) as sfield"
	}

	query = query.
		Select([]string{"aeh.created_by_id", sortfield}).
		Group("aeh.created_by_id")

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

func NewUserAdsExpenseMetric(db *gorm.DB) UserMetricBase {
	return &adsExpense{db: db}
}
