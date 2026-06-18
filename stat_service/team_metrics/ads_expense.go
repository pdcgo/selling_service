package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type adsExpenseMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *adsExpenseMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("ads_expense_histories aeh").
		Where("aeh.at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *adsExpenseMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamAdsExpenseMetric{
		Data: map[uint64]*team_metric.TeamAdsExpenseItem{},
	}
	resultList := []*team_metric.TeamAdsExpenseItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("aeh.team_id in ?", teamIds).
		Select([]string{
			"aeh.team_id",
			"sum(aeh.amount) as ads_amount",
		}).
		Group("aeh.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamAdsExpenseMetric{
			TeamAdsExpenseMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *adsExpenseMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamAdsExpenseMetricSort() {
	case team_metric.TeamAdsExpenseMetricSort_TEAM_ADS_EXPENSE_METRIC_SORT_ADS_AMOUNT:
		sortField = "sum(aeh.amount) as sfield"
	default:
		err = errors.New("team ads expense metric sort invalid sort type")
		return nil, err
	}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	query = query.
		Select("aeh.team_id", sortField).
		Group("aeh.team_id")

	wrapquery := t.db.
		Table("(?) w", query).
		Select("team_id")

	switch tsort.GetSortType() {
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(tfilter.Page)
	err = wrapquery.
		Limit(limit).
		Offset(offset).
		Find(&teamIds).
		Error

	return teamIds, err
}

func NewAdsExpenseMetric(db *gorm.DB) TeamMetricBase {
	return &adsExpenseMetric{db}
}
