package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type revenueOrderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *revenueOrderMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.status != 'cancel'")

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *revenueOrderMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamRevenueOrderMetric{
		Data: map[uint64]*team_metric.TeamRevenueOrderItem{},
	}
	resultList := []*team_metric.TeamRevenueOrderItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(o.order_mp_total) as revenue_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamRevenueOrderMetric{
			TeamRevenueOrderMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *revenueOrderMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamRevenueOrderMetricSort() {
	case team_metric.TeamRevenueOrderMetricSort_TEAM_REVENUE_ORDER_METRIC_SORT_REVENUE_AMOUNT:
		sortField = "sum(o.order_mp_total) as sfield"
	default:
		err = errors.New("team revenue order metric sort invalid sort type")
		return nil, err
	}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	query = query.
		Select("o.team_id", sortField).
		Group("o.team_id")

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

func NewRevenueOrderMetric(db *gorm.DB) TeamMetricBase {
	return &revenueOrderMetric{db}
}
