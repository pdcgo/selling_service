package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type costProductCrossMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *costProductCrossMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("order_items oi").
		Joins("join orders o on o.id = oi.order_id").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.status != 'cancel'")

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *costProductCrossMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamCostProductCrossMetric{
		Data: map[uint64]*team_metric.TeamCostProductCrossItem{},
	}
	resultList := []*team_metric.TeamCostProductCrossItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(oi.total) filter (where oi.owned = false) as product_cross_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamCostProductCrossMetric{
			TeamCostProductCrossMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *costProductCrossMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamCostProductCrossMetricSort() {
	case team_metric.TeamCostProductCrossMetricSort_TEAM_COST_PRODUCT_CROSS_METRIC_SORT_PRODUCT_CROSS_AMOUNT:
		sortField = "sum(oi.total) filter (where oi.owned = false) as sfield"
	default:
		err = errors.New("team cost product cross metric sort invalid sort type")
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

func NewCostProductCrossMetric(db *gorm.DB) TeamMetricBase {
	return &costProductCrossMetric{db}
}
