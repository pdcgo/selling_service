package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type revenueProductAdjustmentMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *revenueProductAdjustmentMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("invoices inv").
		Joins("join orders o on o.id = inv.order_id").
		Where("inv.type = 'prod_adjust'").
		Where("inv.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *revenueProductAdjustmentMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamRevenueProductAdjustmentMetric{
		Data: map[uint64]*team_metric.TeamRevenueProductAdjustmentItem{},
	}
	resultList := []*team_metric.TeamRevenueProductAdjustmentItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(inv.amount) as product_adjustment_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamRevenueProductAdjustmentMetric{
			TeamRevenueProductAdjustmentMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *revenueProductAdjustmentMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamRevenueProductAdjustmentMetricSort() {
	case team_metric.TeamRevenueProductAdjustmentMetricSort_TEAM_REVENUE_PRODUCT_ADJUSTMENT_METRIC_SORT_PRODUCT_ADJUSTMENT_AMOUNT:
		sortField = "sum(inv.amount) as sfield"
	default:
		err = errors.New("team revenue product adjustment metric sort invalid sort type")
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

func NewRevenueProductAdjustmentMetric(db *gorm.DB) TeamMetricBase {
	return &revenueProductAdjustmentMetric{db}
}
