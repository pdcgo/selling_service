package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type orderReturnCompletedMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *orderReturnCompletedMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("orders o").
		Joins("join inv_timestamps its on its.tx_id = o.invertory_return_tx_id and its.status = 'completed'").
		Where("its.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *orderReturnCompletedMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamOrderReturnCompletedMetric{
		Data: map[uint64]*team_metric.TeamOrderReturnCompletedItem{},
	}
	resultList := []*team_metric.TeamOrderReturnCompletedItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"count(o.id) as transaction_count",
			"sum(o.total) as total_amount",
			"sum(o.order_mp_total) as mp_total_amount",
			"max(its.timestamp) as last_order_return_completed",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamOrderReturnCompletedMetric{
			TeamOrderReturnCompletedMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *orderReturnCompletedMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamOrderReturnCompletedMetricSort() {
	case team_metric.TeamOrderReturnCompletedMetricSort_TEAM_ORDER_RETURN_COMPLETED_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(o.id) as sfield"
	case team_metric.TeamOrderReturnCompletedMetricSort_TEAM_ORDER_RETURN_COMPLETED_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(o.total) as sfield"
	case team_metric.TeamOrderReturnCompletedMetricSort_TEAM_ORDER_RETURN_COMPLETED_METRIC_SORT_MP_TOTAL_AMOUNT:
		sortField = "sum(o.order_mp_total) as sfield"
	case team_metric.TeamOrderReturnCompletedMetricSort_TEAM_ORDER_RETURN_COMPLETED_METRIC_SORT_LAST_ORDER_RETURN_COMPLETED:
		sortField = "max(its.timestamp) as sfield"
	default:
		err = errors.New("team order return completed metric sort invalid sort type")
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

func NewOrderReturnCompletedMetric(db *gorm.DB) TeamMetricBase {
	return &orderReturnCompletedMetric{db}
}
