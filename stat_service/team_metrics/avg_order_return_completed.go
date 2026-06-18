package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type avgOrderReturnCompletedMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *avgOrderReturnCompletedMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range

	pieces := t.
		db.
		Table("order_items oi").
		Joins("join orders o on o.id = oi.order_id").
		Joins("join inv_timestamps its on its.tx_id = o.invertory_return_tx_id and its.status = 'completed'").
		Where("its.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("oi.order_id", "sum(oi.count) as units").
		Group("oi.order_id")

	query = t.
		db.
		Table("orders o").
		Joins("join (?) pieces on pieces.order_id = o.id", pieces).
		Joins("join inv_timestamps its on its.tx_id = o.invertory_return_tx_id and its.status = 'completed'").
		Where("its.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *avgOrderReturnCompletedMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamAvgOrderReturnCompletedMetric{
		Data: map[uint64]*team_metric.TeamAvgOrderReturnCompletedItem{},
	}
	resultList := []*team_metric.TeamAvgOrderReturnCompletedItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(o.total)::numeric / nullif(count(o.id), 0) as total_per_transaction",
			"sum(coalesce(pieces.units,0))::numeric / nullif(count(o.id), 0) as piece_per_transaction",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamAvgOrderReturnCompletedMetric{
			TeamAvgOrderReturnCompletedMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *avgOrderReturnCompletedMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamAvgOrderReturnCompletedMetricSort() {
	case team_metric.TeamAvgOrderReturnCompletedMetricSort_TEAM_AVG_ORDER_RETURN_COMPLETED_METRIC_SORT_TOTAL_PER_TRANSACTION:
		sortField = "sum(o.total)::numeric / nullif(count(o.id), 0) as sfield"
	case team_metric.TeamAvgOrderReturnCompletedMetricSort_TEAM_AVG_ORDER_RETURN_COMPLETED_METRIC_SORT_PIECE_PER_TRANSACTION:
		sortField = "sum(coalesce(pieces.units,0))::numeric / nullif(count(o.id), 0) as sfield"
	default:
		err = errors.New("team avg order return completed metric sort invalid sort type")
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

func NewAvgOrderReturnCompletedMetric(db *gorm.DB) TeamMetricBase {
	return &avgOrderReturnCompletedMetric{db}
}
