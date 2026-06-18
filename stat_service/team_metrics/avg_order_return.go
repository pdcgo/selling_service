package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type avgOrderReturnMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *avgOrderReturnMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range

	pieces := t.
		db.
		Table("order_items oi").
		Joins("join orders o on o.id = oi.order_id").
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("oi.order_id", "sum(oi.count) as units").
		Group("oi.order_id")

	query = t.
		db.
		Table("orders o").
		Joins("join (?) pieces on pieces.order_id = o.id", pieces).
		Joins("join order_timestamps ot on ot.order_id = o.id and ot.order_status = 'return'").
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *avgOrderReturnMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamAvgOrderReturnMetric{
		Data: map[uint64]*team_metric.TeamAvgOrderReturnItem{},
	}
	resultList := []*team_metric.TeamAvgOrderReturnItem{}

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
		Data: &selling_iface.TeamMetric_TeamAvgOrderReturnMetric{
			TeamAvgOrderReturnMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *avgOrderReturnMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamAvgOrderReturnMetricSort() {
	case team_metric.TeamAvgOrderReturnMetricSort_TEAM_AVG_ORDER_RETURN_METRIC_SORT_TOTAL_PER_TRANSACTION:
		sortField = "sum(o.total)::numeric / nullif(count(o.id), 0) as sfield"
	case team_metric.TeamAvgOrderReturnMetricSort_TEAM_AVG_ORDER_RETURN_METRIC_SORT_PIECE_PER_TRANSACTION:
		sortField = "sum(coalesce(pieces.units,0))::numeric / nullif(count(o.id), 0) as sfield"
	default:
		err = errors.New("team avg order return metric sort invalid sort type")
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

func NewAvgOrderReturnMetric(db *gorm.DB) TeamMetricBase {
	return &avgOrderReturnMetric{db}
}
