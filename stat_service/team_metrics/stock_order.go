package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type stockOrderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *stockOrderMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
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
func (t *stockOrderMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamStockOrderMetric{
		Data: map[uint64]*team_metric.TeamStockOrderItem{},
	}
	resultList := []*team_metric.TeamStockOrderItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(oi.count) as piece_count",
			"sum(oi.count) filter (where oi.owned = true) as own_piece_count",
			"sum(oi.count) filter (where oi.owned = false) as cross_piece_count",
			"sum(oi.total) as piece_amount",
			"sum(oi.total) filter (where oi.owned = true) as own_piece_amount",
			"sum(oi.total) filter (where oi.owned = false) as cross_piece_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamStockOrderMetric{
			TeamStockOrderMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *stockOrderMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamStockOrderMetricSort() {
	case team_metric.TeamStockOrderMetricSort_TEAM_STOCK_ORDER_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(oi.count) as sfield"
	case team_metric.TeamStockOrderMetricSort_TEAM_STOCK_ORDER_METRIC_SORT_OWN_PIECE_COUNT:
		sortField = "sum(oi.count) filter (where oi.owned = true) as sfield"
	case team_metric.TeamStockOrderMetricSort_TEAM_STOCK_ORDER_METRIC_SORT_CROSS_PIECE_COUNT:
		sortField = "sum(oi.count) filter (where oi.owned = false) as sfield"
	case team_metric.TeamStockOrderMetricSort_TEAM_STOCK_ORDER_METRIC_SORT_PIECE_AMOUNT:
		sortField = "sum(oi.total) as sfield"
	case team_metric.TeamStockOrderMetricSort_TEAM_STOCK_ORDER_METRIC_SORT_OWN_PIECE_AMOUNT:
		sortField = "sum(oi.total) filter (where oi.owned = true) as sfield"
	case team_metric.TeamStockOrderMetricSort_TEAM_STOCK_ORDER_METRIC_SORT_CROSS_PIECE_AMOUNT:
		sortField = "sum(oi.total) filter (where oi.owned = false) as sfield"
	default:
		err = errors.New("team stock order metric sort invalid sort type")
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

func NewStockOrderMetric(db *gorm.DB) TeamMetricBase {
	return &stockOrderMetric{db}
}
