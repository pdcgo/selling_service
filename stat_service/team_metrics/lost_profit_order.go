package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

var lostProfitStatus = []string{"return", "return_completed", "problem", "return_problem"}

type lostProfitOrderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *lostProfitOrderMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	firstTimestamp := t.db.
		Table("order_timestamps ot").
		Joins("join orders o on o.id = ot.order_id").
		Where("ot.order_status in (?)", lostProfitStatus).
		Where("ot.timestamp between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("ot.order_id, min(ot.timestamp) as timestamp").
		Group("ot.order_id")

	query = t.
		db.
		Table("orders o").
		Joins("join (?) ot on ot.order_id = o.id", firstTimestamp)

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *lostProfitOrderMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamLostProfitOrderMetric{
		Data: map[uint64]*team_metric.TeamLostProfitOrderItem{},
	}
	resultList := []*team_metric.TeamLostProfitOrderItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(o.order_mp_total - o.total) as lost_profit_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamLostProfitOrderMetric{
			TeamLostProfitOrderMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *lostProfitOrderMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamLostProfitOrderMetricSort() {
	case team_metric.TeamLostProfitOrderMetricSort_TEAM_LOST_PROFIT_ORDER_METRIC_SORT_LOST_PROFIT_AMOUNT:
		sortField = "sum(o.order_mp_total - o.total) as sfield"
	default:
		err = errors.New("team lost profit order metric sort invalid sort type")
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

func NewLostProfitOrderMetric(db *gorm.DB) TeamMetricBase {
	return &lostProfitOrderMetric{db}
}
